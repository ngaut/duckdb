//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_fsst_string_matcher.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_fsst_string_matcher.hpp"

#include "sljit_native_types.hpp"

#include <cstring>

namespace duckdb {

namespace {

//! A pattern token: a literal byte, or a gap that matches any number of bytes
struct PatternToken {
	bool gap;
	uint8_t byte;
};

void AppendLiteral(vector<PatternToken> &tokens, const string &literal) {
	for (auto character : literal) {
		tokens.push_back({false, static_cast<uint8_t>(character)});
	}
}

void AppendGap(vector<PatternToken> &tokens) {
	if (tokens.empty() || !tokens.back().gap) {
		tokens.push_back({true, 0});
	}
}

bool TryReadPatternTokens(const SljitNativePredicate &predicate, vector<PatternToken> &tokens) {
	const auto &constant = predicate.string_constant;
	switch (predicate.kind) {
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
		AppendLiteral(tokens, constant);
		return true;
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		AppendLiteral(tokens, constant);
		AppendGap(tokens);
		return true;
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		AppendGap(tokens);
		AppendLiteral(tokens, constant);
		return true;
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		AppendGap(tokens);
		AppendLiteral(tokens, constant);
		AppendGap(tokens);
		return true;
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
		// the lowered LIKE patterns have no '_' and no escape: '%' is a gap, every other byte is literal
		for (auto character : constant) {
			if (character == '%') {
				AppendGap(tokens);
			} else {
				tokens.push_back({false, static_cast<uint8_t>(character)});
			}
		}
		return true;
	default:
		return false;
	}
}

//! A string predicate as pattern tokens: a leaf under any number of NOTs, of at most 63 tokens so that a set of
//! pattern positions fits a 64-bit mask
bool TryReadPattern(const SljitNativePredicate &predicate, vector<PatternToken> &tokens, bool &negated) {
	static constexpr idx_t MAX_TOKENS = 63;
	negated = false;
	auto leaf = &predicate;
	while (leaf->kind == SljitNativePredicateKind::NOT && leaf->child) {
		negated = !negated;
		leaf = leaf->child.get();
	}
	return TryReadPatternTokens(*leaf, tokens) && tokens.size() <= MAX_TOKENS;
}

} // namespace

bool SljitStringPatternAutomatonSupports(const SljitNativePredicate &predicate) {
	vector<PatternToken> tokens;
	bool negated;
	return TryReadPattern(predicate, tokens, negated);
}

bool TryBuildSljitStringPatternAutomaton(const SljitNativePredicate &predicate,
                                         unique_ptr<SljitStringPatternAutomaton> &result) {
	// the FSST matcher doubles the states for escapes, which a byte addresses
	static constexpr idx_t MAX_STATES = 128;
	result.reset();
	vector<PatternToken> tokens;
	bool negated;
	if (!TryReadPattern(predicate, tokens, negated)) {
		return false;
	}
	// subset construction: a state is the set of pattern positions the input read so far can end at, position i
	// having matched the first i tokens; a gap also matches nothing, so a set holds the position after each of its gaps
	uint64_t gap_positions = 0;
	uint64_t literal_positions[256] = {};
	for (idx_t position = 0; position < tokens.size(); position++) {
		if (tokens[position].gap) {
			gap_positions |= uint64_t(1) << position;
		} else {
			literal_positions[tokens[position].byte] |= uint64_t(1) << position;
		}
	}
	auto close_over_gaps = [&](uint64_t positions) {
		for (auto next = positions | ((positions & gap_positions) << 1); next != positions;
		     next = positions | ((positions & gap_positions) << 1)) {
			positions = next;
		}
		return positions;
	};
	vector<uint64_t> states {close_over_gaps(1)};
	auto state_id = [&](uint64_t positions) -> idx_t {
		for (idx_t id = 0; id < states.size(); id++) {
			if (states[id] == positions) {
				return id;
			}
		}
		states.push_back(positions);
		return states.size() - 1;
	};
	auto automaton = make_uniq<SljitStringPatternAutomaton>();
	automaton->negated = negated;
	const auto accept_position = uint64_t(1) << tokens.size();
	for (idx_t state = 0; state < states.size(); state++) {
		if (states.size() > MAX_STATES) {
			return false;
		}
		const auto positions = states[state];
		automaton->accepting.push_back((positions & accept_position) != 0);
		automaton->transitions.resize(states.size() * 256);
		// a byte no literal of the set expects only keeps the positions on gaps
		const auto other = state_id(close_over_gaps(positions & gap_positions));
		for (idx_t byte = 0; byte < 256; byte++) {
			const auto advanced = positions & literal_positions[byte];
			automaton->transitions[state * 256 + byte] = static_cast<uint8_t>(
			    advanced ? state_id(close_over_gaps((positions & gap_positions) | (advanced << 1))) : other);
		}
	}
	if (states.size() > MAX_STATES) {
		return false;
	}
	automaton->transitions.resize(states.size() * 256);
	result = std::move(automaton);
	return true;
}

namespace {

//! The automaton advanced by whole FSST symbols. States below state_count are the automaton's; state_count + s is
//! state s after an escape code, waiting for the literal byte that follows it
class SljitFSSTStringMatcher final : public CompressedStringMatcher {
public:
	SljitFSSTStringMatcher(const SljitStringPatternAutomaton &automaton, const uint8_t *symbol_lengths,
	                       const uint64_t *symbols)
	    : transitions(automaton.StateCount() * 2 * 256), accepting(automaton.StateCount() * 2, false) {
		static constexpr idx_t ESCAPE_CODE = 255;
		const auto state_count = automaton.StateCount();
		for (idx_t state = 0; state < state_count; state++) {
			for (idx_t code = 0; code < ESCAPE_CODE; code++) {
				idx_t current = state;
				for (idx_t i = 0; i < symbol_lengths[code]; i++) {
					current = automaton.transitions[current * 256 + ((symbols[code] >> (8 * i)) & 0xFF)];
				}
				transitions[state * 256 + code] = static_cast<uint8_t>(current);
			}
			transitions[state * 256 + ESCAPE_CODE] = static_cast<uint8_t>(state_count + state);
			memcpy(transitions.data() + (state_count + state) * 256, automaton.transitions.data() + state * 256, 256);
			accepting[state] = automaton.accepting[state] != automaton.negated;
		}
	}

	void Match(const_data_ptr_t codes, const uint32_t *lengths, idx_t count, bool *matches) const override {
		// strings run in groups of LANES side by side, each lane an independent chain of table lookups
		static constexpr idx_t LANES = 4;
		static const uint8_t EMPTY_STRING_CODE = 0;
		const auto table = transitions.data();
		idx_t offset = 0;
		idx_t i = 0;
		for (; i + LANES <= count; i += LANES) {
			const_data_ptr_t lane_codes[LANES];
			uint32_t lane_lengths[LANES];
			uint32_t steps = 0;
			for (idx_t lane = 0; lane < LANES; lane++) {
				lane_lengths[lane] = lengths[i + lane];
				lane_codes[lane] = lane_lengths[lane] ? codes + offset : &EMPTY_STRING_CODE;
				offset += lane_lengths[lane];
				steps = MaxValue(steps, lane_lengths[lane]);
			}
			uint32_t lane_states[LANES] = {0, 0, 0, 0};
			for (uint32_t step = 0; step < steps; step++) {
				for (idx_t lane = 0; lane < LANES; lane++) {
					const bool active = step < lane_lengths[lane];
					const auto next = table[lane_states[lane] * 256 + lane_codes[lane][active ? step : 0]];
					lane_states[lane] = active ? next : lane_states[lane];
				}
			}
			for (idx_t lane = 0; lane < LANES; lane++) {
				matches[i + lane] = accepting[lane_states[lane]];
			}
		}
		for (; i < count; i++) {
			uint32_t state = 0;
			for (uint32_t step = 0; step < lengths[i]; step++) {
				state = table[state * 256 + codes[offset + step]];
			}
			offset += lengths[i];
			matches[i] = accepting[state];
		}
	}

private:
	vector<uint8_t> transitions;
	vector<bool> accepting;
};

} // namespace

unique_ptr<CompressedStringMatcher> CreateSljitFSSTStringMatcher(const SljitStringPatternAutomaton &automaton,
                                                                 const uint8_t *symbol_lengths,
                                                                 const uint64_t *symbols) {
	return make_uniq<SljitFSSTStringMatcher>(automaton, symbol_lengths, symbols);
}

} // namespace duckdb

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

//! The matcher doubles the states for escapes, which a byte addresses
static constexpr idx_t MAX_STATES = 128;

//! A deterministic automaton over bytes under construction, starting in state 0
struct ByteAutomaton {
	//! transitions[state * 256 + byte]
	vector<idx_t> transitions;
	vector<bool> accepting;

	idx_t StateCount() const {
		return accepting.size();
	}
};

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

//! The automaton of a pattern by subset construction: a state is the set of pattern positions the input read so far
//! can end at, position i having matched the first i tokens; a gap also matches nothing, so a set holds the position
//! after each of its gaps. A set of positions is a 64-bit mask, so a pattern has at most 63 tokens
bool TryBuildPatternAutomaton(const vector<PatternToken> &tokens, ByteAutomaton &result) {
	static constexpr idx_t MAX_TOKENS = 63;
	if (tokens.size() > MAX_TOKENS) {
		return false;
	}
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
	const auto accept_position = uint64_t(1) << tokens.size();
	for (idx_t state = 0; state < states.size() && states.size() <= MAX_STATES; state++) {
		const auto positions = states[state];
		result.accepting.push_back((positions & accept_position) != 0);
		// a byte no literal of the set expects only keeps the positions on gaps
		const auto other = state_id(close_over_gaps(positions & gap_positions));
		for (idx_t byte = 0; byte < 256; byte++) {
			const auto advanced = positions & literal_positions[byte];
			result.transitions.push_back(
			    advanced ? state_id(close_over_gaps((positions & gap_positions) | (advanced << 1))) : other);
		}
	}
	return states.size() <= MAX_STATES;
}

//! The automaton of a comparison with a constant of n bytes, as DuckDB orders strings: bytes unsigned, then lengths.
//! State i < n has read the constant's first i bytes, so the string so far is shorter; state n has read the constant;
//! the last two states have read a smaller or a larger byte, or a longer string, and stay there
bool TryBuildCompareAutomaton(const string &constant, SljitNativeIntegerCompareOp compare_op, ByteAutomaton &result) {
	const auto n = constant.size();
	if (n + 3 > MAX_STATES) {
		return false;
	}
	const auto less = n + 1;
	const auto greater = n + 2;
	for (idx_t state = 0; state <= greater; state++) {
		for (idx_t byte = 0; byte < 256; byte++) {
			idx_t next = state;
			if (state < n) {
				const auto expected = static_cast<uint8_t>(constant[state]);
				next = byte < expected ? less : (byte > expected ? greater : state + 1);
			} else if (state == n) {
				next = greater;
			}
			result.transitions.push_back(next);
		}
		const bool is_less = state < n || state == less;
		const bool is_equal = state == n;
		const bool is_greater = state == greater;
		switch (compare_op) {
		case SljitNativeIntegerCompareOp::EQUAL:
			result.accepting.push_back(is_equal);
			break;
		case SljitNativeIntegerCompareOp::NOT_EQUAL:
			result.accepting.push_back(!is_equal);
			break;
		case SljitNativeIntegerCompareOp::LESS_THAN:
			result.accepting.push_back(is_less);
			break;
		case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
			result.accepting.push_back(is_less || is_equal);
			break;
		case SljitNativeIntegerCompareOp::GREATER_THAN:
			result.accepting.push_back(is_greater);
			break;
		case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
			result.accepting.push_back(is_greater || is_equal);
			break;
		}
	}
	return true;
}

//! The product automaton: it runs both at once and accepts as the conjunction combines their acceptance
bool TryBuildProductAutomaton(const ByteAutomaton &left, const ByteAutomaton &right, bool both, ByteAutomaton &result) {
	vector<idx_t> state_ids(left.StateCount() * right.StateCount(), DConstants::INVALID_INDEX);
	vector<idx_t> pairs {0};
	state_ids[0] = 0;
	for (idx_t state = 0; state < pairs.size(); state++) {
		const auto left_state = pairs[state] / right.StateCount();
		const auto right_state = pairs[state] % right.StateCount();
		result.accepting.push_back(both ? left.accepting[left_state] && right.accepting[right_state]
		                                : left.accepting[left_state] || right.accepting[right_state]);
		for (idx_t byte = 0; byte < 256; byte++) {
			const auto pair = left.transitions[left_state * 256 + byte] * right.StateCount() +
			                  right.transitions[right_state * 256 + byte];
			if (state_ids[pair] == DConstants::INVALID_INDEX) {
				if (pairs.size() == MAX_STATES) {
					return false;
				}
				state_ids[pair] = pairs.size();
				pairs.push_back(pair);
			}
			result.transitions.push_back(state_ids[pair]);
		}
	}
	return true;
}

//! The automaton of a predicate over one string column: comparisons with a constant, patterns of literal bytes and
//! gaps, and lists of constants, combined by NOT, AND and OR. Each is true or false for a string, which is never
//! NULL here: storage evaluates the filter on NULL itself
bool TryBuildPredicateAutomaton(const SljitNativePredicate &predicate, ByteAutomaton &result) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::NOT:
		if (!predicate.child || !TryBuildPredicateAutomaton(*predicate.child, result)) {
			return false;
		}
		result.accepting.flip();
		return true;
	case SljitNativePredicateKind::CONJUNCTION: {
		if (predicate.children.empty() || !predicate.children[0] ||
		    !TryBuildPredicateAutomaton(*predicate.children[0], result)) {
			return false;
		}
		const bool both = predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND;
		for (idx_t child_idx = 1; child_idx < predicate.children.size(); child_idx++) {
			ByteAutomaton child;
			ByteAutomaton product;
			if (!predicate.children[child_idx] || !TryBuildPredicateAutomaton(*predicate.children[child_idx], child) ||
			    !TryBuildProductAutomaton(result, child, both, product)) {
				return false;
			}
			result = std::move(product);
		}
		return true;
	}
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
		return TryBuildCompareAutomaton(predicate.string_constant, SljitNativeIntegerCompareOp::EQUAL, result);
	case SljitNativePredicateKind::STRING_COMPARE_CONSTANT:
		return TryBuildCompareAutomaton(predicate.string_constant, predicate.compare_op, result);
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT: {
		// a NULL in the list makes the result NULL instead of false, which a NOT around it would not turn true
		if (predicate.string_constants.empty() || predicate.list_has_null) {
			return false;
		}
		if (!TryBuildCompareAutomaton(predicate.string_constants[0], SljitNativeIntegerCompareOp::EQUAL, result)) {
			return false;
		}
		for (idx_t constant_idx = 1; constant_idx < predicate.string_constants.size(); constant_idx++) {
			ByteAutomaton constant;
			ByteAutomaton product;
			if (!TryBuildCompareAutomaton(predicate.string_constants[constant_idx], SljitNativeIntegerCompareOp::EQUAL,
			                              constant) ||
			    !TryBuildProductAutomaton(result, constant, false, product)) {
				return false;
			}
			result = std::move(product);
		}
		if (predicate.not_in) {
			result.accepting.flip();
		}
		return true;
	}
	default: {
		vector<PatternToken> tokens;
		return TryReadPatternTokens(predicate, tokens) && TryBuildPatternAutomaton(tokens, result);
	}
	}
}

} // namespace

bool SljitStringPatternAutomatonSupports(const SljitNativePredicate &predicate) {
	unique_ptr<SljitStringPatternAutomaton> automaton;
	return TryBuildSljitStringPatternAutomaton(predicate, automaton);
}

bool TryBuildSljitStringPatternAutomaton(const SljitNativePredicate &predicate,
                                         unique_ptr<SljitStringPatternAutomaton> &result) {
	result.reset();
	ByteAutomaton automaton;
	if (!TryBuildPredicateAutomaton(predicate, automaton)) {
		return false;
	}
	result = make_uniq<SljitStringPatternAutomaton>();
	result->accepting = std::move(automaton.accepting);
	result->transitions.reserve(automaton.transitions.size());
	for (auto next : automaton.transitions) {
		result->transitions.push_back(static_cast<uint8_t>(next));
	}
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
			accepting[state] = automaton.accepting[state];
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

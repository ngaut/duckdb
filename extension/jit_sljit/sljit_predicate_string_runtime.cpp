//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_string_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_string_runtime.hpp"

#include "sljit_native_types.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <cstring>
#include <utility>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace duckdb {

static uint8_t SljitLikeFragmentAnchorScore(unsigned char c) {
	static constexpr char RARE_TO_COMMON[] = "qzxjkvbpygfwmucldrhsnioate";
	if (c >= 'A' && c <= 'Z') {
		c = static_cast<unsigned char>(c - 'A' + 'a');
	}
	for (idx_t idx = 0; idx + 1 < sizeof(RARE_TO_COMMON); idx++) {
		if (c == static_cast<unsigned char>(RARE_TO_COMMON[idx])) {
			return UnsafeNumericCast<uint8_t>(idx + 1);
		}
	}
	return 0;
}

static idx_t SljitLikeFragmentAnchor(const char *data, idx_t length) {
	D_ASSERT(length > 0);
	idx_t anchor = 0;
	auto best_score = SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[0]));
	for (idx_t idx = 1; idx < length; idx++) {
		auto score = SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[idx]));
		if (score < best_score) {
			anchor = idx;
			best_score = score;
		}
	}
	return anchor;
}

static idx_t SljitLikeFragmentPairAnchor(const char *data, idx_t length) {
	D_ASSERT(length >= 2);
	idx_t anchor = 0;
	auto best_score = NumericCast<uint16_t>(SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[0]))) +
	                  SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[1]));
	for (idx_t idx = 1; idx + 1 < length; idx++) {
		auto score = NumericCast<uint16_t>(SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[idx]))) +
		             SljitLikeFragmentAnchorScore(static_cast<unsigned char>(data[idx + 1]));
		if (score < best_score) {
			anchor = idx;
			best_score = score;
		}
	}
	return anchor;
}

#if defined(__aarch64__)
static __attribute__((noinline, cold)) idx_t SljitVerifyLikeFragmentPairCandidates(const char *sdata,
                                                                                   const char *fragment,
                                                                                   idx_t fragment_length,
                                                                                   idx_t pair_anchor, idx_t position) {
	for (idx_t lane = 0; lane < 16; lane++) {
		auto pair_position = position + lane;
		if (sdata[pair_position] != fragment[pair_anchor] || sdata[pair_position + 1] != fragment[pair_anchor + 1]) {
			continue;
		}
		auto fragment_position = pair_position - pair_anchor;
		if (memcmp(sdata + fragment_position, fragment, fragment_length) == 0) {
			return fragment_position;
		}
	}
	return DConstants::INVALID_INDEX;
}

static idx_t SljitFindLikeFragmentPairArm64(const char *sdata, idx_t slen, const char *fragment, idx_t fragment_length,
                                            idx_t pair_anchor) {
	D_ASSERT(fragment_length >= 2);
	if (fragment_length > slen) {
		return DConstants::INVALID_INDEX;
	}
	const auto first_byte = vdupq_n_u8(static_cast<uint8_t>(fragment[pair_anchor]));
	const auto second_byte = vdupq_n_u8(static_cast<uint8_t>(fragment[pair_anchor + 1]));
	idx_t position = pair_anchor;
	const auto last_pair = slen - fragment_length + pair_anchor;
	while (position + 15 <= last_pair) {
		auto first_matches = vceqq_u8(vld1q_u8(reinterpret_cast<const uint8_t *>(sdata + position)), first_byte);
		auto second_matches = vceqq_u8(vld1q_u8(reinterpret_cast<const uint8_t *>(sdata + position + 1)), second_byte);
		if (vmaxvq_u8(vandq_u8(first_matches, second_matches)) != 0) {
			auto fragment_position =
			    SljitVerifyLikeFragmentPairCandidates(sdata, fragment, fragment_length, pair_anchor, position);
			if (fragment_position != DConstants::INVALID_INDEX) {
				return fragment_position;
			}
		}
		position += 16;
	}
	while (position <= last_pair) {
		if (sdata[position] == fragment[pair_anchor] && sdata[position + 1] == fragment[pair_anchor + 1]) {
			auto fragment_position = position - pair_anchor;
			if (memcmp(sdata + fragment_position, fragment, fragment_length) == 0) {
				return fragment_position;
			}
		}
		position++;
	}
	return DConstants::INVALID_INDEX;
}
#endif

static idx_t SljitFindLikeFragment(const char *sdata, idx_t slen, const char *fragment, idx_t fragment_length,
                                   idx_t anchor, idx_t pair_anchor) {
#if defined(__aarch64__)
	if (fragment_length >= 2) {
		return SljitFindLikeFragmentPairArm64(sdata, slen, fragment, fragment_length, pair_anchor);
	}
#endif
	if (fragment_length > slen) {
		return DConstants::INVALID_INDEX;
	}
	const auto anchor_byte = static_cast<unsigned char>(fragment[anchor]);
	idx_t position = anchor;
	const auto last_anchor = slen - fragment_length + anchor;
	while (position <= last_anchor) {
		auto location = static_cast<const char *>(memchr(sdata + position, anchor_byte, last_anchor - position + 1));
		if (!location) {
			return DConstants::INVALID_INDEX;
		}
		const auto anchor_position = UnsafeNumericCast<idx_t>(location - sdata);
		const auto fragment_position = anchor_position - anchor;
		if (memcmp(sdata + fragment_position, fragment, fragment_length) == 0) {
			return fragment_position;
		}
		position = anchor_position + 1;
	}
	return DConstants::INVALID_INDEX;
}

SljitNativeStringConstant::SljitNativeStringConstant(string value_p) : value(std::move(value_p)) {
	like_anchor_start = value.empty() || value[0] != '%';
	like_anchor_end = value.empty() || value[value.size() - 1] != '%';
	idx_t pattern_idx = 0;
	while (pattern_idx < value.size()) {
		while (pattern_idx < value.size() && value[pattern_idx] == '%') {
			pattern_idx++;
		}
		const auto fragment_start = pattern_idx;
		while (pattern_idx < value.size() && value[pattern_idx] != '%') {
			pattern_idx++;
		}
		const auto fragment_length = pattern_idx - fragment_start;
		if (fragment_length > 0) {
			const auto anchor = SljitLikeFragmentAnchor(value.data() + fragment_start, fragment_length);
			const auto pair_anchor = fragment_length >= 2
			                             ? SljitLikeFragmentPairAnchor(value.data() + fragment_start, fragment_length)
			                             : idx_t(0);
			like_fragments.push_back({fragment_start, fragment_length, anchor, pair_anchor});
		}
	}
}

SljitNativeStringConstantList::SljitNativeStringConstantList(vector<string> values_p) : values(std::move(values_p)) {
}

SljitNativeStringLikeBatchPlan::SljitNativeStringLikeBatchPlan(string pattern_p, idx_t source_index_p, bool negate_p)
    : pattern(std::move(pattern_p)), source_index(source_index_p), negate(negate_p) {
}

sljit_sw SLJIT_FUNC SljitNativeStringLikePercentOnly(const char *sdata, idx_t slen,
                                                     const SljitNativeStringConstant *pattern) {
	const auto &pattern_value = pattern->value;
	const auto pdata = pattern_value.data();
	idx_t position = 0;

	for (idx_t fragment_idx = 0; fragment_idx < pattern->like_fragments.size(); fragment_idx++) {
		auto &fragment = pattern->like_fragments[fragment_idx];
		const auto fragment_start = fragment.start;
		const auto fragment_length = fragment.length;
		const bool first_fragment = fragment_idx == 0;
		const bool last_fragment = fragment_idx + 1 == pattern->like_fragments.size();

		if (first_fragment && pattern->like_anchor_start) {
			if (slen < fragment_length || memcmp(sdata, pdata + fragment_start, fragment_length) != 0) {
				return false;
			}
			position = fragment_length;
			if (last_fragment && pattern->like_anchor_end) {
				return position == slen;
			}
			continue;
		}
		if (last_fragment && pattern->like_anchor_end) {
			if (slen < fragment_length) {
				return false;
			}
			auto suffix_position = slen - fragment_length;
			return suffix_position >= position &&
			       memcmp(sdata + suffix_position, pdata + fragment_start, fragment_length) == 0;
		}

		auto match_offset = SljitFindLikeFragment(sdata + position, slen - position, pdata + fragment_start,
		                                          fragment_length, fragment.anchor, fragment.pair_anchor);
		if (match_offset == DConstants::INVALID_INDEX) {
			return false;
		}
		position += match_offset + fragment_length;
	}
	return !pattern->like_fragments.empty() || !pattern->like_anchor_start || !pattern->like_anchor_end || slen == 0;
}

static inline bool SljitStringLikeTwoUnanchoredFragments(const char *sdata, idx_t slen,
                                                         const SljitNativeStringConstant &pattern) {
	D_ASSERT(!pattern.like_anchor_start);
	D_ASSERT(!pattern.like_anchor_end);
	D_ASSERT(pattern.like_fragments.size() == 2);

	const auto fragments = pattern.like_fragments.data();
	const auto pdata = pattern.value.data();
	const auto &first = fragments[0];
	const auto first_offset =
	    SljitFindLikeFragment(sdata, slen, pdata + first.start, first.length, first.anchor, first.pair_anchor);
	if (first_offset == DConstants::INVALID_INDEX) {
		return false;
	}

	const auto second_position = first_offset + first.length;
	const auto &second = fragments[1];
	return SljitFindLikeFragment(sdata + second_position, slen - second_position, pdata + second.start, second.length,
	                             second.anchor, second.pair_anchor) != DConstants::INVALID_INDEX;
}

sljit_sw SLJIT_FUNC SljitNativeStringLikeTwoUnanchoredFragments(const char *sdata, idx_t slen,
                                                                const SljitNativeStringConstant *pattern) {
	return SljitStringLikeTwoUnanchoredFragments(sdata, slen, *pattern);
}

bool TryBuildSljitNativeStringLikeBatchPlan(const SljitNativePredicate &predicate,
                                            unique_ptr<SljitNativeStringLikeBatchPlan> &result) {
	result.reset();
	auto candidate = &predicate;
	bool negate = false;
	while (candidate->kind == SljitNativePredicateKind::NOT && candidate->child) {
		negate = !negate;
		candidate = candidate->child.get();
	}
	if (candidate->kind != SljitNativePredicateKind::STRING_LIKE_CONSTANT) {
		return false;
	}

	auto plan = make_uniq<SljitNativeStringLikeBatchPlan>(candidate->string_constant, candidate->source_index, negate);
	if (plan->pattern.like_anchor_start || plan->pattern.like_anchor_end || plan->pattern.like_fragments.size() != 2) {
		return false;
	}
	result = std::move(plan);
	return true;
}

template <bool HAS_EXECUTE_SELECTION, bool HAS_SOURCE_SELECTION, bool HAS_VALIDITY, bool NEGATE>
static idx_t SljitSelectStringLikeBatchLoop(const SljitNativeStringConstant &pattern, const string_t *source_data,
                                            const sel_t *source_sel, const ValidityMask &validity,
                                            const sel_t *execute_sel, sel_t *result, idx_t count) {
	if constexpr (NEGATE && !HAS_EXECUTE_SELECTION) {
		idx_t selected_count = 0;
		bool selection_materialized = false;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto logical_index = HAS_EXECUTE_SELECTION ? execute_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
			const auto source_index = HAS_SOURCE_SELECTION ? source_sel[logical_index] : logical_index;
			const auto rejected = (HAS_VALIDITY && !validity.RowIsValid(source_index)) ||
			                      SljitStringLikeTwoUnanchoredFragments(source_data[source_index].GetData(),
			                                                            source_data[source_index].GetSize(), pattern);
			if (!rejected) {
				if (selection_materialized) {
					result[selected_count++] = logical_index;
				}
				continue;
			}
			if (!selection_materialized) {
				for (idx_t prefix_idx = 0; prefix_idx < row_idx; prefix_idx++) {
					result[prefix_idx] = UnsafeNumericCast<sel_t>(prefix_idx);
				}
				selected_count = row_idx;
				selection_materialized = true;
			}
		}
		return selection_materialized ? selected_count : count;
	}

	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto logical_index = HAS_EXECUTE_SELECTION ? execute_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		const auto source_index = HAS_SOURCE_SELECTION ? source_sel[logical_index] : logical_index;
		if (HAS_VALIDITY && !validity.RowIsValid(source_index)) {
			continue;
		}
		const auto &value = source_data[source_index];
		const auto matches = SljitStringLikeTwoUnanchoredFragments(value.GetData(), value.GetSize(), pattern);
		if (matches != NEGATE) {
			result[selected_count++] = logical_index;
		}
	}
	return selected_count;
}

template <bool HAS_EXECUTE_SELECTION, bool HAS_SOURCE_SELECTION, bool HAS_VALIDITY>
static idx_t SljitSelectStringLikeBatchNegation(const SljitNativeStringLikeBatchPlan &plan, const string_t *source_data,
                                                const sel_t *source_sel, const ValidityMask &validity,
                                                const sel_t *execute_sel, sel_t *result, idx_t count) {
	if (plan.negate) {
		return SljitSelectStringLikeBatchLoop<HAS_EXECUTE_SELECTION, HAS_SOURCE_SELECTION, HAS_VALIDITY, true>(
		    plan.pattern, source_data, source_sel, validity, execute_sel, result, count);
	}
	return SljitSelectStringLikeBatchLoop<HAS_EXECUTE_SELECTION, HAS_SOURCE_SELECTION, HAS_VALIDITY, false>(
	    plan.pattern, source_data, source_sel, validity, execute_sel, result, count);
}

template <bool HAS_EXECUTE_SELECTION, bool HAS_SOURCE_SELECTION>
static idx_t SljitSelectStringLikeBatchValidity(const SljitNativeStringLikeBatchPlan &plan, const string_t *source_data,
                                                const sel_t *source_sel, const ValidityMask &validity,
                                                const sel_t *execute_sel, sel_t *result, idx_t count) {
	if (validity.CanHaveNull()) {
		return SljitSelectStringLikeBatchNegation<HAS_EXECUTE_SELECTION, HAS_SOURCE_SELECTION, true>(
		    plan, source_data, source_sel, validity, execute_sel, result, count);
	}
	return SljitSelectStringLikeBatchNegation<HAS_EXECUTE_SELECTION, HAS_SOURCE_SELECTION, false>(
	    plan, source_data, source_sel, validity, execute_sel, result, count);
}

idx_t SljitSelectNativeStringLikeBatch(const SljitNativeStringLikeBatchPlan &plan, Vector &source,
                                       SelectionVector &result, const SelectionVector *execute_sel, idx_t count) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	const auto source_data = UnifiedVectorFormat::GetData<string_t>(source_format);
	const auto source_sel = source_format.sel ? source_format.sel->data() : nullptr;
	const auto execute_sel_data = execute_sel ? execute_sel->data() : nullptr;
	if (execute_sel_data) {
		if (source_sel) {
			return SljitSelectStringLikeBatchValidity<true, true>(plan, source_data, source_sel, source_format.validity,
			                                                      execute_sel_data, result.data(), count);
		}
		return SljitSelectStringLikeBatchValidity<true, false>(plan, source_data, nullptr, source_format.validity,
		                                                       execute_sel_data, result.data(), count);
	}
	if (source_sel) {
		return SljitSelectStringLikeBatchValidity<false, true>(plan, source_data, source_sel, source_format.validity,
		                                                       nullptr, result.data(), count);
	}
	return SljitSelectStringLikeBatchValidity<false, false>(plan, source_data, nullptr, source_format.validity, nullptr,
	                                                        result.data(), count);
}

sljit_sw SLJIT_FUNC SljitNativeStringInListConstant(const char *sdata, idx_t slen,
                                                    const SljitNativeStringConstantList *list) {
	for (auto &constant : list->values) {
		if (constant.empty() && slen == 0) {
			return true;
		}
		if (constant.size() == slen && memcmp(sdata, constant.data(), slen) == 0) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb

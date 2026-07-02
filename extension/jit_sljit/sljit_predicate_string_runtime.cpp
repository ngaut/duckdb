//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_string_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_string_runtime.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include <cstring>
#include <utility>

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

static idx_t SljitFindLikeFragment(const char *sdata, idx_t slen, const char *fragment, idx_t fragment_length,
                                   idx_t anchor) {
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
			like_fragments.push_back({fragment_start, fragment_length, anchor});
		}
	}
}

SljitNativeStringConstantList::SljitNativeStringConstantList(vector<string> values_p) : values(std::move(values_p)) {
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
		                                          fragment_length, fragment.anchor);
		if (match_offset == DConstants::INVALID_INDEX) {
			return false;
		}
		position += match_offset + fragment_length;
	}
	return !pattern->like_fragments.empty() || !pattern->like_anchor_start || !pattern->like_anchor_end || slen == 0;
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

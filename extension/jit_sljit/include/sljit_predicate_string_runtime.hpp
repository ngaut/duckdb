//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_string_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitNativeStringLikeFragment {
	idx_t start = 0;
	idx_t length = 0;
	idx_t anchor = 0;
	idx_t pair_anchor = 0;
};

struct SljitNativeStringConstant {
	explicit SljitNativeStringConstant(string value_p);

	string value;
	vector<SljitNativeStringLikeFragment> like_fragments;
	bool like_anchor_start = true;
	bool like_anchor_end = true;
};

struct SljitNativeStringConstantList {
	explicit SljitNativeStringConstantList(vector<string> values_p);

	vector<string> values;
};

sljit_sw SLJIT_FUNC SljitNativeStringLikePercentOnly(const char *sdata, idx_t slen,
                                                     const SljitNativeStringConstant *pattern);
sljit_sw SLJIT_FUNC SljitNativeStringLikeTwoUnanchoredFragments(const char *sdata, idx_t slen,
                                                                const SljitNativeStringConstant *pattern);
sljit_sw SLJIT_FUNC SljitNativeStringInListConstant(const char *sdata, idx_t slen,
                                                    const SljitNativeStringConstantList *list);

} // namespace duckdb

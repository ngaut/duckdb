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
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitNativePredicate;

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

struct SljitNativeStringLikeBatchPlan {
	SljitNativeStringLikeBatchPlan(string pattern_p, idx_t source_index_p, bool negate_p);

	SljitNativeStringConstant pattern;
	idx_t source_index;
	bool negate;
};

bool TryBuildSljitNativeStringLikeBatchPlan(const SljitNativePredicate &predicate,
                                            unique_ptr<SljitNativeStringLikeBatchPlan> &result);
idx_t SljitSelectNativeStringLikeBatch(const SljitNativeStringLikeBatchPlan &plan, Vector &source,
                                       SelectionVector &result, const SelectionVector *execute_sel, idx_t count);

sljit_sw SLJIT_FUNC SljitNativeStringLikePercentOnly(const char *sdata, idx_t slen,
                                                     const SljitNativeStringConstant *pattern);
sljit_sw SLJIT_FUNC SljitNativeStringLikeTwoUnanchoredFragments(const char *sdata, idx_t slen,
                                                                const SljitNativeStringConstant *pattern);
sljit_sw SLJIT_FUNC SljitNativeStringInListConstant(const char *sdata, idx_t slen,
                                                    const SljitNativeStringConstantList *list);

} // namespace duckdb

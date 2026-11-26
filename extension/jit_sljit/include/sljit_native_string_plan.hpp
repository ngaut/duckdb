//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_string_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_plan.hpp"

namespace duckdb {

bool TryReadNativeStringMatchConstant(const ExecutionExpressionIR &root, ExecutionExpressionIntrinsicKind intrinsic,
                                      idx_t &source_index, string &constant);
bool SljitNativeLikePatternIsPercentOnly(const string &pattern);
//! A comparison of a string column with a non-NULL constant, the column on the left of compare_op
bool TryReadNativeStringCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                        idx_t &source_index, string &constant);
bool TryReadNativeStringEqualConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &constant);
bool TryReadNativeStringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                       vector<string> &constants, bool &list_has_null, bool &not_in);

} // namespace duckdb

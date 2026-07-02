//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_integer_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_plan.hpp"

namespace duckdb {

bool TryReadNativeInt128CompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                          idx_t &left_source_index, idx_t &right_source_index);
bool TryReadNativeInt128CompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                        idx_t &source_index, uint64_t &lower, int64_t &upper, bool &constant_on_left);
bool TryReadNativeIntegerCompareNullConstant(const ExecutionExpressionIR &root);
bool IsNativeCompareRoot(const ExecutionExpressionIR &root);

} // namespace duckdb

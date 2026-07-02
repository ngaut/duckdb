//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_double_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_plan.hpp"

namespace duckdb {

bool TryReadNativeDoubleCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                        SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                        double &source_scale, double &constant_value, bool &constant_on_left);
bool TryReadNativeDoubleCompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                          SljitNativeDoubleSourceKind &left_kind, idx_t &left_source_index,
                                          double &left_scale, SljitNativeDoubleSourceKind &right_kind,
                                          idx_t &right_source_index, double &right_scale);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_expression_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

void SljitPrepareExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                            SljitExecutableRegionExpression &expr,
                                            const vector<bool> *input_not_null = nullptr,
                                            bool copy_auxiliary_expression_tree = false);
bool SljitCompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
                                                    string &error);
bool SljitPrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                                      SljitExecutableRegionExpression &expr, string &error,
                                                      const vector<bool> *input_not_null = nullptr);
bool SljitPrepareAndCompileExecutableFilter(const SljitNativeRegionExpressionPlan &plan,
                                            SljitExecutableRegionOp &filter_op, string &error,
                                            const vector<bool> *input_not_null = nullptr,
                                            bool copy_auxiliary_expression_tree = false);
bool SljitTryBuildFlatFusedProjections(SljitExecutableRegionOp &op, string &error);

} // namespace duckdb

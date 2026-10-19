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
//! Prepares a predicate that is already executable: its sources become input slots through input_source_indices
void SljitPrepareExecutablePredicateInputs(SljitExecutableRegionExpression &expr,
                                           const vector<bool> *input_not_null = nullptr);
//! The plan an executable expression was prepared from. Preparation addresses a predicate's sources by input slot;
//! composition, remapping and re-preparation read input columns, so they start from this plan, never from expr.plan.
SljitNativeRegionExpressionPlan SljitCopySemanticExpressionPlan(const SljitExecutableRegionExpression &expr);
bool SljitCompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
                                                    string &error);
bool SljitPrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                                      SljitExecutableRegionExpression &expr, string &error,
                                                      const vector<bool> *input_not_null = nullptr);
void SljitPrepareExecutableFilter(const SljitNativeRegionExpressionPlan &plan, SljitExecutableRegionOp &filter_op,
                                  const vector<bool> *input_not_null = nullptr,
                                  bool copy_auxiliary_expression_tree = false);
bool SljitCompilePreparedExecutableFilter(SljitExecutableRegionOp &filter_op, string &error);
bool SljitPrepareAndCompileExecutableFilter(const SljitNativeRegionExpressionPlan &plan,
                                            SljitExecutableRegionOp &filter_op, string &error,
                                            const vector<bool> *input_not_null = nullptr,
                                            bool copy_auxiliary_expression_tree = false);
bool SljitTryBuildFlatFusedProjections(SljitExecutableRegionOp &op, string &error);

} // namespace duckdb

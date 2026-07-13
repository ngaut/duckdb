//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

bool SljitTryBuildFilteredAggregateUpdate(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &aggregate_op,
                                          string &error, const vector<bool> &input_not_null,
                                          const vector<Value> &input_min_values, const vector<Value> &input_max_values);
void SljitBuildExecutableAggregateUpdateMetadata(const SljitNativeAggregateUpdatePlan &op,
                                                 SljitExecutableAggregateUpdate &executable,
                                                 const vector<bool> &input_not_null);
bool SljitBuildExecutableAggregateUpdatePayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                    SljitExecutableAggregateUpdate &executable, string &error,
                                                    const vector<bool> &input_not_null,
                                                    const vector<Value> &input_min_values,
                                                    const vector<Value> &input_max_values);
bool SljitBuildExecutablePrimitiveRunUpdateCode(const SljitNativeAggregateUpdatePlan &op,
                                                SljitExecutableAggregateUpdate &executable, string &error);
void SljitSelectExecutableAggregateDirectUpdatePlan(SljitExecutableAggregateUpdate &executable);
bool SljitBuildExecutableAggregateUpdateFallbackPayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                            SljitExecutableAggregateUpdate &executable, string &error);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_aggregate_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_preaggregated_primitive_update_codegen.hpp"
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
void SljitPlanExecutablePrimitiveRunUpdate(const SljitNativeAggregateUpdatePlan &op,
                                           SljitExecutableAggregateUpdate &executable);
SljitNativePrimitiveRunFunction
SljitEnsureExecutablePrimitiveRunUpdate(ExecutionRegionRuntime &runtime, SljitExecutablePrimitiveRunUpdate &run_update,
                                        PhysicalType group_source_type, PhysicalType group_output_type,
                                        ExecutionRowPointerGroupKeyCastKind group_cast_kind, bool payload_nullable,
                                        bool shared_payload_validity);
SljitNativePrimitiveRunFunction SljitEnsureExecutableFusedAffineRunUpdate(
    ExecutionRegionRuntime &runtime, SljitExecutablePrimitiveRunUpdate &primitive_run_update,
    const SljitExecutableFusedAffineRunUpdate &affine_run_update, PhysicalType group_source_type,
    PhysicalType group_output_type, ExecutionRowPointerGroupKeyCastKind group_cast_kind, bool payload_nullable);
void SljitSelectExecutableAggregateDirectUpdatePlan(SljitExecutableAggregateUpdate &executable);
bool SljitBuildExecutableAggregateUpdateFallbackPayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                            SljitExecutableAggregateUpdate &executable, string &error);

} // namespace duckdb

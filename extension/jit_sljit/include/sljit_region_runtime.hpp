//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_plan.hpp"

namespace duckdb {

class ClientContext;

unique_ptr<JitRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
	                                                          SljitExecutableRegion &&region,
	                                                          JitRegionABI abi = JitRegionABI::NONE,
	                                                          bool native_source = false);

unique_ptr<JitRegionKernel> CreateSljitFusedFilterProjectionKernel(string backend_name,
                                                                     SljitNativeRegionExpressionPlan filter,
                                                                     SljitNativeRegionExpressionPlan projection,
	                                                                     unique_ptr<JitCodeHandle> code,
	                                                                     SljitFusedFilterProjectionFunction function,
	                                                                     string projection_overflow_message,
	                                                                     JitRegionABI abi = JitRegionABI::NONE,
	                                                                     bool native_source = false);

unique_ptr<JitRegionKernel> CreateSljitFusedFilterProjectionUngroupedSumKernel(
    string backend_name, SljitNativeRegionExpressionPlan filter, SljitNativeRegionExpressionPlan projection,
    SljitNativeUngroupedAggregateUpdatePlan update, unique_ptr<JitCodeHandle> code,
    SljitFusedUngroupedAggregateFunction function, string projection_overflow_message, bool native_source = false);

unique_ptr<JitRegionKernel> CreateSljitFusedProjectionUngroupedSumKernel(
    string backend_name, SljitNativeRegionExpressionPlan projection, SljitNativeUngroupedAggregateUpdatePlan update,
    unique_ptr<JitCodeHandle> code, SljitFusedUngroupedAggregateFunction function,
    string projection_overflow_message, bool native_source = false);

unique_ptr<JitRegionKernel> CreateSljitFusedDirectPerfectHashAggregateKernel(
    string backend_name, SljitNativeRegionPlan region, unique_ptr<JitCodeHandle> code,
    SljitFusedPerfectHashAggregateFunction function, string overflow_message, bool native_source = false);

} // namespace duckdb

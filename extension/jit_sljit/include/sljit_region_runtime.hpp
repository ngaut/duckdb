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
struct SljitFullPipelineRecipePlan;

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                                SljitExecutableRegion &&region,
                                                                SljitFullPipelineRecipePlan recipe_plan,
                                                                ExecutionRegionABI abi);

} // namespace duckdb

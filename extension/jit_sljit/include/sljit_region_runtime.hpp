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

struct SljitFullPipelineRecipePlan;

shared_ptr<const ExecutionRegionArtifact> CreateSljitNativeRegionArtifact(string backend_name,
                                                                          SljitExecutableRegion &&region,
                                                                          SljitFullPipelineRecipePlan recipe_plan,
                                                                          ExecutionRegionABI abi);

unique_ptr<ExecutionRegionKernel>
InstantiateSljitNativeRegionArtifact(const shared_ptr<const ExecutionRegionArtifact> &artifact,
                                     const ExecutionRegionCompilationInput &input);

} // namespace duckdb

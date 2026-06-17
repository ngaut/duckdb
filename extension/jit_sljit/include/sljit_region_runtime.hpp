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

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                          SljitExecutableRegion &&region, ExecutionRegionABI abi);

} // namespace duckdb

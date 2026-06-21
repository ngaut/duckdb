//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_backend.hpp"

namespace duckdb {

ExecutionRegionLoweringPlan AnalyzeSljitRegion(const ExecutionRegionCompilationInput &input);
ExecutionRegionCompileResult CompileSljitRegion(const string &backend_name,
                                                const ExecutionRegionCompilationInput &input);

} // namespace duckdb

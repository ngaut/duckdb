//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

JitRegionLoweringPlan AnalyzeSljitRegion(const JitRegionCompilationInput &input);
JitRegionCompileResult CompileSljitRegion(const string &backend_name, const JitRegionCompilationInput &input);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_preaggregated_primitive_update_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"

#include "duckdb/common/common.hpp"

namespace duckdb {

class ExecutionRegionRuntime;
struct ExecutionPrimitiveAggregateUpdateLane;
struct SljitExecutablePrimitiveRunUpdate;

SljitNativePreaggregatedPrimitiveUpdateFunction
SljitEnsureExecutablePreaggregatedPrimitiveUpdate(ExecutionRegionRuntime &runtime,
                                                  SljitExecutablePrimitiveRunUpdate &primitive_run_update,
                                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                  bool initialize_states, bool address_selected, bool row_selected);

} // namespace duckdb

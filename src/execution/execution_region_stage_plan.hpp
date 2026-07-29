#pragma once

#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

ExecutionRegionStagePlan BuildExecutionRegionStagePlan(const ExecutionRegionIR &region_ir,
                                                       const string &candidate_shape, ExecutionRegionIRMode mode);

} // namespace duckdb

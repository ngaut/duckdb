//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_cost_input.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/cost_model.hpp"

namespace duckdb {

class Pipeline;
struct ExecutionRegionCandidate;

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate);
bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input);
bool ExecutionRegionPipelineHasNonProducingSource(Pipeline &pipeline);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_cost_input.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/cost_model.hpp"

namespace duckdb {

class PhysicalOperator;
class Pipeline;
struct ExecutionRegionCandidate;

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate);
bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input);
bool ExecutionRegionPipelineHasNonProducingSource(Pipeline &pipeline);
PhysicalRunnerCostInput BuildExecutionRegionNonProducingSourceCostInput(const PhysicalOperator &source);

} // namespace duckdb

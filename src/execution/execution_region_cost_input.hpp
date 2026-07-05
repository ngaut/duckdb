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
struct ExecutionRegionLoweringPlan;

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate);
PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate,
                                                               const ExecutionRegionLoweringPlan &lowering_plan);
PhysicalRunnerCostInput BuildExecutionRegionPipelineCandidateUpperBoundCostInput(
    const PhysicalRunnerCostInput &pipeline_input);
bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input);
bool ExecutionRegionPipelineHasNonProducingSource(Pipeline &pipeline);

} // namespace duckdb

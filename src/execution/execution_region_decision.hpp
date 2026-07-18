//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_decision.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/execution_region_common.hpp"
#include "duckdb/planner/cost_model.hpp"

namespace duckdb {

class ClientContext;
class Pipeline;
struct ExecutionRegionCandidate;
struct ExecutionRegionGraph;
struct ExecutionRegionIR;
struct ExecutionRegionLoweringPlan;

struct ExecutionRegionPhysicalRunnerSelection {
	PhysicalRunnerCostProfile runner_cost;
	bool use_compiled_runner = false;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	string reason;
	string blocker;

	bool UsesCompiledRunner() const {
		return use_compiled_runner;
	}

	ExecutionRunnerKind SelectedRunner() const {
		return selected_runner;
	}
};

string ComposeExecutionRegionCompileEventReason(const ExecutionRegionPhysicalRunnerSelection &selection,
                                                const string &compile_reason);
string ExecutionRegionCboCostReasonToken(const PhysicalRunnerCostProfile &cost);
string AttachExecutionRegionCandidateReason(const ExecutionRegionCandidate &candidate, string reason,
                                            bool record_detailed_telemetry);
string FirstExecutionRegionReasonToken(const string &reason);
string ExecutionRegionCandidateBlockerCode(const ExecutionRegionIR &region_ir);
string ExecutionRegionUnsupportedBlockerCode(const ExecutionRegionLoweringPlan &lowering_plan);
string ExecutionRegionCompileResultBlockerCode(ExecutionRegionCompileStatus status);
string ExecutionRegionLoweringEventReason(const ExecutionRegionLoweringPlan &lowering_plan,
                                          bool record_detailed_telemetry);
string DescribeExecutionRegionLoweringRejection(const ExecutionRegionGraph &graph);
string ExecutionRegionDecisionRunnerName(ExecutionRunnerKind runner);

PhysicalRunnerCostParameters BuildPhysicalRunnerCostParameters(ClientContext &context);
bool ExecutionRegionAdaptiveMeasurementWithinBand(ClientContext &context, const PhysicalRunnerCostProfile &cost);
bool ExecutionRegionProductionEligibilityAllowsPlanning(ClientContext &context,
                                                        const PhysicalRunnerCostParameters &parameters);
bool ExecutionRegionPlanningNeedsBackendDiagnostics(ClientContext &context);
bool ExecutionRegionPlanningNeedsCandidateDiagnostics(ClientContext &context);
bool ExecutionRegionGraphMayHaveCostedAcceleration(const ExecutionRegionGraph &graph,
                                                   const PhysicalRunnerCostParameters &parameters);

ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPipelinePhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters, Pipeline &pipeline);
ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionCostOnlyPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                            const ExecutionRegionCandidate &candidate);
ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                    const ExecutionRegionCandidate &candidate,
                                    const ExecutionRegionLoweringPlan &lowering_plan, bool record_detailed_telemetry);

} // namespace duckdb

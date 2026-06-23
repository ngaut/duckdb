//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_decision.cpp
//
//===----------------------------------------------------------------------===//

#include "execution_region_decision.hpp"

#include "duckdb/execution/execution_region_graph.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "execution_region_cost_input.hpp"

namespace duckdb {

string ComposeExecutionRegionCompileEventReason(const ExecutionRegionPhysicalRunnerSelection &selection,
                                                const string &compile_reason) {
	if (selection.reason.empty()) {
		return compile_reason;
	}
	if (compile_reason.empty()) {
		return selection.reason;
	}
	return selection.reason + ";" + compile_reason;
}

string AttachExecutionRegionCandidateReason(const ExecutionRegionCandidate &candidate, string reason,
                                            bool record_detailed_telemetry) {
	if (!record_detailed_telemetry) {
		return reason;
	}
	if (!reason.empty()) {
		reason += ";";
	}
	reason += "candidate_id=" + std::to_string(candidate.candidate_id);
	reason += ";candidate_shape=" + candidate.shape;
	return reason;
}

string FirstExecutionRegionReasonToken(const string &reason) {
	auto separator = reason.find(';');
	return separator == string::npos ? reason : reason.substr(0, separator);
}

string ExecutionRegionCandidateBlockerCode(const ExecutionRegionIR &region_ir) {
	if (region_ir.candidate_blockers.empty()) {
		return EXECUTION_REGION_BLOCKER_NO_EXECUTION_REGION_CANDIDATES;
	}
	if (region_ir.candidate_blockers[0].find("candidate-builder-blocked:no-executable-work") != string::npos) {
		return EXECUTION_REGION_BLOCKER_NO_EXECUTABLE_REGION_WORK;
	}
	return EXECUTION_REGION_BLOCKER_NO_EXECUTION_REGION_CANDIDATES;
}

string ExecutionRegionUnsupportedBlockerCode(const ExecutionRegionLoweringPlan &lowering_plan) {
	return lowering_plan.NativeCount() > 0 ? EXECUTION_REGION_BLOCKER_UNSUPPORTED_REGION_EXECUTION
	                                       : EXECUTION_REGION_BLOCKER_REGION_CONTAINS_NO_NATIVE_NODES;
}

string ExecutionRegionCompileResultBlockerCode(ExecutionRegionCompileStatus status) {
	if (status == ExecutionRegionCompileStatus::COMPILED) {
		return string();
	}
	string result = "backend_compile_";
	result += ExecutionRegionCompileStatusToString(status);
	return result;
}

string ExecutionRegionLoweringEventReason(const ExecutionRegionLoweringPlan &lowering_plan,
                                          bool record_detailed_telemetry) {
	return record_detailed_telemetry ? lowering_plan.EventReason() : lowering_plan.CompactEventReason();
}

string DescribeExecutionRegionLoweringRejection(const ExecutionRegionGraph &graph) {
	string reason = "core region lowering did not produce typed region IR";
	reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
	return reason;
}

PhysicalRunnerCostParameters BuildPhysicalRunnerCostParameters(ClientContext &context) {
	PhysicalRunnerCostParameters result;
	result.generated_stage_benefit = Settings::Get<JitCboGeneratedStageBenefitSetting>(context);
	result.native_operator_stage_benefit = Settings::Get<JitCboNativeOperatorStageBenefitSetting>(context);
	result.materialization_elision_benefit = Settings::Get<JitCboMaterializationElisionBenefitSetting>(context);
	result.full_pipeline_benefit = Settings::Get<JitCboFullPipelineBenefitSetting>(context);
	result.startup_base_cost = Settings::Get<JitCboStartupBaseCostSetting>(context);
	result.startup_margin_basis_points = Settings::Get<JitCboStartupMarginBasisPointsSetting>(context);
	return result;
}

static bool PhysicalRunnerCostingHasEnabledBenefit(const PhysicalRunnerCostParameters &parameters) {
	return parameters.generated_stage_benefit > 0 || parameters.native_operator_stage_benefit > 0 ||
	       parameters.materialization_elision_benefit > 0 || parameters.full_pipeline_benefit > 0;
}

bool ExecutionRegionProductionEligibilityAllowsPlanning(ClientContext &context,
                                                        const PhysicalRunnerCostParameters &parameters) {
	if (ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context)) {
		return true;
	}
	return PhysicalRunnerCostingHasEnabledBenefit(parameters);
}

bool ExecutionRegionPlanningNeedsBackendDiagnostics(ClientContext &context) {
	return ExecutionRegionSettings::DumpIR(context);
}

bool ExecutionRegionPlanningNeedsCandidateDiagnostics(ClientContext &context) {
	return ExecutionRegionSettings::TraceDecisions(context) || ExecutionRegionSettings::DumpIR(context);
}

bool ExecutionRegionGraphMayHaveCostedAcceleration(const ExecutionRegionGraph &graph,
                                                   const PhysicalRunnerCostParameters &parameters) {
	if (parameters.generated_stage_benefit > 0 && graph.HasGeneratedExpression()) {
		return true;
	}
	if (parameters.native_operator_stage_benefit > 0 && graph.HasNativeOperatorWork()) {
		return true;
	}
	if (parameters.materialization_elision_benefit > 0 && graph.HasGeneratedExpression() && graph.HasSink()) {
		return true;
	}
	if (parameters.full_pipeline_benefit > 0 && graph.HasSource() && graph.HasSink()) {
		return true;
	}
	return false;
}

ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPipelinePhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters, Pipeline &pipeline) {
	ExecutionRegionPhysicalRunnerSelection selection;
	if (ExecutionRegionPipelineHasNonProducingSource(pipeline)) {
		auto source = pipeline.GetSource();
		D_ASSERT(source);
		auto cost_input = BuildExecutionRegionNonProducingSourceCostInput(*source);
		selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
		selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
		selection.reason += ";region_graph=skipped;source_produces_rows=false";
		selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
		return selection;
	}
	PhysicalRunnerCostInput cost_input;
	if (!TryBuildExecutionRegionPipelineCostInput(pipeline, cost_input)) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo requires execution-region graph for physical runner decision";
		return selection;
	}
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo physical pipeline cost admits region graph analysis";
		return selection;
	}
	selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
	selection.reason += ";region_graph=skipped";
	selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
	return selection;
}

ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionCostOnlyPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                            const ExecutionRegionCandidate &candidate) {
	ExecutionRegionPhysicalRunnerSelection selection;
	auto cost_input = BuildExecutionRegionCandidateCostInput(candidate);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo cost model admits backend capability analysis";
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner before backend analysis";
	selection.reason += ";backend_analysis=skipped";
	selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
	return selection;
}

ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                    const ExecutionRegionCandidate &candidate,
                                    const ExecutionRegionLoweringPlan &lowering_plan, bool record_detailed_telemetry) {
	ExecutionRegionPhysicalRunnerSelection selection;
	if (!lowering_plan.IsFullyFused()) {
		selection.reason = "duckdb_cbo skips compiled-vectorized runner because region is not fully fused";
		selection.reason += ";requires=fused";
		selection.reason += ";" + ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
		selection.blocker = EXECUTION_REGION_BLOCKER_NOT_FULLY_FUSED;
		return selection;
	}
	auto cost_input = BuildExecutionRegionCandidateCostInput(candidate);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo selects compiled-vectorized physical runner";
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner;";
	selection.reason += ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
	selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
	return selection;
}

} // namespace duckdb

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
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

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
	auto &manager = ExecutionRegionManager::Get(context);
	result.compiled_vectorized_runner_available =
	    manager.HasAvailableBackendForRunner(context, ExecutionRunnerKind::COMPILED_VECTORIZED);
	result.generated_stage_benefit = Settings::Get<JitCboGeneratedStageBenefitSetting>(context);
	result.native_operator_stage_benefit = Settings::Get<JitCboNativeOperatorStageBenefitSetting>(context);
	result.materialization_elision_benefit = Settings::Get<JitCboMaterializationElisionBenefitSetting>(context);
	result.full_pipeline_benefit = Settings::Get<JitCboFullPipelineBenefitSetting>(context);
	result.startup_base_cost = Settings::Get<JitCboStartupBaseCostSetting>(context);
	result.startup_margin_basis_points = Settings::Get<JitCboStartupMarginBasisPointsSetting>(context);
	result.vectorized_parallelism =
	    MaxValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), 1);
	result.gpu_runner_available = manager.HasAvailableBackendForRunner(context, ExecutionRunnerKind::COMPILED_GPU);
	result.gpu_generated_stage_benefit = Settings::Get<GpuCboGeneratedStageBenefitSetting>(context);
	result.gpu_native_operator_stage_benefit = Settings::Get<GpuCboNativeOperatorStageBenefitSetting>(context);
	result.gpu_materialization_elision_benefit = Settings::Get<GpuCboMaterializationElisionBenefitSetting>(context);
	result.gpu_full_pipeline_benefit = Settings::Get<GpuCboFullPipelineBenefitSetting>(context);
	result.gpu_startup_base_cost = Settings::Get<GpuCboStartupBaseCostSetting>(context);
	result.gpu_startup_margin_basis_points = Settings::Get<GpuCboStartupMarginBasisPointsSetting>(context);
	result.gpu_transfer_cost_per_batch = Settings::Get<GpuCboTransferCostPerBatchSetting>(context);
	return result;
}

static bool PhysicalRunnerCostingHasEnabledBenefit(const PhysicalRunnerCostParameters &parameters) {
	const bool compiled_vectorized_costing =
	    parameters.compiled_vectorized_runner_available &&
	    (parameters.generated_stage_benefit > 0 || parameters.native_operator_stage_benefit > 0 ||
	     parameters.materialization_elision_benefit > 0 || parameters.full_pipeline_benefit > 0);
	const bool gpu_costing =
	    parameters.gpu_runner_available &&
	    (parameters.gpu_generated_stage_benefit > 0 || parameters.gpu_native_operator_stage_benefit > 0 ||
	     parameters.gpu_materialization_elision_benefit > 0 || parameters.gpu_full_pipeline_benefit > 0);
	return compiled_vectorized_costing || gpu_costing;
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
	return ExecutionRegionSettings::DumpIR(context);
}

bool ExecutionRegionGraphMayHaveCostedAcceleration(const ExecutionRegionGraph &graph,
                                                   const PhysicalRunnerCostParameters &parameters) {
	const auto generated_stage_benefit =
	    MaxValue(parameters.compiled_vectorized_runner_available ? parameters.generated_stage_benefit : 0,
	             parameters.gpu_runner_available ? parameters.gpu_generated_stage_benefit : 0);
	const auto native_operator_stage_benefit =
	    MaxValue(parameters.compiled_vectorized_runner_available ? parameters.native_operator_stage_benefit : 0,
	             parameters.gpu_runner_available ? parameters.gpu_native_operator_stage_benefit : 0);
	const auto materialization_elision_benefit =
	    MaxValue(parameters.compiled_vectorized_runner_available ? parameters.materialization_elision_benefit : 0,
	             parameters.gpu_runner_available ? parameters.gpu_materialization_elision_benefit : 0);
	const auto full_pipeline_benefit =
	    MaxValue(parameters.compiled_vectorized_runner_available ? parameters.full_pipeline_benefit : 0,
	             parameters.gpu_runner_available ? parameters.gpu_full_pipeline_benefit : 0);
	const bool has_generated_expression = graph.HasGeneratedExpression();
	const bool has_native_operator_work = graph.HasNativeOperatorWork();
	if (generated_stage_benefit > 0 && has_generated_expression) {
		return true;
	}
	if (native_operator_stage_benefit > 0 && has_native_operator_work) {
		return true;
	}
	if (materialization_elision_benefit > 0 && has_generated_expression && graph.HasSink()) {
		return true;
	}
	if (full_pipeline_benefit > 0 && graph.HasSource() && graph.HasSink()) {
		return true;
	}
	return false;
}

static void SelectExecutionRegionAcceleratedRunner(ExecutionRegionPhysicalRunnerSelection &selection) {
	selection.selected_runner = selection.runner_cost.selected_runner;
	selection.use_compiled_runner = selection.selected_runner != ExecutionRunnerKind::VECTORIZED;
}

string ExecutionRegionCboCostReasonToken(const PhysicalRunnerCostProfile &cost) {
	if (!cost.selection_reason.empty()) {
		return "cbo_selection_reason=" + cost.selection_reason;
	}
	return string();
}

static void AppendExecutionRegionCboCostReason(string &reason, const PhysicalRunnerCostProfile &cost) {
	auto token = ExecutionRegionCboCostReasonToken(cost);
	if (token.empty()) {
		return;
	}
	if (!reason.empty() && reason.back() != ';') {
		reason += ";";
	}
	reason += token;
}

string ExecutionRegionDecisionRunnerName(ExecutionRunnerKind runner) {
	switch (runner) {
	case ExecutionRunnerKind::COMPILED_VECTORIZED:
		return "compiled-vectorized";
	case ExecutionRunnerKind::COMPILED_GPU:
		return "compiled-gpu";
	default:
		return ExecutionRunnerKindToString(runner);
	}
}

static bool PhysicalPipelineDecisionNeedsRegionGraph(const PhysicalRunnerCostInput &cost_input) {
	if (!cost_input.full_pipeline) {
		return false;
	}
	return cost_input.native_join_stage_count > 0 || cost_input.native_aggregate_stage_count > 0 ||
	       cost_input.native_grouped_aggregate_stage_count > 0 || cost_input.native_sort_stage_count > 0 ||
	       cost_input.materialization_source_append_count > 0;
}

ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPipelinePhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters, Pipeline &pipeline) {
	ExecutionRegionPhysicalRunnerSelection selection;
	if (ExecutionRegionPipelineHasNonProducingSource(pipeline)) {
		auto source = pipeline.GetSource();
		D_ASSERT(source);
		PhysicalRunnerCostInput cost_input;
		cost_input.estimated_cardinality = source->estimated_cardinality;
		selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
		selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
		selection.reason += ";region_graph=skipped;source_produces_rows=false";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
		return selection;
	}
	PhysicalRunnerCostInput cost_input;
	if (!TryBuildExecutionRegionPipelineCostInput(pipeline, cost_input)) {
		selection.use_compiled_runner = true;
		selection.selected_runner = ExecutionRunnerKind::COMPILED_VECTORIZED;
		selection.reason = "duckdb_cbo requires execution-region graph for physical runner decision";
		return selection;
	}
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo physical pipeline cost admits region graph analysis";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}
	if (PhysicalPipelineDecisionNeedsRegionGraph(cost_input)) {
		selection.use_compiled_runner = true;
		selection.selected_runner = ExecutionRunnerKind::COMPILED_VECTORIZED;
		selection.reason = "duckdb_cbo requires execution-region graph for physical runner decision";
		selection.reason += ";region_graph=required";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}
	selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
	selection.reason += ";region_graph=skipped";
	AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
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
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo cost model admits backend capability analysis";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner before backend analysis";
	selection.reason += ";backend_analysis=skipped";
	AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
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
	cost_input.uses_scan_filters = lowering_plan.UsesScanFilters();
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo selects ";
		selection.reason += ExecutionRegionDecisionRunnerName(selection.SelectedRunner());
		selection.reason += " physical runner";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner";
	AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
	selection.reason += ";" + ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
	selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
	return selection;
}

} // namespace duckdb

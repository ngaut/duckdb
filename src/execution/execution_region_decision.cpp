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
	result.source_contract_scan_filter_penalty = Settings::Get<JitCboSourceContractScanFilterPenaltySetting>(context);
	result.vectorized_parallelism =
	    MaxValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), 1);
	auto &compiled = result.compiled_vectorized;
	compiled.available = manager.HasAvailableBackendForRunner(context, ExecutionRunnerKind::COMPILED_VECTORIZED);
	compiled.generated_stage_benefit = Settings::Get<JitCboGeneratedStageBenefitSetting>(context);
	compiled.native_operator_stage_benefit = Settings::Get<JitCboNativeOperatorStageBenefitSetting>(context);
	compiled.materialization_elision_benefit = Settings::Get<JitCboMaterializationElisionBenefitSetting>(context);
	compiled.full_pipeline_benefit = Settings::Get<JitCboFullPipelineBenefitSetting>(context);
	compiled.startup_base_cost = Settings::Get<JitCboStartupBaseCostSetting>(context);
	compiled.startup_margin_basis_points = Settings::Get<JitCboStartupMarginBasisPointsSetting>(context);
	auto &gpu = result.gpu;
	gpu.available = manager.HasAvailableBackendForRunner(context, ExecutionRunnerKind::COMPILED_GPU);
	gpu.generated_stage_benefit = Settings::Get<GpuCboGeneratedStageBenefitSetting>(context);
	gpu.native_operator_stage_benefit = Settings::Get<GpuCboNativeOperatorStageBenefitSetting>(context);
	gpu.materialization_elision_benefit = Settings::Get<GpuCboMaterializationElisionBenefitSetting>(context);
	gpu.full_pipeline_benefit = Settings::Get<GpuCboFullPipelineBenefitSetting>(context);
	gpu.startup_base_cost = Settings::Get<GpuCboStartupBaseCostSetting>(context);
	gpu.startup_margin_basis_points = Settings::Get<GpuCboStartupMarginBasisPointsSetting>(context);
	gpu.transfer_cost_per_batch = Settings::Get<GpuCboTransferCostPerBatchSetting>(context);
	return result;
}

static bool PhysicalRunnerCostingHasEnabledBenefit(const PhysicalRunnerCostParameters &parameters) {
	for (idx_t axis_idx = 0; axis_idx < PhysicalRunnerCostParameters::AXIS_COUNT; axis_idx++) {
		if (parameters.AxisAt(axis_idx).HasEnabledBenefit()) {
			return true;
		}
	}
	return false;
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
	const bool has_generated_expression = graph.HasGeneratedExpression();
	const bool has_native_operator_work = graph.HasNativeOperatorWork();
	for (idx_t axis_idx = 0; axis_idx < PhysicalRunnerCostParameters::AXIS_COUNT; axis_idx++) {
		auto &axis = parameters.AxisAt(axis_idx);
		if (!axis.available) {
			continue;
		}
		if (axis.generated_stage_benefit > 0 && has_generated_expression) {
			return true;
		}
		if (axis.native_operator_stage_benefit > 0 && has_native_operator_work) {
			return true;
		}
		if (axis.materialization_elision_benefit > 0 && has_generated_expression && graph.HasSink()) {
			return true;
		}
		if (axis.full_pipeline_benefit > 0 && graph.HasSource() && graph.HasSink()) {
			return true;
		}
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
	       cost_input.native_grouped_aggregate_stage_count > 0 || cost_input.native_sort_stage_count > 0;
}

static bool HashJoinBuildNeedsBackendCapabilityAnalysis(const PhysicalRunnerCostInput &cost_input) {
	return cost_input.full_pipeline && cost_input.native_hash_join_build_sink_count > 0 &&
	       cost_input.generated_work_class != PhysicalRunnerGeneratedWorkClass::NONE &&
	       cost_input.generated_work_class != PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
}

static bool PhysicalPipelineNeedsRegionGraph(const PhysicalRunnerCostInput &cost_input) {
	return HashJoinBuildNeedsBackendCapabilityAnalysis(cost_input) ||
	       PhysicalPipelineDecisionNeedsRegionGraph(cost_input);
}

static string PhysicalPipelineRegionGraphDecisionReason(const PhysicalRunnerCostInput &cost_input) {
	if (HashJoinBuildNeedsBackendCapabilityAnalysis(cost_input)) {
		return "duckdb_cbo requires execution-region graph for hash-join build sink decision";
	}
	return "duckdb_cbo requires execution-region graph for physical runner decision";
}

static PhysicalRunnerCostProfile
SelectExecutionRegionPipelineCandidateUpperBoundRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                                       const PhysicalRunnerCostInput &pipeline_cost_input) {
	auto upper_bound_input = BuildExecutionRegionPipelineCandidateUpperBoundCostInput(pipeline_cost_input);
	return DuckDBCostModel::SelectPhysicalRunner(upper_bound_input, cost_parameters);
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
		selection.reason += ";source_produces_rows=false";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
		return selection;
	}
	PhysicalRunnerCostInput cost_input;
	if (!TryBuildExecutionRegionPipelineCostInput(pipeline, cost_input)) {
		selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
		selection.reason += ";pipeline_cost_input=unsupported";
		selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
		return selection;
	}
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.SelectedAcceleratedRunner()) {
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo physical pipeline cost admits region graph analysis";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}
	if (PhysicalPipelineNeedsRegionGraph(cost_input)) {
		auto upper_bound_cost = SelectExecutionRegionPipelineCandidateUpperBoundRunner(cost_parameters, cost_input);
		if (!upper_bound_cost.SelectedAcceleratedRunner()) {
			selection.runner_cost = std::move(upper_bound_cost);
			selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
			selection.reason += ";candidate_upper_bound=rejected";
			AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
			selection.blocker = EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED;
			return selection;
		}
		selection.use_compiled_runner = true;
		selection.selected_runner = upper_bound_cost.selected_runner;
		selection.reason = PhysicalPipelineRegionGraphDecisionReason(cost_input);
		selection.reason += ";region_graph=required";
		AppendExecutionRegionCboCostReason(selection.reason, upper_bound_cost);
		return selection;
	}
	selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
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
	if (selection.runner_cost.SelectedAcceleratedRunner()) {
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo cost model admits backend capability analysis";
		AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
		return selection;
	}
	if (HashJoinBuildNeedsBackendCapabilityAnalysis(cost_input)) {
		auto upper_bound_cost = SelectExecutionRegionPipelineCandidateUpperBoundRunner(cost_parameters, cost_input);
		if (upper_bound_cost.SelectedAcceleratedRunner()) {
			selection.runner_cost = std::move(upper_bound_cost);
			SelectExecutionRegionAcceleratedRunner(selection);
			selection.reason = "duckdb_cbo hash-build upper bound admits backend capability analysis";
			AppendExecutionRegionCboCostReason(selection.reason, selection.runner_cost);
			return selection;
		}
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
		selection.reason = "duckdb_cbo skips accelerated runner because region is not fully fused";
		selection.reason += ";requires=fused";
		selection.reason += ";" + ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
		selection.blocker = EXECUTION_REGION_BLOCKER_NOT_FULLY_FUSED;
		return selection;
	}
	auto cost_input = BuildExecutionRegionCandidateCostInput(candidate, lowering_plan);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.SelectedAcceleratedRunner()) {
		SelectExecutionRegionAcceleratedRunner(selection);
		selection.reason = "duckdb_cbo selects accelerated physical runner";
		selection.reason += ";runner=";
		selection.reason += ExecutionRegionDecisionRunnerName(selection.SelectedRunner());
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

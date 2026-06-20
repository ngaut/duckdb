#include "duckdb/execution/execution_region_planner.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/cost_model.hpp"

#include <chrono>

namespace duckdb {

struct ExecutionRegionPhysicalRunnerSelection {
	PhysicalRunnerCostProfile runner_cost;
	bool use_compiled_runner = false;
	string reason;
	string blocker;

	bool UsesCompiledRunner() const {
		return use_compiled_runner;
	}

	ExecutionRunnerKind SelectedRunner() const {
		return UsesCompiledRunner() ? ExecutionRunnerKind::COMPILED_VECTORIZED : ExecutionRunnerKind::VECTORIZED;
	}
};

static string ComposeExecutionRegionCompileEventReason(const ExecutionRegionPhysicalRunnerSelection &selection,
                                                       ExecutionRegionPolicyMode policy,
                                                       const string &compile_reason) {
	if (policy != ExecutionRegionPolicyMode::AUTO || selection.reason.empty()) {
		return compile_reason;
	}
	if (compile_reason.empty()) {
		return selection.reason;
	}
	return selection.reason + ";" + compile_reason;
}

static string AttachExecutionRegionCandidateReason(const ExecutionRegionCandidate &candidate, string reason,
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

static string FirstExecutionRegionReasonToken(const string &reason) {
	auto separator = reason.find(';');
	return separator == string::npos ? reason : reason.substr(0, separator);
}

static string ExecutionRegionCandidateBlockerCode(const ExecutionRegionIR &region_ir) {
	if (region_ir.candidate_blockers.empty()) {
		return "no_execution_region_candidates";
	}
	if (region_ir.candidate_blockers[0].find("candidate-builder-blocked:no-executable-work") != string::npos) {
		return "no_executable_region_work";
	}
	return "no_execution_region_candidates";
}

static string ExecutionRegionUnsupportedBlockerCode(const ExecutionRegionLoweringPlan &lowering_plan) {
	return lowering_plan.NativeCount() > 0 ? "unsupported_region_execution" : "region_contains_no_native_nodes";
}

static string ExecutionRegionCompileResultBlockerCode(ExecutionRegionCompileStatus status) {
	if (status == ExecutionRegionCompileStatus::COMPILED) {
		return string();
	}
	string result = "backend_compile_";
	result += ExecutionRegionCompileStatusToString(status);
	return result;
}

static string ExecutionRegionLoweringEventReason(const ExecutionRegionLoweringPlan &lowering_plan,
                                                 bool record_detailed_telemetry) {
	return record_detailed_telemetry ? lowering_plan.EventReason() : lowering_plan.CompactEventReason();
}

static int64_t ExecutionRegionPlannerElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static PhysicalRunnerCostInput BuildPhysicalRunnerCostInput(const ExecutionRegionCandidate &candidate,
                                                            const ExecutionRegionLoweringPlan &lowering_plan) {
	PhysicalRunnerCostInput result;
	result.estimated_cardinality = candidate.estimated_cardinality;
	result.expression_cost = lowering_plan.generated_expression_cost;
	result.accelerated_stage_count = lowering_plan.generated_stage_count;
	result.full_pipeline = ExecutionRegionABIIsFullPipeline(candidate.contract.abi);
	result.node_count = candidate.node_count;
	result.stage_count = candidate.stage_plan.stages.size();
	result.expression_node_count = candidate.traits.expression_node_count;
	result.operator_count = candidate.traits.operator_count;
	result.has_accelerated_work = candidate.stage_plan.HasExecutableWork();
	return result;
}

static bool ExecutionRegionHasAcceleratedRunnerWork(const ExecutionRegionCandidate &candidate,
                                                    const PhysicalRunnerCostProfile &runner_cost) {
	return candidate.stage_plan.HasExecutableWork() && runner_cost.accelerated_stage_count > 0 &&
	       runner_cost.saved_work_per_batch > 0;
}

static string DescribeExecutionRegionLoweringRejection(const ExecutionRegionGraph &graph,
                                                       const ExecutionRegionPipelineInventory *inventory) {
	string reason = "core region lowering did not produce typed region IR";
	if (!inventory) {
		reason += ";graph_blocker=no-pipeline-inventory";
		reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
		return reason;
	}
	reason += ";graph_blocker=typed-region-builder-returned-null";
	reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
	reason += ";pipeline_shape=" + inventory->pipeline_shape;
	reason += ";candidate_shape=" + inventory->candidate_shape;
	reason += ";feature_shape=" + inventory->feature_shape;
	reason += ";estimated_cardinality=" + std::to_string(inventory->estimated_cardinality);
	return reason;
}

static ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPhysicalRunner(ExecutionRegionPolicyMode policy, const ExecutionRegionCandidate &candidate,
                                    const ExecutionRegionLoweringPlan &lowering_plan,
                                    bool record_detailed_telemetry) {
	ExecutionRegionPhysicalRunnerSelection selection;
	auto region_execution_form = lowering_plan.ExpectedRegionExecutionForm();
	if (region_execution_form != ExecutionRegionForm::FUSED) {
		selection.reason = "execution_region_policy=" + string(ExecutionRegionPolicyModeToString(policy)) +
		                   " skips region kernel because region execution form is not fused";
		selection.reason += ";region_execution_form=" + string(ExecutionRegionFormToString(region_execution_form));
		selection.reason += ";requires=fused;shape=" + lowering_plan.shape_key;
		selection.reason += ";" + ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
		selection.blocker = "region_execution_form_not_fused";
		return selection;
	}
	auto cost_input = BuildPhysicalRunnerCostInput(candidate, lowering_plan);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo selects compiled-vectorized physical runner";
		return selection;
	}
	if (policy == ExecutionRegionPolicyMode::FORCE &&
	    ExecutionRegionHasAcceleratedRunnerWork(candidate, selection.runner_cost)) {
		selection.use_compiled_runner = true;
		selection.reason = "execution_region_policy=force selects compiled-vectorized physical runner";
		selection.reason += ";duckdb_cbo_selected=vectorized";
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner;";
	selection.reason += ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
	selection.blocker = "duckdb_selected_vectorized";
	return selection;
}

struct ExecutionRegionPlanner::SelectedCandidate {
	idx_t candidate_index = 0;
	ExecutionRegionLoweringPlan lowering_plan;
	ExecutionRegionPhysicalRunnerSelection physical_runner;
	ExecutionRegionStageTimings stage_timings;
	int64_t decision_time_us = 0;
};

static bool ExecutionRegionStageExecutionIsFused(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::GENERATED_IR:
	case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionRegionStageExecutionIsFusionBlocker(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
	case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
		return true;
	default:
		return false;
	}
}

static string DescribeExecutionRegionStageFusionBlocker(const ExecutionRegionStage &stage) {
	string result = "stage=";
	result += ExecutionRegionStageKindToString(stage.kind);
	result += ";stage_execution=";
	result += ExecutionRegionStageExecutionKindToString(stage.execution);
	result += ";stage_ownership=";
	result += ExecutionRegionOwnershipKindToString(stage.ownership);
	if (stage.node_index != DConstants::INVALID_INDEX) {
		result += ";stage_node=" + std::to_string(stage.node_index);
	}
	if (stage.operator_index != DConstants::INVALID_INDEX) {
		result += ";stage_operator=" + std::to_string(stage.operator_index);
	}
	if (!stage.operator_name.empty()) {
		result += ";stage_operator_name=" + stage.operator_name;
	}
	if (!stage.reason.empty()) {
		result += ";stage_reason=" + stage.reason;
	}
	return result;
}

static bool ExecutionRegionContractHasOnlyNativeStages(const ExecutionRegionContract &contract) {
	return contract.source_boundary_count == 0 && contract.missing_contract_count == 0;
}

struct ExecutionRegionFusedStageContractDecision {
	bool valid = true;
	string reason;
	string blocker;
};

static ExecutionRegionFusedStageContractDecision
ValidateExecutionRegionFusedStageContract(const ExecutionRegionCandidate &candidate,
                                          const ExecutionRegionLoweringPlan &lowering_plan) {
	ExecutionRegionFusedStageContractDecision decision;
	if (lowering_plan.ExpectedRegionExecutionForm() != ExecutionRegionForm::FUSED) {
		return decision;
	}
	if (!ExecutionRegionContractHasOnlyNativeStages(candidate.contract)) {
		decision.valid = false;
		decision.blocker = "fused_region_contract_has_boundaries";
		decision.reason = "backend advertised fused region but core contract still has boundaries";
		decision.reason += ";source_boundaries=" + std::to_string(candidate.contract.source_boundary_count);
		decision.reason += ";missing_contracts=" + std::to_string(candidate.contract.missing_contract_count);
		if (!candidate.contract.ir.empty()) {
			decision.reason += ";" + candidate.contract.ir;
		}
		return decision;
	}
	if (!candidate.stage_plan.HasStages()) {
		decision.valid = false;
		decision.blocker = "fused_region_stage_plan_empty";
		decision.reason = "backend advertised fused region but core operator-stage plan is empty";
		return decision;
	}
	bool has_executable_stage = false;
	for (auto &stage : candidate.stage_plan.stages) {
		if (ExecutionRegionStageExecutionIsFusionBlocker(stage.execution)) {
			decision.valid = false;
			decision.blocker = stage.execution == ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
			                       ? "fused_region_source_boundary"
			                       : "fused_region_missing_contract";
			decision.reason = "backend advertised fused region across core boundary stage;";
			decision.reason += DescribeExecutionRegionStageFusionBlocker(stage);
			return decision;
		}
		has_executable_stage = has_executable_stage || ExecutionRegionStageExecutionIsFused(stage.execution);
	}
	if (!has_executable_stage) {
		decision.valid = false;
		decision.blocker = "fused_region_no_executable_stage";
		decision.reason =
		    "backend advertised fused region but core operator-stage plan has no generated or native stage";
		return decision;
	}
	return decision;
}

static bool ExecutionRegionStageNeedsOperatorReadiness(const ExecutionRegionStage &stage) {
	if (stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
		return false;
	}
	switch (stage.kind) {
	case ExecutionRegionStageKind::HASH_JOIN_PROBE:
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE:
	case ExecutionRegionStageKind::OPERATOR_BOUNDARY:
		return true;
	default:
		return false;
	}
}

static const char *ExecutionOperatorReadinessStatusToString(ExecutionOperatorReadinessStatus status) {
	switch (status) {
	case ExecutionOperatorReadinessStatus::READY:
		return "ready";
	case ExecutionOperatorReadinessStatus::NOT_READY:
		return "not_ready";
	case ExecutionOperatorReadinessStatus::INVALID:
		return "invalid";
	default:
		return "unknown";
	}
}

struct ExecutionRegionOperatorReadinessDecision {
	bool ready = true;
	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::SKIPPED;
	string reason;
	string blocker;
};

static ExecutionRegionOperatorReadinessDecision
ValidateExecutionRegionNativeOperatorReadiness(ClientContext &context, Pipeline &pipeline,
                                               const ExecutionRegionIR &region_ir,
                                               const ExecutionRegionCandidate &candidate) {
	ExecutionRegionOperatorReadinessDecision decision;
	auto &operators = pipeline.GetIntermediateOperators();
	for (auto &stage : candidate.stage_plan.stages) {
		if (!ExecutionRegionStageNeedsOperatorReadiness(stage)) {
			continue;
		}
		if (stage.node_index >= region_ir.nodes.size()) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage points outside region IR";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_invalid_stage";
			return decision;
		}
		auto &node = region_ir.nodes[stage.node_index];
		if (!node.operator_info) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage has no operator contract";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_missing_contract";
			return decision;
		}
		if (stage.operator_index >= operators.size()) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage points outside pipeline operators";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_invalid_operator";
			return decision;
		}
		auto &op = operators[stage.operator_index].get();
		auto readiness = op.GetExecutionOperatorReadiness(context, *node.operator_info);
		if (readiness.Ready()) {
			continue;
		}
		decision.ready = false;
		decision.status = readiness.status == ExecutionOperatorReadinessStatus::NOT_READY
		                      ? ExecutionRegionCompileStatus::SKIPPED
		                      : ExecutionRegionCompileStatus::UNSUPPORTED;
		decision.reason = "native operator state is not executable during execution-region inspection";
		decision.reason += ";readiness_status=";
		decision.reason += ExecutionOperatorReadinessStatusToString(readiness.status);
		decision.reason += ";readiness_kind=";
		decision.reason += ExecutionRegionOperatorContractKindToString(readiness.kind);
		decision.reason += ";readiness_blocker=";
		decision.reason += readiness.blocker.empty() ? "unknown" : readiness.blocker;
		decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = readiness.blocker.empty()
			                       ? (decision.status == ExecutionRegionCompileStatus::SKIPPED
			                              ? "native_operator_state_not_ready"
			                              : "native_operator_state_invalid")
			                       : readiness.blocker;
		return decision;
	}
	return decision;
}

static bool ExecutionRegionRuntimeCanEnter(Pipeline &pipeline, string &reason) {
	if (!pipeline.GetSource() || !pipeline.GetSink()) {
		reason = "full pipeline runtime requires both source and sink";
		return false;
	}
	return true;
}

static void AccumulateExecutionRegionOpenRequest(ExecutionRegionPlan &plan, const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate,
                                                 const ExecutionRegionLoweringPlan &lowering_plan) {
	if (!ExecutionRegionABIOwnsSource(candidate.contract.abi)) {
		return;
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (!node.source) {
			continue;
		}
		auto &source = *node.source;
		auto selected_source_execution =
		    lowering_plan.SelectedSourceExecution() != ExecutionRegionSourceExecutionKind::NONE
		        ? lowering_plan.SelectedSourceExecution()
		        : source.execution;
		auto native_fused_source_owner =
		    ExecutionRegionExecutionModeIsCompiled(lowering_plan.ExpectedCompiledExecutionMode()) &&
		    lowering_plan.ExpectedRegionExecutionForm() == ExecutionRegionForm::FUSED;
		if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			auto &contract = plan.source_open_request;
			contract.present = true;
			contract.source_execution =
			    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
			            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
			            native_fused_source_owner
			        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
			        : ExecutionRegionSourceExecutionKind::NONE;
			contract.uses_scan_filters = false;
			return;
		}
		if (!source.table_scan_contract.present) {
			continue;
		}
		auto &table_scan_contract = source.table_scan_contract;
		auto &plan_contract = plan.source_open_request;
		plan_contract.present = true;
		plan_contract.source_execution =
		    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
		            native_fused_source_owner
		        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		        : ExecutionRegionSourceExecutionKind::NONE;
		plan_contract.uses_scan_filters =
		    !source.filters.empty() && native_fused_source_owner && lowering_plan.UsesScanFilters();
		return;
	}
}

static unique_ptr<ExecutionRegionPlan> KeepExecutableExecutionRegionPlan(unique_ptr<ExecutionRegionPlan> plan) {
	if (!plan) {
		return nullptr;
	}
	if (plan->HasExecutableRegions() || plan->RequiresOperatorReadinessRefresh()) {
		return plan;
	}
	return nullptr;
}

unique_ptr<ExecutionRegionPlan> ExecutionRegionPlanner::Build(ClientContext &context, Pipeline &pipeline) {
	auto plan = make_uniq<ExecutionRegionPlan>();
	if (context.IsCompiledExecutionSuppressed() || pipeline.executor.IsCompiledExecutionSuppressed() ||
	    !ExecutionRegionSettings::Enabled(context)) {
		return nullptr;
	}
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	auto policy = ExecutionRegionSettings::Policy(context);
	auto requested_policy = policy;
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	if (policy == ExecutionRegionPolicyMode::OFF) {
		if (!should_record_detailed_telemetry) {
			return nullptr;
		}
		execution_region_manager.RecordEvent(context, "policy", ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::DISABLED, ExecutionRegionExecutionMode::NONE,
		                                     ExecutionRegionPolicyMode::OFF, "execution_region_policy=off",
		                                     "policy_off", nullptr, 0, 0, 0);
		return nullptr;
	}
	string backend_name;
	auto backend = execution_region_manager.SelectBackend(context, backend_name);
	plan->backend_name = backend_name;
	if (!backend) {
		execution_region_manager.RecordEvent(
		    context, std::move(backend_name), ExecutionRegionCompileTarget::REGION,
		    ExecutionRegionCompileStatus::UNAVAILABLE, ExecutionRegionExecutionMode::NONE, requested_policy,
		    "no available execution region backend", "backend_unavailable", nullptr, 0, 0, 0);
		return nullptr;
	}
	if (!backend->SupportsRegions()) {
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
		    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy, "backend does not compile regions",
		    "backend_does_not_compile_regions", nullptr, 0, 0, 0);
		return nullptr;
	}
	ExecutionExpressionAnalysisCache expression_analysis_cache;
	auto graph_build_start = std::chrono::steady_clock::now();
	auto pipeline_descriptor = BuildExecutionRegionGraph(pipeline);
	auto graph_build_time_us = ExecutionRegionPlannerElapsedMicros(graph_build_start);
	if (!pipeline_descriptor) {
		execution_region_manager.RecordEvent(context, backend_name, ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::UNSUPPORTED,
		                                     ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
		                                     "core region graph builder produced no execution-region graph",
		                                     "no_execution_region_graph", nullptr, graph_build_time_us, 0, 0);
		return nullptr;
	}
	auto inventory = TryInspectExecutionRegionPipeline(*pipeline_descriptor, &expression_analysis_cache);
	auto region_decision_start = std::chrono::steady_clock::now();
	auto region_ir_mode =
	    ExecutionRegionSettings::DumpIR(context) ? ExecutionRegionIRMode::TRACE : ExecutionRegionIRMode::COMPACT;
	auto region_ir =
	    TryLowerExecutionRegion(*pipeline_descriptor, region_ir_mode, inventory.get(), &expression_analysis_cache);
	auto region_lowering_time_us = ExecutionRegionPlannerElapsedMicros(region_decision_start);
	if (!region_ir) {
		auto local_rejected_inventory =
		    inventory ? nullptr : TryInspectExecutionRegionPipeline(*pipeline_descriptor, &expression_analysis_cache);
		auto rejected_inventory = inventory ? inventory.get() : local_rejected_inventory.get();
		string rejected_ir;
		const string *rejected_ir_ptr = nullptr;
		if (rejected_inventory && ExecutionRegionSettings::DumpIR(context)) {
			rejected_ir = RenderExecutionRegionPipelineInventoryIR(*rejected_inventory);
			rejected_ir_ptr = &rejected_ir;
		}
		auto rejected_reason = DescribeExecutionRegionLoweringRejection(*pipeline_descriptor, rejected_inventory);
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
		    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy, std::move(rejected_reason),
		    "no_typed_region_ir", rejected_ir_ptr, graph_build_time_us + region_lowering_time_us, 0, 0, nullptr,
		    ExecutionRunnerKind::VECTORIZED, nullptr, ExecutionRegionForm::NONE,
		    ExecutionRegionSourceExecutionKind::NONE, false,
		    rejected_inventory);
		return nullptr;
	}
	auto &lowered_region = *region_ir;
	if (lowered_region.candidates.empty()) {
		string reason = "core region lowering produced no candidates";
		if (!lowered_region.candidate_blockers.empty()) {
			reason = "core-region-candidate-blocked:" +
			         FirstExecutionRegionReasonToken(lowered_region.candidate_blockers[0]) + ";" + reason + ";" +
			         lowered_region.candidate_blockers[0];
		}
		execution_region_manager.RecordEvent(context, std::move(backend_name), ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::UNSUPPORTED,
		                                     ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy, reason,
		                                     ExecutionRegionCandidateBlockerCode(lowered_region), &lowered_region.ir,
		                                     graph_build_time_us + region_lowering_time_us, 0, 0);
		return nullptr;
	}

	vector<SelectedCandidate> selected_regions;
	for (idx_t candidate_index = 0; candidate_index < lowered_region.candidates.size(); candidate_index++) {
		auto &candidate = lowered_region.candidates[candidate_index];
		auto candidate_decision_start = std::chrono::steady_clock::now();
		ExecutionRegionStageTimings stage_timings;
		stage_timings.ir_lowering_time_us = region_lowering_time_us;
		auto candidate_decision_time_us = [&]() {
			return graph_build_time_us + region_lowering_time_us +
			       ExecutionRegionPlannerElapsedMicros(candidate_decision_start);
		};
		if (ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
			string full_pipeline_entry_reason;
			if (!ExecutionRegionRuntimeCanEnter(pipeline, full_pipeline_entry_reason)) {
				auto decision_time_us = candidate_decision_time_us();
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
				    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
				    AttachExecutionRegionCandidateReason(candidate, std::move(full_pipeline_entry_reason),
				                                         should_record_detailed_telemetry),
				    "full_pipeline_runtime_missing_source_or_sink", &lowered_region.ir, decision_time_us, 0, 0,
				    &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings);
				continue;
			}
		}
		ExecutionRegionCompilationInput input(context, lowered_region, candidate);
		auto analysis_start = std::chrono::steady_clock::now();
		auto lowering_plan = backend->AnalyzeRegion(input);
		stage_timings.backend_analysis_time_us = ExecutionRegionPlannerElapsedMicros(analysis_start);
		input.lowering_plan = &lowering_plan;
		if (lowering_plan.nodes.empty()) {
			auto decision_time_us = candidate_decision_time_us();
			string empty_analysis_reason = "backend produced an empty execution-region capability analysis";
			auto lowering_reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
			if (!lowering_reason.empty()) {
				empty_analysis_reason += ";";
				empty_analysis_reason += lowering_reason;
			}
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, std::move(empty_analysis_reason),
			                                         should_record_detailed_telemetry),
			    "backend_empty_analysis", &lowered_region.ir, decision_time_us, 0, 0, &candidate,
			    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters());
			continue;
		}
		if (lowering_plan.ExpectedCompiledExecutionMode() == ExecutionRegionExecutionMode::UNSUPPORTED) {
			auto decision_time_us = candidate_decision_time_us();
			auto unsupported_reason =
			    ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
			if (lowering_plan.NativeCount() > 0) {
				unsupported_reason +=
				    ";execution:unsupported;backend cannot generate executable code for this whole region";
			} else {
				unsupported_reason = "region lowering contains no native executable nodes: " + unsupported_reason +
				                     ";execution:unsupported";
			}
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, std::move(unsupported_reason),
			                                         should_record_detailed_telemetry),
			    ExecutionRegionUnsupportedBlockerCode(lowering_plan), &lowered_region.ir, decision_time_us, 0, 0,
			    &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings,
			    lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution(),
			    lowering_plan.UsesScanFilters());
			continue;
		}
		if (lowering_plan.ExpectedRegionExecutionForm() == ExecutionRegionForm::FUSED &&
		    lowering_plan.ExpectedExecutionBody() != ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE) {
			auto decision_time_us = candidate_decision_time_us();
			auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
			if (!reason.empty()) {
				reason += ";";
			}
			reason += "execution:unsupported;backend did not advertise generated machine code for compiled runner";
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason), should_record_detailed_telemetry),
			    "region_without_generated_machine_code", &lowered_region.ir, decision_time_us, 0, 0, &candidate,
			    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters(), nullptr,
			    lowering_plan.ExpectedExecutionBody());
			continue;
		}
		auto fused_contract_decision = ValidateExecutionRegionFusedStageContract(candidate, lowering_plan);
		if (!fused_contract_decision.valid) {
			auto decision_time_us = candidate_decision_time_us();
			auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) +
			              ";execution:unsupported;" + std::move(fused_contract_decision.reason);
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason), should_record_detailed_telemetry),
			    fused_contract_decision.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
			    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters());
			continue;
		}
		auto readiness_decision =
		    ValidateExecutionRegionNativeOperatorReadiness(context, pipeline, lowered_region, candidate);
		if (!readiness_decision.ready) {
			auto decision_time_us = candidate_decision_time_us();
			if (readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED) {
				plan->operator_readiness_refresh = true;
			}
			auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) + ";";
			reason += readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED ? "execution:state-not-ready;"
			                                                                             : "execution:unsupported;";
			reason += std::move(readiness_decision.reason);
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, readiness_decision.status,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason), should_record_detailed_telemetry),
			    readiness_decision.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
			    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters());
			continue;
		}
		auto physical_runner =
		    SelectExecutionRegionPhysicalRunner(policy, candidate, lowering_plan, should_record_detailed_telemetry);
		if (!physical_runner.UsesCompiledRunner()) {
			auto decision_time_us = candidate_decision_time_us();
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, requested_policy,
			    AttachExecutionRegionCandidateReason(candidate, physical_runner.reason,
			                                         should_record_detailed_telemetry),
			    physical_runner.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
			    physical_runner.SelectedRunner(), &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters(), nullptr,
			    lowering_plan.ExpectedExecutionBody(), &physical_runner.runner_cost);
			continue;
		}

		SelectedCandidate selected_region;
		selected_region.candidate_index = candidate_index;
		selected_region.lowering_plan = std::move(lowering_plan);
		selected_region.physical_runner = std::move(physical_runner);
		selected_region.stage_timings = stage_timings;
		selected_region.decision_time_us = candidate_decision_time_us();
		AccumulateExecutionRegionOpenRequest(*plan, lowered_region, candidate, selected_region.lowering_plan);
		selected_regions.push_back(std::move(selected_region));
	}
	Compile(context, *backend, backend_name, requested_policy, *plan, lowered_region, selected_regions);
	if (plan->HasExecutableFullPipeline() && !ExecutionRegionSettings::TraceVectorizedBaseline(context)) {
		plan->SelectRunner(ExecutionRunnerKind::COMPILED_VECTORIZED);
	}
	return KeepExecutableExecutionRegionPlan(std::move(plan));
}

void ExecutionRegionPlanner::Compile(ClientContext &context, ExecutionRegionBackend &backend,
                                     const string &backend_name, ExecutionRegionPolicyMode requested_policy,
                                     ExecutionRegionPlan &plan,
                                     ExecutionRegionIR &region_ir,
                                     vector<ExecutionRegionPlanner::SelectedCandidate> &selected_regions) {
	plan.kernels.clear();
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::Enabled(context) ||
	    selected_regions.empty()) {
		return;
	}
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	for (auto &compiled_region : selected_regions) {
		auto &candidate = region_ir.candidates[compiled_region.candidate_index];
		auto stage_timings = compiled_region.stage_timings;
		ExecutionRegionCompilationInput input(context, region_ir, candidate);
		input.lowering_plan = &compiled_region.lowering_plan;
		auto start = std::chrono::steady_clock::now();
		auto result = backend.CompileRegion(input);
		auto compile_time_us = ExecutionRegionPlannerElapsedMicros(start);
		stage_timings.codegen_time_us = compile_time_us;

		idx_t code_size = result.kernel ? result.kernel->CodeSize() : 0;
		auto status = result.status;
		auto reason = AttachExecutionRegionCandidateReason(
		    candidate,
		    ComposeExecutionRegionCompileEventReason(compiled_region.physical_runner, requested_policy, result.reason),
		    should_record_detailed_telemetry);
		auto execution_mode = result.execution_mode;
		if (status != ExecutionRegionCompileStatus::COMPILED && result.kernel) {
			throw InternalException("execution region backend \"%s\" returned kernel for non-compiled region status %s",
			                        backend_name, ExecutionRegionCompileStatusToString(status));
		}
		if (status == ExecutionRegionCompileStatus::COMPILED) {
			auto expected_mode = compiled_region.lowering_plan.ExpectedCompiledExecutionMode();
			auto expected_form = compiled_region.lowering_plan.ExpectedRegionExecutionForm();
			if (expected_mode == ExecutionRegionExecutionMode::UNSUPPORTED) {
				throw InternalException(
				    "execution region backend \"%s\" compiled region without native executable nodes", backend_name);
			}
			if (expected_form == ExecutionRegionForm::NONE) {
				throw InternalException(
				    "execution region backend \"%s\" compiled region without an explicit region execution form",
				    backend_name);
			}
			if (expected_form != ExecutionRegionForm::FUSED) {
				throw InternalException("execution region backend \"%s\" compiled unsupported region execution form %s",
				                        backend_name, ExecutionRegionFormToString(expected_form));
			}
			if (execution_mode != expected_mode) {
				throw InternalException(
				    "execution region backend \"%s\" compiled region with execution mode %s after advertising %s",
				    backend_name, ExecutionRegionExecutionModeToString(execution_mode),
				    ExecutionRegionExecutionModeToString(expected_mode));
			}
			if (!result.kernel || !result.kernel->HasExecutableBody()) {
				throw InternalException("execution region backend \"%s\" compiled region without executable code",
				                        backend_name);
			}
			if (ExecutionRegionABIIsFullPipeline(candidate.contract.abi) && !result.kernel->CanExecuteFullPipeline()) {
				throw InternalException(
				    "execution region backend \"%s\" compiled full pipeline without full-pipeline executable ABI",
				    backend_name);
			}
		}
		auto trace_id = execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, status, execution_mode,
		    requested_policy, reason, ExecutionRegionCompileResultBlockerCode(status),
		    &result.ir, compiled_region.decision_time_us, compile_time_us, code_size, &candidate,
		    compiled_region.physical_runner.SelectedRunner(), &stage_timings,
		    compiled_region.lowering_plan.ExpectedRegionExecutionForm(),
		    compiled_region.lowering_plan.SelectedSourceExecution(), compiled_region.lowering_plan.UsesScanFilters(),
		    nullptr, result.execution_body, &compiled_region.physical_runner.runner_cost);
		if (status == ExecutionRegionCompileStatus::COMPILED && result.kernel) {
			result.kernel->SetTraceInfo(trace_id, execution_mode, result.execution_body, reason, compile_time_us,
			                            code_size);
			result.kernel->SetExecutionABI(candidate.contract.abi);
			result.kernel->SetTraceRegionExecutionForm(compiled_region.lowering_plan.ExpectedRegionExecutionForm());
			result.kernel->SetTraceSelectedSourceExecution(compiled_region.lowering_plan.SelectedSourceExecution());
			result.kernel->SetTraceUsesScanFilters(compiled_region.lowering_plan.UsesScanFilters());
			if (ExecutionRegionSettings::TraceRuntime(context)) {
				result.kernel->SetTracePipeline(candidate);
			}
		}
		if (status == ExecutionRegionCompileStatus::ERROR) {
			throw InvalidInputException("Execution region compilation failed: %s", reason);
		}
		if (result.kernel) {
			plan.kernels.push_back(std::move(result.kernel));
		}
	}
}

} // namespace duckdb

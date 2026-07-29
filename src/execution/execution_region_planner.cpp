#include "duckdb/execution/execution_region_planner.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "execution_region_decision.hpp"

#include <chrono>

namespace duckdb {

static int64_t ExecutionRegionPlannerElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

struct ExecutionRegionPlannerCandidateDecisionTrace {
	std::chrono::steady_clock::time_point start;
	ExecutionRegionStageTimings stage_timings;
};

class ExecutionRegionPlannerDecisionRecorder {
public:
	ExecutionRegionStageTimings &SharedStageTimings() {
		return shared_stage_timings;
	}

	int64_t SharedDecisionTime() const {
		return shared_stage_timings.pipeline_cbo_time_us + shared_stage_timings.graph_build_time_us +
		       shared_stage_timings.ir_lowering_time_us;
	}

	ExecutionRegionPlannerCandidateDecisionTrace BeginCandidate() const {
		ExecutionRegionPlannerCandidateDecisionTrace result;
		result.start = std::chrono::steady_clock::now();
		return result;
	}

	int64_t ClaimCandidateDecisionTime(ExecutionRegionPlannerCandidateDecisionTrace &trace) {
		auto decision_time_us = ExecutionRegionPlannerElapsedMicros(trace.start);
		if (!shared_decision_time_recorded) {
			decision_time_us += SharedDecisionTime();
			trace.stage_timings.pipeline_cbo_time_us = shared_stage_timings.pipeline_cbo_time_us;
			trace.stage_timings.graph_build_time_us = shared_stage_timings.graph_build_time_us;
			trace.stage_timings.ir_lowering_time_us = shared_stage_timings.ir_lowering_time_us;
			shared_decision_time_recorded = true;
		}
		return decision_time_us;
	}

private:
	ExecutionRegionStageTimings shared_stage_timings;
	bool shared_decision_time_recorded = false;
};

struct ExecutionRegionPlanner::SelectedCandidate {
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

struct ExecutionRegionFusedStagePlanDecision {
	bool valid = true;
	string reason;
	string blocker;
};

static ExecutionRegionFusedStagePlanDecision
ValidateExecutionRegionFusedStagePlan(const ExecutionRegionStagePlan &stage_plan) {
	ExecutionRegionFusedStagePlanDecision decision;
	if (!stage_plan.HasStages()) {
		decision.valid = false;
		decision.blocker = "fused_region_stage_plan_empty";
		decision.reason = "core operator-stage plan is empty";
		return decision;
	}
	bool has_executable_stage = false;
	for (auto &stage : stage_plan.stages) {
		if (ExecutionRegionStageExecutionIsFusionBlocker(stage.execution)) {
			decision.valid = false;
			decision.blocker = stage.execution == ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
			                       ? "fused_region_source_boundary"
			                       : "fused_region_missing_contract";
			decision.reason = "core operator-stage plan contains a boundary stage;";
			decision.reason += DescribeExecutionRegionStageFusionBlocker(stage);
			return decision;
		}
		has_executable_stage = has_executable_stage || ExecutionRegionStageExecutionIsFused(stage.execution);
	}
	if (!has_executable_stage) {
		decision.valid = false;
		decision.blocker = "fused_region_no_executable_stage";
		decision.reason = "core operator-stage plan has no generated or native stage";
		return decision;
	}
	return decision;
}

static ExecutionRegionFusedStagePlanDecision
ValidateExecutionRegionFusedStagePlan(const ExecutionRegionIR &region_ir, const ExecutionRegionCandidate &candidate,
                                      const ExecutionRegionLoweringPlan &lowering_plan) {
	auto decision = ValidateExecutionRegionFusedStagePlan(candidate.stage_plan);
	if (!decision.valid) {
		return decision;
	}
	auto &candidate_stages = candidate.stage_plan.stages;
	auto &selected_stages = lowering_plan.SelectedStages();
	if (selected_stages.size() != candidate_stages.size()) {
		decision.valid = false;
		decision.blocker = "fused_region_stage_selection_incomplete";
		decision.reason = "backend stage selection does not cover the complete core stage plan;selected=" +
		                  std::to_string(selected_stages.size()) +
		                  ";required=" + std::to_string(candidate_stages.size());
		return decision;
	}

	optional_ptr<const ExecutionRegionSourceInfo> selected_source;
	bool has_source_filters = false;
	bool has_generated_source_filters = false;
	for (idx_t stage_idx = 0; stage_idx < candidate_stages.size(); stage_idx++) {
		auto &selected_stage = selected_stages[stage_idx];
		if (selected_stage.stage_index != stage_idx) {
			decision.valid = false;
			decision.blocker = "fused_region_stage_selection_order_mismatch";
			decision.reason = "backend stage selection must use every core stage exactly once in order;receipt_index=" +
			                  std::to_string(stage_idx) +
			                  ";selected_stage_index=" + std::to_string(selected_stage.stage_index);
			return decision;
		}
		auto &candidate_stage = candidate_stages[stage_idx];
		if (!ExecutionRegionStageExecutionIsFused(selected_stage.execution)) {
			decision.valid = false;
			decision.blocker = "fused_region_stage_selection_not_executable";
			auto rejected_stage = candidate_stage;
			rejected_stage.execution = selected_stage.execution;
			decision.reason =
			    "backend stage selection contains an unfused stage;stage_index=" + std::to_string(stage_idx) + ";" +
			    DescribeExecutionRegionStageFusionBlocker(rejected_stage);
			return decision;
		}
		if (candidate_stage.kind != ExecutionRegionStageKind::SOURCE_FILTER &&
		    selected_stage.execution != candidate_stage.execution) {
			decision.valid = false;
			decision.blocker = "fused_region_stage_selection_execution_mismatch";
			auto rejected_stage = candidate_stage;
			rejected_stage.execution = selected_stage.execution;
			decision.reason =
			    "backend stage selection changed the core execution kind;stage_index=" + std::to_string(stage_idx) +
			    ";core_stage_execution=" + ExecutionRegionStageExecutionKindToString(candidate_stage.execution) + ";" +
			    DescribeExecutionRegionStageFusionBlocker(rejected_stage);
			return decision;
		}
		if (candidate_stage.node_index >= region_ir.nodes.size()) {
			decision.valid = false;
			decision.blocker = "fused_region_stage_selection_invalid_node";
			decision.reason =
			    "backend stage selection points outside core region nodes;stage_index=" + std::to_string(stage_idx);
			return decision;
		}
		auto &node = region_ir.nodes[candidate_stage.node_index];
		if (candidate_stage.kind == ExecutionRegionStageKind::SOURCE) {
			if (!node.source || selected_stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
				decision.valid = false;
				decision.blocker = "fused_region_source_selection_invalid";
				decision.reason = "backend source stage selection requires a native source contract;stage_index=" +
				                  std::to_string(stage_idx);
				return decision;
			}
			selected_source = node.source.get();
		}
		if (candidate_stage.kind == ExecutionRegionStageKind::SOURCE_FILTER) {
			has_source_filters = true;
			has_generated_source_filters = has_generated_source_filters ||
			                               selected_stage.execution == ExecutionRegionStageExecutionKind::GENERATED_IR;
		}
	}

	if (!selected_source) {
		decision.valid = false;
		decision.blocker = "fused_region_source_selection_missing";
		decision.reason = "backend stage selection contains no source stage";
		return decision;
	}
	auto &source = *selected_source;
	if (lowering_plan.SelectedSourceExecution() != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT ||
	    source.source_contract.status != ExecutionRegionSourceContractStatus::READY) {
		decision.valid = false;
		decision.blocker = "fused_region_source_recipe_invalid";
		decision.reason = "backend source recipe requires a ready source contract";
		return decision;
	}
	if (!lowering_plan.SourceContractInputTypes().empty()) {
		if (!source.table_scan_contract.present ||
		    lowering_plan.SourceContractInputTypes() != source.table_scan_contract.source_contract_input_types) {
			decision.valid = false;
			decision.blocker = "fused_region_source_recipe_layout_mismatch";
			decision.reason = "backend source recipe input layout disagrees with the core source contract";
			return decision;
		}
	}

	auto scan_filter_mode = lowering_plan.ScanFilterMode();
	if (!source.table_scan_contract.present) {
		if (scan_filter_mode != ExecutionRegionScanFilterMode::NONE || has_source_filters) {
			decision.valid = false;
			decision.blocker = "fused_region_stateful_source_filter_recipe_invalid";
			decision.reason = "stateful source recipe cannot claim table scan filter ownership";
		}
		return decision;
	}
	const bool has_dynamic_filters = source.table_scan_contract.dynamic_filters;
	if (scan_filter_mode != ExecutionRegionScanFilterMode::NONE && !source.table_scan_contract.filter_pushdown) {
		decision.valid = false;
		decision.blocker = "fused_region_scan_filter_contract_missing";
		decision.reason = "backend source recipe claims scan-filter ownership without source filter pushdown";
		return decision;
	}
	if (has_dynamic_filters && scan_filter_mode != ExecutionRegionScanFilterMode::ALL &&
	    scan_filter_mode != ExecutionRegionScanFilterMode::DYNAMIC_FILTERS_WITH_STATIC_PRUNING) {
		decision.valid = false;
		decision.blocker = "fused_region_dynamic_filter_ownership_missing";
		decision.reason = "backend source recipe does not preserve dynamic scan filters";
		return decision;
	}
	if (has_generated_source_filters && lowering_plan.SourceContractInputTypes().empty()) {
		decision.valid = false;
		decision.blocker = "fused_region_generated_filter_layout_missing";
		decision.reason = "generated source-filter stages require the source-contract input layout";
		return decision;
	}
	if (!has_generated_source_filters && !lowering_plan.SourceContractInputTypes().empty()) {
		decision.valid = false;
		decision.blocker = "fused_region_source_recipe_layout_spurious";
		decision.reason = "backend source recipe publishes an input layout without generated source filters";
		return decision;
	}
	if (!has_dynamic_filters &&
	    scan_filter_mode == ExecutionRegionScanFilterMode::DYNAMIC_FILTERS_WITH_STATIC_PRUNING) {
		decision.valid = false;
		decision.blocker = "fused_region_dynamic_filter_ownership_spurious";
		decision.reason = "backend source recipe claims dynamic scan filters that are absent";
		return decision;
	}
	if (!has_source_filters && scan_filter_mode == ExecutionRegionScanFilterMode::STATIC_PRUNING_ONLY) {
		decision.valid = false;
		decision.blocker = "fused_region_static_filter_ownership_spurious";
		decision.reason = "backend source recipe claims static filter pruning without static filters";
		return decision;
	}
	const auto expected_static_filter_execution = scan_filter_mode == ExecutionRegionScanFilterMode::ALL
	                                                  ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
	                                                  : ExecutionRegionStageExecutionKind::GENERATED_IR;
	for (auto &selected_stage : selected_stages) {
		D_ASSERT(selected_stage.stage_index < candidate_stages.size());
		auto &candidate_stage = candidate_stages[selected_stage.stage_index];
		if (candidate_stage.kind != ExecutionRegionStageKind::SOURCE_FILTER ||
		    selected_stage.execution == expected_static_filter_execution) {
			continue;
		}
		decision.valid = false;
		decision.blocker = "fused_region_static_filter_ownership_mismatch";
		auto rejected_stage = candidate_stage;
		rejected_stage.execution = selected_stage.execution;
		decision.reason = "backend source recipe and selected source-filter stages disagree;" +
		                  DescribeExecutionRegionStageFusionBlocker(rejected_stage);
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
		decision.blocker = readiness.blocker.empty() ? (decision.status == ExecutionRegionCompileStatus::SKIPPED
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

static bool ExecutionRegionCandidateNeedsFinalizedSourceCardinality(const ExecutionRegionIR &region_ir,
                                                                    const ExecutionRegionCandidate &candidate,
                                                                    string &reason) {
	for (idx_t node_idx = 0; node_idx < region_ir.nodes.size(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (!node.source || !node.source->finalized_source_cardinality_required ||
		    node.source->estimated_source_cardinality_exact) {
			continue;
		}
		if (candidate.estimated_cardinality > 0 || node.estimated_cardinality > 0) {
			continue;
		}
		reason = "native state scan source cardinality is not finalized";
		reason += ";source_node=" + std::to_string(node_idx);
		reason += ";source_function=" + node.source->function_name;
		reason += ";source_execution=";
		reason += ExecutionRegionSourceExecutionKindToString(node.source->execution);
		reason += ";native_state_scan_status=";
		reason += ExecutionRegionStateContractStatusToString(node.source->native_state_scan_contract.status);
		return true;
	}
	return false;
}

static void AccumulateExecutionRegionOpenRequest(ExecutionRegionPlan &plan, const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate,
                                                 const ExecutionRegionLoweringPlan &lowering_plan) {
	if (!ExecutionRegionABIIsFullPipeline(candidate.abi)) {
		return;
	}
	for (idx_t node_idx = 0; node_idx < region_ir.nodes.size(); node_idx++) {
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
		    lowering_plan.SelectedStageCount() == candidate.stage_plan.stages.size();
		if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			auto &contract = plan.source_open_request;
			contract.source_execution =
			    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
			            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
			            native_fused_source_owner
			        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
			        : ExecutionRegionSourceExecutionKind::NONE;
			contract.scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
			contract.source_contract_input_types =
			    native_fused_source_owner ? lowering_plan.SourceContractInputTypes() : vector<LogicalType>();
			return;
		}
		if (!source.table_scan_contract.present) {
			continue;
		}
		auto &table_scan_contract = source.table_scan_contract;
		auto &plan_contract = plan.source_open_request;
		plan_contract.source_execution =
		    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
		            native_fused_source_owner
		        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		        : ExecutionRegionSourceExecutionKind::NONE;
		plan_contract.scan_filter_mode =
		    native_fused_source_owner && (!source.filters.empty() || table_scan_contract.dynamic_filters)
		        ? lowering_plan.ScanFilterMode()
		        : ExecutionRegionScanFilterMode::NONE;
		plan_contract.source_contract_input_types =
		    native_fused_source_owner ? lowering_plan.SourceContractInputTypes() : vector<LogicalType>();
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
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	auto should_record_decision_telemetry = ExecutionRegionSettings::ShouldRecordDecisionTelemetry(context);
	auto needs_backend_diagnostics = ExecutionRegionPlanningNeedsBackendDiagnostics(context);
	auto needs_candidate_diagnostics = ExecutionRegionPlanningNeedsCandidateDiagnostics(context);
	string backend_name;
	optional_ptr<ExecutionRegionBackend> backend;
	const string *active_pipeline_shape = nullptr;
	auto record_decision_event = [&](string event_backend_name, ExecutionRegionCompileStatus status,
	                                 ExecutionRegionExecutionMode execution_mode, string reason, string blocker,
	                                 const string *ir, int64_t decision_time_us,
	                                 const ExecutionRegionCandidate *candidate, ExecutionRunnerKind selected_runner,
	                                 const ExecutionRegionStageTimings *stage_timings,
	                                 ExecutionRegionSourceExecutionKind selected_source_execution,
	                                 ExecutionRegionScanFilterMode selected_scan_filter_mode,
	                                 const PhysicalRunnerCostProfile *runner_cost) -> idx_t {
		return execution_region_manager.RecordEvent(context, std::move(event_backend_name), status, execution_mode,
		                                            std::move(reason), std::move(blocker), ir, decision_time_us, 0, 0,
		                                            active_pipeline_shape, candidate, selected_runner, stage_timings,
		                                            selected_source_execution, selected_scan_filter_mode, runner_cost);
	};
	auto select_backend_for_runner = [&](ExecutionRunnerKind runner_kind) -> bool {
		if (backend) {
			return backend->RunnerKind() == runner_kind;
		}
		backend = execution_region_manager.SelectBackend(context, backend_name, runner_kind);
		plan->backend_name = backend_name;
		if (!backend) {
			if (should_record_decision_telemetry) {
				record_decision_event(backend_name, ExecutionRegionCompileStatus::UNAVAILABLE,
				                      ExecutionRegionExecutionMode::NONE, "no available execution region backend",
				                      "backend_unavailable", nullptr, 0, nullptr, ExecutionRunnerKind::VECTORIZED,
				                      nullptr, ExecutionRegionSourceExecutionKind::NONE,
				                      ExecutionRegionScanFilterMode::NONE, nullptr);
			}
			return false;
		}
		if (!backend->SupportsRegions()) {
			if (should_record_decision_telemetry) {
				record_decision_event(
				    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED,
				    "backend does not compile regions", "backend_does_not_compile_regions", nullptr, 0, nullptr,
				    ExecutionRunnerKind::VECTORIZED, nullptr, ExecutionRegionSourceExecutionKind::NONE,
				    ExecutionRegionScanFilterMode::NONE, nullptr);
			}
			return false;
		}
		return true;
	};
	if (policy == ExecutionRegionPolicyMode::OFF) {
		if (!should_record_decision_telemetry) {
			return nullptr;
		}
		record_decision_event("policy", ExecutionRegionCompileStatus::DISABLED, ExecutionRegionExecutionMode::NONE,
		                      "execution_region_policy=off", "policy_off", nullptr, 0, nullptr,
		                      ExecutionRunnerKind::VECTORIZED, nullptr, ExecutionRegionSourceExecutionKind::NONE,
		                      ExecutionRegionScanFilterMode::NONE, nullptr);
		return nullptr;
	}
	auto cost_parameters = BuildPhysicalRunnerCostParameters(context);
	if (!ExecutionRegionProductionEligibilityAllowsPlanning(context, cost_parameters)) {
		return nullptr;
	}
	auto restrict_cost_parameters_to_backend = [&]() {
		if (!backend) {
			return;
		}
		auto runner_kind = backend->RunnerKind();
		for (idx_t axis_idx = 0; axis_idx < PhysicalRunnerCostParameters::AXIS_COUNT; axis_idx++) {
			cost_parameters.AxisAt(axis_idx).available =
			    PhysicalRunnerCostParameters::AxisRunner(axis_idx) == runner_kind;
		}
	};
	auto requested_backend_is_auto = StringUtil::CIEquals(ExecutionRegionSettings::RequestedBackend(context), "auto");
	auto desired_runner = ExecutionRunnerKind::COMPILED_VECTORIZED;
	auto decision_event_backend_name = [&](ExecutionRunnerKind runner_kind) {
		if (!backend_name.empty()) {
			return backend_name;
		}
		if (!requested_backend_is_auto) {
			return ExecutionRegionSettings::RequestedBackend(context);
		}
		string selected_name;
		try {
			if (execution_region_manager.SelectBackend(context, selected_name, runner_kind)) {
				return selected_name;
			}
		} catch (...) {
		}
		return string("auto");
	};
	auto select_requested_backend = [&]() -> bool {
		if (requested_backend_is_auto) {
			return true;
		}
		backend =
		    execution_region_manager.SelectBackend(context, backend_name, ExecutionRunnerKind::COMPILED_VECTORIZED);
		if (!backend) {
			backend = execution_region_manager.SelectBackend(context, backend_name, ExecutionRunnerKind::COMPILED_GPU);
		}
		plan->backend_name = backend_name;
		if (!backend) {
			if (should_record_decision_telemetry) {
				record_decision_event(backend_name, ExecutionRegionCompileStatus::UNAVAILABLE,
				                      ExecutionRegionExecutionMode::NONE, "no available execution region backend",
				                      "backend_unavailable", nullptr, 0, nullptr, ExecutionRunnerKind::VECTORIZED,
				                      nullptr, ExecutionRegionSourceExecutionKind::NONE,
				                      ExecutionRegionScanFilterMode::NONE, nullptr);
			}
			return false;
		}
		if (!backend->SupportsRegions()) {
			if (should_record_decision_telemetry) {
				record_decision_event(
				    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED,
				    "backend does not compile regions", "backend_does_not_compile_regions", nullptr, 0, nullptr,
				    ExecutionRunnerKind::VECTORIZED, nullptr, ExecutionRegionSourceExecutionKind::NONE,
				    ExecutionRegionScanFilterMode::NONE, nullptr);
			}
			return false;
		}
		desired_runner = backend->RunnerKind();
		restrict_cost_parameters_to_backend();
		return true;
	};
	if (!select_requested_backend()) {
		return nullptr;
	}
	ExecutionRegionPlannerDecisionRecorder decision_recorder;
	auto &shared_stage_timings = decision_recorder.SharedStageTimings();
	if (!needs_candidate_diagnostics) {
		auto pipeline_decision_start = std::chrono::steady_clock::now();
		auto physical_runner = SelectExecutionRegionPipelinePhysicalRunner(cost_parameters, pipeline);
		auto pipeline_cbo_time_us = ExecutionRegionPlannerElapsedMicros(pipeline_decision_start);
		shared_stage_timings.pipeline_cbo_time_us = pipeline_cbo_time_us;
		if (!physical_runner.UsesCompiledRunner()) {
			if (should_record_decision_telemetry) {
				if (!backend && !select_backend_for_runner(ExecutionRunnerKind::COMPILED_VECTORIZED)) {
					return nullptr;
				}
				auto decision_time_us = ExecutionRegionPlannerElapsedMicros(pipeline_decision_start);
				record_decision_event(
				    backend_name, ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
				    physical_runner.reason, physical_runner.blocker, nullptr, decision_time_us, nullptr,
				    physical_runner.SelectedRunner(), &shared_stage_timings, ExecutionRegionSourceExecutionKind::NONE,
				    ExecutionRegionScanFilterMode::NONE, &physical_runner.runner_cost);
			}
			return nullptr;
		}
		desired_runner = physical_runner.SelectedRunner();
		if (!select_backend_for_runner(physical_runner.SelectedRunner())) {
			return nullptr;
		}
	}
	if (!requested_backend_is_auto && !select_backend_for_runner(desired_runner)) {
		return nullptr;
	}
	if (!requested_backend_is_auto) {
		restrict_cost_parameters_to_backend();
	}
	ExecutionExpressionAnalysisCache expression_analysis_cache;
	auto region_ir_mode =
	    ExecutionRegionSettings::DumpIR(context) ? ExecutionRegionIRMode::TRACE : ExecutionRegionIRMode::COMPACT;
	auto graph_build_start = std::chrono::steady_clock::now();
	auto pipeline_descriptor = BuildExecutionRegionGraph(pipeline, region_ir_mode == ExecutionRegionIRMode::TRACE);
	auto graph_build_time_us = ExecutionRegionPlannerElapsedMicros(graph_build_start);
	shared_stage_timings.graph_build_time_us = graph_build_time_us;
	if (!pipeline_descriptor) {
		if (should_record_decision_telemetry) {
			record_decision_event(
			    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED,
			    "core region graph builder produced no execution-region graph", "no_execution_region_graph", nullptr,
			    shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us, nullptr,
			    ExecutionRunnerKind::VECTORIZED, &shared_stage_timings, ExecutionRegionSourceExecutionKind::NONE,
			    ExecutionRegionScanFilterMode::NONE, nullptr);
		}
		return nullptr;
	}
	if (!needs_candidate_diagnostics &&
	    !ExecutionRegionGraphMayHaveCostedAcceleration(*pipeline_descriptor, cost_parameters)) {
		if (should_record_decision_telemetry) {
			string reason = "duckdb_cbo skips region lowering because pipeline has no costed acceleration";
			reason += ";region_lowering=skipped";
			record_decision_event(
			    backend_name, ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
			    std::move(reason), EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED, nullptr,
			    shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us, nullptr,
			    ExecutionRunnerKind::VECTORIZED, &shared_stage_timings, ExecutionRegionSourceExecutionKind::NONE,
			    ExecutionRegionScanFilterMode::NONE, nullptr);
		}
		return nullptr;
	}
	auto region_decision_start = std::chrono::steady_clock::now();
	auto region_ir = TryLowerExecutionRegion(*pipeline_descriptor, region_ir_mode, &expression_analysis_cache);
	auto region_lowering_time_us = ExecutionRegionPlannerElapsedMicros(region_decision_start);
	shared_stage_timings.ir_lowering_time_us = region_lowering_time_us;
	if (!region_ir) {
		if (should_record_decision_telemetry) {
			auto rejected_reason = DescribeExecutionRegionLoweringRejection(*pipeline_descriptor);
			record_decision_event(
			    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED,
			    std::move(rejected_reason), "no_typed_region_ir", nullptr, decision_recorder.SharedDecisionTime(),
			    nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings,
			    ExecutionRegionSourceExecutionKind::NONE, ExecutionRegionScanFilterMode::NONE, nullptr);
		}
		return nullptr;
	}
	auto &lowered_region = *region_ir;
	active_pipeline_shape = &lowered_region.pipeline_shape;
	if (!lowered_region.candidate) {
		if (should_record_decision_telemetry) {
			string reason = "core region lowering produced no candidates";
			if (!lowered_region.candidate_blocker.empty()) {
				reason = "core-region-candidate-blocked:" +
				         FirstExecutionRegionReasonToken(lowered_region.candidate_blocker) + ";" + reason + ";" +
				         lowered_region.candidate_blocker;
			}
			record_decision_event(
			    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED,
			    reason, ExecutionRegionCandidateBlockerCode(lowered_region), &lowered_region.ir,
			    decision_recorder.SharedDecisionTime(), nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings,
			    ExecutionRegionSourceExecutionKind::NONE, ExecutionRegionScanFilterMode::NONE, nullptr);
		}
		return nullptr;
	}

	unique_ptr<SelectedCandidate> selected_region;
	do {
		auto &candidate = *lowered_region.candidate;
		auto candidate_trace = decision_recorder.BeginCandidate();
		auto &stage_timings = candidate_trace.stage_timings;
		ExecutionRegionPhysicalRunnerSelection cost_only_physical_runner;
		bool has_cost_only_physical_runner = false;
		if (ExecutionRegionABIIsFullPipeline(candidate.abi)) {
			string full_pipeline_entry_reason;
			if (!ExecutionRegionRuntimeCanEnter(pipeline, full_pipeline_entry_reason)) {
				if (should_record_decision_telemetry) {
					auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
					record_decision_event(
					    backend_name, ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
					    AttachExecutionRegionCandidateReason(candidate, std::move(full_pipeline_entry_reason),
					                                         should_record_detailed_telemetry),
					    "full_pipeline_runtime_missing_source_or_sink", &lowered_region.ir, decision_time_us,
					    &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings,
					    ExecutionRegionSourceExecutionKind::NONE, ExecutionRegionScanFilterMode::NONE, nullptr);
				}
				continue;
			}
		}
		string finalized_cardinality_reason;
		if (ExecutionRegionCandidateNeedsFinalizedSourceCardinality(lowered_region, candidate,
		                                                            finalized_cardinality_reason)) {
			plan->operator_readiness_refresh = true;
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				record_decision_event(
				    decision_event_backend_name(ExecutionRunnerKind::COMPILED_VECTORIZED),
				    ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(finalized_cardinality_reason),
				                                         should_record_detailed_telemetry),
				    "state_scan_source_cardinality_not_ready", &lowered_region.ir, decision_time_us, &candidate,
				    ExecutionRunnerKind::VECTORIZED, &stage_timings, ExecutionRegionSourceExecutionKind::NONE,
				    ExecutionRegionScanFilterMode::NONE, nullptr);
			}
			continue;
		}
		if (!needs_backend_diagnostics) {
			auto candidate_cbo_start = std::chrono::steady_clock::now();
			auto physical_runner = SelectExecutionRegionCostOnlyPhysicalRunner(cost_parameters, candidate);
			stage_timings.candidate_cbo_time_us += ExecutionRegionPlannerElapsedMicros(candidate_cbo_start);
			if (!physical_runner.UsesCompiledRunner()) {
				if (should_record_decision_telemetry) {
					auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
					record_decision_event(
					    decision_event_backend_name(ExecutionRunnerKind::COMPILED_VECTORIZED),
					    ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
					    AttachExecutionRegionCandidateReason(candidate, physical_runner.reason,
					                                         should_record_detailed_telemetry),
					    physical_runner.blocker, &lowered_region.ir, decision_time_us, &candidate,
					    physical_runner.SelectedRunner(), &stage_timings, ExecutionRegionSourceExecutionKind::NONE,
					    ExecutionRegionScanFilterMode::NONE, &physical_runner.runner_cost);
				}
				continue;
			}
			cost_only_physical_runner = std::move(physical_runner);
			has_cost_only_physical_runner = true;
		}
		if (has_cost_only_physical_runner &&
		    (!backend || backend->RunnerKind() != cost_only_physical_runner.SelectedRunner())) {
			if (!select_backend_for_runner(cost_only_physical_runner.SelectedRunner())) {
				return nullptr;
			}
			restrict_cost_parameters_to_backend();
		}
		if (!needs_backend_diagnostics) {
			auto stage_plan_decision = ValidateExecutionRegionFusedStagePlan(candidate.stage_plan);
			if (!stage_plan_decision.valid) {
				if (should_record_decision_telemetry) {
					auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
					record_decision_event(
					    backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
					    ExecutionRegionExecutionMode::UNSUPPORTED,
					    AttachExecutionRegionCandidateReason(candidate, std::move(stage_plan_decision.reason),
					                                         should_record_detailed_telemetry),
					    stage_plan_decision.blocker, &lowered_region.ir, decision_time_us, &candidate,
					    ExecutionRunnerKind::VECTORIZED, &stage_timings, ExecutionRegionSourceExecutionKind::NONE,
					    ExecutionRegionScanFilterMode::NONE, nullptr);
				}
				continue;
			}
		}
		if (!backend) {
			if (!select_backend_for_runner(desired_runner)) {
				return nullptr;
			}
			restrict_cost_parameters_to_backend();
		}
		ExecutionRegionCompilationInput input(context, lowered_region, candidate);
		auto analysis_start = std::chrono::steady_clock::now();
		auto lowering_plan = backend->AnalyzeRegion(input);
		stage_timings.backend_analysis_time_us = ExecutionRegionPlannerElapsedMicros(analysis_start);
		input.lowering_plan = &lowering_plan;
		if (!lowering_plan.HasNodes()) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				string empty_analysis_reason = "backend produced an empty execution-region capability analysis";
				auto lowering_reason =
				    ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
				if (!lowering_reason.empty()) {
					empty_analysis_reason += ";";
					empty_analysis_reason += lowering_reason;
				}
				record_decision_event(backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				                      ExecutionRegionExecutionMode::UNSUPPORTED,
				                      AttachExecutionRegionCandidateReason(candidate, std::move(empty_analysis_reason),
				                                                           should_record_detailed_telemetry),
				                      "backend_empty_analysis", &lowered_region.ir, decision_time_us, &candidate,
				                      ExecutionRunnerKind::VECTORIZED, &stage_timings,
				                      lowering_plan.SelectedSourceExecution(), lowering_plan.ScanFilterMode(), nullptr);
			}
			continue;
		}
		if (lowering_plan.ExpectedCompiledExecutionMode() == ExecutionRegionExecutionMode::UNSUPPORTED) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				auto unsupported_reason =
				    ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
				if (lowering_plan.NativeCount() > 0) {
					unsupported_reason +=
					    ";execution:unsupported;backend cannot generate executable code for this whole region";
				} else {
					unsupported_reason = "region lowering contains no native executable nodes: " + unsupported_reason +
					                     ";execution:unsupported";
				}
				record_decision_event(backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				                      ExecutionRegionExecutionMode::UNSUPPORTED,
				                      AttachExecutionRegionCandidateReason(candidate, std::move(unsupported_reason),
				                                                           should_record_detailed_telemetry),
				                      ExecutionRegionUnsupportedBlockerCode(lowering_plan), &lowered_region.ir,
				                      decision_time_us, &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings,
				                      lowering_plan.SelectedSourceExecution(), lowering_plan.ScanFilterMode(), nullptr);
			}
			continue;
		}
		auto fused_stage_plan_decision =
		    ValidateExecutionRegionFusedStagePlan(lowered_region, candidate, lowering_plan);
		if (!fused_stage_plan_decision.valid) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) +
				              ";execution:unsupported;" + std::move(fused_stage_plan_decision.reason);
				record_decision_event(backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				                      ExecutionRegionExecutionMode::UNSUPPORTED,
				                      AttachExecutionRegionCandidateReason(candidate, std::move(reason),
				                                                           should_record_detailed_telemetry),
				                      fused_stage_plan_decision.blocker, &lowered_region.ir, decision_time_us,
				                      &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings,
				                      lowering_plan.SelectedSourceExecution(), lowering_plan.ScanFilterMode(), nullptr);
			}
			continue;
		}
		auto readiness_decision =
		    ValidateExecutionRegionNativeOperatorReadiness(context, pipeline, lowered_region, candidate);
		if (!readiness_decision.ready) {
			if (readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED) {
				plan->operator_readiness_refresh = true;
			}
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) + ";";
				reason += readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED
				              ? "execution:state-not-ready;"
				              : "execution:unsupported;";
				reason += std::move(readiness_decision.reason);
				record_decision_event(backend_name, readiness_decision.status,
				                      ExecutionRegionExecutionMode::UNSUPPORTED,
				                      AttachExecutionRegionCandidateReason(candidate, std::move(reason),
				                                                           should_record_detailed_telemetry),
				                      readiness_decision.blocker, &lowered_region.ir, decision_time_us, &candidate,
				                      ExecutionRunnerKind::VECTORIZED, &stage_timings,
				                      lowering_plan.SelectedSourceExecution(), lowering_plan.ScanFilterMode(), nullptr);
			}
			continue;
		}
		ExecutionRegionPhysicalRunnerSelection physical_runner;
		auto candidate_cbo_start = std::chrono::steady_clock::now();
		physical_runner = SelectExecutionRegionPhysicalRunner(cost_parameters, candidate, lowering_plan,
		                                                      should_record_detailed_telemetry);
		stage_timings.candidate_cbo_time_us += ExecutionRegionPlannerElapsedMicros(candidate_cbo_start);
		if (!physical_runner.UsesCompiledRunner()) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
				record_decision_event(
				    backend_name, ExecutionRegionCompileStatus::SKIPPED, ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, physical_runner.reason,
				                                         should_record_detailed_telemetry),
				    physical_runner.blocker, &lowered_region.ir, decision_time_us, &candidate,
				    physical_runner.SelectedRunner(), &stage_timings, lowering_plan.SelectedSourceExecution(),
				    lowering_plan.ScanFilterMode(), &physical_runner.runner_cost);
			}
			continue;
		}

		selected_region = make_uniq<SelectedCandidate>();
		selected_region->lowering_plan = std::move(lowering_plan);
		selected_region->physical_runner = std::move(physical_runner);
		selected_region->decision_time_us = decision_recorder.ClaimCandidateDecisionTime(candidate_trace);
		selected_region->stage_timings = stage_timings;
		AccumulateExecutionRegionOpenRequest(*plan, lowered_region, candidate, selected_region->lowering_plan);
	} while (false);
	if (!selected_region) {
		return KeepExecutableExecutionRegionPlan(std::move(plan));
	}
	Compile(context, *backend, backend_name, pipeline, *plan, lowered_region, *selected_region);
	if (auto kernel = plan->GetExecutableFullPipelineKernel(); kernel && kernel->HasTableFilterKernels()) {
		plan->source_open_request.table_filter_kernel_provider =
		    optional_ptr<const TableFilterKernelProvider>(kernel.get());
	}
	if (plan->HasExecutableFullPipeline() && !ExecutionRegionSettings::TraceVectorizedBaseline(context)) {
		plan->SelectRunner(selected_region->physical_runner.SelectedRunner());
	}
	return KeepExecutableExecutionRegionPlan(std::move(plan));
}

static void ValidateExecutionRegionCompileResult(const string &backend_name, const ExecutionRegionCandidate &candidate,
                                                 const ExecutionRegionLoweringPlan &lowering_plan,
                                                 const string &artifact_cache_key,
                                                 const ExecutionRegionCompileResult &result) {
	if (result.status != ExecutionRegionCompileStatus::COMPILED) {
		if (result.kernel || result.artifact) {
			throw InternalException(
			    "execution region backend \"%s\" returned executable state for non-compiled region status %s",
			    backend_name, ExecutionRegionCompileStatusToString(result.status));
		}
		return;
	}
	auto expected_mode = lowering_plan.ExpectedCompiledExecutionMode();
	if (expected_mode == ExecutionRegionExecutionMode::UNSUPPORTED) {
		throw InternalException("execution region backend \"%s\" compiled region without native executable nodes",
		                        backend_name);
	}
	if (lowering_plan.SelectedStageCount() != candidate.stage_plan.stages.size()) {
		throw InternalException("execution region backend \"%s\" compiled a region that is not fully fused",
		                        backend_name);
	}
	if (result.execution_mode != expected_mode) {
		throw InternalException(
		    "execution region backend \"%s\" compiled region with execution mode %s after advertising %s", backend_name,
		    ExecutionRegionExecutionModeToString(result.execution_mode),
		    ExecutionRegionExecutionModeToString(expected_mode));
	}
	if (static_cast<bool>(result.kernel) == static_cast<bool>(result.artifact)) {
		throw InternalException(
		    "execution region backend \"%s\" must return exactly one of a kernel or a reusable artifact", backend_name);
	}
	if (result.artifact && artifact_cache_key.empty()) {
		throw InternalException("execution region backend \"%s\" returned an artifact without a semantic cache key",
		                        backend_name);
	}
	if (!artifact_cache_key.empty() && !result.artifact) {
		throw InternalException("execution region backend \"%s\" advertised an artifact cache key without an artifact",
		                        backend_name);
	}
	if (result.kernel && !result.kernel->HasExecutableBody()) {
		throw InternalException("execution region backend \"%s\" compiled region without executable code",
		                        backend_name);
	}
	if (result.kernel && ExecutionRegionABIIsFullPipeline(candidate.abi) && !result.kernel->CanExecuteFullPipeline()) {
		throw InternalException(
		    "execution region backend \"%s\" compiled full pipeline without full-pipeline executable ABI",
		    backend_name);
	}
}

void ExecutionRegionPlanner::Compile(ClientContext &context, ExecutionRegionBackend &backend,
                                     const string &backend_name, Pipeline &pipeline, ExecutionRegionPlan &plan,
                                     ExecutionRegionIR &region_ir,
                                     ExecutionRegionPlanner::SelectedCandidate &selected_region) {
	plan.kernel.reset();
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::Enabled(context)) {
		return;
	}
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	auto &compiled_region = selected_region;
	auto &candidate = *region_ir.candidate;
	auto stage_timings = compiled_region.stage_timings;
	ExecutionRegionCompilationInput input(context, region_ir, candidate);
	input.lowering_plan = &compiled_region.lowering_plan;
	ExecutionRegionCompileResult result;
	int64_t compile_time_us = 0;
	bool reused_artifact = false;
	string artifact_cache_key;
	ExecutionRegionArtifactCacheReservation artifact_reservation;
	if (compiled_region.lowering_plan.backend_plan) {
		artifact_cache_key = compiled_region.lowering_plan.backend_plan->ArtifactCacheKey();
	}
	if (!artifact_cache_key.empty()) {
		artifact_cache_key =
		    backend_name + ":" + std::to_string(EXECUTION_REGION_BACKEND_ABI_VERSION) + ":" + artifact_cache_key;
		auto &cache = execution_region_manager.artifact_cache;
		artifact_reservation = cache.LookupOrReserve(artifact_cache_key);
		if (artifact_reservation.IsHit()) {
			auto &cached = *artifact_reservation.Cached();
			auto reason = cached.compile_reason;
			if (!reason.empty()) {
				reason += ";";
			}
			reason += "compiled artifact cache hit";
			result = ExecutionRegionCompileResult::CompiledArtifact(cached.artifact, cached.execution_mode,
			                                                        std::move(reason), cached.ir);
			reused_artifact = true;
		} else {
			auto compile_start = std::chrono::steady_clock::now();
			result = backend.CompileRegion(input);
			compile_time_us = ExecutionRegionPlannerElapsedMicros(compile_start);
			ValidateExecutionRegionCompileResult(backend_name, candidate, compiled_region.lowering_plan,
			                                     artifact_cache_key, result);
		}
	} else {
		auto compile_start = std::chrono::steady_clock::now();
		result = backend.CompileRegion(input);
		compile_time_us = ExecutionRegionPlannerElapsedMicros(compile_start);
		ValidateExecutionRegionCompileResult(backend_name, candidate, compiled_region.lowering_plan, artifact_cache_key,
		                                     result);
	}
	if (result.status == ExecutionRegionCompileStatus::COMPILED && result.artifact) {
		auto artifact = result.artifact;
		auto instantiate_start = std::chrono::steady_clock::now();
		result.kernel = backend.InstantiateRegionArtifact(artifact, input);
		result.timings.kernel_build_time_us = ExecutionRegionPlannerElapsedMicros(instantiate_start);
		if (!result.kernel) {
			throw InternalException(
			    "execution region backend \"%s\" could not instantiate its artifact for the advertised cache key",
			    backend_name);
		}
		result.artifact.reset();
		ValidateExecutionRegionCompileResult(backend_name, candidate, compiled_region.lowering_plan, string(), result);
		if (artifact_reservation.IsBuilder()) {
			auto cached = make_shared_ptr<ExecutionRegionCachedArtifact>();
			cached->artifact = std::move(artifact);
			cached->execution_mode = result.execution_mode;
			cached->compile_reason = result.reason;
			cached->ir = result.ir;
			execution_region_manager.artifact_cache.Publish(artifact_reservation, std::move(cached));
		}
	} else if (artifact_reservation.IsBuilder()) {
		execution_region_manager.artifact_cache.Abort(artifact_reservation);
	}
	if (reused_artifact) {
		compile_time_us = 0;
	}
	stage_timings.codegen_time_us = compile_time_us;
	stage_timings.executable_build_time_us = result.timings.executable_build_time_us;
	stage_timings.machine_codegen_time_us = result.timings.machine_codegen_time_us;
	stage_timings.kernel_build_time_us = result.timings.kernel_build_time_us;

	idx_t code_size = result.kernel ? result.kernel->CodeSize() : 0;
	auto status = result.status;
	auto reason = AttachExecutionRegionCandidateReason(
	    candidate, ComposeExecutionRegionCompileEventReason(compiled_region.physical_runner, result.reason),
	    should_record_detailed_telemetry);
	auto execution_mode = result.execution_mode;
	auto trace_id = execution_region_manager.RecordEvent(
	    context, backend_name, status, execution_mode, reason, ExecutionRegionCompileResultBlockerCode(status),
	    &result.ir, compiled_region.decision_time_us, compile_time_us, code_size, &region_ir.pipeline_shape, &candidate,
	    compiled_region.physical_runner.SelectedRunner(), &stage_timings,
	    compiled_region.lowering_plan.SelectedSourceExecution(), compiled_region.lowering_plan.ScanFilterMode(),
	    &compiled_region.physical_runner.runner_cost);
	if (status == ExecutionRegionCompileStatus::COMPILED && result.kernel) {
		result.kernel->SetTraceInfo(trace_id, execution_mode, reason, compile_time_us, code_size);
		result.kernel->SetAdaptiveMeasurementCandidate(
		    ExecutionRegionAdaptiveMeasurementWithinBand(context, compiled_region.physical_runner.runner_cost));
		result.kernel->SetExecutionABI(candidate.abi);
		result.kernel->SetTraceSelectedSourceExecution(compiled_region.lowering_plan.SelectedSourceExecution());
		result.kernel->SetTraceScanFilterMode(compiled_region.lowering_plan.ScanFilterMode());
		if (ExecutionRegionSettings::TraceRuntime(context)) {
			result.kernel->SetTracePipeline(candidate, region_ir.pipeline_shape);
		}
	}
	if (status == ExecutionRegionCompileStatus::ERROR) {
		throw InvalidInputException("Execution region compilation failed: %s", reason);
	}
	if (result.kernel) {
		plan.kernel = std::move(result.kernel);
	}
}

} // namespace duckdb

#include "duckdb/execution/execution_region_planner.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/execution_region_admission.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

static string ComposeExecutionRegionCompileEventReason(const ExecutionRegionAdmissionDecision &admission,
                                                       const string &compile_reason) {
	if (admission.policy_decision != "auto" || admission.reason.empty()) {
		return compile_reason;
	}
	if (compile_reason.empty()) {
		return admission.reason;
	}
	return admission.reason + ";" + compile_reason;
}

static string AttachExecutionRegionCandidateReason(const ExecutionRegionCandidate &candidate, string reason) {
	if (!reason.empty()) {
		reason += ";";
	}
	reason += "candidate_id=" + std::to_string(candidate.candidate_id);
	reason += ";candidate_shape=" + candidate.shape;
	return reason;
}

static ExecutionRegionAdmissionInfo
BuildExecutionRegionDecisionIdentity(const ExecutionRegionLoweringPlan &lowering_plan) {
	ExecutionRegionAdmissionInfo result;
	if (lowering_plan.shape_key.empty()) {
		return result;
	}
	result.has_admission = true;
	result.admission_key = lowering_plan.shape_key;
	return result;
}

static int64_t ExecutionRegionPlannerElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static int64_t ExecutionRegionPlannerSharedDecisionTime(int64_t total_time_us, idx_t entry_count, idx_t entry_index) {
	if (entry_count == 0 || total_time_us <= 0) {
		return 0;
	}
	auto entry_count_i = NumericCast<int64_t>(entry_count);
	auto shared_time = total_time_us / entry_count_i;
	if (NumericCast<int64_t>(entry_index) < total_time_us % entry_count_i) {
		shared_time++;
	}
	return shared_time;
}

static bool ExecutionRegionHasAutoAdmissionRules(const ExecutionRegionManager &manager, const string &backend_name,
                                                 const ExecutionRegionBackend &backend,
                                                 ExecutionRegionCompileTarget target) {
	return backend.HasAutoAdmissionRules(target) || manager.HasAdmissionProfileRules(backend_name, target);
}

static bool GetExecutionRegionAutoAdmissionRule(const ExecutionRegionManager &manager, const string &backend_name,
                                                const ExecutionRegionBackend &backend,
                                                const ExecutionRegionPipelineInventory &inventory,
                                                ExecutionRegionAdmissionRule &rule) {
	if (backend.GetAutoAdmissionRule(ExecutionRegionCompileTarget::REGION, inventory, rule)) {
		return true;
	}
	auto admission_key = BuildExecutionRegionAdmissionShapeKey(backend_name, inventory);
	admission_key = BuildExecutionRegionAdmissionContextShapeKey(inventory, admission_key);
	return manager.GetAdmissionProfileRule(backend_name, ExecutionRegionCompileTarget::REGION, admission_key, rule);
}

static string BuildExecutionRegionInventoryAdmissionKey(const string &backend_name,
                                                        const ExecutionRegionPipelineInventory &inventory) {
	auto admission_key = BuildExecutionRegionAdmissionShapeKey(backend_name, inventory);
	return BuildExecutionRegionAdmissionContextShapeKey(inventory, admission_key);
}

static bool GetExecutionRegionAutoAdmissionRule(const ExecutionRegionManager &manager, const string &backend_name,
                                                const ExecutionRegionBackend &backend,
                                                const ExecutionRegionCandidate &candidate,
                                                ExecutionRegionAdmissionRule &rule) {
	if (backend.GetAutoAdmissionRule(ExecutionRegionCompileTarget::REGION, candidate, rule)) {
		return true;
	}
	auto admission_key = BuildExecutionRegionAdmissionShapeKey(backend_name, candidate.signature);
	admission_key = BuildExecutionRegionAdmissionContextShapeKey(candidate.signature, admission_key);
	return manager.GetAdmissionProfileRule(backend_name, ExecutionRegionCompileTarget::REGION, admission_key, rule);
}

static bool GetExecutionRegionAutoAdmissionRule(const ExecutionRegionManager &manager, const string &backend_name,
                                                const ExecutionRegionBackend &backend,
                                                const ExecutionRegionCandidate &candidate,
                                                const ExecutionRegionLoweringPlan &lowering_plan,
                                                ExecutionRegionAdmissionRule &rule) {
	if (backend.GetAutoAdmissionRule(ExecutionRegionCompileTarget::REGION, candidate, lowering_plan, rule)) {
		return true;
	}
	if (lowering_plan.shape_key.empty()) {
		return false;
	}
	return manager.GetAdmissionProfileRule(backend_name, ExecutionRegionCompileTarget::REGION, lowering_plan.shape_key,
	                                       rule);
}

static string DescribeExecutionRegionLoweringRejection(const ExecutionRegionGraph &graph,
                                                       const ExecutionRegionPipelineInventory *inventory) {
	string reason = "core region lowering did not produce typed region IR";
	if (!inventory) {
		reason += ";graph_blocker=no-pipeline-inventory";
		reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
		return reason;
	}
	if (!inventory->workload_relevant) {
		reason += ";graph_blocker=not-workload-relevant";
		if (!inventory->workload_relevance_reason.empty()) {
			reason += ";workload_relevance_reason=" + inventory->workload_relevance_reason;
		}
	} else {
		reason += ";graph_blocker=typed-region-builder-returned-null";
	}
	reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
	reason += ";pipeline_shape=" + inventory->pipeline_shape;
	reason += ";candidate_shape=" + inventory->candidate_shape;
	reason += ";feature_shape=" + inventory->feature_shape;
	reason += ";estimated_cardinality=" + std::to_string(inventory->estimated_cardinality);
	return reason;
}

static ExecutionRegionAdmissionDecision
AdmitExecutionRegion(const ExecutionRegionManager &manager, const string &backend_name,
                     const ExecutionRegionBackend &backend, ExecutionRegionPolicyMode policy,
                     const ExecutionRegionCandidate &candidate, const ExecutionRegionLoweringPlan &lowering_plan) {
	ExecutionRegionAdmissionDecision decision;
	decision.policy_decision = ExecutionRegionPolicyModeToString(policy);
	decision.info.has_admission = true;
	decision.info.admission_key = lowering_plan.shape_key;
	auto estimated_work = candidate.estimated_cardinality;
	ExecutionRegionAdmissionRule candidate_admission_rule;
	auto has_candidate_admission_rule =
	    GetExecutionRegionAutoAdmissionRule(manager, backend_name, backend, candidate, candidate_admission_rule);
	if (has_candidate_admission_rule) {
		decision.info.rule_present = true;
		decision.info.min_cardinality = candidate_admission_rule.min_cardinality;
		decision.info.proof = candidate_admission_rule.proof;
		decision.info.has_score = true;
		decision.info.score =
		    NumericCast<int64_t>(estimated_work) - NumericCast<int64_t>(candidate_admission_rule.min_cardinality);
	}
	ExecutionRegionAdmissionRule lowered_admission_rule;
	auto has_lowered_admission_rule = GetExecutionRegionAutoAdmissionRule(manager, backend_name, backend, candidate,
	                                                                      lowering_plan, lowered_admission_rule);
	if (has_lowered_admission_rule) {
		decision.info.rule_present = true;
		decision.info.admission_key = lowered_admission_rule.admission_key;
		decision.info.min_cardinality = lowered_admission_rule.min_cardinality;
		decision.info.proof = lowered_admission_rule.proof;
		decision.info.has_score = true;
		decision.info.score =
		    NumericCast<int64_t>(estimated_work) - NumericCast<int64_t>(lowered_admission_rule.min_cardinality);
	}
	auto region_execution_form = lowering_plan.ExpectedRegionExecutionForm();
	if (region_execution_form != ExecutionRegionForm::FUSED) {
		decision.reason = "execution_region_policy=" + string(ExecutionRegionPolicyModeToString(policy)) +
		                  " skips region kernel because region execution form is not fused";
		decision.reason += ";region_execution_form=" + string(ExecutionRegionFormToString(region_execution_form));
		decision.reason += ";requires=fused;shape=" + decision.info.admission_key;
		decision.reason += ";" + lowering_plan.EventReason();
		if (has_candidate_admission_rule) {
			decision.reason += ";min_cardinality=" + std::to_string(candidate_admission_rule.min_cardinality);
			decision.reason += ";proof=" + candidate_admission_rule.proof;
		} else {
			decision.reason += ";admission_rule=missing";
		}
		return decision;
	}
	if (policy == ExecutionRegionPolicyMode::FORCE) {
		decision.compile = true;
		decision.reason = "execution_region_policy=force admits supported fused region kernel";
		return decision;
	}

	D_ASSERT(policy == ExecutionRegionPolicyMode::AUTO);
	if (has_lowered_admission_rule && estimated_work >= lowered_admission_rule.min_cardinality) {
		decision.compile = true;
		decision.reason = "execution_region_policy=auto admits shape=" + decision.info.admission_key +
		                  ";estimated_cardinality=" + std::to_string(estimated_work) +
		                  ";proof=" + lowered_admission_rule.proof;
		return decision;
	}

	decision.reason = "execution_region_policy=auto skips region kernel without measured auto admission";
	if (!decision.info.admission_key.empty()) {
		decision.reason += ": shape=" + decision.info.admission_key;
	}
	decision.reason += ";estimated_cardinality=" + std::to_string(estimated_work);
	if (has_lowered_admission_rule) {
		decision.reason += ";min_cardinality=" + std::to_string(lowered_admission_rule.min_cardinality);
		decision.reason += ";proof=" + lowered_admission_rule.proof;
	} else {
		decision.info.rule_present = false;
		decision.info.min_cardinality = 0;
		decision.info.proof.clear();
		decision.info.has_score = false;
		decision.info.score = 0;
		decision.reason += ";admission_rule=missing";
	}
	decision.reason += ";";
	decision.reason += lowering_plan.EventReason();
	return decision;
}

static void PopulateExecutionRegionAdmissionInfo(ExecutionRegionAdmissionInfo &info,
                                                 const ExecutionRegionAdmissionRule &rule, idx_t estimated_work) {
	info.has_admission = true;
	info.admission_key = rule.admission_key;
	info.rule_present = true;
	info.min_cardinality = rule.min_cardinality;
	info.proof = rule.proof;
	info.has_score = true;
	info.score = NumericCast<int64_t>(estimated_work) - NumericCast<int64_t>(rule.min_cardinality);
}

static ExecutionRegionAdmissionDecision
AdmitExecutionRegionPipelineForLowering(const ExecutionRegionManager &manager, const string &backend_name,
                                        const ExecutionRegionBackend &backend,
                                        const ExecutionRegionPipelineInventory &inventory) {
	ExecutionRegionAdmissionDecision decision;
	decision.policy_decision = ExecutionRegionPolicyModeToString(ExecutionRegionPolicyMode::AUTO);
	decision.info.has_admission = true;
	decision.info.admission_key = BuildExecutionRegionInventoryAdmissionKey(backend_name, inventory);

	ExecutionRegionAdmissionRule admission_rule;
	auto has_admission_rule =
	    GetExecutionRegionAutoAdmissionRule(manager, backend_name, backend, inventory, admission_rule);
	if (!has_admission_rule) {
		decision.reason = "execution_region_policy=auto skips pipeline before region lowering without measured auto "
		                  "admission: shape=" +
		                  decision.info.admission_key +
		                  ";estimated_cardinality=" + std::to_string(inventory.estimated_cardinality) +
		                  ";admission_rule=missing";
		return decision;
	}
	PopulateExecutionRegionAdmissionInfo(decision.info, admission_rule, inventory.estimated_cardinality);
	if (inventory.estimated_cardinality < admission_rule.min_cardinality) {
		decision.reason =
		    "execution_region_policy=auto skips pipeline before region lowering: estimated cardinality is below "
		    "measured auto admission threshold;shape=" +
		    admission_rule.admission_key + ";estimated_cardinality=" + std::to_string(inventory.estimated_cardinality) +
		    ";min_cardinality=" + std::to_string(admission_rule.min_cardinality) + ";proof=" + admission_rule.proof;
		return decision;
	}
	decision.compile = true;
	decision.reason =
	    "execution_region_policy=auto admits pipeline for region lowering: shape=" + admission_rule.admission_key +
	    ";estimated_cardinality=" + std::to_string(inventory.estimated_cardinality) + ";proof=" + admission_rule.proof;
	return decision;
}

static bool ExecutionRegionCandidatesOverlap(const ExecutionRegionCandidate &left,
                                             const ExecutionRegionCandidate &right) {
	return left.start_operator_index < right.end_operator_index && right.start_operator_index < left.end_operator_index;
}

struct ExecutionRegionAnalyzedCandidate {
	idx_t candidate_index = 0;
	const ExecutionRegionCandidate *candidate = nullptr;
	ExecutionRegionLoweringPlan lowering_plan;
	ExecutionRegionAdmissionDecision admission;
	ExecutionRegionStageTimings stage_timings;
	int64_t decision_time_us = 0;
};

struct ExecutionRegionPlanner::SelectedCandidate {
	idx_t candidate_index = 0;
	ExecutionRegionLoweringPlan lowering_plan;
	ExecutionRegionAdmissionDecision admission;
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

static string ValidateExecutionRegionFusedStageContract(const ExecutionRegionCandidate &candidate,
                                                        const ExecutionRegionLoweringPlan &lowering_plan) {
	if (lowering_plan.ExpectedRegionExecutionForm() != ExecutionRegionForm::FUSED) {
		return string();
	}
	if (!ExecutionRegionContractHasOnlyNativeStages(candidate.contract)) {
		return "backend advertised fused region but core contract still has boundaries;" + candidate.contract.ir;
	}
	if (!candidate.stage_plan.HasStages()) {
		return "backend advertised fused region but core operator-stage plan is empty";
	}
	bool has_executable_stage = false;
	for (auto &stage : candidate.stage_plan.stages) {
		if (ExecutionRegionStageExecutionIsFusionBlocker(stage.execution)) {
			return "backend advertised fused region across core boundary stage;" +
			       DescribeExecutionRegionStageFusionBlocker(stage);
		}
		has_executable_stage = has_executable_stage || ExecutionRegionStageExecutionIsFused(stage.execution);
	}
	if (!has_executable_stage) {
		return "backend advertised fused region but core operator-stage plan has no generated or native stage";
	}
	return string();
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
			return decision;
		}
		auto &node = region_ir.nodes[stage.node_index];
		if (!node.operator_info) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage has no operator contract";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			return decision;
		}
		if (stage.operator_index >= operators.size()) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage points outside pipeline operators";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
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
		decision.reason = "native operator state is not executable at region admission";
		decision.reason += ";readiness_status=";
		decision.reason += ExecutionOperatorReadinessStatusToString(readiness.status);
		decision.reason += ";readiness_kind=";
		decision.reason += ExecutionRegionOperatorContractKindToString(readiness.kind);
		decision.reason += ";readiness_blocker=";
		decision.reason += readiness.blocker.empty() ? "unknown" : readiness.blocker;
		decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
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

static int64_t ExecutionRegionCandidateAdmissionScore(const ExecutionRegionAnalyzedCandidate &entry) {
	if (!entry.admission.info.has_score) {
		return 0;
	}
	return entry.admission.info.score;
}

static idx_t ExecutionRegionCandidateOperatorCoverage(const ExecutionRegionAnalyzedCandidate &entry) {
	D_ASSERT(entry.candidate);
	idx_t result = 0;
	if (entry.candidate->end_operator_index > entry.candidate->start_operator_index) {
		result = entry.candidate->end_operator_index - entry.candidate->start_operator_index;
	}
	if (ExecutionRegionABIOwnsSource(entry.candidate->contract.abi)) {
		result++;
	}
	if (ExecutionRegionABIOwnsSink(entry.candidate->contract.abi)) {
		result++;
	}
	return result;
}

static bool ExecutionRegionSelectionLexicographicallyBefore(const vector<idx_t> &left, const vector<idx_t> &right) {
	return std::lexicographical_compare(left.begin(), left.end(), right.begin(), right.end());
}

static bool ExecutionRegionCandidatesDoNotOverlap(const ExecutionRegionCandidate &left,
                                                  const ExecutionRegionCandidate &right) {
	if (left.end_operator_index < right.start_operator_index) {
		return true;
	}
	if (left.end_operator_index > right.start_operator_index) {
		return false;
	}
	if (ExecutionRegionABIOwnsSource(left.contract.abi) && ExecutionRegionABIOwnsSource(right.contract.abi)) {
		return false;
	}
	if (ExecutionRegionABIOwnsSink(left.contract.abi) && ExecutionRegionABIOwnsSink(right.contract.abi)) {
		return false;
	}
	return true;
}

struct ExecutionRegionAutoSelectionState {
	bool valid = false;
	int64_t score = 0;
	idx_t covered_operators = 0;
	int64_t native_ownership_score = 0;
	idx_t candidate_count = 0;
	vector<idx_t> eligible_indices;
};

static bool ExecutionRegionSelectionBetter(const ExecutionRegionAutoSelectionState &left,
                                           const ExecutionRegionAutoSelectionState &right) {
	if (!left.valid) {
		return false;
	}
	if (!right.valid) {
		return true;
	}
	if (left.score != right.score) {
		return left.score > right.score;
	}
	if (left.covered_operators != right.covered_operators) {
		return left.covered_operators > right.covered_operators;
	}
	if (left.native_ownership_score != right.native_ownership_score) {
		return left.native_ownership_score > right.native_ownership_score;
	}
	if (left.candidate_count != right.candidate_count) {
		return left.candidate_count < right.candidate_count;
	}
	return ExecutionRegionSelectionLexicographicallyBefore(left.eligible_indices, right.eligible_indices);
}

static int64_t ExecutionRegionCandidateNativeOwnershipScore(const ExecutionRegionAnalyzedCandidate &entry) {
	D_ASSERT(entry.candidate);
	auto &candidate = *entry.candidate;
	if (entry.lowering_plan.ExpectedRegionExecutionForm() != ExecutionRegionForm::FUSED ||
	    entry.lowering_plan.SelectedSourceExecution() != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
		return 0;
	}
	if (!ExecutionRegionABIOwnsSource(candidate.contract.abi) ||
	    candidate.traits.source_execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT ||
	    candidate.contract.source_ownership != ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
		return 0;
	}
	int64_t result = NumericCast<int64_t>(candidate.estimated_cardinality);
	if (result == 0) {
		result = 1;
	}
	if (ExecutionRegionABIOwnsSink(candidate.contract.abi) &&
	    candidate.contract.sink_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
		result += 1;
	}
	if (candidate.contract.transform_ownership == ExecutionRegionOwnershipKind::GENERATED_IR ||
	    candidate.contract.transform_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
		result += 1;
	}
	return result;
}

template <class SCORE_FUNC>
static vector<bool>
SelectExecutionRegionCandidatesByScore(const vector<ExecutionRegionAnalyzedCandidate> &eligible_candidates,
                                       SCORE_FUNC score_func) {
	vector<bool> selected(eligible_candidates.size(), false);
	if (eligible_candidates.empty()) {
		return selected;
	}

	vector<idx_t> sorted_indices;
	sorted_indices.reserve(eligible_candidates.size());
	for (idx_t eligible_idx = 0; eligible_idx < eligible_candidates.size(); eligible_idx++) {
		sorted_indices.push_back(eligible_idx);
	}
	std::sort(sorted_indices.begin(), sorted_indices.end(), [&](idx_t left_idx, idx_t right_idx) {
		auto &left = *eligible_candidates[left_idx].candidate;
		auto &right = *eligible_candidates[right_idx].candidate;
		if (left.end_operator_index != right.end_operator_index) {
			return left.end_operator_index < right.end_operator_index;
		}
		if (left.start_operator_index != right.start_operator_index) {
			return left.start_operator_index < right.start_operator_index;
		}
		return left.candidate_id < right.candidate_id;
	});

	vector<idx_t> previous_non_overlapping(sorted_indices.size(), DConstants::INVALID_INDEX);
	for (idx_t sorted_idx = 0; sorted_idx < sorted_indices.size(); sorted_idx++) {
		auto &candidate = *eligible_candidates[sorted_indices[sorted_idx]].candidate;
		for (idx_t previous_idx = sorted_idx; previous_idx > 0; previous_idx--) {
			auto &previous_candidate = *eligible_candidates[sorted_indices[previous_idx - 1]].candidate;
			if (ExecutionRegionCandidatesDoNotOverlap(previous_candidate, candidate)) {
				previous_non_overlapping[sorted_idx] = previous_idx - 1;
				break;
			}
		}
	}

	vector<ExecutionRegionAutoSelectionState> dp(sorted_indices.size() + 1);
	dp[0].valid = true;
	for (idx_t sorted_idx = 0; sorted_idx < sorted_indices.size(); sorted_idx++) {
		auto exclude = dp[sorted_idx];
		ExecutionRegionAutoSelectionState include;
		auto previous_idx = previous_non_overlapping[sorted_idx];
		if (previous_idx == DConstants::INVALID_INDEX) {
			include = dp[0];
		} else {
			include = dp[previous_idx + 1];
		}
		if (include.valid) {
			auto eligible_idx = sorted_indices[sorted_idx];
			include.score += score_func(eligible_candidates[eligible_idx]);
			include.covered_operators += ExecutionRegionCandidateOperatorCoverage(eligible_candidates[eligible_idx]);
			include.native_ownership_score +=
			    ExecutionRegionCandidateNativeOwnershipScore(eligible_candidates[eligible_idx]);
			include.candidate_count++;
			include.eligible_indices.push_back(eligible_idx);
		}
		dp[sorted_idx + 1] = ExecutionRegionSelectionBetter(include, exclude) ? std::move(include) : std::move(exclude);
	}

	for (auto eligible_idx : dp.back().eligible_indices) {
		selected[eligible_idx] = true;
	}
	return selected;
}

static vector<bool>
SelectExecutionRegionAutoCandidates(const vector<ExecutionRegionAnalyzedCandidate> &eligible_candidates) {
	return SelectExecutionRegionCandidatesByScore(eligible_candidates, ExecutionRegionCandidateAdmissionScore);
}

static int64_t ExecutionRegionCandidateForceScore(const ExecutionRegionAnalyzedCandidate &entry) {
	D_ASSERT(entry.candidate);
	auto &candidate = *entry.candidate;
	int64_t result = NumericCast<int64_t>(entry.lowering_plan.NativeCount());
	result += NumericCast<int64_t>(candidate.contract.generated_operator_count);
	result += NumericCast<int64_t>(candidate.traits.filter_count + candidate.traits.projection_count);
	return result;
}

static vector<bool>
SelectExecutionRegionForceCandidates(const vector<ExecutionRegionAnalyzedCandidate> &eligible_candidates) {
	return SelectExecutionRegionCandidatesByScore(eligible_candidates, ExecutionRegionCandidateForceScore);
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
			contract.input_types = node.output_types;
			contract.source_execution =
			    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
			            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
			            native_fused_source_owner
			        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
			        : ExecutionRegionSourceExecutionKind::NONE;
			contract.source_filter_ownership = ExecutionRegionSourceFilterOwnershipKind::NONE;
			return;
		}
		if (!source.table_scan_contract.present) {
			continue;
		}
		auto &table_scan_contract = source.table_scan_contract;
		auto &plan_contract = plan.source_open_request;
		plan_contract.present = true;
		plan_contract.input_types = table_scan_contract.source_contract_input_types;
		plan_contract.source_execution =
		    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
		            native_fused_source_owner
		        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		        : ExecutionRegionSourceExecutionKind::NONE;
		plan_contract.source_filter_ownership = !source.filters.empty() && native_fused_source_owner
		                                            ? lowering_plan.SourceFilterOwnership()
		                                            : ExecutionRegionSourceFilterOwnershipKind::NONE;
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
	auto policy = execution_region_manager.GetPolicy(context);
	auto policy_decision = string(ExecutionRegionPolicyModeToString(policy));
	if (policy == ExecutionRegionPolicyMode::OFF) {
		execution_region_manager.RecordEvent(context, "policy", ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::DISABLED, ExecutionRegionExecutionMode::NONE,
		                                     "off", "execution_region_policy=off", string(), 0, 0, 0);
		return nullptr;
	}

	string backend_name;
	auto backend = execution_region_manager.SelectBackend(context, backend_name);
	plan->backend_name = backend_name;
	if (!backend) {
		execution_region_manager.RecordEvent(context, std::move(backend_name), ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::UNAVAILABLE,
		                                     ExecutionRegionExecutionMode::NONE, policy_decision,
		                                     "no available execution region backend", string(), 0, 0, 0);
		return nullptr;
	}
	if (!backend->SupportsRegions()) {
		execution_region_manager.RecordEvent(context, backend_name, ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::UNSUPPORTED,
		                                     ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
		                                     "backend does not compile regions", string(), 0, 0, 0);
		return nullptr;
	}
	auto has_auto_admission_rules = ExecutionRegionHasAutoAdmissionRules(
	    execution_region_manager, backend_name, *backend, ExecutionRegionCompileTarget::REGION);
	if (policy == ExecutionRegionPolicyMode::AUTO && !has_auto_admission_rules) {
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
		    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
		    "execution_region_policy=auto skips pipeline before graph lowering: backend has no measured auto admission "
		    "policy",
		    string(), 0, 0, 0);
		return nullptr;
	}
	auto pipeline_descriptor = BuildExecutionRegionGraph(pipeline);
	if (!pipeline_descriptor) {
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
		    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
		    "core region graph builder produced no execution-region graph", string(), 0, 0, 0);
		return nullptr;
	}
	if (policy == ExecutionRegionPolicyMode::AUTO) {
		auto inventory_start = std::chrono::steady_clock::now();
		auto inventory =
		    TryInspectExecutionRegionPipeline(*pipeline_descriptor, ExecutionRegionPipelineInventoryMode::ADMISSION);
		if (inventory) {
			ExecutionRegionStageTimings stage_timings;
			stage_timings.admission_time_us = ExecutionRegionPlannerElapsedMicros(inventory_start);
			auto admission =
			    AdmitExecutionRegionPipelineForLowering(execution_region_manager, backend_name, *backend, *inventory);
			if (!admission.compile) {
				if (ExecutionRegionSettings::DumpIR(context)) {
					auto trace_inventory = TryInspectExecutionRegionPipeline(
					    *pipeline_descriptor, ExecutionRegionPipelineInventoryMode::TRACE);
					if (trace_inventory && trace_inventory->workload_relevant) {
						inventory = std::move(trace_inventory);
					}
				}
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
				    ExecutionRegionExecutionMode::UNSUPPORTED, admission.policy_decision, admission.reason,
				    inventory->ir, stage_timings.admission_time_us, 0, 0, nullptr, &admission.info, &stage_timings,
				    ExecutionRegionForm::NONE, ExecutionRegionSourceExecutionKind::NONE, inventory.get());
				return nullptr;
			}
		}
	}
	auto region_decision_start = std::chrono::steady_clock::now();
	auto region_ir = TryLowerExecutionRegion(*pipeline_descriptor);
	auto region_lowering_time_us = ExecutionRegionPlannerElapsedMicros(region_decision_start);
	if (!region_ir) {
		auto rejected_inventory =
		    TryInspectExecutionRegionPipeline(*pipeline_descriptor, ExecutionRegionPipelineInventoryMode::TRACE);
		auto rejected_ir = rejected_inventory ? rejected_inventory->ir : string();
		auto rejected_reason = DescribeExecutionRegionLoweringRejection(*pipeline_descriptor, rejected_inventory.get());
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
		    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision, std::move(rejected_reason),
		    std::move(rejected_ir), region_lowering_time_us, 0, 0, nullptr, nullptr, nullptr, ExecutionRegionForm::NONE,
		    ExecutionRegionSourceExecutionKind::NONE,
		    rejected_inventory && rejected_inventory->workload_relevant ? rejected_inventory.get() : nullptr);
		return nullptr;
	}
	auto &lowered_region = *region_ir;
	if (lowered_region.candidates.empty()) {
		execution_region_manager.RecordEvent(
		    context, std::move(backend_name), ExecutionRegionCompileTarget::REGION,
		    ExecutionRegionCompileStatus::UNSUPPORTED, ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
		    "core region lowering produced no candidates", lowered_region.ir, region_lowering_time_us, 0, 0);
		return nullptr;
	}

	vector<ExecutionRegionAnalyzedCandidate> eligible_candidates;
	for (idx_t candidate_index = 0; candidate_index < lowered_region.candidates.size(); candidate_index++) {
		auto &candidate = lowered_region.candidates[candidate_index];
		auto candidate_decision_start = std::chrono::steady_clock::now();
		auto shared_decision_time_us = ExecutionRegionPlannerSharedDecisionTime(
		    region_lowering_time_us, lowered_region.candidates.size(), candidate_index);
		ExecutionRegionStageTimings stage_timings;
		stage_timings.ir_lowering_time_us = shared_decision_time_us;
		auto candidate_decision_time_us = [&]() {
			return shared_decision_time_us + ExecutionRegionPlannerElapsedMicros(candidate_decision_start);
		};
		if (ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
			string full_pipeline_entry_reason;
			if (!ExecutionRegionRuntimeCanEnter(pipeline, full_pipeline_entry_reason)) {
				auto decision_time_us = candidate_decision_time_us();
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
				    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
				    AttachExecutionRegionCandidateReason(candidate, std::move(full_pipeline_entry_reason)),
				    lowered_region.ir, decision_time_us, 0, 0, &candidate, nullptr, &stage_timings);
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
			auto lowering_reason = lowering_plan.EventReason();
			if (!lowering_reason.empty()) {
				empty_analysis_reason += ";";
				empty_analysis_reason += lowering_reason;
			}
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, std::move(empty_analysis_reason)), lowered_region.ir,
			    decision_time_us, 0, 0, &candidate, nullptr, &stage_timings,
			    lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}
		if (lowering_plan.ExpectedCompiledExecutionMode() == ExecutionRegionExecutionMode::UNSUPPORTED) {
			auto decision_time_us = candidate_decision_time_us();
			auto unsupported_reason = lowering_plan.EventReason();
			auto decision_identity = BuildExecutionRegionDecisionIdentity(lowering_plan);
			if (lowering_plan.NativeCount() > 0) {
				unsupported_reason +=
				    ";execution:unsupported;backend cannot generate executable code for this whole region";
			} else {
				unsupported_reason = "region lowering contains no native executable nodes: " + unsupported_reason +
				                     ";execution:unsupported";
			}
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, std::move(unsupported_reason)), lowered_region.ir,
			    decision_time_us, 0, 0, &candidate, &decision_identity, &stage_timings,
			    lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}
		auto fused_contract_blocker = ValidateExecutionRegionFusedStageContract(candidate, lowering_plan);
		if (!fused_contract_blocker.empty()) {
			auto decision_time_us = candidate_decision_time_us();
			auto reason = lowering_plan.EventReason() + ";execution:unsupported;" + std::move(fused_contract_blocker);
			auto decision_identity = BuildExecutionRegionDecisionIdentity(lowering_plan);
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason)), lowered_region.ir, decision_time_us,
			    0, 0, &candidate, &decision_identity, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution());
			continue;
		}
		auto readiness_decision =
		    ValidateExecutionRegionNativeOperatorReadiness(context, pipeline, lowered_region, candidate);
		if (!readiness_decision.ready) {
			auto decision_time_us = candidate_decision_time_us();
			if (readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED) {
				plan->operator_readiness_refresh = true;
			}
			auto reason = lowering_plan.EventReason() + ";";
			reason += readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED ? "execution:state-not-ready;"
			                                                                             : "execution:unsupported;";
			reason += std::move(readiness_decision.reason);
			auto decision_identity = BuildExecutionRegionDecisionIdentity(lowering_plan);
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, readiness_decision.status,
			    ExecutionRegionExecutionMode::UNSUPPORTED, policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason)), lowered_region.ir, decision_time_us,
			    0, 0, &candidate, &decision_identity, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution());
			continue;
		}
		auto admission_start = std::chrono::steady_clock::now();
		auto admission =
		    AdmitExecutionRegion(execution_region_manager, backend_name, *backend, policy, candidate, lowering_plan);
		stage_timings.admission_time_us += ExecutionRegionPlannerElapsedMicros(admission_start);
		if (!admission.compile) {
			auto decision_time_us = candidate_decision_time_us();
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, admission.policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, admission.reason), lowered_region.ir, decision_time_us,
			    0, 0, &candidate, &admission.info, &stage_timings, lowering_plan.ExpectedRegionExecutionForm(),
			    lowering_plan.SelectedSourceExecution());
			continue;
		}

		ExecutionRegionAnalyzedCandidate analyzed_candidate;
		analyzed_candidate.candidate_index = candidate_index;
		analyzed_candidate.candidate = &candidate;
		analyzed_candidate.lowering_plan = std::move(lowering_plan);
		analyzed_candidate.admission = std::move(admission);
		analyzed_candidate.stage_timings = stage_timings;
		analyzed_candidate.decision_time_us = candidate_decision_time_us();
		eligible_candidates.push_back(std::move(analyzed_candidate));
	}

	auto selection_start = std::chrono::steady_clock::now();
	auto selected_candidates = policy == ExecutionRegionPolicyMode::AUTO
	                               ? SelectExecutionRegionAutoCandidates(eligible_candidates)
	                               : SelectExecutionRegionForceCandidates(eligible_candidates);
	auto selection_time_us = ExecutionRegionPlannerElapsedMicros(selection_start);
	vector<SelectedCandidate> selected_regions;
	for (idx_t eligible_idx = 0; eligible_idx < eligible_candidates.size(); eligible_idx++) {
		auto &entry = eligible_candidates[eligible_idx];
		auto &candidate = *entry.candidate;
		entry.stage_timings.overlap_check_time_us =
		    ExecutionRegionPlannerSharedDecisionTime(selection_time_us, eligible_candidates.size(), eligible_idx);
		auto decision_time_us = entry.decision_time_us + entry.stage_timings.overlap_check_time_us;
		if (!selected_candidates[eligible_idx]) {
			auto reason = policy == ExecutionRegionPolicyMode::AUTO
			                  ? "region candidate not selected by auto non-overlapping admission selection"
			                  : "region candidate overlaps an already selected non-overlapping region";
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::SKIPPED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, entry.admission.policy_decision,
			    AttachExecutionRegionCandidateReason(candidate, std::move(reason)), lowered_region.ir, decision_time_us,
			    0, 0, &candidate, &entry.admission.info, &entry.stage_timings,
			    entry.lowering_plan.ExpectedRegionExecutionForm(), entry.lowering_plan.SelectedSourceExecution());
			continue;
		}

		SelectedCandidate selected_region;
		selected_region.candidate_index = entry.candidate_index;
		selected_region.lowering_plan = std::move(entry.lowering_plan);
		selected_region.admission = std::move(entry.admission);
		selected_region.stage_timings = entry.stage_timings;
		selected_region.decision_time_us = decision_time_us;
		AccumulateExecutionRegionOpenRequest(*plan, lowered_region, candidate, selected_region.lowering_plan);
		selected_regions.push_back(std::move(selected_region));
	}
	Compile(context, *plan, lowered_region, selected_regions);
	return KeepExecutableExecutionRegionPlan(std::move(plan));
}

void ExecutionRegionPlanner::Compile(ClientContext &context, ExecutionRegionPlan &plan, ExecutionRegionIR &region_ir,
                                     vector<ExecutionRegionPlanner::SelectedCandidate> &selected_regions) {
	plan.kernels.clear();
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::Enabled(context) ||
	    selected_regions.empty()) {
		return;
	}
	auto plan_policy_decision = selected_regions[0].admission.policy_decision;
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	string backend_name;
	auto backend = execution_region_manager.SelectBackend(context, backend_name);
	if (!backend) {
		execution_region_manager.RecordEvent(context, std::move(backend_name), ExecutionRegionCompileTarget::REGION,
		                                     ExecutionRegionCompileStatus::UNAVAILABLE,
		                                     ExecutionRegionExecutionMode::NONE, plan_policy_decision,
		                                     "no available execution region backend", string(), 0, 0, 0);
		return;
	}
	if (backend_name != plan.backend_name) {
		string reason = "compiled pipeline backend changed before executable plan materialization";
		reason += ";plan_backend=";
		reason += plan.backend_name;
		reason += ";selected_backend=";
		reason += backend_name;
		execution_region_manager.RecordEvent(
		    context, backend_name, ExecutionRegionCompileTarget::REGION, ExecutionRegionCompileStatus::UNAVAILABLE,
		    ExecutionRegionExecutionMode::NONE, plan_policy_decision, std::move(reason), string(), 0, 0, 0);
		return;
	}
	for (auto &compiled_region : selected_regions) {
		auto &candidate = region_ir.candidates[compiled_region.candidate_index];
		auto stage_timings = compiled_region.stage_timings;
		ExecutionRegionCompilationInput input(context, region_ir, candidate);
		input.lowering_plan = &compiled_region.lowering_plan;
		auto start = std::chrono::steady_clock::now();
		auto result = backend->CompileRegion(input);
		auto compile_time_us = ExecutionRegionPlannerElapsedMicros(start);
		stage_timings.codegen_time_us = compile_time_us;

		idx_t code_size = result.kernel ? result.kernel->CodeSize() : 0;
		auto status = result.status;
		auto reason = AttachExecutionRegionCandidateReason(
		    candidate, ComposeExecutionRegionCompileEventReason(compiled_region.admission, result.reason));
		auto execution_mode = result.execution_mode;
		auto ir = result.ir;
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
		    compiled_region.admission.policy_decision, reason, ir, compiled_region.decision_time_us, compile_time_us,
		    code_size, &candidate, &compiled_region.admission.info, &stage_timings,
		    compiled_region.lowering_plan.ExpectedRegionExecutionForm(),
		    compiled_region.lowering_plan.SelectedSourceExecution());
		if (status == ExecutionRegionCompileStatus::COMPILED && result.kernel) {
			result.kernel->SetTraceInfo(trace_id, execution_mode, reason, compile_time_us, code_size);
			result.kernel->SetTraceRegionExecutionForm(compiled_region.lowering_plan.ExpectedRegionExecutionForm());
			result.kernel->SetTraceSelectedSourceExecution(compiled_region.lowering_plan.SelectedSourceExecution());
			result.kernel->SetTraceCandidate(candidate);
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

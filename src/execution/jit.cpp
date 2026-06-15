#include "duckdb/execution/jit/manager.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/jit/lowering.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/jit/registration.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

JitManager::JitManager(DatabaseInstance &db) : db(db) {
}

void JitManager::RegisterBackend(unique_ptr<JitBackend> backend) {
	if (!backend) {
		throw InvalidInputException("Cannot register a NULL JIT backend");
	}
	auto name = StringUtil::Lower(backend->Name());
	if (name.empty()) {
		throw InvalidInputException("Cannot register a JIT backend with an empty name");
	}
	lock_guard<mutex> guard(lock);
	for (auto &entry : backends) {
		if (StringUtil::Lower(entry->Name()) == name) {
			throw InvalidInputException("JIT backend \"%s\" is already registered", name);
		}
	}
	backends.push_back(std::move(backend));
}

void RegisterJitBackend(DatabaseInstance &db, unique_ptr<JitBackend> backend) {
	JitManager::Get(db).RegisterBackend(std::move(backend));
}

JitPolicyMode JitManager::GetPolicy(ClientContext &context) const {
	auto policy = StringUtil::Lower(Settings::Get<JitPolicySetting>(context));
	if (policy == "auto") {
		return JitPolicyMode::AUTO;
	}
	if (policy == "force") {
		return JitPolicyMode::FORCE;
	}
	if (policy == "off") {
		return JitPolicyMode::OFF;
	}
	throw InvalidInputException("Invalid JIT policy \"%s\"", policy);
}

static bool ShouldRecordJitDecisionCounters(ClientContext &context) {
	return Settings::Get<JitTraceDecisionsSetting>(context) || Settings::Get<JitDumpIrSetting>(context) ||
	       Settings::Get<JitTraceRuntimeSetting>(context);
}

optional_ptr<JitBackend> JitManager::SelectBackend(ClientContext &context, string &backend_name) const {
	auto requested = StringUtil::Lower(Settings::Get<JitBackendSetting>(context));
	lock_guard<mutex> guard(lock);
	if (requested == "auto") {
		for (auto &backend : backends) {
			if (backend->IsAvailable()) {
				backend_name = backend->Name();
				return *backend;
			}
		}
		backend_name = "auto";
		return nullptr;
	}
	for (auto &backend : backends) {
		if (StringUtil::Lower(backend->Name()) != requested) {
			continue;
		}
		backend_name = backend->Name();
		if (!backend->IsAvailable()) {
			throw InvalidInputException("JIT backend \"%s\" is registered but not available", requested);
		}
		return *backend;
	}
	throw InvalidInputException("JIT backend \"%s\" is not registered", requested);
}

vector<JitBackendInfo> JitManager::GetBackends(ClientContext *context) const {
	string selected_name;
	if (context && Settings::Get<EnableJitSetting>(*context)) {
		try {
			SelectBackend(*context, selected_name);
		} catch (...) {
			selected_name.clear();
		}
	}
	selected_name = StringUtil::Lower(selected_name);

	lock_guard<mutex> guard(lock);
	vector<JitBackendInfo> result;
	result.reserve(backends.size());
	for (auto &backend : backends) {
		JitBackendInfo info;
		info.name = backend->Name();
		info.description = backend->Description();
		info.available = backend->IsAvailable();
		info.supports_regions = backend->SupportsRegions();
		info.selected = StringUtil::Lower(info.name) == selected_name;
		result.push_back(std::move(info));
	}
	return result;
}

vector<JitEvent> JitManager::GetEvents() const {
	return event_log.GetEvents();
}

vector<JitCounter> JitManager::GetCounters() const {
	return event_log.GetCounters();
}

vector<JitDecisionCounter> JitManager::GetDecisionCounters() const {
	return event_log.GetDecisionCounters();
}

vector<JitKernelCounter> JitManager::GetKernelCounters() const {
	return event_log.GetKernelCounters();
}

void JitManager::ClearEvents() {
	event_log.ClearEvents();
}

void JitManager::ClearCounters() {
	event_log.ClearCounters();
}

void JitManager::ApplyEventRetentionLimit(idx_t event_log_size) {
	event_log.ApplyRetentionLimit(event_log_size);
}

idx_t JitManager::RecordEvent(ClientContext &context, string backend_name, JitCompileTarget target,
                              JitCompileStatus status, JitExecutionMode execution_mode, string policy_decision,
                              string reason, string ir, int64_t decision_time_us, int64_t compile_time_us,
                              idx_t code_size, const JitRegionCandidate *candidate, const JitAdmissionInfo *admission,
                              const JitStageTimings *stage_timings, JitRegionExecutionForm region_execution_form,
                              JitRegionSourceExecutionKind selected_source_execution,
                              const JitRegionPipelineInventory *inventory) {
	if (context.IsJitSuppressed()) {
		return 0;
	}
	JitEvent event;
	if (inventory) {
		event.has_pipeline = true;
		event.pipeline_shape = inventory->pipeline_shape;
		event.pipeline_estimated_cardinality = inventory->estimated_cardinality;
	}
	if (candidate) {
		event.has_candidate = true;
		event.candidate_id = candidate->candidate_id;
		event.candidate_shape = candidate->shape;
		event.candidate_scope = JitRegionCandidateScopeToString(candidate->scope);
		event.candidate_pipeline_shape = candidate->pipeline_shape;
		event.candidate_context_pipeline_shape = candidate->context_pipeline_shape;
		event.candidate_node_count = candidate->node_count;
		event.candidate_start_operator_index = candidate->start_operator_index;
		event.candidate_end_operator_index = candidate->end_operator_index;
		event.candidate_estimated_cardinality = candidate->estimated_cardinality;
		event.candidate_traits = candidate->traits;
		event.candidate_contract = candidate->contract;
	}
	if (admission && admission->has_admission) {
		event.has_admission = true;
		event.admission_shape_key = admission->admission_key;
		event.admission_rule_present = admission->rule_present;
		event.admission_min_cardinality = admission->min_cardinality;
		event.admission_proof = admission->proof;
		event.has_admission_score = admission->has_score;
		event.admission_score = admission->score;
	}
	event.phase = status == JitCompileStatus::COMPILED || status == JitCompileStatus::ERROR ? "compile" : "decision";
	event.backend_name = std::move(backend_name);
	event.target = JitCompileTargetToString(target);
	event.status = JitCompileStatusToString(status);
	event.execution_mode = JitExecutionModeToString(execution_mode);
	event.region_execution_form = JitRegionExecutionFormToString(region_execution_form);
	event.selected_source_execution = selected_source_execution;
	event.policy_decision = std::move(policy_decision);
	event.reason = std::move(reason);
	event.ir = Settings::Get<JitDumpIrSetting>(context) ? std::move(ir) : string();
	event.decision_time_us = decision_time_us;
	event.compile_time_us = compile_time_us;
	event.code_size = code_size;
	if (stage_timings) {
		event.ir_lowering_time_us = stage_timings->ir_lowering_time_us;
		event.backend_analysis_time_us = stage_timings->backend_analysis_time_us;
		event.admission_time_us = stage_timings->admission_time_us;
		event.overlap_check_time_us = stage_timings->overlap_check_time_us;
		event.codegen_time_us = stage_timings->codegen_time_us;
	}

	auto event_log_size = Settings::Get<JitEventLogSizeSetting>(db);
	return event_log.Record(event_log_size, ShouldRecordJitDecisionCounters(context), std::move(event));
}

template <class KERNEL>
static JitEvent BuildRuntimeEvent(const KERNEL &kernel, JitCompileTarget target, JitExecutionMode execution_mode,
                                  string status, string reason, idx_t input_rows, idx_t output_rows,
                                  int64_t runtime_time_us, string runtime_result) {
	JitEvent event;
	event.phase = "runtime";
	event.backend_name = kernel.BackendName();
	event.target = JitCompileTargetToString(target);
	event.status = std::move(status);
	event.execution_mode = JitExecutionModeToString(execution_mode);
	event.region_execution_form = JitRegionExecutionFormToString(JitRegionExecutionForm::NONE);
	event.policy_decision = "runtime";
	event.reason = std::move(reason);
	event.kernel_id = kernel.TraceId();
	event.kernel_compile_reason = kernel.TraceCompileReason();
	event.kernel_compile_time_us = kernel.TraceCompileTime();
	event.kernel_code_size = kernel.TraceCodeSize();
	event.input_rows = input_rows;
	event.output_rows = output_rows;
	event.invocation_count = 1;
	event.runtime_time_us = runtime_time_us;
	event.runtime_result = std::move(runtime_result);
	return event;
}

static void SetRuntimeRegionCandidate(JitEvent &event, const JitRegionKernel &kernel) {
	if (!kernel.HasTraceCandidate()) {
		return;
	}
	event.has_candidate = true;
	event.candidate_id = kernel.TraceCandidateId();
	event.candidate_shape = kernel.TraceCandidateShape();
	event.candidate_scope = kernel.TraceCandidateScope();
	event.candidate_pipeline_shape = kernel.TraceCandidatePipelineShape();
	event.candidate_context_pipeline_shape = kernel.TraceCandidateContextPipelineShape();
	event.candidate_node_count = kernel.TraceCandidateNodeCount();
	event.candidate_start_operator_index = kernel.TraceCandidateStartOperatorIndex();
	event.candidate_end_operator_index = kernel.TraceCandidateEndOperatorIndex();
	event.candidate_estimated_cardinality = kernel.TraceCandidateEstimatedCardinality();
	event.candidate_traits = kernel.TraceCandidateTraits();
	event.candidate_contract = kernel.TraceCandidateContract();
}

static void SetRuntimeRegionExecutionForm(JitEvent &event, const JitRegionKernel &kernel) {
	event.region_execution_form = JitRegionExecutionFormToString(kernel.RegionExecutionForm());
	event.selected_source_execution = kernel.SelectedSourceExecution();
}

static void SetRuntimeMetrics(JitEvent &event, const JitRuntimeMetrics &metrics) {
	event.source_native_output_rows = metrics.source_native_output_rows;
	event.source_native_invocation_count = metrics.source_native_invocation_count;
	event.source_native_runtime_time_us = metrics.source_native_runtime_time_us;
	event.generated_body_runtime_time_us = metrics.generated_body_runtime_time_us;
	event.fused_prepare_runtime_time_us = metrics.fused_prepare_runtime_time_us;
	event.fused_group_runtime_time_us = metrics.fused_group_runtime_time_us;
	event.fused_state_bind_runtime_time_us = metrics.fused_state_bind_runtime_time_us;
	event.fused_update_runtime_time_us = metrics.fused_update_runtime_time_us;
	event.fused_finish_runtime_time_us = metrics.fused_finish_runtime_time_us;
	event.generated_body_flat_input_rows = metrics.generated_body_flat_input_rows;
	event.generated_body_flat_invocation_count = metrics.generated_body_flat_invocation_count;
	event.generated_body_shared_selection_input_rows = metrics.generated_body_shared_selection_input_rows;
	event.generated_body_shared_selection_invocation_count = metrics.generated_body_shared_selection_invocation_count;
	event.generated_body_selection_input_rows = metrics.generated_body_selection_input_rows;
	event.generated_body_selection_invocation_count = metrics.generated_body_selection_invocation_count;
	event.native_operator_loop_input_rows = metrics.native_operator_loop_input_rows;
	event.native_operator_loop_invocation_count = metrics.native_operator_loop_invocation_count;
}

void JitManager::RecordRuntimeEvent(ClientContext &context, const JitRegionKernel &kernel, JitCompileTarget target,
                                    string status, string reason, idx_t input_rows, idx_t output_rows,
                                    int64_t runtime_time_us, string runtime_result) {
	if (context.IsJitSuppressed() || !Settings::Get<JitTraceRuntimeSetting>(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, target, kernel.ExecutionMode(), std::move(status), std::move(reason),
	                               input_rows, output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeRegionExecutionForm(event, kernel);
	SetRuntimeRegionCandidate(event, kernel);
	auto event_log_size = Settings::Get<JitEventLogSizeSetting>(db);
	event_log.Record(event_log_size, ShouldRecordJitDecisionCounters(context), std::move(event));
}

void JitManager::RecordRuntimeEvent(ClientContext &context, const JitRegionKernel &kernel, JitCompileTarget target,
                                    string status, string reason, idx_t input_rows, idx_t output_rows,
                                    int64_t runtime_time_us, string runtime_result,
                                    const JitRuntimeMetrics &runtime_metrics) {
	if (context.IsJitSuppressed() || !Settings::Get<JitTraceRuntimeSetting>(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, target, kernel.ExecutionMode(), std::move(status), std::move(reason),
	                               input_rows, output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeMetrics(event, runtime_metrics);
	SetRuntimeRegionExecutionForm(event, kernel);
	SetRuntimeRegionCandidate(event, kernel);
	auto event_log_size = Settings::Get<JitEventLogSizeSetting>(db);
	event_log.Record(event_log_size, ShouldRecordJitDecisionCounters(context), std::move(event));
}

void JitManager::RecordRuntimeFallbackEvent(ClientContext &context, const JitRegionKernel &kernel,
                                            JitCompileTarget target, string status, string reason, idx_t input_rows,
                                            idx_t output_rows, int64_t runtime_time_us, string runtime_result) {
	if (context.IsJitSuppressed() || !Settings::Get<JitTraceRuntimeSetting>(context)) {
		return;
	}
	auto event =
	    BuildRuntimeEvent(kernel, target, JitExecutionMode::EXECUTOR_FALLBACK, std::move(status), std::move(reason),
	                      input_rows, output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeRegionExecutionForm(event, kernel);
	SetRuntimeRegionCandidate(event, kernel);
	auto event_log_size = Settings::Get<JitEventLogSizeSetting>(db);
	event_log.Record(event_log_size, ShouldRecordJitDecisionCounters(context), std::move(event));
}

static string ComposeJitCompileEventReason(const JitAdmissionDecision &admission, const string &compile_reason) {
	if (admission.policy_decision != "auto" || admission.reason.empty()) {
		return compile_reason;
	}
	if (compile_reason.empty()) {
		return admission.reason;
	}
	return admission.reason + ";" + compile_reason;
}

static string AttachJitRegionCandidateReason(const JitRegionCandidate &candidate, string reason) {
	if (!reason.empty()) {
		reason += ";";
	}
	reason += "candidate_id=" + std::to_string(candidate.candidate_id);
	reason += ";candidate_shape=" + candidate.shape;
	reason += ";candidate_scope=" + string(JitRegionCandidateScopeToString(candidate.scope));
	return reason;
}

static int64_t JitManagerElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static int64_t JitManagerSharedDecisionTime(int64_t total_time_us, idx_t entry_count, idx_t entry_index) {
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

static JitAdmissionDecision AdmitJitRegion(const JitBackend &backend, JitPolicyMode policy,
                                           const JitRegionCandidate &candidate,
                                           const JitRegionLoweringPlan &lowering_plan) {
	JitAdmissionDecision decision;
	decision.policy_decision = JitPolicyModeToString(policy);
	decision.info.has_admission = true;
	decision.info.admission_key = lowering_plan.shape_key;
	auto estimated_work = candidate.estimated_cardinality;
	JitAutoAdmissionRule admission_rule;
	auto has_admission_rule =
	    backend.GetAutoAdmissionRule(JitCompileTarget::REGION, candidate, lowering_plan, admission_rule);
	decision.info.rule_present = has_admission_rule;
	if (has_admission_rule) {
		decision.info.admission_key = admission_rule.admission_key;
		decision.info.min_cardinality = admission_rule.min_cardinality;
		decision.info.proof = admission_rule.proof;
		decision.info.has_score = true;
		decision.info.score =
		    NumericCast<int64_t>(estimated_work) - NumericCast<int64_t>(admission_rule.min_cardinality);
	}
	auto region_execution_form = lowering_plan.ExpectedRegionExecutionForm();
	if (region_execution_form != JitRegionExecutionForm::FUSED) {
		decision.reason = "jit_policy=" + string(JitPolicyModeToString(policy)) +
		                  " skips region kernel because region execution form is not fused";
		decision.reason += ";region_execution_form=" + string(JitRegionExecutionFormToString(region_execution_form));
		decision.reason += ";requires=fused;shape=" + decision.info.admission_key;
		decision.reason += ";" + lowering_plan.EventReason();
		if (has_admission_rule) {
			decision.reason += ";min_cardinality=" + std::to_string(admission_rule.min_cardinality);
			decision.reason += ";proof=" + admission_rule.proof;
		} else {
			decision.reason += ";admission_rule=missing";
		}
		return decision;
	}
	if (policy == JitPolicyMode::FORCE) {
		decision.compile = true;
		decision.reason = "jit_policy=force admits supported fused region kernel";
		return decision;
	}

	D_ASSERT(policy == JitPolicyMode::AUTO);
	if (has_admission_rule && estimated_work >= admission_rule.min_cardinality) {
		decision.compile = true;
		decision.reason = "jit_policy=auto admits shape=" + decision.info.admission_key +
		                  ";estimated_cardinality=" + std::to_string(estimated_work) + ";proof=" + admission_rule.proof;
		return decision;
	}

	decision.reason = "jit_policy=auto skips region kernel without admitted performance proof";
	if (!decision.info.admission_key.empty()) {
		decision.reason += ": shape=" + decision.info.admission_key;
	}
	decision.reason += ";estimated_cardinality=" + std::to_string(estimated_work);
	if (has_admission_rule) {
		decision.reason += ";min_cardinality=" + std::to_string(admission_rule.min_cardinality);
		decision.reason += ";proof=" + admission_rule.proof;
	} else {
		decision.reason += ";admission_rule=missing";
	}
	decision.reason += ";";
	decision.reason += lowering_plan.EventReason();
	return decision;
}

static bool JitRegionCandidatesOverlap(const JitRegionCandidate &left, const JitRegionCandidate &right) {
	return left.start_operator_index < right.end_operator_index && right.start_operator_index < left.end_operator_index;
}

struct JitAnalyzedRegionCandidate {
	idx_t candidate_index = 0;
	const JitRegionCandidate *candidate = nullptr;
	JitRegionLoweringPlan lowering_plan;
	JitAdmissionDecision admission;
	JitStageTimings stage_timings;
	int64_t decision_time_us = 0;
};

static bool JitRegionStageExecutionIsFused(JitRegionStageExecutionKind execution) {
	switch (execution) {
	case JitRegionStageExecutionKind::GENERATED_IR:
	case JitRegionStageExecutionKind::NATIVE_PROTOCOL:
		return true;
	default:
		return false;
	}
}

static bool JitRegionStageExecutionIsFusionBlocker(JitRegionStageExecutionKind execution) {
	switch (execution) {
	case JitRegionStageExecutionKind::SOURCE_BOUNDARY:
	case JitRegionStageExecutionKind::EXECUTOR_FALLBACK:
	case JitRegionStageExecutionKind::MISSING_PROTOCOL:
		return true;
	default:
		return false;
	}
}

static string DescribeJitRegionStageFusionBlocker(const JitRegionStage &stage) {
	string result = "stage=";
	result += JitRegionStageKindToString(stage.kind);
	result += ";stage_execution=";
	result += JitRegionStageExecutionKindToString(stage.execution);
	result += ";stage_ownership=";
	result += JitRegionOwnershipKindToString(stage.ownership);
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

static string ValidateJitFusedRegionStageContract(const JitRegionCandidate &candidate,
                                                  const JitRegionLoweringPlan &lowering_plan) {
	if (lowering_plan.ExpectedRegionExecutionForm() != JitRegionExecutionForm::FUSED) {
		return string();
	}
	if (!candidate.contract.native_fusion_ready) {
		return "backend advertised fused region but core native-fusion contract is not ready;" + candidate.contract.ir;
	}
	if (!candidate.stage_plan.HasStages()) {
		return "backend advertised fused region but core operator-stage plan is empty";
	}
	bool has_executable_stage = false;
	for (auto &stage : candidate.stage_plan.stages) {
		if (JitRegionStageExecutionIsFusionBlocker(stage.execution)) {
			return "backend advertised fused region across non-fused core stage;" +
			       DescribeJitRegionStageFusionBlocker(stage);
		}
		has_executable_stage = has_executable_stage || JitRegionStageExecutionIsFused(stage.execution);
	}
	if (!has_executable_stage) {
		return "backend advertised fused region but core operator-stage plan has no generated or native stage";
	}
	return string();
}

static bool JitFullPipelineRuntimeCanEnter(Pipeline &pipeline, string &reason) {
	if (!pipeline.GetSource() || !pipeline.GetSink()) {
		reason = "full pipeline runtime requires both source and sink";
		return false;
	}
	return true;
}

static bool ShouldRecordAutoAdmissionSkip(ClientContext &context, const JitAdmissionInfo &admission) {
	if (!admission.has_admission) {
		return false;
	}
	if (admission.rule_present) {
		return true;
	}
	return ShouldRecordJitDecisionCounters(context);
}

static int64_t JitRegionCandidateAdmissionScore(const JitAnalyzedRegionCandidate &entry) {
	if (!entry.admission.info.has_score) {
		return 0;
	}
	return entry.admission.info.score;
}

static idx_t JitRegionCandidateOperatorCoverage(const JitAnalyzedRegionCandidate &entry) {
	D_ASSERT(entry.candidate);
	idx_t result = 0;
	if (entry.candidate->end_operator_index > entry.candidate->start_operator_index) {
		result = entry.candidate->end_operator_index - entry.candidate->start_operator_index;
	}
	if (JitRegionABIOwnsSource(entry.candidate->contract.abi)) {
		result++;
	}
	if (JitRegionABIOwnsSink(entry.candidate->contract.abi)) {
		result++;
	}
	return result;
}

static bool JitRegionSelectionLexicographicallyBefore(const vector<idx_t> &left, const vector<idx_t> &right) {
	return std::lexicographical_compare(left.begin(), left.end(), right.begin(), right.end());
}

static bool JitRegionCandidatesDoNotOverlap(const JitRegionCandidate &left, const JitRegionCandidate &right) {
	if (left.end_operator_index < right.start_operator_index) {
		return true;
	}
	if (left.end_operator_index > right.start_operator_index) {
		return false;
	}
	if (JitRegionABIOwnsSource(left.contract.abi) && JitRegionABIOwnsSource(right.contract.abi)) {
		return false;
	}
	if (JitRegionABIOwnsSink(left.contract.abi) && JitRegionABIOwnsSink(right.contract.abi)) {
		return false;
	}
	return true;
}

struct JitRegionAutoSelectionState {
	bool valid = false;
	int64_t score = 0;
	idx_t covered_operators = 0;
	int64_t native_fusion_score = 0;
	idx_t candidate_count = 0;
	vector<idx_t> eligible_indices;
};

static bool JitRegionSelectionBetter(const JitRegionAutoSelectionState &left,
                                     const JitRegionAutoSelectionState &right) {
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
	if (left.native_fusion_score != right.native_fusion_score) {
		return left.native_fusion_score > right.native_fusion_score;
	}
	if (left.candidate_count != right.candidate_count) {
		return left.candidate_count < right.candidate_count;
	}
	return JitRegionSelectionLexicographicallyBefore(left.eligible_indices, right.eligible_indices);
}

static int64_t JitRegionCandidateNativeFusionStrategyScore(const JitAnalyzedRegionCandidate &entry) {
	D_ASSERT(entry.candidate);
	auto &candidate = *entry.candidate;
	if (entry.lowering_plan.ExpectedRegionExecutionForm() != JitRegionExecutionForm::FUSED ||
	    entry.lowering_plan.SelectedSourceExecution() != JitRegionSourceExecutionKind::NATIVE_SOURCE) {
		return 0;
	}
	if (!JitRegionABIOwnsSource(candidate.contract.abi) ||
	    candidate.traits.source_execution != JitRegionSourceExecutionKind::NATIVE_SOURCE ||
	    candidate.contract.source_ownership != JitRegionOwnershipKind::NATIVE_PROTOCOL) {
		return 0;
	}
	int64_t result = NumericCast<int64_t>(candidate.estimated_cardinality);
	if (result == 0) {
		result = 1;
	}
	if (JitRegionABIOwnsSink(candidate.contract.abi) &&
	    candidate.contract.sink_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL) {
		result += 1;
	}
	if (candidate.contract.transform_ownership == JitRegionOwnershipKind::GENERATED_IR ||
	    candidate.contract.transform_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL) {
		result += 1;
	}
	return result;
}

template <class SCORE_FUNC>
static vector<bool> SelectJitRegionCandidatesByScore(const vector<JitAnalyzedRegionCandidate> &eligible_candidates,
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
			if (JitRegionCandidatesDoNotOverlap(previous_candidate, candidate)) {
				previous_non_overlapping[sorted_idx] = previous_idx - 1;
				break;
			}
		}
	}

	vector<JitRegionAutoSelectionState> dp(sorted_indices.size() + 1);
	dp[0].valid = true;
	for (idx_t sorted_idx = 0; sorted_idx < sorted_indices.size(); sorted_idx++) {
		auto exclude = dp[sorted_idx];
		JitRegionAutoSelectionState include;
		auto previous_idx = previous_non_overlapping[sorted_idx];
		if (previous_idx == DConstants::INVALID_INDEX) {
			include = dp[0];
		} else {
			include = dp[previous_idx + 1];
		}
		if (include.valid) {
			auto eligible_idx = sorted_indices[sorted_idx];
			include.score += score_func(eligible_candidates[eligible_idx]);
			include.covered_operators += JitRegionCandidateOperatorCoverage(eligible_candidates[eligible_idx]);
			include.native_fusion_score +=
			    JitRegionCandidateNativeFusionStrategyScore(eligible_candidates[eligible_idx]);
			include.candidate_count++;
			include.eligible_indices.push_back(eligible_idx);
		}
		dp[sorted_idx + 1] = JitRegionSelectionBetter(include, exclude) ? std::move(include) : std::move(exclude);
	}

	for (auto eligible_idx : dp.back().eligible_indices) {
		selected[eligible_idx] = true;
	}
	return selected;
}

static vector<bool> SelectJitAutoRegionCandidates(const vector<JitAnalyzedRegionCandidate> &eligible_candidates) {
	return SelectJitRegionCandidatesByScore(eligible_candidates, JitRegionCandidateAdmissionScore);
}

static int64_t JitRegionCandidateForceScore(const JitAnalyzedRegionCandidate &) {
	return 0;
}

static vector<bool> SelectJitForceRegionCandidates(const vector<JitAnalyzedRegionCandidate> &eligible_candidates) {
	return SelectJitRegionCandidatesByScore(eligible_candidates, JitRegionCandidateForceScore);
}

static void AccumulateJitPreparedSourceContract(JitPreparedPipeline &prepared, const JitRegionCandidate &candidate,
                                                const JitRegionLoweringPlan &lowering_plan) {
	if (!JitRegionABIOwnsSource(candidate.contract.abi)) {
		return;
	}
	if (!prepared.region_ir) {
		return;
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = prepared.region_ir->nodes[node_idx];
		if (!node.source) {
			continue;
		}
		auto &source = *node.source;
		auto selected_source_execution = lowering_plan.SelectedSourceExecution() != JitRegionSourceExecutionKind::NONE
		                                     ? lowering_plan.SelectedSourceExecution()
		                                     : source.execution;
		auto native_fused_source_owner = lowering_plan.ExpectedCompiledExecutionMode() == JitExecutionMode::NATIVE &&
		                                 lowering_plan.ExpectedRegionExecutionForm() == JitRegionExecutionForm::FUSED;
		if (source.kind == JitRegionSourceKind::STATEFUL_OPERATOR) {
			auto &contract = prepared.source_contract;
			contract.present = true;
			contract.selected = true;
			contract.requires_unfiltered_input = false;
			contract.filter_prune_required = false;
			contract.filter_takeover_supported = false;
			contract.input_types = node.output_types;
			contract.output_projection_map.clear();
			contract.filter_column_map.clear();
			contract.reason = source.reason;
			contract.ir = source.ir;
			contract.native_source = selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
			                         source.native_source_contract.status == JitRegionNativeSourceStatus::READY &&
			                         native_fused_source_owner;
			contract.owns_filters = false;
			return;
		}
		if (!source.table_scan_protocol.present) {
			continue;
		}
		auto &protocol = source.table_scan_protocol;
		auto &contract = prepared.source_contract;
		contract.present = true;
		contract.selected = true;
		contract.requires_unfiltered_input = protocol.native_source_requires_unfiltered_input;
		contract.filter_prune_required = protocol.native_source_filter_prune_required;
		contract.filter_takeover_supported = protocol.native_source_filter_takeover_supported;
		contract.input_types = protocol.native_source_input_types;
		contract.output_projection_map = protocol.native_source_output_projection_map;
		contract.filter_column_map = protocol.native_source_filter_column_map;
		contract.reason = source.reason;
		contract.ir = source.ir;
		contract.native_source = selected_source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
		                         source.native_source_contract.status == JitRegionNativeSourceStatus::READY &&
		                         native_fused_source_owner;
		contract.owns_filters = !source.filters.empty() && contract.filter_takeover_supported &&
		                        lowering_plan.OwnsSourceFilters() && native_fused_source_owner;
		return;
	}
}

unique_ptr<JitPreparedPipeline> JitManager::PreparePipelineRegions(ClientContext &context, Pipeline &pipeline) {
	auto prepared = make_uniq<JitPreparedPipeline>();
	prepared->initialized = true;
	if (context.IsJitSuppressed() || !Settings::Get<EnableJitSetting>(context)) {
		return prepared;
	}
	auto policy = GetPolicy(context);
	auto policy_decision = string(JitPolicyModeToString(policy));
	prepared->policy = policy;
	prepared->policy_decision = policy_decision;
	if (policy == JitPolicyMode::OFF) {
		RecordEvent(context, "policy", JitCompileTarget::REGION, JitCompileStatus::DISABLED, JitExecutionMode::NONE,
		            "off", "jit_policy=off", string(), 0, 0, 0);
		return prepared;
	}

	string backend_name;
	auto backend = SelectBackend(context, backend_name);
	prepared->backend_name = backend_name;
	if (!backend) {
		RecordEvent(context, std::move(backend_name), JitCompileTarget::REGION, JitCompileStatus::UNAVAILABLE,
		            JitExecutionMode::NONE, policy_decision, "no available JIT backend", string(), 0, 0, 0);
		return prepared;
	}
	prepared->enabled = true;
	if (!backend->SupportsRegions()) {
		RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::UNSUPPORTED,
		            JitExecutionMode::UNSUPPORTED, policy_decision, "backend does not compile regions", string(), 0, 0,
		            0);
		return prepared;
	}
	auto pipeline_descriptor = BuildJitPipelineDescriptor(pipeline);
	if (!pipeline_descriptor) {
		return prepared;
	}
	if (policy == JitPolicyMode::AUTO) {
		auto explain_inventory = ShouldRecordJitDecisionCounters(context) || Settings::Get<JitDumpIrSetting>(context);
		auto inventory_start = std::chrono::steady_clock::now();
		auto inventory = TryInspectJitRegionPipeline(
		    *pipeline_descriptor,
		    explain_inventory ? JitRegionPipelineInventoryMode::DIAGNOSTIC : JitRegionPipelineInventoryMode::ADMISSION);
		auto inventory_time_us = JitManagerElapsedMicros(inventory_start);
		if (!inventory) {
			return prepared;
		}
		JitAdmissionInfo inventory_info;
		string inventory_reason;
		auto maybe_admissible = backend->MayHaveAutoAdmissionRule(JitCompileTarget::REGION, *inventory,
		                                                          explain_inventory, inventory_info, inventory_reason);
		if (!maybe_admissible) {
			if (ShouldRecordAutoAdmissionSkip(context, inventory_info)) {
				if (inventory->ir.empty() || inventory_reason.empty()) {
					inventory =
					    TryInspectJitRegionPipeline(*pipeline_descriptor, JitRegionPipelineInventoryMode::DIAGNOSTIC);
					if (inventory) {
						backend->MayHaveAutoAdmissionRule(JitCompileTarget::REGION, *inventory, true, inventory_info,
						                                  inventory_reason);
					}
				}
				RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::SKIPPED,
				            JitExecutionMode::EXECUTOR_FALLBACK, policy_decision, std::move(inventory_reason),
				            inventory ? inventory->ir : string(), inventory_time_us, 0, 0, nullptr, &inventory_info,
				            nullptr, JitRegionExecutionForm::NONE, JitRegionSourceExecutionKind::NONE, inventory.get());
			}
			return prepared;
		}
	}
	auto region_decision_start = std::chrono::steady_clock::now();
	auto region_ir = TryLowerJitRegion(*pipeline_descriptor);
	auto region_lowering_time_us = JitManagerElapsedMicros(region_decision_start);
	if (!region_ir) {
		return prepared;
	}
	prepared->region_ir = std::move(region_ir);
	if (prepared->region_ir->candidates.empty()) {
		RecordEvent(context, std::move(backend_name), JitCompileTarget::REGION, JitCompileStatus::UNSUPPORTED,
		            JitExecutionMode::UNSUPPORTED, policy_decision, "core region lowering produced no candidates",
		            prepared->region_ir->ir, region_lowering_time_us, 0, 0);
		return prepared;
	}

	vector<JitAnalyzedRegionCandidate> eligible_candidates;
	for (idx_t candidate_index = 0; candidate_index < prepared->region_ir->candidates.size(); candidate_index++) {
		auto &candidate = prepared->region_ir->candidates[candidate_index];
		auto candidate_decision_start = std::chrono::steady_clock::now();
		auto shared_decision_time_us = JitManagerSharedDecisionTime(
		    region_lowering_time_us, prepared->region_ir->candidates.size(), candidate_index);
		JitStageTimings stage_timings;
		stage_timings.ir_lowering_time_us = shared_decision_time_us;
		auto candidate_decision_time_us = [&]() {
			return shared_decision_time_us + JitManagerElapsedMicros(candidate_decision_start);
		};
		if (JitRegionABIIsFullPipeline(candidate.contract.abi)) {
			string full_pipeline_entry_reason;
			if (!JitFullPipelineRuntimeCanEnter(pipeline, full_pipeline_entry_reason)) {
				auto decision_time_us = candidate_decision_time_us();
				RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::SKIPPED,
				            JitExecutionMode::EXECUTOR_FALLBACK, policy_decision,
				            AttachJitRegionCandidateReason(candidate, std::move(full_pipeline_entry_reason)),
				            prepared->region_ir->ir, decision_time_us, 0, 0, &candidate, nullptr, &stage_timings);
				continue;
			}
		}
		if (policy == JitPolicyMode::AUTO) {
			auto admission_start = std::chrono::steady_clock::now();
			JitAdmissionInfo precheck_info;
			string precheck_reason;
			auto maybe_admissible =
			    backend->MayHaveAutoAdmissionRule(JitCompileTarget::REGION, candidate, precheck_info, precheck_reason);
			stage_timings.admission_time_us = JitManagerElapsedMicros(admission_start);
			if (!maybe_admissible) {
				if (!precheck_info.has_admission) {
					precheck_info.has_admission = true;
					precheck_info.admission_key = candidate.shape;
					precheck_info.rule_present = false;
				}
				if (precheck_reason.empty()) {
					precheck_reason =
					    "jit_policy=auto skips region before backend analysis: candidate cannot map to an admitted "
					    "backend shape;admission_rule=missing";
				}
				if (!Settings::Get<JitDumpIrSetting>(context) &&
				    !ShouldRecordAutoAdmissionSkip(context, precheck_info)) {
					continue;
				}
				auto decision_time_us = candidate_decision_time_us();
				RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::SKIPPED,
				            JitExecutionMode::EXECUTOR_FALLBACK, policy_decision,
				            AttachJitRegionCandidateReason(candidate, std::move(precheck_reason)),
				            prepared->region_ir->ir, decision_time_us, 0, 0, &candidate, &precheck_info,
				            &stage_timings);
				continue;
			}
		}
		JitRegionCompilationInput input(context, *prepared->region_ir, candidate);
		auto analysis_start = std::chrono::steady_clock::now();
		auto lowering_plan = backend->AnalyzeRegion(input);
		stage_timings.backend_analysis_time_us = JitManagerElapsedMicros(analysis_start);
		input.lowering_plan = &lowering_plan;
		if (lowering_plan.nodes.empty()) {
			auto decision_time_us = candidate_decision_time_us();
			string empty_analysis_reason = "backend produced an empty JIT region capability analysis";
			auto lowering_reason = lowering_plan.EventReason();
			if (!lowering_reason.empty()) {
				empty_analysis_reason += ";";
				empty_analysis_reason += lowering_reason;
			}
			RecordEvent(
			    context, backend_name, JitCompileTarget::REGION, JitCompileStatus::UNSUPPORTED,
			    JitExecutionMode::UNSUPPORTED, policy_decision,
			    AttachJitRegionCandidateReason(candidate, std::move(empty_analysis_reason)),
			    prepared->region_ir->ir, decision_time_us, 0, 0, &candidate, nullptr, &stage_timings,
			    lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}
		if (lowering_plan.ExpectedCompiledExecutionMode() == JitExecutionMode::UNSUPPORTED) {
			auto decision_time_us = candidate_decision_time_us();
			auto unsupported_reason =
			    lowering_plan.NativeCount() > 0
			        ? lowering_plan.EventReason() +
			              ";execution:unsupported;backend cannot generate executable code for "
			              "this whole region"
				        : "region lowering contains no native executable nodes: " + lowering_plan.EventReason() +
				              ";execution:unsupported";
			RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::UNSUPPORTED,
			            JitExecutionMode::UNSUPPORTED, policy_decision,
			            AttachJitRegionCandidateReason(candidate, std::move(unsupported_reason)),
			            prepared->region_ir->ir, decision_time_us, 0, 0, &candidate, nullptr, &stage_timings,
			            lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}
		auto fused_contract_blocker = ValidateJitFusedRegionStageContract(candidate, lowering_plan);
		if (!fused_contract_blocker.empty()) {
			auto decision_time_us = candidate_decision_time_us();
			auto reason = lowering_plan.EventReason() + ";execution:unsupported;" + std::move(fused_contract_blocker);
			RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::UNSUPPORTED,
			            JitExecutionMode::UNSUPPORTED, policy_decision,
			            AttachJitRegionCandidateReason(candidate, std::move(reason)), prepared->region_ir->ir,
			            decision_time_us, 0, 0, &candidate, nullptr, &stage_timings,
			            lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}
		auto admission_start = std::chrono::steady_clock::now();
		auto admission = AdmitJitRegion(*backend, policy, candidate, lowering_plan);
		stage_timings.admission_time_us = JitManagerElapsedMicros(admission_start);
		if (!admission.compile) {
			auto decision_time_us = candidate_decision_time_us();
			RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::SKIPPED,
			            JitExecutionMode::EXECUTOR_FALLBACK, admission.policy_decision,
			            AttachJitRegionCandidateReason(candidate, admission.reason), prepared->region_ir->ir,
			            decision_time_us, 0, 0, &candidate, &admission.info, &stage_timings,
			            lowering_plan.ExpectedRegionExecutionForm(), lowering_plan.SelectedSourceExecution());
			continue;
		}

		JitAnalyzedRegionCandidate analyzed_candidate;
		analyzed_candidate.candidate_index = candidate_index;
		analyzed_candidate.candidate = &candidate;
		analyzed_candidate.lowering_plan = std::move(lowering_plan);
		analyzed_candidate.admission = std::move(admission);
		analyzed_candidate.stage_timings = stage_timings;
		analyzed_candidate.decision_time_us = candidate_decision_time_us();
		eligible_candidates.push_back(std::move(analyzed_candidate));
	}

	auto selection_start = std::chrono::steady_clock::now();
	auto selected_candidates = policy == JitPolicyMode::AUTO ? SelectJitAutoRegionCandidates(eligible_candidates)
	                                                         : SelectJitForceRegionCandidates(eligible_candidates);
	auto selection_time_us = JitManagerElapsedMicros(selection_start);
	for (idx_t eligible_idx = 0; eligible_idx < eligible_candidates.size(); eligible_idx++) {
		auto &entry = eligible_candidates[eligible_idx];
		auto &candidate = *entry.candidate;
		entry.stage_timings.overlap_check_time_us =
		    JitManagerSharedDecisionTime(selection_time_us, eligible_candidates.size(), eligible_idx);
		auto decision_time_us = entry.decision_time_us + entry.stage_timings.overlap_check_time_us;
		if (!selected_candidates[eligible_idx]) {
			auto reason = policy == JitPolicyMode::AUTO
			                  ? "region candidate not selected by auto non-overlapping admission selection"
			                  : "region candidate overlaps an already selected non-overlapping region";
			RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::SKIPPED,
			            JitExecutionMode::EXECUTOR_FALLBACK, entry.admission.policy_decision,
			            AttachJitRegionCandidateReason(candidate, std::move(reason)), prepared->region_ir->ir,
			            decision_time_us, 0, 0, &candidate, &entry.admission.info, &entry.stage_timings,
			            entry.lowering_plan.ExpectedRegionExecutionForm(),
			            entry.lowering_plan.SelectedSourceExecution());
			continue;
		}

		JitPreparedRegionCandidate prepared_region;
		prepared_region.candidate_index = entry.candidate_index;
		prepared_region.lowering_plan = std::move(entry.lowering_plan);
		prepared_region.admission = std::move(entry.admission);
		prepared_region.stage_timings = entry.stage_timings;
		prepared_region.decision_time_us = decision_time_us;
		prepared->selected_regions.push_back(std::move(prepared_region));
		AccumulateJitPreparedSourceContract(*prepared, candidate, prepared->selected_regions.back().lowering_plan);
	}
	return prepared;
}

vector<unique_ptr<JitRegionKernel>> JitManager::CompilePreparedRegions(ClientContext &context,
                                                                       const JitPreparedPipeline &prepared) {
	vector<unique_ptr<JitRegionKernel>> kernels;
	if (context.IsJitSuppressed() || !Settings::Get<EnableJitSetting>(context) || !prepared.enabled ||
	    !prepared.region_ir || prepared.selected_regions.empty()) {
		return kernels;
	}
	string backend_name;
	auto backend = SelectBackend(context, backend_name);
	if (!backend) {
		RecordEvent(context, std::move(backend_name), JitCompileTarget::REGION, JitCompileStatus::UNAVAILABLE,
		            JitExecutionMode::NONE, prepared.policy_decision, "no available JIT backend", string(), 0, 0, 0);
		return kernels;
	}
	if (backend_name != prepared.backend_name) {
		string reason = "prepared region backend changed before executor kernel instantiation";
		reason += ";prepared_backend=";
		reason += prepared.backend_name;
		reason += ";selected_backend=";
		reason += backend_name;
		RecordEvent(context, backend_name, JitCompileTarget::REGION, JitCompileStatus::UNAVAILABLE,
		            JitExecutionMode::NONE, prepared.policy_decision, std::move(reason), string(), 0, 0, 0);
		return kernels;
	}
	for (auto &prepared_region : prepared.selected_regions) {
		auto &candidate = prepared.region_ir->candidates[prepared_region.candidate_index];
		auto stage_timings = prepared_region.stage_timings;
		JitRegionCompilationInput input(context, *prepared.region_ir, candidate);
		input.lowering_plan = &prepared_region.lowering_plan;
		auto start = std::chrono::steady_clock::now();
		auto result = backend->CompileRegion(input);
		auto compile_time_us = JitManagerElapsedMicros(start);
		stage_timings.codegen_time_us = compile_time_us;

		idx_t code_size = result.kernel ? result.kernel->CodeSize() : 0;
		auto status = result.status;
		auto reason = AttachJitRegionCandidateReason(
		    candidate, ComposeJitCompileEventReason(prepared_region.admission, result.reason));
		auto execution_mode = result.execution_mode;
		auto ir = result.ir;
		if (status != JitCompileStatus::COMPILED && result.kernel) {
			throw InternalException("JIT backend \"%s\" returned kernel for non-compiled region status %s", backend_name,
			                        JitCompileStatusToString(status));
		}
		if (status == JitCompileStatus::COMPILED) {
			auto expected_mode = prepared_region.lowering_plan.ExpectedCompiledExecutionMode();
			auto expected_form = prepared_region.lowering_plan.ExpectedRegionExecutionForm();
			if (expected_mode == JitExecutionMode::UNSUPPORTED) {
				throw InternalException("JIT backend \"%s\" compiled region without native executable nodes",
				                        backend_name);
			}
			if (expected_form == JitRegionExecutionForm::NONE) {
				throw InternalException("JIT backend \"%s\" compiled region without an explicit region execution form",
				                        backend_name);
			}
			if (expected_form != JitRegionExecutionForm::FUSED) {
				throw InternalException("JIT backend \"%s\" compiled non-fused region execution form %s", backend_name,
				                        JitRegionExecutionFormToString(expected_form));
			}
			if (execution_mode != expected_mode) {
				throw InternalException(
				    "JIT backend \"%s\" compiled region with execution mode %s after advertising %s", backend_name,
				    JitExecutionModeToString(execution_mode), JitExecutionModeToString(expected_mode));
			}
			if (!result.kernel || !result.kernel->HasExecutableBody()) {
				throw InternalException("JIT backend \"%s\" compiled region without executable code", backend_name);
			}
			if (JitRegionABIIsFullPipeline(candidate.contract.abi) && !result.kernel->CanExecuteFullPipeline()) {
				throw InternalException(
				    "JIT backend \"%s\" compiled full pipeline without full-pipeline executable ABI", backend_name);
			}
		}
		auto trace_id = RecordEvent(context, backend_name, JitCompileTarget::REGION, status, execution_mode,
		                            prepared.policy_decision, reason, ir, prepared_region.decision_time_us,
		                            compile_time_us, code_size, &candidate, &prepared_region.admission.info,
		                            &stage_timings, prepared_region.lowering_plan.ExpectedRegionExecutionForm(),
		                            prepared_region.lowering_plan.SelectedSourceExecution());
		if (status == JitCompileStatus::COMPILED && result.kernel) {
			result.kernel->SetTraceInfo(trace_id, execution_mode, reason, compile_time_us, code_size);
			result.kernel->SetTraceRegionExecutionForm(prepared_region.lowering_plan.ExpectedRegionExecutionForm());
			result.kernel->SetTraceSelectedSourceExecution(prepared_region.lowering_plan.SelectedSourceExecution());
			result.kernel->SetTraceCandidate(candidate);
		}
		if (status == JitCompileStatus::ERROR) {
			throw InvalidInputException("JIT region compilation failed: %s", reason);
		}
		if (result.kernel) {
			kernels.push_back(std::move(result.kernel));
		}
	}
	return kernels;
}

JitManager &JitManager::Get(DatabaseInstance &db) {
	return db.GetJitManager();
}

JitManager &JitManager::Get(ClientContext &context) {
	return Get(DatabaseInstance::GetDatabase(context));
}

static bool IsJitIntrospectionOperator(const PhysicalOperator &op) {
	if (op.type != PhysicalOperatorType::TABLE_SCAN) {
		return false;
	}
	auto &scan = op.Cast<PhysicalTableScan>();
	auto name = StringUtil::Lower(scan.function.name.GetIdentifierName());
	return name == "duckdb_jit_events" || name == "duckdb_jit_counters" || name == "duckdb_jit_decision_counters" ||
	       name == "duckdb_jit_kernel_counters" || name == "duckdb_jit_backends" || name == "duckdb_jit_clear_events" ||
	       name == "duckdb_jit_clear_counters";
}

bool JitManager::IsJitIntrospectionPipeline(Pipeline &pipeline) {
	if (pipeline.GetSource() && IsJitIntrospectionOperator(*pipeline.GetSource())) {
		return true;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		if (IsJitIntrospectionOperator(op.get())) {
			return true;
		}
	}
	if (pipeline.GetSink() && IsJitIntrospectionOperator(*pipeline.GetSink())) {
		return true;
	}
	return false;
}

} // namespace duckdb

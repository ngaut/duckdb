#include "duckdb/execution/jit/runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

JitCodeHandle::~JitCodeHandle() {
}

JitNativeGroupedAggregateStateSet::JitNativeGroupedAggregateStateSet() : aggregate_addresses(LogicalType::POINTER) {
}

idx_t JitCodeHandle::CodeSize() const {
	return 0;
}

JitSuppressionGuard::JitSuppressionGuard(ClientContext &context_p) : context(context_p) {
	context.PushJitSuppression();
}

JitSuppressionGuard::~JitSuppressionGuard() {
	context.PopJitSuppression();
}

JitFullPipelineRuntime::~JitFullPipelineRuntime() {
}

void JitEventLog::RecordCounter(const JitEvent &event) {
	for (auto &counter : counters) {
		if (counter.backend_name != event.backend_name || counter.target != event.target ||
		    counter.status != event.status || counter.execution_mode != event.execution_mode ||
		    counter.region_execution_form != event.region_execution_form ||
		    counter.policy_decision != event.policy_decision) {
			continue;
		}
		counter.count++;
		counter.decision_time_us += event.decision_time_us;
		counter.compile_time_us += event.compile_time_us;
		counter.code_size += event.code_size;
		counter.input_rows += event.input_rows;
		counter.output_rows += event.output_rows;
		counter.invocation_count += event.invocation_count;
		counter.runtime_time_us += event.runtime_time_us;
		counter.source_native_output_rows += event.source_native_output_rows;
		counter.source_native_invocation_count += event.source_native_invocation_count;
		counter.source_native_runtime_time_us += event.source_native_runtime_time_us;
		counter.generated_body_runtime_time_us += event.generated_body_runtime_time_us;
		counter.fused_prepare_runtime_time_us += event.fused_prepare_runtime_time_us;
		counter.fused_group_runtime_time_us += event.fused_group_runtime_time_us;
		counter.fused_state_bind_runtime_time_us += event.fused_state_bind_runtime_time_us;
		counter.fused_update_runtime_time_us += event.fused_update_runtime_time_us;
		counter.fused_finish_runtime_time_us += event.fused_finish_runtime_time_us;
		counter.generated_body_flat_input_rows += event.generated_body_flat_input_rows;
		counter.generated_body_flat_invocation_count += event.generated_body_flat_invocation_count;
		counter.generated_body_shared_selection_input_rows += event.generated_body_shared_selection_input_rows;
		counter.generated_body_shared_selection_invocation_count +=
		    event.generated_body_shared_selection_invocation_count;
		counter.generated_body_selection_input_rows += event.generated_body_selection_input_rows;
		counter.generated_body_selection_invocation_count += event.generated_body_selection_invocation_count;
		counter.native_operator_loop_input_rows += event.native_operator_loop_input_rows;
		counter.native_operator_loop_invocation_count += event.native_operator_loop_invocation_count;
		counter.ir_lowering_time_us += event.ir_lowering_time_us;
		counter.backend_analysis_time_us += event.backend_analysis_time_us;
		counter.admission_time_us += event.admission_time_us;
		counter.overlap_check_time_us += event.overlap_check_time_us;
		counter.codegen_time_us += event.codegen_time_us;
		return;
	}
	JitCounter counter;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.status = event.status;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.policy_decision = event.policy_decision;
	counter.count = 1;
	counter.decision_time_us = event.decision_time_us;
	counter.compile_time_us = event.compile_time_us;
	counter.code_size = event.code_size;
	counter.input_rows = event.input_rows;
	counter.output_rows = event.output_rows;
	counter.invocation_count = event.invocation_count;
	counter.runtime_time_us = event.runtime_time_us;
	counter.source_native_output_rows = event.source_native_output_rows;
	counter.source_native_invocation_count = event.source_native_invocation_count;
	counter.source_native_runtime_time_us = event.source_native_runtime_time_us;
	counter.generated_body_runtime_time_us = event.generated_body_runtime_time_us;
	counter.fused_prepare_runtime_time_us = event.fused_prepare_runtime_time_us;
	counter.fused_group_runtime_time_us = event.fused_group_runtime_time_us;
	counter.fused_state_bind_runtime_time_us = event.fused_state_bind_runtime_time_us;
	counter.fused_update_runtime_time_us = event.fused_update_runtime_time_us;
	counter.fused_finish_runtime_time_us = event.fused_finish_runtime_time_us;
	counter.generated_body_flat_input_rows = event.generated_body_flat_input_rows;
	counter.generated_body_flat_invocation_count = event.generated_body_flat_invocation_count;
	counter.generated_body_shared_selection_input_rows = event.generated_body_shared_selection_input_rows;
	counter.generated_body_shared_selection_invocation_count = event.generated_body_shared_selection_invocation_count;
	counter.generated_body_selection_input_rows = event.generated_body_selection_input_rows;
	counter.generated_body_selection_invocation_count = event.generated_body_selection_invocation_count;
	counter.native_operator_loop_input_rows = event.native_operator_loop_input_rows;
	counter.native_operator_loop_invocation_count = event.native_operator_loop_invocation_count;
	counter.ir_lowering_time_us = event.ir_lowering_time_us;
	counter.backend_analysis_time_us = event.backend_analysis_time_us;
	counter.admission_time_us = event.admission_time_us;
	counter.overlap_check_time_us = event.overlap_check_time_us;
	counter.codegen_time_us = event.codegen_time_us;
	counters.push_back(std::move(counter));
}

static bool JitDecisionCounterMatches(const JitDecisionCounter &counter, const JitEvent &event) {
	return counter.backend_name == event.backend_name && counter.target == event.target &&
	       counter.phase == event.phase && counter.status == event.status &&
	       counter.execution_mode == event.execution_mode &&
	       counter.region_execution_form == event.region_execution_form &&
	       counter.policy_decision == event.policy_decision && counter.has_pipeline == event.has_pipeline &&
	       counter.pipeline_shape == event.pipeline_shape && counter.candidate_shape == event.candidate_shape &&
	       counter.candidate_scope == event.candidate_scope &&
	       counter.admission_shape_key == event.admission_shape_key &&
	       counter.admission_rule_present == event.admission_rule_present &&
	       counter.admission_min_cardinality == event.admission_min_cardinality &&
	       counter.admission_proof == event.admission_proof &&
	       counter.candidate_traits.ir == event.candidate_traits.ir &&
	       counter.candidate_contract.ir == event.candidate_contract.ir;
}

static void AccumulateJitDecisionCounter(JitDecisionCounter &counter, const JitEvent &event) {
	counter.count++;
	counter.max_estimated_cardinality =
	    MaxValue(counter.max_estimated_cardinality,
	             MaxValue(event.candidate_estimated_cardinality, event.pipeline_estimated_cardinality));
	counter.pipeline_estimated_cardinality =
	    MaxValue(counter.pipeline_estimated_cardinality, event.pipeline_estimated_cardinality);
	if (event.has_admission_score) {
		if (!counter.has_admission_score) {
			counter.has_admission_score = true;
			counter.min_admission_score = event.admission_score;
			counter.max_admission_score = event.admission_score;
		} else {
			counter.min_admission_score = MinValue(counter.min_admission_score, event.admission_score);
			counter.max_admission_score = MaxValue(counter.max_admission_score, event.admission_score);
		}
	}
	counter.decision_time_us += event.decision_time_us;
	counter.compile_time_us += event.compile_time_us;
	counter.code_size += event.code_size;
	counter.ir_lowering_time_us += event.ir_lowering_time_us;
	counter.backend_analysis_time_us += event.backend_analysis_time_us;
	counter.admission_time_us += event.admission_time_us;
	counter.overlap_check_time_us += event.overlap_check_time_us;
	counter.codegen_time_us += event.codegen_time_us;
}

void JitEventLog::RecordDecisionCounter(const JitEvent &event) {
	if (!event.has_candidate && !event.has_pipeline) {
		return;
	}
	for (auto &counter : decision_counters) {
		if (!JitDecisionCounterMatches(counter, event)) {
			continue;
		}
		AccumulateJitDecisionCounter(counter, event);
		return;
	}
	JitDecisionCounter counter;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.phase = event.phase;
	counter.status = event.status;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.policy_decision = event.policy_decision;
	counter.has_pipeline = event.has_pipeline;
	counter.pipeline_shape = event.pipeline_shape;
	counter.candidate_shape = event.candidate_shape;
	counter.candidate_scope = event.candidate_scope;
	counter.admission_shape_key = event.admission_shape_key;
	counter.admission_rule_present = event.admission_rule_present;
	counter.admission_min_cardinality = event.admission_min_cardinality;
	counter.admission_proof = event.admission_proof;
	counter.candidate_traits = event.candidate_traits;
	counter.candidate_contract = event.candidate_contract;
	counter.example_reason = event.reason;
	AccumulateJitDecisionCounter(counter, event);
	decision_counters.push_back(std::move(counter));
}

void JitEventLog::AccumulateKernelRuntime(JitKernelCounter &counter, const JitEvent &event) {
	counter.last_runtime_status = event.status;
	counter.last_runtime_result = event.runtime_result;
	if (event.status == "source_native") {
		counter.source_native_output_rows += event.source_native_output_rows;
		counter.source_native_invocation_count += event.source_native_invocation_count;
		counter.source_native_runtime_time_us += event.source_native_runtime_time_us;
		return;
	}
	if (event.execution_mode == JitExecutionModeToString(JitExecutionMode::EXECUTOR_FALLBACK)) {
		counter.fallback_input_rows += event.input_rows;
		counter.fallback_output_rows += event.output_rows;
		counter.fallback_invocation_count += event.invocation_count;
		counter.fallback_runtime_time_us += event.runtime_time_us;
		return;
	}
	counter.input_rows += event.input_rows;
	counter.output_rows += event.output_rows;
	counter.invocation_count += event.invocation_count;
	counter.runtime_time_us += event.runtime_time_us;
	counter.source_native_output_rows += event.source_native_output_rows;
	counter.source_native_invocation_count += event.source_native_invocation_count;
	counter.source_native_runtime_time_us += event.source_native_runtime_time_us;
	counter.generated_body_runtime_time_us += event.generated_body_runtime_time_us;
	counter.fused_prepare_runtime_time_us += event.fused_prepare_runtime_time_us;
	counter.fused_group_runtime_time_us += event.fused_group_runtime_time_us;
	counter.fused_state_bind_runtime_time_us += event.fused_state_bind_runtime_time_us;
	counter.fused_update_runtime_time_us += event.fused_update_runtime_time_us;
	counter.fused_finish_runtime_time_us += event.fused_finish_runtime_time_us;
	counter.generated_body_flat_input_rows += event.generated_body_flat_input_rows;
	counter.generated_body_flat_invocation_count += event.generated_body_flat_invocation_count;
	counter.generated_body_shared_selection_input_rows += event.generated_body_shared_selection_input_rows;
	counter.generated_body_shared_selection_invocation_count += event.generated_body_shared_selection_invocation_count;
	counter.generated_body_selection_input_rows += event.generated_body_selection_input_rows;
	counter.generated_body_selection_invocation_count += event.generated_body_selection_invocation_count;
	counter.native_operator_loop_input_rows += event.native_operator_loop_input_rows;
	counter.native_operator_loop_invocation_count += event.native_operator_loop_invocation_count;
}

static void SetJitKernelCounterCandidate(JitKernelCounter &counter, const JitEvent &event) {
	if (!event.has_candidate) {
		return;
	}
	counter.has_candidate = true;
	counter.candidate_id = event.candidate_id;
	counter.candidate_shape = event.candidate_shape;
	counter.candidate_scope = event.candidate_scope;
	counter.candidate_pipeline_shape = event.candidate_pipeline_shape;
	counter.candidate_context_pipeline_shape = event.candidate_context_pipeline_shape;
	counter.candidate_node_count = event.candidate_node_count;
	counter.candidate_start_operator_index = event.candidate_start_operator_index;
	counter.candidate_end_operator_index = event.candidate_end_operator_index;
	counter.candidate_estimated_cardinality = event.candidate_estimated_cardinality;
	counter.candidate_traits = event.candidate_traits;
	counter.candidate_contract = event.candidate_contract;
}

void JitEventLog::RecordKernelCounter(idx_t kernel_counter_log_size, const JitEvent &event) {
	if (kernel_counter_log_size == 0) {
		return;
	}
	if (event.kernel_id == 0) {
		return;
	}
	for (auto &counter : kernel_counters) {
		if (counter.kernel_id != event.kernel_id) {
			continue;
		}
		SetJitKernelCounterCandidate(counter, event);
		if (event.phase == "runtime") {
			AccumulateKernelRuntime(counter, event);
		}
		return;
	}
	if (event.phase != "compile" && event.phase != "runtime") {
		return;
	}
	if (event.phase == "compile" && event.status != "compiled") {
		return;
	}
	if (event.phase == "runtime" && event.kernel_compile_reason.empty()) {
		return;
	}
	JitKernelCounter counter;
	counter.kernel_id = event.kernel_id;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.compile_reason = event.phase == "compile" ? event.reason : event.kernel_compile_reason;
	counter.compile_time_us = event.phase == "compile" ? event.compile_time_us : event.kernel_compile_time_us;
	counter.code_size = event.phase == "compile" ? event.code_size : event.kernel_code_size;
	SetJitKernelCounterCandidate(counter, event);
	if (event.phase == "runtime") {
		AccumulateKernelRuntime(counter, event);
	}
	kernel_counters.push_back(std::move(counter));
	TrimKernelCounters(kernel_counter_log_size);
}

void JitEventLog::TrimEvents(idx_t event_log_size) {
	if (event_log_size == 0) {
		events.clear();
		return;
	}
	if (events.size() > event_log_size) {
		events.erase(events.begin(), events.begin() + NumericCast<int64_t>(events.size() - event_log_size));
	}
}

void JitEventLog::TrimKernelCounters(idx_t kernel_counter_log_size) {
	if (kernel_counter_log_size == 0) {
		kernel_counters.clear();
		return;
	}
	if (kernel_counters.size() > kernel_counter_log_size) {
		kernel_counters.erase(kernel_counters.begin(),
		                      kernel_counters.begin() +
		                          NumericCast<int64_t>(kernel_counters.size() - kernel_counter_log_size));
	}
}

idx_t JitEventLog::Record(idx_t event_log_size, bool record_decision_counter, JitEvent event) {
	lock_guard<mutex> guard(lock);
	event.event_id = next_event_id++;
	if (event.kernel_id == 0 && event.status == "compiled") {
		event.kernel_id = event.event_id;
	}
	auto event_id = event.event_id;
	RecordCounter(event);
	if (record_decision_counter) {
		RecordDecisionCounter(event);
	}
	RecordKernelCounter(event_log_size, event);
	if (event_log_size == 0) {
		return event_id;
	}
	events.push_back(std::move(event));
	TrimEvents(event_log_size);
	return event_id;
}

vector<JitEvent> JitEventLog::GetEvents() const {
	lock_guard<mutex> guard(lock);
	return events;
}

vector<JitCounter> JitEventLog::GetCounters() const {
	lock_guard<mutex> guard(lock);
	return counters;
}

vector<JitDecisionCounter> JitEventLog::GetDecisionCounters() const {
	lock_guard<mutex> guard(lock);
	return decision_counters;
}

vector<JitKernelCounter> JitEventLog::GetKernelCounters() const {
	lock_guard<mutex> guard(lock);
	return kernel_counters;
}

void JitEventLog::ClearEvents() {
	lock_guard<mutex> guard(lock);
	events.clear();
	kernel_counters.clear();
}

void JitEventLog::ClearCounters() {
	lock_guard<mutex> guard(lock);
	counters.clear();
	decision_counters.clear();
}

void JitEventLog::ApplyRetentionLimit(idx_t event_log_size) {
	lock_guard<mutex> guard(lock);
	TrimEvents(event_log_size);
	TrimKernelCounters(event_log_size);
}

JitRegionKernel::~JitRegionKernel() {
}

idx_t JitRegionKernel::CodeSize() const {
	return 0;
}

bool JitRegionKernel::HasExecutableBody() const {
	return CodeSize() > 0;
}

void JitRegionKernel::SetTraceInfo(idx_t trace_id_p, JitExecutionMode execution_mode_p, string compile_reason,
                                   int64_t compile_time_us, idx_t code_size) {
	trace_id = trace_id_p;
	execution_mode = execution_mode_p;
	trace_compile_reason = std::move(compile_reason);
	trace_compile_time_us = compile_time_us;
	trace_code_size = code_size;
}

void JitRegionKernel::SetTraceRegionExecutionForm(JitRegionExecutionForm execution_form_p) {
	region_execution_form = execution_form_p;
}

void JitRegionKernel::SetTraceSelectedSourceExecution(JitRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void JitRegionKernel::SetTraceCandidate(const JitRegionCandidate &candidate) {
	has_trace_candidate = true;
	trace_candidate_id = candidate.candidate_id;
	trace_candidate_shape = candidate.shape;
	trace_candidate_scope = JitRegionCandidateScopeToString(candidate.scope);
	trace_candidate_pipeline_shape = candidate.pipeline_shape;
	trace_candidate_context_pipeline_shape = candidate.context_pipeline_shape;
	trace_candidate_node_count = candidate.node_count;
	trace_candidate_start_operator_index = candidate.start_operator_index;
	trace_candidate_end_operator_index = candidate.end_operator_index;
	trace_candidate_estimated_cardinality = candidate.estimated_cardinality;
	trace_candidate_traits = candidate.traits;
	trace_candidate_contract = candidate.contract;
	trace_candidate_output_types = candidate.output_types;
}

idx_t JitRegionKernel::TraceId() const {
	return trace_id;
}

JitExecutionMode JitRegionKernel::ExecutionMode() const {
	return execution_mode;
}

JitRegionExecutionForm JitRegionKernel::RegionExecutionForm() const {
	return region_execution_form;
}

JitRegionSourceExecutionKind JitRegionKernel::SelectedSourceExecution() const {
	return selected_source_execution;
}

const string &JitRegionKernel::TraceCompileReason() const {
	return trace_compile_reason;
}

int64_t JitRegionKernel::TraceCompileTime() const {
	return trace_compile_time_us;
}

idx_t JitRegionKernel::TraceCodeSize() const {
	return trace_code_size;
}

bool JitRegionKernel::HasTraceCandidate() const {
	return has_trace_candidate;
}

idx_t JitRegionKernel::TraceCandidateId() const {
	return trace_candidate_id;
}

const string &JitRegionKernel::TraceCandidateShape() const {
	return trace_candidate_shape;
}

const string &JitRegionKernel::TraceCandidateScope() const {
	return trace_candidate_scope;
}

const string &JitRegionKernel::TraceCandidatePipelineShape() const {
	return trace_candidate_pipeline_shape;
}

const string &JitRegionKernel::TraceCandidateContextPipelineShape() const {
	return trace_candidate_context_pipeline_shape;
}

idx_t JitRegionKernel::TraceCandidateNodeCount() const {
	return trace_candidate_node_count;
}

idx_t JitRegionKernel::TraceCandidateStartOperatorIndex() const {
	return trace_candidate_start_operator_index;
}

idx_t JitRegionKernel::TraceCandidateEndOperatorIndex() const {
	return trace_candidate_end_operator_index;
}

idx_t JitRegionKernel::TraceCandidateEstimatedCardinality() const {
	return trace_candidate_estimated_cardinality;
}

const JitRegionCandidateTraits &JitRegionKernel::TraceCandidateTraits() const {
	return trace_candidate_traits;
}

const JitRegionContract &JitRegionKernel::TraceCandidateContract() const {
	return trace_candidate_contract;
}

const vector<LogicalType> &JitRegionKernel::TraceCandidateOutputTypes() const {
	return trace_candidate_output_types;
}

bool JitRegionKernel::CanExecuteFullPipeline() const {
	return false;
}

bool JitRegionKernel::RequiresNativeSource() const {
	return false;
}

bool JitRegionKernel::CanEnterFullPipeline(JitFullPipelineRuntime &, string &) {
	return true;
}

bool JitRegionKernel::TryExecuteFullPipeline(JitFullPipelineRuntime &, JitFullPipelineResult &) {
	return false;
}

JitRegionCompilationInput::JitRegionCompilationInput(ClientContext &context, const JitRegionIR &region_ir,
                                                     const JitRegionCandidate &candidate)
    : context(context), region_ir(region_ir), candidate(candidate) {
}

JitRegionCompileResult JitRegionCompileResult::Compiled(unique_ptr<JitRegionKernel> kernel,
                                                        JitExecutionMode execution_mode, string reason, string ir) {
	if (!kernel) {
		throw InternalException("JIT region compile result marked compiled without a kernel");
	}
	if (execution_mode == JitExecutionMode::NONE || execution_mode == JitExecutionMode::UNSUPPORTED ||
	    execution_mode == JitExecutionMode::EXECUTOR_FALLBACK) {
		throw InternalException("JIT region compile result uses invalid compiled execution mode");
	}
	JitRegionCompileResult result;
	result.status = JitCompileStatus::COMPILED;
	result.execution_mode = execution_mode;
	result.reason = std::move(reason);
	result.ir = std::move(ir);
	result.kernel = std::move(kernel);
	return result;
}

JitRegionCompileResult JitRegionCompileResult::Unsupported(string reason) {
	JitRegionCompileResult result;
	result.status = JitCompileStatus::UNSUPPORTED;
	result.execution_mode = JitExecutionMode::UNSUPPORTED;
	result.reason = std::move(reason);
	return result;
}

JitRegionCompileResult JitRegionCompileResult::Unavailable(string reason) {
	JitRegionCompileResult result;
	result.status = JitCompileStatus::UNAVAILABLE;
	result.execution_mode = JitExecutionMode::NONE;
	result.reason = std::move(reason);
	return result;
}

JitRegionCompileResult JitRegionCompileResult::Error(string reason) {
	JitRegionCompileResult result;
	result.status = JitCompileStatus::ERROR;
	result.execution_mode = JitExecutionMode::NONE;
	result.reason = std::move(reason);
	return result;
}

JitBackend::~JitBackend() {
}

bool JitBackend::IsAvailable() const {
	return true;
}

bool JitBackend::SupportsRegions() const {
	return false;
}

JitRegionLoweringPlan JitBackend::AnalyzeRegion(const JitRegionCompilationInput &) {
	JitRegionLoweringPlan plan;
	plan.SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED);
	plan.AddNode("region", "unknown", JitLoweringKind::FALLBACK, "backend does not analyze regions");
	return plan;
}

JitRegionCompileResult JitBackend::CompileRegion(const JitRegionCompilationInput &) {
	return JitRegionCompileResult::Unsupported("backend does not compile regions");
}

bool JitBackend::MayHaveAutoAdmissionRule(JitCompileTarget, const JitRegionPipelineInventory &, bool,
                                          JitAdmissionInfo &, string &) const {
	return true;
}

bool JitBackend::MayHaveAutoAdmissionRule(JitCompileTarget, const JitRegionCandidate &, JitAdmissionInfo &,
                                          string &) const {
	return true;
}

bool JitBackend::GetAutoAdmissionRule(JitCompileTarget, const JitRegionCandidate &, const JitRegionLoweringPlan &,
                                      JitAutoAdmissionRule &) const {
	return false;
}

JitBackendPlan::~JitBackendPlan() {
}

} // namespace duckdb

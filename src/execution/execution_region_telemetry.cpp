#include "duckdb/execution/execution_region_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

static bool ShouldRecordExecutionRegionDecisionCounters(ClientContext &context) {
	return ExecutionRegionSettings::ShouldRecordDecisionCounters(context);
}

vector<ExecutionRegionEvent> ExecutionRegionManager::GetEvents() const {
	return event_log.GetEvents();
}

vector<ExecutionRegionCounter> ExecutionRegionManager::GetCounters() const {
	return event_log.GetCounters();
}

vector<ExecutionRegionDecisionCounter> ExecutionRegionManager::GetDecisionCounters() const {
	return event_log.GetDecisionCounters();
}

vector<ExecutionRegionKernelCounter> ExecutionRegionManager::GetKernelCounters() const {
	return event_log.GetKernelCounters();
}

void ExecutionRegionManager::ClearEvents() {
	event_log.ClearEvents();
}

void ExecutionRegionManager::ClearCounters() {
	event_log.ClearCounters();
}

void ExecutionRegionManager::ApplyEventRetentionLimit(idx_t event_log_size) {
	event_log.ApplyRetentionLimit(event_log_size);
}

idx_t ExecutionRegionManager::RecordEvent(
    ClientContext &context, string backend_name, ExecutionRegionCompileTarget target,
    ExecutionRegionCompileStatus status, ExecutionRegionExecutionMode execution_mode, string policy_decision,
    string reason, string ir, int64_t decision_time_us, int64_t compile_time_us, idx_t code_size,
    const ExecutionRegionCandidate *candidate, const ExecutionRegionAdmissionInfo *admission,
    const ExecutionRegionStageTimings *stage_timings, ExecutionRegionForm region_execution_form,
    ExecutionRegionSourceExecutionKind selected_source_execution, const ExecutionRegionPipelineInventory *inventory) {
	if (context.IsCompiledExecutionSuppressed()) {
		return 0;
	}
	ExecutionRegionEvent event;
	if (inventory) {
		event.has_pipeline = true;
		event.pipeline_shape = inventory->pipeline_shape;
		event.pipeline_estimated_cardinality = inventory->estimated_cardinality;
	}
	if (candidate) {
		event.has_candidate = true;
		event.candidate_id = candidate->candidate_id;
		event.candidate_shape = candidate->shape;
		event.candidate_pipeline_shape = candidate->pipeline_shape;
		event.candidate_context_pipeline_shape = candidate->context_pipeline_shape;
		event.candidate_signature = candidate->signature;
		if (!ExecutionRegionSettings::DumpIR(context)) {
			event.candidate_signature.ir.clear();
		}
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
	event.phase = status == ExecutionRegionCompileStatus::COMPILED || status == ExecutionRegionCompileStatus::ERROR
	                  ? "compile"
	                  : "decision";
	event.backend_name = std::move(backend_name);
	event.target = ExecutionRegionCompileTargetToString(target);
	event.status = ExecutionRegionCompileStatusToString(status);
	event.execution_mode = ExecutionRegionExecutionModeToString(execution_mode);
	event.region_execution_form = ExecutionRegionFormToString(region_execution_form);
	event.execution_body = ExecutionRegionExecutionBodyToString(
	    ExecutionRegionExecutionBodyForCompileEvent(status, execution_mode, code_size));
	event.selected_source_execution = selected_source_execution;
	event.policy_decision = std::move(policy_decision);
	event.reason = std::move(reason);
	event.ir = ExecutionRegionSettings::DumpIR(context) ? std::move(ir) : string();
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

	auto event_log_size = ExecutionRegionSettings::EventLogSize(db);
	return event_log.Record(event_log_size, ShouldRecordExecutionRegionDecisionCounters(context), std::move(event));
}

template <class KERNEL>
static ExecutionRegionEvent BuildRuntimeEvent(const KERNEL &kernel, ExecutionRegionCompileTarget target,
                                              ExecutionRegionExecutionMode execution_mode, string status, string reason,
                                              idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
                                              string runtime_result) {
	ExecutionRegionEvent event;
	event.phase = "runtime";
	event.backend_name = kernel.BackendName();
	event.target = ExecutionRegionCompileTargetToString(target);
	event.status = std::move(status);
	event.execution_mode = ExecutionRegionExecutionModeToString(execution_mode);
	event.region_execution_form = ExecutionRegionFormToString(ExecutionRegionForm::NONE);
	event.execution_body = ExecutionRegionExecutionBodyToString(kernel.ExecutionBody());
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

static void SetRuntimeRegionCandidate(ExecutionRegionEvent &event, const ExecutionRegionKernel &kernel) {
	if (!kernel.HasTraceCandidate()) {
		return;
	}
	event.has_candidate = true;
	event.candidate_id = kernel.TraceCandidateId();
	event.candidate_shape = kernel.TraceCandidateShape();
	event.candidate_pipeline_shape = kernel.TraceCandidatePipelineShape();
	event.candidate_context_pipeline_shape = kernel.TraceCandidateContextPipelineShape();
	event.candidate_signature = kernel.TraceCandidateSignature();
	event.candidate_node_count = kernel.TraceCandidateNodeCount();
	event.candidate_start_operator_index = kernel.TraceCandidateStartOperatorIndex();
	event.candidate_end_operator_index = kernel.TraceCandidateEndOperatorIndex();
	event.candidate_estimated_cardinality = kernel.TraceCandidateEstimatedCardinality();
	event.candidate_traits = kernel.TraceCandidateTraits();
	event.candidate_contract = kernel.TraceCandidateContract();
}

static void SetRuntimeRegionExecutionForm(ExecutionRegionEvent &event, const ExecutionRegionKernel &kernel) {
	event.region_execution_form = ExecutionRegionFormToString(kernel.RegionExecutionForm());
	event.selected_source_execution = kernel.SelectedSourceExecution();
}

static void SetRuntimeMetrics(ExecutionRegionEvent &event, const ExecutionRegionRuntimeMetrics &metrics) {
	event.source_contract_output_rows = metrics.source_contract_output_rows;
	event.source_contract_invocation_count = metrics.source_contract_invocation_count;
	event.source_contract_runtime_time_us = metrics.source_contract_runtime_time_us;
	event.generated_body_runtime_time_us = metrics.generated_body_runtime_time_us;
	event.generated_stage_runtime_breakdown = metrics.generated_stage_runtime_breakdown;
}

void ExecutionRegionManager::RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
                                                ExecutionRegionCompileTarget target, string status, string reason,
                                                idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
                                                string runtime_result) {
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::TraceRuntime(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, target, kernel.ExecutionMode(), std::move(status), std::move(reason),
	                               input_rows, output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeRegionExecutionForm(event, kernel);
	SetRuntimeRegionCandidate(event, kernel);
	if (!ExecutionRegionSettings::DumpIR(context)) {
		event.candidate_signature.ir.clear();
	}
	auto event_log_size = ExecutionRegionSettings::EventLogSize(db);
	event_log.Record(event_log_size, ShouldRecordExecutionRegionDecisionCounters(context), std::move(event));
}

void ExecutionRegionManager::RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
                                                ExecutionRegionCompileTarget target, string status, string reason,
                                                idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
                                                string runtime_result,
                                                const ExecutionRegionRuntimeMetrics &runtime_metrics) {
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::TraceRuntime(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, target, kernel.ExecutionMode(), std::move(status), std::move(reason),
	                               input_rows, output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeMetrics(event, runtime_metrics);
	SetRuntimeRegionExecutionForm(event, kernel);
	SetRuntimeRegionCandidate(event, kernel);
	if (!ExecutionRegionSettings::DumpIR(context)) {
		event.candidate_signature.ir.clear();
	}
	auto event_log_size = ExecutionRegionSettings::EventLogSize(db);
	event_log.Record(event_log_size, ShouldRecordExecutionRegionDecisionCounters(context), std::move(event));
}

} // namespace duckdb

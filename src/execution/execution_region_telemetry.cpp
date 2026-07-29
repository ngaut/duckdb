#include "duckdb/execution/execution_region_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

static bool ShouldCaptureExecutionRegionQueryProfile(ClientContext &context) {
	if (!context.QueryProfilerAcceptsExecutionRegionEvents()) {
		return false;
	}
	return context.QueryProfilerIsExplainAnalyze() || ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
}

const char *ExecutionRegionEventPhaseToString(ExecutionRegionEventPhase phase) {
	switch (phase) {
	case ExecutionRegionEventPhase::DECISION:
		return "decision";
	case ExecutionRegionEventPhase::COMPILE:
		return "compile";
	case ExecutionRegionEventPhase::RUNTIME:
		return "runtime";
	default:
		return "none";
	}
}

const char *ExecutionRegionEventStatusToString(ExecutionRegionEventStatus status) {
	switch (status) {
	case ExecutionRegionEventStatus::COMPILED:
		return "compiled";
	case ExecutionRegionEventStatus::SKIPPED:
		return "skipped";
	case ExecutionRegionEventStatus::UNSUPPORTED:
		return "unsupported";
	case ExecutionRegionEventStatus::UNAVAILABLE:
		return "unavailable";
	case ExecutionRegionEventStatus::DISABLED:
		return "disabled";
	case ExecutionRegionEventStatus::ERROR:
		return "error";
	case ExecutionRegionEventStatus::EXECUTED:
		return "executed";
	case ExecutionRegionEventStatus::SOURCE_CONTRACT:
		return "source_contract";
	default:
		return "none";
	}
}

ExecutionRegionEventStatus ExecutionRegionEventStatusFromCompileStatus(ExecutionRegionCompileStatus status) {
	switch (status) {
	case ExecutionRegionCompileStatus::COMPILED:
		return ExecutionRegionEventStatus::COMPILED;
	case ExecutionRegionCompileStatus::SKIPPED:
		return ExecutionRegionEventStatus::SKIPPED;
	case ExecutionRegionCompileStatus::UNSUPPORTED:
		return ExecutionRegionEventStatus::UNSUPPORTED;
	case ExecutionRegionCompileStatus::UNAVAILABLE:
		return ExecutionRegionEventStatus::UNAVAILABLE;
	case ExecutionRegionCompileStatus::DISABLED:
		return ExecutionRegionEventStatus::DISABLED;
	case ExecutionRegionCompileStatus::ERROR:
		return ExecutionRegionEventStatus::ERROR;
	default:
		return ExecutionRegionEventStatus::NONE;
	}
}

static void SetRecordedExecutionRegionEventId(ExecutionRegionEvent &event, idx_t event_id) {
	event.event_id = event_id;
	if (event.kernel_id == 0 && event.status_kind == ExecutionRegionEventStatus::COMPILED) {
		event.kernel_id = event_id;
	}
}

static ExecutionRunnerKind ExecutionRegionRunnerFromExecutionMode(ExecutionRegionExecutionMode execution_mode) {
	switch (execution_mode) {
	case ExecutionRegionExecutionMode::GPU:
		return ExecutionRunnerKind::COMPILED_GPU;
	case ExecutionRegionExecutionMode::NATIVE:
		return ExecutionRunnerKind::COMPILED_VECTORIZED;
	default:
		return ExecutionRunnerKind::VECTORIZED;
	}
}

static void SetDetailedExecutionRegionCandidate(ExecutionRegionEvent &event,
                                                const ExecutionRegionCandidate &candidate) {
	event.has_candidate = true;
	event.candidate_shape = candidate.shape;
	event.candidate_signature = candidate.signature;
	event.candidate_estimated_cardinality = candidate.estimated_cardinality;
	event.candidate_traits = candidate.traits;
	event.candidate_abi = candidate.abi;
}

bool ExecutionRegionEventIsRuntime(const ExecutionRegionEvent &event) {
	return event.phase_kind == ExecutionRegionEventPhase::RUNTIME;
}

bool ExecutionRegionEventWasInvoked(const ExecutionRegionEvent &event) {
	return event.kernel_id > 0 && (event.invocation_count > 0 || event.source_contract_invocation_count > 0 ||
	                               event.runtime_time_us > 0 || event.source_contract_runtime_time_us > 0);
}

bool ExecutionRegionEventIsVisibleInQueryProfile(const ExecutionRegionEvent &event) {
	return !ExecutionRegionEventIsRuntime(event) || ExecutionRegionEventWasInvoked(event);
}

const string &ExecutionRegionEventPipelineShape(const ExecutionRegionEvent &event) {
	return event.pipeline_shape;
}

idx_t ExecutionRegionEventEstimatedCardinality(const ExecutionRegionEvent &event) {
	return event.candidate_estimated_cardinality > 0 ? event.candidate_estimated_cardinality
	                                                 : event.pipeline_estimated_cardinality;
}

idx_t ExecutionRegionEventProfileCodeSize(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventIsRuntime(event) ? event.kernel_code_size : event.code_size;
}

int64_t ExecutionRegionEventProfileCompileTime(const ExecutionRegionEvent &event) {
	return ExecutionRegionEventIsRuntime(event) ? event.kernel_compile_time_us : event.compile_time_us;
}

static void SummarizeRuntimeEvent(ExecutionRegionTraceSummary &summary, unordered_set<idx_t> &runtime_kernels,
                                  const ExecutionRegionEvent &event) {
	summary.runtime_events++;
	summary.source_us += event.source_contract_runtime_time_us;
	if (event.status_kind != ExecutionRegionEventStatus::SOURCE_CONTRACT) {
		summary.runtime_us += event.runtime_time_us;
		summary.sink_us += event.sink_next_batch_runtime_time_us;
		summary.generated_us += event.generated_body_runtime_time_us;
	}
	if (ExecutionRegionEventWasInvoked(event)) {
		runtime_kernels.insert(event.kernel_id);
	}
}

static void SummarizeDecisionEvent(ExecutionRegionTraceSummary &summary, const ExecutionRegionEvent &event) {
	summary.decisions++;
	summary.decision_us += event.decision_time_us;
	summary.compile_us += event.compile_time_us;
	summary.code_size += event.code_size;
	summary.pipeline_cbo_us += event.stage_timings.pipeline_cbo_time_us;
	summary.graph_build_us += event.stage_timings.graph_build_time_us;
	summary.candidate_cbo_us += event.stage_timings.candidate_cbo_time_us;
	summary.ir_lowering_us += event.stage_timings.ir_lowering_time_us;
	summary.backend_analysis_us += event.stage_timings.backend_analysis_time_us;
	summary.codegen_us += event.stage_timings.codegen_time_us;
	summary.executable_build_us += event.stage_timings.executable_build_time_us;
	summary.machine_codegen_us += event.stage_timings.machine_codegen_time_us;
	summary.kernel_build_us += event.stage_timings.kernel_build_time_us;
	if (event.status_kind == ExecutionRegionEventStatus::COMPILED) {
		summary.compiled++;
	} else if (event.status_kind == ExecutionRegionEventStatus::ERROR) {
		summary.compile_errors++;
	} else if (event.status_kind == ExecutionRegionEventStatus::UNSUPPORTED) {
		summary.unsupported++;
	} else if (event.status_kind == ExecutionRegionEventStatus::SKIPPED) {
		summary.skipped++;
	} else if (event.status_kind == ExecutionRegionEventStatus::UNAVAILABLE) {
		summary.unavailable++;
	} else if (event.status_kind == ExecutionRegionEventStatus::DISABLED) {
		summary.disabled++;
	}
}

ExecutionRegionTraceSummary SummarizeExecutionRegionTrace(const vector<ExecutionRegionEvent> &trace) {
	ExecutionRegionTraceSummary summary;
	unordered_set<idx_t> runtime_kernels;
	for (const auto &event : trace) {
		AddExecutionRegionLazyCodegenMetrics(summary.lazy_codegen, event.jit_runtime.lazy_codegen);
		if (ExecutionRegionEventIsRuntime(event)) {
			SummarizeRuntimeEvent(summary, runtime_kernels, event);
		} else {
			SummarizeDecisionEvent(summary, event);
		}
	}
	summary.runtime_regions = runtime_kernels.size();
	return summary;
}

static idx_t RecordExecutionRegionTelemetryEvent(ClientContext &context, DatabaseInstance &db,
                                                 ExecutionRegionEventLog &event_log, ExecutionRegionEvent event) {
	auto should_capture_query_profile = ShouldCaptureExecutionRegionQueryProfile(context);
	auto event_log_size = ExecutionRegionSettings::EventLogSize(db);
	ExecutionRegionEvent query_profile_event;
	if (should_capture_query_profile) {
		query_profile_event = event;
	}
	auto event_id = event_log.Record(event_log_size, std::move(event));
	if (should_capture_query_profile) {
		SetRecordedExecutionRegionEventId(query_profile_event, event_id);
		context.RecordExecutionRegionProfileEvent(query_profile_event);
	}
	return event_id;
}

vector<ExecutionRegionEvent> ExecutionRegionManager::GetEvents() const {
	return event_log.GetEvents();
}

vector<ExecutionRegionCounter> ExecutionRegionManager::GetCounters() const {
	return event_log.GetCounters();
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
    ClientContext &context, string backend_name, ExecutionRegionCompileStatus status,
    ExecutionRegionExecutionMode execution_mode, string reason, string blocker, const string *ir,
    int64_t decision_time_us, int64_t compile_time_us, idx_t code_size, const string *pipeline_shape,
    const ExecutionRegionCandidate *candidate, ExecutionRunnerKind selected_runner,
    const ExecutionRegionStageTimings *stage_timings, ExecutionRegionSourceExecutionKind selected_source_execution,
    ExecutionRegionScanFilterMode selected_scan_filter_mode, const PhysicalRunnerCostProfile *runner_cost) {
	if (context.IsCompiledExecutionSuppressed()) {
		return 0;
	}
	ExecutionRegionEvent event;
	if (candidate && ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context)) {
		SetDetailedExecutionRegionCandidate(event, *candidate);
	}
	if (pipeline_shape && !pipeline_shape->empty()) {
		event.has_pipeline = true;
		event.pipeline_shape = *pipeline_shape;
		if (candidate) {
			event.pipeline_estimated_cardinality = candidate->estimated_cardinality;
		}
	}
	event.selected_runner = selected_runner;
	if (runner_cost) {
		event.runner_cost = *runner_cost;
		if (!event.has_pipeline && event.runner_cost.rows > 0) {
			event.has_pipeline = true;
			event.pipeline_estimated_cardinality = static_cast<idx_t>(event.runner_cost.rows);
		}
	}
	event.phase_kind = status == ExecutionRegionCompileStatus::COMPILED || status == ExecutionRegionCompileStatus::ERROR
	                       ? ExecutionRegionEventPhase::COMPILE
	                       : ExecutionRegionEventPhase::DECISION;
	event.backend_name = std::move(backend_name);
	event.status_kind = ExecutionRegionEventStatusFromCompileStatus(status);
	event.execution_mode_kind = execution_mode;
	event.selected_source_execution = selected_source_execution;
	event.selected_uses_scan_filters = selected_scan_filter_mode != ExecutionRegionScanFilterMode::NONE;
	event.reason = std::move(reason);
	event.blocker = std::move(blocker);
	if (ir && ExecutionRegionSettings::DumpIR(context)) {
		event.ir = *ir;
	}
	event.decision_time_us = decision_time_us;
	event.compile_time_us = compile_time_us;
	event.code_size = code_size;
	if (stage_timings) {
		event.stage_timings = *stage_timings;
	}

	return RecordExecutionRegionTelemetryEvent(context, db, event_log, std::move(event));
}

template <class KERNEL>
static ExecutionRegionEvent BuildRuntimeEvent(const KERNEL &kernel, ExecutionRegionExecutionMode execution_mode,
                                              ExecutionRegionEventStatus status, string reason, idx_t input_rows,
                                              idx_t output_rows, int64_t runtime_time_us, string runtime_result) {
	ExecutionRegionEvent event;
	event.phase_kind = ExecutionRegionEventPhase::RUNTIME;
	event.backend_name = kernel.BackendName();
	event.status_kind = status;
	event.execution_mode_kind = execution_mode;
	event.selected_runner = ExecutionRegionRunnerFromExecutionMode(execution_mode);
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

static void SetRuntimeRegionPipeline(ExecutionRegionEvent &event, const ExecutionRegionKernel &kernel) {
	if (!kernel.HasTracePipeline()) {
		return;
	}
	event.has_pipeline = true;
	event.pipeline_shape = kernel.TracePipelineShape();
	event.pipeline_estimated_cardinality = kernel.TraceCandidateEstimatedCardinality();
	event.candidate_shape = kernel.TraceCandidateShape();
	event.candidate_estimated_cardinality = kernel.TraceCandidateEstimatedCardinality();
}

static void SetRuntimeRegionSourceTrace(ExecutionRegionEvent &event, const ExecutionRegionKernel &kernel) {
	event.selected_source_execution = kernel.SelectedSourceExecution();
	event.selected_uses_scan_filters = kernel.UsesScanFilters();
}

static void SetRuntimeMetrics(ExecutionRegionEvent &event, const ExecutionRegionRuntimeMetrics &metrics) {
	event.source_contract_output_rows = metrics.source_contract_output_rows;
	event.source_contract_invocation_count = metrics.source_contract_invocation_count;
	event.source_contract_runtime_time_us = metrics.source_contract_runtime_time_us;
	event.source_stage_runtime = metrics.source_stage_runtime;
	event.sink_next_batch_invocation_count = metrics.sink_next_batch_invocation_count;
	event.sink_next_batch_runtime_time_us = metrics.sink_next_batch_runtime_time_us;
	event.generated_body_runtime_time_us = metrics.generated_body_runtime_time_us;
	event.generated_stage_runtime = metrics.generated_stage_runtime;
	event.jit_runtime = metrics.jit_runtime;
}

void ExecutionRegionManager::RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
                                                ExecutionRegionEventStatus status, string reason, idx_t input_rows,
                                                idx_t output_rows, int64_t runtime_time_us, string runtime_result) {
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::TraceRuntime(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, kernel.ExecutionMode(), std::move(status), std::move(reason), input_rows,
	                               output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeRegionSourceTrace(event, kernel);
	SetRuntimeRegionPipeline(event, kernel);
	RecordExecutionRegionTelemetryEvent(context, db, event_log, std::move(event));
}

void ExecutionRegionManager::RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
                                                ExecutionRegionEventStatus status, string reason, idx_t input_rows,
                                                idx_t output_rows, int64_t runtime_time_us, string runtime_result,
                                                const ExecutionRegionRuntimeMetrics &runtime_metrics) {
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::TraceRuntime(context)) {
		return;
	}
	auto event = BuildRuntimeEvent(kernel, kernel.ExecutionMode(), std::move(status), std::move(reason), input_rows,
	                               output_rows, runtime_time_us, std::move(runtime_result));
	SetRuntimeMetrics(event, runtime_metrics);
	SetRuntimeRegionSourceTrace(event, kernel);
	SetRuntimeRegionPipeline(event, kernel);
	RecordExecutionRegionTelemetryEvent(context, db, event_log, std::move(event));
}

void ExecutionRegionManager::RecordVectorizedBaselineRuntimeEvent(ClientContext &context,
                                                                  const ExecutionRegionKernel &kernel, string reason,
                                                                  int64_t runtime_time_us, string runtime_result) {
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::TraceRuntime(context)) {
		return;
	}
	auto event =
	    BuildRuntimeEvent(kernel, ExecutionRegionExecutionMode::VECTORIZED, ExecutionRegionEventStatus::EXECUTED,
	                      std::move(reason), 0, 0, runtime_time_us, std::move(runtime_result));
	SetRuntimeRegionSourceTrace(event, kernel);
	SetRuntimeRegionPipeline(event, kernel);
	RecordExecutionRegionTelemetryEvent(context, db, event_log, std::move(event));
}

} // namespace duckdb

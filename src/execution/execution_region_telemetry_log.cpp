#include "duckdb/execution/execution_region_telemetry.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

ExecutionRegionSuppressionGuard::ExecutionRegionSuppressionGuard(ClientContext &context_p) : context(context_p) {
	context.PushCompiledExecutionSuppression();
}

ExecutionRegionSuppressionGuard::~ExecutionRegionSuppressionGuard() {
	context.PopCompiledExecutionSuppression();
}

static void AppendExecutionRegionStageRuntimeBreakdown(string &target, const string &source) {
	if (source.empty()) {
		return;
	}
	if (!target.empty()) {
		target += ";";
	}
	target += source;
}

void ExecutionRegionEventLog::RecordCounter(const ExecutionRegionEvent &event) {
	for (auto &counter : counters) {
		if (counter.backend_name != event.backend_name || counter.target != event.target ||
		    counter.status != event.status || counter.execution_mode != event.execution_mode ||
		    counter.region_execution_form != event.region_execution_form ||
		    counter.execution_body != event.execution_body || counter.policy_decision != event.policy_decision) {
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
		counter.source_contract_output_rows += event.source_contract_output_rows;
		counter.source_contract_invocation_count += event.source_contract_invocation_count;
		counter.source_contract_runtime_time_us += event.source_contract_runtime_time_us;
		counter.generated_body_runtime_time_us += event.generated_body_runtime_time_us;
		AppendExecutionRegionStageRuntimeBreakdown(counter.generated_stage_runtime_breakdown,
		                                           event.generated_stage_runtime_breakdown);
		counter.ir_lowering_time_us += event.ir_lowering_time_us;
		counter.backend_analysis_time_us += event.backend_analysis_time_us;
		counter.admission_time_us += event.admission_time_us;
		counter.overlap_check_time_us += event.overlap_check_time_us;
		counter.codegen_time_us += event.codegen_time_us;
		return;
	}
	ExecutionRegionCounter counter;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.status = event.status;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.execution_body = event.execution_body;
	counter.policy_decision = event.policy_decision;
	counter.count = 1;
	counter.decision_time_us = event.decision_time_us;
	counter.compile_time_us = event.compile_time_us;
	counter.code_size = event.code_size;
	counter.input_rows = event.input_rows;
	counter.output_rows = event.output_rows;
	counter.invocation_count = event.invocation_count;
	counter.runtime_time_us = event.runtime_time_us;
	counter.source_contract_output_rows = event.source_contract_output_rows;
	counter.source_contract_invocation_count = event.source_contract_invocation_count;
	counter.source_contract_runtime_time_us = event.source_contract_runtime_time_us;
	counter.generated_body_runtime_time_us = event.generated_body_runtime_time_us;
	counter.generated_stage_runtime_breakdown = event.generated_stage_runtime_breakdown;
	counter.ir_lowering_time_us = event.ir_lowering_time_us;
	counter.backend_analysis_time_us = event.backend_analysis_time_us;
	counter.admission_time_us = event.admission_time_us;
	counter.overlap_check_time_us = event.overlap_check_time_us;
	counter.codegen_time_us = event.codegen_time_us;
	counters.push_back(std::move(counter));
}

static bool ExecutionRegionDecisionCounterMatches(const ExecutionRegionDecisionCounter &counter,
                                                  const ExecutionRegionEvent &event) {
	return counter.backend_name == event.backend_name && counter.target == event.target &&
	       counter.phase == event.phase && counter.status == event.status &&
	       counter.execution_mode == event.execution_mode &&
	       counter.region_execution_form == event.region_execution_form &&
	       counter.execution_body == event.execution_body && counter.policy_decision == event.policy_decision &&
	       counter.has_pipeline == event.has_pipeline && counter.pipeline_shape == event.pipeline_shape &&
	       counter.candidate_shape == event.candidate_shape &&
	       counter.candidate_signature.context == event.candidate_signature.context &&
	       counter.candidate_signature.shape == event.candidate_signature.shape &&
	       counter.candidate_signature.feature_shape == event.candidate_signature.feature_shape &&
	       counter.candidate_signature.context_feature_shape == event.candidate_signature.context_feature_shape &&
	       counter.candidate_signature.contract_shape == event.candidate_signature.contract_shape &&
	       counter.admission_shape_key == event.admission_shape_key &&
	       counter.admission_rule_present == event.admission_rule_present &&
	       counter.admission_min_cardinality == event.admission_min_cardinality &&
	       counter.admission_proof == event.admission_proof &&
	       counter.candidate_traits.ir == event.candidate_traits.ir &&
	       counter.candidate_contract.ir == event.candidate_contract.ir;
}

static void AccumulateExecutionRegionDecisionCounter(ExecutionRegionDecisionCounter &counter,
                                                     const ExecutionRegionEvent &event) {
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

void ExecutionRegionEventLog::RecordDecisionCounter(const ExecutionRegionEvent &event) {
	for (auto &counter : decision_counters) {
		if (!ExecutionRegionDecisionCounterMatches(counter, event)) {
			continue;
		}
		AccumulateExecutionRegionDecisionCounter(counter, event);
		return;
	}
	ExecutionRegionDecisionCounter counter;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.phase = event.phase;
	counter.status = event.status;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.execution_body = event.execution_body;
	counter.policy_decision = event.policy_decision;
	counter.has_pipeline = event.has_pipeline;
	counter.pipeline_shape = event.pipeline_shape;
	counter.candidate_shape = event.candidate_shape;
	counter.candidate_signature = event.candidate_signature;
	counter.admission_shape_key = event.admission_shape_key;
	counter.admission_rule_present = event.admission_rule_present;
	counter.admission_min_cardinality = event.admission_min_cardinality;
	counter.admission_proof = event.admission_proof;
	counter.candidate_traits = event.candidate_traits;
	counter.candidate_contract = event.candidate_contract;
	counter.example_reason = event.reason;
	AccumulateExecutionRegionDecisionCounter(counter, event);
	decision_counters.push_back(std::move(counter));
}

void ExecutionRegionEventLog::AccumulateKernelRuntime(ExecutionRegionKernelCounter &counter,
                                                      const ExecutionRegionEvent &event) {
	counter.last_runtime_status = event.status;
	counter.last_runtime_result = event.runtime_result;
	if (event.status == "source_contract") {
		counter.source_contract_output_rows += event.source_contract_output_rows;
		counter.source_contract_invocation_count += event.source_contract_invocation_count;
		counter.source_contract_runtime_time_us += event.source_contract_runtime_time_us;
		return;
	}
	counter.input_rows += event.input_rows;
	counter.output_rows += event.output_rows;
	counter.invocation_count += event.invocation_count;
	counter.runtime_time_us += event.runtime_time_us;
	counter.source_contract_output_rows += event.source_contract_output_rows;
	counter.source_contract_invocation_count += event.source_contract_invocation_count;
	counter.source_contract_runtime_time_us += event.source_contract_runtime_time_us;
	counter.generated_body_runtime_time_us += event.generated_body_runtime_time_us;
	AppendExecutionRegionStageRuntimeBreakdown(counter.generated_stage_runtime_breakdown,
	                                           event.generated_stage_runtime_breakdown);
}

static void SetExecutionRegionKernelCounterCandidate(ExecutionRegionKernelCounter &counter,
                                                     const ExecutionRegionEvent &event) {
	if (!event.has_candidate) {
		return;
	}
	counter.has_candidate = true;
	counter.candidate_id = event.candidate_id;
	counter.candidate_shape = event.candidate_shape;
	counter.candidate_pipeline_shape = event.candidate_pipeline_shape;
	counter.candidate_context_pipeline_shape = event.candidate_context_pipeline_shape;
	counter.candidate_signature = event.candidate_signature;
	counter.candidate_node_count = event.candidate_node_count;
	counter.candidate_start_operator_index = event.candidate_start_operator_index;
	counter.candidate_end_operator_index = event.candidate_end_operator_index;
	counter.candidate_estimated_cardinality = event.candidate_estimated_cardinality;
	counter.candidate_traits = event.candidate_traits;
	counter.candidate_contract = event.candidate_contract;
}

void ExecutionRegionEventLog::RecordKernelCounter(idx_t kernel_counter_log_size, const ExecutionRegionEvent &event) {
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
		SetExecutionRegionKernelCounterCandidate(counter, event);
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
	ExecutionRegionKernelCounter counter;
	counter.kernel_id = event.kernel_id;
	counter.backend_name = event.backend_name;
	counter.target = event.target;
	counter.execution_mode = event.execution_mode;
	counter.region_execution_form = event.region_execution_form;
	counter.execution_body = event.execution_body;
	counter.compile_reason = event.phase == "compile" ? event.reason : event.kernel_compile_reason;
	counter.compile_time_us = event.phase == "compile" ? event.compile_time_us : event.kernel_compile_time_us;
	counter.code_size = event.phase == "compile" ? event.code_size : event.kernel_code_size;
	SetExecutionRegionKernelCounterCandidate(counter, event);
	if (event.phase == "runtime") {
		AccumulateKernelRuntime(counter, event);
	}
	kernel_counters.push_back(std::move(counter));
	TrimKernelCounters(kernel_counter_log_size);
}

void ExecutionRegionEventLog::TrimEvents(idx_t event_log_size) {
	if (event_log_size == 0) {
		events.clear();
		return;
	}
	if (events.size() > event_log_size) {
		events.erase(events.begin(), events.begin() + NumericCast<int64_t>(events.size() - event_log_size));
	}
}

void ExecutionRegionEventLog::TrimKernelCounters(idx_t kernel_counter_log_size) {
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

idx_t ExecutionRegionEventLog::Record(idx_t event_log_size, bool record_decision_counter, ExecutionRegionEvent event) {
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

vector<ExecutionRegionEvent> ExecutionRegionEventLog::GetEvents() const {
	lock_guard<mutex> guard(lock);
	return events;
}

vector<ExecutionRegionCounter> ExecutionRegionEventLog::GetCounters() const {
	lock_guard<mutex> guard(lock);
	return counters;
}

vector<ExecutionRegionDecisionCounter> ExecutionRegionEventLog::GetDecisionCounters() const {
	lock_guard<mutex> guard(lock);
	return decision_counters;
}

vector<ExecutionRegionKernelCounter> ExecutionRegionEventLog::GetKernelCounters() const {
	lock_guard<mutex> guard(lock);
	return kernel_counters;
}

void ExecutionRegionEventLog::ClearEvents() {
	lock_guard<mutex> guard(lock);
	events.clear();
	kernel_counters.clear();
}

void ExecutionRegionEventLog::ClearCounters() {
	lock_guard<mutex> guard(lock);
	counters.clear();
	decision_counters.clear();
}

void ExecutionRegionEventLog::ApplyRetentionLimit(idx_t event_log_size) {
	lock_guard<mutex> guard(lock);
	TrimEvents(event_log_size);
	TrimKernelCounters(event_log_size);
}

} // namespace duckdb

#include "duckdb/execution/execution_region_telemetry.hpp"

#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ExecutionRegionSuppressionGuard::ExecutionRegionSuppressionGuard(ClientContext &context_p) : context(context_p) {
	context.PushCompiledExecutionSuppression();
}

ExecutionRegionSuppressionGuard::~ExecutionRegionSuppressionGuard() {
	context.PopCompiledExecutionSuppression();
}

static hash_t ExecutionRegionTelemetryHashString(const string &value) {
	return Hash(value.c_str(), value.size());
}

template <class T>
static hash_t ExecutionRegionTelemetryHashEnum(T value) {
	return Hash(static_cast<uint8_t>(value));
}

static hash_t ExecutionRegionTelemetryCombine(hash_t result, hash_t value) {
	return CombineHash(result, value);
}

static idx_t ExecutionRegionRingIndex(idx_t start, idx_t capacity, idx_t offset) {
	D_ASSERT(capacity > 0);
	return (start + offset) % capacity;
}

static hash_t ExecutionRegionCounterHash(const ExecutionRegionEvent &event) {
	auto result = ExecutionRegionTelemetryHashString(event.backend_name);
	result = ExecutionRegionTelemetryCombine(result, ExecutionRegionTelemetryHashEnum(event.status_kind));
	result = ExecutionRegionTelemetryCombine(result, ExecutionRegionTelemetryHashEnum(event.execution_mode_kind));
	result = ExecutionRegionTelemetryCombine(result, ExecutionRegionTelemetryHashEnum(event.selected_runner));
	result = ExecutionRegionTelemetryCombine(result, ExecutionRegionTelemetryHashString(event.blocker));
	result = ExecutionRegionTelemetryCombine(
	    result, ExecutionRegionTelemetryHashString(event.jit_runtime.hash_join_probe_layout));
	return result;
}

static bool ExecutionRegionCounterMatches(const ExecutionRegionCounter &counter, const ExecutionRegionEvent &event) {
	return counter.backend_name == event.backend_name && counter.status_kind == event.status_kind &&
	       counter.execution_mode_kind == event.execution_mode_kind &&
	       counter.selected_runner_kind == event.selected_runner && counter.blocker == event.blocker &&
	       counter.jit_runtime.hash_join_probe_layout == event.jit_runtime.hash_join_probe_layout;
}

static void AccumulateExecutionRegionRunnerCostTotals(ExecutionRegionRunnerCostTotals &target,
                                                      const PhysicalRunnerCostProfile &source) {
	if (!source.present) {
		return;
	}
	target.present = true;
	target.rows += source.rows;
	target.batches += source.batches;
	target.expression_cost += source.expression_cost;
	target.generated_stage_count += source.generated_stage_count;
	target.materialization_elision_count += source.materialization_elision_count;
	target.native_join_stage_count += source.native_join_stage_count;
	target.native_aggregate_stage_count += source.native_aggregate_stage_count;
	target.native_grouped_aggregate_stage_count += source.native_grouped_aggregate_stage_count;
	target.native_sort_stage_count += source.native_sort_stage_count;
	target.full_pipeline = target.full_pipeline || source.full_pipeline;
	target.generated_expression_work += source.generated_expression_work;
	target.generated_stage_work += source.generated_stage_work;
	target.native_operator_work += source.native_operator_work;
	target.materialization_elision_work += source.materialization_elision_work;
	target.full_pipeline_work += source.full_pipeline_work;
	target.stateful_protocol_penalty += source.stateful_protocol_penalty;
	target.saved_work_per_batch += source.saved_work_per_batch;
	target.accelerated_runner_benefit += source.accelerated_runner_benefit;
	target.startup_cost += source.startup_cost;
	target.required_benefit += source.required_benefit;
	target.net_benefit += source.net_benefit;
	target.selected_accelerated_runner_count += source.selected_accelerated_runner ? 1 : 0;
}

static void AccumulateExecutionRegionStageTimings(ExecutionRegionStageTimings &target,
                                                  const ExecutionRegionStageTimings &source) {
	target.pipeline_cbo_time_us += source.pipeline_cbo_time_us;
	target.graph_build_time_us += source.graph_build_time_us;
	target.candidate_cbo_time_us += source.candidate_cbo_time_us;
	target.ir_lowering_time_us += source.ir_lowering_time_us;
	target.backend_analysis_time_us += source.backend_analysis_time_us;
	target.codegen_time_us += source.codegen_time_us;
	target.executable_build_time_us += source.executable_build_time_us;
	target.machine_codegen_time_us += source.machine_codegen_time_us;
	target.kernel_build_time_us += source.kernel_build_time_us;
}

static void AccumulateExecutionRegionCounter(ExecutionRegionCounter &counter, const ExecutionRegionEvent &event) {
	counter.count++;
	AccumulateExecutionRegionRunnerCostTotals(counter.runner_cost, event.runner_cost);
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
	MergeExecutionRegionStageRuntime(counter.source_stage_runtime, event.source_stage_runtime);
	counter.sink_next_batch_invocation_count += event.sink_next_batch_invocation_count;
	counter.sink_next_batch_runtime_time_us += event.sink_next_batch_runtime_time_us;
	counter.generated_body_runtime_time_us += event.generated_body_runtime_time_us;
	MergeExecutionRegionStageRuntime(counter.generated_stage_runtime, event.generated_stage_runtime);
	AccumulateExecutionRegionStageTimings(counter.stage_timings, event.stage_timings);
	AddExecutionRegionLazyCodegenMetrics(counter.jit_runtime.lazy_codegen, event.jit_runtime.lazy_codegen);
}

void ExecutionRegionEventLog::RecordCounter(const ExecutionRegionEvent &event) {
	auto hash = ExecutionRegionCounterHash(event);
	auto index_entry = counter_index.find(hash);
	if (index_entry != counter_index.end()) {
		for (auto index : index_entry->second) {
			auto &counter = counters[index];
			if (!ExecutionRegionCounterMatches(counter, event)) {
				continue;
			}
			AccumulateExecutionRegionCounter(counter, event);
			return;
		}
	}
	ExecutionRegionCounter counter;
	counter.backend_name = event.backend_name;
	counter.status_kind = event.status_kind;
	counter.execution_mode_kind = event.execution_mode_kind;
	counter.selected_runner_kind = event.selected_runner;
	counter.blocker = event.blocker;
	counter.jit_runtime.hash_join_probe_layout = event.jit_runtime.hash_join_probe_layout;
	AccumulateExecutionRegionCounter(counter, event);
	auto index = counters.size();
	counters.push_back(std::move(counter));
	counter_index[hash].push_back(index);
}

void ExecutionRegionEventLog::ResizeEventRing(idx_t event_log_size) {
	if (event_log_size == 0) {
		events.clear();
		event_ring_capacity = 0;
		event_ring_start = 0;
		event_ring_count = 0;
		return;
	}
	if (event_ring_capacity == event_log_size && event_ring_count <= event_log_size) {
		return;
	}
	auto retained_count = MinValue(event_ring_count, event_log_size);
	vector<ExecutionRegionEvent> retained;
	retained.reserve(event_log_size);
	auto retained_start = event_ring_count - retained_count;
	for (idx_t retained_idx = 0; retained_idx < retained_count; retained_idx++) {
		auto source_idx =
		    ExecutionRegionRingIndex(event_ring_start, event_ring_capacity, retained_start + retained_idx);
		retained.push_back(std::move(events[source_idx]));
	}
	events = std::move(retained);
	event_ring_capacity = event_log_size;
	event_ring_start = 0;
	event_ring_count = retained_count;
}

void ExecutionRegionEventLog::PushEvent(idx_t event_log_size, ExecutionRegionEvent event) {
	ResizeEventRing(event_log_size);
	if (event_log_size == 0) {
		return;
	}
	if (event_ring_count < event_ring_capacity) {
		D_ASSERT(event_ring_start == 0);
		D_ASSERT(events.size() == event_ring_count);
		events.push_back(std::move(event));
		event_ring_count++;
		return;
	}
	events[event_ring_start] = std::move(event);
	event_ring_start = ExecutionRegionRingIndex(event_ring_start, event_ring_capacity, 1);
}

vector<ExecutionRegionEvent> ExecutionRegionEventLog::CopyEventsInOrder() const {
	vector<ExecutionRegionEvent> result;
	result.reserve(event_ring_count);
	if (events.empty()) {
		return result;
	}
	for (idx_t event_idx = 0; event_idx < event_ring_count; event_idx++) {
		result.push_back(events[ExecutionRegionRingIndex(event_ring_start, event_ring_capacity, event_idx)]);
	}
	return result;
}

idx_t ExecutionRegionEventLog::Record(idx_t event_log_size, ExecutionRegionEvent event) {
	lock_guard<mutex> guard(lock);
	event.event_id = next_event_id++;
	if (event.kernel_id == 0 && event.status_kind == ExecutionRegionEventStatus::COMPILED) {
		event.kernel_id = event.event_id;
	}
	auto event_id = event.event_id;
	RecordCounter(event);
	if (event_log_size == 0) {
		return event_id;
	}
	PushEvent(event_log_size, std::move(event));
	return event_id;
}

vector<ExecutionRegionEvent> ExecutionRegionEventLog::GetEvents() const {
	return SnapshotEventsInOrder();
}

vector<ExecutionRegionEvent> ExecutionRegionEventLog::SnapshotEventsInOrder() const {
	lock_guard<mutex> guard(lock);
	return CopyEventsInOrder();
}

vector<ExecutionRegionCounter> ExecutionRegionEventLog::GetCounters() const {
	return SnapshotCounters();
}

vector<ExecutionRegionCounter> ExecutionRegionEventLog::SnapshotCounters() const {
	lock_guard<mutex> guard(lock);
	return counters;
}

void ExecutionRegionEventLog::ClearEvents() {
	lock_guard<mutex> guard(lock);
	events.clear();
	event_ring_capacity = 0;
	event_ring_start = 0;
	event_ring_count = 0;
}

void ExecutionRegionEventLog::ClearCounters() {
	lock_guard<mutex> guard(lock);
	counters.clear();
	counter_index.clear();
}

void ExecutionRegionEventLog::ApplyRetentionLimit(idx_t event_log_size) {
	lock_guard<mutex> guard(lock);
	ResizeEventRing(event_log_size);
}

} // namespace duckdb

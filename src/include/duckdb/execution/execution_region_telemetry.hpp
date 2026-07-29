//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_telemetry.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/planner/cost_model.hpp"

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;
struct ExecutionContext;
struct OperatorSinkInput;

enum class ExecutionRegionEventPhase : uint8_t { NONE, DECISION, COMPILE, RUNTIME };
enum class ExecutionRegionEventStatus : uint8_t {
	NONE,
	COMPILED,
	SKIPPED,
	UNSUPPORTED,
	UNAVAILABLE,
	DISABLED,
	ERROR,
	EXECUTED,
	SOURCE_CONTRACT
};

struct ExecutionRegionStageTimings {
	int64_t pipeline_cbo_time_us = 0;
	int64_t graph_build_time_us = 0;
	int64_t candidate_cbo_time_us = 0;
	int64_t ir_lowering_time_us = 0;
	int64_t backend_analysis_time_us = 0;
	int64_t codegen_time_us = 0;
	int64_t executable_build_time_us = 0;
	int64_t machine_codegen_time_us = 0;
	int64_t kernel_build_time_us = 0;
};

using ExecutionRegionRunnerCostAxisTotals = PhysicalRunnerAxisCostValues;

struct ExecutionRegionRunnerCostTotals : PhysicalRunnerCostNumericFields {
	bool present = false;
	bool source_contract_output_cardinality_unknown = false;
	bool full_pipeline = false;
	string input_scope;
	string admission_class;
	string selection_reason;
	ExecutionRegionJitRuntimeProofMask required_runtime_proofs = 0;
	//! Mirrors PhysicalRunnerAxisCostBreakdown's cost fields so the runner-cost column
	//! appenders can be templated over a profile and its totals alike.
	ExecutionRegionRunnerCostAxisTotals compiled_vectorized;
	ExecutionRegionRunnerCostAxisTotals gpu;
	idx_t selected_accelerated_runner_count = 0;
	idx_t selected_compiled_vectorized_runner_count = 0;
	idx_t selected_gpu_runner_count = 0;

	ExecutionRegionRunnerCostAxisTotals &AxisAt(idx_t axis_idx) {
		return axis_idx == 0 ? compiled_vectorized : gpu;
	}
};

struct ExecutionRegionEvent {
	idx_t event_id = 0;
	bool has_pipeline = false;
	string pipeline_shape;
	idx_t pipeline_estimated_cardinality = 0;
	bool has_candidate = false;
	idx_t candidate_id = 0;
	string candidate_shape;
	string candidate_pipeline_shape;
	ExecutionRegionSignature candidate_signature;
	idx_t candidate_node_count = 0;
	idx_t candidate_start_operator_index = 0;
	idx_t candidate_end_operator_index = 0;
	idx_t candidate_estimated_cardinality = 0;
	ExecutionRegionCandidateTraits candidate_traits;
	ExecutionRegionContract candidate_contract;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	PhysicalRunnerCostProfile runner_cost;
	ExecutionRegionEventPhase phase_kind = ExecutionRegionEventPhase::NONE;
	string backend_name;
	ExecutionRegionEventStatus status_kind = ExecutionRegionEventStatus::NONE;
	ExecutionRegionExecutionMode execution_mode_kind = ExecutionRegionExecutionMode::NONE;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool selected_uses_scan_filters = false;
	bool candidate_uses_scan_filters = false;
	string reason;
	string blocker;
	string ir;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	idx_t kernel_id = 0;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> source_stage_runtime;
	idx_t sink_next_batch_invocation_count = 0;
	int64_t sink_next_batch_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> generated_stage_runtime;
	string runtime_result;
	string kernel_compile_reason;
	int64_t kernel_compile_time_us = 0;
	idx_t kernel_code_size = 0;
	ExecutionRegionStageTimings stage_timings;
	ExecutionRegionJitRuntimeMetrics jit_runtime;
};

//! Single authority for an event's numeric runtime metrics, split by type the
//! way the runner-cost visitors are: every surface that renders runtime metrics
//! (the events table function, the profiler's structured output, EXPLAIN
//! ANALYZE text) iterates these, so a metric cannot reach one surface and
//! silently miss another. Non-numeric runtime state (breakdowns, result
//! strings, jit path counters) stays surface-specific.
template <class FN>
void ForEachExecutionRegionEventRuntimeCountField(const ExecutionRegionEvent &event, FN &&fn) {
	fn("input_rows", event.input_rows);
	fn("output_rows", event.output_rows);
	fn("invocation_count", event.invocation_count);
	fn("source_contract_output_rows", event.source_contract_output_rows);
	fn("source_contract_invocation_count", event.source_contract_invocation_count);
	fn("sink_next_batch_invocation_count", event.sink_next_batch_invocation_count);
}

//! Time fields carry two labels: the canonical struct name used by the events
//! table function, and the profiler's summary-consistent name. The surfaces
//! keep their own vocabularies; the field LIST is what must not diverge.
template <class FN>
void ForEachExecutionRegionEventRuntimeTimeField(const ExecutionRegionEvent &event, FN &&fn) {
	fn("runtime_time_us", "runtime_time_us", event.runtime_time_us);
	fn("source_contract_runtime_time_us", "source_runtime_time_us", event.source_contract_runtime_time_us);
	fn("sink_next_batch_runtime_time_us", "sink_next_batch_runtime_time_us", event.sink_next_batch_runtime_time_us);
	fn("generated_body_runtime_time_us", "generated_runtime_time_us", event.generated_body_runtime_time_us);
}

struct ExecutionRegionTraceSummary {
	idx_t decisions = 0;
	idx_t compiled = 0;
	idx_t compile_errors = 0;
	idx_t unsupported = 0;
	idx_t skipped = 0;
	idx_t unavailable = 0;
	idx_t disabled = 0;
	idx_t runtime_events = 0;
	idx_t runtime_regions = 0;
	idx_t code_size = 0;
	int64_t runtime_us = 0;
	int64_t source_us = 0;
	int64_t sink_us = 0;
	int64_t generated_us = 0;
	int64_t decision_us = 0;
	int64_t compile_us = 0;
	int64_t pipeline_cbo_us = 0;
	int64_t graph_build_us = 0;
	int64_t candidate_cbo_us = 0;
	int64_t ir_lowering_us = 0;
	int64_t backend_analysis_us = 0;
	int64_t codegen_us = 0;
	int64_t executable_build_us = 0;
	int64_t machine_codegen_us = 0;
	int64_t kernel_build_us = 0;
	ExecutionRegionLazyCodegenMetrics lazy_codegen;
};

DUCKDB_API ExecutionRegionTraceSummary SummarizeExecutionRegionTrace(const vector<ExecutionRegionEvent> &trace);
DUCKDB_API const char *ExecutionRegionEventPhaseToString(ExecutionRegionEventPhase phase);
DUCKDB_API const char *ExecutionRegionEventStatusToString(ExecutionRegionEventStatus status);
DUCKDB_API ExecutionRegionEventStatus ExecutionRegionEventStatusFromCompileStatus(ExecutionRegionCompileStatus status);
DUCKDB_API bool ExecutionRegionEventIsRuntime(const ExecutionRegionEvent &event);
DUCKDB_API bool ExecutionRegionEventWasInvoked(const ExecutionRegionEvent &event);
DUCKDB_API bool ExecutionRegionEventIsVisibleInQueryProfile(const ExecutionRegionEvent &event);
DUCKDB_API const string &ExecutionRegionEventPipelineShape(const ExecutionRegionEvent &event);
DUCKDB_API idx_t ExecutionRegionEventEstimatedCardinality(const ExecutionRegionEvent &event);
DUCKDB_API idx_t ExecutionRegionEventProfileCodeSize(const ExecutionRegionEvent &event);
DUCKDB_API int64_t ExecutionRegionEventProfileCompileTime(const ExecutionRegionEvent &event);

struct ExecutionRegionCounter {
	string backend_name;
	idx_t kernel_id = 0;
	ExecutionRegionEventStatus status_kind = ExecutionRegionEventStatus::NONE;
	ExecutionRegionExecutionMode execution_mode_kind = ExecutionRegionExecutionMode::NONE;
	ExecutionRunnerKind selected_runner_kind = ExecutionRunnerKind::VECTORIZED;
	ExecutionRegionRunnerCostTotals runner_cost;
	string blocker;
	idx_t count = 0;
	int64_t decision_time_us = 0;
	int64_t compile_time_us = 0;
	idx_t code_size = 0;
	idx_t input_rows = 0;
	idx_t output_rows = 0;
	idx_t invocation_count = 0;
	int64_t runtime_time_us = 0;
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> source_stage_runtime;
	idx_t sink_next_batch_invocation_count = 0;
	int64_t sink_next_batch_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> generated_stage_runtime;
	ExecutionRegionStageTimings stage_timings;
	ExecutionRegionJitRuntimeMetrics jit_runtime;
};

class ExecutionRegionSuppressionGuard {
public:
	explicit ExecutionRegionSuppressionGuard(ClientContext &context);
	~ExecutionRegionSuppressionGuard();
	ExecutionRegionSuppressionGuard(const ExecutionRegionSuppressionGuard &) = delete;
	ExecutionRegionSuppressionGuard &operator=(const ExecutionRegionSuppressionGuard &) = delete;

	static bool IsActive(const ClientContext &context);

private:
	ClientContext &context;
	ExecutionRegionSuppressionGuard *previous;
	static thread_local ExecutionRegionSuppressionGuard *active;
};

class ExecutionRegionEventLog {
public:
	idx_t Record(idx_t event_log_size, ExecutionRegionEvent event);
	vector<ExecutionRegionEvent> GetEvents() const;
	vector<ExecutionRegionCounter> GetCounters() const;
	void ClearEvents();
	void ClearCounters();
	void ApplyRetentionLimit(idx_t event_log_size);

private:
	void RecordCounter(const ExecutionRegionEvent &event);
	void ResizeEventRing(idx_t event_log_size);
	void PushEvent(idx_t event_log_size, ExecutionRegionEvent event);
	vector<ExecutionRegionEvent> CopyEventsInOrder() const;
	vector<ExecutionRegionEvent> SnapshotEventsInOrder() const;
	vector<ExecutionRegionCounter> SnapshotCounters() const;

private:
	mutable mutex lock;
	vector<ExecutionRegionEvent> events;
	idx_t event_ring_capacity = 0;
	idx_t event_ring_start = 0;
	idx_t event_ring_count = 0;
	vector<ExecutionRegionCounter> counters;
	unordered_map<hash_t, vector<idx_t>> counter_index;
	idx_t next_event_id = 1;
};

} // namespace duckdb

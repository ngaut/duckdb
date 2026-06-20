//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
struct ExecutionRegionPipelineInventory;
struct ExecutionRegionRuntimeMetrics;
struct ExecutionRegionStageTimings;

class ExecutionRegionManager {
public:
	explicit ExecutionRegionManager(DatabaseInstance &db);

	DUCKDB_API void RegisterBackend(unique_ptr<ExecutionRegionBackend> backend);
	DUCKDB_API vector<ExecutionRegionBackendInfo> GetBackends(ClientContext *context = nullptr) const;
	DUCKDB_API optional_ptr<ExecutionRegionBackend> SelectBackend(ClientContext &context, string &backend_name) const;
	DUCKDB_API vector<ExecutionRegionEvent> GetEvents() const;
	DUCKDB_API vector<ExecutionRegionCounter> GetCounters() const;
	DUCKDB_API void ClearEvents();
	DUCKDB_API void ClearCounters();
	DUCKDB_API void ApplyEventRetentionLimit(idx_t event_log_size);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionCompileTarget target, ExecutionRegionEventStatus status,
	                                   string reason, idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
	                                   string runtime_result);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionCompileTarget target, ExecutionRegionEventStatus status,
	                                   string reason, idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
	                                   string runtime_result, const ExecutionRegionRuntimeMetrics &runtime_metrics);
	DUCKDB_API void RecordVectorizedBaselineRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                                     ExecutionRegionCompileTarget target, string reason,
	                                                     int64_t runtime_time_us, string runtime_result);

	DUCKDB_API static ExecutionRegionManager &Get(DatabaseInstance &db);
	DUCKDB_API static ExecutionRegionManager &Get(ClientContext &context);

private:
	friend class ExecutionRegionPlanner;

	idx_t
	RecordEvent(ClientContext &context, string backend_name, ExecutionRegionCompileTarget target,
	            ExecutionRegionCompileStatus status, ExecutionRegionExecutionMode execution_mode,
	            ExecutionRegionPolicyMode requested_policy, string reason, string blocker, const string *ir,
	            int64_t decision_time_us, int64_t compile_time_us, idx_t code_size,
	            const ExecutionRegionCandidate *candidate = nullptr,
	            ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED,
	            const ExecutionRegionStageTimings *stage_timings = nullptr,
	            ExecutionRegionForm region_execution_form = ExecutionRegionForm::NONE,
	            ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE,
	            bool selected_uses_scan_filters = false, const ExecutionRegionPipelineInventory *inventory = nullptr,
	            ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE,
	            const PhysicalRunnerCostProfile *runner_cost = nullptr);

private:
	DatabaseInstance &db;
	mutable mutex lock;
	vector<unique_ptr<ExecutionRegionBackend>> backends;
	ExecutionRegionEventLog event_log;
};

} // namespace duckdb

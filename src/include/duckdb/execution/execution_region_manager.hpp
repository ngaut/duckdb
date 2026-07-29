//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_artifact_cache.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
struct ExecutionRegionRuntimeMetrics;
struct ExecutionRegionStageTimings;

class ExecutionRegionManager {
public:
	explicit ExecutionRegionManager(DatabaseInstance &db);

	DUCKDB_API void RegisterBackend(unique_ptr<ExecutionRegionBackend> backend, uint64_t backend_abi_version);
	DUCKDB_API vector<ExecutionRegionBackendInfo> GetBackends(ClientContext *context = nullptr) const;
	DUCKDB_API bool HasAvailableBackendForRunner(ClientContext &context, ExecutionRunnerKind runner_kind) const;
	DUCKDB_API optional_ptr<ExecutionRegionBackend>
	SelectBackend(ClientContext &context, string &backend_name,
	              ExecutionRunnerKind runner_kind = ExecutionRunnerKind::COMPILED_VECTORIZED) const;
	DUCKDB_API vector<ExecutionRegionEvent> GetEvents() const;
	DUCKDB_API vector<ExecutionRegionCounter> GetCounters() const;
	DUCKDB_API void ClearEvents();
	DUCKDB_API void ClearCounters();
	DUCKDB_API void ApplyEventRetentionLimit(idx_t event_log_size);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionEventStatus status, string reason, idx_t input_rows,
	                                   idx_t output_rows, int64_t runtime_time_us, string runtime_result);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionEventStatus status, string reason, idx_t input_rows,
	                                   idx_t output_rows, int64_t runtime_time_us, string runtime_result,
	                                   const ExecutionRegionRuntimeMetrics &runtime_metrics);
	DUCKDB_API void RecordVectorizedBaselineRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                                     string reason, int64_t runtime_time_us, string runtime_result);

	DUCKDB_API static ExecutionRegionManager &Get(DatabaseInstance &db);
	DUCKDB_API static ExecutionRegionManager &Get(ClientContext &context);

private:
	friend class ExecutionRegionPlanner;

	struct RegisteredExecutionRegionBackend {
		RegisteredExecutionRegionBackend(unique_ptr<ExecutionRegionBackend> backend_p, string name_p,
		                                 string normalized_name_p, string description_p,
		                                 ExecutionRunnerKind runner_kind_p, bool supports_regions_p)
		    : backend(std::move(backend_p)), name(std::move(name_p)), normalized_name(std::move(normalized_name_p)),
		      description(std::move(description_p)), runner_kind(runner_kind_p), supports_regions(supports_regions_p) {
		}

		unique_ptr<ExecutionRegionBackend> backend;
		string name;
		string normalized_name;
		string description;
		ExecutionRunnerKind runner_kind;
		bool supports_regions;
	};

	idx_t
	RecordEvent(ClientContext &context, string backend_name, ExecutionRegionCompileStatus status,
	            ExecutionRegionExecutionMode execution_mode, string reason, string blocker, const string *ir,
	            int64_t decision_time_us, int64_t compile_time_us, idx_t code_size,
	            const string *pipeline_shape = nullptr, const ExecutionRegionCandidate *candidate = nullptr,
	            ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED,
	            const ExecutionRegionStageTimings *stage_timings = nullptr,
	            ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE,
	            ExecutionRegionScanFilterMode selected_scan_filter_mode = ExecutionRegionScanFilterMode::NONE,
	            const PhysicalRunnerCostProfile *runner_cost = nullptr);

private:
	DatabaseInstance &db;
	mutable mutex lock;
	vector<RegisteredExecutionRegionBackend> backends;
	//! Database-local because artifacts are backend code, not physical-operator
	//! state. Semantic keys make equivalent prepared and ad-hoc plans share
	//! code while kernel wrappers retain all execution-local bindings.
	ExecutionRegionArtifactCache artifact_cache;
	ExecutionRegionEventLog event_log;
};

} // namespace duckdb

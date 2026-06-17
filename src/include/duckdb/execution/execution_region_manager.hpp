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
struct ExecutionRegionAdmissionInfo;
struct ExecutionRegionPipelineInventory;
struct ExecutionRegionRuntimeMetrics;
struct ExecutionRegionStageTimings;

class ExecutionRegionManager {
public:
	explicit ExecutionRegionManager(DatabaseInstance &db);

	DUCKDB_API void RegisterBackend(unique_ptr<ExecutionRegionBackend> backend);
	DUCKDB_API vector<ExecutionRegionBackendInfo> GetBackends(ClientContext *context = nullptr) const;
	DUCKDB_API void AddAdmissionProfileRule(string backend_name, ExecutionRegionAdmissionRule rule);
	DUCKDB_API vector<ExecutionRegionAdmissionProfileRule> GetAdmissionProfileRules() const;
	DUCKDB_API bool HasAdmissionProfileRules(const string &backend_name, ExecutionRegionCompileTarget target) const;
	DUCKDB_API bool GetAdmissionProfileRule(const string &backend_name, ExecutionRegionCompileTarget target,
	                                        const string &admission_key, ExecutionRegionAdmissionRule &rule) const;
	DUCKDB_API void ClearAdmissionProfileRules();
	DUCKDB_API ExecutionRegionPolicyMode GetPolicy(ClientContext &context) const;
	DUCKDB_API optional_ptr<ExecutionRegionBackend> SelectBackend(ClientContext &context, string &backend_name) const;
	DUCKDB_API vector<ExecutionRegionEvent> GetEvents() const;
	DUCKDB_API vector<ExecutionRegionCounter> GetCounters() const;
	DUCKDB_API vector<ExecutionRegionDecisionCounter> GetDecisionCounters() const;
	DUCKDB_API vector<ExecutionRegionKernelCounter> GetKernelCounters() const;
	DUCKDB_API void ClearEvents();
	DUCKDB_API void ClearCounters();
	DUCKDB_API void ApplyEventRetentionLimit(idx_t event_log_size);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionCompileTarget target, string status, string reason,
	                                   idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
	                                   string runtime_result);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const ExecutionRegionKernel &kernel,
	                                   ExecutionRegionCompileTarget target, string status, string reason,
	                                   idx_t input_rows, idx_t output_rows, int64_t runtime_time_us,
	                                   string runtime_result, const ExecutionRegionRuntimeMetrics &runtime_metrics);

	DUCKDB_API static ExecutionRegionManager &Get(DatabaseInstance &db);
	DUCKDB_API static ExecutionRegionManager &Get(ClientContext &context);

private:
	friend class ExecutionRegionPlanner;

	idx_t
	RecordEvent(ClientContext &context, string backend_name, ExecutionRegionCompileTarget target,
	            ExecutionRegionCompileStatus status, ExecutionRegionExecutionMode execution_mode,
	            string policy_decision, string reason, string ir, int64_t decision_time_us, int64_t compile_time_us,
	            idx_t code_size, const ExecutionRegionCandidate *candidate = nullptr,
	            const ExecutionRegionAdmissionInfo *admission = nullptr,
	            const ExecutionRegionStageTimings *stage_timings = nullptr,
	            ExecutionRegionForm region_execution_form = ExecutionRegionForm::NONE,
	            ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE,
	            const ExecutionRegionPipelineInventory *inventory = nullptr);

private:
	DatabaseInstance &db;
	mutable mutex lock;
	vector<unique_ptr<ExecutionRegionBackend>> backends;
	vector<ExecutionRegionAdmissionProfileRule> admission_profile_rules;
	ExecutionRegionEventLog event_log;
};

} // namespace duckdb

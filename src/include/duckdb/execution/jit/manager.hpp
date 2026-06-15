//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/manager.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class Pipeline;

class JitManager {
public:
	explicit JitManager(DatabaseInstance &db);

	DUCKDB_API void RegisterBackend(unique_ptr<JitBackend> backend);
	DUCKDB_API vector<JitBackendInfo> GetBackends(ClientContext *context = nullptr) const;
	DUCKDB_API vector<JitEvent> GetEvents() const;
	DUCKDB_API vector<JitCounter> GetCounters() const;
	DUCKDB_API vector<JitDecisionCounter> GetDecisionCounters() const;
	DUCKDB_API vector<JitKernelCounter> GetKernelCounters() const;
	DUCKDB_API void ClearEvents();
	DUCKDB_API void ClearCounters();
	DUCKDB_API void ApplyEventRetentionLimit(idx_t event_log_size);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const JitRegionKernel &kernel,
	                                   JitCompileTarget target, string status, string reason, idx_t input_rows,
	                                   idx_t output_rows, int64_t runtime_time_us, string runtime_result);
	DUCKDB_API void RecordRuntimeEvent(ClientContext &context, const JitRegionKernel &kernel,
	                                   JitCompileTarget target, string status, string reason, idx_t input_rows,
	                                   idx_t output_rows, int64_t runtime_time_us, string runtime_result,
	                                   const JitRuntimeMetrics &runtime_metrics);
	DUCKDB_API void RecordRuntimeFallbackEvent(ClientContext &context, const JitRegionKernel &kernel,
	                                           JitCompileTarget target, string status, string reason, idx_t input_rows,
	                                           idx_t output_rows, int64_t runtime_time_us, string runtime_result);

	DUCKDB_API unique_ptr<JitPreparedPipeline> PreparePipelineRegions(ClientContext &context, Pipeline &pipeline);
	DUCKDB_API vector<unique_ptr<JitRegionKernel>>
	CompilePreparedRegions(ClientContext &context, const JitPreparedPipeline &prepared);

	DUCKDB_API static JitManager &Get(DatabaseInstance &db);
	DUCKDB_API static JitManager &Get(ClientContext &context);
	DUCKDB_API static bool IsJitIntrospectionPipeline(Pipeline &pipeline);

private:
	JitPolicyMode GetPolicy(ClientContext &context) const;
	optional_ptr<JitBackend> SelectBackend(ClientContext &context, string &backend_name) const;
	idx_t RecordEvent(ClientContext &context, string backend_name, JitCompileTarget target, JitCompileStatus status,
	                  JitExecutionMode execution_mode, string policy_decision, string reason, string ir,
	                  int64_t decision_time_us, int64_t compile_time_us, idx_t code_size,
	                  const JitRegionCandidate *candidate = nullptr, const JitAdmissionInfo *admission = nullptr,
	                  const JitStageTimings *stage_timings = nullptr,
	                  JitRegionExecutionForm region_execution_form = JitRegionExecutionForm::NONE,
	                  JitRegionSourceExecutionKind selected_source_execution = JitRegionSourceExecutionKind::NONE,
	                  const JitRegionPipelineInventory *inventory = nullptr);

private:
	DatabaseInstance &db;
	mutable mutex lock;
	vector<unique_ptr<JitBackend>> backends;
	JitEventLog event_log;
};

} // namespace duckdb

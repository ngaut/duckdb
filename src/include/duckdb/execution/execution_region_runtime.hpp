//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

class Allocator;
class ExecutionRegionLocalState;

class ExecutionOperatorRuntime {
public:
	virtual ~ExecutionOperatorRuntime();

	virtual ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                                 const ExecutionRegionOperatorInfo &operator_info,
	                                                 ExecutionOperatorBinding &binding) = 0;
	//! Chunk-free readiness probe for a native-delegated operator; kernels call
	//! this at entry so not-ready runtime states defer before anything is fetched.
	virtual ExecutionOperatorReadiness GetOperatorReadiness(idx_t operator_index,
	                                                        const ExecutionRegionOperatorInfo &operator_info) = 0;
	virtual bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                      ExecutionSinkBinding &binding) = 0;
	//! Records the sink result. BLOCKED transfers the chunk to the core continuation and leaves it empty, ensuring
	//! that the core executor is the only owner allowed to retry those rows.
	virtual SinkResultType RecordSinkResult(DataChunk &chunk, SinkResultType sink_result) = 0;
	//! Records results for sinks that cannot block after entering their native state.
	virtual SinkResultType RecordSinkResult(idx_t input_rows, SinkResultType sink_result) = 0;
};

struct ExecutionRegionLazyCodegenMetrics {
	int64_t codegen_time_us = 0;
	int64_t machine_codegen_time_us = 0;
	idx_t code_size = 0;
};

DUCKDB_API const char *ExecutionRegionJitRuntimeProofName(ExecutionRegionJitRuntimeProof proof);
DUCKDB_API string RenderExecutionRegionJitRuntimeProofRequirements(ExecutionRegionJitRuntimeProofMask requirements);

struct ExecutionRegionJitRuntimeMetrics {
	string hash_join_probe_layout;
	vector<ExecutionRegionRecordedCounter> runtime_path_counts;
	vector<ExecutionRegionRecordedCounter> runtime_proof_counts;
	vector<ExecutionRegionRecordedCounter> runtime_delegation_counts;
	ExecutionRegionLazyCodegenMetrics lazy_codegen;
};

struct ExecutionRegionRuntimeMetrics {
	idx_t source_contract_output_rows = 0;
	idx_t source_contract_invocation_count = 0;
	int64_t source_contract_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> source_stage_runtime;
	idx_t sink_next_batch_invocation_count = 0;
	int64_t sink_next_batch_runtime_time_us = 0;
	int64_t generated_body_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> generated_stage_runtime;
	ExecutionRegionJitRuntimeMetrics jit_runtime;
};

enum class ExecutionRegionRuntimeOnceFlag : uint8_t { AGGREGATE_GROUP_RESERVE, CANONICAL_GROUP_HASH_DECLARED };
static constexpr idx_t EXECUTION_REGION_RUNTIME_ONCE_FLAG_COUNT = 2;

DUCKDB_API void AddExecutionRegionStageRuntime(vector<ExecutionRegionRecordedStageRuntime> &stages,
                                               ExecutionRegionStageId stage, int64_t runtime_time_us, idx_t count = 1);
DUCKDB_API void MergeExecutionRegionStageRuntime(vector<ExecutionRegionRecordedStageRuntime> &target,
                                                 const vector<ExecutionRegionRecordedStageRuntime> &source);
DUCKDB_API void AddExecutionRegionRecordedCounter(vector<ExecutionRegionRecordedCounter> &counters,
                                                  ExecutionRegionStageId counter, idx_t count = 1);
DUCKDB_API void MergeExecutionRegionRecordedCounters(vector<ExecutionRegionRecordedCounter> &target,
                                                     const vector<ExecutionRegionRecordedCounter> &source);
DUCKDB_API void AddExecutionRegionLazyCodegenMetrics(ExecutionRegionLazyCodegenMetrics &target,
                                                     const ExecutionRegionLazyCodegenMetrics &source);
DUCKDB_API string RenderExecutionRegionStageRuntimeBreakdown(const vector<ExecutionRegionRecordedStageRuntime> &stages);
DUCKDB_API string RenderExecutionRegionStageCountBreakdown(const vector<ExecutionRegionRecordedStageRuntime> &stages);
DUCKDB_API string RenderExecutionRegionCounterBreakdown(const vector<ExecutionRegionRecordedCounter> &counters);

struct ExecutionRegionSourceContractMetrics : public ExecutionOperatorStageRecorder {
	int64_t setup_runtime_time_us = 0;
	int64_t start_operator_runtime_time_us = 0;
	int64_t get_data_runtime_time_us = 0;
	int64_t finish_source_runtime_time_us = 0;
	int64_t end_operator_runtime_time_us = 0;
	vector<ExecutionRegionRecordedStageRuntime> get_data_stages;

	void RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) override;
	int64_t GetDataStageRuntimeSum() const;
};

class ExecutionRegionRuntime {
public:
	virtual ~ExecutionRegionRuntime();

	virtual idx_t MaxChunks() const = 0;
	virtual idx_t MaxThreads() const = 0;
	virtual Allocator &GetAllocator() = 0;
	virtual ExecutionRegionLocalState &LocalState() = 0;
	//! Whether source chunks must remain separate to preserve the sink's partition protocol.
	virtual bool PreserveSourceChunkBoundaries() const = 0;
	virtual SourceResultType FetchSourceContract(DataChunk *&result) = 0;
	virtual SinkNextBatchType AdvanceSinkBatch(DataChunk &source_chunk, bool have_more_output) = 0;
	virtual ExecutionOperatorRuntime &ExecutionOperators() = 0;
	virtual bool TraceRuntime() const = 0;
	virtual void RecordGeneratedStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us,
	                                         idx_t count = 1) = 0;
	virtual void RecordHashJoinProbeLayout(const char *layout);
	virtual void RecordJitRuntimePath(const char *path, idx_t count = 1);
	virtual void RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof proof, idx_t count = 1);
	virtual void RecordJitRuntimeProofDetail(const char *proof, idx_t count = 1);
	virtual void RecordJitRuntimeDelegation(const char *delegation, idx_t count = 1);
	virtual void RecordLazyCodegen(const ExecutionRegionLazyCodegenMetrics &metrics);
	virtual bool TryMarkOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index);
	//! Request a handoff to the vectorized continuation. Only legal while
	//! CanDeferAtEntry() holds — afterwards the source contract cursor can hold a
	//! partially-read row group that a handoff would silently abandon, so a
	//! mid-stream request throws.
	virtual void Defer(string reason) = 0;
	virtual const string &DeferredReason() const = 0;
	//! True until the source contract has been touched for this pipeline.
	virtual bool CanDeferAtEntry() const = 0;
	//! Whether this execution can hand the pipeline over to the vectorized runner mid-query, so a
	//! sink can receive rows from both runners. Sinks that store a producer-computed hash must fall
	//! back to the canonical one when this holds, or the two runners' hashes disagree for one key.
	//! Conservative by default: only a runtime that knows its handoff protocol may relax it.
	virtual bool RunnerHandoffPossible() const {
		return true;
	}
};

} // namespace duckdb

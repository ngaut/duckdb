//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/execution_region_pipeline_adapter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/parallel/pipeline_execution.hpp"

namespace duckdb {

class Allocator;
class ClientContext;
class PipelineExecutor;
struct ExecutionContext;
struct ExecutionRegionSourceContractMetrics;

class ExecutionRegionPipelineAdapter {
public:
	explicit ExecutionRegionPipelineAdapter(PipelineExecutor &executor);

	ClientContext &GetClientContext() const;
	ExecutionContext &GetExecutionContext() const;
	Allocator &GetAllocator() const;
	bool IsCompiledExecutionSuppressed() const;
	const void *ExecutionIdentity() const;
	idx_t MaxThreads() const;

	PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks);
	bool HasSourceAndSink() const;
	optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel() const;
	ExecutionRegionLocalState &GetOrCreateLocalState(ExecutionRegionKernel &kernel);
	bool IsAtCleanSourceToSinkBoundary() const;

	SourceResultType FetchSourceContract(DataChunk *&result, ExecutionRegionSourceContractMetrics *metrics = nullptr,
	                                     bool decline_new_row_group = false, bool *declined_new_row_group = nullptr);
	SourceResultType
	FetchPrimitiveAggregateStateSourceContract(ExecutionAggregateStateScanBatch *&result,
	                                           ExecutionRegionSourceContractMetrics *metrics = nullptr);
	SinkNextBatchType AdvanceSinkBatch(DataChunk &source_chunk, bool have_more_output);
	bool AdvancesSinkBatch(DataChunk &source_chunk, bool have_more_output);
	void SetVectorizedSourceClaimBudget(idx_t budget);
	void ClearVectorizedSourceClaimBudget();
	bool ConsumeVectorizedSourceDeclinedYield();
	bool HasVectorizedSourceClaimBudget() const;
	void LatchDeferredCompiledExecution();
	idx_t VectorizedSourceLegRows() const;
	ExecutionOperatorReadiness GetOperatorReadiness(idx_t operator_index,
	                                                const ExecutionRegionOperatorInfo &operator_info);
	//! True while nothing is in flight on this executor's source cursor.
	bool SourceCursorUntouched() const;
	bool VectorizedSourceCursorDirty() const;
	void ClearVectorizedSourceClaimBudgetAtBoundary();
	//! EXPLAIN ANALYZE attribution: annotate this pipeline's profiling nodes.
	//! covered_operators=true also marks the intermediate operators and sink.
	void AddProfilingAnnotation(const string &key, const string &value, bool covered_operators);
	ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                         const ExecutionRegionOperatorInfo &operator_info,
	                                         ExecutionOperatorBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding);
	//! Appends a compiled kernel's output chunk to the Sink. Once the Sink blocks, the core continuation owns that
	//! chunk and every later one: it sinks them in order before the kernel re-enters or the pipeline finalizes. The
	//! chunk the Sink blocked on retries as is, because a blocked Sink may already hold it
	SinkResultType AppendSink(const ExecutionAppendSinkBinding &binding, DataChunk &chunk);
	//! Hands a chunk to the core continuation without calling the Sink, behind the chunks already queued
	void QueueSinkChunk(DataChunk &chunk);
	//! Whether the Sink blocked and chunks wait for the core continuation
	bool HasRemainingSinkChunks() const;
	void FinishProcessing();
	bool TryMarkRuntimeOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index);
	PipelineExecuteResult FlushAndFinalizeAfterCompiledFinish(idx_t max_chunks, string &runtime_reason);

private:
	PipelineExecutor &executor;
};

} // namespace duckdb

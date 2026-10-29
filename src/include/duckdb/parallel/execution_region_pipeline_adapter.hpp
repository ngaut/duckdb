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
	Allocator &GetAllocator() const;
	bool IsCompiledExecutionSuppressed() const;
	idx_t MaxThreads() const;

	PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks);
	//! Binds a chunk-step kernel to the executor until it resets: from then on the vectorized loop hands every source
	//! chunk to the step in place of the operator chain
	void BindCompiledStep(const ExecutionRegionKernel &kernel, unique_ptr<ExecutionRegionRuntime> runtime,
	                      unique_ptr<ExecutionRegionChunkStep> step);
	//! The runtime of the chunk step bound for this kernel, or nullptr while none is bound
	optional_ptr<ExecutionRegionRuntime> CompiledStepRuntime(const ExecutionRegionKernel &kernel) const;
	bool HasSourceAndSink() const;
	optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel() const;
	//! Whether the planner selected a compiled runner for the pipeline
	bool CompiledRunnerSelected() const;
	ExecutionRegionLocalState &GetOrCreateLocalState(ExecutionRegionKernel &kernel);
	bool IsAtCleanSourceToSinkBoundary() const;

	SourceResultType FetchSourceContract(DataChunk *&result, ExecutionRegionSourceContractMetrics *metrics = nullptr);
	SourceResultType
	FetchPrimitiveAggregateStateSourceContract(ExecutionAggregateStateScanBatch *&result,
	                                           ExecutionRegionSourceContractMetrics *metrics = nullptr);
	void LatchDeferredCompiledExecution();
	ExecutionOperatorReadiness GetOperatorReadiness(idx_t operator_index,
	                                                const ExecutionRegionOperatorInfo &operator_info);
	//! True while nothing is in flight on this executor's source cursor.
	bool SourceCursorUntouched() const;
	//! EXPLAIN ANALYZE attribution: annotate this pipeline's profiling nodes.
	//! covered_operators=true also marks the intermediate operators and sink.
	void AddProfilingAnnotation(const string &key, const string &value, bool covered_operators);
	ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                         const ExecutionRegionOperatorInfo &operator_info,
	                                         ExecutionOperatorBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionAggregateSinkInfo &sink_info, ExecutionSinkBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionHashJoinInfo &sink_info, ExecutionSinkBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionOrderSinkInfo &sink_info, ExecutionSinkBinding &binding);
	//! Appends a compiled kernel's output chunk to the Sink. Once the Sink blocks, the executor owns that chunk and
	//! every later one: it sinks them in order before its loop continues or the pipeline finalizes. The chunk the
	//! Sink blocked on retries as is, because a blocked Sink may already hold it
	SinkResultType AppendSink(const ExecutionAppendSinkBinding &binding, DataChunk &chunk);
	void FinishProcessing();
	bool TryMarkRuntimeOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index);
	PipelineExecuteResult FlushAndFinalizeAfterCompiledFinish(idx_t max_chunks, string &runtime_reason);

private:
	template <class SINK_INFO>
	bool BindSinkInternal(DataChunk &input, const SINK_INFO &sink_info, ExecutionRegionSinkKind kind,
	                      ExecutionSinkBinding &binding);
	SinkResultType SinkCoalescedChunk();

private:
	PipelineExecutor &executor;
};

} // namespace duckdb

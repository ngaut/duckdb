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

	PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks);
	bool HasSourceAndSink() const;
	optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel() const;
	bool IsAtCleanSourceToSinkBoundary() const;

	SourceResultType FetchSourceContract(DataChunk *&result, ExecutionRegionSourceContractMetrics *metrics = nullptr);
	SinkNextBatchType AdvanceSinkBatch(DataChunk &source_chunk, bool have_more_output);
	optional_ptr<DataChunk> PendingSourceContractBatch();
	DataChunk &PrepareSourceContractBatch(const vector<LogicalType> &types);
	void ResetSourceContractBatch();
	ExecutionOperatorBindResult BindOperator(idx_t operator_index, DataChunk &input,
	                                         const ExecutionRegionOperatorInfo &operator_info,
	                                         ExecutionOperatorBinding &binding);
	bool BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info, ExecutionSinkBinding &binding);
	void RecordBlockedSinkChunk(DataChunk &chunk);
	void FinishProcessing();
	bool TryMarkRuntimeOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index);
	PipelineExecuteResult FlushAndFinalizeAfterCompiledFinish(idx_t max_chunks, string &runtime_reason);

private:
	PipelineExecutor &executor;
};

} // namespace duckdb

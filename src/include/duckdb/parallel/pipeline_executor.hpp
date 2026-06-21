//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/pipeline_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_execution.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/stack.hpp"

#include <functional>

namespace duckdb {
class Executor;
class ExecutionRegionPipelineAdapter;
struct ExecutionRegionSourceContractMetrics;

//! The Pipeline class represents an execution pipeline
class PipelineExecutor {
	friend class ExecutionRegionPipelineAdapter;

public:
	PipelineExecutor(ClientContext &context, Pipeline &pipeline);
	~PipelineExecutor();

	//! Fully execute a pipeline with a source and a sink until the source is completely exhausted
	PipelineExecuteResult Execute();
	//! Execute a pipeline with a source and a sink until finished, or until max_chunks were processed from the source
	//! Returns true if execution is finished, false if Execute should be called again
	PipelineExecuteResult Execute(idx_t max_chunks);

	//! Called after depleting the source: finalizes the execution of this pipeline executor
	//! This should only be called once per PipelineExecutor.
	PipelineExecuteResult PushFinalize();

	bool RemainingSinkChunk() const;

	//! Initializes a chunk with the types that will flow out of the chunk
	void InitializeChunk(DataChunk &chunk);
	//! Execute a pipeline without a sink, and retrieve a single DataChunk
	//! Returns an empty chunk when finished.

	//! Registers the task in the interrupt_state to allow Source/Sink operators to block the task
	void SetTaskForInterrupts(weak_ptr<Task> current_task);
	//! Replaces the interrupt state used by source/sink/finalize calls
	void SetInterruptState(InterruptState interrupt_state_p);

	//! Resets the executor for re-execution while reusing allocated intermediate buffers.
	//! Reuses local source/sink/operator states where operators provide explicit reset hooks,
	//! and falls back to recreation otherwise.
	void Reset();
	//! Prepare the executor for another execution, skipping Reset() on the very first run.
	void PrepareForExecution();

private:
	void PrepareExecutionSourceInputChunk();
	void InitializeOperatorExecutionState(vector<unique_ptr<DataChunk>> &chunks,
	                                      vector<unique_ptr<OperatorState>> &states,
	                                      bool disable_operator_caching = false);
	//! Pushes a chunk through the regular DuckDB pipeline path. Used by verification and vectorized runner paths.
	OperatorResultType ExecutePipelineReference(DataChunk &input, DataChunk &result, idx_t initial_index = 0);
	OperatorResultType ExecutePipelineOperators(DataChunk &input, DataChunk &result, idx_t initial_idx,
	                                            vector<unique_ptr<DataChunk>> &chunks,
	                                            vector<unique_ptr<OperatorState>> &states,
	                                            stack<idx_t> &operators_in_process, bool apply_finish_processing);
	PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks);

	//! The pipeline to process
	Pipeline &pipeline;
	//! The thread context of this executor
	ThreadContext thread;
	//! The total execution context of this executor
	ExecutionContext context;

	//! Intermediate chunks for the operators
	vector<unique_ptr<DataChunk>> intermediate_chunks;
	//! Intermediate states for the operators
	vector<unique_ptr<OperatorState>> intermediate_states;
	//! The local source state
	unique_ptr<LocalSourceState> local_source_state;
	//! The local sink state (if any)
	unique_ptr<LocalSinkState> local_sink_state;
	//! The interrupt state, holding required information for sink/source operators to block
	InterruptState interrupt_state;
	//! The final chunk used for moving data into the sink
	DataChunk final_chunk;
	//! Scratch chunk used when compiled source-contract execution fetches a source chunk before generated prefix code.
	DataChunk execution_source_input_chunk;

	//! The operators that are not yet finished executing and have data remaining
	//! If the stack of in_process_operators is empty, we fetch from the source instead
	stack<idx_t> in_process_operators;
	//! Whether or not the pipeline has been finalized (used for verification only)
	bool finalized = false;
	//! Whether or not the pipeline has finished processing
	int32_t finished_processing_idx = -1;
	//! Partition info that is used by this executor
	OperatorPartitionInfo required_partition_info;

	//! Source operator indicated that there is no more output possible
	bool exhausted_source = false;
	//! Source or intermediate operator indicated that there is no more output possible
	bool exhausted_pipeline = false;
	//! Flushing of intermediate operators has started
	bool started_flushing = false;
	//! Flushing of caching operators is done
	bool done_flushing = false;

	//! Whether FinishSource has already been called (so FinalizeSource is skipped in PushFinalize)
	bool source_profiling_finalized = false;

	//! This flag is set when the pipeline gets interrupted by the Sink -> the final_chunk should be re-sink-ed.
	bool remaining_sink_chunk = false;

	//! This flag is set when the pipeline gets interrupted by NextBatch -> NextBatch should be called again and the
	//! source_chunk should be sent through the pipeline
	bool next_batch_blocked = false;

	//! Current operator being flushed
	idx_t flushing_idx;
	//! Whether the current flushing_idx should be flushed: this needs to be stored to make flushing code re-entrant
	bool should_flush_current_idx = true;
	//! Whether this executor has already run at least once
	bool has_executed = false;
	//! Operator index where processing should resume for the most recently fetched source chunk.
	idx_t source_chunk_initial_idx = 0;

private:
	void StartOperator(PhysicalOperator &op);
	void EndOperator(PhysicalOperator &op, optional_ptr<DataChunk> chunk);

	//! Reset the operator index to the first operator
	void GoToSource(idx_t &current_idx, idx_t initial_idx, stack<idx_t> &operators_in_process);
	DataChunk &GetSourceChunkForInitialIdx(idx_t initial_idx);
	SourceResultType FetchFromSource(DataChunk *&result);
	SourceResultType FetchFromSourceContract(DataChunk *&result,
	                                         ExecutionRegionSourceContractMetrics *metrics = nullptr);

	void FinishProcessing(int32_t operator_idx = -1);
	bool IsFinished();

	//! Wrappers for sink/source calls to respective operators
	SourceResultType GetData(DataChunk &chunk, OperatorSourceInput &input);
	SinkResultType Sink(DataChunk &chunk, OperatorSinkInput &input);

	OperatorResultType ExecutePushInternal(DataChunk &input, ExecutionBudget &chunk_budget, idx_t initial_idx = 0);
	//! Pushes a chunk through the pipeline and returns a single result chunk
	//! Returns whether or not a new input chunk is needed, or whether or not we are finished
	OperatorResultType Execute(DataChunk &input, DataChunk &result, idx_t initial_index = 0);

	//! Notifies the sink that a new batch has started
	SinkNextBatchType NextBatch(DataChunk &source_chunk, const bool have_more_output);

	//! Tries to flush all state from intermediate operators. Will return true if all state is flushed, false in the
	//! case of a blocked sink.
	bool TryFlushCachingOperators(ExecutionBudget &chunk_budget);

	static bool CanCacheType(const LogicalType &type);
	void CacheChunk(DataChunk &input, idx_t operator_idx);

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	//! Debugging state: number of times blocked
	int debug_blocked_sink_count = 0;
	int debug_blocked_source_count = 0;
	int debug_blocked_combine_count = 0;
	int debug_blocked_next_batch_count = 0;
	//! Number of times the Sink/Source will block before actually returning data
	int debug_blocked_target_count = 1;
#endif
};

} // namespace duckdb

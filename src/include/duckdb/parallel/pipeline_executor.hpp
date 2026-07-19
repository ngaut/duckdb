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
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/stack.hpp"

#include <array>
#include <functional>

namespace duckdb {
class Executor;
class ExecutionRegionPipelineAdapter;
class ExecutionRegionKernel;
class ExecutionRegionLocalState;
struct ExecutionRegionSourceContractMetrics;

//! The Pipeline class represents an execution pipeline
//! What this executor's source cursor has done, and therefore which runner
//! transitions are legal. Runners may only change where no cursor holds an
//! in-flight row group; this state makes the legality queries structural
//! instead of scattered booleans.
enum class SourceCursorState : uint8_t {
	//! Nothing in flight: entry deferral and compiled entry are both legal.
	UNTOUCHED,
	//! The compiled source contract owns the cursor: only compiled execution
	//! may continue; deferral is legal only through the declined-boundary path
	//! (or after the one-way deferral latch hands the pipeline over).
	COMPILED_CONTRACT,
	//! A budgeted measurement leg is fetching: a declined yield returns the
	//! cursor to UNTOUCHED, anything else degrades it to VECTORIZED_UNMANAGED.
	VECTORIZED_MANAGED,
	//! Unmanaged vectorized fetches happened: the cursor can hold a partially
	//! scanned row group at any yield, so this executor is latched vectorized.
	VECTORIZED_UNMANAGED,
};

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
	std::array<vector<bool>, EXECUTION_REGION_RUNTIME_ONCE_FLAG_COUNT> execution_region_runtime_once_flags;
	unique_ptr<ExecutionRegionLocalState> execution_region_local_state;
	optional_ptr<const ExecutionRegionKernel> execution_region_local_state_kernel;

	//! The operators that are not yet finished executing and have data remaining
	//! If the stack of in_process_operators is empty, we fetch from the source instead
	stack<idx_t> in_process_operators;
	//! Whether or not the pipeline has been finalized (used for verification only)
	bool finalized = false;
	//! Whether or not the pipeline has finished processing
	int32_t finished_processing_idx = -1;
	//! Partition info that is used by this executor
	OperatorPartitionInfo required_partition_info;

	//! Remaining vectorized new-row-group claims; INVALID_INDEX means unlimited.
	idx_t vectorized_source_claim_budget = DConstants::INVALID_INDEX;
	//! Set when the source declined a claim and the executor yielded at the boundary.
	bool vectorized_source_declined_yield = false;
	//! The source-cursor state machine; transitions live in FetchFromSource and
	//! FetchFromSourceContract only.
	SourceCursorState source_cursor_state = SourceCursorState::UNTOUCHED;
	idx_t vectorized_source_leg_rows = 0;
	//! Latched when a compiled kernel defers: deferral hands this pipeline to the
	//! vectorized continuation permanently, because a deferred kernel's terminal
	//! state does not support re-entry.
	bool compiled_execution_deferred = false;
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
	//! Runner-switch support: the vectorized source declines new row-group claims
	//! once the budget is exhausted; the executor then yields at the boundary.
	void SetVectorizedSourceClaimBudget(idx_t budget);
	void ClearVectorizedSourceClaimBudget();
	//! Boundary variant: the declined fetch proved the cursor sits at a
	//! row-group boundary with nothing in flight, so it returns to UNTOUCHED.
	void ClearVectorizedSourceClaimBudgetAtBoundary();
	bool ConsumeVectorizedSourceDeclinedYield();
	bool HasVectorizedSourceClaimBudget() const;
	idx_t VectorizedSourceLegRows() const;
	SourceResultType FetchFromSourceContract(DataChunk *&result,
	                                         ExecutionRegionSourceContractMetrics *metrics = nullptr,
	                                         bool decline_new_row_group = false,
	                                         bool *declined_new_row_group = nullptr);
	bool TryMarkExecutionRegionRuntimeOnceFlag(ExecutionRegionRuntimeOnceFlag flag, idx_t index);

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

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
class ColumnDataCollection;
class Executor;
class ExecutionRegionPipelineAdapter;
class ExecutionRegionChunkStep;
class ExecutionRegionKernel;
class ExecutionRegionLocalState;
struct ExecutionRegionSourceContractMetrics;

//! Which fetch path this executor's source cursor took. A pipeline runs on one path from its first fetch to its end,
//! so a compiled kernel may only start, or leave the pipeline to the vectorized loop, while nothing was fetched
enum class SourceCursorState : uint8_t { UNTOUCHED, COMPILED_CONTRACT, VECTORIZED };

class PipelineExecutor {
	friend class ExecutionRegionPipelineAdapter;

public:
	PipelineExecutor(ClientContext &context, Pipeline &pipeline, optional_idx reserved_batch_index = optional_idx());
	~PipelineExecutor();

	//! Fully execute a pipeline with a source and a sink until the source is completely exhausted
	PipelineExecuteResult Execute();
	//! Execute a pipeline with a source and a sink until finished, or until max_chunks were processed from the source
	//! Returns true if execution is finished, false if Execute should be called again
	PipelineExecuteResult Execute(idx_t max_chunks);
	//! Pushes a chunk from an external producer through this pipeline into the sink
	PipelineExecuteResult PushExternal(DataChunk &input, const OperatorPartitionData &partition_data,
	                                   optional_idx source_min_batch_index);
	//! Advances an externally-fed pipeline to the producer's next batch
	PipelineExecuteResult NextBatchExternal(const OperatorPartitionData &partition_data,
	                                        optional_idx source_min_batch_index);
	//! Completes the current batch of an externally-fed pipeline
	PipelineExecuteResult FinishBatchExternal(optional_idx source_min_batch_index);
	//! Propagates a minimum batch index change through an externally-fed pipeline
	PipelineExecuteResult UpdateMinBatchIndexExternal(optional_idx source_min_batch_index);
	//! Finalizes an externally-fed pipeline executor after the producer is exhausted
	PipelineExecuteResult FinishExternal(optional_idx source_min_batch_index);

	//! Called after depleting the source: finalizes the execution of this pipeline executor
	//! This should only be called once per PipelineExecutor.
	PipelineExecuteResult PushFinalize();

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
	//! Whether this executor stopped consuming input early
	bool IsFinishedProcessing() const;

private:
	enum class PipelineInputChunkMode : uint8_t { PUSH_INPUT, RESUME_INPUT };
	enum class SourceFinishNotificationState : uint8_t { PENDING, SENT };
	enum class PendingBatchAdvanceState : uint8_t { NONE, NEXT_BATCH, MINIMUM_UPDATE };
	struct SourceFetchResult {
		SourceResultType result = SourceResultType::BLOCKED;
		SourceBatchIndexState batch_index_state = SourceBatchIndexState::UNCHANGED;
	};

	void PrepareExecutionSourceInputChunk();
	PipelineExecuteResult ExecuteVectorizedPipeline(idx_t max_chunks);
	//! The chunk the loop fetches into: a bound chunk step reads the source contract's chunk
	DataChunk &LoopSourceChunk();
	SourceFetchResult FetchLoopSource(DataChunk &result);
	//! Pushes a source chunk through the bound chunk step, or else through the operator chain
	OperatorResultType PushSourceChunk(DataChunk &input, ExecutionBudget &chunk_budget);
	//! A partitioned sink takes rows under the partition current when they sink: the rows a chunk step holds sink
	//! before the fetched chunk moves the sink to another partition
	OperatorResultType FlushCompiledStepBeforeAdvance(DataChunk &source_chunk, const SourceFetchResult &source_result);
	//! The same for pushed input, before the producer moves the sink to next_batch_index. A flush that blocked the
	//! sink leaves the advance pending: the producer's retry sinks the flushed rows, then advances
	PipelineExecuteResult FlushCompiledStepBeforeExternalAdvance(idx_t next_batch_index);

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

	//! The global source state used by this executor
	shared_ptr<GlobalSourceState> global_source_state;
	//! The local source state
	unique_ptr<LocalSourceState> local_source_state;
	//! The local sink state (if any)
	unique_ptr<LocalSinkState> local_sink_state;
	//! The interrupt state, holding required information for sink/source operators to block
	InterruptState interrupt_state;
	//! The final chunk used for moving data into the sink
	DataChunk final_chunk;
	//! The source chunk of the pipeline loop while a compiled chunk step is bound.
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

	//! Transitions live in FetchFromSource and FetchFromCompiledSourceContract only
	SourceCursorState source_cursor_state = SourceCursorState::UNTOUCHED;
	//! Latched when a compiled kernel cannot start: the vectorized loop runs this pipeline to its end
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
	//! Whether the source has been told this executor will not fetch more input
	SourceFinishNotificationState source_finish_notification_state = SourceFinishNotificationState::PENDING;

	//! This flag is set when the pipeline gets interrupted by the Sink -> the final_chunk should be re-sink-ed.
	bool remaining_sink_chunk = false;
	//! The output a compiled kernel appended after its sink blocked on final_chunk. It sinks in order after
	//! final_chunk, so the kernel finishes its source chunk instead of dropping the rest of that chunk's output
	unique_ptr<ColumnDataCollection> queued_sink_chunks;
	idx_t next_queued_sink_chunk = 0;
	//! Small chunks a compiled kernel appended, held back until they fill a chunk as a caching operator holds back
	//! its output: a sink pays per call, and a compiled probe emits a few rows per source chunk. Only sinks without
	//! partition requirements coalesce, so no partition advance has to wait for these rows
	DataChunk coalesced_sink_chunk;
	bool coalesced_sink_chunk_initialized = false;
	//! The chunk-step kernel bound to this executor until it resets, the runtime the step calls back through, and the
	//! step (declared last: it refers to both the runtime and the region local state). Once bound, the loop fetches,
	//! advances batches and sinks, and the step replaces the operator chain for every source chunk
	optional_ptr<const ExecutionRegionKernel> compiled_step_kernel;
	unique_ptr<ExecutionRegionRuntime> compiled_step_runtime;
	unique_ptr<ExecutionRegionChunkStep> compiled_step;
	//! Whether the chunk step sank the rows it held once the source was exhausted
	bool compiled_step_finished = false;
	//! The chunk the operator chain runs once the chunk step handed the pipeline to it: the step still fetches the
	//! source, and the operators, their flushes and the sink are DuckDB's own
	optional_ptr<DataChunk> compiled_step_native_input;
	//! Whether DuckDB's operators ran any chunk of the compiled step: their caching operators then flush at the end
	bool compiled_step_continued_natively = false;

	//! This flag is set when the pipeline gets interrupted by NextBatch -> NextBatch should be called again and the
	//! source_chunk should be sent through the pipeline
	bool next_batch_blocked = false;
	PendingBatchAdvanceState pending_batch_advance_state = PendingBatchAdvanceState::NONE;

	//! Current operator being flushed
	idx_t flushing_idx;
	//! Whether the current flushing_idx should be flushed: this needs to be stored to make flushing code re-entrant
	bool should_flush_current_idx = true;
	//! Whether this executor has already run at least once
	bool has_executed = false;
	//! Whether an externally-fed sink has observed its initial batch
	bool external_batch_initialized = false;
	//! Rows of the last source chunk that have not been reported to the progress verifier yet
	idx_t unsampled_progress_rows = 0;

private:
	void StartOperator(PhysicalOperator &op);
	void EndOperator(PhysicalOperator &op, optional_ptr<DataChunk> chunk);

	//! Reset the operator index to the first operator
	void GoToSource(idx_t &current_idx, idx_t initial_idx);
	SourceFetchResult FetchFromSource(DataChunk &result);
	SourceResultType FetchFromSourceContract(DataChunk *&result,
	                                         ExecutionRegionSourceContractMetrics *metrics = nullptr);
	SourceResultType
	FetchFromPrimitiveAggregateStateSourceContract(ExecutionAggregateStateScanBatch *&result,
	                                               ExecutionRegionSourceContractMetrics *metrics = nullptr);
	using SupportsSourceContract = bool (PhysicalOperator::*)(const ExecutionRegionOpenRequest &) const;
	//! The compiled source-contract fetch protocol shared by both contract fetches: cursor claim, source profiling,
	//! stage metrics and exhaustion. FETCH performs the source call; source_chunk is the chunk it fills, if any.
	template <class FETCH>
	SourceResultType FetchFromCompiledSourceContract(SupportsSourceContract supports_contract,
	                                                 ExecutionRegionSourceContractMetrics *metrics,
	                                                 optional_ptr<DataChunk> source_chunk, FETCH &&fetch);
	bool TryMarkExecutionRegionRuntimeOnceFlag(ExecutionRegionRuntimeOnceFlag flag, idx_t index);
	//! Reports the last source chunk to the progress verifier (if any), once it has been fully processed
	void SampleProgress();

	void FinishProcessing(int32_t operator_idx = -1);
	void NotifySourceFinished();
	bool IsFinished() const;

	//! Wrappers for sink/source calls to respective operators
	SourceResultType GetData(DataChunk &chunk, OperatorSourceInput &input);
	SinkResultType Sink(DataChunk &chunk, OperatorSinkInput &input);
	SinkNextBatchType UpdateMinBatchIndex();

	OperatorResultType ExecutePushInternal(DataChunk &input, ExecutionBudget &chunk_budget, idx_t initial_idx = 0);
	//! Sinks final_chunk, which the Sink blocked on, and then every queued sink chunk
	OperatorResultType SinkRemainingChunks(ExecutionBudget &chunk_budget);
	//! Queues a chunk for SinkRemainingChunks: the first becomes final_chunk, later ones follow it in order
	void EnqueueSinkChunk(DataChunk &chunk);
	//! Moves the coalesced rows of a compiled kernel into the sink queue, ahead of anything queued after them
	void QueueCoalescedSinkChunk();
	PipelineExecuteResult PushInputChunk(DataChunk &input, ExecutionBudget &chunk_budget, PipelineInputChunkMode mode);
	PipelineExecuteResult FlushAndFinalize(ExecutionBudget &chunk_budget);
	//! Pushes a chunk through the pipeline and returns a single result chunk
	//! Returns whether or not a new input chunk is needed, or whether or not we are finished
	OperatorResultType Execute(DataChunk &input, DataChunk &result, idx_t initial_index = 0);

	//! Notifies the sink that a new batch has started
	SinkNextBatchType NextBatch(DataChunk &source_chunk, const SourceFetchResult &source_result);
	//! Whether NextBatch for this source chunk moves the sink to another batch
	bool NextBatchAdvances(DataChunk &source_chunk, const SourceFetchResult &source_result);
	OperatorPartitionData NextPartitionData(DataChunk &source_chunk, const SourceFetchResult &source_result);
	SinkNextBatchType NextBatch(OperatorPartitionData next_data, bool force = false,
	                            optional_idx external_min_batch_index = optional_idx());
	OperatorPartitionData ToPipelinePartitionData(const OperatorPartitionData &source_data) const;
	idx_t MapExternalMinBatchIndex(optional_idx source_min_batch_index) const;

	//! Tries to flush all state from intermediate operators. Will return true if all state is flushed, false in the
	//! case of a blocked sink.
	bool TryFlushCachingOperators(ExecutionBudget &chunk_budget);

	static bool CanCacheType(const LogicalType &type);
	void CacheChunk(DataChunk &input, idx_t operator_idx);

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	//! Blocks until the target count is reached, spawning a thread that fires the interrupt callback shortly after
	bool TryDebugBlock(int &debug_counter, const InterruptState &interrupt_state_p);

	//! Debugging state: number of times blocked
	int debug_blocked_sink_count = 0;
	int debug_blocked_source_count = 0;
	int debug_blocked_combine_count = 0;
	int debug_blocked_next_batch_count = 0;
	int debug_blocked_min_batch_count = 0;
	//! Number of times the Sink/Source will block before actually returning data
	int debug_blocked_target_count = 1;
#endif
};

} // namespace duckdb

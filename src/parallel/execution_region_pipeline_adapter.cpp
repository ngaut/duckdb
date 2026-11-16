#include "duckdb/parallel/execution_region_pipeline_adapter.hpp"
#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_plan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"

namespace duckdb {

ExecutionRegionPipelineAdapter::ExecutionRegionPipelineAdapter(PipelineExecutor &executor_p) : executor(executor_p) {
}

ClientContext &ExecutionRegionPipelineAdapter::GetClientContext() const {
	return executor.context.client;
}

Allocator &ExecutionRegionPipelineAdapter::GetAllocator() const {
	return BufferAllocator::Get(GetClientContext());
}

bool ExecutionRegionPipelineAdapter::IsCompiledExecutionSuppressed() const {
	return GetClientContext().IsCompiledExecutionSuppressed() || executor.compiled_execution_deferred;
}

idx_t ExecutionRegionPipelineAdapter::MaxThreads() const {
	return executor.pipeline.GetMaxThreads();
}

PipelineExecuteResult ExecutionRegionPipelineAdapter::ExecuteVectorizedPipeline(idx_t max_chunks) {
	return executor.ExecuteVectorizedPipeline(max_chunks);
}

void ExecutionRegionPipelineAdapter::BindCompiledStep(const ExecutionRegionKernel &kernel,
                                                      unique_ptr<ExecutionRegionRuntime> runtime,
                                                      unique_ptr<ExecutionRegionChunkStep> step) {
	if (executor.compiled_step || !SourceCursorUntouched()) {
		throw InternalException("chunk step bound to an executor that already runs its pipeline");
	}
	executor.compiled_step_kernel = &kernel;
	executor.compiled_step_runtime = std::move(runtime);
	executor.compiled_step = std::move(step);
}

optional_ptr<ExecutionRegionRuntime>
ExecutionRegionPipelineAdapter::CompiledStepRuntime(const ExecutionRegionKernel &kernel) const {
	if (!executor.compiled_step) {
		return nullptr;
	}
	if (executor.compiled_step_kernel.get() != &kernel) {
		throw InternalException("pipeline executor runs a chunk step of another kernel");
	}
	return executor.compiled_step_runtime.get();
}

bool ExecutionRegionPipelineAdapter::HasSourceAndSink() const {
	return executor.pipeline.GetSource() && executor.pipeline.GetSink();
}

optional_ptr<ExecutionRegionKernel> ExecutionRegionPipelineAdapter::GetExecutableFullPipelineKernel() const {
	auto plan = executor.pipeline.GetExecutionRegionPlan();
	if (!plan) {
		return nullptr;
	}
	return plan->GetExecutableFullPipelineKernel();
}

bool ExecutionRegionPipelineAdapter::CompiledRunnerSelected() const {
	return executor.pipeline.GetExecutionRunnerKind() != ExecutionRunnerKind::VECTORIZED;
}

ExecutionRegionLocalState &ExecutionRegionPipelineAdapter::GetOrCreateLocalState(ExecutionRegionKernel &kernel) {
	if (executor.execution_region_local_state_kernel.get() != &kernel) {
		executor.execution_region_local_state = kernel.CreateLocalState(GetAllocator());
		if (!executor.execution_region_local_state) {
			throw InternalException("execution region kernel returned no pipeline-local state");
		}
		executor.execution_region_local_state_kernel = &kernel;
	}
	return *executor.execution_region_local_state;
}

bool ExecutionRegionPipelineAdapter::IsAtCleanSourceToSinkBoundary() const {
	// No rows wait between source and sink and the source is not exhausted, so a self-looping kernel can start fetching
	return executor.in_process_operators.empty() && !executor.remaining_sink_chunk && !executor.next_batch_blocked &&
	       !executor.started_flushing && !executor.done_flushing && !executor.finalized && !executor.exhausted_source &&
	       !executor.exhausted_pipeline;
}

SourceResultType ExecutionRegionPipelineAdapter::FetchSourceContract(DataChunk *&result,
                                                                     ExecutionRegionSourceContractMetrics *metrics) {
	auto source_result = executor.FetchFromSourceContract(result, metrics);
	if (source_result == SourceResultType::FINISHED) {
		executor.exhausted_source = true;
		executor.exhausted_pipeline = true;
	}
	return source_result;
}

SourceResultType ExecutionRegionPipelineAdapter::FetchPrimitiveAggregateStateSourceContract(
    ExecutionAggregateStateScanBatch *&result, ExecutionRegionSourceContractMetrics *metrics) {
	auto source_result = executor.FetchFromPrimitiveAggregateStateSourceContract(result, metrics);
	if (source_result == SourceResultType::FINISHED) {
		executor.exhausted_source = true;
		executor.exhausted_pipeline = true;
	}
	return source_result;
}

void ExecutionRegionPipelineAdapter::LatchDeferredCompiledExecution() {
	executor.compiled_execution_deferred = true;
}

ExecutionOperatorReadiness
ExecutionRegionPipelineAdapter::GetOperatorReadiness(idx_t operator_index,
                                                     const ExecutionRegionOperatorInfo &operator_info) {
	auto &operators = executor.pipeline.GetIntermediateOperators();
	if (operator_index >= operators.size()) {
		ExecutionOperatorReadiness readiness;
		readiness.kind = operator_info.Kind();
		readiness.blocker = "execution-operator-runtime-index-out-of-range";
		return readiness;
	}
	return operators[operator_index].get().GetExecutionOperatorReadiness(GetClientContext(), operator_info);
}

bool ExecutionRegionPipelineAdapter::SourceCursorUntouched() const {
	return executor.source_cursor_state == SourceCursorState::UNTOUCHED;
}

void ExecutionRegionPipelineAdapter::AddProfilingAnnotation(const string &key, const string &value,
                                                            bool covered_operators) {
	auto &profiler = QueryProfiler::Get(executor.context.client);
	if (!profiler.IsEnabled()) {
		return;
	}
	auto &pipeline = executor.pipeline;
	auto source = pipeline.GetSource();
	if (source) {
		profiler.AddOperatorAnnotation(*source, key, value);
	}
	if (!covered_operators) {
		return;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		profiler.AddOperatorAnnotation(op.get(), key, value);
	}
	if (pipeline.GetSink()) {
		profiler.AddOperatorAnnotation(*pipeline.GetSink(), key, value);
	}
}

ExecutionOperatorBindResult
ExecutionRegionPipelineAdapter::BindOperator(idx_t operator_index, DataChunk &input,
                                             const ExecutionRegionOperatorInfo &operator_info,
                                             ExecutionOperatorBinding &binding) {
	binding = ExecutionOperatorBinding();
	binding.kind = operator_info.Kind();
	auto &operators = executor.pipeline.GetIntermediateOperators();
	if (operator_index >= operators.size()) {
		binding.blocker = "execution-operator-runtime-index-out-of-range";
		return ExecutionOperatorBindResult::INVALID;
	}
	if (operator_index >= executor.intermediate_states.size() || !executor.intermediate_states[operator_index]) {
		binding.blocker = "execution-operator-runtime-missing-local-state";
		return ExecutionOperatorBindResult::INVALID;
	}
	auto &op = operators[operator_index].get();
	if (!op.op_state) {
		binding.blocker = "execution-operator-runtime-missing-global-state";
		return ExecutionOperatorBindResult::INVALID;
	}
	return op.BindExecutionOperator(executor.context, input, *op.op_state,
	                                *executor.intermediate_states[operator_index], operator_info, binding);
}

template <class SINK_INFO>
bool ExecutionRegionPipelineAdapter::BindSinkInternal(DataChunk &input, const SINK_INFO &sink_info,
                                                      ExecutionRegionSinkKind kind, ExecutionSinkBinding &binding) {
	binding = ExecutionSinkBinding();
	binding.kind = kind;
	auto sink = executor.pipeline.GetSink();
	if (!sink) {
		throw InternalException("compiled region runtime cannot sink without a pipeline sink");
	}
	if (!executor.local_sink_state) {
		throw InternalException("compiled region runtime cannot sink without local sink state");
	}
	OperatorSinkInput sink_input {*sink->sink_state, *executor.local_sink_state, executor.interrupt_state};
	return sink->BindExecutionSink(executor.context, input, sink_input, sink_info, binding);
}

bool ExecutionRegionPipelineAdapter::BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
                                              ExecutionSinkBinding &binding) {
	return BindSinkInternal(input, sink_info, sink_info.Kind(), binding);
}

bool ExecutionRegionPipelineAdapter::BindSink(DataChunk &input, const ExecutionRegionAggregateSinkInfo &sink_info,
                                              ExecutionSinkBinding &binding) {
	return BindSinkInternal(input, sink_info, sink_info.Kind(), binding);
}

bool ExecutionRegionPipelineAdapter::BindSink(DataChunk &input, const ExecutionRegionHashJoinInfo &sink_info,
                                              ExecutionSinkBinding &binding) {
	return BindSinkInternal(input, sink_info, ExecutionRegionSinkKind::HASH_JOIN_BUILD, binding);
}

bool ExecutionRegionPipelineAdapter::BindSink(DataChunk &input, const ExecutionRegionOrderSinkInfo &sink_info,
                                              ExecutionSinkBinding &binding) {
	return BindSinkInternal(input, sink_info, ExecutionRegionSinkKind::SORT, binding);
}

//! The rows the core holds for a kernel sink through the operator's Sink, as the retry of a blocked chunk does
SinkResultType ExecutionRegionPipelineAdapter::SinkCoalescedChunk() {
	auto &coalesced = executor.coalesced_sink_chunk;
	if (!executor.coalesced_sink_chunk_initialized || coalesced.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
	                              executor.interrupt_state};
	auto result = executor.Sink(coalesced, sink_input);
	if (result == SinkResultType::BLOCKED) {
		executor.EnqueueSinkChunk(coalesced);
		result = SinkResultType::NEED_MORE_INPUT;
	}
	coalesced.Reset();
	return result;
}

//! A column that carries a global dictionary keeps that dictionary for the producer's lifetime: downstream caching
//! operators accumulate its indices and treat a change of identity as a producer bug. Coalescing flattens, so such
//! rows reach the sink as they are.
static bool ChunkCarriesGlobalDictionary(const DataChunk &chunk) {
	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		if (DictionaryVector::IsGlobalDictionary(chunk.data[col_idx])) {
			return true;
		}
	}
	return false;
}

SinkResultType ExecutionRegionPipelineAdapter::AppendSink(const ExecutionAppendSinkBinding &binding, DataChunk &chunk) {
	if (executor.remaining_sink_chunk) {
		executor.EnqueueSinkChunk(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	auto &coalesced = executor.coalesced_sink_chunk;
	const bool coalesce = chunk.size() <= CachingPhysicalOperator::CACHE_THRESHOLD &&
	                      !executor.required_partition_info.AnyRequired() && !ChunkCarriesGlobalDictionary(chunk);
	if (coalesce) {
		if (!executor.coalesced_sink_chunk_initialized) {
			coalesced.Initialize(GetAllocator(), executor.final_chunk.GetTypes());
			executor.coalesced_sink_chunk_initialized = true;
		}
		if (coalesced.size() + chunk.size() <= STANDARD_VECTOR_SIZE) {
			coalesced.Append(chunk);
			return SinkResultType::NEED_MORE_INPUT;
		}
	}
	// The chunk does not join the coalesced rows: they sink first
	auto result = SinkCoalescedChunk();
	if (result == SinkResultType::FINISHED) {
		return result;
	}
	if (executor.remaining_sink_chunk) {
		// The coalesced rows blocked the Sink: the chunk queues behind them
		executor.EnqueueSinkChunk(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (coalesce) {
		coalesced.Append(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	result = executor.TryDebugBlock(executor.debug_blocked_sink_count, executor.interrupt_state)
	             ? SinkResultType::BLOCKED
	             : ExecutionSinkAppend(binding, chunk);
#else
	result = ExecutionSinkAppend(binding, chunk);
#endif
	if (result != SinkResultType::BLOCKED) {
		return result;
	}
	executor.EnqueueSinkChunk(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkResultType ExecutionRegionPipelineAdapter::PushOperators(idx_t operator_index, DataChunk &chunk, idx_t &sink_rows) {
	return executor.PushCompiledStepOperators(operator_index, chunk, sink_rows);
}

void ExecutionRegionPipelineAdapter::FinishProcessing() {
	executor.FinishProcessing();
}

bool ExecutionRegionPipelineAdapter::TryMarkRuntimeOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index) {
	return executor.TryMarkExecutionRegionRuntimeOnceFlag(flag, index);
}

PipelineExecuteResult ExecutionRegionPipelineAdapter::FlushAndFinalizeAfterCompiledFinish(idx_t max_chunks,
                                                                                          string &runtime_reason) {
	// The vectorized loop's finish protocol is the single authority: pending sink chunk, caching-operator flush,
	// final batch advance for partition-aware sinks, then sink finalize.
	ExecutionBudget chunk_budget(max_chunks);
	auto pipeline_result = executor.FlushAndFinalize(chunk_budget);
	if (pipeline_result == PipelineExecuteResult::FINISHED) {
		runtime_reason = "full pipeline kernel executed; core flushed final operators and finalized sink";
	} else {
		runtime_reason = "full pipeline kernel executed; core final-operator flush is pending";
	}
	return pipeline_result;
}

} // namespace duckdb

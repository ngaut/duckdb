#include "duckdb/parallel/execution_region_pipeline_adapter.hpp"
#include "duckdb/main/query_profiler.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_plan.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"

namespace duckdb {

ExecutionRegionPipelineAdapter::ExecutionRegionPipelineAdapter(PipelineExecutor &executor_p) : executor(executor_p) {
}

ClientContext &ExecutionRegionPipelineAdapter::GetClientContext() const {
	return executor.context.client;
}

ExecutionContext &ExecutionRegionPipelineAdapter::GetExecutionContext() const {
	return executor.context;
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

bool ExecutionRegionPipelineAdapter::PreserveSourceChunkBoundaries() const {
	return executor.required_partition_info.AnyRequired();
}

PipelineExecuteResult ExecutionRegionPipelineAdapter::ExecuteVectorizedPipeline(idx_t max_chunks) {
	return executor.ExecuteVectorizedPipeline(max_chunks);
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
	return executor.in_process_operators.empty() && !executor.remaining_sink_chunk && !executor.next_batch_blocked &&
	       !executor.started_flushing && !executor.done_flushing && !executor.exhausted_source &&
	       !executor.exhausted_pipeline && !executor.finalized;
}

SourceResultType ExecutionRegionPipelineAdapter::FetchSourceContract(DataChunk *&result,
                                                                     ExecutionRegionSourceContractMetrics *metrics,
                                                                     bool decline_new_row_group,
                                                                     bool *declined_new_row_group) {
	ExecutionRegionSuppressionGuard guard(GetClientContext());
	auto source_result =
	    executor.FetchFromSourceContract(result, metrics, decline_new_row_group, declined_new_row_group);
	if (source_result == SourceResultType::FINISHED) {
		executor.exhausted_source = true;
		executor.exhausted_pipeline = true;
	}
	return source_result;
}

void ExecutionRegionPipelineAdapter::SetVectorizedSourceClaimBudget(idx_t budget) {
	executor.SetVectorizedSourceClaimBudget(budget);
}

void ExecutionRegionPipelineAdapter::ClearVectorizedSourceClaimBudget() {
	executor.ClearVectorizedSourceClaimBudget();
}

bool ExecutionRegionPipelineAdapter::HasVectorizedSourceClaimBudget() const {
	return executor.HasVectorizedSourceClaimBudget();
}

void ExecutionRegionPipelineAdapter::LatchDeferredCompiledExecution() {
	executor.compiled_execution_deferred = true;
}

idx_t ExecutionRegionPipelineAdapter::VectorizedSourceLegRows() const {
	return executor.VectorizedSourceLegRows();
}

bool ExecutionRegionPipelineAdapter::ConsumeVectorizedSourceDeclinedYield() {
	return executor.ConsumeVectorizedSourceDeclinedYield();
}

SinkNextBatchType ExecutionRegionPipelineAdapter::AdvanceSinkBatch(DataChunk &source_chunk, bool have_more_output) {
	if (!executor.required_partition_info.AnyRequired()) {
		return SinkNextBatchType::READY;
	}
	auto result = executor.NextBatch(source_chunk, have_more_output);
	executor.next_batch_blocked = result == SinkNextBatchType::BLOCKED;
	return result;
}

ExecutionOperatorReadiness
ExecutionRegionPipelineAdapter::GetOperatorReadiness(idx_t operator_index,
                                                     const ExecutionRegionOperatorInfo &operator_info) {
	auto &operators = executor.pipeline.GetIntermediateOperators();
	if (operator_index >= operators.size()) {
		ExecutionOperatorReadiness readiness;
		readiness.kind = operator_info.kind;
		readiness.blocker = "execution-operator-runtime-index-out-of-range";
		return readiness;
	}
	return operators[operator_index].get().GetExecutionOperatorReadiness(GetClientContext(), operator_info);
}

bool ExecutionRegionPipelineAdapter::SourceContractFetched() const {
	return executor.compiled_source_contract_fetched;
}

bool ExecutionRegionPipelineAdapter::VectorizedSourceCursorDirty() const {
	return executor.vectorized_source_unmanaged_fetch;
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
	binding.kind = operator_info.kind;
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

bool ExecutionRegionPipelineAdapter::BindSink(DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
                                              ExecutionSinkBinding &binding) {
	binding = ExecutionSinkBinding();
	binding.kind = sink_info.kind;
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

void ExecutionRegionPipelineAdapter::TakeBlockedSinkChunk(DataChunk &chunk) {
	executor.final_chunk.Reset();
	chunk.Copy(executor.final_chunk);
	chunk.Reset();
	executor.remaining_sink_chunk = true;
}

void ExecutionRegionPipelineAdapter::FinishProcessing() {
	executor.FinishProcessing();
}

bool ExecutionRegionPipelineAdapter::TryMarkRuntimeOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index) {
	return executor.TryMarkExecutionRegionRuntimeOnceFlag(flag, index);
}

PipelineExecuteResult ExecutionRegionPipelineAdapter::FlushAndFinalizeAfterCompiledFinish(idx_t max_chunks,
                                                                                          string &runtime_reason) {
	PipelineExecuteResult pipeline_result;
	if (executor.done_flushing) {
		pipeline_result = PipelineExecuteResult::FINISHED;
	} else {
		ExecutionBudget chunk_budget(max_chunks);
		auto flush_completed = executor.TryFlushCachingOperators(chunk_budget);
		if (flush_completed) {
			executor.done_flushing = true;
			pipeline_result = PipelineExecuteResult::FINISHED;
		} else if (executor.remaining_sink_chunk) {
			pipeline_result = PipelineExecuteResult::INTERRUPTED;
		} else {
			D_ASSERT(chunk_budget.IsDepleted());
			pipeline_result = PipelineExecuteResult::NOT_FINISHED;
		}
	}
	if (pipeline_result == PipelineExecuteResult::FINISHED) {
		pipeline_result = executor.PushFinalize();
		runtime_reason = "full pipeline kernel executed; core flushed final operators and finalized sink";
	} else {
		runtime_reason = "full pipeline kernel executed; core final-operator flush is pending";
	}
	return pipeline_result;
}

} // namespace duckdb

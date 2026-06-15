#include "duckdb/execution/jit/region_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/stack.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/jit/manager.hpp"
#include "duckdb/execution/jit/runtime.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"

#include <chrono>

namespace duckdb {

static int64_t JitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static string JitFullPipelineResultToString(JitFullPipelineResult result) {
	switch (result) {
	case JitFullPipelineResult::NOT_FINISHED:
		return "not_finished";
	case JitFullPipelineResult::FINISHED:
		return "finished";
	case JitFullPipelineResult::INTERRUPTED:
		return "interrupted";
	default:
		return "unknown";
	}
}

static PipelineExecuteResult JitFullPipelineResultToPipelineExecuteResult(JitFullPipelineResult result) {
	switch (result) {
	case JitFullPipelineResult::NOT_FINISHED:
		return PipelineExecuteResult::NOT_FINISHED;
	case JitFullPipelineResult::FINISHED:
		return PipelineExecuteResult::FINISHED;
	case JitFullPipelineResult::INTERRUPTED:
		return PipelineExecuteResult::INTERRUPTED;
	default:
		throw InternalException("Unknown JIT full pipeline result");
	}
}

static JitFullPipelineResult PipelineExecuteResultToJitFullPipelineResult(PipelineExecuteResult result) {
	switch (result) {
	case PipelineExecuteResult::NOT_FINISHED:
		return JitFullPipelineResult::NOT_FINISHED;
	case PipelineExecuteResult::FINISHED:
		return JitFullPipelineResult::FINISHED;
	case PipelineExecuteResult::INTERRUPTED:
		return JitFullPipelineResult::INTERRUPTED;
	default:
		throw InternalException("Unknown pipeline execution result");
	}
}

static string JitRegionExceptionMessage(const std::exception_ptr &error) {
	try {
		std::rethrow_exception(error);
	} catch (std::exception &ex) {
		return ex.what();
	} catch (...) {
		return "unknown non-standard exception";
	}
}

class PipelineJitFullPipelineRuntime : public JitFullPipelineRuntime {
public:
	PipelineJitFullPipelineRuntime(PipelineExecutor &executor_p, idx_t max_chunks_p, bool trace_runtime_p)
	    : executor(executor_p), max_chunks(max_chunks_p), trace_runtime(trace_runtime_p) {
	}

	idx_t MaxChunks() const override {
		return max_chunks;
	}

	bool TraceRuntime() const override {
		return trace_runtime;
	}

	bool HasRequiredPartitionInfo() const override {
		return executor.required_partition_info.AnyRequired();
	}

	bool HasInProcessOperators() const override {
		return !executor.in_process_operators.empty();
	}

	SourceResultType FetchNativeSource(DataChunk *&result, int64_t &source_fetch_time_us) override {
		if (sink_blocked) {
			throw InternalException("JIT full pipeline runtime cannot fetch native source data after a blocked sink");
		}
		if (sink_finished) {
			throw InternalException("JIT full pipeline runtime cannot fetch native source data after a finished sink");
		}
		DataChunk *source_chunk = &executor.jit_source_input_chunk;
		source_chunk->Reset();
		result = source_chunk;
		executor.source_chunk_initial_idx = 0;
		auto trace_start = std::chrono::steady_clock::now();
		JitSuppressionGuard guard(executor.context.client);
		auto source_result = executor.FetchFromNativeSource(result);
		source_fetch_time_us = JitRegionElapsedMicros(trace_start);
		source_output_rows += result ? result->size() : 0;
		source_native_output_rows += result ? result->size() : 0;
		source_invocation_count++;
		source_native_invocation_count++;
		source_runtime_time_us += source_fetch_time_us;
		source_native_runtime_time_us += source_fetch_time_us;
		if (source_result == SourceResultType::FINISHED) {
			executor.exhausted_source = true;
			executor.exhausted_pipeline = true;
		}
		return source_result;
	}

	bool BindNativeOperator(idx_t operator_index, DataChunk &input, const JitRegionOperatorInfo &operator_info,
	                        JitNativeOperatorBinding &binding) override {
		binding = JitNativeOperatorBinding();
		binding.kind = operator_info.kind;
		auto &operators = executor.pipeline.GetIntermediateOperators();
		if (operator_index >= operators.size()) {
			binding.blocker = "jit-native-operator-runtime-index-out-of-range";
			return false;
		}
		if (operator_index >= executor.intermediate_states.size() || !executor.intermediate_states[operator_index]) {
			binding.blocker = "jit-native-operator-runtime-missing-local-state";
			return false;
		}
		auto &op = operators[operator_index].get();
		if (!op.op_state) {
			binding.blocker = "jit-native-operator-runtime-missing-global-state";
			return false;
		}
		return op.BindJitNativeOperator(executor.context, input, *op.op_state,
		                                *executor.intermediate_states[operator_index], operator_info, binding);
	}

	bool BindNativeSink(DataChunk &input, const JitRegionSinkInfo &sink_info,
	                    JitNativeSinkBinding &binding) override {
		binding = JitNativeSinkBinding();
		binding.kind = sink_info.kind;
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		return executor.pipeline.GetSink()->BindJitNativeSink(executor.context, input, sink_input, sink_info,
		                                                      binding);
	}

	void BindNativeUngroupedAggregateStates(const vector<JitNativeUngroupedAggregateState> &requested_states,
	                                        vector<JitNativeUngroupedAggregateState> &bound_states) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		JitBindNativeUngroupedAggregateStates(sink_input, requested_states, bound_states);
	}

	void BindNativeHashAggregateStates(
	    DataChunk &chunk, const vector<JitGroupedAggregateGroupBinding> &group_bindings,
	    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	    JitNativeGroupedAggregateStateSet &bound_states) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		JitBindNativeHashAggregateStates(executor.context, sink_input, chunk, group_bindings, requested_states,
		                                 bound_states);
	}

	SinkResultType FinishNativeHashAggregateUpdate(idx_t input_rows) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		auto sink_result = JitFinishNativeHashAggregateUpdate(executor.context, sink_input, input_rows);
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException("JIT full pipeline native hash aggregate update returned BLOCKED");
		}
		return sink_result;
	}

	void BindNativePerfectHashAggregateStates(
	    DataChunk &chunk, const vector<JitGroupedAggregateGroupBinding> &group_bindings,
	    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	    JitNativeGroupedAggregateStateSet &bound_states) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		JitBindNativePerfectHashAggregateStates(executor.context, sink_input, chunk, group_bindings, requested_states,
		                                       bound_states);
	}

	void BindNativePerfectHashAggregateStateLayout(
	    const vector<JitNativeGroupedAggregateStateRequest> &requested_states,
	    JitNativeGroupedAggregateStateSet &bound_states,
	    JitNativePerfectHashAggregateStateLayout &state_layout) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		JitBindNativePerfectHashAggregateStateLayout(executor.context, sink_input, requested_states, bound_states,
		                                             state_layout);
	}

	SinkResultType FinishNativePerfectHashAggregateUpdate(idx_t input_rows) override {
		ValidateSink();
		OperatorSinkInput sink_input {*executor.pipeline.GetSink()->sink_state, *executor.local_sink_state,
		                              executor.interrupt_state};
		auto sink_result = JitFinishNativePerfectHashAggregateUpdate(executor.context, sink_input, input_rows);
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException("JIT full pipeline native perfect hash aggregate update returned BLOCKED");
		}
		return sink_result;
	}

	SinkResultType RecordNativeSinkResult(DataChunk &chunk, SinkResultType sink_result) override {
		if (sink_result == SinkResultType::BLOCKED) {
			executor.final_chunk.Reset();
			chunk.Copy(executor.final_chunk);
			RecordSinkResult(chunk.size(), sink_result);
			return sink_result;
		}
		return RecordNativeSinkResult(chunk.size(), sink_result);
	}

	SinkResultType RecordNativeSinkResult(idx_t input_rows, SinkResultType sink_result) override {
		ValidateSink();
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException("JIT full pipeline native sink update returned BLOCKED after entering sink state");
		}
		RecordSinkResult(input_rows, sink_result);
		return sink_result;
	}

	void RecordGeneratedBodyPath(JitGeneratedBodyPath path, idx_t input_rows) override {
		switch (path) {
		case JitGeneratedBodyPath::FLAT_ALL_VALID:
			generated_body_flat_input_rows += input_rows;
			generated_body_flat_invocation_count++;
			break;
		case JitGeneratedBodyPath::SHARED_SELECTION_ALL_VALID:
			generated_body_shared_selection_input_rows += input_rows;
			generated_body_shared_selection_invocation_count++;
			break;
		case JitGeneratedBodyPath::SELECTION_ALL_VALID:
			generated_body_selection_input_rows += input_rows;
			generated_body_selection_invocation_count++;
			break;
		case JitGeneratedBodyPath::GENERIC:
			native_operator_loop_input_rows += input_rows;
			native_operator_loop_invocation_count++;
			break;
		default:
			throw InternalException("Unknown JIT generated body path");
		}
	}

	void RecordFusedStageRuntime(JitFusedRuntimeStage stage, int64_t runtime_time_us) override {
		if (!trace_runtime) {
			return;
		}
		switch (stage) {
		case JitFusedRuntimeStage::PREPARE:
			fused_prepare_runtime_time_us += runtime_time_us;
			break;
		case JitFusedRuntimeStage::GROUP:
			fused_group_runtime_time_us += runtime_time_us;
			break;
		case JitFusedRuntimeStage::STATE_BIND:
			fused_state_bind_runtime_time_us += runtime_time_us;
			break;
		case JitFusedRuntimeStage::UPDATE:
			fused_update_runtime_time_us += runtime_time_us;
			break;
		case JitFusedRuntimeStage::FINISH:
			fused_finish_runtime_time_us += runtime_time_us;
			break;
		default:
			throw InternalException("Unknown JIT fused runtime stage");
		}
	}

	idx_t SourceOutputRows() const {
		return source_output_rows;
	}

	idx_t SourceInvocationCount() const {
		return source_invocation_count;
	}

	int64_t SourceRuntimeTime() const {
		return source_runtime_time_us;
	}

	idx_t SinkInputRows() const {
		return sink_input_rows;
	}

	idx_t SinkInvocationCount() const {
		return sink_invocation_count;
	}

	int64_t SinkRuntimeTime() const {
		return sink_runtime_time_us;
	}

	JitRuntimeMetrics Metrics(int64_t runtime_time_us) const {
		JitRuntimeMetrics result;
		result.source_native_output_rows = source_native_output_rows;
		result.source_native_invocation_count = source_native_invocation_count;
		result.source_native_runtime_time_us = source_native_runtime_time_us;
		auto protocol_time_us = source_runtime_time_us + sink_runtime_time_us;
		result.generated_body_runtime_time_us =
		    runtime_time_us > protocol_time_us ? runtime_time_us - protocol_time_us : 0;
		result.fused_prepare_runtime_time_us = fused_prepare_runtime_time_us;
		result.fused_group_runtime_time_us = fused_group_runtime_time_us;
		result.fused_state_bind_runtime_time_us = fused_state_bind_runtime_time_us;
		result.fused_update_runtime_time_us = fused_update_runtime_time_us;
		result.fused_finish_runtime_time_us = fused_finish_runtime_time_us;
		result.generated_body_flat_input_rows = generated_body_flat_input_rows;
		result.generated_body_flat_invocation_count = generated_body_flat_invocation_count;
		result.generated_body_shared_selection_input_rows = generated_body_shared_selection_input_rows;
		result.generated_body_shared_selection_invocation_count = generated_body_shared_selection_invocation_count;
		result.generated_body_selection_input_rows = generated_body_selection_input_rows;
		result.generated_body_selection_invocation_count = generated_body_selection_invocation_count;
		result.native_operator_loop_input_rows = native_operator_loop_input_rows;
		result.native_operator_loop_invocation_count = native_operator_loop_invocation_count;
		return result;
	}

	bool HasRuntimeSideEffects() const {
		return source_invocation_count > 0 || sink_invocation_count > 0;
	}

private:
	void ValidateSink() const {
		if (sink_blocked) {
			throw InternalException("JIT full pipeline runtime cannot sink after a blocked sink");
		}
		if (sink_finished) {
			throw InternalException("JIT full pipeline runtime cannot sink after a finished sink");
		}
		if (!executor.pipeline.GetSink()) {
			throw InternalException("JIT full pipeline runtime cannot sink without a pipeline sink");
		}
		if (!executor.local_sink_state) {
			throw InternalException("JIT full pipeline runtime cannot sink without local sink state");
		}
	}

	void RecordSinkResult(idx_t input_rows, SinkResultType sink_result) {
		sink_input_rows += input_rows;
		sink_invocation_count++;
		if (sink_result == SinkResultType::BLOCKED) {
			sink_blocked = true;
			executor.remaining_sink_chunk = true;
		}
		if (sink_result == SinkResultType::FINISHED) {
			sink_finished = true;
			executor.FinishProcessing();
		}
	}

private:
	PipelineExecutor &executor;
	idx_t max_chunks;
	bool trace_runtime;
	idx_t source_output_rows = 0;
	idx_t source_invocation_count = 0;
	int64_t source_runtime_time_us = 0;
	idx_t source_native_output_rows = 0;
	idx_t source_native_invocation_count = 0;
	int64_t source_native_runtime_time_us = 0;
	idx_t sink_input_rows = 0;
	idx_t sink_invocation_count = 0;
	int64_t sink_runtime_time_us = 0;
	int64_t fused_prepare_runtime_time_us = 0;
	int64_t fused_group_runtime_time_us = 0;
	int64_t fused_state_bind_runtime_time_us = 0;
	int64_t fused_update_runtime_time_us = 0;
	int64_t fused_finish_runtime_time_us = 0;
	idx_t generated_body_flat_input_rows = 0;
	idx_t generated_body_flat_invocation_count = 0;
	idx_t generated_body_shared_selection_input_rows = 0;
	idx_t generated_body_shared_selection_invocation_count = 0;
	idx_t generated_body_selection_input_rows = 0;
	idx_t generated_body_selection_invocation_count = 0;
	idx_t native_operator_loop_input_rows = 0;
	idx_t native_operator_loop_invocation_count = 0;
	bool sink_blocked = false;
	bool sink_finished = false;
};

OperatorResultType JitRegionExecutor::ExecuteReferenceOperatorInterval(PipelineExecutor &executor, DataChunk &input,
                                                                       DataChunk &result, idx_t start_operator_idx,
                                                                       idx_t end_operator_idx) {
	if (input.size() == 0 || start_operator_idx == end_operator_idx) {
		result.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}
	vector<unique_ptr<DataChunk>> chunks;
	vector<unique_ptr<OperatorState>> states;
	stack<idx_t> operators_in_process;
	executor.InitializeOperatorExecutionState(chunks, states, true);
	DataChunk *current = &input;
	auto &operators = executor.pipeline.GetIntermediateOperators();
	for (idx_t operator_idx = start_operator_idx; operator_idx < end_operator_idx; operator_idx++) {
		auto &current_operator = operators[operator_idx].get();
		auto &current_chunk = operator_idx + 1 == end_operator_idx ? result : *chunks[operator_idx + 1];
		current_chunk.Reset();
		executor.StartOperator(current_operator);
		auto execute_result = current_operator.Execute(executor.context, *current, current_chunk,
		                                               *current_operator.op_state, *states[operator_idx]);
		executor.EndOperator(current_operator, &current_chunk);
		if (execute_result == OperatorResultType::FINISHED) {
			return execute_result;
		}
		if (execute_result == OperatorResultType::HAVE_MORE_OUTPUT) {
			return execute_result;
		}
		if (current_chunk.size() == 0) {
			result.Reset();
			return OperatorResultType::NEED_MORE_INPUT;
		}
		current = &current_chunk;
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

bool JitRegionExecutor::TryExecuteFullPipeline(PipelineExecutor &executor, idx_t max_chunks,
                                               PipelineExecuteResult &result) {
	if (executor.context.client.IsJitSuppressed()) {
		return false;
	}
	if (!executor.pipeline.GetSource() || !executor.pipeline.GetSink()) {
		return false;
	}
	optional_ptr<JitRegionKernel> kernel;
	for (auto &candidate_kernel : executor.jit_kernels) {
		D_ASSERT(candidate_kernel);
		if (!candidate_kernel->HasTraceCandidate()) {
			continue;
		}
		auto &contract = candidate_kernel->TraceCandidateContract();
		if (!JitRegionABIIsFullPipeline(contract.abi) || !candidate_kernel->CanExecuteFullPipeline()) {
			continue;
		}
		kernel = *candidate_kernel;
		break;
	}
	if (!kernel) {
		return false;
	}

	auto trace_runtime = Settings::Get<JitTraceRuntimeSetting>(executor.context.client);
	std::chrono::steady_clock::time_point trace_start;
	bool trace_started = false;
	if (!executor.in_process_operators.empty() || executor.remaining_sink_chunk || executor.next_batch_blocked ||
	    executor.started_flushing || executor.done_flushing || executor.exhausted_source || executor.exhausted_pipeline ||
	    executor.finalized) {
		if (trace_runtime) {
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "skipped",
			                        "full pipeline kernel not entered because executor state is not at a clean "
			                        "source-to-sink boundary",
			                        0, 0, 0, "fallback");
		}
		return false;
	}

	try {
		if (trace_runtime) {
			trace_start = std::chrono::steady_clock::now();
			trace_started = true;
		}
		PipelineJitFullPipelineRuntime runtime(executor, max_chunks, trace_runtime);
		string entry_blocker;
		if (!kernel->CanEnterFullPipeline(runtime, entry_blocker)) {
			if (entry_blocker.empty()) {
				entry_blocker = "full-pipeline-entry-precondition-failed";
			}
			if (trace_runtime) {
				auto elapsed_us = JitRegionElapsedMicros(trace_start);
				JitManager::Get(executor.context.client)
				    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "skipped",
				                        "full pipeline kernel not entered: " + entry_blocker, 0, 0, elapsed_us,
				                        "fallback");
			}
			return false;
		}
		JitFullPipelineResult jit_result = JitFullPipelineResult::NOT_FINISHED;
		auto jit_executed = kernel->TryExecuteFullPipeline(runtime, jit_result);
		if (!jit_executed) {
			throw InternalException("JIT full pipeline kernel returned false at runtime");
		}
		auto pipeline_result = JitFullPipelineResultToPipelineExecuteResult(jit_result);
		string runtime_reason = "full pipeline kernel executed";
		if (jit_result == JitFullPipelineResult::FINISHED) {
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
			jit_result = PipelineExecuteResultToJitFullPipelineResult(pipeline_result);
		}
		result = pipeline_result;
		auto elapsed_us = trace_runtime ? JitRegionElapsedMicros(trace_start) : 0;
		if (trace_runtime) {
			auto runtime_metrics = runtime.Metrics(elapsed_us);
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "executed",
			                        std::move(runtime_reason), runtime.SourceOutputRows(),
			                        runtime.SinkInputRows(), elapsed_us, JitFullPipelineResultToString(jit_result),
			                        runtime_metrics);
		}
		return true;
	} catch (...) {
		auto jit_error = std::current_exception();
		if (trace_runtime) {
			auto elapsed_us = trace_started ? JitRegionElapsedMicros(trace_start) : 0;
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "error",
			                        "full pipeline kernel threw: " + JitRegionExceptionMessage(jit_error), 0, 0,
			                        elapsed_us, "error");
		}
		throw;
	}
}

void JitRegionExecutor::VerifyRegionResult(const DataChunk &actual, OperatorResultType actual_result,
                                           const DataChunk &expected, OperatorResultType expected_result) {
	if (actual_result != expected_result) {
		throw InternalException("JIT region verification failed: expected operator result %d, got %d",
		                        static_cast<int>(expected_result), static_cast<int>(actual_result));
	}
	if (actual.ColumnCount() != expected.ColumnCount()) {
		throw InternalException("JIT region verification failed: expected %llu columns, got %llu",
		                        static_cast<unsigned long long>(expected.ColumnCount()),
		                        static_cast<unsigned long long>(actual.ColumnCount()));
	}
	if (actual.size() != expected.size()) {
		throw InternalException("JIT region verification failed: expected %llu rows, got %llu",
		                        static_cast<unsigned long long>(expected.size()),
		                        static_cast<unsigned long long>(actual.size()));
	}
	for (idx_t row_idx = 0; row_idx < actual.size(); row_idx++) {
		for (idx_t col_idx = 0; col_idx < actual.ColumnCount(); col_idx++) {
			auto actual_value = actual.GetValue(col_idx, row_idx);
			auto expected_value = expected.GetValue(col_idx, row_idx);
			if (!ValueOperations::DistinctFrom(actual_value, expected_value)) {
				continue;
			}
			throw InternalException(
			    "JIT region verification failed at row %llu, column %llu: expected %s, got %s",
			    static_cast<unsigned long long>(row_idx), static_cast<unsigned long long>(col_idx),
			    expected_value.ToString(), actual_value.ToString());
		}
	}
}

} // namespace duckdb

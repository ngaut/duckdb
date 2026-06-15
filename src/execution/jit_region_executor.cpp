#include "duckdb/execution/jit/region_executor.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/stack.hpp"
#include "duckdb/common/string_util.hpp"
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

static JitRuntimeMetrics GeneratedBodyRuntimeMetrics(int64_t runtime_time_us) {
	JitRuntimeMetrics metrics;
	metrics.generated_body_runtime_time_us = runtime_time_us;
	return metrics;
}

static string JitOperatorResultTypeToString(OperatorResultType result) {
	switch (result) {
	case OperatorResultType::NEED_MORE_INPUT:
		return "need_more_input";
	case OperatorResultType::HAVE_MORE_OUTPUT:
		return "have_more_output";
	case OperatorResultType::FINISHED:
		return "finished";
	case OperatorResultType::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

static string JitSourceResultTypeToString(SourceResultType result) {
	switch (result) {
	case SourceResultType::HAVE_MORE_OUTPUT:
		return "have_more_output";
	case SourceResultType::FINISHED:
		return "finished";
	case SourceResultType::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

static string JitSinkResultTypeToString(SinkResultType result) {
	switch (result) {
	case SinkResultType::NEED_MORE_INPUT:
		return "need_more_input";
	case SinkResultType::FINISHED:
		return "finished";
	case SinkResultType::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
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

static Value JitNullBooleanValue() {
	return Value(LogicalType::BOOLEAN);
}

static bool JitValueIsTrue(const Value &value) {
	return !value.IsNull() && value.GetValue<bool>();
}

static idx_t JitVerifierUTF8CharacterLength(const string &value, idx_t byte_idx) {
	auto byte = static_cast<uint8_t>(value[byte_idx]);
	if (byte < 0x80) {
		return 1;
	}
	if ((byte & 0xE0) == 0xC0) {
		return MinValue<idx_t>(2, value.size() - byte_idx);
	}
	if ((byte & 0xF0) == 0xE0) {
		return MinValue<idx_t>(3, value.size() - byte_idx);
	}
	if ((byte & 0xF8) == 0xF0) {
		return MinValue<idx_t>(4, value.size() - byte_idx);
	}
	return 1;
}

static int64_t JitVerifierInt64Value(const Value &value) {
	if (value.IsNull()) {
		throw InternalException("JIT source-prefix verifier cannot read NULL as an integer");
	}
	Value cast_value;
	string error;
	if (!value.DefaultTryCastAs(LogicalType::BIGINT, cast_value, &error) || cast_value.IsNull()) {
		throw InternalException("JIT source-prefix verifier cannot cast an intrinsic argument to BIGINT");
	}
	return cast_value.GetValue<int64_t>();
}

static string JitVerifierSubstringFromOne(const string &input, int64_t length) {
	if (length < 0) {
		throw InternalException("JIT source-prefix verifier only supports non-negative substring lengths");
	}
	idx_t byte_end = 0;
	for (int64_t char_idx = 0; char_idx < length && byte_end < input.size(); char_idx++) {
		byte_end += JitVerifierUTF8CharacterLength(input, byte_end);
	}
	return input.substr(0, byte_end);
}

static Value EvaluateJitSourceExpression(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                         idx_t source_column_idx);

static Value EvaluateJitSourceIntrinsic(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                        idx_t source_column_idx) {
	switch (node.intrinsic) {
	case JitExpressionIntrinsicKind::STRING_PREFIX: {
		if (node.children.size() != 2) {
			throw InternalException("JIT source-prefix verifier encountered a malformed string_prefix intrinsic");
		}
		auto source = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
		auto pattern = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
		if (source.IsNull() || pattern.IsNull()) {
			return JitNullBooleanValue();
		}
		return Value::BOOLEAN(StringUtil::StartsWith(StringValue::Get(source), StringValue::Get(pattern)));
	}
	case JitExpressionIntrinsicKind::STRING_SUFFIX: {
		if (node.children.size() != 2) {
			throw InternalException("JIT source-prefix verifier encountered a malformed string_suffix intrinsic");
		}
		auto source = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
		auto pattern = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
		if (source.IsNull() || pattern.IsNull()) {
			return JitNullBooleanValue();
		}
		return Value::BOOLEAN(StringUtil::EndsWith(StringValue::Get(source), StringValue::Get(pattern)));
	}
	case JitExpressionIntrinsicKind::STRING_CONTAINS: {
		if (node.children.size() != 2) {
			throw InternalException("JIT source-prefix verifier encountered a malformed string_contains intrinsic");
		}
		auto source = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
		auto pattern = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
		if (source.IsNull() || pattern.IsNull()) {
			return JitNullBooleanValue();
		}
		return Value::BOOLEAN(StringUtil::Contains(StringValue::Get(source), StringValue::Get(pattern)));
	}
	case JitExpressionIntrinsicKind::STRING_LIKE: {
		if (node.children.size() != 2) {
			throw InternalException("JIT source-prefix verifier encountered a malformed string_like intrinsic");
		}
		auto source = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
		auto pattern = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
		if (source.IsNull() || pattern.IsNull()) {
			return JitNullBooleanValue();
		}
		auto input_string = StringValue::Get(source);
		auto pattern_string = StringValue::Get(pattern);
		idx_t position = 0;
		idx_t fragment_start = 0;
		const bool anchor_start = pattern_string.empty() || pattern_string[0] != '%';
		const bool anchor_end = pattern_string.empty() || pattern_string[pattern_string.size() - 1] != '%';
		vector<string> fragments;
		for (idx_t pattern_idx = 0; pattern_idx <= pattern_string.size(); pattern_idx++) {
			if (pattern_idx < pattern_string.size() && pattern_string[pattern_idx] != '%') {
				continue;
			}
			if (pattern_idx > fragment_start) {
				fragments.push_back(pattern_string.substr(fragment_start, pattern_idx - fragment_start));
			}
			fragment_start = pattern_idx + 1;
		}
		if (fragments.empty()) {
			return Value::BOOLEAN(!(anchor_start && anchor_end) || input_string.empty());
		}
		idx_t fragment_idx = 0;
		if (anchor_start) {
			if (!StringUtil::StartsWith(input_string, fragments[0])) {
				return Value::BOOLEAN(false);
			}
			position = fragments[0].size();
			fragment_idx = 1;
		}
		for (; fragment_idx < fragments.size(); fragment_idx++) {
			auto &fragment = fragments[fragment_idx];
			const bool is_last = fragment_idx + 1 == fragments.size();
			if (is_last && anchor_end) {
				if (!StringUtil::EndsWith(input_string, fragment)) {
					return Value::BOOLEAN(false);
				}
				return Value::BOOLEAN(input_string.size() - fragment.size() >= position);
			}
			auto match = input_string.find(fragment, position);
			if (match == string::npos) {
				return Value::BOOLEAN(false);
			}
			position = match + fragment.size();
		}
		return Value::BOOLEAN(true);
	}
	case JitExpressionIntrinsicKind::STRING_SUBSTRING: {
		if (node.children.size() != 2 && node.children.size() != 3) {
			throw InternalException("JIT source-prefix verifier encountered a malformed string_substring intrinsic");
		}
		auto source = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
		auto offset = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
		if (source.IsNull() || offset.IsNull()) {
			return Value(LogicalType::VARCHAR);
		}
		auto start = JitVerifierInt64Value(offset);
		if (start != 1) {
			throw InternalException("JIT source-prefix verifier only supports substring start 1");
		}
		if (node.children.size() == 2) {
			return Value(StringValue::Get(source));
		}
		auto length = EvaluateJitSourceExpression(*node.children[2], input, row_idx, source_column_idx);
		if (length.IsNull()) {
			return Value(LogicalType::VARCHAR);
		}
		return Value(JitVerifierSubstringFromOne(StringValue::Get(source), JitVerifierInt64Value(length)));
	}
	default:
		throw InternalException("JIT source-prefix verifier encountered an unsupported intrinsic expression");
	}
}

static Value EvaluateJitSourceComparison(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                         idx_t source_column_idx) {
	auto left = EvaluateJitSourceExpression(*node.left, input, row_idx, source_column_idx);
	auto right = EvaluateJitSourceExpression(*node.right, input, row_idx, source_column_idx);
	if (left.IsNull() || right.IsNull()) {
		return JitNullBooleanValue();
	}
	switch (node.binary_op) {
	case JitExpressionBinaryOp::COMPARE_EQUAL:
		return Value::BOOLEAN(ValueOperations::Equals(left, right));
	case JitExpressionBinaryOp::COMPARE_NOTEQUAL:
		return Value::BOOLEAN(ValueOperations::NotEquals(left, right));
	case JitExpressionBinaryOp::COMPARE_LESSTHAN:
		return Value::BOOLEAN(ValueOperations::LessThan(left, right));
	case JitExpressionBinaryOp::COMPARE_GREATERTHAN:
		return Value::BOOLEAN(ValueOperations::GreaterThan(left, right));
	case JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return Value::BOOLEAN(ValueOperations::LessThanEquals(left, right));
	case JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return Value::BOOLEAN(ValueOperations::GreaterThanEquals(left, right));
	case JitExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		return Value::BOOLEAN(ValueOperations::DistinctFrom(left, right));
	case JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return Value::BOOLEAN(ValueOperations::NotDistinctFrom(left, right));
	default:
		throw InternalException("JIT source-prefix verifier encountered a non-comparison binary expression");
	}
}

static Value EvaluateJitSourceConjunction(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                          idx_t source_column_idx) {
	bool saw_null = false;
	if (node.conjunction_op == JitExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			auto value = EvaluateJitSourceExpression(*child, input, row_idx, source_column_idx);
			if (value.IsNull()) {
				saw_null = true;
				continue;
			}
			if (!value.GetValue<bool>()) {
				return Value::BOOLEAN(false);
			}
		}
		return saw_null ? JitNullBooleanValue() : Value::BOOLEAN(true);
	}
	for (auto &child : node.children) {
		auto value = EvaluateJitSourceExpression(*child, input, row_idx, source_column_idx);
		if (value.IsNull()) {
			saw_null = true;
			continue;
		}
		if (value.GetValue<bool>()) {
			return Value::BOOLEAN(true);
		}
	}
	return saw_null ? JitNullBooleanValue() : Value::BOOLEAN(false);
}

static Value EvaluateJitSourceInList(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                     idx_t source_column_idx) {
	if (node.children.empty()) {
		throw InternalException("JIT source-prefix verifier encountered an empty IN expression");
	}
	auto needle = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
	if (needle.IsNull()) {
		return JitNullBooleanValue();
	}
	bool saw_null = false;
	for (idx_t child_idx = 1; child_idx < node.children.size(); child_idx++) {
		auto candidate = EvaluateJitSourceExpression(*node.children[child_idx], input, row_idx, source_column_idx);
		if (candidate.IsNull()) {
			saw_null = true;
			continue;
		}
		if (ValueOperations::Equals(needle, candidate)) {
			return Value::BOOLEAN(!node.not_in);
		}
	}
	if (saw_null) {
		return JitNullBooleanValue();
	}
	return Value::BOOLEAN(node.not_in);
}

static Value EvaluateJitSourceBetween(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                      idx_t source_column_idx) {
	if (node.children.size() != 3) {
		throw InternalException("JIT source-prefix verifier encountered a malformed BETWEEN expression");
	}
	auto value = EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
	auto lower = EvaluateJitSourceExpression(*node.children[1], input, row_idx, source_column_idx);
	auto upper = EvaluateJitSourceExpression(*node.children[2], input, row_idx, source_column_idx);
	if (value.IsNull() || lower.IsNull() || upper.IsNull()) {
		return JitNullBooleanValue();
	}
	auto lower_ok = node.lower_inclusive ? ValueOperations::GreaterThanEquals(value, lower)
	                                     : ValueOperations::GreaterThan(value, lower);
	auto upper_ok = node.upper_inclusive ? ValueOperations::LessThanEquals(value, upper)
	                                     : ValueOperations::LessThan(value, upper);
	auto result = lower_ok && upper_ok;
	return Value::BOOLEAN(node.not_between ? !result : result);
}

static Value EvaluateJitSourceConstantOrNull(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                             idx_t source_column_idx) {
	if (node.children.empty()) {
		throw InternalException("JIT source-prefix verifier encountered an empty constant_or_null expression");
	}
	for (idx_t child_idx = 1; child_idx < node.children.size(); child_idx++) {
		auto guard = EvaluateJitSourceExpression(*node.children[child_idx], input, row_idx, source_column_idx);
		if (guard.IsNull()) {
			return Value(node.return_type);
		}
	}
	return EvaluateJitSourceExpression(*node.children[0], input, row_idx, source_column_idx);
}

static Value EvaluateJitSourceExpression(const JitExpressionIR &node, DataChunk &input, idx_t row_idx,
                                         idx_t source_column_idx) {
	switch (node.kind) {
	case JitExpressionIRKind::CONSTANT:
		return node.constant;
	case JitExpressionIRKind::REFERENCE:
		if (node.ref_index != 0) {
			throw InternalException("JIT source-prefix verifier only supports table-filter-local references");
		}
		if (source_column_idx >= input.ColumnCount()) {
			throw InternalException("JIT source-prefix verifier source column is outside the raw input chunk");
		}
		return input.data[source_column_idx].GetValue(row_idx);
	case JitExpressionIRKind::UNARY: {
		auto child = EvaluateJitSourceExpression(*node.left, input, row_idx, source_column_idx);
		switch (node.unary_op) {
		case JitExpressionUnaryOp::IS_NULL:
			return Value::BOOLEAN(child.IsNull());
		case JitExpressionUnaryOp::IS_NOT_NULL:
			return Value::BOOLEAN(!child.IsNull());
		case JitExpressionUnaryOp::NOT:
			return child.IsNull() ? JitNullBooleanValue() : Value::BOOLEAN(!child.GetValue<bool>());
		default:
			throw InternalException("JIT source-prefix verifier encountered an unsupported unary expression");
		}
	}
	case JitExpressionIRKind::BINARY:
		return EvaluateJitSourceComparison(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::CONJUNCTION:
		return EvaluateJitSourceConjunction(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::IN_LIST:
		return EvaluateJitSourceInList(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::BETWEEN:
		return EvaluateJitSourceBetween(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::CONSTANT_OR_NULL:
		return EvaluateJitSourceConstantOrNull(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::INTRINSIC:
		return EvaluateJitSourceIntrinsic(node, input, row_idx, source_column_idx);
	case JitExpressionIRKind::COALESCE:
		for (auto &child : node.children) {
			auto value = EvaluateJitSourceExpression(*child, input, row_idx, source_column_idx);
			if (!value.IsNull()) {
				return value;
			}
		}
		return Value(node.return_type);
	default:
		throw InternalException("JIT source-prefix verifier encountered an unsupported expression kind");
	}
}

static const JitRegionIRNode *GetJitSourcePrefixNode(optional_ptr<const JitPreparedPipeline> prepared,
                                                     const JitRegionKernel &kernel) {
	if (!prepared || !prepared->region_ir || !kernel.HasTraceCandidate() || !prepared->RequiresPreparedSourceInput()) {
		return nullptr;
	}
	auto candidate_id = kernel.TraceCandidateId();
	if (candidate_id >= prepared->region_ir->candidates.size()) {
		return nullptr;
	}
	auto &candidate = prepared->region_ir->candidates[candidate_id];
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = prepared->region_ir->nodes[node_idx];
		if (node.kind == JitRegionIRNodeKind::SOURCE && node.source && !node.source->filters.empty() &&
		    node.source->table_scan_protocol.source_prefix_filter_takeover_supported) {
			return &node;
		}
	}
	return nullptr;
}

static const JitRegionIRNode *GetJitPreparedSourceReferenceNode(optional_ptr<const JitPreparedPipeline> prepared) {
	if (!prepared || !prepared->region_ir || !prepared->RequiresPreparedSourceInput()) {
		return nullptr;
	}
	for (auto &prepared_region : prepared->selected_regions) {
		if (!prepared_region.lowering_plan.OwnsSourceFilters()) {
			continue;
		}
		if (prepared_region.candidate_index >= prepared->region_ir->candidates.size()) {
			throw InternalException("JIT prepared source candidate index is outside the region IR");
		}
		auto &candidate = prepared->region_ir->candidates[prepared_region.candidate_index];
		if (!JitRegionABIOwnsSource(candidate.contract.abi)) {
			continue;
		}
		for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
			auto &node = prepared->region_ir->nodes[node_idx];
			if (node.kind == JitRegionIRNodeKind::SOURCE && node.source && !node.source->filters.empty() &&
			    node.source->table_scan_protocol.source_prefix_filter_takeover_supported) {
				return &node;
			}
		}
	}
	return nullptr;
}

static bool BuildJitSourcePrefixReference(ClientContext &context, const JitRegionIRNode &source_node,
                                          DataChunk &raw_input, DataChunk &source_output) {
	auto &source = *source_node.source;
	auto &protocol = source.table_scan_protocol;
	DataChunk *current = &raw_input;
	vector<unique_ptr<DataChunk>> filter_chunks;
	vector<unique_ptr<SelectionVector>> filter_selections;
	filter_selections.reserve(source.filters.size());
	for (auto &filter : source.filters) {
		if (!filter.expression || !filter.expression->root) {
			throw InternalException("JIT source-prefix verifier requires lowered source filter IR");
		}
		auto filter_sel = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		auto filtered = make_uniq<DataChunk>();
		filtered->Initialize(BufferAllocator::Get(context), current->GetTypes());
		idx_t selected_count = 0;
		for (idx_t row_idx = 0; row_idx < current->size(); row_idx++) {
			if (JitValueIsTrue(EvaluateJitSourceExpression(*filter.expression->root, *current, row_idx,
			                                               filter.scan_column_index))) {
				filter_sel->set_index(selected_count++, NumericCast<sel_t>(row_idx));
			}
		}
		if (selected_count == current->size()) {
			filtered->Reference(*current);
		} else if (selected_count > 0) {
			filtered->Slice(*current, *filter_sel, selected_count);
		} else {
			filtered->SetChildCardinality(0);
		}
		filter_selections.push_back(std::move(filter_sel));
		current = filtered.get();
		filter_chunks.push_back(std::move(filtered));
	}

	source_output.Reset();
	auto projection_map = protocol.source_prefix_output_projection_map;
	if (projection_map.empty()) {
		projection_map.reserve(source_node.output_types.size());
		for (idx_t output_idx = 0; output_idx < source_node.output_types.size(); output_idx++) {
			projection_map.push_back(output_idx);
		}
	}
	if (projection_map.size() != source_output.ColumnCount()) {
		throw InternalException("JIT source-prefix verifier projection map does not match source output chunk");
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= current->ColumnCount()) {
			throw InternalException("JIT source-prefix verifier projection references an invalid raw source column");
		}
		source_output.data[output_idx].Reference(current->data[input_idx]);
	}
	source_output.SetChildCardinality(current->size());
	return true;
}

bool JitRegionExecutor::TryExecutePreparedSourceReference(PipelineExecutor &executor, DataChunk &source_chunk,
                                                          DataChunk *&result, SourceResultType source_result,
                                                          int64_t source_fetch_time_us,
                                                          idx_t &next_operator_idx) {
	(void)source_fetch_time_us;
	auto prepared = executor.pipeline.GetJitPreparedPipeline();
	if (!prepared || !prepared->RequiresPreparedSourceInput()) {
		return false;
	}
	next_operator_idx = 0;
	if (source_result == SourceResultType::BLOCKED || source_chunk.size() == 0) {
		result->Reset();
		return true;
	}
	auto source_node = GetJitPreparedSourceReferenceNode(prepared);
	if (!source_node) {
		throw InternalException("prepared JIT source input requires a lowered source-prefix reference node");
	}
	BuildJitSourcePrefixReference(executor.context.client, *source_node, source_chunk, *result);
	return true;
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
	if (!executor.in_process_operators.empty() || executor.required_partition_info.AnyRequired() ||
	    executor.remaining_sink_chunk || executor.next_batch_blocked || executor.started_flushing ||
	    executor.done_flushing || executor.exhausted_source || executor.exhausted_pipeline || executor.finalized) {
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

bool JitRegionExecutor::HasSourcePrefixKernel(PipelineExecutor &executor) {
	if (executor.context.client.IsJitSuppressed()) {
		return false;
	}
	for (auto &candidate_kernel : executor.jit_kernels) {
		D_ASSERT(candidate_kernel);
		if (!candidate_kernel->HasTraceCandidate()) {
			continue;
		}
		auto &contract = candidate_kernel->TraceCandidateContract();
		if (!JitRegionABIIsSourcePrefix(contract.abi) || !candidate_kernel->CanExecuteSourcePrefix() ||
		    !candidate_kernel->RequiresNativeSource()) {
			continue;
		}
		return executor.in_process_operators.empty();
	}
	return false;
}

bool JitRegionExecutor::TryExecuteSourcePrefix(PipelineExecutor &executor, DataChunk &source_chunk,
                                               DataChunk *&result, SourceResultType source_result,
                                               int64_t source_fetch_time_us, idx_t &next_operator_idx) {
	if (executor.context.client.IsJitSuppressed()) {
		return false;
	}
	if (!executor.in_process_operators.empty()) {
		return false;
	}
	optional_ptr<JitRegionKernel> kernel;
	for (auto &candidate_kernel : executor.jit_kernels) {
		D_ASSERT(candidate_kernel);
		if (!candidate_kernel->HasTraceCandidate()) {
			continue;
		}
		auto &contract = candidate_kernel->TraceCandidateContract();
		if (!JitRegionABIIsSourcePrefix(contract.abi) || !candidate_kernel->CanExecuteSourcePrefix() ||
		    !candidate_kernel->RequiresNativeSource()) {
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
	try {
		if (trace_runtime) {
			JitRuntimeMetrics metrics;
			metrics.source_native_output_rows = source_chunk.size();
			metrics.source_native_invocation_count = 1;
			metrics.source_native_runtime_time_us = source_fetch_time_us;
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "source_native",
			                        "source-prefix native source fetched before generated prefix execution",
			                        0, source_chunk.size(), source_fetch_time_us,
			                        JitSourceResultTypeToString(source_result), metrics);
		}
		if (source_result == SourceResultType::BLOCKED) {
			result->Reset();
			next_operator_idx = 0;
			return true;
		}
		if (source_chunk.size() == 0) {
			result->Reset();
			next_operator_idx = 0;
			return true;
		}
		if (trace_runtime) {
			trace_start = std::chrono::steady_clock::now();
			trace_started = true;
		}
		const bool verify_jit = Settings::Get<JitVerifySetting>(executor.context.client);
		DataChunk reference_input;
		if (verify_jit) {
			reference_input.Initialize(BufferAllocator::Get(executor.context.client), source_chunk.GetTypes());
			source_chunk.Copy(reference_input);
		}
		auto operator_count = executor.pipeline.GetIntermediateOperators().size();
		auto candidate_end = kernel->TraceCandidateEndOperatorIndex();
		if (candidate_end > operator_count) {
			throw InternalException("JIT source-prefix candidate end %llu exceeds operator count %llu",
			                        static_cast<unsigned long long>(candidate_end),
			                        static_cast<unsigned long long>(operator_count));
		}
		DataChunk &prefix_result = executor.GetSourceChunkForInitialIdx(candidate_end);
		OperatorResultType operator_result;
		auto jit_executed = kernel->TryExecute(source_chunk, prefix_result, 0, operator_result);
		auto elapsed_us = trace_runtime ? JitRegionElapsedMicros(trace_start) : 0;
		if (!jit_executed) {
			throw InternalException("JIT native source-prefix kernel returned false after native source fetch");
		}
		if (operator_result != OperatorResultType::NEED_MORE_INPUT) {
			throw InternalException("JIT source-prefix kernel returned unsupported operator result %s",
			                        JitOperatorResultTypeToString(operator_result));
		}
		result = &prefix_result;
		next_operator_idx = candidate_end;
		if (verify_jit) {
			DataChunk reference_result;
			reference_result.Initialize(BufferAllocator::Get(executor.context.client), result->GetTypes());
			JitSuppressionGuard reference_guard(executor.context.client);
			OperatorResultType expected_result;
			auto source_node = GetJitSourcePrefixNode(executor.pipeline.GetJitPreparedPipeline(), *kernel);
			if (source_node) {
				DataChunk source_reference;
				source_reference.Initialize(BufferAllocator::Get(executor.context.client), source_node->output_types);
				BuildJitSourcePrefixReference(executor.context.client, *source_node, reference_input, source_reference);
				if (candidate_end == 0) {
					source_reference.Copy(reference_result);
					expected_result = OperatorResultType::NEED_MORE_INPUT;
				} else {
					expected_result =
					    ExecuteReferenceOperatorInterval(executor, source_reference, reference_result, 0, candidate_end);
				}
			} else {
				expected_result =
				    ExecuteReferenceOperatorInterval(executor, reference_input, reference_result, 0, candidate_end);
			}
			VerifyRegionResult(*result, operator_result, reference_result, expected_result);
		}
		if (trace_runtime) {
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "executed",
			                        "source-prefix kernel executed;next_operator_idx=" +
			                            std::to_string(next_operator_idx),
			                        source_chunk.size(), result->size(), elapsed_us,
			                        JitSourceResultTypeToString(source_result),
			                        GeneratedBodyRuntimeMetrics(elapsed_us));
		}
		return true;
	} catch (...) {
		auto jit_error = std::current_exception();
		if (trace_runtime) {
			auto elapsed_us = trace_started ? JitRegionElapsedMicros(trace_start) : 0;
			JitManager::Get(executor.context.client)
			    .RecordRuntimeEvent(executor.context.client, *kernel, JitCompileTarget::REGION, "error",
			                        "source-prefix kernel threw: " + JitRegionExceptionMessage(jit_error), 0, 0,
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

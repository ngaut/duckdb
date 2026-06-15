//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_runtime.hpp"

#include "sljit_native_runtime.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/jit/aggregate_runtime.hpp"
#include "duckdb/execution/jit/operator_runtime.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <exception>

namespace duckdb {

static int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static bool SljitSourcesFlatAllValid(const vector<const_data_ptr_t> &source_data, const vector<const sel_t *> &source_sel,
                                     const vector<const validity_t *> &source_validity) {
	for (idx_t source_idx = 0; source_idx < source_data.size(); source_idx++) {
		if (source_sel[source_idx] || source_validity[source_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitSourcesAllValid(const vector<const validity_t *> &source_validity) {
	for (auto validity : source_validity) {
		if (validity) {
			return false;
		}
	}
	return true;
}

static const sel_t *SljitNormalizedSourceSelectionData(const UnifiedVectorFormat &format) {
	if (!format.sel || format.sel == FlatVector::IncrementalSelectionVector()) {
		return nullptr;
	}
	return format.sel->data();
}

static const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format,
                                                    SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeHashJoinKeyKind::INT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int16_t>(format));
	case SljitNativeHashJoinKeyKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeHashJoinKeyKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeHashJoinKeyKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeHashJoinKeyKind::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeHashJoinKeyKind::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	case SljitNativeHashJoinKeyKind::UINT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint64_t>(format));
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static bool SljitSourcesAllSelectedAllValid(const vector<const_data_ptr_t> &source_data,
                                            const vector<const sel_t *> &source_sel,
                                            const vector<const validity_t *> &source_validity) {
	if (source_data.empty()) {
		return false;
	}
	for (idx_t source_idx = 0; source_idx < source_data.size(); source_idx++) {
		if (!source_sel[source_idx] || source_validity[source_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitSourcesSharedSelectionAllValid(const vector<const_data_ptr_t> &source_data,
                                                const vector<const sel_t *> &source_sel,
                                                const vector<const validity_t *> &source_validity) {
	if (source_data.empty() || !source_sel[0]) {
		return false;
	}
	for (idx_t source_idx = 0; source_idx < source_data.size(); source_idx++) {
		if (source_sel[source_idx] != source_sel[0] || source_validity[source_idx]) {
			return false;
		}
	}
	return true;
}

static bool RecordSljitGeneratedBodyPath(JitFullPipelineRuntime &runtime, const vector<const_data_ptr_t> &source_data,
                                         const vector<const sel_t *> &source_sel,
                                         const vector<const validity_t *> &source_validity, idx_t input_size) {
	auto flat_all_valid = SljitSourcesFlatAllValid(source_data, source_sel, source_validity);
	if (flat_all_valid) {
		runtime.RecordGeneratedBodyPath(JitGeneratedBodyPath::FLAT_ALL_VALID, input_size);
		return true;
	}
	if (SljitSourcesSharedSelectionAllValid(source_data, source_sel, source_validity)) {
		runtime.RecordGeneratedBodyPath(JitGeneratedBodyPath::SHARED_SELECTION_ALL_VALID, input_size);
	} else if (SljitSourcesAllValid(source_validity)) {
		runtime.RecordGeneratedBodyPath(JitGeneratedBodyPath::SELECTION_ALL_VALID, input_size);
	} else {
		runtime.RecordGeneratedBodyPath(JitGeneratedBodyPath::GENERIC, input_size);
	}
	return false;
}

class SljitFusedUngroupedSumKernel : public JitRegionKernel {
private:
	struct SljitFusedUngroupedSumExecutionState {
		vector<JitNativeUngroupedAggregateState> bound_states;
		vector<UnifiedVectorFormat> formats;
		vector<const_data_ptr_t> source_data;
		vector<const sel_t *> source_sel;
		vector<const validity_t *> source_validity;
	};

public:
	SljitFusedUngroupedSumKernel(string backend_name_p, SljitNativeRegionExpressionPlan filter_p,
	                             SljitNativeRegionExpressionPlan projection_p,
	                             SljitNativeUngroupedAggregateUpdatePlan update_p, unique_ptr<JitCodeHandle> code_p,
	                             SljitFusedUngroupedAggregateFunction function_p,
	                             string projection_overflow_message_p, bool native_source_p)
	    : backend_name(std::move(backend_name_p)), filter(std::move(filter_p)), projection(std::move(projection_p)),
	      update(std::move(update_p)), code(std::move(code_p)), function(function_p),
	      projection_overflow_message(std::move(projection_overflow_message_p)), native_source(native_source_p) {
	}

	SljitFusedUngroupedSumKernel(string backend_name_p, SljitNativeRegionExpressionPlan projection_p,
	                             SljitNativeUngroupedAggregateUpdatePlan update_p, unique_ptr<JitCodeHandle> code_p,
	                             SljitFusedUngroupedAggregateFunction function_p,
	                             string projection_overflow_message_p, bool native_source_p)
	    : backend_name(std::move(backend_name_p)), projection(std::move(projection_p)), update(std::move(update_p)),
	      code(std::move(code_p)), function(function_p),
	      projection_overflow_message(std::move(projection_overflow_message_p)), native_source(native_source_p) {
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return code->CodeSize();
	}

	bool HasExecutableBody() const override {
		return true;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return native_source;
	}

	bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result) override {
		if (!HasTraceCandidate()) {
			return false;
		}
		SljitFusedUngroupedSumExecutionState state;
		BindExecutionState(runtime, state);
		idx_t processed_chunks = 0;
		while (true) {
			if (processed_chunks >= runtime.MaxChunks()) {
				result = JitFullPipelineResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			int64_t source_fetch_time_us = 0;
			auto source_result = native_source ? runtime.FetchNativeSource(source_chunk, source_fetch_time_us)
			                                  : runtime.FetchSource(source_chunk, source_fetch_time_us);
			if (source_result == SourceResultType::BLOCKED) {
				result = JitFullPipelineResult::INTERRUPTED;
				return true;
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto sink_rows = ExecuteFused(runtime, state, *source_chunk);
				if (sink_rows > 0) {
					runtime.RecordNativeSinkResult(sink_rows, SinkResultType::NEED_MORE_INPUT);
				}
				processed_chunks++;
			}
			if (source_result == SourceResultType::FINISHED) {
				result = JitFullPipelineResult::FINISHED;
				return true;
			}
		}
	}

private:
	void BindExecutionState(JitFullPipelineRuntime &runtime, SljitFusedUngroupedSumExecutionState &state) {
		vector<JitNativeUngroupedAggregateState> requested_states;
		JitNativeUngroupedAggregateState requested_state;
		requested_state.aggregate_index = update.aggregate_index;
		requested_state.update_kind = update.update_kind;
		requested_states.push_back(requested_state);

		runtime.BindNativeUngroupedAggregateStates(requested_states, state.bound_states);
		if (state.bound_states.size() != 1 || !state.bound_states[0].state || !state.bound_states[0].count) {
			throw InternalException("SLJIT fused ungrouped aggregate received an unbound state");
		}
	}

	idx_t ExecuteFused(JitFullPipelineRuntime &runtime, SljitFusedUngroupedSumExecutionState &state,
	                  DataChunk &input) {
		PrepareSljitPredicateSources(&input, true, state.formats, state.source_data, state.source_sel,
		                             state.source_validity);
		auto flat_all_valid =
		    RecordSljitGeneratedBodyPath(runtime, state.source_data, state.source_sel, state.source_validity,
		                                 input.size());
		auto shared_selection_all_valid =
		    !flat_all_valid &&
		    SljitSourcesSharedSelectionAllValid(state.source_data, state.source_sel, state.source_validity);

		SljitFusedUngroupedAggregateInput native_input;
		native_input.source_data = state.source_data.data();
		native_input.source_sel = state.source_sel.data();
		native_input.source_validity = state.source_validity.data();
		native_input.count = input.size();
		native_input.state = state.bound_states[0].state;
		native_input.state_count = state.bound_states[0].count;
		native_input.state_value_offset = update.state_value_offset;
		native_input.state_is_set_offset = update.state_is_set_offset;
		native_input.selected_count = 0;
		native_input.flat_all_valid = flat_all_valid;
		native_input.shared_selection_all_valid = shared_selection_all_valid;
		native_input.overflow_message = projection_overflow_message.c_str();
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return native_input.selected_count;
	}

private:
	string backend_name;
	SljitNativeRegionExpressionPlan filter;
	SljitNativeRegionExpressionPlan projection;
	SljitNativeUngroupedAggregateUpdatePlan update;
	unique_ptr<JitCodeHandle> code;
	SljitFusedUngroupedAggregateFunction function;
	string projection_overflow_message;
	bool native_source;
};

class SljitNativeRegionKernel : public JitRegionKernel {
private:
	struct SljitHashJoinProbeDrainState {
		idx_t input_offset = 0;
		data_ptr_t resume_row_pointer = nullptr;
		bool finished = false;
	};

public:
	SljitNativeRegionKernel(ClientContext &context, string backend_name_p, vector<SljitExecutableRegionOp> ops_p,
	                        JitRegionABI abi_p, bool native_source_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)), abi(abi_p), native_source(native_source_p) {
		filter_selections.reserve(ops.size());
		hash_join_row_pointers.reserve(ops.size());
		native_operator_binding_inputs.reserve(ops.size());
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			filter_selections.push_back(make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE));
			hash_join_row_pointers.push_back(make_uniq<Vector>(LogicalType::POINTER));
			auto binding_input = make_uniq<DataChunk>();
			if (ops[op_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				binding_input->Initialize(BufferAllocator::Get(context), ops[op_idx].hash_join_probe.plan.input_types);
			}
			native_operator_binding_inputs.push_back(std::move(binding_input));
		}
		if (ops.size() > 1) {
			temporary_chunks.reserve(ops.size() - 1);
			for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
				auto chunk = make_uniq<DataChunk>();
				chunk->Initialize(BufferAllocator::Get(context), ops[op_idx].output_types);
				temporary_chunks.push_back(std::move(chunk));
			}
		}
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		idx_t result = 0;
		for (auto &op : ops) {
			result += op.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const override {
		return !ops.empty();
	}

	bool CanExecuteSourcePipeline() const override {
		return JitRegionABIIsSourcePipeline(abi);
	}

	bool CanExecuteSinkPipeline() const override {
		return JitRegionABIIsSinkPipeline(abi);
	}

	bool CanExecuteFullPipeline() const override {
		return JitRegionABIIsFullPipeline(abi);
	}

	bool RequiresNativeSource() const override {
		return native_source;
	}

	bool TryExecute(DataChunk &input, DataChunk &result, idx_t initial_idx, OperatorResultType &execute_result) override {
		if (JitRegionABIOwnsSink(abi)) {
			return false;
		}
		if (!HasTraceCandidate() || initial_idx != TraceCandidateStartOperatorIndex()) {
			return false;
		}
		ExecuteNative(input, result);
		execute_result = OperatorResultType::NEED_MORE_INPUT;
		return true;
	}

	bool TrySink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
	             SinkResultType &sink_result) override {
		if (!JitRegionABIIsSinkPipeline(abi) || !HasTraceCandidate()) {
			return false;
		}
		ExecuteNativeSink(context, input, sink_input, sink_result);
		return true;
	}

	bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result) override {
		if (!JitRegionABIIsFullPipeline(abi) || !HasTraceCandidate()) {
			return false;
		}
		string blocker;
		if (!CanBindNativeOperators(runtime, blocker)) {
			SetRuntimeDeclineReason("native-operator-runtime-binding-blocked:" + blocker);
			return false;
		}
		idx_t processed_chunks = 0;
		while (true) {
			if (processed_chunks >= runtime.MaxChunks()) {
				result = JitFullPipelineResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			int64_t source_fetch_time_us = 0;
			auto source_result = native_source ? runtime.FetchNativeSource(source_chunk, source_fetch_time_us)
			                                  : runtime.FetchSource(source_chunk, source_fetch_time_us);
			if (source_result == SourceResultType::BLOCKED) {
				result = JitFullPipelineResult::INTERRUPTED;
				return true;
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto sink_result = ExecuteNativeFullPipeline(runtime, *source_chunk);
				if (sink_result == SinkResultType::BLOCKED) {
					result = JitFullPipelineResult::INTERRUPTED;
					return true;
				}
				if (sink_result == SinkResultType::FINISHED) {
					result = JitFullPipelineResult::FINISHED;
					return true;
				}
				processed_chunks++;
			}
			if (source_result == SourceResultType::FINISHED) {
				result = JitFullPipelineResult::FINISHED;
				return true;
			}
		}
	}

private:
	bool CanBindNativeOperators(JitFullPipelineRuntime &runtime, string &blocker) {
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				continue;
			}
			if (op_idx >= native_operator_binding_inputs.size() || !native_operator_binding_inputs[op_idx]) {
				throw InternalException("SLJIT native operator binding input is missing");
			}
			JitNativeOperatorBinding binding;
			if (!runtime.BindNativeOperator(op.operator_index, *native_operator_binding_inputs[op_idx],
			                                op.hash_join_probe.plan.operator_info, binding)) {
				blocker = binding.blocker.empty() ? "native-operator-runtime-binding-failed" : binding.blocker;
				return false;
			}
		}
		return true;
	}

	void ExecuteNative(DataChunk &input, DataChunk &result) {
		result.Reset();
		if (input.size() == 0) {
			return;
		}

		DataChunk *current = &input;
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (CanExecuteFilterProjection(op_idx)) {
				auto &output = op_idx + 2 == ops.size() ? result : *temporary_chunks[op_idx + 1];
				output.Reset();
				ExecuteFilterProjection(op, ops[op_idx + 1], *current, output, *filter_selections[op_idx]);
				current = &output;
				op_idx++;
				if (current->size() == 0) {
					result.Reset();
					return;
				}
				continue;
			}
			auto &output = op_idx + 1 == ops.size() ? result : *temporary_chunks[op_idx];
			output.Reset();
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER:
				ExecuteFilter(op, *current, output, *filter_selections[op_idx]);
				break;
			case SljitNativeRegionOpKind::PROJECTION:
				ExecuteProjection(op, *current, output);
				break;
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
				throw InternalException("SLJIT native hash join probe requires full-pipeline runtime binding");
			case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
			case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
			case SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE:
				throw InternalException("SLJIT sink operator cannot execute through the DataChunk result ABI");
			default:
				throw InternalException("Invalid SLJIT native region operator");
			}
			current = &output;
			if (current->size() == 0) {
				result.Reset();
				return;
			}
		}
	}

	SinkResultType ExecuteNativeFullPipeline(JitFullPipelineRuntime &runtime, DataChunk &input) {
		return ExecuteNativeFullPipelineFrom(runtime, 0, input);
	}

	SinkResultType ExecuteNativeFullPipelineFrom(JitFullPipelineRuntime &runtime, idx_t start_op_idx,
	                                            DataChunk &input) {
		if (input.size() == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}

		DataChunk *current = &input;
		for (idx_t op_idx = start_op_idx; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash join build sink must be the final full pipeline operator");
				}
				auto sink_result = ExecuteNativeHashJoinBuild(runtime, op, *current);
				return runtime.RecordNativeSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash aggregate sink must be the final full pipeline operator");
				}
				if (!op.native_grouped_aggregate_updates.empty()) {
					ExecuteNativeHashAggregateUpdate(runtime, op, *current);
					auto sink_result = runtime.FinishNativeHashAggregateUpdate(current->size());
					return runtime.RecordNativeSinkResult(*current, sink_result);
				}
				throw InternalException("SLJIT hash aggregate update reached runtime without native state update codegen");
			}
			if (op.kind == SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT perfect hash aggregate sink must be the final full pipeline operator");
				}
				if (!op.native_grouped_aggregate_updates.empty()) {
					ExecuteNativePerfectHashAggregateUpdate(runtime, op, *current);
					auto sink_result = runtime.FinishNativePerfectHashAggregateUpdate(current->size());
					return runtime.RecordNativeSinkResult(*current, sink_result);
				}
				throw InternalException("SLJIT perfect hash aggregate update reached runtime without native state update codegen");
			}
			if (op.kind == SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT ungrouped aggregate sink must be the final full pipeline operator");
				}
				if (!op.native_ungrouped_aggregate_updates.empty()) {
					ExecuteNativeUngroupedAggregateUpdate(runtime, op, *current);
					return runtime.RecordNativeSinkResult(*current, SinkResultType::NEED_MORE_INPUT);
				}
				throw InternalException("SLJIT ungrouped aggregate update reached runtime without native state update codegen");
			}
			if (CanExecuteFilterProjection(op_idx)) {
				if (op_idx + 1 >= temporary_chunks.size()) {
					throw InternalException("SLJIT full pipeline fused filter/projection has no temporary output chunk");
				}
				auto &output = *temporary_chunks[op_idx + 1];
				output.Reset();
				ExecuteFilterProjection(op, ops[op_idx + 1], *current, output, *filter_selections[op_idx]);
				current = &output;
				op_idx++;
				if (current->size() == 0) {
					return SinkResultType::NEED_MORE_INPUT;
				}
				continue;
			}
			if (op_idx >= temporary_chunks.size()) {
				throw InternalException("SLJIT full pipeline transform has no temporary output chunk");
			}
			auto &output = *temporary_chunks[op_idx];
			output.Reset();
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER:
				ExecuteFilter(op, *current, output, *filter_selections[op_idx]);
				break;
			case SljitNativeRegionOpKind::PROJECTION:
				ExecuteProjection(op, *current, output);
				break;
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
				return DrainNativeHashJoinProbe(runtime, op_idx, *current, output);
			case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
				throw InternalException("Invalid SLJIT hash aggregate update operator before sink");
			default:
				throw InternalException("Invalid SLJIT full pipeline operator before sink");
			}
			current = &output;
			if (current->size() == 0) {
				return SinkResultType::NEED_MORE_INPUT;
			}
		}
		throw InternalException("SLJIT full pipeline region has no native sink operator");
	}

	SinkResultType DrainNativeHashJoinProbe(JitFullPipelineRuntime &runtime, idx_t op_idx, DataChunk &input,
	                                        DataChunk &output) {
		auto &op = ops[op_idx];
		SljitHashJoinProbeDrainState state;
		do {
			output.Reset();
			ExecuteNativeHashJoinProbe(runtime, op, input, output, *filter_selections[op_idx],
			                           *hash_join_row_pointers[op_idx], state);
			if (output.size() == 0) {
				continue;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, op_idx + 1, output);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return sink_result;
			}
		} while (!state.finished);
		return SinkResultType::NEED_MORE_INPUT;
	}

	void ExecuteNativeSink(ExecutionContext &context, DataChunk &input, OperatorSinkInput &sink_input,
	                       SinkResultType &sink_result) {
		if (input.size() == 0) {
			sink_result = SinkResultType::NEED_MORE_INPUT;
			return;
		}

		DataChunk *current = &input;
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash join build sink must be the final region operator");
				}
				sink_result = ExecuteNativeHashJoinBuild(context, sink_input, op, *current);
				return;
			}
			if (op.kind == SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash aggregate sink must be the final region operator");
				}
				if (!op.native_grouped_aggregate_updates.empty()) {
					ExecuteNativeHashAggregateUpdate(context, sink_input, op, *current);
					sink_result = JitFinishNativeHashAggregateUpdate(context, sink_input, current->size());
					return;
				}
				throw InternalException("SLJIT hash aggregate sink reached runtime without native state update codegen");
			}
			if (op.kind == SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT perfect hash aggregate sink must be the final region operator");
				}
				if (!op.native_grouped_aggregate_updates.empty()) {
					ExecuteNativePerfectHashAggregateUpdate(context, sink_input, op, *current);
					sink_result = JitFinishNativePerfectHashAggregateUpdate(context, sink_input, current->size());
					return;
				}
				throw InternalException(
				    "SLJIT perfect hash aggregate sink reached runtime without native state update codegen");
			}
			if (op.kind == SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT ungrouped aggregate sink must be the final region operator");
				}
				if (!op.native_ungrouped_aggregate_updates.empty()) {
					ExecuteNativeUngroupedAggregateUpdate(sink_input, op, *current);
					sink_result = SinkResultType::NEED_MORE_INPUT;
					return;
				}
				throw InternalException(
				    "SLJIT ungrouped aggregate sink reached runtime without native state update codegen");
			}
			if (CanExecuteFilterProjection(op_idx)) {
				if (op_idx + 1 >= temporary_chunks.size()) {
					throw InternalException("SLJIT sink region fused filter/projection has no temporary output chunk");
				}
				auto &output = *temporary_chunks[op_idx + 1];
				output.Reset();
				ExecuteFilterProjection(op, ops[op_idx + 1], *current, output, *filter_selections[op_idx]);
				current = &output;
				op_idx++;
				if (current->size() == 0) {
					sink_result = SinkResultType::NEED_MORE_INPUT;
					return;
				}
				continue;
			}
			if (op_idx >= temporary_chunks.size()) {
				throw InternalException("SLJIT sink region transform has no temporary output chunk");
			}
			auto &output = *temporary_chunks[op_idx];
			output.Reset();
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER:
				ExecuteFilter(op, *current, output, *filter_selections[op_idx]);
				break;
			case SljitNativeRegionOpKind::PROJECTION:
				ExecuteProjection(op, *current, output);
				break;
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
				throw InternalException("SLJIT native hash join probe requires full-pipeline runtime binding");
			case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
				throw InternalException("Invalid SLJIT hash aggregate update operator before sink");
			default:
				throw InternalException("Invalid SLJIT sink region operator before sink");
			}
			current = &output;
			if (current->size() == 0) {
				sink_result = SinkResultType::NEED_MORE_INPUT;
				return;
			}
		}
		throw InternalException("SLJIT sink region has no sink operator");
	}

	bool CanExecuteFilterProjection(idx_t op_idx) const {
		return op_idx + 1 < ops.size() && ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
		       ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION;
	}

	idx_t SelectFilter(SljitExecutableRegionOp &op, DataChunk &input, SelectionVector &filter_selection) {
		auto &filter = op.filter.plan;
		if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			vector<UnifiedVectorFormat> formats;
			vector<const_data_ptr_t> source_data;
			vector<const sel_t *> source_sel;
			vector<const validity_t *> source_validity;
			PrepareSljitPredicateSources(&input, NativePredicateRequiresInput(*filter.predicate), formats,
			                             source_data, source_sel, source_validity);

			SljitNativePredicateInput native_input;
			native_input.source_data = source_data.data();
			native_input.source_sel = source_sel.data();
			native_input.source_validity = source_validity.data();
			native_input.execute_sel = nullptr;
			native_input.result_data = nullptr;
			native_input.result_validity = nullptr;
			native_input.true_sel = filter_selection.data();
			native_input.false_sel = nullptr;
			native_input.selected_count = 0;
			native_input.count = input.size();
			op.filter.predicate_select_function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			return native_input.selected_count;
		}
		D_ASSERT(filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
		         filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		input.data[filter.source_index].ToUnifiedFormat(source_format);
		if (filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
			D_ASSERT(filter.right_source_index < input.ColumnCount());
			input.data[filter.right_source_index].ToUnifiedFormat(right_source_format);
		}

		SljitNativeVectorInput native_input;
		native_input.source_data = filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK
		                               ? nullptr
		                               : NativeIntegerSourceData(source_format, filter.integer_kind);
		native_input.right_source_data = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
		                                     ? NativeIntegerSourceData(right_source_format, filter.integer_kind)
		                                     : nullptr;
		native_input.execute_sel = nullptr;
		native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
		native_input.right_source_sel = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES &&
		                                        right_source_format.sel
		                                    ? right_source_format.sel->data()
		                                    : nullptr;
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_validity = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
		                                         ? right_source_format.validity.GetData()
		                                         : nullptr;
		native_input.constants = filter.constants.data();
		native_input.constant = filter.constant;
		native_input.result_data = nullptr;
		native_input.result_validity = nullptr;
		native_input.true_sel = filter_selection.data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.overflow_message = nullptr;
		native_input.count = input.size();
		op.filter.select_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}

		return native_input.selected_count;
	}

	void ExecuteFilter(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                   SelectionVector &filter_selection) {
		auto selected_count = SelectFilter(op, input, filter_selection);
		if (selected_count == input.size()) {
			output.Reference(input);
		} else if (selected_count > 0) {
			output.Slice(input, filter_selection, selected_count);
		}
	}

	void ExecuteFilterProjection(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &projection_op,
	                             DataChunk &input, DataChunk &output, SelectionVector &filter_selection) {
		auto selected_count = SelectFilter(filter_op, input, filter_selection);
		if (selected_count == 0) {
			output.Reset();
			return;
		}
		auto *execute_sel = selected_count == input.size() ? nullptr : &filter_selection;
		ExecuteProjection(projection_op, input, output, execute_sel, selected_count);
	}

	void ExecuteProjection(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                       const SelectionVector *execute_sel = nullptr, idx_t count = DConstants::INVALID_INDEX) {
		if (count == DConstants::INVALID_INDEX) {
			count = input.size();
		}
		for (idx_t col_idx = 0; col_idx < op.projections.size(); col_idx++) {
			ExecuteProjectionExpression(op.projections[col_idx], input, output.data[col_idx], execute_sel, count);
		}
		output.SetChildCardinality(count);
	}

	void ExecuteProjectionExpression(SljitExecutableRegionExpression &expr, DataChunk &input, Vector &result,
	                                 const SelectionVector *execute_sel, idx_t count) {
		auto &plan = expr.plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			D_ASSERT(plan.source_index < input.ColumnCount());
			if (execute_sel) {
				result.Slice(input.data[plan.source_index], *execute_sel, count);
			} else {
				result.Reference(input.data[plan.source_index]);
			}
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT) {
			result.Reference(plan.constant_value, count_t(count));
			result.Flatten();
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			vector<UnifiedVectorFormat> formats;
			vector<const_data_ptr_t> source_data;
			vector<const sel_t *> source_sel;
			vector<const validity_t *> source_validity;
			PrepareSljitPredicateSources(&input, NativePredicateRequiresInput(*plan.predicate), formats, source_data,
			                             source_sel, source_validity);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			SljitNativePredicateInput native_input;
			native_input.source_data = source_data.data();
			native_input.source_sel = source_sel.data();
			native_input.source_validity = source_validity.data();
			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
			native_input.result_validity = result_validity.GetData();
			native_input.true_sel = nullptr;
			native_input.false_sel = nullptr;
			native_input.selected_count = 0;
			native_input.count = count;
			expr.predicate_function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
			auto constant = plan.constant_or_null.guard_has_null_constant || plan.constant_or_null.constant.IsNull()
			                    ? Value(plan.return_type)
			                    : plan.constant_or_null.constant;
			result.Reference(constant, count_t(count));
			result.Flatten();
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.EnsureWritable();
			if (!constant.IsNull()) {
				result_validity.SetAllValid(count);
				vector<UnifiedVectorFormat> formats;
				vector<const_data_ptr_t> source_data;
				vector<const sel_t *> source_sel;
				vector<const validity_t *> source_validity;
				PrepareSljitPredicateSources(&input, !plan.constant_or_null.guard_source_indices.empty(), formats,
				                             source_data, source_sel, source_validity);

				SljitNativePredicateInput native_input;
				native_input.source_data = source_data.data();
				native_input.source_sel = source_sel.data();
				native_input.source_validity = source_validity.data();
				native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
				native_input.result_data = nullptr;
				native_input.result_validity = result_validity.GetData();
				native_input.true_sel = nullptr;
				native_input.false_sel = nullptr;
				native_input.selected_count = 0;
				native_input.count = count;
				expr.predicate_function(&native_input);
				if (native_input.error) {
					std::rethrow_exception(native_input.error);
				}
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		D_ASSERT(plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
		         plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8 ||
		         plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
		         plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		auto has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		                        (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
		                         plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE);
		if (has_right_source) {
			D_ASSERT(plan.right_source_index < input.ColumnCount());
			input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
		}
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		validity_t *result_validity_data = nullptr;
		if ((plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST && plan.list_has_null) ||
		    (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && plan.try_cast) ||
		    (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST && plan.try_cast) ||
		    plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
		    (plan.kind != SljitNativeRegionExpressionKind::NULL_CHECK &&
		     (source_format.validity.CanHaveNull() ||
		      (has_right_source && right_source_format.validity.CanHaveNull())))) {
			result_validity.EnsureWritable();
			result_validity.SetAllValid(input.size());
			result_validity_data = result_validity.GetData();
		}

		SljitNativeVectorInput native_input;
		if (plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK) {
			native_input.source_data = nullptr;
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8) {
			native_input.source_data = source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.source_data = NativeUnsignedIntegerSourceData(source_format, plan.unsigned_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.cast_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.source_data = NativeIntegerSourceData(source_format, SljitNativeIntegerKind::INT32);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.source_data = source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
			native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.signed_integer_width);
		} else {
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
		}
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
		    plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			native_input.right_source_data =
			    NativeSignedIntegerSourceData(right_source_format, plan.signed_integer_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.right_source_data = right_source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
			native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
		} else {
			native_input.right_source_data = nullptr;
		}
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
		native_input.right_source_sel = has_right_source && right_source_format.sel ? right_source_format.sel->data()
		                                                                           : nullptr;
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_validity = has_right_source ? right_source_format.validity.GetData() : nullptr;
		native_input.constants = plan.constants.data();
		native_input.constant = plan.constant;
		native_input.double_constant = plan.double_constant;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.signed_integer_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint8_t>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			native_input.result_data = NativeIntegerResultData(result, plan.integer_kind);
		} else {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
		}
		native_input.result_validity = result_validity_data;
		native_input.true_sel = nullptr;
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
			native_input.overflow_message = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
			                                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
			                                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
			                                        plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
			                                    ? expr.overflow_message.c_str()
		                                    : nullptr;
		native_input.overflow_value = 0;
		native_input.count = count;
		expr.function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		FlatVector::SetSize(result, count_t(count));
	}

	void ExecuteNativeHashJoinProbe(JitFullPipelineRuntime &runtime, SljitExecutableRegionOp &op, DataChunk &input,
	                                DataChunk &output, SelectionVector &match_selection, Vector &row_pointers,
	                                SljitHashJoinProbeDrainState &state) {
		if (!op.hash_join_probe.function) {
			throw InternalException("SLJIT native hash join probe reached runtime without generated code");
		}
		JitNativeOperatorBinding binding;
		if (!runtime.BindNativeOperator(op.operator_index, input, op.hash_join_probe.plan.operator_info, binding)) {
			auto blocker = binding.blocker.empty() ? "unknown" : binding.blocker;
			throw InternalException("SLJIT native hash join probe operator binding failed: %s", blocker);
		}
		if (!binding.ready || !binding.hash_join_probe.ready) {
			throw InternalException("SLJIT native hash join probe received an incomplete operator binding");
		}
		auto &probe = binding.hash_join_probe;
		if (op.hash_join_probe.plan.output_mode == JitRegionHashJoinProbeOutputMode::NONE ||
		    probe.output_mode != op.hash_join_probe.plan.output_mode) {
			throw InternalException("SLJIT native hash join probe output mode mismatch");
		}
		if (probe.probe_key_input_indices.size() != op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe key binding count mismatch");
		}
		auto &layout = probe.table_layout;
		if (!layout.ready || !layout.entries || layout.layout_offsets.empty()) {
			throw InternalException("SLJIT native hash join probe received an incomplete hash table layout");
		}
		if (layout.layout_offsets.size() < op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe layout key count mismatch");
		}
		if (layout.pointer_offset != op.hash_join_probe.plan.pointer_offset) {
			throw InternalException("SLJIT native hash join probe pointer offset mismatch");
		}
		if (op.hash_join_probe.plan.mark_build_match) {
			if (!layout.found_match_column_present) {
				throw InternalException("SLJIT native hash join probe expected a build-side found-match column");
			}
			if (layout.tuple_size != op.hash_join_probe.plan.found_match_offset) {
				throw InternalException("SLJIT native hash join probe found-match offset mismatch");
			}
			if (op.hash_join_probe.plan.output_mode == JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY &&
			    layout.dictionary_emission && layout.chains_longer_than_one && !layout.aux_next_ptrs) {
				throw InternalException("SLJIT native hash join mark-only probe requires dictionary chain pointers");
			}
		}
		for (idx_t key_idx = 0; key_idx < op.hash_join_probe.plan.keys.size(); key_idx++) {
			auto &key = op.hash_join_probe.plan.keys[key_idx];
			if (probe.probe_key_input_indices[key_idx] != key.key_input_index) {
				throw InternalException("SLJIT native hash join probe key binding mismatch");
			}
			if (layout.layout_offsets[key_idx] != key.key_layout_offset) {
				throw InternalException("SLJIT native hash join probe key layout offset mismatch");
			}
		}

		vector<UnifiedVectorFormat> source_formats(op.hash_join_probe.plan.keys.size());
		vector<const_data_ptr_t> source_data(op.hash_join_probe.plan.keys.size());
		vector<const sel_t *> source_sel(op.hash_join_probe.plan.keys.size());
		vector<const validity_t *> source_validity(op.hash_join_probe.plan.keys.size());
		for (idx_t key_idx = 0; key_idx < op.hash_join_probe.plan.keys.size(); key_idx++) {
			auto &key = op.hash_join_probe.plan.keys[key_idx];
			input.data[key.key_input_index].ToUnifiedFormat(source_formats[key_idx]);
			source_data[key_idx] = NativeHashJoinKeySourceData(source_formats[key_idx], key.key_kind);
			source_sel[key_idx] = SljitNormalizedSourceSelectionData(source_formats[key_idx]);
			source_validity[key_idx] = source_formats[key_idx].validity.CannotHaveNull()
			                               ? nullptr
			                               : source_formats[key_idx].validity.GetData();
		}
		row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
		auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);

		SljitNativeHashJoinProbeInput native_input;
		native_input.source_data = source_data.data();
		native_input.source_sel = source_sel.data();
		native_input.source_validity = source_validity.data();
		native_input.count = input.size();
		native_input.entries = reinterpret_cast<const_data_ptr_t>(layout.entries);
		native_input.bitmask = layout.bitmask;
		native_input.pointer_mask = layout.pointer_mask;
		native_input.use_salt = layout.use_salt;
		native_input.rhs_keys_have_validity = layout.can_have_null;
		native_input.chains_longer_than_one = layout.chains_longer_than_one;
		native_input.dictionary_emission = layout.dictionary_emission;
		native_input.key_offset = layout.layout_offsets[0];
		native_input.pointer_offset = layout.pointer_offset;
		native_input.aux_next_ptrs = layout.aux_next_ptrs;
		native_input.match_sel = match_selection.data();
		native_input.row_pointers = row_pointer_data;
		native_input.selected_count = 0;
		native_input.input_offset = state.input_offset;
		native_input.resume_row_pointer = state.resume_row_pointer;
		native_input.finished = false;

		op.hash_join_probe.function(&native_input);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = native_input.resume_row_pointer;
		state.finished = native_input.finished;
		if (op.hash_join_probe.plan.output_mode == JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
			output.Reset();
			return;
		}
		if (native_input.selected_count == 0) {
			output.Reset();
			return;
		}
		FlatVector::SetSize(row_pointers, count_t(native_input.selected_count));
		JitMaterializeNativeHashJoinProbe(probe, input, row_pointers, match_selection, native_input.selected_count,
		                                  output);
	}

	SinkResultType ExecuteNativeHashJoinBuild(JitFullPipelineRuntime &runtime, SljitExecutableRegionOp &op,
	                                          DataChunk &input) {
		JitNativeSinkBinding binding;
		if (!runtime.BindNativeSink(input, op.hash_join_build.plan.sink_info, binding)) {
			auto blocker = binding.blocker.empty() ? "hash-join-build-native-runtime-binding-failed"
			                                      : binding.blocker;
			throw InternalException("SLJIT native hash join build binding failed: %s", blocker);
		}
		if (!binding.ready || !binding.hash_join_build.ready) {
			throw InternalException("SLJIT native hash join build binding did not return a ready build state");
		}
		return JitAppendNativeHashJoinBuild(binding.hash_join_build, input);
	}

	SinkResultType ExecuteNativeHashJoinBuild(ExecutionContext &context, OperatorSinkInput &sink_input,
	                                          SljitExecutableRegionOp &op, DataChunk &input) {
		JitNativeSinkBinding binding;
		if (!JitBindNativeHashJoinBuild(context, sink_input, input, op.hash_join_build.plan.sink_info, binding)) {
			auto blocker = binding.blocker.empty() ? "hash-join-build-native-runtime-binding-failed"
			                                      : binding.blocker;
			throw InternalException("SLJIT native hash join build sink binding failed: %s", blocker);
		}
		if (!binding.ready || !binding.hash_join_build.ready) {
			throw InternalException("SLJIT native hash join build sink binding did not return a ready build state");
		}
		return JitAppendNativeHashJoinBuild(binding.hash_join_build, input);
	}

	void ExecuteNativeUngroupedAggregateUpdate(JitFullPipelineRuntime &runtime, SljitExecutableRegionOp &op,
	                                           DataChunk &input) {
		vector<JitNativeUngroupedAggregateState> requested_states;
		BuildNativeUngroupedAggregateStateRequests(op, requested_states);
		vector<JitNativeUngroupedAggregateState> bound_states;
		runtime.BindNativeUngroupedAggregateStates(requested_states, bound_states);
		ExecuteNativeUngroupedAggregateUpdate(op, input, bound_states);
	}

	void ExecuteNativeUngroupedAggregateUpdate(OperatorSinkInput &sink_input, SljitExecutableRegionOp &op,
	                                           DataChunk &input) {
		vector<JitNativeUngroupedAggregateState> requested_states;
		BuildNativeUngroupedAggregateStateRequests(op, requested_states);
		vector<JitNativeUngroupedAggregateState> bound_states;
		JitBindNativeUngroupedAggregateStates(sink_input, requested_states, bound_states);
		ExecuteNativeUngroupedAggregateUpdate(op, input, bound_states);
	}

	void ExecuteNativeHashAggregateUpdate(JitFullPipelineRuntime &runtime, SljitExecutableRegionOp &op,
	                                      DataChunk &input) {
		vector<JitNativeGroupedAggregateStateRequest> requested_states;
		BuildNativeGroupedAggregateStateRequests(op, requested_states);
		JitNativeGroupedAggregateStateSet bound_states;
		runtime.BindNativeHashAggregateStates(input, op.grouped_aggregate_groups, requested_states, bound_states);
		ExecuteNativeGroupedAggregateUpdate(op, input, bound_states);
	}

	void ExecuteNativeHashAggregateUpdate(ExecutionContext &context, OperatorSinkInput &sink_input,
	                                      SljitExecutableRegionOp &op, DataChunk &input) {
		vector<JitNativeGroupedAggregateStateRequest> requested_states;
		BuildNativeGroupedAggregateStateRequests(op, requested_states);
		JitNativeGroupedAggregateStateSet bound_states;
		JitBindNativeHashAggregateStates(context, sink_input, input, op.grouped_aggregate_groups, requested_states,
		                                 bound_states);
		ExecuteNativeGroupedAggregateUpdate(op, input, bound_states);
	}

	void ExecuteNativePerfectHashAggregateUpdate(JitFullPipelineRuntime &runtime, SljitExecutableRegionOp &op,
	                                             DataChunk &input) {
		vector<JitNativeGroupedAggregateStateRequest> requested_states;
		BuildNativeGroupedAggregateStateRequests(op, requested_states);
		JitNativeGroupedAggregateStateSet bound_states;
		runtime.BindNativePerfectHashAggregateStates(input, op.grouped_aggregate_groups, requested_states, bound_states);
		ExecuteNativeGroupedAggregateUpdate(op, input, bound_states);
	}

	void ExecuteNativePerfectHashAggregateUpdate(ExecutionContext &context, OperatorSinkInput &sink_input,
	                                             SljitExecutableRegionOp &op, DataChunk &input) {
		vector<JitNativeGroupedAggregateStateRequest> requested_states;
		BuildNativeGroupedAggregateStateRequests(op, requested_states);
		JitNativeGroupedAggregateStateSet bound_states;
		JitBindNativePerfectHashAggregateStates(context, sink_input, input, op.grouped_aggregate_groups,
		                                       requested_states, bound_states);
		ExecuteNativeGroupedAggregateUpdate(op, input, bound_states);
	}

	static void BuildNativeUngroupedAggregateStateRequests(
	    SljitExecutableRegionOp &op, vector<JitNativeUngroupedAggregateState> &requested_states) {
		requested_states.reserve(op.native_ungrouped_aggregate_updates.size());
		for (auto &update : op.native_ungrouped_aggregate_updates) {
			JitNativeUngroupedAggregateState state;
			state.aggregate_index = update.plan.aggregate_index;
			state.update_kind = update.plan.update_kind;
			requested_states.push_back(state);
		}
	}

	static void BuildNativeGroupedAggregateStateRequests(
	    SljitExecutableRegionOp &op, vector<JitNativeGroupedAggregateStateRequest> &requested_states) {
		requested_states.reserve(op.native_grouped_aggregate_updates.size());
		for (auto &update : op.native_grouped_aggregate_updates) {
			JitNativeGroupedAggregateStateRequest state;
			state.aggregate_index = update.plan.aggregate_index;
			state.update_kind = update.plan.update_kind;
			requested_states.push_back(state);
		}
	}

	void ExecuteNativeUngroupedAggregateUpdate(SljitExecutableRegionOp &op, DataChunk &input,
	                                           const vector<JitNativeUngroupedAggregateState> &bound_states) {
		if (bound_states.size() != op.native_ungrouped_aggregate_updates.size()) {
			throw InternalException("SLJIT native ungrouped aggregate state binding count mismatch");
		}
		for (idx_t update_idx = 0; update_idx < op.native_ungrouped_aggregate_updates.size(); update_idx++) {
			auto &update = op.native_ungrouped_aggregate_updates[update_idx];
			auto &state = bound_states[update_idx];
			if (!state.state || !state.count) {
				throw InternalException("SLJIT native ungrouped aggregate update received an unbound state");
			}
			UnifiedVectorFormat source_format;
			SljitNativeUngroupedAggregateInput native_input;
			if (update.plan.update_kind == JitAggregateUpdateKind::COUNT ||
			    update.plan.update_kind == JitAggregateUpdateKind::SUM) {
				if (update.plan.payload_index >= input.ColumnCount()) {
					throw InternalException("SLJIT native aggregate update references payload column %llu beyond %llu",
					                        static_cast<unsigned long long>(update.plan.payload_index),
					                        static_cast<unsigned long long>(input.ColumnCount()));
				}
				input.data[update.plan.payload_index].ToUnifiedFormat(source_format);
				native_input.source_data = source_format.data;
				native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
				native_input.source_validity = source_format.validity.GetData();
			}
			native_input.count = input.size();
			native_input.state = state.state;
			native_input.state_count = state.count;
			native_input.state_value_offset = update.plan.state_value_offset;
			native_input.state_is_set_offset = update.plan.state_is_set_offset;
			update.function(&native_input);
		}
	}

	void ExecuteNativeGroupedAggregateUpdate(SljitExecutableRegionOp &op, DataChunk &input,
	                                         const JitNativeGroupedAggregateStateSet &bound_states) {
		if (bound_states.states.size() != op.native_grouped_aggregate_updates.size()) {
			throw InternalException("SLJIT native grouped aggregate state binding count mismatch");
		}
		if (bound_states.count != input.size()) {
			throw InternalException("SLJIT native grouped aggregate state binding row count mismatch");
		}
		auto state_addresses = FlatVector::GetData<data_ptr_t>(bound_states.aggregate_addresses);
		for (idx_t update_idx = 0; update_idx < op.native_grouped_aggregate_updates.size(); update_idx++) {
			auto &update = op.native_grouped_aggregate_updates[update_idx];
			auto &state = bound_states.states[update_idx];
			if (state.aggregate_index != update.plan.aggregate_index ||
			    state.update_kind != update.plan.update_kind) {
				throw InternalException("SLJIT native grouped aggregate state binding order mismatch");
			}
			UnifiedVectorFormat source_format;
			SljitNativeGroupedAggregateInput native_input;
			if (update.plan.update_kind == JitAggregateUpdateKind::COUNT ||
			    update.plan.update_kind == JitAggregateUpdateKind::SUM) {
				if (update.plan.payload_index >= input.ColumnCount()) {
					throw InternalException(
					    "SLJIT native grouped aggregate update references payload column %llu beyond %llu",
					    static_cast<unsigned long long>(update.plan.payload_index),
					    static_cast<unsigned long long>(input.ColumnCount()));
				}
				input.data[update.plan.payload_index].ToUnifiedFormat(source_format);
				native_input.source_data = source_format.data;
				native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
				native_input.source_validity = source_format.validity.GetData();
			}
			native_input.state_addresses = state_addresses;
			native_input.count = input.size();
			native_input.aggregate_state_offset = state.aggregate_state_offset;
			native_input.state_value_offset = update.plan.state_value_offset;
			native_input.state_is_set_offset = update.plan.state_is_set_offset;
			update.function(&native_input);
		}
	}

private:
	string backend_name;
	vector<SljitExecutableRegionOp> ops;
	vector<unique_ptr<DataChunk>> temporary_chunks;
	vector<unique_ptr<SelectionVector>> filter_selections;
	vector<unique_ptr<Vector>> hash_join_row_pointers;
	vector<unique_ptr<DataChunk>> native_operator_binding_inputs;
	JitRegionABI abi;
	bool native_source;
};

class SljitFusedDirectPerfectHashAggregateKernel : public JitRegionKernel {
private:
	struct SljitFusedDirectPerfectHashExecutionState {
		vector<JitNativeGroupedAggregateStateRequest> requested_states;
		JitNativeGroupedAggregateStateSet bound_states;
		JitNativePerfectHashAggregateStateLayout state_layout;
		vector<UnifiedVectorFormat> formats;
		vector<const_data_ptr_t> source_data;
		vector<const sel_t *> source_sel;
		vector<const validity_t *> source_validity;
	};

public:
	SljitFusedDirectPerfectHashAggregateKernel(string backend_name_p, SljitNativeRegionPlan region_p,
	                                           unique_ptr<JitCodeHandle> code_p,
	                                           SljitFusedPerfectHashAggregateFunction function_p,
	                                           string overflow_message_p, bool native_source_p)
	    : backend_name(std::move(backend_name_p)), region(std::move(region_p)), code(std::move(code_p)),
	      function(function_p), overflow_message(std::move(overflow_message_p)), native_source(native_source_p) {
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return code ? code->CodeSize() : 0;
	}

	bool HasExecutableBody() const override {
		return code && function;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool RequiresNativeSource() const override {
		return native_source;
	}

	bool TryExecuteFullPipeline(JitFullPipelineRuntime &runtime, JitFullPipelineResult &result) override {
		if (!HasTraceCandidate()) {
			return false;
		}
		SljitFusedDirectPerfectHashExecutionState state;
		BuildRequestedStates(state);
		BindStateLayout(runtime, state);
		idx_t processed_chunks = 0;
		while (true) {
			if (processed_chunks >= runtime.MaxChunks()) {
				result = JitFullPipelineResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			int64_t source_fetch_time_us = 0;
			auto source_result = native_source ? runtime.FetchNativeSource(source_chunk, source_fetch_time_us)
			                                  : runtime.FetchSource(source_chunk, source_fetch_time_us);
			if (source_result == SourceResultType::BLOCKED) {
				result = JitFullPipelineResult::INTERRUPTED;
				return true;
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto sink_rows = ExecuteFused(runtime, state, *source_chunk);
				if (sink_rows > 0) {
					std::chrono::steady_clock::time_point finish_start;
					if (runtime.TraceRuntime()) {
						finish_start = std::chrono::steady_clock::now();
					}
					auto sink_result = runtime.FinishNativePerfectHashAggregateUpdate(sink_rows);
					if (runtime.TraceRuntime()) {
						runtime.RecordFusedStageRuntime(JitFusedRuntimeStage::FINISH,
						                                SljitRegionElapsedMicros(finish_start));
					}
					if (runtime.RecordNativeSinkResult(sink_rows, sink_result) == SinkResultType::FINISHED) {
						result = JitFullPipelineResult::FINISHED;
						return true;
					}
				}
				processed_chunks++;
			}
			if (source_result == SourceResultType::FINISHED) {
				result = JitFullPipelineResult::FINISHED;
				return true;
			}
		}
	}

private:
	const SljitNativeRegionOpPlan &SinkOp() const {
		D_ASSERT(!region.ops.empty());
		return region.ops.back();
	}

	void BuildRequestedStates(SljitFusedDirectPerfectHashExecutionState &state) {
		auto &sink = SinkOp();
		state.requested_states.clear();
		state.requested_states.reserve(sink.native_grouped_aggregate_updates.size());
		for (auto &update : sink.native_grouped_aggregate_updates) {
			JitNativeGroupedAggregateStateRequest request;
			request.aggregate_index = update.aggregate_index;
			request.update_kind = update.update_kind;
			state.requested_states.push_back(request);
		}
	}

	void ValidateBoundStates(const SljitFusedDirectPerfectHashExecutionState &state) const {
		auto &updates = SinkOp().native_grouped_aggregate_updates;
		if (state.bound_states.states.size() != updates.size()) {
			throw InternalException("SLJIT fused direct perfect hash aggregate state binding count mismatch");
		}
		for (idx_t update_idx = 0; update_idx < updates.size(); update_idx++) {
			auto &bound = state.bound_states.states[update_idx];
			auto &update = updates[update_idx];
			if (bound.aggregate_index != update.aggregate_index || bound.update_kind != update.update_kind ||
			    bound.aggregate_state_offset != update.aggregate_state_offset) {
				throw InternalException("SLJIT fused direct perfect hash aggregate state binding layout mismatch");
			}
		}
		if (!state.state_layout.data || !state.state_layout.group_is_set || state.state_layout.total_groups == 0 ||
		    state.state_layout.tuple_size == 0) {
			throw InternalException("SLJIT fused direct perfect hash aggregate received an incomplete state layout");
		}
	}

	void BindStateLayout(JitFullPipelineRuntime &runtime, SljitFusedDirectPerfectHashExecutionState &state) {
		std::chrono::steady_clock::time_point stage_start;
		if (runtime.TraceRuntime()) {
			stage_start = std::chrono::steady_clock::now();
		}
		runtime.BindNativePerfectHashAggregateStateLayout(state.requested_states, state.bound_states,
		                                                  state.state_layout);
		ValidateBoundStates(state);
		if (runtime.TraceRuntime()) {
			runtime.RecordFusedStageRuntime(JitFusedRuntimeStage::STATE_BIND,
			                                SljitRegionElapsedMicros(stage_start));
		}
	}

	idx_t ExecuteFused(JitFullPipelineRuntime &runtime, SljitFusedDirectPerfectHashExecutionState &state,
	                  DataChunk &input) {
		std::chrono::steady_clock::time_point stage_start;
		if (runtime.TraceRuntime()) {
			stage_start = std::chrono::steady_clock::now();
		}
		PrepareSljitPredicateSources(&input, true, state.formats, state.source_data, state.source_sel,
		                             state.source_validity);
		if (runtime.TraceRuntime()) {
			runtime.RecordFusedStageRuntime(JitFusedRuntimeStage::PREPARE, SljitRegionElapsedMicros(stage_start));
		}
		auto flat_all_valid =
		    RecordSljitGeneratedBodyPath(runtime, state.source_data, state.source_sel, state.source_validity,
		                                 input.size());

		SljitFusedPerfectHashAggregateInput native_input;
		native_input.source_data = state.source_data.data();
		native_input.source_sel = state.source_sel.data();
		native_input.source_validity = state.source_validity.data();
		native_input.count = input.size();
		native_input.perfect_hash_data = state.state_layout.data;
		native_input.perfect_hash_group_is_set = state.state_layout.group_is_set;
		native_input.perfect_hash_total_groups = state.state_layout.total_groups;
		native_input.perfect_hash_tuple_size = state.state_layout.tuple_size;
		native_input.perfect_hash_aggregate_state_offset = state.state_layout.aggregate_state_offset;
		native_input.flat_all_valid = flat_all_valid;
		native_input.all_selected =
		    !flat_all_valid &&
		    SljitSourcesAllSelectedAllValid(state.source_data, state.source_sel, state.source_validity);
		native_input.all_valid = SljitSourcesAllValid(state.source_validity);
		native_input.selected_count = 0;
		native_input.overflow_message = overflow_message.c_str();
		if (runtime.TraceRuntime()) {
			stage_start = std::chrono::steady_clock::now();
		}
		function(&native_input);
		if (runtime.TraceRuntime()) {
			runtime.RecordFusedStageRuntime(JitFusedRuntimeStage::UPDATE, SljitRegionElapsedMicros(stage_start));
		}
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return native_input.selected_count;
	}

private:
	string backend_name;
	SljitNativeRegionPlan region;
	unique_ptr<JitCodeHandle> code;
	SljitFusedPerfectHashAggregateFunction function;
	string overflow_message;
	bool native_source;
};

class SljitFusedFilterProjectionKernel : public JitRegionKernel {
public:
	SljitFusedFilterProjectionKernel(string backend_name_p, SljitNativeRegionExpressionPlan filter_p,
	                                 SljitNativeRegionExpressionPlan projection_p, unique_ptr<JitCodeHandle> code_p,
	                                 SljitFusedFilterProjectionFunction function_p,
	                                 string projection_overflow_message_p, JitRegionABI abi_p, bool native_source_p)
	    : backend_name(std::move(backend_name_p)), filter(std::move(filter_p)), projection(std::move(projection_p)),
	      code(std::move(code_p)), function(function_p),
	      projection_overflow_message(std::move(projection_overflow_message_p)), abi(abi_p),
	      native_source(native_source_p) {
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return code->CodeSize();
	}

	bool CanExecuteSourcePipeline() const override {
		return JitRegionABIIsSourcePipeline(abi);
	}

	bool RequiresNativeSource() const override {
		return native_source;
	}

	bool TryExecute(DataChunk &input, DataChunk &result, idx_t initial_idx, OperatorResultType &execute_result) override {
		if (!HasTraceCandidate() || initial_idx != TraceCandidateStartOperatorIndex()) {
			return false;
		}
		ExecuteFused(input, result);
		execute_result = OperatorResultType::NEED_MORE_INPUT;
		return true;
	}

private:
	void ExecuteFused(DataChunk &input, DataChunk &result) {
		result.Reset();
		if (input.size() == 0) {
			return;
		}

		UnifiedVectorFormat filter_format;
		UnifiedVectorFormat projection_format;
		input.data[filter.source_index].ToUnifiedFormat(filter_format);
		input.data[projection.source_index].ToUnifiedFormat(projection_format);

		auto &output = result.data[0];
		output.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(output);
		result_validity.Reset(input.size());
		validity_t *result_validity_data = nullptr;
		if (projection_format.validity.CanHaveNull()) {
			result_validity.EnsureWritable();
			result_validity.SetAllValid(input.size());
			result_validity_data = result_validity.GetData();
		}

		SljitFusedFilterProjectionInput native_input;
		native_input.filter_data = NativeIntegerSourceData(filter_format, filter.integer_kind);
		native_input.filter_sel = filter_format.sel ? filter_format.sel->data() : nullptr;
		native_input.filter_validity = filter_format.validity.GetData();
		native_input.filter_constant = filter.constant;
		native_input.projection_data = NativeIntegerSourceData(projection_format, projection.integer_kind);
		native_input.projection_sel = projection_format.sel ? projection_format.sel->data() : nullptr;
		native_input.projection_validity = projection_format.validity.GetData();
		native_input.projection_constant = projection.constant;
		native_input.result_data = NativeIntegerResultData(output, projection.integer_kind);
		native_input.result_validity = result_validity_data;
		native_input.input_count = input.size();
		native_input.output_count = 0;
		native_input.overflow_message = projection_overflow_message.c_str();
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		FlatVector::SetSize(output, count_t(native_input.output_count));
		result.SetChildCardinality(native_input.output_count);
	}

private:
	string backend_name;
	SljitNativeRegionExpressionPlan filter;
	SljitNativeRegionExpressionPlan projection;
	unique_ptr<JitCodeHandle> code;
	SljitFusedFilterProjectionFunction function;
	string projection_overflow_message;
	JitRegionABI abi;
	bool native_source;
};

unique_ptr<JitRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                          SljitExecutableRegion &&region, JitRegionABI abi,
                                                          bool native_source) {
	return make_uniq<SljitNativeRegionKernel>(context, std::move(backend_name), std::move(region.ops),
	                                          abi, native_source);
}

unique_ptr<JitRegionKernel> CreateSljitFusedFilterProjectionKernel(string backend_name,
                                                                     SljitNativeRegionExpressionPlan filter,
                                                                     SljitNativeRegionExpressionPlan projection,
                                                                     unique_ptr<JitCodeHandle> code,
                                                                     SljitFusedFilterProjectionFunction function,
                                                                     string projection_overflow_message,
                                                                     JitRegionABI abi, bool native_source) {
	return make_uniq<SljitFusedFilterProjectionKernel>(std::move(backend_name), std::move(filter),
	                                                   std::move(projection), std::move(code), function,
	                                                   std::move(projection_overflow_message), abi, native_source);
}

unique_ptr<JitRegionKernel> CreateSljitFusedFilterProjectionUngroupedSumKernel(
    string backend_name, SljitNativeRegionExpressionPlan filter, SljitNativeRegionExpressionPlan projection,
    SljitNativeUngroupedAggregateUpdatePlan update, unique_ptr<JitCodeHandle> code,
	SljitFusedUngroupedAggregateFunction function, string projection_overflow_message, bool native_source) {
	return make_uniq<SljitFusedUngroupedSumKernel>(
	    std::move(backend_name), std::move(filter), std::move(projection), std::move(update), std::move(code), function,
	    std::move(projection_overflow_message), native_source);
}

unique_ptr<JitRegionKernel> CreateSljitFusedProjectionUngroupedSumKernel(
    string backend_name, SljitNativeRegionExpressionPlan projection, SljitNativeUngroupedAggregateUpdatePlan update,
    unique_ptr<JitCodeHandle> code, SljitFusedUngroupedAggregateFunction function,
    string projection_overflow_message, bool native_source) {
	return make_uniq<SljitFusedUngroupedSumKernel>(
	    std::move(backend_name), std::move(projection), std::move(update), std::move(code), function,
	    std::move(projection_overflow_message), native_source);
}

unique_ptr<JitRegionKernel> CreateSljitFusedDirectPerfectHashAggregateKernel(
    string backend_name, SljitNativeRegionPlan region, unique_ptr<JitCodeHandle> code,
    SljitFusedPerfectHashAggregateFunction function, string overflow_message, bool native_source) {
	return make_uniq<SljitFusedDirectPerfectHashAggregateKernel>(
	    std::move(backend_name), std::move(region), std::move(code), function, std::move(overflow_message),
	    native_source);
}

} // namespace duckdb

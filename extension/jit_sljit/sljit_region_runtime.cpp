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
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <exception>

namespace duckdb {

static int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static const sel_t *SljitNormalizedSourceSelectionData(const UnifiedVectorFormat &format) {
	if (!format.sel || format.sel == FlatVector::IncrementalSelectionVector()) {
		return nullptr;
	}
	return format.sel->data();
}

static const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format, SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeHashJoinKeyKind::INT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int16_t>(format));
	case SljitNativeHashJoinKeyKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeHashJoinKeyKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeHashJoinKeyKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeHashJoinKeyKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeHashJoinKeyKind::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeHashJoinKeyKind::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	case SljitNativeHashJoinKeyKind::UINT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint64_t>(format));
	case SljitNativeHashJoinKeyKind::UINT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uhugeint_t>(format));
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static const_data_ptr_t NativeNestedLoopJoinConditionSourceData(UnifiedVectorFormat &format,
                                                                SljitNativeNestedLoopJoinValueKind kind) {
	switch (kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<double>(format));
	default:
		throw InternalException("Unknown SLJIT native nested loop join value kind");
	}
}

static const char *SljitRegionOpKindName(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::FILTER:
		return "filter";
	case SljitNativeRegionOpKind::PROJECTION:
		return "projection";
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return "hash_join_probe";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return "nested_loop_join_probe";
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return "hash_join_build";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return "nested_loop_join_build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order_sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append_sink";
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return "delim_join_sink";
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return "aggregate_update";
	default:
		return "unknown";
	}
}

static string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind) {
	return "op" + std::to_string(op_idx) + ":" + SljitRegionOpKindName(kind);
}

static std::chrono::steady_clock::time_point SljitRegionStageStart(ExecutionRegionRuntime &runtime) {
	return runtime.TraceRuntime() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

static void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, const string &stage,
                                          std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(stage, SljitRegionElapsedMicros(start));
}

class SljitNativeRegionKernel : public ExecutionRegionKernel {
private:
	struct SljitHashJoinProbeDrainState {
		idx_t input_offset = 0;
		data_ptr_t resume_row_pointer = nullptr;
		vector<uint8_t> found_match;
		bool left_initialized = false;
		bool left_unmatched_emitted = false;
		bool finished = false;
	};

	struct SljitNestedLoopJoinProbeDrainState {
		bool lhs_materialized = false;
		bool started = false;
		bool right_chunk_finished = false;
		bool finished = false;
		idx_t left_offset = 0;
		idx_t right_offset = 0;
	};

	struct SljitRegionExecutionScratch {
		SljitRegionExecutionScratch(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops) {
			temporary_chunks.resize(ops.size());
			filter_selections.resize(ops.size());
			operator_bindings.resize(ops.size());
			operator_binding_ready.resize(ops.size());
			sink_bindings.resize(ops.size());
			sink_binding_ready.resize(ops.size());
			hash_join_build_selections.resize(ops.size());
			hash_join_row_pointers.resize(ops.size());
			hash_join_residual_chunks.resize(ops.size());
			hash_join_residual_selections.resize(ops.size());
			hash_join_residual_match_selections.resize(ops.size());
			hash_join_residual_row_pointers.resize(ops.size());
			nested_loop_left_condition_chunks.resize(ops.size());
			nested_loop_left_selections.resize(ops.size());
			nested_loop_right_selections.resize(ops.size());
			nested_loop_condition_chunks.resize(ops.size());
			order_key_chunks.resize(ops.size());
			order_payload_chunks.resize(ops.size());
			aggregate_state_addresses.resize(ops.size());
			for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
				auto &op = ops[op_idx];
				if (op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
					filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
				}
				if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
					hash_join_build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					hash_join_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
					if (op.hash_join_probe.plan.residual_predicate) {
						hash_join_residual_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
						hash_join_residual_match_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
						hash_join_residual_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
						auto residual_chunk = make_uniq<DataChunk>();
						residual_chunk->Initialize(allocator, op.hash_join_probe.plan.residual_source_types);
						hash_join_residual_chunks[op_idx] = std::move(residual_chunk);
					}
				}
				if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
					nested_loop_left_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					nested_loop_right_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					auto condition_chunk = make_uniq<DataChunk>();
					condition_chunk->Initialize(allocator, op.nested_loop_join_probe.plan.condition_types);
					nested_loop_left_condition_chunks[op_idx] = std::move(condition_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
					auto condition_chunk = make_uniq<DataChunk>();
					condition_chunk->Initialize(allocator, op.nested_loop_join_build.plan.condition_types);
					nested_loop_condition_chunks[op_idx] = std::move(condition_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::ORDER_SINK) {
					auto order_key_chunk = make_uniq<DataChunk>();
					order_key_chunk->Initialize(allocator, op.order_sink.plan.key_types);
					order_key_chunks[op_idx] = std::move(order_key_chunk);
					auto order_payload_chunk = make_uniq<DataChunk>();
					order_payload_chunk->Initialize(allocator, op.order_sink.plan.input_types);
					order_payload_chunks[op_idx] = std::move(order_payload_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
				    op.aggregate_update.plan.use_grouped_state_addresses) {
					aggregate_state_addresses[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
				}
				if (OpIsSink(op.kind)) {
					continue;
				}
				if (CanFuseFilterProjection(ops, op_idx)) {
					InitializeTemporaryChunk(allocator, ops, op_idx + 1);
					continue;
				}
				InitializeTemporaryChunk(allocator, ops, op_idx);
			}
		}

		DataChunk &TemporaryChunk(idx_t op_idx) {
			if (op_idx >= temporary_chunks.size() || !temporary_chunks[op_idx]) {
				throw InternalException("SLJIT full pipeline transform has no stage scratch chunk");
			}
			return *temporary_chunks[op_idx];
		}

		SelectionVector &FilterSelection(idx_t op_idx) {
			if (op_idx >= filter_selections.size() || !filter_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline transform has no selection scratch");
			}
			return *filter_selections[op_idx];
		}

		bool HasSinkBinding(idx_t op_idx) const {
			return op_idx < sink_binding_ready.size() && sink_binding_ready[op_idx];
		}

		ExecutionSinkBinding &SinkBinding(idx_t op_idx) {
			if (op_idx >= sink_bindings.size()) {
				throw InternalException("SLJIT full pipeline sink has no binding scratch");
			}
			return sink_bindings[op_idx];
		}

		void MarkSinkBindingReady(idx_t op_idx) {
			if (op_idx >= sink_binding_ready.size()) {
				throw InternalException("SLJIT full pipeline sink has no binding-ready scratch");
			}
			sink_binding_ready[op_idx] = true;
		}

		bool HasOperatorBinding(idx_t op_idx) const {
			return op_idx < operator_binding_ready.size() && operator_binding_ready[op_idx];
		}

		ExecutionOperatorBinding &OperatorBinding(idx_t op_idx) {
			if (op_idx >= operator_bindings.size()) {
				throw InternalException("SLJIT full pipeline operator has no binding scratch");
			}
			return operator_bindings[op_idx];
		}

		void MarkOperatorBindingReady(idx_t op_idx) {
			if (op_idx >= operator_binding_ready.size()) {
				throw InternalException("SLJIT full pipeline operator has no binding-ready scratch");
			}
			operator_binding_ready[op_idx] = true;
		}

		Vector &HashJoinRowPointers(idx_t op_idx) {
			if (op_idx >= hash_join_row_pointers.size() || !hash_join_row_pointers[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no row-pointer scratch");
			}
			return *hash_join_row_pointers[op_idx];
		}

		SelectionVector &HashJoinBuildSelection(idx_t op_idx) {
			if (op_idx >= hash_join_build_selections.size() || !hash_join_build_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no build-selection scratch");
			}
			return *hash_join_build_selections[op_idx];
		}

		DataChunk &HashJoinResidualChunk(idx_t op_idx) {
			if (op_idx >= hash_join_residual_chunks.size() || !hash_join_residual_chunks[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual chunk scratch");
			}
			return *hash_join_residual_chunks[op_idx];
		}

		SelectionVector &HashJoinResidualSelection(idx_t op_idx) {
			if (op_idx >= hash_join_residual_selections.size() || !hash_join_residual_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual selection scratch");
			}
			return *hash_join_residual_selections[op_idx];
		}

		SelectionVector &HashJoinResidualMatchSelection(idx_t op_idx) {
			if (op_idx >= hash_join_residual_match_selections.size() || !hash_join_residual_match_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual match selection scratch");
			}
			return *hash_join_residual_match_selections[op_idx];
		}

		Vector &HashJoinResidualRowPointers(idx_t op_idx) {
			if (op_idx >= hash_join_residual_row_pointers.size() || !hash_join_residual_row_pointers[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual row-pointer scratch");
			}
			return *hash_join_residual_row_pointers[op_idx];
		}

		DataChunk &NestedLoopLeftConditionChunk(idx_t op_idx) {
			if (op_idx >= nested_loop_left_condition_chunks.size() || !nested_loop_left_condition_chunks[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no left condition scratch chunk");
			}
			return *nested_loop_left_condition_chunks[op_idx];
		}

		SelectionVector &NestedLoopLeftSelection(idx_t op_idx) {
			if (op_idx >= nested_loop_left_selections.size() || !nested_loop_left_selections[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no left selection scratch");
			}
			return *nested_loop_left_selections[op_idx];
		}

		SelectionVector &NestedLoopRightSelection(idx_t op_idx) {
			if (op_idx >= nested_loop_right_selections.size() || !nested_loop_right_selections[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no right selection scratch");
			}
			return *nested_loop_right_selections[op_idx];
		}

		DataChunk &NestedLoopConditionChunk(idx_t op_idx) {
			if (op_idx >= nested_loop_condition_chunks.size() || !nested_loop_condition_chunks[op_idx]) {
				throw InternalException("SLJIT nested loop join build has no condition scratch chunk");
			}
			return *nested_loop_condition_chunks[op_idx];
		}

		DataChunk &OrderKeyChunk(idx_t op_idx) {
			if (op_idx >= order_key_chunks.size() || !order_key_chunks[op_idx]) {
				throw InternalException("SLJIT ordered sink has no order-key scratch chunk");
			}
			return *order_key_chunks[op_idx];
		}

		DataChunk &OrderPayloadChunk(idx_t op_idx) {
			if (op_idx >= order_payload_chunks.size() || !order_payload_chunks[op_idx]) {
				throw InternalException("SLJIT ordered sink has no payload scratch chunk");
			}
			return *order_payload_chunks[op_idx];
		}

		Vector &AggregateStateAddresses(idx_t op_idx) {
			if (op_idx >= aggregate_state_addresses.size() || !aggregate_state_addresses[op_idx]) {
				throw InternalException("SLJIT aggregate update has no grouped state-address scratch");
			}
			return *aggregate_state_addresses[op_idx];
		}

		vector<unique_ptr<DataChunk>> temporary_chunks;
		vector<unique_ptr<SelectionVector>> filter_selections;
		vector<ExecutionOperatorBinding> operator_bindings;
		vector<bool> operator_binding_ready;
		vector<ExecutionSinkBinding> sink_bindings;
		vector<bool> sink_binding_ready;
		vector<unique_ptr<SelectionVector>> hash_join_build_selections;
		vector<unique_ptr<Vector>> hash_join_row_pointers;
		vector<unique_ptr<DataChunk>> hash_join_residual_chunks;
		vector<unique_ptr<SelectionVector>> hash_join_residual_selections;
		vector<unique_ptr<SelectionVector>> hash_join_residual_match_selections;
		vector<unique_ptr<Vector>> hash_join_residual_row_pointers;
		vector<unique_ptr<DataChunk>> nested_loop_left_condition_chunks;
		vector<unique_ptr<SelectionVector>> nested_loop_left_selections;
		vector<unique_ptr<SelectionVector>> nested_loop_right_selections;
		vector<unique_ptr<DataChunk>> nested_loop_condition_chunks;
		vector<unique_ptr<DataChunk>> order_key_chunks;
		vector<unique_ptr<DataChunk>> order_payload_chunks;
		vector<unique_ptr<Vector>> aggregate_state_addresses;

	private:
		static bool OpIsSink(SljitNativeRegionOpKind kind) {
			switch (kind) {
			case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			case SljitNativeRegionOpKind::ORDER_SINK:
			case SljitNativeRegionOpKind::APPEND_SINK:
			case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
				return true;
			default:
				return false;
			}
		}

		static bool CanFuseFilterProjection(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
			return op_idx + 1 < ops.size() && ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
			       ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION;
		}

		void InitializeTemporaryChunk(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
			if (op_idx >= ops.size() || temporary_chunks[op_idx]) {
				return;
			}
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(allocator, ops[op_idx].output_types);
			temporary_chunks[op_idx] = std::move(chunk);
		}
	};

public:
	SljitNativeRegionKernel(string backend_name_p, vector<SljitExecutableRegionOp> ops_p, ExecutionRegionABI abi_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)), abi(abi_p) {
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

	bool CanExecuteFullPipeline() const override {
		return ExecutionRegionABIIsFullPipeline(abi);
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) override {
		if (!ExecutionRegionABIIsFullPipeline(abi) || !HasTraceCandidate()) {
			throw InternalException("SLJIT full pipeline kernel entered without full-pipeline trace contract");
		}
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t processed_chunks = 0;
		while (true) {
			if (processed_chunks >= runtime.MaxChunks()) {
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto sink_result = ExecuteNativeFullPipeline(runtime, scratch, *source_chunk);
				if (sink_result == SinkResultType::BLOCKED) {
					result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
					                                          : ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (sink_result == SinkResultType::FINISHED) {
					result = ExecutionRegionResult::FINISHED;
					return true;
				}
				processed_chunks++;
			}
			if (source_result == SourceResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	SinkResultType ExecuteNativeFullPipeline(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                         DataChunk &input) {
		return ExecuteNativeFullPipelineFrom(runtime, scratch, 0, input);
	}

	SinkResultType ExecuteNativeFullPipelineFrom(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                             idx_t start_op_idx, DataChunk &input) {
		if (input.size() == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}

		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk *current = &input;
		for (idx_t op_idx = start_op_idx; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			auto stage_name = SljitRegionStageName(op_idx, op.kind);
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash join build sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeHashJoinBuild(native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException(
					    "SLJIT nested loop join build sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeNestedLoopJoinBuild(native_runtime, scratch, op_idx, op, *current,
				                                                    scratch.NestedLoopConditionChunk(op_idx));
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::ORDER_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT ordered sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result =
				    ExecuteNativeOrderSink(native_runtime, scratch, op_idx, op, *current, scratch.OrderKeyChunk(op_idx),
				                           scratch.OrderPayloadChunk(op_idx));
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::APPEND_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT append sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeAppendSink(native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::DELIM_JOIN_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT delimiter join sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeDelimJoinSink(native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT aggregate update sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (CanExecuteFilterProjection(op_idx)) {
				auto &output = scratch.TemporaryChunk(op_idx + 1);
				output.Reset();
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteFilterProjection(runtime, scratch, op, ops[op_idx + 1], op_idx + 1, *current, output,
				                        scratch.FilterSelection(op_idx));
				RecordSljitRegionStageRuntime(runtime, stage_name + "+projection", stage_start);
				current = &output;
				op_idx++;
				if (current->size() == 0) {
					return SinkResultType::NEED_MORE_INPUT;
				}
				continue;
			}
			auto &output = scratch.TemporaryChunk(op_idx);
			output.Reset();
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER: {
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteFilter(op, *current, output, scratch.FilterSelection(op_idx));
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				break;
			}
			case SljitNativeRegionOpKind::PROJECTION: {
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteProjection(runtime, scratch, op_idx, op, *current, output);
				RecordSljitRegionStageRuntime(runtime, stage_name, stage_start);
				break;
			}
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
				return DrainNativeHashJoinProbe(runtime, scratch, op_idx, *current, output);
			}
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE: {
				return DrainNativeNestedLoopJoinProbe(runtime, scratch, op_idx, *current, output);
			}
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

	SinkResultType DrainNativeHashJoinProbe(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                        idx_t op_idx, DataChunk &input, DataChunk &output) {
		auto &op = ops[op_idx];
		SljitHashJoinProbeDrainState state;
		auto &native_runtime = runtime.ExecutionOperators();
		do {
			output.Reset();
			string deferred_reason;
			auto stage_start = SljitRegionStageStart(runtime);
			auto bind_result = ExecuteNativeHashJoinProbe(
			    native_runtime, op, input, output, scratch.FilterSelection(op_idx),
			    scratch.HashJoinBuildSelection(op_idx), scratch.HashJoinRowPointers(op_idx),
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualMatchSelection(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(op_idx) : nullptr,
			    state, deferred_reason);
			RecordSljitRegionStageRuntime(runtime, SljitRegionStageName(op_idx, op.kind), stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				return SinkResultType::BLOCKED;
			}
			if (output.size() == 0) {
				continue;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, op_idx + 1, output);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return sink_result;
			}
		} while (!HashJoinProbeDrainFinished(op.hash_join_probe.plan.output_mode, state));
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkResultType DrainNativeNestedLoopJoinProbe(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                              idx_t op_idx, DataChunk &input, DataChunk &output) {
		auto &op = ops[op_idx];
		SljitNestedLoopJoinProbeDrainState state;
		auto &native_runtime = runtime.ExecutionOperators();
		auto &left_condition = scratch.NestedLoopLeftConditionChunk(op_idx);
		do {
			output.Reset();
			string deferred_reason;
			auto stage_start = SljitRegionStageStart(runtime);
			auto bind_result = ExecuteNativeNestedLoopJoinProbe(
			    native_runtime, op, input, left_condition, output, scratch.NestedLoopLeftSelection(op_idx),
			    scratch.NestedLoopRightSelection(op_idx), state, deferred_reason);
			RecordSljitRegionStageRuntime(runtime, SljitRegionStageName(op_idx, op.kind), stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				return SinkResultType::BLOCKED;
			}
			if (output.size() == 0) {
				continue;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, op_idx + 1, output);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return sink_result;
			}
		} while (!state.finished);
		return SinkResultType::NEED_MORE_INPUT;
	}

	static bool IsLeftHashJoinProbeOutputMode(ExecutionHashJoinProbeOutputMode output_mode) {
		return output_mode == ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD;
	}

	static bool HashJoinProbeDrainFinished(ExecutionHashJoinProbeOutputMode output_mode,
	                                       const SljitHashJoinProbeDrainState &state) {
		return state.finished && (!IsLeftHashJoinProbeOutputMode(output_mode) || state.left_unmatched_emitted);
	}

	static void InitializeLeftHashJoinProbeState(SljitHashJoinProbeDrainState &state, idx_t input_count) {
		if (state.left_initialized) {
			return;
		}
		state.found_match.assign(input_count, 0);
		state.left_initialized = true;
		state.left_unmatched_emitted = false;
	}

	static void MarkLeftHashJoinProbeMatches(SljitHashJoinProbeDrainState &state,
	                                         const SelectionVector &match_selection, idx_t count) {
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			auto input_idx = match_selection.get_index(match_idx);
			if (input_idx >= state.found_match.size()) {
				throw InternalException("SLJIT native LEFT hash join match selection index out of range");
			}
			state.found_match[input_idx] = 1;
		}
	}

	static void MaterializeLeftHashJoinProbeUnmatched(const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
	                                                  DataChunk &output, SelectionVector &unmatched_selection,
	                                                  SljitHashJoinProbeDrainState &state) {
		InitializeLeftHashJoinProbeState(state, input.size());
		idx_t unmatched_count = 0;
		for (idx_t input_idx = 0; input_idx < input.size(); input_idx++) {
			if (state.found_match[input_idx]) {
				continue;
			}
			unmatched_selection.set_index(unmatched_count++, input_idx);
		}
		state.left_unmatched_emitted = true;
		if (unmatched_count == 0) {
			output.Reset();
			return;
		}
		ExecutionMaterializeHashJoinProbeLeftUnmatched(probe, input, unmatched_selection, unmatched_count, output);
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
			PrepareSljitPredicateSources(&input, NativePredicateRequiresInput(*filter.predicate), formats, source_data,
			                             source_sel, source_validity);

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
		native_input.right_source_sel =
		    filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES && right_source_format.sel
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

	ExecutionOperatorBinding &BindProjectionOperator(ExecutionRegionRuntime &runtime,
	                                                 SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                 SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = scratch.OperatorBinding(op_idx);
		if (scratch.HasOperatorBinding(op_idx)) {
			return binding;
		}
		if (op.operator_index == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT vectorized projection contract is missing an operator index");
		}
		ExecutionRegionOperatorInfo operator_info;
		operator_info.kind = ExecutionRegionOperatorContractKind::PROJECTION;
		auto bind_result = runtime.ExecutionOperators().BindOperator(op.operator_index, input, operator_info, binding);
		if (bind_result != ExecutionOperatorBindResult::READY || !binding.ready || !binding.projection.ready) {
			auto blocker = binding.blocker.empty() ? "projection-runtime-binding-failed" : binding.blocker;
			throw InternalException("SLJIT projection operator binding failed: %s", blocker.c_str());
		}
		scratch.MarkOperatorBindingReady(op_idx);
		return binding;
	}

	void ExecuteFilterProjection(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                             SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &projection_op,
	                             idx_t projection_op_idx, DataChunk &input, DataChunk &output,
	                             SelectionVector &filter_selection) {
		auto selected_count = SelectFilter(filter_op, input, filter_selection);
		if (selected_count == 0) {
			output.Reset();
			return;
		}
		auto *execute_sel = selected_count == input.size() ? nullptr : &filter_selection;
		ExecuteProjection(runtime, scratch, projection_op_idx, projection_op, input, output, execute_sel,
		                  selected_count);
	}

	void ExecuteProjection(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                       SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                       const SelectionVector *execute_sel = nullptr, idx_t count = DConstants::INVALID_INDEX) {
		if (count == DConstants::INVALID_INDEX) {
			count = input.size();
		}
		if (op.use_vectorized_projection) {
			auto &binding = BindProjectionOperator(runtime, scratch, op_idx, op, input);
			if (execute_sel) {
				DataChunk selected_input;
				selected_input.Initialize(runtime.GetAllocator(), input.GetTypes());
				selected_input.Slice(input, *execute_sel, count);
				ExecutionOperatorProject(binding.projection, selected_input, output);
			} else {
				ExecutionOperatorProject(binding.projection, input, output);
			}
			return;
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
		if (plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE) {
			vector<UnifiedVectorFormat> formats(plan.expression_tree_source_indices.size());
			vector<const_data_ptr_t> source_data(plan.expression_tree_source_indices.size());
			vector<const sel_t *> source_sel(plan.expression_tree_source_indices.size());
			vector<const validity_t *> source_validity(plan.expression_tree_source_indices.size());
			bool source_can_have_null = false;
			for (idx_t source_idx = 0; source_idx < plan.expression_tree_source_indices.size(); source_idx++) {
				auto input_index = plan.expression_tree_source_indices[source_idx];
				D_ASSERT(input_index < input.ColumnCount());
				input.data[input_index].ToUnifiedFormat(formats[source_idx]);
				source_data[source_idx] =
				    NativeIntegerSourceData(formats[source_idx], SljitNativeIntegerKind::DECIMAL64);
				source_sel[source_idx] = SljitNormalizedSourceSelectionData(formats[source_idx]);
				source_validity[source_idx] = formats[source_idx].validity.GetData();
				source_can_have_null = source_can_have_null || formats[source_idx].validity.CanHaveNull();
			}

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			validity_t *result_validity_data = nullptr;
			if (source_can_have_null) {
				result_validity.EnsureWritable();
				result_validity.SetAllValid(count);
				result_validity_data = result_validity.GetData();
			}

			SljitNativeVectorInput native_input;
			native_input.source_data_array = source_data.data();
			native_input.source_sel_array = source_sel.data();
			native_input.source_validity_array = source_validity.data();
			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.result_data = NativeIntegerResultData(result, SljitNativeIntegerKind::DECIMAL64);
			native_input.result_validity = result_validity_data;
			native_input.overflow_message = expr.overflow_message.c_str();
			native_input.count = count;
			expr.function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE) {
			UnifiedVectorFormat source_format;
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			auto source_data = UnifiedVectorFormat::GetData<int64_t>(source_format);
			auto result_data = FlatVector::GetDataMutable<double>(result);
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
				auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
				if (!source_format.validity.RowIsValid(source_idx)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				result_data[row_idx] = static_cast<double>(source_data[source_idx]) / plan.double_constant;
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP) {
			UnifiedVectorFormat source_format;
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			auto source_data = UnifiedVectorFormat::GetData<hugeint_t>(source_format);
			auto result_data = FlatVector::GetDataMutable<hugeint_t>(result);
			hugeint_t scale_factor(plan.constant);
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
				auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
				if (!source_format.validity.RowIsValid(source_idx)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				hugeint_t scaled;
				if (!Hugeint::TryMultiply(source_data[source_idx], scale_factor, scaled)) {
					throw OutOfRangeException("Overflow in DECIMAL128 scale-up");
				}
				result_data[row_idx] = scaled;
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
		         plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE ||
		         plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
		         plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
		         plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		auto has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
		                        (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
		                         plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE);
		if (has_right_source) {
			auto right_source_index = plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE
			                              ? plan.guard_source_index
			                              : plan.right_source_index;
			D_ASSERT(right_source_index < input.ColumnCount());
			input.data[right_source_index].ToUnifiedFormat(right_source_format);
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
			result_validity.SetAllValid(count);
			result_validity_data = result_validity.GetData();
		}

		SljitNativeVectorInput native_input;
		if (plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK) {
			native_input.source_data = nullptr;
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
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
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
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
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			native_input.right_source_data =
			    NativeIntegerSourceData(right_source_format, SljitNativeIntegerKind::INT64);
		} else {
			native_input.right_source_data = nullptr;
		}
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
		native_input.right_source_sel =
		    has_right_source && right_source_format.sel ? right_source_format.sel->data() : nullptr;
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_validity = has_right_source ? right_source_format.validity.GetData() : nullptr;
		native_input.constants = plan.constants.data();
		native_input.constant =
		    plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ? plan.guard_constant : plan.constant;
		native_input.double_constant = plan.double_constant;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.signed_integer_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
			native_input.result_data = FlatVector::GetDataMutable(result);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			native_input.result_data = FlatVector::GetDataMutable(result);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			native_input.result_data = NativeIntegerResultData(result, plan.integer_kind);
		} else {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
		}
		native_input.result_vector =
		    plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ? &result : nullptr;
		native_input.result_validity = result_validity_data;
		native_input.true_sel = nullptr;
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.overflow_message =
		    plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
		            plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
		        ? expr.overflow_message.c_str()
		        : nullptr;
		native_input.error_message = plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE
		                                 ? plan.error_message.c_str()
		                                 : nullptr;
		native_input.overflow_value = 0;
		native_input.string_decompress_source_size = plan.string_decompress_source_size;
		native_input.active_source_index = 0;
		native_input.active_result_index = 0;
		native_input.count = count;
		native_input.has_error = false;
		expr.function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		FlatVector::SetSize(result, count_t(count));
	}

	void ExecutePrimitiveAggregatePayloadUpdate(SljitExecutableRegionExpression &payload,
	                                            SljitNativeAggregateUpdateFunction function,
	                                            const ExecutionPrimitiveAggregateUpdateLane &lane, DataChunk &input,
	                                            const SelectionVector *execute_sel, idx_t count) {
		if (!function) {
			throw InternalException("SLJIT aggregate primitive payload update is missing generated code");
		}
		if (!lane.ready || lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64 || !lane.sum_int64_value ||
		    !lane.state_is_set || !lane.row_count) {
			auto blocker = lane.blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane.blocker;
			throw InternalException("SLJIT aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
		auto &plan = payload.plan;
		if (plan.return_type.InternalType() != lane.payload_type) {
			throw InternalException("SLJIT aggregate primitive payload type mismatch");
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.count = count;
		native_input.aggregate_int64_value = lane.sum_int64_value;
		native_input.aggregate_state_is_set = lane.state_is_set;
		native_input.aggregate_row_count = lane.row_count;
		native_input.has_error = false;

		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		vector<UnifiedVectorFormat> expression_tree_formats;
		vector<const_data_ptr_t> expression_tree_source_data;
		vector<const sel_t *> expression_tree_source_sel;
		vector<const validity_t *> expression_tree_source_validity;

		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive reference source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.constant = plan.constant;
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
			native_input.right_source_sel = SljitNormalizedSourceSelectionData(right_source_format);
			native_input.right_source_validity = right_source_format.validity.GetData();
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
			if (!plan.expression_tree) {
				throw InternalException("SLJIT aggregate primitive expression-tree payload is missing IR");
			}
			expression_tree_formats.resize(plan.expression_tree_source_indices.size());
			expression_tree_source_data.resize(plan.expression_tree_source_indices.size());
			expression_tree_source_sel.resize(plan.expression_tree_source_indices.size());
			expression_tree_source_validity.resize(plan.expression_tree_source_indices.size());
			for (idx_t source_idx = 0; source_idx < plan.expression_tree_source_indices.size(); source_idx++) {
				auto input_index = plan.expression_tree_source_indices[source_idx];
				if (input_index >= input.ColumnCount()) {
					throw InternalException("SLJIT aggregate primitive expression-tree source is out of range");
				}
				input.data[input_index].ToUnifiedFormat(expression_tree_formats[source_idx]);
				expression_tree_source_data[source_idx] =
				    NativeIntegerSourceData(expression_tree_formats[source_idx], SljitNativeIntegerKind::DECIMAL64);
				expression_tree_source_sel[source_idx] =
				    SljitNormalizedSourceSelectionData(expression_tree_formats[source_idx]);
				expression_tree_source_validity[source_idx] = expression_tree_formats[source_idx].validity.GetData();
			}
			native_input.source_data_array = expression_tree_source_data.data();
			native_input.source_sel_array = expression_tree_source_sel.data();
			native_input.source_validity_array = expression_tree_source_validity.data();
			break;
		default:
			throw InternalException("SLJIT aggregate primitive payload has no runtime input adapter");
		}

		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	void ExecuteGroupedPrimitiveAggregatePayloadUpdate(SljitExecutableRegionExpression &payload,
	                                                   SljitNativeAggregateUpdateFunction function,
	                                                   Vector &state_addresses, DataChunk &input,
	                                                   const SelectionVector *execute_sel, idx_t count) {
		if (!function) {
			throw InternalException("SLJIT grouped aggregate primitive payload update is missing generated code");
		}
		auto &plan = payload.plan;
		if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			throw InternalException("SLJIT grouped aggregate primitive update only supports reference payloads");
		}
		if (plan.source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT grouped aggregate primitive reference source is out of range");
		}

		UnifiedVectorFormat source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.count = count;
		native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
		native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
		native_input.source_validity = source_format.validity.GetData();
		native_input.aggregate_state_addresses = FlatVector::GetData<data_ptr_t>(state_addresses);
		native_input.has_error = false;

		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	idx_t ApplyNativeHashJoinResidualPredicate(SljitExecutableRegionOp &op, const ExecutionHashJoinProbeBinding &probe,
	                                           DataChunk &input, Vector &row_pointers, SelectionVector &match_selection,
	                                           idx_t count, DataChunk *residual_chunk,
	                                           SelectionVector *residual_selection,
	                                           SelectionVector *compact_match_selection, Vector *compact_row_pointers) {
		if (!op.hash_join_probe.plan.residual_predicate) {
			return count;
		}
		if (!residual_chunk || !residual_selection || !compact_match_selection || !compact_row_pointers) {
			throw InternalException("SLJIT native hash join residual predicate requires residual scratch state");
		}
		auto &residual_filter = op.hash_join_probe.residual_filter;
		if (residual_filter.plan.kind != SljitNativeRegionExpressionKind::PREDICATE ||
		    !residual_filter.predicate_select_function) {
			throw InternalException("SLJIT native hash join residual predicate reached runtime without generated code");
		}
		if (count == 0) {
			return 0;
		}

		residual_chunk->Reset();
		ExecutionMaterializeHashJoinResidualSources(probe, input, row_pointers, match_selection, count,
		                                            *residual_chunk);

		vector<UnifiedVectorFormat> formats;
		vector<const_data_ptr_t> source_data;
		vector<const sel_t *> source_sel;
		vector<const validity_t *> source_validity;
		auto &filter_plan = residual_filter.plan;
		PrepareSljitPredicateSources(residual_chunk, NativePredicateRequiresInput(*filter_plan.predicate), formats,
		                             source_data, source_sel, source_validity);

		SljitNativePredicateInput native_input;
		native_input.source_data = source_data.data();
		native_input.source_sel = source_sel.data();
		native_input.source_validity = source_validity.data();
		native_input.execute_sel = nullptr;
		native_input.result_data = nullptr;
		native_input.result_validity = nullptr;
		native_input.true_sel = residual_selection->data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.count = count;
		residual_filter.predicate_select_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}

		const auto selected_count = native_input.selected_count;
		compact_row_pointers->SetVectorType(VectorType::FLAT_VECTOR);
		auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
		auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(*compact_row_pointers);
		for (idx_t out_idx = 0; out_idx < selected_count; out_idx++) {
			auto dense_idx = residual_selection->get_index(out_idx);
			if (dense_idx >= count) {
				throw InternalException("SLJIT native hash join residual predicate selected row out of range");
			}
			compact_match_selection->set_index(out_idx, match_selection.get_index(dense_idx));
			compact_row_pointer_data[out_idx] = row_pointer_data[dense_idx];
		}
		for (idx_t out_idx = 0; out_idx < selected_count; out_idx++) {
			match_selection.set_index(out_idx, compact_match_selection->get_index(out_idx));
			row_pointer_data[out_idx] = compact_row_pointer_data[out_idx];
		}
		FlatVector::SetSize(row_pointers, count_t(selected_count));
		return selected_count;
	}

	static void MarkHashJoinBuildMatchesAfterResidual(const SljitNativeHashJoinProbePlan &plan, Vector &row_pointers,
	                                                  idx_t count) {
		if (!plan.mark_build_match_after_residual) {
			return;
		}
		auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_pointer = row_pointer_data[row_idx];
			if (!row_pointer) {
				throw InternalException("SLJIT native hash join residual build match has no row pointer");
			}
			row_pointer[plan.found_match_offset] = 1;
		}
	}

	ExecutionOperatorBindResult
	ExecuteNativeHashJoinProbe(ExecutionOperatorRuntime &native_runtime, SljitExecutableRegionOp &op, DataChunk &input,
	                           DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection,
	                           Vector &row_pointers, DataChunk *residual_chunk, SelectionVector *residual_selection,
	                           SelectionVector *compact_match_selection, Vector *compact_row_pointers,
	                           SljitHashJoinProbeDrainState &state, string &deferred_reason) {
		if (!op.hash_join_probe.function) {
			throw InternalException("SLJIT native hash join probe reached runtime without generated code");
		}
		ExecutionOperatorBinding binding;
		auto bind_result =
		    native_runtime.BindOperator(op.operator_index, input, op.hash_join_probe.plan.operator_info, binding);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			deferred_reason = binding.blocker.empty() ? "native-operator-runtime-deferred" : binding.blocker;
			return bind_result;
		}
		if (bind_result != ExecutionOperatorBindResult::READY) {
			auto blocker = binding.blocker.empty() ? "unknown" : binding.blocker;
			throw InternalException("SLJIT native hash join probe operator binding failed: %s", blocker);
		}
		if (!binding.ready || !binding.hash_join_probe.ready) {
			throw InternalException("SLJIT native hash join probe received an incomplete operator binding");
		}
		auto &probe = binding.hash_join_probe;
		if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::NONE ||
		    probe.output_mode != op.hash_join_probe.plan.output_mode) {
			throw InternalException("SLJIT native hash join probe output mode mismatch");
		}
		const bool left_probe_output = IsLeftHashJoinProbeOutputMode(op.hash_join_probe.plan.output_mode);
		if (left_probe_output) {
			InitializeLeftHashJoinProbeState(state, input.size());
			if (state.finished && !state.left_unmatched_emitted) {
				MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state);
				return ExecutionOperatorBindResult::READY;
			}
		}
		if (probe.probe_key_input_indices.size() != op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe key binding count mismatch");
		}
		if (probe.empty_build_side) {
			state.finished = true;
			switch (op.hash_join_probe.plan.output_mode) {
			case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD:
				MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state);
				break;
			case ExecutionHashJoinProbeOutputMode::MARK_PROBE:
				for (idx_t input_idx = 0; input_idx < input.size(); input_idx++) {
					match_selection.set_index(input_idx, 0);
				}
				ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, input.size(), output);
				break;
			case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
			case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
			case ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
				output.Reset();
				break;
			default:
				throw InternalException("SLJIT native hash join probe cannot execute empty build side for output mode");
			}
			return ExecutionOperatorBindResult::READY;
		}
		if (probe.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
			if (!op.hash_join_probe.plan.perfect_hash_probe || !op.hash_join_probe.perfect_function) {
				throw InternalException("SLJIT native hash join probe received a perfect layout without perfect code");
			}
			if (op.hash_join_probe.plan.residual_predicate) {
				throw InternalException("SLJIT native perfect hash join probe does not support residual predicates");
			}
			if (op.hash_join_probe.plan.keys.size() != 1 || probe.probe_key_input_indices.size() != 1) {
				throw InternalException("SLJIT native perfect hash join probe requires one key");
			}
			auto &perfect_layout = probe.perfect_layout;
			if (!perfect_layout.ready || perfect_layout.build_capacity == 0) {
				throw InternalException("SLJIT native perfect hash join probe received an incomplete layout");
			}
			auto &key = op.hash_join_probe.plan.keys[0];
			if (probe.probe_key_input_indices[0] != key.key_input_index) {
				throw InternalException("SLJIT native perfect hash join probe key binding mismatch");
			}
			if (input.data[key.key_input_index].GetType().InternalType() != perfect_layout.key_physical_type) {
				throw InternalException("SLJIT native perfect hash join probe key type mismatch");
			}

			UnifiedVectorFormat source_format;
			input.data[key.key_input_index].ToUnifiedFormat(source_format);
			auto source_data = NativeHashJoinKeySourceData(source_format, key.key_kind);
			auto source_sel = SljitNormalizedSourceSelectionData(source_format);
			auto source_validity = source_format.validity.CannotHaveNull() ? nullptr : source_format.validity.GetData();
			const_data_ptr_t source_data_array[] = {source_data};
			const sel_t *source_sel_array[] = {source_sel};
			const validity_t *source_validity_array[] = {source_validity};

			SljitNativeHashJoinProbeInput native_input;
			native_input.source_data = source_data_array;
			native_input.source_sel = source_sel_array;
			native_input.source_validity = source_validity_array;
			native_input.count = input.size();
			native_input.match_sel = match_selection.data();
			native_input.build_sel = build_selection.data();
			native_input.perfect_min = perfect_layout.build_min;
			native_input.perfect_max = perfect_layout.build_max;
			native_input.perfect_validity = perfect_layout.build_validity;
			native_input.selected_count = 0;
			native_input.input_offset = state.input_offset;
			native_input.finished = false;

			op.hash_join_probe.perfect_function(&native_input);
			state.input_offset = native_input.input_offset;
			state.resume_row_pointer = nullptr;
			state.finished = native_input.finished;
			if (native_input.selected_count == 0) {
				output.Reset();
				return ExecutionOperatorBindResult::READY;
			}
			ExecutionMaterializePerfectHashJoinProbe(probe, input, match_selection, build_selection,
			                                         native_input.selected_count, output);
			return ExecutionOperatorBindResult::READY;
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
			if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY &&
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
		native_input.output_capacity = STANDARD_VECTOR_SIZE;
		native_input.selected_count = 0;
		native_input.input_offset = state.input_offset;
		native_input.resume_row_pointer = state.resume_row_pointer;
		native_input.finished = false;

		op.hash_join_probe.function(&native_input);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = native_input.resume_row_pointer;
		state.finished = native_input.finished;
		auto selected_count = ApplyNativeHashJoinResidualPredicate(
		    op, probe, input, row_pointers, match_selection, native_input.selected_count, residual_chunk,
		    residual_selection, compact_match_selection, compact_row_pointers);
		if (left_probe_output && selected_count != 0) {
			MarkLeftHashJoinProbeMatches(state, match_selection, selected_count);
		}
		MarkHashJoinBuildMatchesAfterResidual(op.hash_join_probe.plan, row_pointers, selected_count);
		if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		if (selected_count == 0) {
			if (left_probe_output && state.finished) {
				MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state);
				return ExecutionOperatorBindResult::READY;
			}
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		FlatVector::SetSize(row_pointers, count_t(selected_count));
		ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, selected_count, output);
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionOperatorBindResult
	ExecuteNativeNestedLoopJoinProbe(ExecutionOperatorRuntime &native_runtime, SljitExecutableRegionOp &op,
	                                 DataChunk &input, DataChunk &left_condition, DataChunk &output,
	                                 SelectionVector &left_selection, SelectionVector &right_selection,
	                                 SljitNestedLoopJoinProbeDrainState &state, string &deferred_reason) {
		if (!op.nested_loop_join_probe.function) {
			throw InternalException("SLJIT native nested loop join probe reached runtime without generated code");
		}
		if (op.nested_loop_join_probe.plan.conditions.size() != 1 ||
		    op.nested_loop_join_probe.lhs_conditions.size() != 1) {
			throw InternalException("SLJIT native nested loop join probe requires one executable condition");
		}

		ExecutionOperatorBinding binding;
		auto bind_result = native_runtime.BindOperator(op.operator_index, input,
		                                               op.nested_loop_join_probe.plan.operator_info, binding);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			deferred_reason = binding.blocker.empty() ? "native-operator-runtime-deferred" : binding.blocker;
			return bind_result;
		}
		if (bind_result != ExecutionOperatorBindResult::READY) {
			auto blocker = binding.blocker.empty() ? "unknown" : binding.blocker;
			throw InternalException("SLJIT native nested loop join probe operator binding failed: %s", blocker);
		}
		if (!binding.ready || !binding.nested_loop_join_probe.ready) {
			throw InternalException("SLJIT native nested loop join probe received an incomplete operator binding");
		}
		auto &probe = binding.nested_loop_join_probe;
		if (probe.join_type != ExecutionRegionJoinType::INNER ||
		    op.nested_loop_join_probe.plan.join_type != ExecutionRegionJoinType::INNER) {
			throw InternalException("SLJIT native nested loop join probe currently requires INNER join");
		}
		if (probe.empty_build_side) {
			state.finished = true;
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		if (!state.lhs_materialized) {
			left_condition.Reset();
			for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_probe.lhs_conditions.size();
			     condition_idx++) {
				ExecuteProjectionExpression(op.nested_loop_join_probe.lhs_conditions[condition_idx], input,
				                            left_condition.data[condition_idx], nullptr, input.size());
			}
			left_condition.SetChildCardinality(input.size());
			state.lhs_materialized = true;
		}

		if (!state.started) {
			state.started = true;
			state.left_offset = 0;
			state.right_offset = 0;
			state.right_chunk_finished = false;
			if (!ExecutionNestedLoopJoinProbeStartInput(probe)) {
				state.finished = true;
				output.Reset();
				return ExecutionOperatorBindResult::READY;
			}
		}
		while (!state.finished) {
			if (state.right_chunk_finished) {
				state.left_offset = 0;
				state.right_offset = 0;
				state.right_chunk_finished = false;
				if (!ExecutionNestedLoopJoinProbeAdvanceRight(probe)) {
					state.finished = true;
					output.Reset();
					return ExecutionOperatorBindResult::READY;
				}
			}
			if (!probe.right_condition || probe.right_condition->size() == 0) {
				state.right_chunk_finished = true;
				continue;
			}

			auto &condition_plan = op.nested_loop_join_probe.plan.conditions[0];
			if (left_condition.ColumnCount() != 1 || probe.right_condition->ColumnCount() != 1) {
				throw InternalException("SLJIT native nested loop join probe condition width mismatch");
			}
			UnifiedVectorFormat left_format;
			UnifiedVectorFormat right_format;
			left_condition.data[0].ToUnifiedFormat(left_format);
			probe.right_condition->data[0].ToUnifiedFormat(right_format);

			SljitNativeNestedLoopJoinProbeInput native_input;
			native_input.left_data = NativeNestedLoopJoinConditionSourceData(left_format, condition_plan.value_kind);
			native_input.right_data = NativeNestedLoopJoinConditionSourceData(right_format, condition_plan.value_kind);
			native_input.left_sel = SljitNormalizedSourceSelectionData(left_format);
			native_input.right_sel = SljitNormalizedSourceSelectionData(right_format);
			native_input.left_validity =
			    left_format.validity.CannotHaveNull() ? nullptr : left_format.validity.GetData();
			native_input.right_validity =
			    right_format.validity.CannotHaveNull() ? nullptr : right_format.validity.GetData();
			native_input.left_count = left_condition.size();
			native_input.right_count = probe.right_condition->size();
			native_input.left_offset = state.left_offset;
			native_input.right_offset = state.right_offset;
			native_input.output_capacity = STANDARD_VECTOR_SIZE;
			native_input.left_match_sel = left_selection.data();
			native_input.right_match_sel = right_selection.data();
			native_input.selected_count = 0;
			native_input.right_chunk_finished = false;

			op.nested_loop_join_probe.function(&native_input);
			state.left_offset = native_input.left_offset;
			state.right_offset = native_input.right_offset;
			state.right_chunk_finished = native_input.right_chunk_finished;
			if (probe.left_tuple) {
				*probe.left_tuple = state.left_offset;
			}
			if (probe.right_tuple) {
				*probe.right_tuple = state.right_offset;
			}
			if (native_input.selected_count == 0) {
				continue;
			}
			ExecutionMaterializeNestedLoopJoinProbe(probe, input, left_selection, right_selection,
			                                        native_input.selected_count, output);
			return ExecutionOperatorBindResult::READY;
		}
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionSinkBinding &BindNativeSink(ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	                                     idx_t op_idx, DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                                     const char *blocker_prefix, const char *error_prefix) {
		auto &binding = scratch.SinkBinding(op_idx);
		if (scratch.HasSinkBinding(op_idx)) {
			return binding;
		}
		if (!native_runtime.BindSink(input, sink_info, binding)) {
			auto blocker = binding.blocker.empty() ? string(blocker_prefix) : binding.blocker;
			throw InternalException("%s binding failed: %s", error_prefix, blocker);
		}
		scratch.MarkSinkBindingReady(op_idx);
		return binding;
	}

	SinkResultType ExecuteNativeHashJoinBuild(ExecutionOperatorRuntime &native_runtime,
	                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                          SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding =
		    BindNativeSink(native_runtime, scratch, op_idx, input, op.hash_join_build.plan.sink_info,
		                   "hash-join-build-native-runtime-binding-failed", "SLJIT native hash join build sink");
		if (!binding.ready || !binding.hash_join_build.ready) {
			throw InternalException("SLJIT native hash join build sink binding did not return a ready build state");
		}
		return ExecutionSinkHashJoinBuild(binding.hash_join_build, input);
	}

	SinkResultType ExecuteNativeNestedLoopJoinBuild(ExecutionOperatorRuntime &native_runtime,
	                                                SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                SljitExecutableRegionOp &op, DataChunk &input,
	                                                DataChunk &right_condition) {
		if (op.nested_loop_join_build.rhs_conditions.size() != right_condition.ColumnCount()) {
			throw InternalException("SLJIT nested loop join build condition expression count mismatch");
		}
		right_condition.Reset();
		for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_build.rhs_conditions.size();
		     condition_idx++) {
			ExecuteProjectionExpression(op.nested_loop_join_build.rhs_conditions[condition_idx], input,
			                            right_condition.data[condition_idx], nullptr, input.size());
		}
		right_condition.SetChildCardinality(input.size());

		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.nested_loop_join_build.plan.sink_info,
		                               "nested-loop-join-build-native-runtime-binding-failed",
		                               "SLJIT native nested loop join build sink");
		if (!binding.ready || !binding.nested_loop_join_build.ready) {
			throw InternalException(
			    "SLJIT native nested loop join build sink binding did not return a ready build state");
		}
		return ExecutionSinkNestedLoopJoinBuild(binding.nested_loop_join_build, input, right_condition);
	}

	SinkResultType ExecuteNativeAppendSink(ExecutionOperatorRuntime &native_runtime,
	                                       SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                       SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.append_sink.plan.sink_info,
		                               "append-sink-runtime-binding-failed", "SLJIT append sink");
		if (!binding.ready || !binding.append_sink.ready) {
			throw InternalException("SLJIT append sink binding did not return a ready append state");
		}
		return ExecutionSinkAppend(binding.append_sink, input);
	}

	SinkResultType ExecuteNativeDelimJoinSink(ExecutionOperatorRuntime &native_runtime,
	                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                          SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.delim_join_sink.plan.sink_info,
		                               "delim-join-sink-runtime-binding-failed", "SLJIT delimiter join sink");
		if (!binding.ready || !binding.delim_join_sink.ready) {
			throw InternalException("SLJIT delimiter join sink binding did not return a ready delimiter state");
		}
		return ExecutionSinkDelimJoin(binding.delim_join_sink, input);
	}

	SinkResultType ExecuteNativeAggregateUpdate(ExecutionRegionRuntime &runtime,
	                                            ExecutionOperatorRuntime &native_runtime,
	                                            SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                            SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.aggregate_update.plan.sink_info,
		                               "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
		if (!binding.ready || !binding.aggregate_update.ready) {
			throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
		}
		if (op.aggregate_update.plan.use_primitive_payloads) {
			if (op.aggregate_update.plan.use_grouped_state_addresses) {
				auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
				if (aggregates.size() != op.aggregate_update.payloads.size() ||
				    aggregates.size() != op.aggregate_update.payload_update_functions.size()) {
					throw InternalException("SLJIT grouped aggregate primitive payload count mismatch");
				}
				auto &state_addresses = scratch.AggregateStateAddresses(op_idx);
				auto lookup_start = SljitRegionStageStart(runtime);
				ExecutionFindOrCreateAggregateStates(binding.aggregate_update, input,
				                                      op.aggregate_update.plan.group_input_indices, state_addresses);
				RecordSljitRegionStageRuntime(runtime, SljitRegionStageName(op_idx, op.kind) + ".lookup",
				                              lookup_start);
				for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
					auto reduce_start = SljitRegionStageStart(runtime);
					ExecuteGroupedPrimitiveAggregatePayloadUpdate(op.aggregate_update.payloads[payload_idx],
					                                              op.aggregate_update.payload_update_functions[payload_idx],
					                                              state_addresses, input, nullptr, input.size());
					RecordSljitRegionStageRuntime(runtime, SljitRegionStageName(op_idx, op.kind) + ".reduce",
					                              reduce_start);
				}
				auto finish_start = SljitRegionStageStart(runtime);
				ExecutionFinishAggregateUpdate(binding.aggregate_update);
				RecordSljitRegionStageRuntime(runtime, SljitRegionStageName(op_idx, op.kind) + ".finish",
				                              finish_start);
				return SinkResultType::NEED_MORE_INPUT;
			}
			auto &primitive = binding.aggregate_update.primitive;
			if (!primitive.ready) {
				auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
				throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
			}
			auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
			if (aggregates.size() != op.aggregate_update.payloads.size() ||
			    aggregates.size() != op.aggregate_update.payload_update_functions.size()) {
				throw InternalException("SLJIT aggregate primitive payload count mismatch");
			}
			for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
				auto &aggregate = aggregates[payload_idx];
				auto lane = primitive.FindLane(aggregate.aggregate_index);
				if (!lane) {
					throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
					                        static_cast<unsigned long long>(aggregate.aggregate_index));
				}
				ExecutePrimitiveAggregatePayloadUpdate(op.aggregate_update.payloads[payload_idx],
				                                       op.aggregate_update.payload_update_functions[payload_idx], *lane,
				                                       input, nullptr, input.size());
			}
			return SinkResultType::NEED_MORE_INPUT;
		}
		return ExecutionSinkAggregateUpdate(binding.aggregate_update, input);
	}

	SinkResultType ExecuteNativeOrderSink(ExecutionOperatorRuntime &native_runtime,
	                                      SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                      SljitExecutableRegionOp &op, DataChunk &input, DataChunk &order_keys,
	                                      DataChunk &payload) {
		if (op.order_sink.order_keys.size() != order_keys.ColumnCount()) {
			throw InternalException("SLJIT ordered sink key expression count mismatch");
		}
		order_keys.Reset();
		for (idx_t key_idx = 0; key_idx < op.order_sink.order_keys.size(); key_idx++) {
			ExecuteProjectionExpression(op.order_sink.order_keys[key_idx], input, order_keys.data[key_idx], nullptr,
			                            input.size());
		}
		order_keys.SetChildCardinality(input.size());

		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.order_sink.plan.sink_info,
		                               "ordered-sink-runtime-binding-failed", "SLJIT ordered sink");
		if (!binding.ready || !binding.ordered_sink.ready) {
			throw InternalException("SLJIT ordered sink binding did not return a ready ordered sink state");
		}
		payload.Reset();
		payload.Reference(input);
		return ExecutionSinkOrdered(binding.ordered_sink, order_keys, payload);
	}

private:
	string backend_name;
	vector<SljitExecutableRegionOp> ops;
	ExecutionRegionABI abi;
};

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                                SljitExecutableRegion &&region,
                                                                ExecutionRegionABI abi) {
	(void)context;
	return make_uniq<SljitNativeRegionKernel>(std::move(backend_name), std::move(region.ops), abi);
}

} // namespace duckdb

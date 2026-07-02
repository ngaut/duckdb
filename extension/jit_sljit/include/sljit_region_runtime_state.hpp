//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"
#include "sljit_join_drain_state.hpp"
#include "sljit_native_runtime.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_execution_scratch_helpers.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitRegionExecutionScratch {
	SljitRegionExecutionScratch(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops) {
		const auto op_count = ops.size();
		temporary_chunks.resize(op_count);
		filter_selections.resize(op_count);
		operator_bindings.resize(op_count);
		operator_binding_ready.resize(op_count);
		sink_bindings.resize(op_count);
		sink_binding_ready.resize(op_count);
		hash_join_build_selections.resize(op_count);
		hash_join_row_pointers.resize(op_count);
		hash_join_residual_chunks.resize(op_count);
		hash_join_residual_selections.resize(op_count);
		hash_join_residual_match_selections.resize(op_count);
		hash_join_residual_row_pointers.resize(op_count);
		hash_join_sources.resize(op_count);
		hash_join_build_source_chunks.resize(op_count);
		hash_join_build_hash_values.resize(op_count);
		nested_loop_left_condition_chunks.resize(op_count);
		nested_loop_left_selections.resize(op_count);
		nested_loop_right_selections.resize(op_count);
		nested_loop_condition_chunks.resize(op_count);
		order_key_chunks.resize(op_count);
		order_payload_chunks.resize(op_count);
		aggregate_scratch.Resize(op_count);
		projection_adapter_scratch.resize(op_count);
		expression_adapter_scratch.resize(op_count);
		for (idx_t op_idx = 0; op_idx < op_count; op_idx++) {
			auto &op = ops[op_idx];
			InitializeExpressionAdapterScratch(op_idx, op);
			InitializeOperatorScratch(allocator, op_idx, op);
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
		return CheckedScratchPtr(temporary_chunks, op_idx, "SLJIT full pipeline transform has no stage scratch chunk");
	}

	Vector &AggregateStateAddresses(idx_t op_idx) {
		return aggregate_scratch.StateAddresses(op_idx);
	}

	SljitAggregatePayloadAdapterScratch &AggregatePayloadScratch(idx_t op_idx) {
		return aggregate_scratch.PayloadScratch(op_idx);
	}

	DataChunk &AggregatePreaggregatedGroups(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedGroups(op_idx);
	}

	Vector &AggregatePreaggregatedRowPointers(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedRowPointers(op_idx);
	}

	SljitPreaggregatedPrimitiveAggregateScratch &AggregatePreaggregateScratch(idx_t op_idx) {
		return aggregate_scratch.PreaggregateScratch(op_idx);
	}

	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &
	AggregatePayloadLanes(idx_t op_idx, const vector<ExecutionRegionAggregateInput> &aggregates,
	                      const ExecutionPrimitiveAggregateUpdateBinding &primitive) {
		return aggregate_scratch.PayloadLanes(op_idx, aggregates, primitive);
	}

	bool DirectNewAggregateUpdateDisabled(idx_t op_idx) const {
		return aggregate_scratch.DirectNewDisabled(op_idx);
	}

	void RecordDirectNewAggregateUpdateResult(idx_t op_idx, bool updated) {
		aggregate_scratch.RecordDirectNewResult(op_idx, updated);
	}

	bool DirectAppendNewAggregateUpdateDisabled(idx_t op_idx) const {
		return aggregate_scratch.DirectAppendNewDisabled(op_idx);
	}

	void RecordDirectAppendNewAggregateUpdateResult(idx_t op_idx, bool updated) {
		aggregate_scratch.RecordDirectAppendNewResult(op_idx, updated);
	}

	SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t expression_idx) {
		if (op_idx >= expression_adapter_scratch.size() ||
		    expression_idx >= expression_adapter_scratch[op_idx].size()) {
			throw InternalException("SLJIT expression has no adapter scratch");
		}
		return expression_adapter_scratch[op_idx][expression_idx];
	}

	SljitProjectionAdapterScratch &ProjectionScratch(idx_t op_idx) {
		return CheckedScratchSlot(projection_adapter_scratch, op_idx, "SLJIT projection has no adapter scratch");
	}

	SelectionVector &FilterSelection(idx_t op_idx) {
		return CheckedScratchPtr(filter_selections, op_idx, "SLJIT full pipeline transform has no selection scratch");
	}

	bool HasSinkBinding(idx_t op_idx) const {
		return op_idx < sink_binding_ready.size() && sink_binding_ready[op_idx];
	}

	ExecutionSinkBinding &SinkBinding(idx_t op_idx) {
		return CheckedScratchSlot(sink_bindings, op_idx, "SLJIT full pipeline sink has no binding scratch");
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
		return CheckedScratchSlot(operator_bindings, op_idx, "SLJIT full pipeline operator has no binding scratch");
	}

	void MarkOperatorBindingReady(idx_t op_idx) {
		if (op_idx >= operator_binding_ready.size()) {
			throw InternalException("SLJIT full pipeline operator has no binding-ready scratch");
		}
		operator_binding_ready[op_idx] = true;
	}

	Vector &HashJoinRowPointers(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_row_pointers, op_idx,
		                         "SLJIT full pipeline hash join probe has no row-pointer scratch");
	}

	SelectionVector &HashJoinBuildSelection(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_build_selections, op_idx,
		                         "SLJIT full pipeline hash join probe has no build-selection scratch");
	}

	DataChunk &HashJoinBuildSourceChunk(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_build_source_chunks, op_idx,
		                         "SLJIT full pipeline hash join build has no source-chunk scratch");
	}

	Vector &HashJoinBuildHashValues(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_build_hash_values, op_idx,
		                         "SLJIT full pipeline hash join build has no hash-value scratch");
	}

	DataChunk &HashJoinResidualChunk(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_residual_chunks, op_idx,
		                         "SLJIT full pipeline hash join probe has no residual chunk scratch");
	}

	SelectionVector &HashJoinResidualSelection(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_residual_selections, op_idx,
		                         "SLJIT full pipeline hash join probe has no residual selection scratch");
	}

	SelectionVector &HashJoinResidualMatchSelection(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_residual_match_selections, op_idx,
		                         "SLJIT full pipeline hash join probe has no residual match selection scratch");
	}

	Vector &HashJoinResidualRowPointers(idx_t op_idx) {
		return CheckedScratchPtr(hash_join_residual_row_pointers, op_idx,
		                         "SLJIT full pipeline hash join probe has no residual row-pointer scratch");
	}

	SljitHashJoinProbeSourceScratch &HashJoinSources(idx_t op_idx) {
		return CheckedScratchSlot(hash_join_sources, op_idx, "SLJIT hash join probe has no source scratch slot");
	}

	DataChunk &NestedLoopLeftConditionChunk(idx_t op_idx) {
		return CheckedScratchPtr(nested_loop_left_condition_chunks, op_idx,
		                         "SLJIT nested loop join probe has no left condition scratch chunk");
	}

	SelectionVector &NestedLoopLeftSelection(idx_t op_idx) {
		return CheckedScratchPtr(nested_loop_left_selections, op_idx,
		                         "SLJIT nested loop join probe has no left selection scratch");
	}

	SelectionVector &NestedLoopRightSelection(idx_t op_idx) {
		return CheckedScratchPtr(nested_loop_right_selections, op_idx,
		                         "SLJIT nested loop join probe has no right selection scratch");
	}

	DataChunk &NestedLoopConditionChunk(idx_t op_idx) {
		return CheckedScratchPtr(nested_loop_condition_chunks, op_idx,
		                         "SLJIT nested loop join build has no condition scratch chunk");
	}

	DataChunk &OrderKeyChunk(idx_t op_idx) {
		return CheckedScratchPtr(order_key_chunks, op_idx, "SLJIT ordered sink has no order-key scratch chunk");
	}

	DataChunk &OrderPayloadChunk(idx_t op_idx) {
		return CheckedScratchPtr(order_payload_chunks, op_idx, "SLJIT ordered sink has no payload scratch chunk");
	}

	vector<unique_ptr<DataChunk>> temporary_chunks;
	vector<unique_ptr<SelectionVector>> filter_selections;
	vector<ExecutionOperatorBinding> operator_bindings;
	vector<bool> operator_binding_ready;
	vector<ExecutionSinkBinding> sink_bindings;
	vector<bool> sink_binding_ready;
	vector<unique_ptr<SelectionVector>> hash_join_build_selections;
	vector<unique_ptr<Vector>> hash_join_row_pointers;
	vector<unique_ptr<DataChunk>> hash_join_build_source_chunks;
	vector<unique_ptr<Vector>> hash_join_build_hash_values;
	vector<unique_ptr<DataChunk>> hash_join_residual_chunks;
	vector<unique_ptr<SelectionVector>> hash_join_residual_selections;
	vector<unique_ptr<SelectionVector>> hash_join_residual_match_selections;
	vector<unique_ptr<Vector>> hash_join_residual_row_pointers;
	vector<SljitHashJoinProbeSourceScratch> hash_join_sources;
	vector<unique_ptr<DataChunk>> nested_loop_left_condition_chunks;
	vector<unique_ptr<SelectionVector>> nested_loop_left_selections;
	vector<unique_ptr<SelectionVector>> nested_loop_right_selections;
	vector<unique_ptr<DataChunk>> nested_loop_condition_chunks;
	vector<unique_ptr<DataChunk>> order_key_chunks;
	vector<unique_ptr<DataChunk>> order_payload_chunks;
	SljitAggregateUpdateScratchState aggregate_scratch;
	vector<SljitProjectionAdapterScratch> projection_adapter_scratch;
	vector<vector<SljitExpressionAdapterScratch>> expression_adapter_scratch;
	DirectAppendReservation direct_append_reservation;

private:
	template <class T>
	static T &CheckedScratchPtr(vector<unique_ptr<T>> &scratch, idx_t op_idx, const char *message) {
		if (op_idx >= scratch.size() || !scratch[op_idx]) {
			throw InternalException(message);
		}
		return *scratch[op_idx];
	}

	template <class T>
	static T &CheckedScratchSlot(vector<T> &scratch, idx_t op_idx, const char *message) {
		if (op_idx >= scratch.size()) {
			throw InternalException(message);
		}
		return scratch[op_idx];
	}

	void InitializeOperatorScratch(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			InitializeHashJoinProbeScratch(allocator, op_idx, op);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			hash_join_build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			hash_join_build_source_chunks[op_idx] = make_uniq<DataChunk>();
			hash_join_build_hash_values[op_idx] = make_uniq<Vector>(LogicalType::HASH);
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			nested_loop_left_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			nested_loop_right_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			InitializeChunk(allocator, op.nested_loop_join_probe.plan.condition_types,
			                nested_loop_left_condition_chunks[op_idx]);
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			InitializeChunk(allocator, op.nested_loop_join_build.plan.condition_types,
			                nested_loop_condition_chunks[op_idx]);
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			InitializeChunk(allocator, op.order_sink.plan.key_types, order_key_chunks[op_idx]);
			InitializeChunk(allocator, op.order_sink.plan.input_types, order_payload_chunks[op_idx]);
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			aggregate_scratch.Initialize(allocator, op_idx, op);
			break;
		default:
			break;
		}
	}

	static void InitializeChunk(Allocator &allocator, const vector<LogicalType> &types, unique_ptr<DataChunk> &target) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, types);
		target = std::move(chunk);
	}

	void InitializeHashJoinProbeScratch(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		hash_join_build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		hash_join_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		auto key_count = op.hash_join_probe.plan.keys.size();
		hash_join_sources[op_idx].Resize(key_count);
		if (!op.hash_join_probe.plan.residual_predicate) {
			return;
		}
		hash_join_residual_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		hash_join_residual_match_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		hash_join_residual_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
		InitializeChunk(allocator, op.hash_join_probe.plan.residual_source_types, hash_join_residual_chunks[op_idx]);
	}

	void InitializeExpressionAdapterScratch(idx_t op_idx, const SljitExecutableRegionOp &op) {
		idx_t expression_count = 0;
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			expression_count = 1;
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			expression_count = op.projections.size();
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			expression_count = op.hash_join_probe.plan.residual_predicate ? 1 : 0;
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			expression_count = op.nested_loop_join_probe.lhs_conditions.size();
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			expression_count = op.nested_loop_join_build.rhs_conditions.size();
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			expression_count = op.order_sink.order_keys.size();
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			expression_count = op.aggregate_update.payloads.size();
			break;
		default:
			break;
		}
		expression_adapter_scratch[op_idx].resize(expression_count);
	}

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

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_binding_execution_scratch.hpp"
#include "sljit_hash_join_execution_scratch.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_join_drain_state.hpp"
#include "sljit_nested_loop_execution_scratch.hpp"
#include "sljit_native_runtime.hpp"
#include "sljit_order_execution_scratch.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_execution_scratch_helpers.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitSelectedProjectionCache {
	const SljitExecutableRegionOp *semantic_projection = nullptr;
	vector<idx_t> source_map;
	vector<LogicalType> input_types;
	unique_ptr<SljitExecutableRegionOp> mapped_projection;
};

struct SljitRegionExecutionScratch {
	SljitRegionExecutionScratch(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops) {
		const auto op_count = ops.size();
		temporary_chunks.resize(op_count);
		filter_selections.resize(op_count);
		binding_scratch.Resize(op_count);
		hash_join_scratch.Resize(op_count);
		nested_loop_scratch.Resize(op_count);
		order_scratch.Resize(op_count);
		aggregate_scratch.Resize(op_count);
		projection_adapter_scratch.resize(op_count);
		expression_adapter_scratch.resize(op_count);
		selected_projection_caches.resize(op_count);
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
		return SljitCheckedScratchPtr(temporary_chunks, op_idx,
		                              "SLJIT full pipeline transform has no stage scratch chunk");
	}

	Vector &AggregateStateAddresses(idx_t op_idx) {
		return aggregate_scratch.StateAddresses(op_idx);
	}

	SljitAggregatePayloadAdapterScratch &AggregatePayloadScratch(idx_t op_idx) {
		return aggregate_scratch.PayloadScratch(op_idx);
	}

	SljitBoundGroupedPrimitiveAggregateUpdate &AggregateBoundGroupedUpdate(idx_t op_idx) {
		return aggregate_scratch.BoundGroupedUpdate(op_idx);
	}

	DataChunk &AggregatePreaggregatedGroups(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedGroups(op_idx);
	}

	DataChunk &AggregatePreaggregatedGroupSlice(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedGroupSlice(op_idx);
	}

	Vector &AggregatePreaggregatedRowPointers(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedRowPointers(op_idx);
	}

	SljitPreaggregatedGroupContinuationState &AggregatePreaggregatedGroupContinuation(idx_t op_idx) {
		return aggregate_scratch.PreaggregatedGroupContinuation(op_idx);
	}

	SljitPreaggregatedPrimitiveAggregateScratch &AggregatePreaggregateScratch(idx_t op_idx) {
		return aggregate_scratch.PreaggregateScratch(op_idx);
	}

	SljitPreaggregatedPrimitiveAggregateScratch &AggregatePreaggregateScratchSlice(idx_t op_idx) {
		return aggregate_scratch.PreaggregateScratchSlice(op_idx);
	}

	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &
	AggregatePayloadLanes(idx_t op_idx, const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
	                      const ExecutionPrimitiveAggregateUpdateBinding &primitive) {
		return aggregate_scratch.PayloadLanes(op_idx, payload_descriptors, primitive);
	}

	const vector<SljitGroupedReductionLaneBinding> &
	GroupedReductionLanes(idx_t op_idx, const ExecutionRegionAggregateContract &contract,
	                      const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
	                      const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		return aggregate_scratch.GroupedReductionLanes(op_idx, contract, payload_descriptors, payload_lanes);
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

	bool RowPointerPreaggregateDisabled(idx_t op_idx) const {
		return aggregate_scratch.RowPointerPreaggregateDisabled(op_idx);
	}

	void RecordRowPointerPreaggregateResult(idx_t op_idx, bool updated) {
		aggregate_scratch.RecordRowPointerPreaggregateResult(op_idx, updated);
	}

	SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t expression_idx) {
		if (op_idx >= expression_adapter_scratch.size() ||
		    expression_idx >= expression_adapter_scratch[op_idx].size()) {
			throw InternalException("SLJIT expression has no adapter scratch");
		}
		return expression_adapter_scratch[op_idx][expression_idx];
	}

	SljitProjectionAdapterScratch &ProjectionScratch(idx_t op_idx) {
		return SljitCheckedScratchSlot(projection_adapter_scratch, op_idx, "SLJIT projection has no adapter scratch");
	}

	SljitSelectedProjectionCache &SelectedProjectionCache(idx_t op_idx) {
		return SljitCheckedScratchSlot(selected_projection_caches, op_idx,
		                               "SLJIT selected projection has no cache slot");
	}

	SelectionVector &FilterSelection(idx_t op_idx) {
		return SljitCheckedScratchPtr(filter_selections, op_idx,
		                              "SLJIT full pipeline transform has no selection scratch");
	}

	bool HasSinkBinding(idx_t op_idx) const {
		return binding_scratch.HasSink(op_idx);
	}

	ExecutionSinkBinding &SinkBinding(idx_t op_idx) {
		return binding_scratch.Sink(op_idx);
	}

	void MarkSinkBindingReady(idx_t op_idx) {
		binding_scratch.MarkSinkReady(op_idx);
	}

	bool HasOperatorBinding(idx_t op_idx) const {
		return binding_scratch.HasOperator(op_idx);
	}

	ExecutionOperatorBinding &OperatorBinding(idx_t op_idx) {
		return binding_scratch.Operator(op_idx);
	}

	void MarkOperatorBindingReady(idx_t op_idx) {
		binding_scratch.MarkOperatorReady(op_idx);
	}

	Vector &HashJoinRowPointers(idx_t op_idx) {
		return hash_join_scratch.RowPointers(op_idx);
	}

	SelectionVector &HashJoinBuildSelection(idx_t op_idx) {
		return hash_join_scratch.BuildSelection(op_idx);
	}

	DataChunk &HashJoinBuildSourceChunk(idx_t op_idx) {
		return hash_join_scratch.BuildSourceChunk(op_idx);
	}

	Vector &HashJoinBuildHashValues(idx_t op_idx) {
		return hash_join_scratch.BuildHashValues(op_idx);
	}

	DataChunk &HashJoinResidualChunk(idx_t op_idx) {
		return hash_join_scratch.ResidualChunk(op_idx);
	}

	SelectionVector &HashJoinResidualSelection(idx_t op_idx) {
		return hash_join_scratch.ResidualSelection(op_idx);
	}

	SelectionVector &HashJoinResidualMatchSelection(idx_t op_idx) {
		return hash_join_scratch.ResidualMatchSelection(op_idx);
	}

	Vector &HashJoinResidualRowPointers(idx_t op_idx) {
		return hash_join_scratch.ResidualRowPointers(op_idx);
	}

	SljitHashJoinProbeSourceScratch &HashJoinSources(idx_t op_idx) {
		return hash_join_scratch.Sources(op_idx);
	}

	DataChunk &NestedLoopLeftConditionChunk(idx_t op_idx) {
		return nested_loop_scratch.LeftConditionChunk(op_idx);
	}

	SelectionVector &NestedLoopLeftSelection(idx_t op_idx) {
		return nested_loop_scratch.LeftSelection(op_idx);
	}

	SelectionVector &NestedLoopRightSelection(idx_t op_idx) {
		return nested_loop_scratch.RightSelection(op_idx);
	}

	DataChunk &NestedLoopConditionChunk(idx_t op_idx) {
		return nested_loop_scratch.ConditionChunk(op_idx);
	}

	DataChunk &OrderKeyChunk(idx_t op_idx) {
		return order_scratch.KeyChunk(op_idx);
	}

	DataChunk &OrderPayloadChunk(idx_t op_idx) {
		return order_scratch.PayloadChunk(op_idx);
	}

	vector<unique_ptr<DataChunk>> temporary_chunks;
	vector<unique_ptr<SelectionVector>> filter_selections;
	SljitBindingExecutionScratch binding_scratch;
	SljitHashJoinExecutionScratch hash_join_scratch;
	SljitNestedLoopJoinExecutionScratch nested_loop_scratch;
	SljitOrderExecutionScratch order_scratch;
	SljitAggregateUpdateScratchState aggregate_scratch;
	vector<SljitProjectionAdapterScratch> projection_adapter_scratch;
	vector<vector<SljitExpressionAdapterScratch>> expression_adapter_scratch;
	vector<SljitSelectedProjectionCache> selected_projection_caches;
	DirectAppendReservation direct_append_reservation;

	void InitializeOperatorScratch(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			hash_join_scratch.InitializeProbe(allocator, op_idx, op);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			hash_join_scratch.InitializeBuild(op_idx);
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			nested_loop_scratch.InitializeProbe(allocator, op_idx, op);
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			nested_loop_scratch.InitializeBuild(allocator, op_idx, op);
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			order_scratch.InitializeSink(allocator, op_idx, op);
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			aggregate_scratch.Initialize(allocator, op_idx, op);
			break;
		default:
			break;
		}
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
		SljitInitializeScratchChunk(allocator, ops[op_idx].output_types, temporary_chunks[op_idx]);
	}
};

} // namespace duckdb

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

//! The groups a hash aggregate without aggregates already handed to the DuckDB sink on this thread, as a bitmap over
//! its bounded group-key domain, and the scratch that selects the rows bringing a new group
struct SljitDistinctGroupScratch {
	explicit SljitDistinctGroupScratch(idx_t domain_size)
	    : seen((domain_size + 63) / 64, 0), new_rows(STANDARD_VECTOR_SIZE) {
	}

	vector<uint64_t> seen;
	SelectionVector new_rows;
	idx_t domain_index[STANDARD_VECTOR_SIZE];
	bool in_domain[STANDARD_VECTOR_SIZE];
	DataChunk new_input;
};

//! The query's exact-filter proof for a probe's binding slot (null without one): the region source applied that
//! build side's exact membership filter to the probe key
static inline const shared_ptr<ExecutionRuntimeFilterIdentity> &
SljitExactSourceFilterIdentity(const vector<shared_ptr<ExecutionRuntimeFilterIdentity>> &bindings, idx_t binding) {
	static const shared_ptr<ExecutionRuntimeFilterIdentity> no_identity;
	return binding < bindings.size() ? bindings[binding] : no_identity;
}

struct SljitRegionExecutionScratch {
	SljitRegionExecutionScratch(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops,
	                            vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings_p)
	    : exact_source_filter_bindings(std::move(exact_source_filter_bindings_p)) {
		const auto op_count = ops.size();
		temporary_chunks.resize(op_count);
		distinct_groups.resize(op_count);
		filter_selections.resize(op_count);
		binding_scratch.Resize(op_count);
		hash_join_scratch.Resize(op_count);
		order_scratch.Resize(op_count);
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

	void ResetDistinctGroups(idx_t op_idx) {
		D_ASSERT(op_idx < distinct_groups.size());
		distinct_groups[op_idx].reset();
	}

	SljitDistinctGroupScratch &DistinctGroups(idx_t op_idx, idx_t domain_size) {
		D_ASSERT(op_idx < distinct_groups.size());
		auto &result = distinct_groups[op_idx];
		if (!result) {
			result = make_uniq<SljitDistinctGroupScratch>(domain_size);
		}
		return *result;
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

	const shared_ptr<ExecutionRuntimeFilterIdentity> &ExactSourceFilterIdentity(idx_t binding) const {
		return SljitExactSourceFilterIdentity(exact_source_filter_bindings, binding);
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

	bool HasOutputOperatorBinding(idx_t op_idx) const {
		return binding_scratch.HasOutputOperator(op_idx);
	}

	ExecutionOperatorBinding &OutputOperatorBinding(idx_t op_idx) {
		return binding_scratch.OutputOperator(op_idx);
	}

	void MarkOutputOperatorBindingReady(idx_t op_idx) {
		binding_scratch.MarkOutputOperatorReady(op_idx);
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

	DataChunk &OrderKeyChunk(idx_t op_idx) {
		return order_scratch.KeyChunk(op_idx);
	}

	DataChunk &OrderPayloadChunk(idx_t op_idx) {
		return order_scratch.PayloadChunk(op_idx);
	}

	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings;
	vector<unique_ptr<DataChunk>> temporary_chunks;
	vector<unique_ptr<SljitDistinctGroupScratch>> distinct_groups;
	vector<unique_ptr<SelectionVector>> filter_selections;
	SljitBindingExecutionScratch binding_scratch;
	SljitHashJoinExecutionScratch hash_join_scratch;
	SljitOrderExecutionScratch order_scratch;
	SljitAggregateUpdateScratchState aggregate_scratch;
	vector<SljitProjectionAdapterScratch> projection_adapter_scratch;
	vector<vector<SljitExpressionAdapterScratch>> expression_adapter_scratch;

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
		case SljitNativeRegionOpKind::ORDER_SINK:
		case SljitNativeRegionOpKind::APPEND_SINK:
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

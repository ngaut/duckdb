//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_count_star_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_fixed_preaggregation.hpp"
#include "sljit_aggregate_count_star_string_preaggregation.hpp"
#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_count_star_projection_input.hpp"
#include "sljit_projected_aggregate_input_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

struct SljitCountStarGroupedAggregateUpdateDescriptor {
	optional_ptr<ExecutionGroupedAggregateStateAddressBinding> grouped_state;
	optional_ptr<const ExecutionPrimitiveAggregateUpdateLane> lane;

	bool Ready() const {
		return grouped_state && lane;
	}
};

static bool TryBuildCountStarGroupedAggregateUpdateDescriptor(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &bind_groups,
    ExecutionSinkBinding &binding, SljitCountStarGroupedAggregateUpdateDescriptor &descriptor) {
	descriptor = SljitCountStarGroupedAggregateUpdateDescriptor();
	if (op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.UsesPrimitivePayloads() || !op.aggregate_update.plan.use_grouped_state_addresses) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != bind_groups.ColumnCount() || sink_info.aggregates.size() != 1 ||
	    op.aggregate_update.payloads.size() != 1 || op.aggregate_update.payload_descriptors.size() != 1) {
		return false;
	}
	auto &aggregate = sink_info.aggregates[0];
	auto &payload_descriptor = op.aggregate_update.payload_descriptors[0];
	if (aggregate.child_count != 0 || payload_descriptor.primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return false;
	}
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
	    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state) {
		return false;
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.aggregate_update.payload_descriptors,
	                                                    binding.aggregate_update.primitive);
	if (payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	auto lane = payload_lanes[0];
	SljitGroupedReductionLaneBinding reduction_lane;
	if (!SljitTryBindGroupedReductionLane(sink_info.aggregate_contract, payload_descriptor, lane, reduction_lane) ||
	    reduction_lane.descriptor->primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return false;
	}

	descriptor.grouped_state = &binding.aggregate_update.grouped_state;
	descriptor.lane = lane;
	return true;
}

static bool SljitTryPrepareCountStarGroupedAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                           ExecutionOperatorRuntime &native_runtime,
                                                           SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                           SljitExecutableRegionOp &op, DataChunk &bind_groups,
                                                           SljitCountStarGroupedAggregateUpdateDescriptor &descriptor) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &binding =
	    SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, bind_groups, sink_info,
	                                "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	return TryBuildCountStarGroupedAggregateUpdateDescriptor(scratch, op_idx, op, bind_groups, binding, descriptor);
}

static bool TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &compact_groups,
    const vector<int64_t> &count_deltas, SljitCountStarGroupedAggregateUpdateDescriptor &descriptor,
    idx_t preaggregated_row_count, bool defer_grouped_finish) {
	if (compact_groups.size() == 0 || count_deltas.size() < compact_groups.size() || !descriptor.Ready()) {
		return false;
	}
	auto &grouped_state = *descriptor.grouped_state;
	auto lane = descriptor.lane.get();

	SljitPreaggregatedCountStarUpdateState update_state;
	update_state.lane = lane;
	update_state.counts = count_deltas.data();
	const bool finish = !defer_grouped_finish;
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_preaggregated_count_star_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateGroupKeysWithSelectedStateAddresses(
		        compact_groups, op.aggregate_update.plan.sink_info, ExecuteSljitPreaggregatedCountStarUpdate,
		        &update_state, recorder);
	    });
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_preaggregated_count_star_update"
	                                          : "direct_preaggregated_count_star_update_miss",
	                                  stage_start);
	if (!updated) {
		return false;
	}
	RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "direct_preaggregated_count_star_update",
	                                             preaggregated_row_count);
	return true;
}

class SljitGroupedCountStarPreaggregationRuntimeState {
public:
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy != SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION) {
			return false;
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
		group_types.clear();
		group_types.reserve(sink_info.groups.size());
		for (auto &group : sink_info.groups) {
			group_types.push_back(group.type);
		}
		preaggregated_groups.Initialize(runtime.GetAllocator(), group_types);
		pending_preaggregated_groups.Initialize(runtime.GetAllocator(), group_types);
		pending_row_groups.Initialize(runtime.GetAllocator(), group_types);
		pending_preaggregated_count_deltas.clear();
		pending_row_count_deltas.clear();
		pending_preaggregated_row_count = 0;
		pending_row_count = 0;
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			projected_count_star_group_projection = primitive.projected_count_star_group_projection;
			if (!projected_count_star_group_projection ||
			    projected_count_star_group_projection->kind != SljitNativeRegionOpKind::PROJECTION ||
			    !SljitCountStarProjectionInputSupported(*projected_count_star_group_projection)) {
				return false;
			}
		}
		SljitTryPrepareCountStarGroupedAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch,
		                                               primitive.aggregate_idx, aggregate_op, preaggregated_groups,
		                                               count_star_update);
		return count_star_update.Ready();
	}

	bool Execute(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (TryExecutePreaggregatedCountStar(runtime, ops, scratch, primitive, aggregate_op, input)) {
			processed_batches++;
			return false;
		}
		throw InternalException("SLJIT grouped count-star preaggregation update failed");
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, primitive.aggregate_idx,
		                                        ops[primitive.aggregate_idx])) {
			throw InternalException("SLJIT grouped count-star pending preaggregation flush failed");
		}
		if (!FlushPendingRowCountStar(runtime, scratch, primitive.aggregate_idx, ops[primitive.aggregate_idx])) {
			throw InternalException("SLJIT grouped count-star pending row-delta flush failed");
		}
		return false;
	}

private:
	bool TryExecutePreaggregatedCountStar(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                      SljitRegionExecutionScratch &scratch,
	                                      const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                      SljitExecutableRegionOp &aggregate_op, const SljitRuntimeBatchView &input) {
		if (!count_star_update.Ready()) {
			throw InternalException("SLJIT grouped count-star preaggregation descriptor is not prepared");
		}
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return TryExecuteProjectedPreaggregatedCountStar(runtime, ops, scratch, primitive, aggregate_op, input);
		}
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT grouped count-star update");
		return TryExecuteMaterializedPreaggregatedCountStar(runtime, scratch, primitive.aggregate_idx, aggregate_op,
		                                                    input_chunk);
	}

	bool TryExecuteMaterializedPreaggregatedCountStar(ExecutionRegionRuntime &runtime,
	                                                  SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
	                                                  SljitExecutableRegionOp &aggregate_op, DataChunk &input) {
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (TryPreaggregateFixedWidthCountStarGroups(input, preaggregated_groups, preaggregated_count_deltas)) {
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "primitive_grouped_preaggregate_count_star_groups", preaggregate_stage_start);
			if (!AccumulatePreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
			                                      preaggregated_count_deltas, input.size())) {
				return false;
			}
			RecordSljitRegionMaterializationElisionPath(
			    runtime, aggregate_op.kind, "primitive_grouped_preaggregated_count_star_update", input.size());
			return true;
		}
		row_count_deltas.assign(input.size(), 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "primitive_grouped_count_star_row_deltas", preaggregate_stage_start);
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (!AccumulateRowCountStar(runtime, scratch, aggregate_idx, aggregate_op, input, row_count_deltas,
		                            input.size())) {
			return false;
		}
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
		                                            "primitive_grouped_count_star_row_update", input.size());
		return true;
	}

	static bool SljitTryPreaggregateProjectedCountStarGroups(SljitExecutableRegionOp &projection_op, DataChunk &input,
	                                                         const SelectionVector *execute_sel, idx_t count,
	                                                         DataChunk &compact_groups, vector<int64_t> &count_deltas) {
		if (TryPreaggregateProjectedCountStarGroups(projection_op, input, execute_sel, count, compact_groups,
		                                            count_deltas)) {
			return true;
		}
		return TryPreaggregateProjectedFixedWidthCountStarGroups(projection_op, input, execute_sel, count,
		                                                         compact_groups, count_deltas);
	}

	bool TryExecuteProjectedPreaggregatedCountStar(ExecutionRegionRuntime &runtime,
	                                               vector<SljitExecutableRegionOp> &ops,
	                                               SljitRegionExecutionScratch &scratch,
	                                               const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                               SljitExecutableRegionOp &aggregate_op,
	                                               const SljitRuntimeBatchView &input) {
		const auto aggregate_idx = primitive.aggregate_idx;
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (!projected_count_star_group_projection) {
			throw InternalException("SLJIT projected count-star group projection is not prepared");
		}
		SljitPreparedProjectedAggregateInput prepared;
		if (!SljitPrepareProjectedAggregateInput(
		        runtime, ops, scratch, primitive, input, *projected_count_star_group_projection,
		        projected_count_star_selected_hash_join_input, "SLJIT projected grouped count-star update", prepared)) {
			return true;
		}
		if (SljitTryPreaggregateProjectedCountStarGroups(*prepared.projection_op, *prepared.source_chunk,
		                                                 prepared.selection, prepared.count, preaggregated_groups,
		                                                 preaggregated_count_deltas)) {
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "primitive_projected_preaggregate_count_star_groups",
			                              preaggregate_stage_start);
			if (!AccumulatePreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
			                                      preaggregated_count_deltas, prepared.count)) {
				return false;
			}
			RecordSljitRegionMaterializationElisionPath(
			    runtime, aggregate_op.kind, "primitive_projected_preaggregated_count_star_update", prepared.count);
			return true;
		}

		preaggregated_groups.Reset();
		SljitExecuteProjectionExpression(prepared.projection_op->projections[0], *prepared.source_chunk,
		                                 preaggregated_groups.data[0], prepared.selection, prepared.count,
		                                 projected_count_star_group_scratch);
		preaggregated_groups.SetChildCardinality(prepared.count);
		row_count_deltas.assign(prepared.count, 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind, "count_star_projected_row_groups",
		                              preaggregate_stage_start);
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (!AccumulateRowCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
		                            row_count_deltas, prepared.count)) {
			return false;
		}
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind, "count_star_projected_row_update",
		                                            prepared.count);
		return true;
	}

	bool AccumulatePreaggregatedCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                      idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
	                                      DataChunk &compact_groups, const vector<int64_t> &count_deltas,
	                                      idx_t represented_row_count) {
		if (!FlushPendingRowCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (TryAccumulatePreaggregatedCountStarGroups(compact_groups, count_deltas, pending_preaggregated_groups,
		                                              pending_preaggregated_count_deltas)) {
			pending_preaggregated_row_count += represented_row_count;
			return true;
		}
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (TryAccumulatePreaggregatedCountStarGroups(compact_groups, count_deltas, pending_preaggregated_groups,
		                                              pending_preaggregated_count_deltas)) {
			pending_preaggregated_row_count += represented_row_count;
			return true;
		}
		return TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		    runtime, aggregate_idx, aggregate_op, compact_groups, count_deltas, count_star_update,
		    represented_row_count, true);
	}

	bool FlushPendingPreaggregatedCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                        idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op) {
		if (pending_preaggregated_groups.ColumnCount() == 0 || pending_preaggregated_groups.size() == 0) {
			return true;
		}
		const auto represented_row_count = pending_preaggregated_row_count;
		auto updated = TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		    runtime, aggregate_idx, aggregate_op, pending_preaggregated_groups, pending_preaggregated_count_deltas,
		    count_star_update, represented_row_count, true);
		pending_preaggregated_groups.Reset();
		pending_preaggregated_count_deltas.clear();
		pending_preaggregated_row_count = 0;
		if (updated) {
			RecordSljitRegionMaterializationElisionPath(
			    runtime, aggregate_op.kind, "primitive_pending_preaggregated_count_star_flush", represented_row_count);
		}
		return updated;
	}

	bool AccumulateRowCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                            idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op, DataChunk &groups,
	                            const vector<int64_t> &count_deltas, idx_t represented_row_count) {
		if (groups.size() == 0) {
			return true;
		}
		if (count_deltas.size() < groups.size()) {
			return false;
		}
		if (pending_row_groups.ColumnCount() == 0) {
			return TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
			    runtime, aggregate_idx, aggregate_op, groups, count_deltas, count_star_update, represented_row_count,
			    true);
		}
		if (pending_row_groups.size() + groups.size() > STANDARD_VECTOR_SIZE &&
		    !FlushPendingRowCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		pending_row_groups.Append(groups);
		for (idx_t row_idx = 0; row_idx < groups.size(); row_idx++) {
			pending_row_count_deltas.push_back(count_deltas[row_idx]);
		}
		pending_row_count += represented_row_count;
		if (pending_row_groups.size() < STANDARD_VECTOR_SIZE) {
			return true;
		}
		return FlushPendingRowCountStar(runtime, scratch, aggregate_idx, aggregate_op);
	}

	bool FlushPendingRowCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                              idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op) {
		if (pending_row_groups.ColumnCount() == 0 || pending_row_groups.size() == 0) {
			return true;
		}
		const auto represented_row_count = pending_row_count;
		auto updated = TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		    runtime, aggregate_idx, aggregate_op, pending_row_groups, pending_row_count_deltas, count_star_update,
		    represented_row_count, true);
		pending_row_groups.Reset();
		pending_row_count_deltas.clear();
		pending_row_count = 0;
		if (updated) {
			RecordSljitRegionMaterializationElisionPath(
			    runtime, aggregate_op.kind, "primitive_pending_count_star_row_flush", represented_row_count);
		}
		return updated;
	}

private:
	vector<LogicalType> group_types;
	DataChunk preaggregated_groups;
	vector<int64_t> preaggregated_count_deltas;
	vector<int64_t> row_count_deltas;
	DataChunk pending_preaggregated_groups;
	vector<int64_t> pending_preaggregated_count_deltas;
	idx_t pending_preaggregated_row_count = 0;
	DataChunk pending_row_groups;
	vector<int64_t> pending_row_count_deltas;
	idx_t pending_row_count = 0;
	shared_ptr<SljitExecutableRegionOp> projected_count_star_group_projection;
	SljitExpressionAdapterScratch projected_count_star_group_scratch;
	SljitDataChunkBatch projected_count_star_selected_hash_join_input;
	SljitCountStarGroupedAggregateUpdateDescriptor count_star_update;
};

} // namespace duckdb

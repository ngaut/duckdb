//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pending_preaggregated_group_batch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_preaggregation_common_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"

#include <cstring>

namespace duckdb {

struct SljitPendingPreaggregatedPrimitiveGroupBatch {
	idx_t Count() const {
		return count;
	}

	bool Empty() const {
		return count == 0;
	}

	void Reset() {
		groups.Reset();
		scratch.Prepare(lanes, STANDARD_VECTOR_SIZE);
		represented_row_count = 0;
		count = 0;
	}

	SljitDataChunkBatch groups;
	SljitPreaggregatedPrimitiveAggregateScratch scratch;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes;
	idx_t represented_row_count = 0;
	idx_t count = 0;
};

static bool SljitPreaggregatedPrimitiveSingleGroupKeysMatch(DataChunk &left, idx_t left_idx, DataChunk &right,
                                                            idx_t right_idx) {
	if (left.ColumnCount() != 1 || right.ColumnCount() != 1 || left_idx >= left.size() || right_idx >= right.size() ||
	    left.data[0].GetType() != right.data[0].GetType()) {
		return false;
	}
	const auto physical_type = left.data[0].GetType().InternalType();
	if (!TypeIsConstantSize(physical_type)) {
		return false;
	}
	const auto type_size = GetTypeIdSize(physical_type);
	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;
	left.data[0].ToUnifiedFormat(left_format);
	right.data[0].ToUnifiedFormat(right_format);
	const auto left_source_idx = left_format.sel->get_index(left_idx);
	const auto right_source_idx = right_format.sel->get_index(right_idx);
	if (!left_format.validity.RowIsValid(left_source_idx) || !right_format.validity.RowIsValid(right_source_idx)) {
		return false;
	}
	return memcmp(left_format.data + left_source_idx * type_size, right_format.data + right_source_idx * type_size,
	              type_size) == 0;
}

static bool
SljitAppendPreaggregatedPrimitiveGroupRange(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                            idx_t op_idx, SljitExecutableRegionOp &op,
                                            SljitPendingPreaggregatedPrimitiveGroupBatch &pending, DataChunk &groups,
                                            SljitPreaggregatedPrimitiveAggregateScratch &source_scratch,
                                            const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                            idx_t offset, idx_t count, idx_t represented_row_count) {
	if (count == 0) {
		return true;
	}
	if (!pending.groups.Initialized()) {
		pending.groups.EnsureFromChunk(runtime.GetAllocator(), groups);
		pending.lanes = payload_lanes;
		pending.scratch.Prepare(payload_lanes, STANDARD_VECTOR_SIZE);
	}
	if (pending.groups.chunk.ColumnCount() != groups.ColumnCount() || pending.lanes.size() != payload_lanes.size()) {
		return false;
	}
	if (!CanSlicePreaggregatedPrimitiveScratch(source_scratch, payload_lanes, offset, count) ||
	    pending.scratch.payloads.size() != payload_lanes.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		if (pending.scratch.payloads[payload_idx].kind != source_scratch.payloads[payload_idx].kind) {
			return false;
		}
	}
	DataChunk *source_groups = &groups;
	auto &group_slice = scratch.AggregatePreaggregatedGroupSlice(op_idx);
	if (offset != 0 || count != groups.size()) {
		group_slice.Reset();
		group_slice.Slice(groups, offset, offset + count);
		source_groups = &group_slice;
	}
	auto append_start = SljitRegionStageStart(runtime);
	if (!SljitTryFastAppendFixedAllValid(pending.groups.chunk, *source_groups)) {
		pending.groups.chunk.Append(*source_groups);
	}
	pending.count += count;
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "pending_preaggregated_group_append", append_start);
	if (!AppendPreaggregatedPrimitiveScratch(source_scratch, payload_lanes, offset, count, pending.scratch)) {
		return false;
	}
	pending.represented_row_count += represented_row_count;
	return true;
}

static bool SljitFlushPendingPreaggregatedPrimitiveGroups(ExecutionRegionRuntime &runtime,
                                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                          SljitExecutableRegionOp &op,
                                                          SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                          ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                                          optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (pending.Empty()) {
		return true;
	}
	pending.groups.chunk.SetChildCardinality(pending.Count());
	if (pending.Count() != pending.scratch.group_row_counts.size()) {
		return false;
	}
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state, pending.Count());
	if (!TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(
	        runtime, scratch, op_idx, op, pending.groups.chunk, pending.scratch, pending.lanes, grouped_state,
	        pending.represented_row_count, true, deferred_grouped_finish)) {
		return false;
	}
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "pending_preaggregated_grouped_update_flush",
	                                            pending.represented_row_count);
	pending.Reset();
	return true;
}

static bool SljitBufferPreaggregatedPrimitiveGroups(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &groups, SljitPreaggregatedPrimitiveAggregateScratch &source_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, bool finish,
    optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (groups.size() == 0 || groups.ColumnCount() != 1 ||
	    !TypeIsConstantSize(groups.data[0].GetType().InternalType()) ||
	    !CanSlicePreaggregatedPrimitiveScratch(source_scratch, payload_lanes, 0, groups.size())) {
		return false;
	}
	if (pending.Empty()) {
		pending.lanes = payload_lanes;
		pending.scratch.Prepare(payload_lanes, STANDARD_VECTOR_SIZE);
	}
	idx_t offset = 0;
	if (!pending.Empty() &&
	    SljitPreaggregatedPrimitiveSingleGroupKeysMatch(pending.groups.chunk, pending.Count() - 1, groups, 0)) {
		idx_t merged_row_count;
		if (!PreaggregatedPrimitiveRepresentedRowCount(source_scratch, 0, 1, merged_row_count) ||
		    !MergePreaggregatedPrimitiveScratchGroup(source_scratch, payload_lanes, 0, pending.scratch,
		                                             pending.Count() - 1)) {
			return false;
		}
		pending.represented_row_count += merged_row_count;
		offset = 1;
		RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
	}
	const auto append_count = groups.size() - offset;
	if (append_count > 0 && pending.Count() + append_count > STANDARD_VECTOR_SIZE) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			return false;
		}
	}
	if (append_count > 0) {
		idx_t append_row_count;
		if (!PreaggregatedPrimitiveRepresentedRowCount(source_scratch, offset, append_count, append_row_count) ||
		    !SljitAppendPreaggregatedPrimitiveGroupRange(runtime, scratch, op_idx, op, pending, groups, source_scratch,
		                                                 payload_lanes, offset, append_count, append_row_count)) {
			return false;
		}
	}
	if (finish || pending.Count() == STANDARD_VECTOR_SIZE) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_runtime_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_fixed_preaggregation.hpp"
#include "sljit_aggregate_count_star_string_preaggregation.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_direct_aggregate_update_runtime_state.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_projected_grouped_aggregate_update_primitive.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

namespace duckdb {

struct SljitGroupedAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE ||
		    primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return direct_payload_update.Prepare(primitive);
		}
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
		deferred_grouped_finish = false;
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

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION:
			return ExecuteCountStarPreaggregation(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return direct_payload_update.Execute(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			break;
		}
		throw InternalException("SLJIT grouped aggregate update primitive has an unknown strategy");
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE ||
		    primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return direct_payload_update.Flush(runtime, scratch, primitive);
		}
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, primitive.aggregate_idx,
		                                        ops[primitive.aggregate_idx])) {
			throw InternalException("SLJIT grouped count-star pending preaggregation flush failed");
		}
		if (!FlushPendingRowCountStar(runtime, scratch, primitive.aggregate_idx, ops[primitive.aggregate_idx])) {
			throw InternalException("SLJIT grouped count-star pending row-delta flush failed");
		}
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx, deferred_grouped_finish);
		return false;
	}

private:
	bool ExecuteCountStarPreaggregation(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                    const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                    const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		(void)result;
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (TryExecutePreaggregatedCountStar(runtime, ops, scratch, primitive, aggregate_op, input)) {
			processed_batches++;
			return false;
		}
		throw InternalException("SLJIT grouped count-star preaggregation update failed");
	}

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
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
			                             "primitive_grouped_preaggregated_count_star_update", input.size());
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
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_grouped_count_star_row_update",
		                             input.size());
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
		DataChunk *input_chunk;
		unique_ptr<SljitExecutableRegionOp> mapped_projection;
		optional_ptr<SljitExecutableRegionOp> projection_op = projected_count_star_group_projection.get();
		const SelectionVector *selection = input.selection;
		idx_t count = input.count;
		if (input.HasHashJoinSelection()) {
			if (!SljitTryPrepareSelectedHashJoinProjectionInput(runtime, scratch, ops, primitive.final_projection_idx,
			                                                    *projected_count_star_group_projection, input,
			                                                    projected_count_star_selected_hash_join_input,
			                                                    input_chunk, mapped_projection, projection_op)) {
				return true;
			}
			selection = nullptr;
			count = input_chunk->size();
		} else {
			input_chunk = &SljitBindRuntimeBatchInput(input, "SLJIT projected grouped count-star update");
		}
		if (SljitTryPreaggregateProjectedCountStarGroups(*projection_op, *input_chunk, selection, count,
		                                                 preaggregated_groups, preaggregated_count_deltas)) {
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "primitive_projected_preaggregate_count_star_groups",
			                              preaggregate_stage_start);
			if (!AccumulatePreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
			                                      preaggregated_count_deltas, count)) {
				return false;
			}
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
			                             "primitive_projected_preaggregated_count_star_update", count);
			return true;
		}

		preaggregated_groups.Reset();
		SljitExecuteProjectionExpression(projection_op->projections[0], *input_chunk, preaggregated_groups.data[0],
		                                 selection, count, projected_count_star_group_scratch);
		preaggregated_groups.SetChildCardinality(count);
		row_count_deltas.assign(count, 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "primitive_projected_count_star_row_groups", preaggregate_stage_start);
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (!AccumulateRowCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
		                            row_count_deltas, count)) {
			return false;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_projected_count_star_row_update", count);
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
			RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind,
			                                         "pending_preaggregated_count_star_groups",
			                                         pending_preaggregated_groups.size());
			return true;
		}
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (TryAccumulatePreaggregatedCountStarGroups(compact_groups, count_deltas, pending_preaggregated_groups,
		                                              pending_preaggregated_count_deltas)) {
			pending_preaggregated_row_count += represented_row_count;
			RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind,
			                                         "pending_preaggregated_count_star_groups",
			                                         pending_preaggregated_groups.size());
			return true;
		}
		return TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		    runtime, scratch, aggregate_idx, aggregate_op, compact_groups, count_deltas, count_star_update,
		    represented_row_count, true, &deferred_grouped_finish);
	}

	bool FlushPendingPreaggregatedCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                        idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op) {
		if (pending_preaggregated_groups.ColumnCount() == 0 || pending_preaggregated_groups.size() == 0) {
			return true;
		}
		const auto represented_row_count = pending_preaggregated_row_count;
		auto updated = TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		    runtime, scratch, aggregate_idx, aggregate_op, pending_preaggregated_groups,
		    pending_preaggregated_count_deltas, count_star_update, represented_row_count, true,
		    &deferred_grouped_finish);
		pending_preaggregated_groups.Reset();
		pending_preaggregated_count_deltas.clear();
		pending_preaggregated_row_count = 0;
		if (updated) {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_pending_preaggregated_count_star_flush",
			                             represented_row_count);
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
			    runtime, scratch, aggregate_idx, aggregate_op, groups, count_deltas, count_star_update,
			    represented_row_count, true, &deferred_grouped_finish);
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
		RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind, "pending_count_star_row_groups",
		                                         pending_row_groups.size());
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
		    runtime, scratch, aggregate_idx, aggregate_op, pending_row_groups, pending_row_count_deltas,
		    count_star_update, represented_row_count, true, &deferred_grouped_finish);
		pending_row_groups.Reset();
		pending_row_count_deltas.clear();
		pending_row_count = 0;
		if (updated) {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_pending_count_star_row_flush",
			                             represented_row_count);
		}
		return updated;
	}

private:
	bool deferred_grouped_finish = false;
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
	SljitGroupedDirectPrimitivePayloadUpdateRuntimeState direct_payload_update;
};

} // namespace duckdb

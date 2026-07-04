//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_preaggregation.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

enum class SljitGroupedAggregateUpdateStrategyKind : uint8_t {
	INVALID,
	COUNT_STAR_PREAGGREGATION,
	DISTINCT_COUNT_POINTER
};

enum class SljitGroupedAggregateUpdateInputKind : uint8_t { MATERIALIZED, PROJECTED_INPUT };

struct SljitGroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	SljitGroupedAggregateUpdateStrategyKind strategy = SljitGroupedAggregateUpdateStrategyKind::INVALID;
	SljitGroupedAggregateUpdateInputKind input_kind = SljitGroupedAggregateUpdateInputKind::MATERIALIZED;
};

static bool SljitCanBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t aggregate_idx) {
	return aggregate_idx < ops.size() && ops[aggregate_idx].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       ops[aggregate_idx].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
}

static bool SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.use_primitive_payloads || !op.aggregate_update.plan.use_grouped_state_addresses) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1 || sink_info.aggregates.size() != 1 || op.aggregate_update.payloads.size() != 1) {
		return false;
	}
	auto &aggregate = sink_info.aggregates[0];
	return aggregate.child_count == 0 && aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
}

static SljitGroupedAggregateUpdateStrategyKind
SljitChooseGroupedAggregateUpdateStrategy(const SljitExecutableRegionOp &op) {
	if (op.aggregate_update.plan.sink_info.aggregate_contract.distinct_count_pointer_keys) {
		return SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER;
	}
	if (SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	}
	return SljitGroupedAggregateUpdateStrategyKind::INVALID;
}

static bool SljitCountStarFixedWidthProjectionInputSupported(const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.size() != 1 ||
	    projection_op.output_types.size() != 1) {
		return false;
	}
	idx_t source_index;
	auto &projection = projection_op.projections[0].plan;
	return TryReadProjectionSourceReferenceIndex(projection, source_index) &&
	       source_index < projection_op.input_types.size() && projection.return_type == projection_op.output_types[0] &&
	       projection_op.input_types[source_index] == projection_op.output_types[0] &&
	       TypeIsConstantSize(projection_op.output_types[0].InternalType());
}

static bool SljitCountStarStringCompressedProjectionInputSupported(const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.size() != 1 ||
	    projection_op.output_types.size() != 1) {
		return false;
	}
	auto &projection = projection_op.projections[0].plan;
	if (projection.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	    projection.return_type != projection_op.output_types[0] ||
	    projection.source_index >= projection_op.input_types.size() ||
	    projection_op.input_types[projection.source_index].id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	switch (projection_op.output_types[0].InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
		return projection.string_compress_target_size == GetTypeIdSize(projection_op.output_types[0].InternalType());
	default:
		return false;
	}
}

static bool SljitCountStarProjectionInputSupported(const SljitExecutableRegionOp &projection_op) {
	return SljitCountStarFixedWidthProjectionInputSupported(projection_op) ||
	       SljitCountStarStringCompressedProjectionInputSupported(projection_op);
}

static bool SljitTryBuildCountStarGroupProjection(const SljitExecutableRegionOp &projection_op,
                                                  const SljitExecutableRegionOp &aggregate_op,
                                                  SljitExecutableRegionOp &group_projection) {
	if (!SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(aggregate_op) ||
	    projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1) {
		return false;
	}
	auto &group = sink_info.groups[0];
	if (group.input_index >= projection_op.projections.size() ||
	    group.input_index >= projection_op.output_types.size() ||
	    projection_op.output_types[group.input_index] != group.type) {
		return false;
	}

	group_projection = SljitExecutableRegionOp();
	group_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	group_projection.operator_index = projection_op.operator_index;
	group_projection.input_types = projection_op.input_types;
	group_projection.output_types.push_back(projection_op.output_types[group.input_index]);
	if (group.input_index < projection_op.output_not_null.size()) {
		group_projection.output_not_null.push_back(projection_op.output_not_null[group.input_index]);
	}
	SljitExecutableRegionExpression group_expression;
	SljitBuildBorrowedProjectionExpression(projection_op.projections[group.input_index], group_expression);
	group_projection.projections.push_back(std::move(group_expression));
	return SljitCountStarProjectionInputSupported(group_projection);
}

static bool SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                      idx_t first_projection_idx,
                                                                      idx_t final_projection_idx, idx_t aggregate_idx) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx) ||
	    !SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx) ||
	    SljitChooseGroupedAggregateUpdateStrategy(ops[aggregate_idx]) !=
	        SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION) {
		return false;
	}
	SljitExecutableRegionOp semantic_projection;
	if (!SljitBuildProjectionChainSemanticProjection(ops, first_projection_idx, final_projection_idx,
	                                                 semantic_projection)) {
		return false;
	}
	SljitExecutableRegionOp group_projection;
	return SljitTryBuildCountStarGroupProjection(semantic_projection, ops[aggregate_idx], group_projection);
}

static bool SljitGroupedAggregateUpdateHasDedicatedBackend(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t aggregate_idx) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		return false;
	}
	auto &op = ops[aggregate_idx];
	return SljitChooseGroupedAggregateUpdateStrategy(op) != SljitGroupedAggregateUpdateStrategyKind::INVALID;
}

static SljitGroupedAggregateUpdatePrimitive
SljitBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		throw InternalException("SLJIT grouped aggregate update primitive cannot bind requested operator");
	}
	auto strategy = SljitChooseGroupedAggregateUpdateStrategy(ops[aggregate_idx]);
	if (strategy == SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		throw InternalException("SLJIT grouped aggregate update primitive has no dedicated backend");
	}
	SljitGroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	primitive.strategy = strategy;
	return primitive;
}

static SljitGroupedAggregateUpdatePrimitive
SljitBindProjectedInputGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                       idx_t first_projection_idx, idx_t final_projection_idx,
                                                       idx_t aggregate_idx) {
	if (!SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(ops, first_projection_idx, final_projection_idx,
	                                                               aggregate_idx)) {
		throw InternalException("SLJIT projected grouped aggregate update primitive cannot bind requested operators");
	}
	auto primitive = SljitBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx);
	primitive.input_kind = SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
	return primitive;
}

static bool SljitCanBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        const SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx)) {
		return false;
	}
	auto strategy = SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]);
	if (strategy == SljitGroupedAggregateUpdateStrategyKind::INVALID || primitive.strategy != strategy) {
		return false;
	}
	switch (primitive.input_kind) {
	case SljitGroupedAggregateUpdateInputKind::MATERIALIZED:
		return primitive.first_projection_idx == DConstants::INVALID_INDEX &&
		       primitive.final_projection_idx == DConstants::INVALID_INDEX;
	case SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT:
		return SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
		    ops, primitive.first_projection_idx, primitive.final_projection_idx, primitive.aggregate_idx);
	}
	return false;
}

static bool SljitBindGroupedAggregateUpdateInputView(const SljitRuntimeBatchView &input, DataChunk *&chunk) {
	if (!input.HasChunk()) {
		throw InternalException("SLJIT grouped aggregate update requires an input chunk");
	}
	chunk = &input.Chunk();
	if (input.count > chunk->size()) {
		throw InternalException("SLJIT grouped aggregate update count exceeds input chunk cardinality");
	}
	if (!input.selection && input.count != chunk->size()) {
		throw InternalException("SLJIT grouped aggregate update requires a selection for partial chunk input");
	}
	return input.count > 0;
}

static DataChunk &SljitBindMaterializedGroupedAggregateUpdateInput(const SljitRuntimeBatchView &input) {
	DataChunk *chunk;
	if (!SljitBindGroupedAggregateUpdateInputView(input, chunk)) {
		return *chunk;
	}
	if (!input.IsMaterializedChunk()) {
		throw InternalException("SLJIT grouped aggregate update requires a materialized batch view");
	}
	if (input.count != chunk->size()) {
		throw InternalException("SLJIT grouped aggregate update count does not match input chunk cardinality");
	}
	return *chunk;
}

struct SljitGroupedAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER) {
			return true;
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
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			SljitExecutableRegionOp semantic_projection;
			if (!SljitBuildProjectionChainSemanticProjection(ops, primitive.first_projection_idx,
			                                                 primitive.final_projection_idx, semantic_projection) ||
			    !SljitTryBuildCountStarGroupProjection(semantic_projection, aggregate_op,
			                                           projected_count_star_group_projection)) {
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
		DataChunk *input_chunk;
		if (!SljitBindGroupedAggregateUpdateInputView(input, input_chunk)) {
			return false;
		}
		(void)input_chunk;
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER:
			return ExecuteDistinctCountPointer(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION:
			return ExecuteCountStarPreaggregation(runtime, result, ops, scratch, primitive, input, processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			break;
		}
		throw InternalException("SLJIT grouped aggregate update primitive has an unknown strategy");
	}

	bool Flush(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	           const SljitGroupedAggregateUpdatePrimitive &primitive) {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx, deferred_grouped_finish);
		return false;
	}

private:
	bool ExecuteDistinctCountPointer(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                 vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                 const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                 const SljitRuntimeBatchView &input_view, idx_t &processed_batches) {
		auto &input = SljitBindMaterializedGroupedAggregateUpdateInput(input_view);
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &binding = SljitBindRecordedNativeSink(
		    runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx, aggregate_op.kind, input,
		    aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
		    "SLJIT grouped distinct count-pointer update");
		auto sink_result = SljitExecuteDistinctCountPointerAggregateUpdate(
		    runtime, binding, primitive.aggregate_idx, aggregate_op, input, true, &deferred_grouped_finish);
		if (SljitSinkResultStopsPipeline(sink_result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
		}
		processed_batches++;
		return false;
	}

	bool ExecuteCountStarPreaggregation(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                    const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                    const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		(void)result;
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (TryExecutePreaggregatedCountStar(runtime, scratch, primitive, aggregate_op, input)) {
			processed_batches++;
			return false;
		}
		throw InternalException("SLJIT grouped count-star preaggregation update failed");
	}

	bool TryExecutePreaggregatedCountStar(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                      const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                      SljitExecutableRegionOp &aggregate_op, const SljitRuntimeBatchView &input) {
		if (!count_star_update.Ready()) {
			throw InternalException("SLJIT grouped count-star preaggregation descriptor is not prepared");
		}
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return TryExecuteProjectedPreaggregatedCountStar(runtime, scratch, primitive.aggregate_idx, aggregate_op,
			                                                 input);
		}
		auto &input_chunk = SljitBindMaterializedGroupedAggregateUpdateInput(input);
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
			if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
			        runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups, preaggregated_count_deltas,
			        count_star_update, input.size(), true, &deferred_grouped_finish)) {
				return false;
			}
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
			                             "primitive_grouped_preaggregated_count_star_update", input.size());
			return true;
		}
		row_count_deltas.assign(input.size(), 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "primitive_grouped_count_star_row_deltas", preaggregate_stage_start);
		if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		        runtime, scratch, aggregate_idx, aggregate_op, input, row_count_deltas, count_star_update, input.size(),
		        true, &deferred_grouped_finish)) {
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
	                                               SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
	                                               SljitExecutableRegionOp &aggregate_op,
	                                               const SljitRuntimeBatchView &input) {
		DataChunk *input_chunk;
		if (!SljitBindGroupedAggregateUpdateInputView(input, input_chunk)) {
			return false;
		}
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryPreaggregateProjectedCountStarGroups(projected_count_star_group_projection, *input_chunk,
		                                                 input.selection, input.count, preaggregated_groups,
		                                                 preaggregated_count_deltas)) {
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "primitive_projected_preaggregate_count_star_groups",
			                              preaggregate_stage_start);
			if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
			        runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups, preaggregated_count_deltas,
			        count_star_update, input.count, true, &deferred_grouped_finish)) {
				return false;
			}
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
			                             "primitive_projected_preaggregated_count_star_update", input.count);
			return true;
		}

		preaggregated_groups.Reset();
		SljitExecuteProjectionExpression(projected_count_star_group_projection.projections[0], *input_chunk,
		                                 preaggregated_groups.data[0], input.selection, input.count,
		                                 projected_count_star_group_scratch);
		preaggregated_groups.SetChildCardinality(input.count);
		row_count_deltas.assign(input.count, 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "primitive_projected_count_star_row_groups", preaggregate_stage_start);
		if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		        runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups, row_count_deltas,
		        count_star_update, input.count, true, &deferred_grouped_finish)) {
			return false;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_projected_count_star_row_update",
		                             input.count);
		return true;
	}

private:
	bool deferred_grouped_finish = false;
	vector<LogicalType> group_types;
	DataChunk preaggregated_groups;
	vector<int64_t> preaggregated_count_deltas;
	vector<int64_t> row_count_deltas;
	SljitExecutableRegionOp projected_count_star_group_projection;
	SljitExpressionAdapterScratch projected_count_star_group_scratch;
	SljitCountStarGroupedAggregateUpdateDescriptor count_star_update;
};

} // namespace duckdb

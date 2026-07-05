//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_fixed_preaggregation.hpp"
#include "sljit_aggregate_count_star_string_preaggregation.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_descriptor.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

enum class SljitGroupedAggregateUpdateStrategyKind : uint8_t {
	INVALID,
	COUNT_STAR_PREAGGREGATION,
	DIRECT_PRIMITIVE_PAYLOAD_UPDATE
};

enum class SljitGroupedAggregateUpdateInputKind : uint8_t { MATERIALIZED, PROJECTED_INPUT };

struct SljitGroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	SljitGroupedAggregateUpdateStrategyKind strategy = SljitGroupedAggregateUpdateStrategyKind::INVALID;
	SljitGroupedAggregateUpdateInputKind input_kind = SljitGroupedAggregateUpdateInputKind::MATERIALIZED;
	shared_ptr<SljitExecutableRegionOp> projected_count_star_group_projection;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
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

static bool SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(const SljitExecutableRegionOp &op) {
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.use_primitive_payloads) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	return !sink_info.groups.empty() && sink_info.aggregates.size() == op.aggregate_update.payloads.size();
}

static SljitGroupedAggregateUpdateStrategyKind
SljitChooseGroupedAggregateUpdateStrategy(const SljitExecutableRegionOp &op) {
	if (SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	}
	if (SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(op)) {
		return SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
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
	return true;
}

static void SljitInitializeProjectedInputGroupedAggregatePrimitive(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                   idx_t first_projection_idx,
                                                                   idx_t final_projection_idx) {
	primitive.input_kind = SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
}

static bool SljitTryBindProjectedCountStarGroupedAggregateStrategy(
    SljitGroupedAggregateUpdatePrimitive &primitive, const SljitExecutableRegionOp &semantic_projection,
    const SljitExecutableRegionOp &aggregate_op) {
	SljitExecutableRegionOp group_projection;
	if (!SljitTryBuildCountStarGroupProjection(semantic_projection, aggregate_op, group_projection) ||
	    !SljitCountStarProjectionInputSupported(group_projection)) {
		return false;
	}
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	primitive.projected_count_star_group_projection =
	    make_shared_ptr<SljitExecutableRegionOp>(std::move(group_projection));
	return true;
}

static bool SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(
    SljitGroupedAggregateUpdatePrimitive &primitive, const vector<SljitExecutableRegionOp> &ops,
    idx_t first_projection_idx, idx_t final_projection_idx) {
	SljitProjectedInputGroupedAggregateDescriptor descriptor;
	if (!SljitTryBuildProjectedInputGroupedAggregateDescriptor(
	        ops, first_projection_idx, final_projection_idx, primitive.aggregate_idx,
	        optional_ptr<SljitProjectedInputGroupedAggregateDescriptor>(&descriptor)) ||
	    !SljitProjectedInputGroupedAggregateCanUseCompactInput(descriptor)) {
		return false;
	}
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	primitive.projected_direct_update =
	    make_shared_ptr<SljitProjectedInputGroupedAggregateDescriptor>(std::move(descriptor));
	return true;
}

static bool SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(
    const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx, idx_t final_projection_idx,
    bool allow_direct_primitive_payload_update, SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx) ||
	    !SljitCanBindGroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx) ||
	    SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]) ==
	        SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		return false;
	}
	SljitInitializeProjectedInputGroupedAggregatePrimitive(primitive, first_projection_idx, final_projection_idx);
	SljitExecutableRegionOp semantic_projection;
	if (!SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
	                                                 semantic_projection)) {
		return false;
	}
	if (SljitTryBindProjectedCountStarGroupedAggregateStrategy(primitive, semantic_projection,
	                                                          ops[primitive.aggregate_idx])) {
		return true;
	}
	if (allow_direct_primitive_payload_update) {
		return SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(primitive, ops, first_projection_idx,
		                                                                final_projection_idx);
	}
	return false;
}

static bool SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                      idx_t first_projection_idx,
                                                                      idx_t final_projection_idx, idx_t aggregate_idx,
                                                                      bool allow_direct_primitive_payload_update =
                                                                          false) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		return false;
	}
	SljitGroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	return SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(ops, first_projection_idx, final_projection_idx,
	                                                                allow_direct_primitive_payload_update, primitive);
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
                                                       idx_t aggregate_idx,
                                                       bool allow_direct_primitive_payload_update = false) {
	auto primitive = SljitBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx);
	if (!SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(ops, first_projection_idx, final_projection_idx,
	                                                              allow_direct_primitive_payload_update, primitive)) {
		throw InternalException("SLJIT projected grouped aggregate update primitive cannot bind requested operators");
	}
	return primitive;
}

static bool SljitCanBindGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        const SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx)) {
		return false;
	}
	auto strategy = SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]);
	if (strategy == SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		return false;
	}
	switch (primitive.input_kind) {
	case SljitGroupedAggregateUpdateInputKind::MATERIALIZED:
		return primitive.strategy == strategy && primitive.first_projection_idx == DConstants::INVALID_INDEX &&
		       primitive.final_projection_idx == DConstants::INVALID_INDEX;
	case SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT:
		if (!SljitCanBindProjectionChainPrimitive(ops, primitive.first_projection_idx,
		                                          primitive.final_projection_idx)) {
			return false;
		}
		switch (primitive.strategy) {
		case SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION:
			return primitive.projected_count_star_group_projection &&
			       primitive.projected_count_star_group_projection->kind == SljitNativeRegionOpKind::PROJECTION &&
			       SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(ops[primitive.aggregate_idx]) &&
			       SljitCountStarProjectionInputSupported(*primitive.projected_count_star_group_projection);
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
			return primitive.projected_direct_update && primitive.projected_direct_update->Ready() &&
			       SljitProjectedInputGroupedAggregateCanUseCompactInput(*primitive.projected_direct_update);
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			return false;
		}
	}
	return false;
}

struct SljitGroupedAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE) {
			deferred_grouped_finish = false;
			if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
				projected_direct_update = primitive.projected_direct_update;
				return projected_direct_update &&
				       SljitProjectedInputGroupedAggregateCanUseCompactInput(*projected_direct_update);
			}
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
			return ExecuteDirectPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
			                                           processed_batches);
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			break;
		}
		throw InternalException("SLJIT grouped aggregate update primitive has an unknown strategy");
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return false;
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
	bool ExecuteDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                         vector<SljitExecutableRegionOp> &ops,
	                                         SljitRegionExecutionScratch &scratch,
	                                         const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                         const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return ExecuteProjectedDirectPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
			                                                   processed_batches);
		}
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT grouped aggregate direct update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		auto sink_result =
		    SljitExecutePrimitiveAggregateUpdate(runtime, native_runtime, scratch, primitive.aggregate_idx,
		                                         aggregate_op, input_chunk, nullptr, DConstants::INVALID_INDEX, true,
		                                         optional_ptr<bool>(&deferred_grouped_finish));
		sink_result = native_runtime.RecordSinkResult(input_chunk, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return true;
		}
		processed_batches++;
		return false;
	}

	bool ExecuteProjectedDirectPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                                  vector<SljitExecutableRegionOp> &ops,
	                                                  SljitRegionExecutionScratch &scratch,
	                                                  const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                                  const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		(void)result;
		if (!projected_direct_update || !projected_direct_update->Ready()) {
			throw InternalException("SLJIT projected grouped aggregate direct update descriptor is not prepared");
		}
		if (input.HasHashJoinSelection()) {
			throw InternalException("SLJIT projected grouped aggregate direct update requires a source batch view");
		}
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT projected grouped aggregate direct update");
		if (input_chunk.ColumnCount() != projected_direct_update->input_types.size()) {
			throw InternalException("SLJIT projected grouped aggregate direct update input schema mismatch");
		}
		DataChunk *source_input = &input_chunk;
		if (input.selection) {
			projected_direct_selected_input.Ensure(runtime.GetAllocator(), projected_direct_update->input_types);
			auto &selected_input = projected_direct_selected_input.chunk;
			selected_input.Reset();
			selected_input.Slice(input_chunk, *input.selection, input.count);
			source_input = &selected_input;
		}
		auto &aggregate_input = BuildProjectedDirectAggregateInput(runtime, ops, primitive, *source_input);
		auto &aggregate_op = ops[primitive.aggregate_idx];
		if (aggregate_op.aggregate_update.dense_group_domain.ready) {
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_dense_group_domain",
			                             aggregate_input.size());
		}
		auto sink_result =
		    SljitExecutePrimitiveAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx,
		                                         aggregate_op, aggregate_input, nullptr, DConstants::INVALID_INDEX, true,
		                                         optional_ptr<bool>(&deferred_grouped_finish));
		sink_result = runtime.ExecutionOperators().RecordSinkResult(aggregate_input, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, primitive.aggregate_idx,
			                                          deferred_grouped_finish);
			return true;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_input_vector_grouped_update",
		                             aggregate_input.size());
		processed_batches++;
		return false;
	}

	DataChunk &BuildProjectedDirectAggregateInput(ExecutionRegionRuntime &runtime,
	                                             const vector<SljitExecutableRegionOp> &ops,
	                                             const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                             DataChunk &source_input) {
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
		auto &projection = projected_direct_update->projection;
		projected_direct_aggregate_input.Ensure(runtime.GetAllocator(), projection.output_types);
		auto &aggregate_input = projected_direct_aggregate_input.chunk;
		aggregate_input.Reset();
		if (aggregate_input.ColumnCount() != projection.output_types.size()) {
			throw InternalException("SLJIT projected compact aggregate input schema mismatch");
		}
		for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
			auto &group = sink_info.groups[group_idx];
			if (group.input_index >= aggregate_input.ColumnCount() ||
			    group_idx >= projected_direct_update->group_sources.size()) {
				throw InternalException("SLJIT projected compact aggregate group source mismatch");
			}
			if (!SljitTryMaterializeInputVectorGroupSource(source_input, projected_direct_update->group_sources[group_idx],
			                                               aggregate_input.data[group.input_index], source_input.size(),
			                                               false)) {
				throw InternalException("SLJIT projected compact aggregate group materialization failed");
			}
		}
		if (projected_direct_update->payload_projection_indices.size() !=
		    projected_direct_update->payload_source_indices.size()) {
			throw InternalException("SLJIT projected compact aggregate payload mapping mismatch");
		}
		for (idx_t payload_idx = 0; payload_idx < projected_direct_update->payload_projection_indices.size();
		     payload_idx++) {
			const auto projection_idx = projected_direct_update->payload_projection_indices[payload_idx];
			const auto source_idx = projected_direct_update->payload_source_indices[payload_idx];
			if (projection_idx == DConstants::INVALID_INDEX || source_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			if (projection_idx >= aggregate_input.ColumnCount() || source_idx >= source_input.ColumnCount() ||
			    aggregate_input.data[projection_idx].GetType() != source_input.data[source_idx].GetType()) {
				throw InternalException("SLJIT projected compact aggregate payload source mismatch");
			}
			aggregate_input.data[projection_idx].Reference(source_input.data[source_idx]);
		}
		aggregate_input.SetChildCardinality(source_input.size());
		RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind, "projected_compact_aggregate_input",
		                                         source_input.size());
		return aggregate_input;
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
			return TryExecuteProjectedPreaggregatedCountStar(runtime, scratch, primitive, aggregate_op, input);
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
	                                               SljitRegionExecutionScratch &scratch,
	                                               const SljitGroupedAggregateUpdatePrimitive &primitive,
	                                               SljitExecutableRegionOp &aggregate_op,
	                                               const SljitRuntimeBatchView &input) {
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT projected grouped count-star update");
		const auto aggregate_idx = primitive.aggregate_idx;
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (!projected_count_star_group_projection) {
			throw InternalException("SLJIT projected count-star group projection is not prepared");
		}
		if (SljitTryPreaggregateProjectedCountStarGroups(*projected_count_star_group_projection, input_chunk,
		                                                 input.selection, input.count, preaggregated_groups,
		                                                 preaggregated_count_deltas)) {
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "primitive_projected_preaggregate_count_star_groups",
			                              preaggregate_stage_start);
			if (!AccumulatePreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
			                                      preaggregated_count_deltas, input.count)) {
				return false;
			}
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
			                             "primitive_projected_preaggregated_count_star_update", input.count);
			return true;
		}

		preaggregated_groups.Reset();
		SljitExecuteProjectionExpression(projected_count_star_group_projection->projections[0], input_chunk,
		                                 preaggregated_groups.data[0], input.selection, input.count,
		                                 projected_count_star_group_scratch);
		preaggregated_groups.SetChildCardinality(input.count);
		row_count_deltas.assign(input.count, 1);
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "primitive_projected_count_star_row_groups", preaggregate_stage_start);
		if (!FlushPendingPreaggregatedCountStar(runtime, scratch, aggregate_idx, aggregate_op)) {
			return false;
		}
		if (!AccumulateRowCountStar(runtime, scratch, aggregate_idx, aggregate_op, preaggregated_groups,
		                            row_count_deltas, input.count)) {
			return false;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "primitive_projected_count_star_row_update",
		                             input.count);
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
	SljitCountStarGroupedAggregateUpdateDescriptor count_star_update;
	shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update;
	SljitDataChunkBatch projected_direct_selected_input;
	SljitDataChunkBatch projected_direct_aggregate_input;
};

} // namespace duckdb

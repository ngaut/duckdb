//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_count_star_projection_input.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_projected_input_grouped_aggregate_descriptor.hpp"

namespace duckdb {

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
	auto group_expression = make_uniq<SljitExecutableRegionExpression>();
	SljitPrepareExecutableRegionExpression(projection_op.projections[group.input_index].plan, *group_expression,
	                                       nullptr, true);
	string compile_error;
	if (!SljitCompilePreparedExecutableRegionExpression(*group_expression, false, compile_error)) {
		return false;
	}
	group_projection.projections.push_back(std::move(*group_expression));
	return true;
}

static void SljitInitializeProjectedInputGroupedAggregatePrimitive(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                   idx_t first_projection_idx,
                                                                   idx_t final_projection_idx) {
	primitive.input_kind = SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
}

static bool SljitTryBindProjectedCountStarGroupedAggregateStrategy(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                   const SljitExecutableRegionOp &semantic_projection,
                                                                   const SljitExecutableRegionOp &aggregate_op) {
	auto group_projection = make_shared_ptr<SljitExecutableRegionOp>();
	if (!SljitTryBuildCountStarGroupProjection(semantic_projection, aggregate_op, *group_projection) ||
	    !SljitCountStarProjectionInputSupported(*group_projection)) {
		return false;
	}
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	primitive.projected_count_star_group_projection = std::move(group_projection);
	return true;
}

static bool SljitTryBindProjectedDistinctKeySinkStrategy(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                         SljitExecutableRegionOp &semantic_projection,
                                                         const SljitExecutableRegionOp &aggregate_op) {
	if (!SljitGroupedAggregateUpdateCanUseDistinctKeySink(aggregate_op)) {
		return false;
	}
	vector<LogicalType> expected_input_types;
	if (!SljitTryBuildDistinctKeySinkInputTypes(aggregate_op.aggregate_update.plan.sink_info, expected_input_types)) {
		return false;
	}
	if (semantic_projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    semantic_projection.output_types != expected_input_types) {
		return false;
	}
	auto projected_input = make_shared_ptr<SljitExecutableRegionOp>();
	*projected_input = std::move(semantic_projection);
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK;
	primitive.projected_distinct_key_input_projection = std::move(projected_input);
	return true;
}

static bool SljitTryBindSingleProjectionDistinctKeySinkStrategy(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                const vector<SljitExecutableRegionOp> &ops,
                                                                idx_t first_projection_idx,
                                                                idx_t final_projection_idx) {
	if (first_projection_idx != final_projection_idx || final_projection_idx >= ops.size()) {
		return false;
	}
	auto &projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[primitive.aggregate_idx];
	vector<LogicalType> expected_input_types;
	if (!SljitGroupedAggregateUpdateCanUseDistinctKeySink(aggregate_op) ||
	    !SljitTryBuildDistinctKeySinkInputTypes(aggregate_op.aggregate_update.plan.sink_info, expected_input_types) ||
	    projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.output_types != expected_input_types) {
		return false;
	}
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK;
	primitive.projected_distinct_key_input_projection.reset();
	return true;
}

static bool SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                      const vector<SljitExecutableRegionOp> &ops,
                                                                      idx_t first_projection_idx,
                                                                      idx_t final_projection_idx) {
	auto descriptor = make_shared_ptr<SljitProjectedInputGroupedAggregateDescriptor>();
	if (!SljitTryBuildProjectedInputGroupedAggregateDescriptor(
	        ops, first_projection_idx, final_projection_idx, primitive.aggregate_idx,
	        optional_ptr<SljitProjectedInputGroupedAggregateDescriptor>(descriptor.get())) ||
	    !SljitProjectedInputGroupedAggregateCanUseSourceInput(*descriptor)) {
		return false;
	}
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	primitive.projected_direct_update = std::move(descriptor);
	return true;
}

static bool SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(const vector<SljitExecutableRegionOp> &ops,
                                                                     idx_t first_projection_idx,
                                                                     idx_t final_projection_idx,
                                                                     bool allow_direct_primitive_payload_update,
                                                                     SljitGroupedAggregateUpdatePrimitive &primitive) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		return false;
	}
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx)) {
		return false;
	}
	if (SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]) ==
	    SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		return false;
	}
	SljitInitializeProjectedInputGroupedAggregatePrimitive(primitive, first_projection_idx, final_projection_idx);
	if (SljitTryBindSingleProjectionDistinctKeySinkStrategy(primitive, ops, first_projection_idx,
	                                                        final_projection_idx)) {
		return true;
	}
	auto semantic_projection = make_uniq<SljitExecutableRegionOp>();
	if (SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
	                                                *semantic_projection)) {
		if (SljitTryBindProjectedCountStarGroupedAggregateStrategy(primitive, *semantic_projection,
		                                                           ops[primitive.aggregate_idx])) {
			return true;
		}
		if (SljitTryBindProjectedDistinctKeySinkStrategy(primitive, *semantic_projection,
		                                                 ops[primitive.aggregate_idx])) {
			return true;
		}
	}
	if (SljitAggregateSinkHasDistinctState(ops[primitive.aggregate_idx].aggregate_update.plan.sink_info)) {
		return false;
	}
	if (allow_direct_primitive_payload_update) {
		return SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(primitive, ops, first_projection_idx,
		                                                                 final_projection_idx);
	}
	return false;
}

static bool SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
    const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t aggregate_idx, bool allow_direct_primitive_payload_update = false) {
	if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		return false;
	}
	SljitGroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	return SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(ops, first_projection_idx, final_projection_idx,
	                                                                allow_direct_primitive_payload_update, primitive);
}

static SljitGroupedAggregateUpdatePrimitive SljitBindProjectedInputGroupedAggregateUpdatePrimitive(
    const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t aggregate_idx, bool allow_direct_primitive_payload_update = false) {
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
	switch (primitive.input_kind) {
	case SljitGroupedAggregateUpdateInputKind::MATERIALIZED: {
		if (primitive.first_projection_idx != DConstants::INVALID_INDEX ||
		    primitive.final_projection_idx != DConstants::INVALID_INDEX) {
			return false;
		}
		if (primitive.strategy == SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, primitive.filter_idx,
			                                                           primitive.aggregate_idx);
		}
		auto strategy = SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]);
		if (strategy == SljitGroupedAggregateUpdateStrategyKind::INVALID) {
			return false;
		}
		return primitive.filter_idx == DConstants::INVALID_INDEX && primitive.strategy == strategy;
	}
	case SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT:
		if (primitive.filter_idx != DConstants::INVALID_INDEX) {
			return false;
		}
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
			       SljitProjectedInputGroupedAggregateCanUseSourceInput(*primitive.projected_direct_update);
		case SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK: {
			if (!SljitGroupedAggregateUpdateCanUseDistinctKeySink(ops[primitive.aggregate_idx])) {
				return false;
			}
			vector<LogicalType> expected_input_types;
			if (!SljitTryBuildDistinctKeySinkInputTypes(ops[primitive.aggregate_idx].aggregate_update.plan.sink_info,
			                                            expected_input_types)) {
				return false;
			}
			if (primitive.projected_distinct_key_input_projection) {
				return primitive.projected_distinct_key_input_projection->kind == SljitNativeRegionOpKind::PROJECTION &&
				       primitive.projected_distinct_key_input_projection->output_types == expected_input_types;
			}
			return primitive.first_projection_idx == primitive.final_projection_idx &&
			       primitive.final_projection_idx < ops.size() &&
			       ops[primitive.final_projection_idx].kind == SljitNativeRegionOpKind::PROJECTION &&
			       ops[primitive.final_projection_idx].output_types == expected_input_types;
		}
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return false;
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			return false;
		}
	}
	return false;
}

} // namespace duckdb

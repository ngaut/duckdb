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
	    projection_op.Kind() != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	auto &sink_info = aggregate_op.AggregateUpdate().plan.sink_info;
	if (sink_info.groups.size() != 1) {
		return false;
	}
	auto &group = sink_info.groups[0];
	if (group.input_index >= projection_op.Projections().size() ||
	    group.input_index >= projection_op.output_types.size() ||
	    projection_op.output_types[group.input_index] != group.type) {
		return false;
	}

	group_projection = SljitExecutableRegionOp();
	group_projection.SetKind(SljitNativeRegionOpKind::PROJECTION);
	group_projection.operator_index = projection_op.operator_index;
	group_projection.input_types = projection_op.input_types;
	group_projection.output_types.push_back(projection_op.output_types[group.input_index]);
	if (group.input_index < projection_op.output_not_null.size()) {
		group_projection.output_not_null.push_back(projection_op.output_not_null[group.input_index]);
	}
	auto group_expression = make_uniq<SljitExecutableRegionExpression>();
	SljitPrepareExecutableRegionExpression(
	    SljitCopySemanticExpressionPlan(projection_op.Projections()[group.input_index]), *group_expression, nullptr,
	    true);
	string compile_error;
	if (!SljitCompilePreparedExecutableRegionExpression(*group_expression, false, compile_error)) {
		return false;
	}
	group_projection.Projections().push_back(std::move(*group_expression));
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
	auto semantic_projection = make_uniq<SljitExecutableRegionOp>();
	if (SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
	                                                *semantic_projection) &&
	    SljitTryBindProjectedCountStarGroupedAggregateStrategy(primitive, *semantic_projection,
	                                                           ops[primitive.aggregate_idx])) {
		return true;
	}
	if (allow_direct_primitive_payload_update) {
		return SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(primitive, ops, first_projection_idx,
		                                                                 final_projection_idx);
	}
	return false;
}

static bool SljitTryBindProjectedInputGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                      idx_t first_projection_idx,
                                                                      idx_t final_projection_idx, idx_t aggregate_idx,
                                                                      bool allow_direct_primitive_payload_update,
                                                                      SljitGroupedAggregateUpdatePrimitive &primitive) {
	SljitGroupedAggregateUpdatePrimitive candidate;
	candidate.aggregate_idx = aggregate_idx;
	if (!SljitTryBindProjectedInputGroupedAggregateUpdateStrategy(ops, first_projection_idx, final_projection_idx,
	                                                              allow_direct_primitive_payload_update, candidate)) {
		return false;
	}
	primitive = std::move(candidate);
	return true;
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
			       primitive.projected_count_star_group_projection->Kind() == SljitNativeRegionOpKind::PROJECTION &&
			       SljitGroupedAggregateUpdateCanUseCountStarPreaggregation(ops[primitive.aggregate_idx]) &&
			       SljitCountStarProjectionInputSupported(*primitive.projected_count_star_group_projection);
		case SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
			return primitive.projected_direct_update && primitive.projected_direct_update->Ready() &&
			       SljitProjectedInputGroupedAggregateCanUseSourceInput(*primitive.projected_direct_update);
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return false;
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			return false;
		}
	}
	return false;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_descriptor.hpp"
#include "sljit_projection_chain_primitive.hpp"

namespace duckdb {

enum class SljitGroupedAggregateUpdateStrategyKind : uint8_t {
	INVALID,
	COUNT_STAR_PREAGGREGATION,
	DIRECT_PRIMITIVE_PAYLOAD_UPDATE,
	FILTERED_PRIMITIVE_PAYLOAD_UPDATE
};

enum class SljitGroupedAggregateUpdateInputKind : uint8_t { MATERIALIZED, PROJECTED_INPUT };

struct SljitGroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
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

static bool SljitCanBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                idx_t filter_idx, idx_t aggregate_idx) {
	return filter_idx < ops.size() && ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       aggregate_idx < ops.size() &&
	       SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate(ops[aggregate_idx]);
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

static bool SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                      const vector<SljitExecutableRegionOp> &ops,
                                                                      idx_t first_projection_idx,
                                                                      idx_t final_projection_idx) {
	auto descriptor = make_shared_ptr<SljitProjectedInputGroupedAggregateDescriptor>();
	if (!SljitTryBuildProjectedInputGroupedAggregateDescriptor(
	        ops, first_projection_idx, final_projection_idx, primitive.aggregate_idx,
	        optional_ptr<SljitProjectedInputGroupedAggregateDescriptor>(descriptor.get())) ||
	    !SljitProjectedInputGroupedAggregateCanUseCompactInput(*descriptor)) {
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
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx) ||
	    !SljitCanBindGroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx) ||
	    SljitChooseGroupedAggregateUpdateStrategy(ops[primitive.aggregate_idx]) ==
	        SljitGroupedAggregateUpdateStrategyKind::INVALID) {
		return false;
	}
	SljitInitializeProjectedInputGroupedAggregatePrimitive(primitive, first_projection_idx, final_projection_idx);
	auto semantic_projection = make_uniq<SljitExecutableRegionOp>();
	if (!SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
	                                                 *semantic_projection)) {
		return false;
	}
	if (SljitTryBindProjectedCountStarGroupedAggregateStrategy(primitive, *semantic_projection,
	                                                           ops[primitive.aggregate_idx])) {
		return true;
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
SljitBindFilteredGroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx,
                                                 idx_t aggregate_idx) {
	if (!SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx)) {
		throw InternalException("SLJIT filtered grouped aggregate update primitive cannot bind requested operators");
	}
	SljitGroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	primitive.filter_idx = filter_idx;
	primitive.strategy = SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE;
	return primitive;
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
			       SljitProjectedInputGroupedAggregateCanUseCompactInput(*primitive.projected_direct_update);
		case SljitGroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
			return false;
		case SljitGroupedAggregateUpdateStrategyKind::INVALID:
			return false;
		}
	}
	return false;
}

} // namespace duckdb

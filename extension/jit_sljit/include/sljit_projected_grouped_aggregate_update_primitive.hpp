//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_grouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_projected_input_grouped_aggregate_descriptor.hpp"

namespace duckdb {

static void SljitInitializeProjectedInputGroupedAggregatePrimitive(SljitGroupedAggregateUpdatePrimitive &primitive,
                                                                   idx_t first_projection_idx,
                                                                   idx_t final_projection_idx) {
	primitive.input_kind = SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
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
	return allow_direct_primitive_payload_update && SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy(
	                                                    primitive, ops, first_projection_idx, final_projection_idx);
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_post_join_projection_strategy.hpp"

namespace duckdb {

struct SljitPostJoinProjectionAggregatePrimitive {
	SljitPostJoinProjectionPrimitive post_join_projection;
	SljitDirectJoinOutputAggregatePrimitive direct_join_output_aggregate;
};

static bool
SljitCanBindPostJoinProjectionAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                 const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	auto &post_join_projection = primitive.post_join_projection;
	return SljitCanBindPostJoinProjectionPrimitive(ops, post_join_projection.hash_join_idx,
	                                               post_join_projection.first_projection_idx,
	                                               post_join_projection.final_projection_idx) &&
	       SljitCanBindDirectJoinOutputAggregatePrimitive(ops, primitive.direct_join_output_aggregate.aggregate_idx);
}

struct SljitJoinProjectionAggregateUpdatePrimitive {
	SljitPostJoinProjectionAggregatePrimitive selected_join_output;
};

static SljitJoinProjectionAggregateUpdatePrimitive
SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	SljitJoinProjectionAggregateUpdatePrimitive update;
	update.selected_join_output = primitive;
	return update;
}

static idx_t
SljitJoinProjectionAggregateUpdateFirstOpIdx(const SljitJoinProjectionAggregateUpdatePrimitive &primitive) {
	return primitive.selected_join_output.post_join_projection.hash_join_idx;
}

static idx_t
SljitJoinProjectionAggregateUpdateAggregateIdx(const SljitJoinProjectionAggregateUpdatePrimitive &primitive) {
	return primitive.selected_join_output.direct_join_output_aggregate.aggregate_idx;
}

static bool
SljitCanBindJoinProjectionAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                   const SljitJoinProjectionAggregateUpdatePrimitive &primitive) {
	return SljitCanBindPostJoinProjectionAggregatePrimitive(ops, primitive.selected_join_output);
}

} // namespace duckdb

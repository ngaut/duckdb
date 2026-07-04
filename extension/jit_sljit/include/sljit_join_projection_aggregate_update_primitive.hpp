//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_projection_aggregate_primitive.hpp"

namespace duckdb {

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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_aggregate_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_post_join_projection_strategy.hpp"

namespace duckdb {

struct SljitPostJoinProjectionAggregatePrimitive {
	SljitPostJoinProjectionPrimitive post_join_projection;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
};

static bool
SljitCanBindPostJoinProjectionAggregatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                 const SljitPostJoinProjectionAggregatePrimitive &primitive) {
	auto &post_join_projection = primitive.post_join_projection;
	return SljitCanBindPostJoinProjectionPrimitive(ops, post_join_projection.hash_join_idx,
	                                               post_join_projection.first_projection_idx,
	                                               post_join_projection.final_projection_idx) &&
	       primitive.aggregate_idx < ops.size() &&
	       SljitAggregateUpdateHasDedicatedCompiledBackend(ops[primitive.aggregate_idx]);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"

namespace duckdb {

enum class SljitJoinProjectionAggregateSourceKind : uint8_t { SOURCE_CHUNK, PRE_JOIN_PROJECTION, FILTER_PROJECTION };

struct SljitJoinProjectionAggregatePrimitive {
	SljitJoinProjectionAggregateSourceKind input_kind = SljitJoinProjectionAggregateSourceKind::SOURCE_CHUNK;
	SljitInt64ToInt32PreJoinProjection pre_join_projection;
	SljitPostJoinProjectionPrimitive post_join_projection;
	SljitDirectJoinOutputAggregatePrimitive direct_join_output_aggregate;
	idx_t pre_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t source_projection_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
};

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

} // namespace duckdb

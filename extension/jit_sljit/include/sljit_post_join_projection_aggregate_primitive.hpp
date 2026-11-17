//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_aggregate_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_join_aggregate_projection.hpp"

namespace duckdb {

static bool SljitAggregateUpdateHasDedicatedCompiledBackend(const SljitExecutableRegionOp &op) {
	if (op.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return op.AggregateUpdate().plan.UsesPrimitivePayloads();
}

struct SljitPostJoinProjectionAggregatePrimitive {
	SljitPostJoinProjectionPrimitive post_join_projection;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	shared_ptr<SljitJoinAggregateProjection> projection;
};

} // namespace duckdb

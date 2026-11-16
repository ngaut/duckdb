//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_facts.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_shape.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_mark_probe_filter_mode.hpp"

namespace duckdb {

//! ops [0, first_hash_join_idx) form the source prefix (see SljitFullPipelineSourcePrefixEnd).
struct SljitHashJoinBuildSinkFacts {
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t final_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;

	bool HasProjection() const {
		return projection_idx != DConstants::INVALID_INDEX;
	}
};

//! ops [0, hash_join_idx) form the source prefix (see SljitFullPipelineSourcePrefixEnd).
struct SljitJoinFilterAggregateFacts {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t first_post_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_post_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	bool HasPostJoinProjection() const {
		return first_post_join_projection_idx != DConstants::INVALID_INDEX;
	}
};

enum class SljitProjectionAggregatePrefixKind { INVALID, SOURCE, JOIN_PREFIX };

struct SljitProjectionAggregateJoinPrefixStep {
	idx_t input_projection_idx = DConstants::INVALID_INDEX;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;

	bool HasInputProjection() const {
		return input_projection_idx != DConstants::INVALID_INDEX;
	}
};

struct SljitProjectionAggregatePrefixFacts {
	idx_t source_filter_idx = DConstants::INVALID_INDEX;
	idx_t source_projection_idx = DConstants::INVALID_INDEX;
	vector<SljitProjectionAggregateJoinPrefixStep> joins;
	idx_t mark_filter_idx = DConstants::INVALID_INDEX;

	bool HasSourceFilter() const {
		return source_filter_idx != DConstants::INVALID_INDEX;
	}

	bool HasSourceProjection() const {
		return source_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasFirstHashJoin() const {
		return !joins.empty();
	}

	bool HasMarkFilter() const {
		return mark_filter_idx != DConstants::INVALID_INDEX;
	}

	idx_t JoinCount() const {
		return joins.size();
	}

	bool HasJoinInputProjection(idx_t join_idx) const {
		return join_idx < JoinCount() && joins[join_idx].HasInputProjection();
	}

	idx_t JoinInputProjectionIdx(idx_t join_idx) const {
		if (!HasJoinInputProjection(join_idx)) {
			return DConstants::INVALID_INDEX;
		}
		return joins[join_idx].input_projection_idx;
	}

	idx_t HashJoinIdx(idx_t join_idx) const {
		if (join_idx >= JoinCount()) {
			return DConstants::INVALID_INDEX;
		}
		return joins[join_idx].hash_join_idx;
	}

	idx_t FinalHashJoinIdx() const {
		if (joins.empty()) {
			return DConstants::INVALID_INDEX;
		}
		return joins.back().hash_join_idx;
	}

	idx_t MarkFilterHashJoinIdx() const {
		if (!HasMarkFilter() || !HasFirstHashJoin()) {
			return DConstants::INVALID_INDEX;
		}
		return FinalHashJoinIdx();
	}

	SljitProjectionAggregatePrefixKind Kind() const {
		if (joins.empty()) {
			return SljitProjectionAggregatePrefixKind::SOURCE;
		}
		return SljitProjectionAggregatePrefixKind::JOIN_PREFIX;
	}
};

struct SljitProjectionAggregatePlanFacts {
	SljitFullPipelineProjectionAggregateShape shape;
	SljitProjectionAggregatePrefixFacts prefix;

	idx_t ProjectionCount() const {
		return shape.ProjectionCount();
	}
};

bool SljitFullPipelineOpIsAt(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx, SljitNativeRegionOpKind kind);
//! The source prefix is the longest run of ops in [0, limit) that only select rows (FILTER) or compute columns
//! (PROJECTION) ahead of the recipe's first operator; recipes bind it as generated-filter and projection-chain steps.
//! Returns the run's end.
idx_t SljitFullPipelineSourcePrefixEnd(const vector<SljitExecutableRegionOp> &ops, idx_t limit);
bool SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx);

bool SljitTryAnalyzeHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops, SljitHashJoinBuildSinkFacts &facts);
bool SljitTryAnalyzeJoinFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                        SljitJoinFilterAggregateFacts &facts);
bool SljitTryAnalyzeProjectionAggregatePlan(const vector<SljitExecutableRegionOp> &ops,
                                            SljitProjectionAggregatePlanFacts &plan);

} // namespace duckdb

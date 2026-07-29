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

struct SljitFullPipelineScheduleFacts {
	bool uses_extended_source_fetch_budget = false;
};

struct SljitHashJoinDelimJoinSinkFacts {
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t final_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;
};

struct SljitHashJoinAppendSinkFacts {
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t final_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;
};

struct SljitHashJoinBuildSinkFacts {
	idx_t pre_projection_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t filter_projection_idx = DConstants::INVALID_INDEX;
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t final_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;

	bool HasPreProjection() const {
		return pre_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasFilterProjection() const {
		return filter_idx != DConstants::INVALID_INDEX && filter_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasProjection() const {
		return projection_idx != DConstants::INVALID_INDEX;
	}
};

struct SljitSourceHashJoinBuildSinkFacts {
	idx_t sink_idx = DConstants::INVALID_INDEX;
};

struct SljitSourceUngroupedAggregateFacts {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
};

struct SljitSourceFilterAggregateFacts {
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
};

struct SljitJoinFilterAggregateFacts {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t first_post_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_post_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	bool HasProjectionPrefix() const {
		return first_projection_idx != DConstants::INVALID_INDEX;
	}

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

	bool HasAnyJoinInputProjection() const {
		for (auto &join : joins) {
			if (join.HasInputProjection()) {
				return true;
			}
		}
		return false;
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

struct SljitMarkFilterProjectionNativeTailFacts {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t tail_start_idx = DConstants::INVALID_INDEX;
};

struct SljitGeneratedFilterProjectionNativeTailFacts {
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t tail_start_idx = DConstants::INVALID_INDEX;
};

struct SljitProjectionFilterProjectionNativeTailFacts {
	idx_t pre_projection_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t tail_start_idx = DConstants::INVALID_INDEX;
	bool filter_can_run_before_pre_projection = false;
};

bool SljitFullPipelineOpIsAt(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx, SljitNativeRegionOpKind kind);
bool SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx);

SljitFullPipelineScheduleFacts SljitAnalyzeFullPipelineScheduleFacts(const vector<SljitExecutableRegionOp> &ops);
bool SljitTryAnalyzeHashJoinDelimJoinSink(const vector<SljitExecutableRegionOp> &ops,
                                          SljitHashJoinDelimJoinSinkFacts &facts);
bool SljitTryAnalyzeHashJoinAppendSink(const vector<SljitExecutableRegionOp> &ops, SljitHashJoinAppendSinkFacts &facts);
bool SljitTryAnalyzeHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops, SljitHashJoinBuildSinkFacts &facts);
bool SljitTryAnalyzeSourceHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops,
                                            SljitSourceHashJoinBuildSinkFacts &facts);
bool SljitTryAnalyzeSourceUngroupedAggregate(const vector<SljitExecutableRegionOp> &ops,
                                             SljitSourceUngroupedAggregateFacts &facts);
bool SljitTryAnalyzeSourceFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                          SljitSourceFilterAggregateFacts &facts);
bool SljitTryAnalyzeJoinFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                        SljitJoinFilterAggregateFacts &facts);
bool SljitTryAnalyzeProjectionAggregatePlan(const vector<SljitExecutableRegionOp> &ops,
                                            SljitProjectionAggregatePlanFacts &plan);
bool SljitTryAnalyzeMarkFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                   SljitMarkFilterProjectionNativeTailFacts &facts);
bool SljitTryAnalyzeGeneratedFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                        SljitGeneratedFilterProjectionNativeTailFacts &facts);
bool SljitTryAnalyzeProjectionFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                         SljitProjectionFilterProjectionNativeTailFacts &facts);

} // namespace duckdb

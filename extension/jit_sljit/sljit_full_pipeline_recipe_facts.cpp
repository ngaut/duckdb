//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_facts.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_facts.hpp"

#include "sljit_projection_source_runtime.hpp"

namespace duckdb {

bool SljitFullPipelineOpIsAt(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx, SljitNativeRegionOpKind kind) {
	return op_idx < ops.size() && ops[op_idx].Kind() == kind;
}

idx_t SljitFullPipelineSourcePrefixEnd(const vector<SljitExecutableRegionOp> &ops, idx_t limit) {
	idx_t op_idx = 0;
	while (op_idx < limit && (SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER) ||
	                          SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION))) {
		op_idx++;
	}
	return op_idx;
}

static bool SljitFullPipelineLastOpIsProjectionAggregateUpdate(const vector<SljitExecutableRegionOp> &ops) {
	if (ops.empty() || !SljitFullPipelineOpIsAt(ops, ops.size() - 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE)) {
		return false;
	}
	auto kind = ops.back().AggregateUpdate().plan.sink_info.Kind();
	if (kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return true;
	}
	return kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	       ops.back().AggregateUpdate().plan.UsesPrimitivePayloads();
}

bool SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
	return SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	       ops[op_idx].HashJoinProbe().plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
}

bool SljitTryAnalyzeHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops, SljitHashJoinBuildSinkFacts &facts) {
	facts = SljitHashJoinBuildSinkFacts();
	if (ops.size() < 2 || ops.back().Kind() != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	facts.sink_idx = ops.size() - 1;
	auto final_hash_join_idx = facts.sink_idx - 1;
	if (ops[final_hash_join_idx].Kind() == SljitNativeRegionOpKind::PROJECTION) {
		facts.projection_idx = final_hash_join_idx;
		if (final_hash_join_idx == 0) {
			return false;
		}
		final_hash_join_idx--;
	}
	const auto first_hash_join_idx = SljitFullPipelineSourcePrefixEnd(ops, final_hash_join_idx);
	for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx <= final_hash_join_idx; hash_join_idx++) {
		if (!SljitFullPipelineOpIsAt(ops, hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
			return false;
		}
	}
	facts.first_hash_join_idx = first_hash_join_idx;
	facts.final_hash_join_idx = final_hash_join_idx;
	return true;
}

bool SljitTryAnalyzeJoinFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                        SljitJoinFilterAggregateFacts &facts) {
	facts = SljitJoinFilterAggregateFacts();
	if (ops.size() < 3) {
		return false;
	}
	const auto aggregate_idx = ops.size() - 1;
	idx_t filter_idx = aggregate_idx - 1;
	while (filter_idx > 0 && SljitFullPipelineOpIsAt(ops, filter_idx, SljitNativeRegionOpKind::PROJECTION)) {
		facts.first_post_join_projection_idx = filter_idx;
		filter_idx--;
	}
	if (facts.HasPostJoinProjection()) {
		facts.final_post_join_projection_idx = aggregate_idx - 1;
	}
	if (filter_idx == 0) {
		return false;
	}
	const auto hash_join_idx = filter_idx - 1;
	if (!SljitFullPipelineOpIsAt(ops, hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) ||
	    !SljitFullPipelineOpIsAt(ops, filter_idx, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, aggregate_idx, SljitNativeRegionOpKind::AGGREGATE_UPDATE) ||
	    !ops[aggregate_idx].AggregateUpdate().plan.UsesPrimitivePayloads()) {
		return false;
	}
	if (SljitFullPipelineSourcePrefixEnd(ops, hash_join_idx) != hash_join_idx) {
		return false;
	}
	facts.hash_join_idx = hash_join_idx;
	facts.filter_idx = filter_idx;
	facts.aggregate_idx = aggregate_idx;
	return true;
}

static bool SljitTryAnalyzeProjectionAggregateSuffix(const vector<SljitExecutableRegionOp> &ops,
                                                     SljitFullPipelineProjectionAggregateShape &shape) {
	shape = SljitFullPipelineProjectionAggregateShape();
	if (!SljitFullPipelineLastOpIsProjectionAggregateUpdate(ops)) {
		return false;
	}
	shape.aggregate_idx = ops.size() - 1;
	shape.final_projection_idx = shape.aggregate_idx - 1;
	if (shape.final_projection_idx >= ops.size() ||
	    !SljitFullPipelineOpIsAt(ops, shape.final_projection_idx, SljitNativeRegionOpKind::PROJECTION)) {
		shape.first_projection_idx = DConstants::INVALID_INDEX;
		shape.final_projection_idx = DConstants::INVALID_INDEX;
		return true;
	}
	shape.first_projection_idx = shape.final_projection_idx;
	while (shape.first_projection_idx > 0 &&
	       SljitFullPipelineOpIsAt(ops, shape.first_projection_idx - 1, SljitNativeRegionOpKind::PROJECTION)) {
		shape.first_projection_idx--;
	}
	return true;
}

static bool SljitTryAnalyzeProjectionAggregatePrefix(const vector<SljitExecutableRegionOp> &ops,
                                                     const SljitFullPipelineProjectionAggregateShape &shape,
                                                     SljitProjectionAggregatePrefixFacts &facts) {
	facts = SljitProjectionAggregatePrefixFacts();
	idx_t op_idx = 0;
	const auto prefix_end = shape.ProjectionCount() == 0 ? shape.aggregate_idx : shape.first_projection_idx;
	if (prefix_end == 0) {
		return true;
	}

	if (op_idx < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER)) {
		facts.source_filter_idx = op_idx++;
		if (op_idx < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION)) {
			facts.source_projection_idx = op_idx++;
		}
		if (op_idx == prefix_end) {
			// A source prefix without joins: the source recipes run it before the aggregate projections.
			return true;
		}
	}

	while (op_idx < prefix_end) {
		SljitProjectionAggregateJoinPrefixStep join;
		if (op_idx + 1 < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION) &&
		    SljitFullPipelineOpIsAt(ops, op_idx + 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
			join.input_projection_idx = op_idx++;
		}
		if (op_idx >= prefix_end || !SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
			return false;
		}
		join.hash_join_idx = op_idx++;
		facts.joins.push_back(join);
		if (op_idx == prefix_end) {
			return true;
		}
		if (SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER)) {
			// Only a MARK probe's marker filter belongs to the join prefix. Any other filter after a join is not a
			// prefix the projection-aggregate recipes can own: none of them evaluates it.
			if (!SljitIsMarkProbeMarkerFilter(ops[join.hash_join_idx], ops[op_idx])) {
				return false;
			}
			facts.mark_filter_idx = op_idx++;
			return op_idx == prefix_end;
		}
	}
	return true;
}

bool SljitTryAnalyzeProjectionAggregatePlan(const vector<SljitExecutableRegionOp> &ops,
                                            SljitProjectionAggregatePlanFacts &plan) {
	plan = SljitProjectionAggregatePlanFacts();
	if (!SljitTryAnalyzeProjectionAggregateSuffix(ops, plan.shape)) {
		return false;
	}
	if (!SljitTryAnalyzeProjectionAggregatePrefix(ops, plan.shape, plan.prefix)) {
		return false;
	}
	return true;
}

} // namespace duckdb

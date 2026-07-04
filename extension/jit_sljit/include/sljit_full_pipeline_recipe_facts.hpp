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
#include "sljit_projection_chain_runtime.hpp"

namespace duckdb {

static bool SljitFullPipelineOpIsAt(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx,
                                    SljitNativeRegionOpKind kind) {
	return op_idx < ops.size() && ops[op_idx].kind == kind;
}

static bool SljitFullPipelineLastOpIsGroupedAggregateUpdate(const vector<SljitExecutableRegionOp> &ops) {
	if (ops.empty() || !SljitFullPipelineOpIsAt(ops, ops.size() - 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE)) {
		return false;
	}
	auto kind = ops.back().aggregate_update.plan.sink_info.kind;
	return kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	       kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
}

static bool SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(const vector<SljitExecutableRegionOp> &ops,
                                                                 idx_t op_idx) {
	return SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	       ops[op_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
}

static bool SljitFullPipelineProjectionAggregateShapeHasFixedFinalProjection(
    const vector<SljitExecutableRegionOp> &ops, const SljitFullPipelineProjectionAggregateShape &shape) {
	return shape.final_projection_idx < ops.size() &&
	       SljitProjectionOutputsAreFixedWidth(ops[shape.final_projection_idx]);
}

struct SljitFilteredSourceAggregateFacts {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
};

struct SljitSelectedJoinAggregateFacts {
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t second_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	bool HasSecondHashJoin() const {
		return second_hash_join_idx != DConstants::INVALID_INDEX;
	}
};

struct SljitHashJoinDelimJoinSinkFacts {
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t final_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;
};

struct SljitSourceBatchNativeTailFacts {
	idx_t boundary_op_idx = DConstants::INVALID_INDEX;
	idx_t tail_start_idx = DConstants::INVALID_INDEX;
};

struct SljitProjectionAggregatePrefixFacts {
	idx_t source_filter_idx = DConstants::INVALID_INDEX;
	idx_t source_projection_idx = DConstants::INVALID_INDEX;
	idx_t pre_join_projection_idx = DConstants::INVALID_INDEX;
	idx_t first_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t between_projection_idx = DConstants::INVALID_INDEX;
	idx_t second_hash_join_idx = DConstants::INVALID_INDEX;
	idx_t mark_filter_idx = DConstants::INVALID_INDEX;

	bool HasSourceFilterProjection() const {
		return source_filter_idx != DConstants::INVALID_INDEX && source_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasPreJoinProjection() const {
		return pre_join_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasFirstHashJoin() const {
		return first_hash_join_idx != DConstants::INVALID_INDEX;
	}

	bool HasSecondHashJoin() const {
		return second_hash_join_idx != DConstants::INVALID_INDEX;
	}

	bool HasBetweenProjection() const {
		return between_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasMarkFilter() const {
		return mark_filter_idx != DConstants::INVALID_INDEX;
	}
};

struct SljitProjectionAggregatePlanFacts {
	SljitFullPipelineProjectionAggregateShape shape;
	SljitProjectionAggregatePrefixFacts prefix;
	bool final_projection_fixed_width = false;

	idx_t ProjectionCount() const {
		return shape.ProjectionCount();
	}

	bool HasFixedFinalProjection() const {
		return final_projection_fixed_width;
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
};

static bool SljitTryAnalyzeFilteredSourceAggregate(const vector<SljitExecutableRegionOp> &ops,
                                                   bool uses_scan_filters,
                                                   SljitFilteredSourceAggregateFacts &facts) {
	facts = SljitFilteredSourceAggregateFacts();
	if (!uses_scan_filters || ops.size() != 2 ||
	    !SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::HASH_JOIN_PROBE) ||
	    !SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE) ||
	    ops[1].aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return false;
	}
	facts.hash_join_idx = 0;
	facts.aggregate_idx = 1;
	return true;
}

static bool SljitTryAnalyzeSelectedJoinAggregate(const vector<SljitExecutableRegionOp> &ops,
                                                 SljitSelectedJoinAggregateFacts &facts) {
	facts = SljitSelectedJoinAggregateFacts();
	if (ops.size() == 2 && SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	    SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE)) {
		facts.first_hash_join_idx = 0;
		facts.aggregate_idx = 1;
		return true;
	}
	if (ops.size() == 3 && SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	    SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	    SljitFullPipelineOpIsAt(ops, 2, SljitNativeRegionOpKind::AGGREGATE_UPDATE)) {
		facts.first_hash_join_idx = 0;
		facts.second_hash_join_idx = 1;
		facts.aggregate_idx = 2;
		return true;
	}
	return false;
}

static bool SljitTryAnalyzeHashJoinDelimJoinSink(const vector<SljitExecutableRegionOp> &ops,
                                                 SljitHashJoinDelimJoinSinkFacts &facts) {
	facts = SljitHashJoinDelimJoinSinkFacts();
	if (ops.size() < 2 || ops.back().kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK) {
		return false;
	}
	const auto sink_idx = ops.size() - 1;
	for (idx_t hash_join_idx = 0; hash_join_idx < sink_idx; hash_join_idx++) {
		if (!SljitFullPipelineOpIsAt(ops, hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
			return false;
		}
	}
	facts.first_hash_join_idx = 0;
	facts.final_hash_join_idx = sink_idx - 1;
	facts.sink_idx = sink_idx;
	return true;
}

static bool SljitTryAnalyzeSourceBatchNativeTail(const vector<SljitExecutableRegionOp> &ops, bool uses_scan_filters,
                                                 SljitSourceBatchNativeTailFacts &facts) {
	facts = SljitSourceBatchNativeTailFacts();
	if (ops.empty()) {
		return false;
	}
	const bool scan_filtered_aggregate_terminal =
	    uses_scan_filters && ops.back().kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	    (ops.back().aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	     ops.back().aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	const bool first_hash_join_native_tail = ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	if (!scan_filtered_aggregate_terminal && !first_hash_join_native_tail) {
		return false;
	}
	facts.boundary_op_idx = 0;
	facts.tail_start_idx = 0;
	return true;
}

static bool SljitTryAnalyzeProjectionAggregateSuffix(const vector<SljitExecutableRegionOp> &ops,
                                                     SljitFullPipelineProjectionAggregateShape &shape) {
	shape = SljitFullPipelineProjectionAggregateShape();
	if (!SljitFullPipelineLastOpIsGroupedAggregateUpdate(ops)) {
		return false;
	}
	shape.aggregate_idx = ops.size() - 1;
	shape.final_projection_idx = shape.aggregate_idx - 1;
	if (shape.final_projection_idx >= ops.size() ||
	    !SljitFullPipelineOpIsAt(ops, shape.final_projection_idx, SljitNativeRegionOpKind::PROJECTION)) {
		return false;
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
	const auto prefix_end = shape.first_projection_idx;
	if (prefix_end == 0) {
		return true;
	}

	if (op_idx + 1 < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER) &&
	    SljitFullPipelineOpIsAt(ops, op_idx + 1, SljitNativeRegionOpKind::PROJECTION)) {
		facts.source_filter_idx = op_idx;
		facts.source_projection_idx = op_idx + 1;
		op_idx += 2;
	}
	if (op_idx + 1 < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION) &&
	    SljitFullPipelineOpIsAt(ops, op_idx + 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
		facts.pre_join_projection_idx = op_idx;
		op_idx++;
	}
	if (op_idx >= prefix_end || !SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
		return false;
	}
	facts.first_hash_join_idx = op_idx++;
	if (op_idx == prefix_end) {
		return true;
	}
	if (SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER)) {
		facts.mark_filter_idx = op_idx++;
		return op_idx == prefix_end;
	}
	if (op_idx + 1 < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION) &&
	    SljitFullPipelineOpIsAt(ops, op_idx + 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
		facts.between_projection_idx = op_idx++;
	}
	if (op_idx >= prefix_end || !SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
		return false;
	}
	facts.second_hash_join_idx = op_idx++;
	if (op_idx == prefix_end) {
		return true;
	}
	if (SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER)) {
		facts.mark_filter_idx = op_idx++;
		return op_idx == prefix_end;
	}
	return false;
}

static bool SljitTryAnalyzeProjectionAggregatePlan(const vector<SljitExecutableRegionOp> &ops,
                                                   SljitProjectionAggregatePlanFacts &plan) {
	plan = SljitProjectionAggregatePlanFacts();
	if (!SljitTryAnalyzeProjectionAggregateSuffix(ops, plan.shape)) {
		return false;
	}
	if (!SljitTryAnalyzeProjectionAggregatePrefix(ops, plan.shape, plan.prefix)) {
		return false;
	}
	plan.final_projection_fixed_width =
	    SljitFullPipelineProjectionAggregateShapeHasFixedFinalProjection(ops, plan.shape);
	return true;
}

static bool
SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate(const vector<SljitExecutableRegionOp> &ops,
                                                              const SljitProjectionAggregatePlanFacts &plan) {
	auto &shape = plan.shape;
	return !plan.prefix.HasFirstHashJoin() && plan.ProjectionCount() == 1 && plan.HasFixedFinalProjection() &&
	       SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx) &&
	       SljitChooseGroupedAggregateUpdateStrategy(ops[shape.aggregate_idx]) ==
	           SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
}

static bool SljitProjectionAggregateCanUseSingleJoinMarkFilterBoundary(const vector<SljitExecutableRegionOp> &ops,
                                                                       const SljitProjectionAggregatePlanFacts &plan) {
	auto &shape = plan.shape;
	auto &facts = plan.prefix;
	return facts.HasFirstHashJoin() && !facts.HasSecondHashJoin() && facts.HasMarkFilter() &&
	       !facts.HasSourceFilterProjection() && !facts.HasPreJoinProjection() &&
	       facts.mark_filter_idx == facts.first_hash_join_idx + 1 && shape.ProjectionCount() == 1 &&
	       plan.HasFixedFinalProjection() &&
	       ops[facts.first_hash_join_idx].hash_join_probe.plan.output_mode ==
	           ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
	       SljitIsMarkProbeMarkerFilter(ops[facts.first_hash_join_idx], ops[facts.mark_filter_idx]);
}

static bool
SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection(const SljitProjectionAggregatePlanFacts &plan) {
	auto &facts = plan.prefix;
	return facts.HasFirstHashJoin() && !facts.HasSecondHashJoin() && facts.HasSourceFilterProjection() &&
	       !facts.HasPreJoinProjection() && !facts.HasMarkFilter() && plan.ProjectionCount() == 1;
}

static bool SljitProjectionAggregateCanUseSingleJoinPreJoinProjection(const SljitProjectionAggregatePlanFacts &plan) {
	auto &facts = plan.prefix;
	return facts.HasFirstHashJoin() && !facts.HasSecondHashJoin() && facts.HasPreJoinProjection() &&
	       !facts.HasSourceFilterProjection() && !facts.HasMarkFilter();
}

static bool SljitProjectionAggregateCanUseSingleJoinSingleProjection(const SljitProjectionAggregatePlanFacts &plan) {
	auto &facts = plan.prefix;
	return facts.HasFirstHashJoin() && !facts.HasSecondHashJoin() && !facts.HasSourceFilterProjection() &&
	       !facts.HasPreJoinProjection() && !facts.HasMarkFilter() && plan.ProjectionCount() == 1;
}

static bool SljitProjectionAggregateCanUseSingleJoinProjectionChain(const SljitProjectionAggregatePlanFacts &plan) {
	auto &facts = plan.prefix;
	return facts.HasFirstHashJoin() && !facts.HasSecondHashJoin() && !facts.HasSourceFilterProjection() &&
	       !facts.HasPreJoinProjection() && !facts.HasMarkFilter() && plan.ProjectionCount() == 2 &&
	       plan.HasFixedFinalProjection();
}

static bool SljitTryAnalyzeMarkFilterProjectionNativeTail(
    const vector<SljitExecutableRegionOp> &ops, SljitMarkFilterProjectionNativeTailFacts &facts) {
	facts = SljitMarkFilterProjectionNativeTailFacts();
	if (ops.size() < 4 || !SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::HASH_JOIN_PROBE) ||
	    !SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, 2, SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	if (ops[0].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE ||
	    !SljitIsMarkProbeMarkerFilter(ops[0], ops[1])) {
		return false;
	}

	idx_t tail_start_idx = 2;
	while (tail_start_idx < ops.size() &&
	       SljitFullPipelineOpIsAt(ops, tail_start_idx, SljitNativeRegionOpKind::PROJECTION)) {
		tail_start_idx++;
	}
	if (tail_start_idx <= 2 || tail_start_idx >= ops.size()) {
		return false;
	}

	facts.hash_join_idx = 0;
	facts.filter_idx = 1;
	facts.first_projection_idx = 2;
	facts.final_projection_idx = tail_start_idx - 1;
	facts.tail_start_idx = tail_start_idx;
	return true;
}

static bool SljitTryAnalyzeGeneratedFilterProjectionNativeTail(
    const vector<SljitExecutableRegionOp> &ops, SljitGeneratedFilterProjectionNativeTailFacts &facts) {
	facts = SljitGeneratedFilterProjectionNativeTailFacts();
	if (ops.size() <= 2 || !SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	facts.filter_idx = 0;
	facts.projection_idx = 1;
	facts.tail_start_idx = 2;
	return true;
}

static bool SljitTryAnalyzeProjectionFilterProjectionNativeTail(
    const vector<SljitExecutableRegionOp> &ops, SljitProjectionFilterProjectionNativeTailFacts &facts) {
	facts = SljitProjectionFilterProjectionNativeTailFacts();
	if (ops.size() <= 3 || !SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::PROJECTION) ||
	    !SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, 2, SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	facts.pre_projection_idx = 0;
	facts.filter_idx = 1;
	facts.projection_idx = 2;
	facts.tail_start_idx = 3;
	return true;
}

} // namespace duckdb

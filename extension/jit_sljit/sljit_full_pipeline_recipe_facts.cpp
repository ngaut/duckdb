//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_facts.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

bool SljitFullPipelineOpIsAt(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx, SljitNativeRegionOpKind kind) {
	return op_idx < ops.size() && ops[op_idx].kind == kind;
}

static bool SljitFullPipelineLastOpIsProjectionAggregateUpdate(const vector<SljitExecutableRegionOp> &ops) {
	if (ops.empty() || !SljitFullPipelineOpIsAt(ops, ops.size() - 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE)) {
		return false;
	}
	auto kind = ops.back().aggregate_update.plan.sink_info.kind;
	if (kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return true;
	}
	return kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	       ops.back().aggregate_update.plan.UsesPrimitivePayloads();
}

bool SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
	return SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
	       ops[op_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
}

static bool SljitFullPipelineOpIsUngroupedPrimitiveAggregateUpdate(const SljitExecutableRegionOp &op) {
	return op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	       op.aggregate_update.plan.UsesPrimitivePayloads();
}

SljitFullPipelineScheduleFacts SljitAnalyzeFullPipelineScheduleFacts(const vector<SljitExecutableRegionOp> &ops) {
	SljitFullPipelineScheduleFacts facts;
	bool has_hash_join_probe = false;
	for (auto &op : ops) {
		if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			has_hash_join_probe = true;
			continue;
		}
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			facts.uses_extended_source_fetch_budget =
			    SljitFullPipelineOpIsUngroupedPrimitiveAggregateUpdate(op) ||
			    (has_hash_join_probe &&
			     op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
			return facts;
		}
	}
	if (ops.empty()) {
		return facts;
	}
	switch (ops.back().kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		facts.uses_extended_source_fetch_budget = true;
		break;
	default:
		break;
	}
	return facts;
}

bool SljitTryAnalyzeHashJoinDelimJoinSink(const vector<SljitExecutableRegionOp> &ops,
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

bool SljitTryAnalyzeHashJoinAppendSink(const vector<SljitExecutableRegionOp> &ops,
                                       SljitHashJoinAppendSinkFacts &facts) {
	facts = SljitHashJoinAppendSinkFacts();
	if (ops.size() < 2 || ops.back().kind != SljitNativeRegionOpKind::APPEND_SINK) {
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

bool SljitTryAnalyzeHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops, SljitHashJoinBuildSinkFacts &facts) {
	facts = SljitHashJoinBuildSinkFacts();
	if (ops.size() < 2 || ops.back().kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	facts.sink_idx = ops.size() - 1;
	auto final_hash_join_idx = facts.sink_idx - 1;
	if (ops[final_hash_join_idx].kind == SljitNativeRegionOpKind::PROJECTION) {
		facts.projection_idx = final_hash_join_idx;
		if (final_hash_join_idx == 0) {
			return false;
		}
		final_hash_join_idx--;
	}
	idx_t first_hash_join_idx = 0;
	if (SljitFullPipelineOpIsAt(ops, first_hash_join_idx, SljitNativeRegionOpKind::PROJECTION)) {
		facts.pre_projection_idx = first_hash_join_idx++;
	}
	if (first_hash_join_idx + 1 <= final_hash_join_idx &&
	    SljitFullPipelineOpIsAt(ops, first_hash_join_idx, SljitNativeRegionOpKind::FILTER) &&
	    SljitFullPipelineOpIsAt(ops, first_hash_join_idx + 1, SljitNativeRegionOpKind::PROJECTION)) {
		facts.filter_idx = first_hash_join_idx;
		facts.filter_projection_idx = first_hash_join_idx + 1;
		first_hash_join_idx += 2;
	}
	if (!SljitFullPipelineOpIsAt(ops, first_hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
		return false;
	}
	for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx <= final_hash_join_idx; hash_join_idx++) {
		if (!SljitFullPipelineOpIsAt(ops, hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE)) {
			return false;
		}
	}
	facts.first_hash_join_idx = first_hash_join_idx;
	facts.final_hash_join_idx = final_hash_join_idx;
	return true;
}

bool SljitTryAnalyzeSourceHashJoinBuildSink(const vector<SljitExecutableRegionOp> &ops,
                                            SljitSourceHashJoinBuildSinkFacts &facts) {
	facts = SljitSourceHashJoinBuildSinkFacts();
	if (ops.empty() || ops.back().kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		switch (ops[op_idx].kind) {
		case SljitNativeRegionOpKind::FILTER:
		case SljitNativeRegionOpKind::PROJECTION:
			break;
		default:
			return false;
		}
	}
	facts.sink_idx = ops.size() - 1;
	return true;
}

bool SljitTryAnalyzeSourceUngroupedAggregate(const vector<SljitExecutableRegionOp> &ops,
                                             SljitSourceUngroupedAggregateFacts &facts) {
	facts = SljitSourceUngroupedAggregateFacts();
	if (ops.size() != 1 || !SljitFullPipelineOpIsUngroupedPrimitiveAggregateUpdate(ops[0])) {
		return false;
	}
	facts.aggregate_idx = 0;
	return true;
}

bool SljitTryAnalyzeSourceFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                          SljitSourceFilterAggregateFacts &facts) {
	facts = SljitSourceFilterAggregateFacts();
	if (ops.size() != 2 || !SljitFullPipelineOpIsAt(ops, 0, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, 1, SljitNativeRegionOpKind::AGGREGATE_UPDATE) ||
	    !ops[1].aggregate_update.plan.UsesPrimitivePayloads()) {
		return false;
	}
	facts.filter_idx = 0;
	facts.aggregate_idx = 1;
	return true;
}

bool SljitTryAnalyzeJoinFilterAggregate(const vector<SljitExecutableRegionOp> &ops,
                                        SljitJoinFilterAggregateFacts &facts) {
	facts = SljitJoinFilterAggregateFacts();
	if (ops.size() < 3) {
		return false;
	}
	const auto aggregate_idx = ops.size() - 1;
	const auto filter_idx = aggregate_idx - 1;
	const auto hash_join_idx = filter_idx - 1;
	if (!SljitFullPipelineOpIsAt(ops, hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) ||
	    !SljitFullPipelineOpIsAt(ops, filter_idx, SljitNativeRegionOpKind::FILTER) ||
	    !SljitFullPipelineOpIsAt(ops, aggregate_idx, SljitNativeRegionOpKind::AGGREGATE_UPDATE) ||
	    !ops[aggregate_idx].aggregate_update.plan.UsesPrimitivePayloads()) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx < hash_join_idx; op_idx++) {
		if (!SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::PROJECTION)) {
			return false;
		}
	}
	if (hash_join_idx > 0) {
		facts.first_projection_idx = 0;
		facts.final_projection_idx = hash_join_idx - 1;
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

	if (op_idx + 1 < prefix_end && SljitFullPipelineOpIsAt(ops, op_idx, SljitNativeRegionOpKind::FILTER) &&
	    SljitFullPipelineOpIsAt(ops, op_idx + 1, SljitNativeRegionOpKind::PROJECTION)) {
		facts.source_filter_idx = op_idx;
		facts.source_projection_idx = op_idx + 1;
		op_idx += 2;
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

bool SljitTryAnalyzeMarkFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                   SljitMarkFilterProjectionNativeTailFacts &facts) {
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

bool SljitTryAnalyzeGeneratedFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                        SljitGeneratedFilterProjectionNativeTailFacts &facts) {
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

static void SljitAddUniqueExpressionSource(vector<idx_t> &sources, idx_t source_idx) {
	for (auto existing : sources) {
		if (existing == source_idx) {
			return;
		}
	}
	sources.push_back(source_idx);
}

static bool SljitCollectExpressionSourceIndices(const SljitExecutableRegionExpression &expr, vector<idx_t> &sources) {
	if (!expr.input_source_indices.empty()) {
		for (auto source_idx : expr.input_source_indices) {
			SljitAddUniqueExpressionSource(sources, source_idx);
		}
		return true;
	}
	auto &plan = expr.plan;
	if (!plan.expression_tree_source_indices.empty()) {
		for (auto source_idx : plan.expression_tree_source_indices) {
			SljitAddUniqueExpressionSource(sources, source_idx);
		}
		return true;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return true;
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		SljitAddUniqueExpressionSource(sources, plan.source_index);
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY:
		SljitAddUniqueExpressionSource(sources, plan.source_index);
		SljitAddUniqueExpressionSource(sources, plan.right_source_index);
		return true;
	default:
		return false;
	}
}

static bool SljitProjectionPreservesSourceColumn(const SljitExecutableRegionOp &projection, idx_t source_idx) {
	if (source_idx >= projection.projections.size() || source_idx >= projection.input_types.size() ||
	    source_idx >= projection.output_types.size()) {
		return false;
	}
	auto &plan = projection.projections[source_idx].plan;
	return plan.kind == SljitNativeRegionExpressionKind::REFERENCE && plan.source_index == source_idx &&
	       plan.return_type == projection.input_types[source_idx] &&
	       projection.output_types[source_idx] == projection.input_types[source_idx];
}

static bool SljitFilterCanRunBeforePreProjection(const vector<SljitExecutableRegionOp> &ops, idx_t pre_projection_idx,
                                                 idx_t filter_idx) {
	if (!SljitFullPipelineOpIsAt(ops, pre_projection_idx, SljitNativeRegionOpKind::PROJECTION) ||
	    !SljitFullPipelineOpIsAt(ops, filter_idx, SljitNativeRegionOpKind::FILTER)) {
		return false;
	}
	vector<idx_t> filter_sources;
	if (!SljitCollectExpressionSourceIndices(ops[filter_idx].filter, filter_sources) || filter_sources.empty()) {
		return false;
	}
	for (auto source_idx : filter_sources) {
		if (!SljitProjectionPreservesSourceColumn(ops[pre_projection_idx], source_idx)) {
			return false;
		}
	}
	return true;
}

bool SljitTryAnalyzeProjectionFilterProjectionNativeTail(const vector<SljitExecutableRegionOp> &ops,
                                                         SljitProjectionFilterProjectionNativeTailFacts &facts) {
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
	facts.filter_can_run_before_pre_projection =
	    SljitFilterCanRunBeforePreProjection(ops, facts.pre_projection_idx, facts.filter_idx);
	return true;
}

} // namespace duckdb

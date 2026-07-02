//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_routes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_second_join_input_runtime.hpp"
#include "sljit_full_pipeline_route_kind.hpp"
#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_chain_runtime.hpp"

#include "duckdb/storage/table/direct_append_stats.hpp"

#include <array>

namespace duckdb {

class SljitFullPipelineRouteSelector {
public:
	SljitFullPipelineRouteSelector(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_scan_filters_p)
	    : ops(ops_p), source_min_values(source_min_values_p), source_max_values(source_max_values_p),
	      uses_scan_filters(uses_scan_filters_p) {
	}

	bool OpIsAt(idx_t op_idx, SljitNativeRegionOpKind kind) const {
		return op_idx < ops.size() && ops[op_idx].kind == kind;
	}

	bool LastOpIs(SljitNativeRegionOpKind kind) const {
		return !ops.empty() && ops.back().kind == kind;
	}

	template <class... KINDS>
	bool OpsAre(KINDS... kinds) const {
		const SljitNativeRegionOpKind expected[] = {static_cast<SljitNativeRegionOpKind>(kinds)...};
		const idx_t expected_count = sizeof...(KINDS);
		if (ops.size() != expected_count) {
			return false;
		}
		for (idx_t op_idx = 0; op_idx < expected_count; op_idx++) {
			if (!OpIsAt(op_idx, expected[op_idx])) {
				return false;
			}
		}
		return true;
	}

	template <class... KINDS>
	bool OpsPrefixIs(KINDS... kinds) const {
		const SljitNativeRegionOpKind expected[] = {static_cast<SljitNativeRegionOpKind>(kinds)...};
		const idx_t expected_count = sizeof...(KINDS);
		if (ops.size() < expected_count) {
			return false;
		}
		for (idx_t op_idx = 0; op_idx < expected_count; op_idx++) {
			if (!OpIsAt(op_idx, expected[op_idx])) {
				return false;
			}
		}
		return true;
	}

	bool IsAggregateUpdateAt(idx_t op_idx, ExecutionRegionSinkKind sink_kind) const {
		return OpIsAt(op_idx, SljitNativeRegionOpKind::AGGREGATE_UPDATE) &&
		       ops[op_idx].aggregate_update.plan.sink_info.kind == sink_kind;
	}

	bool LastOpIsHashAggregateUpdate() const {
		return !ops.empty() && IsAggregateUpdateAt(ops.size() - 1, ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	}

	bool HashJoinProbeIsMatchedProbeAndBuild(idx_t op_idx) const {
		return OpIsAt(op_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		       ops[op_idx].hash_join_probe.plan.output_mode ==
		           ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
	}

	bool OpsRangeIs(idx_t begin, idx_t end, SljitNativeRegionOpKind kind) const {
		if (begin > end || end > ops.size()) {
			return false;
		}
		for (idx_t op_idx = begin; op_idx < end; op_idx++) {
			if (!OpIsAt(op_idx, kind)) {
				return false;
			}
		}
		return true;
	}

	bool OpsRangeContains(idx_t begin, idx_t end, SljitNativeRegionOpKind kind) const {
		if (begin > end || end > ops.size()) {
			return false;
		}
		for (idx_t op_idx = begin; op_idx < end; op_idx++) {
			if (OpIsAt(op_idx, kind)) {
				return true;
			}
		}
		return false;
	}

	bool TryBuildProjectionAggregateShape(idx_t first_projection_idx,
	                                      SljitFullPipelineProjectionAggregateShape &shape) const {
		shape = SljitFullPipelineProjectionAggregateShape();
		if (!LastOpIsHashAggregateUpdate() || first_projection_idx >= ops.size() - 1 ||
		    !OpsRangeIs(first_projection_idx, ops.size() - 1, SljitNativeRegionOpKind::PROJECTION)) {
			return false;
		}
		shape.first_projection_idx = first_projection_idx;
		shape.final_projection_idx = ops.size() - 2;
		shape.aggregate_idx = ops.size() - 1;
		return true;
	}

	bool ProjectionAggregateShapeHasFixedFinalProjection(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return shape.final_projection_idx < ops.size() &&
		       SljitProjectionOutputsAreFixedWidth(ops[shape.final_projection_idx]);
	}

	bool FullPipelineProjectionCountStarGroupedAggregate(const SljitFullPipelineProjectionAggregateShape &shape) const {
		if (shape.ProjectionCount() != 1 || shape.first_projection_idx >= ops.size() ||
		    shape.aggregate_idx >= ops.size() || ops[shape.first_projection_idx].output_types.size() != 1 ||
		    !ProjectionAggregateShapeHasFixedFinalProjection(shape)) {
			return false;
		}
		auto &projection_op = ops[shape.first_projection_idx];
		auto &aggregate_op = ops[shape.aggregate_idx];
		auto &aggregate_update = aggregate_op.aggregate_update.plan;
		auto &sink_info = aggregate_update.sink_info;
		if (!aggregate_update.use_primitive_payloads || !aggregate_update.use_grouped_state_addresses ||
		    aggregate_update.use_perfect_hash_group_lookup || sink_info.groups.size() != 1 ||
		    sink_info.groups[0].input_index != 0 || sink_info.groups[0].type != projection_op.output_types[0] ||
		    sink_info.aggregates.size() != 1 || aggregate_op.aggregate_update.payloads.size() != 1) {
			return false;
		}
		auto &aggregate = sink_info.aggregates[0];
		return aggregate.child_count == 0 &&
		       aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
	}

	bool TryBuildFixedProjectionAggregateShape(idx_t first_projection_idx, idx_t projection_count,
	                                           SljitFullPipelineProjectionAggregateShape &shape) const {
		return TryBuildProjectionAggregateShape(first_projection_idx, shape) &&
		       shape.ProjectionCount() == projection_count && ProjectionAggregateShapeHasFixedFinalProjection(shape);
	}

	bool GeneratedFilterProjectionHashJoinBuildSinkShape() const {
		return OpsAre(SljitNativeRegionOpKind::FILTER, SljitNativeRegionOpKind::PROJECTION,
		              SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::HASH_JOIN_BUILD) ||
		       OpsAre(SljitNativeRegionOpKind::FILTER, SljitNativeRegionOpKind::PROJECTION,
		              SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::PROJECTION,
		              SljitNativeRegionOpKind::HASH_JOIN_BUILD);
	}

	bool GeneratedProjectionFilterProjectionHashJoinBuildSinkShape() const {
		return OpsAre(SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::FILTER,
		              SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_BUILD);
	}

	bool HashJoinBuildSinkShape() const {
		return ops.size() >= 2 && LastOpIs(SljitNativeRegionOpKind::HASH_JOIN_BUILD) &&
		       OpsRangeContains(0, ops.size() - 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	}

	bool HashJoinAppendSinkShape() const {
		return ops.size() >= 2 && LastOpIs(SljitNativeRegionOpKind::APPEND_SINK) &&
		       OpsRangeIs(0, ops.size() - 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	}

	bool HashJoinDelimJoinSinkShape() const {
		return ops.size() >= 2 && LastOpIs(SljitNativeRegionOpKind::DELIM_JOIN_SINK) &&
		       OpsRangeIs(0, ops.size() - 1, SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	}

	SljitFullPipelineRouteKind SelectFullPipelineRouteKind() const {
		SljitFullPipelineProjectionAggregateShape shape;

		if (uses_scan_filters &&
		    OpsAre(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::AGGREGATE_UPDATE) &&
		    IsAggregateUpdateAt(1, ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE)) {
			return SljitFullPipelineRouteKind::FILTERED_SOURCE_AGGREGATE;
		}

		if (OpIsAt(0, SljitNativeRegionOpKind::FILTER) && TryBuildProjectionAggregateShape(1, shape) &&
		    shape.ProjectionCount() == 1 && ProjectionAggregateShapeHasFixedFinalProjection(shape)) {
			return SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION;
		}
		if (OpsAre(SljitNativeRegionOpKind::FILTER, SljitNativeRegionOpKind::PROJECTION,
		           SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::AGGREGATE_UPDATE) &&
		    IsAggregateUpdateAt(3, ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE)) {
			return SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION;
		}

		if (TryBuildProjectionAggregateShape(0, shape) && FullPipelineProjectionCountStarGroupedAggregate(shape)) {
			return SljitFullPipelineRouteKind::PROJECTION_COUNT_STAR_GROUPED_AGGREGATE;
		}

		if (OpIsAt(0, SljitNativeRegionOpKind::HASH_JOIN_PROBE) && TryBuildProjectionAggregateShape(1, shape) &&
		    shape.ProjectionCount() == 1) {
			return SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_GROUPED_AGGREGATE;
		}

		if (HashJoinDelimJoinSinkShape()) {
			return SljitFullPipelineRouteKind::HASH_JOIN_DELIM_JOIN_SINK;
		}

		if (GeneratedFilterProjectionHashJoinBuildSinkShape()) {
			return SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK;
		}

		if (GeneratedProjectionFilterProjectionHashJoinBuildSinkShape()) {
			return SljitFullPipelineRouteKind::GENERATED_PROJECTION_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK;
		}

		if (HashJoinBuildSinkShape()) {
			return SljitFullPipelineRouteKind::HASH_JOIN_BUILD_SINK;
		}

		if (HashJoinAppendSinkShape()) {
			return SljitFullPipelineRouteKind::HASH_JOIN_APPEND_SINK;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildFixedProjectionAggregateShape(1, 2, shape)) {
			return SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::FILTER) &&
		    TryBuildFixedProjectionAggregateShape(2, 1, shape) &&
		    ops[0].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		    SljitIsMarkProbeMarkerFilter(ops[0], ops[1])) {
			return SljitFullPipelineRouteKind::MARK_HASH_JOIN_FILTER_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildProjectionAggregateShape(2, shape)) {
			return SljitFullPipelineRouteKind::PROJECTION_HASH_JOIN_PROJECTION_CHAIN_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::PROJECTION,
		                SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildProjectionAggregateShape(3, shape) && shape.ProjectionCount() >= 2 &&
		    HashJoinProbeIsMatchedProbeAndBuild(0) && HashJoinProbeIsMatchedProbeAndBuild(2) &&
		    ProjectionAggregateShapeHasFixedFinalProjection(shape)) {
			return SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTIONS_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_PROBE,
		                SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildFixedProjectionAggregateShape(4, 2, shape) && SljitOutputTypesAreFixedWidth(ops[1].output_types)) {
			return SljitFullPipelineRouteKind::
			    PROJECTION_HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildFixedProjectionAggregateShape(2, 1, shape) && HashJoinProbeIsMatchedProbeAndBuild(0) &&
		    HashJoinProbeIsMatchedProbeAndBuild(1)) {
			return SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildFixedProjectionAggregateShape(2, 2, shape)) {
			return SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::FILTER, SljitNativeRegionOpKind::PROJECTION,
		                SljitNativeRegionOpKind::HASH_JOIN_PROBE) &&
		    TryBuildProjectionAggregateShape(3, shape) && shape.ProjectionCount() == 1) {
			return SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE;
		}

		if (OpsPrefixIs(SljitNativeRegionOpKind::HASH_JOIN_PROBE, SljitNativeRegionOpKind::HASH_JOIN_PROBE,
		                SljitNativeRegionOpKind::FILTER) &&
		    TryBuildProjectionAggregateShape(3, shape) &&
		    ops[1].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
			return SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_FILTER_PROJECTION_CHAIN_GROUPED_AGGREGATE;
		}

		return SljitFullPipelineRouteKind::NONE;
	}

	bool TryBuildStringSetCaseGroupedPayloadProjection(SljitStringSetCaseGroupedPayloadProjection &descriptor) const {
		if (ops.size() != 5) {
			return false;
		}
		if (!CanBypassInt64ToInt32PreJoinProjection()) {
			return false;
		}
		auto &first_projection = ops[2];
		if (first_projection.projections.size() != 2) {
			return false;
		}
		idx_t predicate_projection_idx = DConstants::INVALID_INDEX;
		idx_t group_projection_idx = DConstants::INVALID_INDEX;
		idx_t predicate_source_idx = DConstants::INVALID_INDEX;
		idx_t compressed_group_source_idx = DConstants::INVALID_INDEX;
		for (idx_t projection_idx = 0; projection_idx < first_projection.projections.size(); projection_idx++) {
			auto &plan = first_projection.projections[projection_idx].plan;
			if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE &&
			    plan.return_type.id() == LogicalTypeId::VARCHAR) {
				if (predicate_projection_idx != DConstants::INVALID_INDEX) {
					return false;
				}
				predicate_projection_idx = projection_idx;
				predicate_source_idx = plan.source_index;
				continue;
			}
			if (plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
			    plan.return_type.id() == LogicalTypeId::VARCHAR) {
				if (group_projection_idx != DConstants::INVALID_INDEX) {
					return false;
				}
				group_projection_idx = projection_idx;
				compressed_group_source_idx = plan.source_index;
				continue;
			}
			return false;
		}
		if (predicate_projection_idx == DConstants::INVALID_INDEX ||
		    group_projection_idx == DConstants::INVALID_INDEX) {
			return false;
		}

		auto &final_projection = ops[3];
		if (final_projection.projections.size() != 3 || final_projection.output_types.size() != 3 ||
		    !DirectAppendSupportsFixedSizeType(final_projection.output_types[0]) ||
		    final_projection.output_types[1].id() != LogicalTypeId::INTEGER ||
		    final_projection.output_types[2].id() != LogicalTypeId::INTEGER) {
			return false;
		}
		auto &group_compress = final_projection.projections[0].plan;
		if (group_compress.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		    group_compress.source_index != group_projection_idx ||
		    group_compress.return_type != final_projection.output_types[0]) {
			return false;
		}
		std::array<string, 2> matching_constants;
		std::array<string, 2> non_matching_constants;
		if (!SljitTryReadStringSetCaseExpression(final_projection.projections[1], predicate_projection_idx, true,
		                                         matching_constants) ||
		    !SljitTryReadStringSetCaseExpression(final_projection.projections[2], predicate_projection_idx, false,
		                                         non_matching_constants) ||
		    !SljitSameStringConstantSet(matching_constants, non_matching_constants)) {
			return false;
		}
		descriptor.predicate_source_idx = predicate_source_idx;
		descriptor.compressed_group_source_idx = compressed_group_source_idx;
		descriptor.constants = matching_constants;
		return true;
	}

	bool CanBypassInt64ToInt32PreJoinProjection(idx_t projection_idx, idx_t hash_join_idx) const {
		if (projection_idx >= ops.size() || hash_join_idx >= ops.size()) {
			return false;
		}
		auto &pre_join_projection = ops[projection_idx];
		if (pre_join_projection.kind != SljitNativeRegionOpKind::PROJECTION ||
		    pre_join_projection.projections.size() != 2 || pre_join_projection.output_types.size() != 2) {
			return false;
		}
		auto &cast_key = pre_join_projection.projections[0].plan;
		if (cast_key.kind != SljitNativeRegionExpressionKind::INTEGER_CAST || cast_key.source_index != 0 ||
		    cast_key.cast_source_width != SljitNativeSignedIntegerWidth::INT64 ||
		    cast_key.cast_target_width != SljitNativeSignedIntegerWidth::INT32 || cast_key.try_cast) {
			return false;
		}
		auto &payload = pre_join_projection.projections[1].plan;
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE || payload.source_index != 1) {
			return false;
		}
		auto &join_op = ops[hash_join_idx];
		if (join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			return false;
		}
		auto &join = join_op.hash_join_probe.plan;
		return join.keys.size() == 1 && join.keys[0].key_input_index == 0 &&
		       join.keys[0].key_kind == SljitNativeHashJoinKeyKind::INT32;
	}

	bool SourceRangeFitsInt32(idx_t source_index) const {
		if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
			return false;
		}
		int64_t min_value;
		int64_t max_value;
		if (!SljitTryReadSignedIntegerValue(source_min_values[source_index], min_value) ||
		    !SljitTryReadSignedIntegerValue(source_max_values[source_index], max_value)) {
			return false;
		}
		return min_value >= NumericLimits<int32_t>::Minimum() && max_value <= NumericLimits<int32_t>::Maximum();
	}

	bool CanUseUncheckedInt64ToInt32PreJoinProjection(idx_t projection_idx, idx_t hash_join_idx) const {
		if (!CanBypassInt64ToInt32PreJoinProjection(projection_idx, hash_join_idx)) {
			return false;
		}
		auto &cast_key = ops[projection_idx].projections[0].plan;
		return SourceRangeFitsInt32(cast_key.source_index);
	}

	bool CanBypassInt64ToInt32PreJoinProjection() const {
		if (SelectFullPipelineRouteKind() !=
		    SljitFullPipelineRouteKind::PROJECTION_HASH_JOIN_PROJECTION_CHAIN_GROUPED_AGGREGATE) {
			return false;
		}
		return CanBypassInt64ToInt32PreJoinProjection(0, 1);
	}

	bool UsesExtendedSourceFetchBudget() const {
		bool has_hash_join_probe = false;
		for (auto &op : ops) {
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				has_hash_join_probe = true;
				continue;
			}
			if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				return has_hash_join_probe &&
				       op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
			}
		}
		if (ops.empty()) {
			return false;
		}
		switch (ops.back().kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			return true;
		default:
			return false;
		}
	}

	bool TryBuildDirectSecondJoinInputProjection(SljitDirectSecondJoinInputProjection &descriptor) const {
		SljitFullPipelineProjectionAggregateShape shape;
		if (!OpsPrefixIs(SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_PROBE,
		                 SljitNativeRegionOpKind::PROJECTION, SljitNativeRegionOpKind::HASH_JOIN_PROBE) ||
		    !TryBuildProjectionAggregateShape(4, shape) || shape.ProjectionCount() != 2) {
			return false;
		}
		if (!CanBypassInt64ToInt32PreJoinProjection(0, 1)) {
			return false;
		}
		auto &projection = ops[2];
		if (projection.projections.size() != 5 || projection.output_types.size() != 5 ||
		    projection.output_types[0].id() != LogicalTypeId::BIGINT ||
		    projection.output_types[1].id() != LogicalTypeId::VARCHAR ||
		    projection.output_types[2].id() != LogicalTypeId::DATE ||
		    projection.output_types[3].InternalType() != PhysicalType::INT64 ||
		    projection.output_types[4].InternalType() != PhysicalType::INT64) {
			return false;
		}
		auto &source_reference = projection.projections[0].plan;
		auto &string_payload = projection.projections[1].plan;
		auto &date_payload = projection.projections[2].plan;
		auto &first_int64_payload = projection.projections[3].plan;
		auto &second_int64_payload = projection.projections[4].plan;
		if (source_reference.kind != SljitNativeRegionExpressionKind::REFERENCE || source_reference.source_index != 0 ||
		    string_payload.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
		    string_payload.string_decompress_source_size != sizeof(uhugeint_t) ||
		    date_payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    first_int64_payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    second_int64_payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return false;
		}
		descriptor.source_input_idx = ops[0].projections[1].plan.source_index;
		descriptor.string_payload_idx = string_payload.source_index;
		descriptor.date_payload_idx = date_payload.source_index;
		descriptor.first_int64_payload_idx = first_int64_payload.source_index;
		descriptor.second_int64_payload_idx = second_int64_payload.source_index;
		return true;
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_scan_filters;
};

} // namespace duckdb

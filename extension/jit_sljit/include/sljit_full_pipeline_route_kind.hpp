//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_route_kind.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct SljitFullPipelineProjectionAggregateShape {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t aggregate_idx = DConstants::INVALID_INDEX;

	idx_t ProjectionCount() const {
		if (first_projection_idx == DConstants::INVALID_INDEX || final_projection_idx == DConstants::INVALID_INDEX ||
		    final_projection_idx < first_projection_idx) {
			return 0;
		}
		return final_projection_idx - first_projection_idx + 1;
	}
};

enum class SljitFullPipelineRouteKind : uint8_t {
	NONE,
	FILTERED_SOURCE_AGGREGATE,
	GENERATED_FILTER_PROJECTION,
	PROJECTION_COUNT_STAR_GROUPED_AGGREGATE,
	HASH_JOIN_PROJECTION_GROUPED_AGGREGATE,
	HASH_JOIN_DELIM_JOIN_SINK,
	GENERATED_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK,
	GENERATED_PROJECTION_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK,
	HASH_JOIN_BUILD_SINK,
	HASH_JOIN_APPEND_SINK,
	HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
	MARK_HASH_JOIN_FILTER_PROJECTION_GROUPED_AGGREGATE,
	PROJECTION_HASH_JOIN_PROJECTION_CHAIN_GROUPED_AGGREGATE,
	HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTIONS_GROUPED_AGGREGATE,
	PROJECTION_HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
	HASH_JOIN_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE,
	HASH_JOIN_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
	GENERATED_FILTER_PROJECTION_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE
};

static bool SljitIsBooleanType(const LogicalType &type) {
	return type.id() == LogicalTypeId::BOOLEAN && type.InternalType() == PhysicalType::BOOL;
}

static bool SljitLocalSourceReferencesColumn(const vector<idx_t> &input_source_indices, idx_t local_source_index,
                                             idx_t column_index) {
	if (input_source_indices.empty()) {
		return local_source_index == column_index;
	}
	return local_source_index < input_source_indices.size() && input_source_indices[local_source_index] == column_index;
}

static bool SljitIsBooleanMarkerReference(const SljitExecutableRegionExpression &expression, idx_t marker_index) {
	auto &plan = expression.plan;
	if (!SljitIsBooleanType(plan.return_type)) {
		return false;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		return SljitLocalSourceReferencesColumn(expression.input_source_indices, plan.source_index, marker_index);
	}
	if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		return plan.predicate && plan.predicate->kind == SljitNativePredicateKind::REFERENCE &&
		       SljitIsBooleanType(plan.predicate->return_type) &&
		       SljitLocalSourceReferencesColumn(expression.input_source_indices, plan.predicate->source_index,
		                                        marker_index);
	}
	if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
	    plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !SljitIsBooleanType(plan.expression_tree->return_type)) {
		return false;
	}
	return SljitLocalSourceReferencesColumn(plan.expression_tree_source_indices, plan.expression_tree->ref_index,
	                                        marker_index);
}

static bool SljitIsMarkProbeMarkerFilter(const SljitExecutableRegionOp &hash_join_op,
                                         const SljitExecutableRegionOp &filter_op) {
	if (hash_join_op.output_types.empty() || hash_join_op.output_types.back().id() != LogicalTypeId::BOOLEAN ||
	    filter_op.kind != SljitNativeRegionOpKind::FILTER) {
		return false;
	}
	return SljitIsBooleanMarkerReference(filter_op.filter, hash_join_op.output_types.size() - 1);
}

static bool SljitTryReadSignedIntegerValue(const Value &value, int64_t &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

} // namespace duckdb

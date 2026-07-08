//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_projection_rewrite.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_plan.hpp"
#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/constants.hpp"

namespace duckdb {

static idx_t AddSljitProjectionSourceReference(const vector<LogicalType> &input_types,
                                               vector<SljitNativeRegionExpressionPlan> &projections,
                                               vector<LogicalType> &projection_types, vector<idx_t> &source_map,
                                               idx_t source_index, bool render_diagnostics) {
	if (source_index >= input_types.size()) {
		return DConstants::INVALID_INDEX;
	}
	if (source_map[source_index] != DConstants::INVALID_INDEX) {
		return source_map[source_index];
	}
	const auto projection_index = projections.size();
	auto ir = render_diagnostics ? "primitive-payload-source-reference" : string();
	projections.push_back(SljitNativeReferenceExpression(source_index, input_types[source_index], std::move(ir), true));
	projection_types.push_back(input_types[source_index]);
	source_map[source_index] = projection_index;
	return projection_index;
}

static bool RewriteSljitDirectPayloadSourceThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, idx_t &source_index, bool render_diagnostics) {
	auto projection_index = AddSljitProjectionSourceReference(input_types, projections, projection_types, source_map,
	                                                          source_index, render_diagnostics);
	if (projection_index == DConstants::INVALID_INDEX) {
		return false;
	}
	source_index = projection_index;
	return true;
}

static bool RewriteSljitDirectExpressionTreeSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, SljitNativeRegionExpressionPlan &expr,
    bool render_diagnostics) {
	for (auto &source_index : expr.expression_tree_source_indices) {
		if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                             source_map, source_index, render_diagnostics)) {
			return false;
		}
	}
	return true;
}

static bool RewriteSljitDirectPredicateSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, SljitNativePredicate &predicate,
    bool render_diagnostics) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return true;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		return RewriteSljitDirectPayloadSourceThroughPartialProjection(
		    input_types, projections, projection_types, source_map, predicate.source_index, render_diagnostics);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		return RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                               source_map, predicate.source_index,
		                                                               render_diagnostics) &&
		       RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                               source_map, predicate.right_source_index,
		                                                               render_diagnostics);
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, source_index, render_diagnostics)) {
				return false;
			}
		}
		return predicate.child &&
		       RewriteSljitDirectPredicateSourcesThroughPartialProjection(
		           input_types, projections, projection_types, source_map, *predicate.child, render_diagnostics);
	case SljitNativePredicateKind::NOT:
		return predicate.child &&
		       RewriteSljitDirectPredicateSourcesThroughPartialProjection(
		           input_types, projections, projection_types, source_map, *predicate.child, render_diagnostics);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (!child || !RewriteSljitDirectPredicateSourcesThroughPartialProjection(
			                  input_types, projections, projection_types, source_map, *child, render_diagnostics)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool RewriteSljitDirectPayloadSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &payloads,
    vector<SljitNativeRegionExpressionPlan> &projections, vector<LogicalType> &projection_types,
    bool render_diagnostics) {
	vector<idx_t> source_map(input_types.size(), DConstants::INVALID_INDEX);
	for (auto &payload : payloads) {
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			break;
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics) ||
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.guard_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			for (auto &source_index : payload.constant_or_null.guard_source_indices) {
				if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
				        input_types, projections, projection_types, source_map, source_index, render_diagnostics)) {
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics) ||
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.right_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics)) {
				return false;
			}
			if (payload.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.right_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::PREDICATE:
			if (!payload.predicate ||
			    !RewriteSljitDirectPredicateSourcesThroughPartialProjection(
			        input_types, projections, projection_types, source_map, *payload.predicate, render_diagnostics)) {
				return false;
			}
			FinalizeSljitNativePredicateSourceIndices(*payload.predicate);
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			break;
		default:
			return false;
		}
		if (!RewriteSljitDirectExpressionTreeSourcesThroughPartialProjection(input_types, projections, projection_types,
		                                                                     source_map, payload, render_diagnostics)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

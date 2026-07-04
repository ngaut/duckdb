//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_mark_probe_filter_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

enum class SljitMarkProbeFilterMode : uint8_t { NONE, MATCHES, NON_MATCHES };

static SljitMarkProbeFilterMode SljitInvertMarkProbeFilterMode(SljitMarkProbeFilterMode mode) {
	switch (mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		return SljitMarkProbeFilterMode::NON_MATCHES;
	case SljitMarkProbeFilterMode::NON_MATCHES:
		return SljitMarkProbeFilterMode::MATCHES;
	default:
		return SljitMarkProbeFilterMode::NONE;
	}
}

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

static SljitMarkProbeFilterMode SljitReadBooleanMarkerPredicateMode(const SljitNativePredicate &predicate,
                                                                    const vector<idx_t> &input_source_indices,
                                                                    idx_t marker_index) {
	if (!SljitIsBooleanType(predicate.return_type)) {
		return SljitMarkProbeFilterMode::NONE;
	}
	switch (predicate.kind) {
	case SljitNativePredicateKind::REFERENCE:
		return SljitLocalSourceReferencesColumn(input_source_indices, predicate.source_index, marker_index)
		           ? SljitMarkProbeFilterMode::MATCHES
		           : SljitMarkProbeFilterMode::NONE;
	case SljitNativePredicateKind::NOT:
		return predicate.child ? SljitInvertMarkProbeFilterMode(SljitReadBooleanMarkerPredicateMode(
		                             *predicate.child, input_source_indices, marker_index))
		                       : SljitMarkProbeFilterMode::NONE;
	default:
		return SljitMarkProbeFilterMode::NONE;
	}
}

static SljitMarkProbeFilterMode SljitReadBooleanMarkerExpressionTreeMode(const ExecutionExpressionIR &node,
                                                                         const vector<idx_t> &input_source_indices,
                                                                         idx_t marker_index) {
	if (!SljitIsBooleanType(node.return_type)) {
		return SljitMarkProbeFilterMode::NONE;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return SljitLocalSourceReferencesColumn(input_source_indices, node.ref_index, marker_index)
		           ? SljitMarkProbeFilterMode::MATCHES
		           : SljitMarkProbeFilterMode::NONE;
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.unary_op == ExecutionExpressionUnaryOp::NOT &&
	    node.left) {
		return SljitInvertMarkProbeFilterMode(
		    SljitReadBooleanMarkerExpressionTreeMode(*node.left, input_source_indices, marker_index));
	}
	return SljitMarkProbeFilterMode::NONE;
}

static SljitMarkProbeFilterMode SljitReadBooleanMarkerFilterMode(const SljitExecutableRegionExpression &expression,
                                                                 idx_t marker_index) {
	auto &plan = expression.plan;
	if (!SljitIsBooleanType(plan.return_type)) {
		return SljitMarkProbeFilterMode::NONE;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return SljitLocalSourceReferencesColumn(expression.input_source_indices, plan.source_index, marker_index)
		           ? SljitMarkProbeFilterMode::MATCHES
		           : SljitMarkProbeFilterMode::NONE;
	case SljitNativeRegionExpressionKind::PREDICATE:
		return plan.predicate
		           ? SljitReadBooleanMarkerPredicateMode(*plan.predicate, expression.input_source_indices, marker_index)
		           : SljitMarkProbeFilterMode::NONE;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return plan.expression_tree ? SljitReadBooleanMarkerExpressionTreeMode(
		                                  *plan.expression_tree, plan.expression_tree_source_indices, marker_index)
		                            : SljitMarkProbeFilterMode::NONE;
	default:
		return SljitMarkProbeFilterMode::NONE;
	}
}

static SljitMarkProbeFilterMode SljitMarkProbeMarkerFilterMode(const SljitExecutableRegionOp &hash_join_op,
                                                               const SljitExecutableRegionOp &filter_op) {
	if (hash_join_op.output_types.empty() || hash_join_op.output_types.back().id() != LogicalTypeId::BOOLEAN ||
	    filter_op.kind != SljitNativeRegionOpKind::FILTER) {
		return SljitMarkProbeFilterMode::NONE;
	}
	return SljitReadBooleanMarkerFilterMode(filter_op.filter, hash_join_op.output_types.size() - 1);
}

static bool SljitIsMarkProbeMarkerFilter(const SljitExecutableRegionOp &hash_join_op,
                                         const SljitExecutableRegionOp &filter_op) {
	return SljitMarkProbeMarkerFilterMode(hash_join_op, filter_op) != SljitMarkProbeFilterMode::NONE;
}

} // namespace duckdb

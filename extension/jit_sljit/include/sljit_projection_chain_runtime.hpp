//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"

#include "duckdb/common/types.hpp"

namespace duckdb {

static bool SljitOutputTypesAreFixedWidth(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return false;
		}
	}
	return true;
}

static bool SljitProjectionOutputsAreFixedWidth(const SljitExecutableRegionOp &projection_op) {
	return projection_op.kind == SljitNativeRegionOpKind::PROJECTION &&
	       SljitOutputTypesAreFixedWidth(projection_op.output_types);
}

static bool SljitBuildReferenceProjectionOutputMap(const SljitExecutableRegionOp &projection_op,
                                                   const SljitExecutableRegionOp &reference_projection_op,
                                                   vector<idx_t> &output_to_projection) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    reference_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    !SljitProjectionOutputsAreFixedWidth(reference_projection_op)) {
		return false;
	}
	output_to_projection.clear();
	output_to_projection.reserve(reference_projection_op.projections.size());
	for (auto &reference_expr : reference_projection_op.projections) {
		auto &reference = reference_expr.plan;
		if (reference.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    reference.source_index >= projection_op.projections.size() ||
		    reference.return_type != projection_op.projections[reference.source_index].plan.return_type) {
			output_to_projection.clear();
			return false;
		}
		output_to_projection.push_back(reference.source_index);
	}
	return true;
}

static bool SljitTryResolveProjectionChainReferenceSource(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t first_projection_idx, idx_t projection_idx,
                                                          idx_t output_idx, idx_t &join_output_source_idx,
                                                          LogicalType &source_type) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    output_idx >= ops[projection_idx].projections.size()) {
		return false;
	}

	SljitExecutableRegionExpression remapped_reference;
	idx_t previous_source_idx;
	auto &projection_expr = ops[projection_idx].projections[output_idx];
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_expr, remapped_reference, previous_source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan)) {
		return false;
	}
	if (projection_idx == first_projection_idx) {
		join_output_source_idx = previous_source_idx;
		source_type = remapped_reference.plan.return_type;
		return true;
	}

	LogicalType resolved_type;
	if (!SljitTryResolveProjectionChainReferenceSource(ops, first_projection_idx, projection_idx - 1,
	                                                   previous_source_idx, join_output_source_idx, resolved_type) ||
	    resolved_type != remapped_reference.plan.return_type) {
		return false;
	}
	source_type = std::move(resolved_type);
	return true;
}

static bool SljitBuildProjectionChainReferenceSourceMap(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t projection_idx,
                                                        vector<idx_t> &source_map) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	auto &projection_op = ops[projection_idx];
	source_map.assign(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t output_idx = 0; output_idx < projection_op.projections.size(); output_idx++) {
		idx_t join_output_source_idx;
		LogicalType source_type;
		if (!SljitTryResolveProjectionChainReferenceSource(ops, first_projection_idx, projection_idx, output_idx,
		                                                   join_output_source_idx, source_type)) {
			continue;
		}
		if (source_type != projection_op.projections[output_idx].plan.return_type) {
			return false;
		}
		source_map[output_idx] = join_output_source_idx;
	}
	return true;
}

static bool SljitTryBuildProjectionChainExpression(const vector<SljitExecutableRegionOp> &ops,
                                                   idx_t first_projection_idx, idx_t projection_idx, idx_t output_idx,
                                                   SljitExecutableRegionExpression &target) {
	if (first_projection_idx >= ops.size() || projection_idx >= ops.size() || first_projection_idx > projection_idx ||
	    ops[projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    output_idx >= ops[projection_idx].projections.size()) {
		return false;
	}
	auto &projection_expr = ops[projection_idx].projections[output_idx];
	if (projection_idx == first_projection_idx) {
		SljitBuildBorrowedProjectionExpression(projection_expr, target);
		return true;
	}

	SljitExecutableRegionExpression remapped_reference;
	idx_t previous_source_idx;
	if (SljitTryBuildSingleSourceProjectionExpression(projection_expr, remapped_reference, previous_source_idx) &&
	    SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan)) {
		return SljitTryBuildProjectionChainExpression(ops, first_projection_idx, projection_idx - 1,
		                                              previous_source_idx, target);
	}

	vector<idx_t> source_map;
	if (!SljitBuildProjectionChainReferenceSourceMap(ops, first_projection_idx, projection_idx - 1, source_map)) {
		return false;
	}
	SljitBuildBorrowedProjectionExpression(projection_expr, target);
	return SljitTryRemapHashJoinProjectionExpressionSources(source_map, target);
}

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx ||
	    ops[first_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    ops[final_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	auto &final_projection = ops[final_projection_idx];
	composed_projection = SljitExecutableRegionOp();
	composed_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	composed_projection.operator_index = final_projection.operator_index;
	composed_projection.output_types = final_projection.output_types;
	composed_projection.projections.reserve(final_projection.projections.size());
	for (idx_t output_idx = 0; output_idx < final_projection.projections.size(); output_idx++) {
		SljitExecutableRegionExpression expression;
		if (!SljitTryBuildProjectionChainExpression(ops, first_projection_idx, final_projection_idx, output_idx,
		                                            expression) ||
		    expression.plan.return_type != final_projection.projections[output_idx].plan.return_type) {
			composed_projection.projections.clear();
			return false;
		}
		composed_projection.projections.push_back(std::move(expression));
	}
	return composed_projection.projections.size() == final_projection.projections.size();
}

static bool SljitTryResolveReferenceThroughProjectionChain(const vector<SljitExecutableRegionOp> &ops,
                                                           idx_t first_projection_idx, idx_t aggregate_idx,
                                                           const SljitExecutableRegionExpression &source_expr,
                                                           idx_t &join_output_source_idx, LogicalType &source_type) {
	SljitExecutableRegionExpression remapped_source;
	idx_t source_idx;
	if (!SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_source, source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_source.plan)) {
		return false;
	}
	source_type = remapped_source.plan.return_type;
	for (idx_t op_idx = aggregate_idx; op_idx > first_projection_idx; op_idx--) {
		auto &projection_op = ops[op_idx - 1];
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    source_idx >= projection_op.projections.size() || source_idx >= projection_op.output_types.size() ||
		    projection_op.output_types[source_idx] != source_type) {
			return false;
		}
		SljitExecutableRegionExpression remapped_projection;
		idx_t next_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[source_idx], remapped_projection,
		                                                   next_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_projection.plan) ||
		    remapped_projection.plan.return_type != source_type) {
			return false;
		}
		source_idx = next_source_idx;
	}
	join_output_source_idx = source_idx;
	return true;
}

static bool SljitTryBuildRemappedPayloadReference(const SljitExecutableRegionExpression &payload,
                                                  idx_t payload_input_idx,
                                                  SljitExecutableRegionExpression &remapped_payload,
                                                  idx_t &join_output_source_idx) {
	if (!SljitTryBuildSingleSourceProjectionExpression(payload, remapped_payload, join_output_source_idx) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_payload.plan)) {
		return false;
	}
	remapped_payload.input_source_indices.clear();
	if (remapped_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		remapped_payload.plan.source_index = payload_input_idx;
		remapped_payload.plan.expression_tree_source_indices.clear();
		return true;
	}
	remapped_payload.plan.expression_tree_source_indices.clear();
	remapped_payload.plan.expression_tree_source_indices.push_back(payload_input_idx);
	return true;
}

} // namespace duckdb

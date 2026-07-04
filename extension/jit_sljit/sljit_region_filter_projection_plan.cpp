//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_filter_projection_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static SljitRegionNodePlan SljitUnsupportedExpressionBoundaryNode(const string &error) {
	string reason = "region IR node is unsupported by SLJIT native contract lowering";
	if (!error.empty()) {
		reason += ";";
		reason += error;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

SljitRegionNodePlan PlanSljitFilterNode(const ExecutionRegionNode &node, string &error, bool render_diagnostics) {
	if (!node.blocker_reason.empty() || !node.filter) {
		return SljitNodeBlockerBoundary(node, "filter expression unsupported by SLJIT IR lowering");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::FILTER;
	native_op.output_types = node.output_types;
	if (!TryLowerNativeRegionExpression(*node.filter, true, native_op.filter, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}

	return SljitNativeNode(std::move(native_op), "generated typed predicate filter");
}

static bool TryPlanDirectSljitProjection(const ExecutionRegionNode &node, SljitNativeRegionOpPlan &native_op,
                                         const vector<LogicalType> &input_types,
                                         string &error, bool render_diagnostics) {
	native_op = SljitNativeRegionOpPlan();
	native_op.kind = SljitNativeRegionOpKind::PROJECTION;
	native_op.input_types = input_types;
	native_op.output_types = node.output_types;
	for (auto &expression : node.projections) {
		SljitNativeRegionExpressionPlan native_expression;
		if (!TryLowerNativeRegionExpression(*expression, false, native_expression, error, render_diagnostics)) {
			return false;
		}
		native_op.projections.push_back(std::move(native_expression));
	}
	return true;
}

static bool TryPlanExpandedSljitProjection(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                           vector<SljitNativeRegionOpPlan> &native_ops, string &error,
                                           bool render_diagnostics) {
	if (input_types.empty() && !node.output_types.empty()) {
		error = "projection expression graph lowering requires input types";
		return false;
	}

	SljitProjectionGraphLowering graph(input_types, render_diagnostics);
	vector<SljitNativeRegionExpressionPlan> final_projections;
	final_projections.reserve(node.projections.size());
	for (auto &fragment : node.projections) {
		if (!fragment->root) {
			error = "projection expression graph lowering requires rooted JIT IR";
			return false;
		}
		SljitNativeRegionExpressionPlan projection;
		if (!TryBuildSljitProjectionGraphExpression(*fragment->root, graph, projection)) {
			return false;
		}
		if (render_diagnostics) {
			projection.ir = fragment->ir;
		}
		final_projections.push_back(std::move(projection));
	}

	SljitNativeRegionOpPlan final_op;
	final_op.kind = SljitNativeRegionOpKind::PROJECTION;
	final_op.input_types = graph.current_types;
	final_op.output_types = node.output_types;
	final_op.projections = std::move(final_projections);
	graph.native_ops.push_back(std::move(final_op));
	native_ops = std::move(graph.native_ops);
	return true;
}

SljitRegionNodePlan PlanSljitProjectionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                            string &error, bool render_diagnostics) {
	if (!node.blocker_reason.empty() || node.projections.empty()) {
		return SljitNodeBlockerBoundary(node, "projection has no lowered JIT IR expressions");
	}

	SljitNativeRegionOpPlan native_op;
	if (TryPlanDirectSljitProjection(node, native_op, input_types, error, render_diagnostics)) {
		return SljitNativeNode(std::move(native_op), "native projection");
	}

	vector<SljitNativeRegionOpPlan> native_ops;
	if (!TryPlanExpandedSljitProjection(node, input_types, native_ops, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}
	return SljitNativeNode(std::move(native_ops), "generated typed projection expression graph");
}

} // namespace duckdb

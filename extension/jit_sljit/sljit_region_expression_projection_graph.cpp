//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_projection_graph.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

bool TryLowerNativeRegionExpressionTreeThroughProjection(
    const ExecutionExpressionFragment &fragment, const vector<SljitNativeRegionExpressionPlan> &input_projection,
    unique_ptr<ExecutionExpressionIR> &tree, string &error, bool render_diagnostics) {
	SljitNativeRegionExpressionPlan expr;
	if (!TryLowerNativeRegionExpression(fragment, true, expr, error, render_diagnostics)) {
		return false;
	}
	if (!TryMapNativeProjectionExpressionSources(input_projection, expr)) {
		error = "sljit-expression-tree-projection-map-failed";
		return false;
	}
	tree = CopySljitExpressionPlanAsInputTree(expr);
	if (!tree) {
		error = "sljit-expression-tree-projection-copy-failed";
		return false;
	}
	return true;
}

static unique_ptr<ExecutionExpressionIR> MakeSljitReferenceExpression(idx_t source_index, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = source_index;
	return result;
}

static string SljitTempExpressionIr(const ExecutionExpressionIR &node, idx_t temp_index) {
	return "ssa.temp#" + std::to_string(temp_index) + ":" + ExecutionExpressionIRKindToString(node.kind) + "<" +
	       node.return_type.ToString() + ">";
}

static void AppendSljitNativeTempProjection(SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan expression) {
	auto temp_type = expression.return_type;

	SljitNativeRegionOpPlan temp_op;
	temp_op.kind = SljitNativeRegionOpKind::PROJECTION;
	temp_op.output_types.reserve(graph.current_types.size() + 1);
	temp_op.projections.reserve(graph.current_types.size() + 1);
	for (idx_t col_idx = 0; col_idx < graph.current_types.size(); col_idx++) {
		auto ir = graph.render_diagnostics ? "ssa.pass#" + std::to_string(col_idx) : string();
		temp_op.output_types.push_back(graph.current_types[col_idx]);
		temp_op.projections.push_back(
		    SljitNativeReferenceExpression(col_idx, graph.current_types[col_idx], std::move(ir), false));
	}
	temp_op.output_types.push_back(temp_type);
	temp_op.projections.push_back(std::move(expression));
	graph.current_types.push_back(std::move(temp_type));
	graph.native_ops.push_back(std::move(temp_op));
}

static bool TryBuildSljitProjectionGraphOperand(const ExecutionExpressionIR &node, SljitProjectionGraphLowering &graph,
                                                unique_ptr<ExecutionExpressionIR> &operand) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE || node.kind == ExecutionExpressionIRKind::CONSTANT) {
		operand = node.Copy();
		return true;
	}

	SljitNativeRegionExpressionPlan temp_expression;
	if (!TryBuildSljitProjectionGraphExpression(node, graph, temp_expression)) {
		return false;
	}
	auto temp_index = graph.current_types.size();
	auto temp_type = temp_expression.return_type;
	if (graph.render_diagnostics && temp_expression.ir.empty()) {
		temp_expression.ir = SljitTempExpressionIr(node, temp_index);
	}
	AppendSljitNativeTempProjection(graph, std::move(temp_expression));
	operand = MakeSljitReferenceExpression(temp_index, temp_type);
	return true;
}

static bool RewriteSljitProjectionGraphOperands(ExecutionExpressionIR &rewritten, SljitProjectionGraphLowering &graph) {
	if (rewritten.left && !TryBuildSljitProjectionGraphOperand(*rewritten.left, graph, rewritten.left)) {
		return false;
	}
	if (rewritten.right && !TryBuildSljitProjectionGraphOperand(*rewritten.right, graph, rewritten.right)) {
		return false;
	}
	if (rewritten.else_node && !TryBuildSljitProjectionGraphOperand(*rewritten.else_node, graph, rewritten.else_node)) {
		return false;
	}
	for (auto &child : rewritten.children) {
		if (!TryBuildSljitProjectionGraphOperand(*child, graph, child)) {
			return false;
		}
	}
	return true;
}

bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root, SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan &expression) {
	if (TryReadNativeRegionExpression(root, false, expression)) {
		if (expression.kind == SljitNativeRegionExpressionKind::REFERENCE &&
		    expression.source_index >= graph.input_type_count) {
			expression.references_region_input = false;
		}
		return true;
	}

	auto rewritten = root.Copy();
	if (!RewriteSljitProjectionGraphOperands(*rewritten, graph)) {
		return false;
	}
	if (!TryReadNativeRegionExpression(*rewritten, false, expression)) {
		return false;
	}
	if (expression.kind == SljitNativeRegionExpressionKind::REFERENCE &&
	    expression.source_index >= graph.input_type_count) {
		expression.references_region_input = false;
	}
	return true;
}

} // namespace duckdb

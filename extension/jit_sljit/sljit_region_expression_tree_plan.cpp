//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_tree_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

static void RemapSljitNativeTreeReferences(ExecutionExpressionIR &node, vector<idx_t> &source_indices) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		auto source_idx = node.ref_index;
		for (idx_t map_idx = 0; map_idx < source_indices.size(); map_idx++) {
			if (source_indices[map_idx] == source_idx) {
				node.ref_index = map_idx;
				return;
			}
		}
		node.ref_index = source_indices.size();
		source_indices.push_back(source_idx);
		return;
	}
	if (node.left) {
		RemapSljitNativeTreeReferences(*node.left, source_indices);
	}
	if (node.right) {
		RemapSljitNativeTreeReferences(*node.right, source_indices);
	}
	if (node.else_node) {
		RemapSljitNativeTreeReferences(*node.else_node, source_indices);
	}
	for (auto &child : node.children) {
		RemapSljitNativeTreeReferences(*child, source_indices);
	}
}

void AttachSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr) {
	expr.expression_tree = root.Copy();
	expr.expression_tree_source_indices.clear();
	RemapSljitNativeTreeReferences(*expr.expression_tree, expr.expression_tree_source_indices);
}

bool TryBuildSljitNativeTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                SljitNativeRegionExpressionPlan &expr) {
	SljitNativeIntegerKind result_kind;
	if (!SljitTypedExpressionTreeIsSupported(root) || !TryGetSljitTypedExpressionTreeResultKind(root, result_kind)) {
		return false;
	}
	expr.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	expr.integer_kind = result_kind;
	expr.return_type = root.return_type;
	AttachSljitNativeExpressionTree(root, expr);
	return true;
}

} // namespace duckdb

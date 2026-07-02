//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_tree_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

static bool IsSljitNativeTreeDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitNativeTreeDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

static bool SljitNativeTreeNodeSupported(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return IsSljitNativeTreeDecimal64Node(node);
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return !node.constant.IsNull() && IsSljitNativeTreeDecimal64Node(node);
	}
	int64_t result_min;
	int64_t result_max;
	if (node.kind != ExecutionExpressionIRKind::BINARY || !node.left || !node.right ||
	    !SljitExpressionTreeBinaryOpSupported(node.binary_op) || !IsSljitNativeTreeDecimal64Node(node) ||
	    !IsSljitNativeTreeDecimal64Node(*node.left) || !IsSljitNativeTreeDecimal64Node(*node.right) ||
	    !SljitNativeTreeDecimal64BinaryHasRawSemantics(node) ||
	    !TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		return false;
	}
	return SljitNativeTreeNodeSupported(*node.left) && SljitNativeTreeNodeSupported(*node.right);
}

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

static bool TryBuildSljitNativeExpressionTreePlan(const ExecutionExpressionIR &root,
                                                  SljitNativeRegionExpressionPlan &expr) {
	if (!SljitNativeTreeNodeSupported(root)) {
		return false;
	}
	expr.kind = SljitNativeRegionExpressionKind::EXPRESSION_TREE;
	expr.return_type = root.return_type;
	AttachSljitNativeExpressionTree(root, expr);
	return true;
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

bool TryBuildSljitNativeAnyExpressionTreePlan(const ExecutionExpressionIR &root,
                                              SljitNativeRegionExpressionPlan &expr) {
	if (TryBuildSljitNativeExpressionTreePlan(root, expr)) {
		return true;
	}
	return TryBuildSljitNativeTypedExpressionTreePlan(root, expr);
}

} // namespace duckdb

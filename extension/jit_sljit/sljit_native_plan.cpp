#include "sljit_native_plan.hpp"

namespace duckdb {

bool TryReadNativeConstantOrNull(const ExecutionExpressionIR &root, SljitNativeConstantOrNull &expr) {
	if (root.kind != ExecutionExpressionIRKind::CONSTANT_OR_NULL || root.children.size() < 2 ||
	    root.children[0]->kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}

	auto &constant_node = *root.children[0];
	expr.constant = constant_node.constant.IsNull() ? Value(root.return_type)
	                                                : constant_node.constant.DefaultCastAs(root.return_type);
	expr.guard_source_indices.clear();
	expr.guard_has_null_constant = false;

	for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
		auto &child = *root.children[child_idx];
		if (child.kind == ExecutionExpressionIRKind::REFERENCE) {
			expr.guard_source_indices.push_back(child.ref_index);
			continue;
		}
		if (child.kind == ExecutionExpressionIRKind::CONSTANT) {
			expr.guard_has_null_constant = expr.guard_has_null_constant || child.constant.IsNull();
			continue;
		}
		return false;
	}
	return true;
}

} // namespace duckdb

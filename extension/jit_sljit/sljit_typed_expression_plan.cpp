//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

bool SljitExpressionTreeBinaryOpSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return true;
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeComparisonSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeIsInt64Node(const ExecutionExpressionIR &node) {
	return node.return_type.IsIntegral() && node.physical_type == PhysicalType::INT64;
}

bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node) {
	return node.return_type.IsIntegral() && node.physical_type == PhysicalType::INT32;
}

bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::BOOLEAN && node.physical_type == PhysicalType::BOOL;
}

bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node) ||
	       SljitTypedExpressionTreeIsBoolNode(node);
}

bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node);
}

bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right));
}

bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right)) ||
	       (SljitTypedExpressionTreeIsBoolNode(left) && SljitTypedExpressionTreeIsBoolNode(right));
}

bool SljitTypedExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
	case ExecutionExpressionIRKind::CONSTANT:
		return SljitTypedExpressionTreeIsValueNode(node);
	case ExecutionExpressionIRKind::CAST:
		return !node.try_cast && node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR &&
		       node.exception_behavior != ExecutionExpressionExceptionKind::ERROR &&
		       SljitTypedExpressionTreeIsInt64Node(node) && node.left &&
		       SljitTypedExpressionTreeIsIntegerNode(*node.left) && SljitTypedExpressionTreeIsSupported(*node.left);
	case ExecutionExpressionIRKind::UNARY:
		if (!node.left) {
			return false;
		}
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::NOT:
			return SljitTypedExpressionTreeIsBoolNode(node) && SljitTypedExpressionTreeIsBoolNode(*node.left) &&
			       SljitTypedExpressionTreeIsSupported(*node.left);
		case ExecutionExpressionUnaryOp::IS_NULL:
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			return SljitTypedExpressionTreeIsBoolNode(node) && SljitTypedExpressionTreeIsValueNode(*node.left) &&
			       SljitTypedExpressionTreeIsSupported(*node.left);
		default:
			return false;
		}
	case ExecutionExpressionIRKind::BINARY:
		if (!node.left || !node.right) {
			return false;
		}
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return SljitTypedExpressionTreeIsBoolNode(node) &&
			       SljitTypedExpressionTreeSameIntegerKind(*node.left, *node.right) &&
			       SljitTypedExpressionTreeIsSupported(*node.left) && SljitTypedExpressionTreeIsSupported(*node.right);
		}
		return SljitTypedExpressionTreeIsIntegerNode(node) &&
		       SljitTypedExpressionTreeSameIntegerKind(node, *node.left) &&
		       SljitTypedExpressionTreeSameIntegerKind(node, *node.right) &&
		       SljitExpressionTreeBinaryOpSupported(node.binary_op) &&
		       SljitTypedExpressionTreeIsSupported(*node.left) && SljitTypedExpressionTreeIsSupported(*node.right);
	case ExecutionExpressionIRKind::CONJUNCTION:
		if (!SljitTypedExpressionTreeIsBoolNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !SljitTypedExpressionTreeIsBoolNode(*child) || !SljitTypedExpressionTreeIsSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::COALESCE:
		if (!SljitTypedExpressionTreeIsValueNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !SljitTypedExpressionTreeSameValueKind(node, *child) ||
			    !SljitTypedExpressionTreeIsSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::CASE:
		if (!SljitTypedExpressionTreeIsValueNode(node) || !node.else_node || node.children.empty() ||
		    node.children.size() % 2 != 0 || !SljitTypedExpressionTreeSameValueKind(node, *node.else_node) ||
		    !SljitTypedExpressionTreeIsSupported(*node.else_node)) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
			auto &condition = node.children[child_idx];
			auto &value = node.children[child_idx + 1];
			if (!condition || !value || !SljitTypedExpressionTreeIsBoolNode(*condition) ||
			    !SljitTypedExpressionTreeSameValueKind(node, *value) ||
			    !SljitTypedExpressionTreeIsSupported(*condition) || !SljitTypedExpressionTreeIsSupported(*value)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeInt64CastSupported(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CAST && SljitTypedExpressionTreeIsSupported(node);
}

bool SljitTypedExpressionTreeFastPathSupported(const ExecutionExpressionIR &node) {
	if (SljitTypedExpressionTreeInt64CastSupported(node)) {
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT && node.constant.IsNull()) {
		return false;
	}
	if (node.left && !SljitTypedExpressionTreeFastPathSupported(*node.left)) {
		return false;
	}
	if (node.right && !SljitTypedExpressionTreeFastPathSupported(*node.right)) {
		return false;
	}
	if (node.else_node && !SljitTypedExpressionTreeFastPathSupported(*node.else_node)) {
		return false;
	}
	for (auto &child : node.children) {
		if (!child || !SljitTypedExpressionTreeFastPathSupported(*child)) {
			return false;
		}
	}
	return SljitTypedExpressionTreeIsSupported(node);
}

bool SljitTypedExpressionTreeCanPrecheckNulls(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitTypedExpressionTreeIsInt64Node(node);
	case ExecutionExpressionIRKind::CONSTANT:
		return !node.constant.IsNull() && SljitTypedExpressionTreeIsInt64Node(node);
	case ExecutionExpressionIRKind::CAST:
		return SljitTypedExpressionTreeInt64CastSupported(node);
	case ExecutionExpressionIRKind::BINARY:
		return SljitTypedExpressionTreeIsInt64Node(node) && node.left && node.right &&
		       SljitExpressionTreeBinaryOpSupported(node.binary_op) &&
		       SljitTypedExpressionTreeCanPrecheckNulls(*node.left) &&
		       SljitTypedExpressionTreeCanPrecheckNulls(*node.right);
	default:
		return false;
	}
}

SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node) {
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return SljitNativeIntegerKind::UINT8;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return SljitNativeIntegerKind::INT32;
	}
	if (SljitTypedExpressionTreeIsInt64Node(node) || node.physical_type == PhysicalType::INT64) {
		return SljitNativeIntegerKind::INT64;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree node type");
}

SljitNativeIntegerCompareOp SljitTypedExpressionTreeCompareOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		return SljitNativeIntegerCompareOp::EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		return SljitNativeIntegerCompareOp::NOT_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree comparison operator");
	}
}

idx_t CountSljitTypedExpressionTreeNodes(const ExecutionExpressionIR &node) {
	idx_t result = 1;
	if (node.left) {
		result += CountSljitTypedExpressionTreeNodes(*node.left);
	}
	if (node.right) {
		result += CountSljitTypedExpressionTreeNodes(*node.right);
	}
	if (node.else_node) {
		result += CountSljitTypedExpressionTreeNodes(*node.else_node);
	}
	for (auto &child : node.children) {
		result += CountSljitTypedExpressionTreeNodes(*child);
	}
	return result;
}

bool SljitTypedExpressionTreeSourceKnownValid(const vector<idx_t> *known_valid_sources, idx_t source_index) {
	if (!known_valid_sources) {
		return false;
	}
	for (auto known_source : *known_valid_sources) {
		if (known_source == source_index) {
			return true;
		}
	}
	return false;
}

void AddSljitTypedKnownValidSource(vector<idx_t> &known_valid_sources, idx_t source_index) {
	for (auto known_source : known_valid_sources) {
		if (known_source == source_index) {
			return;
		}
	}
	known_valid_sources.push_back(source_index);
}

void CollectSljitTypedExpressionTreeReferences(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.ref_index);
		return;
	}
	if (node.left) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
	}
	if (node.right) {
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
	}
	if (node.else_node) {
		CollectSljitTypedExpressionTreeReferences(*node.else_node, known_valid_sources);
	}
	for (auto &child : node.children) {
		CollectSljitTypedExpressionTreeReferences(*child, known_valid_sources);
	}
}

SljitTypedExpressionTreeFastPathPlan BuildSljitTypedExpressionTreeFastPathPlan(const ExecutionExpressionIR &root,
                                                                               bool emit_flat_nullable_fast_path) {
	SljitTypedExpressionTreeFastPathPlan result;
	result.fast_path_supported = SljitTypedExpressionTreeFastPathSupported(root);
	const auto precheck_nulls_candidate =
	    emit_flat_nullable_fast_path && result.fast_path_supported && SljitTypedExpressionTreeCanPrecheckNulls(root);
	if (precheck_nulls_candidate) {
		CollectSljitTypedExpressionTreeReferences(root, result.source_refs);
	}
	result.precheck_nulls_supported = precheck_nulls_candidate && !result.source_refs.empty();
	return result;
}

void CollectSljitTypedExpressionTreeTrueFacts(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NOT_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && node.left && node.right &&
	    SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeTrueFacts(*child, known_valid_sources);
		}
	}
}

void CollectSljitTypedExpressionTreeNotTrueFacts(const ExecutionExpressionIR &node,
                                                 vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeNotTrueFacts(*child, known_valid_sources);
		}
	}
}

bool TryGetSljitTypedExpressionTreeResultKind(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind) {
	if (SljitTypedExpressionTreeIsInt64Node(root)) {
		kind = SljitNativeIntegerKind::INT64;
		return true;
	}
	if (SljitTypedExpressionTreeIsBoolNode(root)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	return false;
}

SljitTypedExpressionTreePlan BuildSljitTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                               bool emit_flat_nullable_fast_path) {
	SljitTypedExpressionTreePlan result;
	result.supported =
	    SljitTypedExpressionTreeIsSupported(root) && TryGetSljitTypedExpressionTreeResultKind(root, result.result_kind);
	if (!result.supported) {
		return result;
	}
	result.result_is_bool = SljitTypedExpressionTreeIsBoolNode(root);
	result.result_is_int64 = SljitTypedExpressionTreeIsInt64Node(root);
	result.node_count = CountSljitTypedExpressionTreeNodes(root);
	result.fast_path = BuildSljitTypedExpressionTreeFastPathPlan(root, emit_flat_nullable_fast_path);
	return result;
}

} // namespace duckdb

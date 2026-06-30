//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/string_type.hpp"

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

bool SljitTypedExpressionTreeIsDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

bool TryGetSljitTypedExpressionTreeDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max) {
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	result_max = NumericHelper::POWERS_OF_TEN[width] - 1;
	result_min = -result_max;
	return true;
}

bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node) {
	return (node.return_type.IsIntegral() || node.return_type.id() == LogicalTypeId::DATE) &&
	       node.physical_type == PhysicalType::INT32;
}

bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::BOOLEAN && node.physical_type == PhysicalType::BOOL;
}

bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node) ||
	       SljitTypedExpressionTreeIsBoolNode(node) || SljitTypedExpressionTreeIsDecimal64Node(node);
}

bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node);
}

static bool SljitTypedExpressionTreeIsArithmeticNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsIntegerNode(node) || SljitTypedExpressionTreeIsDecimal64Node(node);
}

bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right));
}

static bool SljitTypedExpressionTreeSameArithmeticKind(const ExecutionExpressionIR &left,
                                                       const ExecutionExpressionIR &right) {
	return SljitTypedExpressionTreeSameIntegerKind(left, right) ||
	       (SljitTypedExpressionTreeIsDecimal64Node(left) && SljitTypedExpressionTreeIsDecimal64Node(right));
}

static bool SljitTypedExpressionTreeDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	const auto left_decimal = SljitTypedExpressionTreeIsDecimal64Node(*node.left);
	const auto right_decimal = SljitTypedExpressionTreeIsDecimal64Node(*node.right);
	if (!left_decimal && !right_decimal) {
		return true;
	}
	if (left_decimal != right_decimal) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT: {
		int64_t result_min;
		int64_t result_max;
		return SljitTypedExpressionTreeIsDecimal64Node(node) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type) &&
		       TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, result_min, result_max);
	}
	case ExecutionExpressionBinaryOp::MULTIPLY: {
		int64_t result_min;
		int64_t result_max;
		return SljitTypedExpressionTreeIsDecimal64Node(node) &&
		       DecimalType::GetScale(node.return_type) ==
		           DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type) &&
		       TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, result_min, result_max);
	}
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitTypedExpressionTreeIsBoolNode(node) &&
		       DecimalType::GetScale(node.left->return_type) == DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right)) ||
	       (SljitTypedExpressionTreeIsBoolNode(left) && SljitTypedExpressionTreeIsBoolNode(right)) ||
	       (SljitTypedExpressionTreeIsDecimal64Node(left) && SljitTypedExpressionTreeIsDecimal64Node(right));
}

bool TryReadSljitTypedExpressionTreeStringPrefixConstant(const ExecutionExpressionIR &node, idx_t &source_index,
                                                         string &prefix) {
	if (node.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    node.intrinsic != ExecutionExpressionIntrinsicKind::STRING_PREFIX ||
	    node.return_type.id() != LogicalTypeId::BOOLEAN || node.children.size() != 2 || !node.children[0] ||
	    !node.children[1] || node.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    node.children[0]->return_type.id() != LogicalTypeId::VARCHAR ||
	    node.children[1]->kind != ExecutionExpressionIRKind::CONSTANT ||
	    node.children[1]->return_type.id() != LogicalTypeId::VARCHAR || node.children[1]->constant.IsNull()) {
		return false;
	}
	source_index = node.children[0]->ref_index;
	prefix = StringValue::Get(node.children[1]->constant);
	return true;
}

bool TryReadSljitTypedExpressionTreeStringCompareConstant(const ExecutionExpressionIR &node, idx_t &source_index,
                                                          string &constant, bool &compare_equal) {
	if (node.kind != ExecutionExpressionIRKind::BINARY || node.return_type.id() != LogicalTypeId::BOOLEAN ||
	    (node.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL &&
	     node.binary_op != ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL) ||
	    !node.left || !node.right) {
		return false;
	}
	auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
		if (reference.kind != ExecutionExpressionIRKind::REFERENCE || reference.return_type.id() != LogicalTypeId::VARCHAR ||
		    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
		    constant_node.return_type.id() != LogicalTypeId::VARCHAR || constant_node.constant.IsNull()) {
			return false;
		}
		source_index = reference.ref_index;
		constant = StringValue::Get(constant_node.constant);
		compare_equal = node.binary_op == ExecutionExpressionBinaryOp::COMPARE_EQUAL;
		return true;
	};
	return try_read(*node.left, *node.right) || try_read(*node.right, *node.left);
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
		{
			idx_t source_index;
			string string_constant;
			bool compare_equal;
			if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, string_constant,
			                                                         compare_equal)) {
				return true;
			}
		}
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return SljitTypedExpressionTreeIsBoolNode(node) &&
			       SljitTypedExpressionTreeSameArithmeticKind(*node.left, *node.right) &&
			       SljitTypedExpressionTreeDecimal64BinaryHasRawSemantics(node) &&
			       SljitTypedExpressionTreeIsSupported(*node.left) && SljitTypedExpressionTreeIsSupported(*node.right);
		}
		return SljitTypedExpressionTreeIsArithmeticNode(node) &&
		       SljitTypedExpressionTreeSameArithmeticKind(node, *node.left) &&
		       SljitTypedExpressionTreeSameArithmeticKind(node, *node.right) &&
		       SljitExpressionTreeBinaryOpSupported(node.binary_op) &&
		       SljitTypedExpressionTreeDecimal64BinaryHasRawSemantics(node) &&
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
	case ExecutionExpressionIRKind::INTRINSIC: {
		idx_t source_index;
		string prefix;
		return TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix);
	}
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
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC) {
		idx_t source_index;
		string prefix;
		return TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix);
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

static bool TryGetSljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node,
                                                      SljitNativeIntegerKind &kind) {
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		kind = SljitNativeIntegerKind::INT32;
		return true;
	}
	if (SljitTypedExpressionTreeIsDecimal64Node(node)) {
		kind = SljitNativeIntegerKind::DECIMAL64;
		return true;
	}
	if (SljitTypedExpressionTreeIsInt64Node(node)) {
		kind = SljitNativeIntegerKind::INT64;
		return true;
	}
	return false;
}

SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node) {
	SljitNativeIntegerKind kind;
	if (TryGetSljitTypedExpressionTreeIntegerKind(node, kind)) {
		return kind;
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
	return TryGetSljitTypedExpressionTreeIntegerKind(root, kind);
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
	result.result_is_int64 = SljitTypedExpressionTreeIsInt64Node(root) || SljitTypedExpressionTreeIsDecimal64Node(root);
	result.node_count = CountSljitTypedExpressionTreeNodes(root);
	result.fast_path = BuildSljitTypedExpressionTreeFastPathPlan(root, emit_flat_nullable_fast_path);
	return result;
}

} // namespace duckdb

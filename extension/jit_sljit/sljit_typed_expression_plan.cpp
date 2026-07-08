//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

static bool SljitTypedExpressionTreeIsArithmeticNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsIntegerNode(node) || SljitTypedExpressionTreeIsDecimal64Node(node);
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
		if (reference.kind != ExecutionExpressionIRKind::REFERENCE ||
		    reference.return_type.id() != LogicalTypeId::VARCHAR ||
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

static bool SljitTypedExpressionTreeDateYearSupported(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::INTRINSIC &&
	       node.intrinsic == ExecutionExpressionIntrinsicKind::DATE_YEAR &&
	       node.return_type.id() == LogicalTypeId::BIGINT && node.physical_type == PhysicalType::INT64 &&
	       node.children.size() == 1 && node.children[0] &&
	       node.children[0]->return_type.id() == LogicalTypeId::DATE &&
	       node.children[0]->physical_type == PhysicalType::INT32 &&
	       SljitTypedExpressionTreeIsSupported(*node.children[0]);
}

static bool SljitTypedExpressionTreeIntegralCompressSupported(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    node.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS ||
	    node.return_type.id() != LogicalTypeId::UTINYINT || node.physical_type != PhysicalType::UINT8 ||
	    node.children.size() != 2 || !node.children[0] || !node.children[1]) {
		return false;
	}
	auto &source = *node.children[0];
	auto &minimum = *node.children[1];
	return source.return_type.IsIntegral() && SljitTypedExpressionTreeIsIntegerNode(source) &&
	       SljitTypedExpressionTreeIsSupported(source) &&
	       minimum.kind == ExecutionExpressionIRKind::CONSTANT && !minimum.constant.IsNull() &&
	       SljitTypedExpressionTreeSameIntegerKind(source, minimum);
}

static bool SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(const ExecutionExpressionIR &node) {
	return !node.try_cast && node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR &&
	       node.exception_behavior != ExecutionExpressionExceptionKind::ERROR;
}

static bool SljitTypedExpressionTreeInt64CastLogic(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CAST && SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(node) &&
	       SljitTypedExpressionTreeIsInt64Node(node) && node.left &&
	       SljitTypedExpressionTreeIsIntegerNode(*node.left) && SljitTypedExpressionTreeIsSupported(*node.left);
}

static bool SljitTypedExpressionTreeDecimal64WideningCastLogic(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::CAST || !SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(node) ||
	    !node.left) {
		return false;
	}
	if (!SljitTypedExpressionTreeIsDecimal64Node(node) || !SljitTypedExpressionTreeIsDecimal64Node(*node.left)) {
		return false;
	}
	// A same-scale, non-narrowing DECIMAL64 -> DECIMAL64 cast keeps the physical INT64 payload identical, so the cast
	// is a value passthrough that cannot round, rescale, or overflow.
	return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
	       DecimalType::GetWidth(node.return_type) >= DecimalType::GetWidth(node.left->return_type) &&
	       SljitTypedExpressionTreeIsSupported(*node.left);
}

static bool SljitTypedExpressionTreeInt32ToDecimal64ScaleZeroCastLogic(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::CAST || !SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(node) ||
	    !node.left) {
		return false;
	}
	// INT32 -> DECIMAL64 with scale 0 stores the sign-extended integer as the payload (no fractional digits), so it
	// reuses the integer widening cast codegen. A width >= 10 covers the whole INT32 range, so it cannot overflow. This
	// is what the binder emits to align `int_col * decimal_col` (e.g. quantity * price) before a decimal multiply.
	return SljitTypedExpressionTreeIsDecimal64Node(node) && DecimalType::GetScale(node.return_type) == 0 &&
	       DecimalType::GetWidth(node.return_type) >= 10 && SljitTypedExpressionTreeIsInt32Node(*node.left) &&
	       SljitTypedExpressionTreeIsSupported(*node.left);
}

bool SljitTypedExpressionTreeValueCastSupported(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeInt64CastLogic(node) || SljitTypedExpressionTreeDecimal64WideningCastLogic(node) ||
	       SljitTypedExpressionTreeInt32ToDecimal64ScaleZeroCastLogic(node);
}

bool SljitTypedExpressionTreeConstantDivisorReductionSupported(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY ||
	    (node.binary_op != ExecutionExpressionBinaryOp::MODULO &&
	     node.binary_op != ExecutionExpressionBinaryOp::INTEGER_DIVIDE) ||
	    !node.left || !node.right) {
		return false;
	}
	if (!SljitTypedExpressionTreeIsIntegerNode(node) || !SljitTypedExpressionTreeSameIntegerKind(node, *node.left) ||
	    !SljitTypedExpressionTreeSameIntegerKind(node, *node.right)) {
		return false;
	}
	auto &divisor = *node.right;
	if (divisor.kind != ExecutionExpressionIRKind::CONSTANT || divisor.constant.IsNull()) {
		return false;
	}
	// Require a constant divisor >= 2. That is the range where the signed magic-multiply strength reduction (shared by
	// modulo and truncating integer division) is well-defined, and it rules out both undefined division inputs (zero
	// divisor, and -1 with INT_MIN dividend).
	const int64_t divisor_value = SljitTypedExpressionTreeIsInt32Node(divisor)
	                                  ? static_cast<int64_t>(divisor.constant.GetValueUnsafe<int32_t>())
	                                  : divisor.constant.GetValueUnsafe<int64_t>();
	if (divisor_value < 2) {
		return false;
	}
	return SljitTypedExpressionTreeIsSupported(*node.left);
}

bool SljitTypedExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
	case ExecutionExpressionIRKind::CONSTANT:
		return SljitTypedExpressionTreeIsValueNode(node);
	case ExecutionExpressionIRKind::CAST:
		return SljitTypedExpressionTreeValueCastSupported(node);
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
		if (SljitTypedExpressionTreeConstantDivisorReductionSupported(node)) {
			return true;
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
		return TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix) ||
		       SljitTypedExpressionTreeDateYearSupported(node) ||
		       SljitTypedExpressionTreeIntegralCompressSupported(node);
	}
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeInt64CastSupported(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeInt64CastLogic(node);
}

bool SljitTypedExpressionTreeFastPathSupported(const ExecutionExpressionIR &node) {
	if (SljitTypedExpressionTreeValueCastSupported(node)) {
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC) {
		idx_t source_index;
		string prefix;
		if (TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix)) {
			return true;
		}
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS) {
			return node.children.size() == 2 && node.children[0] && node.children[1] &&
			       node.children[1]->kind == ExecutionExpressionIRKind::CONSTANT &&
			       !node.children[1]->constant.IsNull() &&
			       SljitTypedExpressionTreeFastPathSupported(*node.children[0]);
		}
		return false;
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

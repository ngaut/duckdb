//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_type_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"

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

static bool SljitTypedExpressionTreeIsUInt8Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::UTINYINT && node.physical_type == PhysicalType::UINT8;
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
	       SljitTypedExpressionTreeIsBoolNode(node) || SljitTypedExpressionTreeIsDecimal64Node(node) ||
	       SljitTypedExpressionTreeIsUInt8Node(node);
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
	       (SljitTypedExpressionTreeIsBoolNode(left) && SljitTypedExpressionTreeIsBoolNode(right)) ||
	       (SljitTypedExpressionTreeIsDecimal64Node(left) && SljitTypedExpressionTreeIsDecimal64Node(right));
}

static bool TryGetSljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node, SljitNativeIntegerKind &kind) {
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	if (SljitTypedExpressionTreeIsUInt8Node(node)) {
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

bool TryGetSljitTypedExpressionTreeResultKind(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind) {
	return TryGetSljitTypedExpressionTreeIntegerKind(root, kind);
}

} // namespace duckdb

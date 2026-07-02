//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_decimal_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "sljit_native_integer_plan_helpers.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

static bool IsNativeDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool Decimal64BinaryHasRawSemantics(const ExecutionExpressionIR &root) {
	D_ASSERT(root.left);
	D_ASSERT(root.right);
	switch (root.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(root.return_type) == DecimalType::GetScale(root.left->return_type) &&
		       DecimalType::GetScale(root.return_type) == DecimalType::GetScale(root.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(root.return_type) ==
		       DecimalType::GetScale(root.left->return_type) + DecimalType::GetScale(root.right->return_type);
	default:
		return false;
	}
}

static bool TryGetDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max) {
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

bool TryReadNativeDecimal64BinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                            idx_t &left_index, idx_t &right_index, int64_t &result_min,
                                            int64_t &result_max) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDecimal64Node(root) || !root.left || !root.right ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeDecimal64Node(*root.left) ||
	    !IsNativeDecimal64Node(*root.right) || !TryGetNativeIntegerBinaryOp(root.binary_op, native_op) ||
	    !Decimal64BinaryHasRawSemantics(root) || !TryGetDecimal64Range(root.return_type, result_min, result_max)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

static bool TryReadNativeDecimal64ReferenceConstant(const ExecutionExpressionIR &candidate,
                                                    const ExecutionExpressionIR &constant, idx_t &source_index,
                                                    int64_t &constant_value) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeDecimal64Node(candidate) ||
	    !IsNativeDecimal64Node(constant)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeIntegerConstant(constant, SljitNativeIntegerKind::DECIMAL64, constant_value, constant_is_null) ||
	    constant_is_null) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

bool TryReadNativeDecimal64BinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          idx_t &source_index, int64_t &constant_value, bool &constant_on_left,
                                          int64_t &result_min, int64_t &result_max) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDecimal64Node(root) || !root.left || !root.right ||
	    !TryGetNativeIntegerBinaryOp(root.binary_op, native_op) || !Decimal64BinaryHasRawSemantics(root) ||
	    !TryGetDecimal64Range(root.return_type, result_min, result_max)) {
		return false;
	}
	if (TryReadNativeDecimal64ReferenceConstant(*root.left, *root.right, source_index, constant_value)) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeDecimal64ReferenceConstant(*root.right, *root.left, source_index, constant_value)) {
		return false;
	}
	constant_on_left = native_op == SljitNativeIntegerBinaryOp::SUBTRACT;
	return true;
}

} // namespace duckdb

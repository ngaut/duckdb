//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_date_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "sljit_native_integer_plan_helpers.hpp"

namespace duckdb {

static bool IsNativeDateNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DATE && node.physical_type == PhysicalType::INT32;
}

static bool IsNativeIntegerInt32Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::INTEGER && node.physical_type == PhysicalType::INT32;
}

static bool TryReadNativeDateReference(const ExecutionExpressionIR &node, idx_t &source_index) {
	if (node.kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeDateNode(node)) {
		return false;
	}
	source_index = node.ref_index;
	return true;
}

static bool TryReadNativeInt32Reference(const ExecutionExpressionIR &node, idx_t &source_index) {
	if (node.kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeIntegerInt32Node(node)) {
		return false;
	}
	source_index = node.ref_index;
	return true;
}

static bool TryReadNativeInt32Constant(const ExecutionExpressionIR &constant, int64_t &constant_value,
                                       bool &constant_is_null) {
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT || !IsNativeIntegerInt32Node(constant)) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	constant_value = constant.constant.GetValue<int32_t>();
	return true;
}

bool TryReadNativeDateBinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                       idx_t &date_source_index, idx_t &integer_source_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDateNode(root) || !root.left || !root.right ||
	    !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	if (native_op == SljitNativeIntegerBinaryOp::ADD) {
		if (TryReadNativeDateReference(*root.left, date_source_index) &&
		    TryReadNativeInt32Reference(*root.right, integer_source_index)) {
			return true;
		}
		if (TryReadNativeDateReference(*root.right, date_source_index) &&
		    TryReadNativeInt32Reference(*root.left, integer_source_index)) {
			return true;
		}
		return false;
	}
	if (native_op == SljitNativeIntegerBinaryOp::SUBTRACT) {
		return TryReadNativeDateReference(*root.left, date_source_index) &&
		       TryReadNativeInt32Reference(*root.right, integer_source_index);
	}
	return false;
}

bool TryReadNativeDateBinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                     idx_t &date_source_index, int64_t &constant_value, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDateNode(root) || !root.left || !root.right ||
	    !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	bool constant_is_null;
	if (native_op == SljitNativeIntegerBinaryOp::ADD) {
		if (TryReadNativeDateReference(*root.left, date_source_index) &&
		    TryReadNativeInt32Constant(*root.right, constant_value, constant_is_null) && !constant_is_null) {
			constant_on_left = false;
			return true;
		}
		if (TryReadNativeDateReference(*root.right, date_source_index) &&
		    TryReadNativeInt32Constant(*root.left, constant_value, constant_is_null) && !constant_is_null) {
			constant_on_left = false;
			return true;
		}
		return false;
	}
	if (native_op == SljitNativeIntegerBinaryOp::SUBTRACT) {
		if (TryReadNativeDateReference(*root.left, date_source_index) &&
		    TryReadNativeInt32Constant(*root.right, constant_value, constant_is_null) && !constant_is_null) {
			constant_on_left = false;
			return true;
		}
		return false;
	}
	return false;
}

} // namespace duckdb

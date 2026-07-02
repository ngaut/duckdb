//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_double_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_double_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

string NativeDoubleBinaryReason(SljitNativeDoubleBinaryOp op) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		return "native:double-add-constant";
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		return "native:double-subtract-constant";
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		return "native:double-multiply-constant";
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return "native:double-divide-constant";
	default:
		throw InternalException("Unknown SLJIT native double binary operator");
	}
}

string NativeDoubleBinaryReferenceReason(SljitNativeDoubleBinaryOp op) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		return "native:double-add-references";
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		return "native:double-subtract-references";
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		return "native:double-multiply-references";
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return "native:double-divide-references";
	default:
		throw InternalException("Unknown SLJIT native double binary operator");
	}
}

static bool TryGetNativeDoubleBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeDoubleBinaryOp &native_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		native_op = SljitNativeDoubleBinaryOp::ADD;
		return true;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		native_op = SljitNativeDoubleBinaryOp::SUBTRACT;
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		native_op = SljitNativeDoubleBinaryOp::MULTIPLY;
		return true;
	case ExecutionExpressionBinaryOp::DIVIDE:
		native_op = SljitNativeDoubleBinaryOp::DIVIDE;
		return true;
	default:
		return false;
	}
}

static bool IsNativeFloatingNode(const ExecutionExpressionIR &node) {
	return (node.return_type.id() == LogicalTypeId::FLOAT && node.physical_type == PhysicalType::FLOAT) ||
	       (node.return_type.id() == LogicalTypeId::DOUBLE && node.physical_type == PhysicalType::DOUBLE);
}

static bool IsNativeDoubleNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DOUBLE && node.physical_type == PhysicalType::DOUBLE;
}

static bool IsNativeFloatNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::FLOAT && node.physical_type == PhysicalType::FLOAT;
}

static bool IsNativeFloatSource(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::FLOAT;
}

static double NativeDecimalScaleFactor(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto scale = DecimalType::GetScale(type);
	D_ASSERT(scale < Hugeint::CACHED_POWERS_OF_TEN);
	return Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[scale]);
}

static bool TryReadNativeDoubleReference(const ExecutionExpressionIR &node, SljitNativeDoubleSourceKind &source_kind,
                                         idx_t &source_index, double &source_scale) {
	source_scale = 1;
	if (node.kind == ExecutionExpressionIRKind::REFERENCE && IsNativeFloatNode(node)) {
		source_kind = SljitNativeDoubleSourceKind::FLOAT;
		source_index = node.ref_index;
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE && IsNativeDoubleNode(node)) {
		source_kind = SljitNativeDoubleSourceKind::DOUBLE;
		source_index = node.ref_index;
		return true;
	}
	if (node.kind != ExecutionExpressionIRKind::CAST || !IsNativeFloatingNode(node) || !node.left ||
	    node.left->kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	if (IsNativeFloatNode(node)) {
		return false;
	}

	auto &source = *node.left;
	source_index = source.ref_index;
	if (source.return_type.id() == LogicalTypeId::BIGINT && source.physical_type == PhysicalType::INT64) {
		source_kind = SljitNativeDoubleSourceKind::INT64_TO_DOUBLE;
		return true;
	}
	if (source.return_type.id() == LogicalTypeId::HUGEINT && source.physical_type == PhysicalType::INT128) {
		source_kind = SljitNativeDoubleSourceKind::INT128_TO_DOUBLE;
		return true;
	}
	if (source.return_type.id() == LogicalTypeId::DECIMAL && source.physical_type == PhysicalType::INT64) {
		source_kind = SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE;
		source_scale = NativeDecimalScaleFactor(source.return_type);
		return true;
	}
	if (source.return_type.id() == LogicalTypeId::DECIMAL && source.physical_type == PhysicalType::INT128) {
		source_kind = SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
		source_scale = NativeDecimalScaleFactor(source.return_type);
		return true;
	}
	return false;
}

static bool TryReadNativeDoubleConstant(const ExecutionExpressionIR &constant, double &constant_value,
                                        bool &constant_is_null) {
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT || !IsNativeFloatingNode(constant)) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	if (IsNativeFloatNode(constant)) {
		constant_value = constant.constant.GetValue<float>();
	} else {
		constant_value = constant.constant.GetValue<double>();
	}
	return true;
}

static bool TryReadNativeDoubleReferenceConstant(const ExecutionExpressionIR &candidate,
                                                 const ExecutionExpressionIR &constant,
                                                 SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                                 double &source_scale, double &constant_value) {
	if (!TryReadNativeDoubleReference(candidate, source_kind, source_index, source_scale)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeDoubleConstant(constant, constant_value, constant_is_null) || constant_is_null) {
		return false;
	}
	if (IsNativeFloatSource(source_kind) != IsNativeFloatNode(constant)) {
		return false;
	}
	return true;
}

bool TryReadNativeDoubleBinaryConstant(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                       SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                       double &source_scale, double &constant_value, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeFloatingNode(root) || !root.left || !root.right ||
	    !TryGetNativeDoubleBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	if (TryReadNativeDoubleReferenceConstant(*root.left, *root.right, source_kind, source_index, source_scale,
	                                         constant_value)) {
		constant_on_left = false;
		if (native_op == SljitNativeDoubleBinaryOp::DIVIDE && constant_value == 0.0) {
			return false;
		}
		return true;
	}
	if (!TryReadNativeDoubleReferenceConstant(*root.right, *root.left, source_kind, source_index, source_scale,
	                                          constant_value)) {
		return false;
	}
	constant_on_left = true;
	if (native_op == SljitNativeDoubleBinaryOp::DIVIDE) {
		return false;
	}
	return true;
}

bool TryReadNativeDoubleBinaryReferences(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                         SljitNativeDoubleSourceKind &left_kind, idx_t &left_index, double &left_scale,
                                         SljitNativeDoubleSourceKind &right_kind, idx_t &right_index,
                                         double &right_scale) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeFloatingNode(root) || !root.left || !root.right ||
	    !TryGetNativeDoubleBinaryOp(root.binary_op, native_op) ||
	    !TryReadNativeDoubleReference(*root.left, left_kind, left_index, left_scale) ||
	    !TryReadNativeDoubleReference(*root.right, right_kind, right_index, right_scale)) {
		return false;
	}
	if (IsNativeFloatSource(left_kind) != IsNativeFloatSource(right_kind)) {
		return false;
	}
	return true;
}

bool TryReadNativeDoubleCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                        SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                        double &source_scale, double &constant_value, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	if (TryReadNativeDoubleReferenceConstant(*root.left, *root.right, source_kind, source_index, source_scale,
	                                         constant_value)) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeDoubleReferenceConstant(*root.right, *root.left, source_kind, source_index, source_scale,
	                                          constant_value)) {
		return false;
	}
	constant_on_left = true;
	return true;
}

bool TryReadNativeDoubleCompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                          SljitNativeDoubleSourceKind &left_kind, idx_t &left_index, double &left_scale,
                                          SljitNativeDoubleSourceKind &right_kind, idx_t &right_index,
                                          double &right_scale) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op) ||
	    !TryReadNativeDoubleReference(*root.left, left_kind, left_index, left_scale) ||
	    !TryReadNativeDoubleReference(*root.right, right_kind, right_index, right_scale)) {
		return false;
	}
	if (IsNativeFloatSource(left_kind) != IsNativeFloatSource(right_kind)) {
		return false;
	}
	return true;
}

} // namespace duckdb

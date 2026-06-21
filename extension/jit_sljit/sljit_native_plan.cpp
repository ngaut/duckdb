#include "sljit_native_plan.hpp"

#include "sljit_native_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include <algorithm>

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

static bool TryGetNativeIntegerKind(LogicalTypeId logical_type, PhysicalType physical_type,
                                    SljitNativeIntegerKind &kind) {
	switch (logical_type) {
	case LogicalTypeId::INTEGER:
		if (physical_type != PhysicalType::INT32) {
			return false;
		}
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case LogicalTypeId::BIGINT:
		if (physical_type != PhysicalType::INT64) {
			return false;
		}
		kind = SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool TryGetNativeIntegerKind(const ExecutionExpressionIR &node, SljitNativeIntegerKind &kind) {
	return TryGetNativeIntegerKind(node.return_type.id(), node.physical_type, kind);
}

static bool TryGetNativeComparableIntegerKind(const ExecutionExpressionIR &node, SljitNativeIntegerKind &kind) {
	if (TryGetNativeIntegerKind(node, kind)) {
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DATE && node.physical_type == PhysicalType::INT32) {
		kind = SljitNativeIntegerKind::INT32;
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64) {
		kind = SljitNativeIntegerKind::DECIMAL64;
		return true;
	}
	return false;
}

static bool IsNativeInt128ComparableNode(const ExecutionExpressionIR &node) {
	if (node.physical_type != PhysicalType::INT128) {
		return false;
	}
	return node.return_type.id() == LogicalTypeId::DECIMAL || node.return_type.id() == LogicalTypeId::HUGEINT;
}

static bool TryGetNativeSignedIntegerWidth(LogicalTypeId logical_type, PhysicalType physical_type,
                                           SljitNativeSignedIntegerWidth &width) {
	switch (logical_type) {
	case LogicalTypeId::TINYINT:
		if (physical_type != PhysicalType::INT8) {
			return false;
		}
		width = SljitNativeSignedIntegerWidth::INT8;
		return true;
	case LogicalTypeId::SMALLINT:
		if (physical_type != PhysicalType::INT16) {
			return false;
		}
		width = SljitNativeSignedIntegerWidth::INT16;
		return true;
	case LogicalTypeId::INTEGER:
		if (physical_type != PhysicalType::INT32) {
			return false;
		}
		width = SljitNativeSignedIntegerWidth::INT32;
		return true;
	case LogicalTypeId::BIGINT:
		if (physical_type != PhysicalType::INT64) {
			return false;
		}
		width = SljitNativeSignedIntegerWidth::INT64;
		return true;
	default:
		return false;
	}
}

bool TryGetNativeSignedIntegerWidth(const ExecutionExpressionIR &node, SljitNativeSignedIntegerWidth &width) {
	return TryGetNativeSignedIntegerWidth(node.return_type.id(), node.physical_type, width);
}

static bool TryGetNativeUnsignedIntegerWidth(LogicalTypeId logical_type, PhysicalType physical_type,
                                             SljitNativeUnsignedIntegerWidth &width) {
	switch (logical_type) {
	case LogicalTypeId::UTINYINT:
		if (physical_type != PhysicalType::UINT8) {
			return false;
		}
		width = SljitNativeUnsignedIntegerWidth::UINT8;
		return true;
	case LogicalTypeId::USMALLINT:
		if (physical_type != PhysicalType::UINT16) {
			return false;
		}
		width = SljitNativeUnsignedIntegerWidth::UINT16;
		return true;
	case LogicalTypeId::UINTEGER:
		if (physical_type != PhysicalType::UINT32) {
			return false;
		}
		width = SljitNativeUnsignedIntegerWidth::UINT32;
		return true;
	default:
		return false;
	}
}

static bool TryGetNativeUnsignedIntegerWidth(const ExecutionExpressionIR &node,
                                             SljitNativeUnsignedIntegerWidth &width) {
	return TryGetNativeUnsignedIntegerWidth(node.return_type.id(), node.physical_type, width);
}

static string NativeIntegerTypeName(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::UINT8:
		return "utinyint";
	case SljitNativeIntegerKind::INT32:
		return "integer";
	case SljitNativeIntegerKind::INT64:
		return "bigint";
	case SljitNativeIntegerKind::DECIMAL64:
		return "decimal64";
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

static bool ReadNativeIntegerValue(const Value &value, SljitNativeIntegerKind kind, int64_t &constant_value) {
	switch (kind) {
	case SljitNativeIntegerKind::UINT8:
		constant_value = value.GetValue<uint8_t>();
		return true;
	case SljitNativeIntegerKind::INT32:
		constant_value =
		    value.type().id() == LogicalTypeId::DATE ? value.GetValueUnsafe<date_t>().days : value.GetValue<int32_t>();
		return true;
	case SljitNativeIntegerKind::INT64:
		constant_value = value.GetValue<int64_t>();
		return true;
	case SljitNativeIntegerKind::DECIMAL64:
		if (value.type().id() != LogicalTypeId::DECIMAL || value.type().InternalType() != PhysicalType::INT64) {
			return false;
		}
		constant_value = value.GetValueUnsafe<int64_t>();
		return true;
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

static bool TryReadNativeIntegerConstant(const ExecutionExpressionIR &constant, SljitNativeIntegerKind kind,
                                         int64_t &constant_value, bool &constant_is_null) {
	SljitNativeIntegerKind constant_kind;
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	if (!TryGetNativeComparableIntegerKind(constant, constant_kind) || constant_kind != kind) {
		return false;
	}
	return ReadNativeIntegerValue(constant.constant, kind, constant_value);
}

static bool TryReadNativeIntegerReferenceConstant(const ExecutionExpressionIR &candidate,
                                                  const ExecutionExpressionIR &constant, SljitNativeIntegerKind &kind,
                                                  idx_t &source_index, int64_t &constant_value) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !TryGetNativeIntegerKind(candidate, kind)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeIntegerConstant(constant, kind, constant_value, constant_is_null) || constant_is_null) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

static bool TryReadNativeComparableReferenceConstant(const ExecutionExpressionIR &candidate,
                                                     const ExecutionExpressionIR &constant,
                                                     SljitNativeIntegerKind &kind, idx_t &source_index,
                                                     int64_t &constant_value) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !TryGetNativeComparableIntegerKind(candidate, kind)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeIntegerConstant(constant, kind, constant_value, constant_is_null) || constant_is_null) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

static bool TryReadNativeComparableReferenceMaybeNullConstant(const ExecutionExpressionIR &candidate,
                                                              const ExecutionExpressionIR &constant,
                                                              SljitNativeIntegerKind &kind, idx_t &source_index,
                                                              int64_t &constant_value, bool &constant_is_null) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !TryGetNativeComparableIntegerKind(candidate, kind)) {
		return false;
	}
	if (!TryReadNativeIntegerConstant(constant, kind, constant_value, constant_is_null)) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

static bool TryGetNativeIntegerBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		native_op = SljitNativeIntegerBinaryOp::ADD;
		return true;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		native_op = SljitNativeIntegerBinaryOp::SUBTRACT;
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		native_op = SljitNativeIntegerBinaryOp::MULTIPLY;
		return true;
	default:
		return false;
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

string NativeIntegerBinaryReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	auto type_name = NativeIntegerTypeName(kind);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "native:" + type_name + "-add-constant";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "native:" + type_name + "-subtract-constant";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "native:" + type_name + "-multiply-constant";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

string NativeIntegerBinaryReferenceReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	auto type_name = NativeIntegerTypeName(kind);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "native:" + type_name + "-add-references";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "native:" + type_name + "-subtract-references";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "native:" + type_name + "-multiply-references";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

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

string NativeIntegerCompareReason(SljitNativeIntegerKind kind) {
	return "native:" + NativeIntegerTypeName(kind) + "-compare-constant";
}

string NativeIntegerCompareReferenceReason(SljitNativeIntegerKind kind) {
	return "native:" + NativeIntegerTypeName(kind) + "-compare-references";
}

string NativeIntegerCastReason(SljitNativeSignedIntegerWidth source_width, SljitNativeSignedIntegerWidth target_width,
                               bool try_cast) {
	return string("native:") + (try_cast ? "try-" : "") + NativeSignedIntegerTypeName(source_width) + "-to-" +
	       NativeSignedIntegerTypeName(target_width) + "-cast";
}

string NativeIntegerCoalesceReason(SljitNativeSignedIntegerWidth width) {
	return "native:" + NativeSignedIntegerTypeName(width) + "-coalesce";
}

string NativeIntegerInListReason(SljitNativeIntegerKind kind, bool not_in) {
	return "native:" + NativeIntegerTypeName(kind) + (not_in ? "-not-in-list" : "-in-list");
}

string NativeIntegerBetweenReason(SljitNativeIntegerKind kind, bool not_between) {
	return "native:" + NativeIntegerTypeName(kind) + (not_between ? "-not-between" : "-between");
}

string NativeNullCheckReason(SljitNativeNullCheckOp op) {
	switch (op) {
	case SljitNativeNullCheckOp::IS_NULL:
		return "native:is-null";
	case SljitNativeNullCheckOp::IS_NOT_NULL:
		return "native:is-not-null";
	default:
		throw InternalException("Unknown SLJIT native null-check operator");
	}
}

static bool TryGetNativeIntegerCompareOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerCompareOp &compare_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		compare_op = SljitNativeIntegerCompareOp::EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		compare_op = SljitNativeIntegerCompareOp::NOT_EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		compare_op = SljitNativeIntegerCompareOp::LESS_THAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		compare_op = SljitNativeIntegerCompareOp::GREATER_THAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		compare_op = SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		compare_op = SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
		return true;
	default:
		return false;
	}
}

bool TryReadNativeIntegerBinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                        SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                        bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !TryGetNativeIntegerKind(root, kind) || !root.left ||
	    !root.right || !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	SljitNativeIntegerKind candidate_kind;
	if (TryReadNativeIntegerReferenceConstant(*root.left, *root.right, candidate_kind, source_index, constant_value) &&
	    candidate_kind == kind) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeIntegerReferenceConstant(*root.right, *root.left, candidate_kind, source_index, constant_value) ||
	    candidate_kind != kind) {
		return false;
	}
	constant_on_left = native_op == SljitNativeIntegerBinaryOp::SUBTRACT;
	return true;
}

bool TryReadNativeIntegerBinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          SljitNativeIntegerKind &kind, idx_t &left_index, idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !TryGetNativeIntegerKind(root, kind) || !root.left ||
	    !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.return_type ||
	    root.right->return_type != root.return_type || !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

static bool IsNativeDoubleNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DOUBLE && node.physical_type == PhysicalType::DOUBLE;
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
	if (node.kind == ExecutionExpressionIRKind::REFERENCE && IsNativeDoubleNode(node)) {
		source_kind = SljitNativeDoubleSourceKind::DOUBLE;
		source_index = node.ref_index;
		return true;
	}
	if (node.kind != ExecutionExpressionIRKind::CAST || !IsNativeDoubleNode(node) || !node.left ||
	    node.left->kind != ExecutionExpressionIRKind::REFERENCE) {
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
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT || !IsNativeDoubleNode(constant)) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	constant_value = constant.constant.GetValue<double>();
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
	return true;
}

bool TryReadNativeDoubleBinaryConstant(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                       SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                       double &source_scale, double &constant_value, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDoubleNode(root) || !root.left || !root.right ||
	    !TryGetNativeDoubleBinaryOp(root.binary_op, native_op)) {
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

bool TryReadNativeDoubleBinaryReferences(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                         SljitNativeDoubleSourceKind &left_kind, idx_t &left_index, double &left_scale,
                                         SljitNativeDoubleSourceKind &right_kind, idx_t &right_index,
                                         double &right_scale) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !IsNativeDoubleNode(root) || !root.left || !root.right ||
	    !TryGetNativeDoubleBinaryOp(root.binary_op, native_op) ||
	    !TryReadNativeDoubleReference(*root.left, left_kind, left_index, left_scale) ||
	    !TryReadNativeDoubleReference(*root.right, right_kind, right_index, right_scale)) {
		return false;
	}
	return true;
}

static bool TryReadNativeDoubleCompareConstant(const ExecutionExpressionIR &root,
                                               SljitNativeIntegerCompareOp &compare_op,
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

static bool TryReadNativeDoubleCompareReferences(const ExecutionExpressionIR &root,
                                                 SljitNativeIntegerCompareOp &compare_op,
                                                 SljitNativeDoubleSourceKind &left_kind, idx_t &left_index,
                                                 double &left_scale, SljitNativeDoubleSourceKind &right_kind,
                                                 idx_t &right_index, double &right_scale) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op) ||
	    !TryReadNativeDoubleReference(*root.left, left_kind, left_index, left_scale) ||
	    !TryReadNativeDoubleReference(*root.right, right_kind, right_index, right_scale)) {
		return false;
	}
	return true;
}

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

bool TryReadNativeIntegerCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                         SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                         bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	SljitNativeIntegerKind candidate_kind;
	if (TryReadNativeComparableReferenceConstant(*root.left, *root.right, candidate_kind, source_index,
	                                             constant_value)) {
		kind = candidate_kind;
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeComparableReferenceConstant(*root.right, *root.left, candidate_kind, source_index,
	                                              constant_value)) {
		return false;
	}
	kind = candidate_kind;
	constant_on_left = true;
	return true;
}

bool TryReadNativeIntegerCompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                           SljitNativeIntegerKind &kind, idx_t &left_index, idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.right->return_type ||
	    !TryGetNativeComparableIntegerKind(*root.left, kind) ||
	    !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

static bool TryReadNativeInt128Value(const ExecutionExpressionIR &constant, const LogicalType &source_type,
                                     uint64_t &lower, int64_t &upper, bool &constant_is_null) {
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT || !IsNativeInt128ComparableNode(constant) ||
	    constant.return_type != source_type) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		lower = 0;
		upper = 0;
		return true;
	}
	if (constant.constant.type().InternalType() != PhysicalType::INT128) {
		return false;
	}
	auto value = constant.constant.GetValueUnsafe<hugeint_t>();
	lower = value.lower;
	upper = value.upper;
	return true;
}

static bool TryReadNativeInt128ReferenceConstant(const ExecutionExpressionIR &candidate,
                                                 const ExecutionExpressionIR &constant, idx_t &source_index,
                                                 uint64_t &lower, int64_t &upper) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeInt128ComparableNode(candidate)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeInt128Value(constant, candidate.return_type, lower, upper, constant_is_null) ||
	    constant_is_null) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

static bool TryReadNativeInt128CompareReferences(const ExecutionExpressionIR &root,
                                                 SljitNativeIntegerCompareOp &compare_op, idx_t &left_index,
                                                 idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.right->return_type ||
	    !IsNativeInt128ComparableNode(*root.left) || !IsNativeInt128ComparableNode(*root.right) ||
	    !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

static bool TryReadNativeInt128CompareConstant(const ExecutionExpressionIR &root,
                                               SljitNativeIntegerCompareOp &compare_op, idx_t &source_index,
                                               uint64_t &lower, int64_t &upper, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	if (TryReadNativeInt128ReferenceConstant(*root.left, *root.right, source_index, lower, upper)) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeInt128ReferenceConstant(*root.right, *root.left, source_index, lower, upper)) {
		return false;
	}
	constant_on_left = true;
	return true;
}

static bool TryReadNativeIntegerCompareNullConstant(const ExecutionExpressionIR &root) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right) {
		return false;
	}
	SljitNativeIntegerCompareOp compare_op;
	if (!TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	auto try_read_null_compare = [](const ExecutionExpressionIR &candidate, const ExecutionExpressionIR &constant) {
		if (candidate.kind != ExecutionExpressionIRKind::REFERENCE ||
		    constant.kind != ExecutionExpressionIRKind::CONSTANT || !constant.constant.IsNull()) {
			return false;
		}
		SljitNativeIntegerKind kind;
		if (TryGetNativeComparableIntegerKind(candidate, kind)) {
			int64_t constant_value;
			bool constant_is_null;
			return TryReadNativeIntegerConstant(constant, kind, constant_value, constant_is_null) && constant_is_null;
		}
		if (IsNativeInt128ComparableNode(candidate) && IsNativeInt128ComparableNode(constant) &&
		    candidate.return_type == constant.return_type) {
			return true;
		}
		return false;
	};
	return try_read_null_compare(*root.left, *root.right) || try_read_null_compare(*root.right, *root.left);
}

static bool IsNativeCompareRoot(const ExecutionExpressionIR &root) {
	SljitNativeIntegerCompareOp compare_op;
	return root.kind == ExecutionExpressionIRKind::BINARY && root.return_type.id() == LogicalTypeId::BOOLEAN &&
	       TryGetNativeIntegerCompareOp(root.binary_op, compare_op);
}

bool TryReadNativeIntegerCast(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                              SljitNativeSignedIntegerWidth &target_width, idx_t &source_index, bool &try_cast) {
	if (root.kind != ExecutionExpressionIRKind::CAST || !root.left ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.left, source_width) ||
	    !TryGetNativeSignedIntegerWidth(root, target_width)) {
		return false;
	}
	source_index = root.left->ref_index;
	try_cast = root.try_cast;
	return true;
}

bool TryReadNativeSignedToUnsignedIntegerCast(const ExecutionExpressionIR &root,
                                              SljitNativeSignedIntegerWidth &source_width,
                                              SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                              bool &try_cast) {
	if (root.kind != ExecutionExpressionIRKind::CAST || !root.left ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.left, source_width) ||
	    !TryGetNativeUnsignedIntegerWidth(root, target_width)) {
		return false;
	}
	source_index = root.left->ref_index;
	try_cast = root.try_cast;
	return true;
}

static bool TryGetNativeSignedIntegerConstant(const ExecutionExpressionIR &node, SljitNativeSignedIntegerWidth width,
                                              int64_t &constant_value, bool &constant_is_null) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}
	constant_is_null = node.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		constant_value = node.constant.GetValue<int8_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT16:
		constant_value = node.constant.GetValue<int16_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT32:
		constant_value = node.constant.GetValue<int32_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT64:
		constant_value = node.constant.GetValue<int64_t>();
		return true;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

bool TryReadNativeIntegralCompress(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                                   SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                   int64_t &minimum) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS || root.children.size() != 2 ||
	    !root.children[0] || !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.children[0], source_width) ||
	    !TryGetNativeUnsignedIntegerWidth(root, target_width)) {
		return false;
	}
	bool minimum_is_null;
	if (!TryGetNativeSignedIntegerConstant(*root.children[1], source_width, minimum, minimum_is_null) ||
	    minimum_is_null) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	return true;
}

bool TryReadNativeIntegralDecompress(const ExecutionExpressionIR &root, SljitNativeUnsignedIntegerWidth &source_width,
                                     SljitNativeSignedIntegerWidth &target_width, idx_t &source_index,
                                     int64_t &minimum) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_DECOMPRESS || root.children.size() != 2 ||
	    !root.children[0] || !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeUnsignedIntegerWidth(*root.children[0], source_width) ||
	    !TryGetNativeSignedIntegerWidth(root, target_width)) {
		return false;
	}
	bool minimum_is_null;
	if (!TryGetNativeSignedIntegerConstant(*root.children[1], target_width, minimum, minimum_is_null) ||
	    minimum_is_null) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	return true;
}

bool TryReadNativeIntegerCoalesce(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &width,
                                  idx_t &source_index, SljitNativeCoalesceRhsKind &rhs_kind, idx_t &right_source_index,
                                  int64_t &constant_value, bool &constant_is_null) {
	if (root.kind != ExecutionExpressionIRKind::COALESCE || root.children.size() != 2 ||
	    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(root, width) || root.children[0]->return_type != root.return_type ||
	    root.children[1]->return_type != root.return_type) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	auto &rhs = *root.children[1];
	if (TryGetNativeSignedIntegerConstant(rhs, width, constant_value, constant_is_null)) {
		rhs_kind = SljitNativeCoalesceRhsKind::CONSTANT;
		right_source_index = 0;
		return true;
	}
	if (rhs.kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	rhs_kind = SljitNativeCoalesceRhsKind::REFERENCE;
	right_source_index = rhs.ref_index;
	constant_value = 0;
	constant_is_null = false;
	return true;
}

bool TryReadNativeIntegerInList(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                vector<int64_t> &constants, bool &list_has_null, bool &not_in) {
	constants.clear();
	list_has_null = false;
	not_in = root.not_in;

	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() < 2 ||
		    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
		    !TryGetNativeComparableIntegerKind(*root.children[0], kind)) {
			return false;
		}
		source_index = root.children[0]->ref_index;
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			auto &child = *root.children[child_idx];
			bool constant_is_null;
			int64_t constant_value;
			if (!TryReadNativeIntegerConstant(child, kind, constant_value, constant_is_null)) {
				return false;
			}
			if (constant_is_null) {
				list_has_null = true;
				continue;
			}
			constants.push_back(constant_value);
		}
		return true;
	}

	auto candidate = &root;
	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT &&
	    root.left && root.left->kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    root.left->conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		not_in = true;
		candidate = root.left.get();
	}
	if (candidate->kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    candidate->conjunction_op != ExecutionExpressionConjunctionOp::OR || candidate->children.empty()) {
		return false;
	}

	bool initialized = false;
	for (auto &child_ptr : candidate->children) {
		auto &child = *child_ptr;
		if (child.kind != ExecutionExpressionIRKind::BINARY ||
		    child.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL || !child.left || !child.right) {
			return false;
		}
		SljitNativeIntegerKind child_kind;
		idx_t child_source_index;
		int64_t constant_value;
		bool constant_is_null;
		if (!TryReadNativeComparableReferenceMaybeNullConstant(*child.left, *child.right, child_kind,
		                                                       child_source_index, constant_value, constant_is_null) &&
		    !TryReadNativeComparableReferenceMaybeNullConstant(*child.right, *child.left, child_kind,
		                                                       child_source_index, constant_value, constant_is_null)) {
			return false;
		}
		if (!initialized) {
			kind = child_kind;
			source_index = child_source_index;
			initialized = true;
		} else if (kind != child_kind || source_index != child_source_index) {
			return false;
		}
		if (constant_is_null) {
			list_has_null = true;
		} else {
			constants.push_back(constant_value);
		}
	}
	return initialized;
}

bool TryReadNativeIntegerBetween(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                 int64_t &lower, int64_t &upper, bool &lower_inclusive, bool &upper_inclusive,
                                 bool &not_between) {
	if (root.kind != ExecutionExpressionIRKind::BETWEEN || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    root.children.size() != 3 || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeComparableIntegerKind(*root.children[0], kind)) {
		return false;
	}
	auto &lower_node = *root.children[1];
	auto &upper_node = *root.children[2];
	bool lower_is_null;
	bool upper_is_null;
	if (!TryReadNativeIntegerConstant(lower_node, kind, lower, lower_is_null) ||
	    !TryReadNativeIntegerConstant(upper_node, kind, upper, upper_is_null) || lower_is_null || upper_is_null) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	lower_inclusive = root.lower_inclusive;
	upper_inclusive = root.upper_inclusive;
	not_between = root.not_between;
	return true;
}

bool TryReadNativeStringPrefixConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &prefix) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::STRING_PREFIX ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR ||
	    root.children[1]->kind != ExecutionExpressionIRKind::CONSTANT ||
	    root.children[1]->return_type.id() != LogicalTypeId::VARCHAR || root.children[1]->constant.IsNull()) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	prefix = StringValue::Get(root.children[1]->constant);
	return true;
}

static bool TryReadNativeStringMatchConstant(const ExecutionExpressionIR &root,
                                             ExecutionExpressionIntrinsicKind intrinsic, idx_t &source_index,
                                             string &constant) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC || root.intrinsic != intrinsic ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR ||
	    root.children[1]->kind != ExecutionExpressionIRKind::CONSTANT ||
	    root.children[1]->return_type.id() != LogicalTypeId::VARCHAR || root.children[1]->constant.IsNull()) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	constant = StringValue::Get(root.children[1]->constant);
	return true;
}

static bool IsNativeAsciiString(const string &value);

static bool TryReadNativeStringEqualConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &constant) {
	if (root.kind != ExecutionExpressionIRKind::BINARY ||
	    root.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || !root.left || !root.right) {
		return false;
	}
	auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
		if (reference.kind != ExecutionExpressionIRKind::REFERENCE ||
		    reference.return_type.id() != LogicalTypeId::VARCHAR ||
		    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
		    constant_node.return_type.id() != LogicalTypeId::VARCHAR || constant_node.constant.IsNull()) {
			return false;
		}
		auto value = StringValue::Get(constant_node.constant);
		if (!IsNativeAsciiString(value)) {
			return false;
		}
		source_index = reference.ref_index;
		constant = std::move(value);
		return true;
	};
	return try_read(*root.left, *root.right) || try_read(*root.right, *root.left);
}

static bool TryReadNativeStringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                              vector<string> &constants, bool &list_has_null, bool &not_in) {
	constants.clear();
	list_has_null = false;
	not_in = false;
	if (root.kind != ExecutionExpressionIRKind::IN_LIST || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    root.children.size() < 2 || !root.children[0] ||
	    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	not_in = root.not_in;
	for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
		auto &child = *root.children[child_idx];
		if (child.kind != ExecutionExpressionIRKind::CONSTANT || child.return_type.id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		if (child.constant.IsNull()) {
			list_has_null = true;
			continue;
		}
		auto constant = StringValue::Get(child.constant);
		if (!IsNativeAsciiString(constant)) {
			return false;
		}
		constants.push_back(std::move(constant));
	}
	return !constants.empty() || list_has_null;
}

static bool TryReadNativeStringLikeConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                            vector<string> &fragments, bool &anchor_start, bool &anchor_end) {
	string pattern;
	if (!TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_LIKE, source_index, pattern)) {
		return false;
	}
	anchor_start = pattern.empty() || pattern[0] != '%';
	anchor_end = pattern.empty() || pattern[pattern.size() - 1] != '%';
	fragments.clear();
	idx_t fragment_start = 0;
	for (idx_t pattern_idx = 0; pattern_idx <= pattern.size(); pattern_idx++) {
		if (pattern_idx < pattern.size()) {
			if (pattern[pattern_idx] == '_') {
				return false;
			}
			if (pattern[pattern_idx] != '%') {
				continue;
			}
		}
		if (pattern_idx > fragment_start) {
			fragments.push_back(pattern.substr(fragment_start, pattern_idx - fragment_start));
		}
		fragment_start = pattern_idx + 1;
	}
	return true;
}

static bool TryReadNativeInt64Constant(const ExecutionExpressionIR &node, int64_t &value) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT || node.constant.IsNull() || !node.return_type.IsIntegral()) {
		return false;
	}
	Value cast_value;
	string error;
	if (!node.constant.DefaultTryCastAs(LogicalType::BIGINT, cast_value, &error) || cast_value.IsNull()) {
		return false;
	}
	value = cast_value.GetValue<int64_t>();
	return true;
}

static bool IsNativeAsciiString(const string &value) {
	for (auto character : value) {
		if (static_cast<unsigned char>(character) >= 0x80) {
			return false;
		}
	}
	return true;
}

static bool TryReadNativeSubstringReference(const ExecutionExpressionIR &substring, idx_t &source_index,
                                            idx_t &substring_length) {
	if (substring.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    substring.intrinsic != ExecutionExpressionIntrinsicKind::STRING_SUBSTRING ||
	    substring.return_type.id() != LogicalTypeId::VARCHAR || substring.children.size() != 3 ||
	    !substring.children[0] || !substring.children[1] || !substring.children[2] ||
	    substring.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    substring.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
		return false;
	}

	int64_t start;
	int64_t length;
	if (!TryReadNativeInt64Constant(*substring.children[1], start) ||
	    !TryReadNativeInt64Constant(*substring.children[2], length) || start != 1 || length < 0 ||
	    static_cast<uint64_t>(length) > NumericLimits<idx_t>::Maximum()) {
		return false;
	}

	source_index = substring.children[0]->ref_index;
	substring_length = UnsafeNumericCast<idx_t>(length);
	return true;
}

static bool TryReadNativeSubstringConstant(const ExecutionExpressionIR &substring,
                                           const ExecutionExpressionIR &constant, idx_t &source_index,
                                           idx_t &substring_length, string &value) {
	if (!TryReadNativeSubstringReference(substring, source_index, substring_length) ||
	    constant.kind != ExecutionExpressionIRKind::CONSTANT || constant.return_type.id() != LogicalTypeId::VARCHAR ||
	    constant.constant.IsNull()) {
		return false;
	}
	value = StringValue::Get(constant.constant);
	return value.size() == substring_length && IsNativeAsciiString(value);
}

static bool TryReadNativeSubstringEqualConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, string &constant) {
	if (root.kind != ExecutionExpressionIRKind::BINARY ||
	    root.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL || !root.left || !root.right) {
		return false;
	}
	return TryReadNativeSubstringConstant(*root.left, *root.right, source_index, substring_length, constant) ||
	       TryReadNativeSubstringConstant(*root.right, *root.left, source_index, substring_length, constant);
}

bool TryReadNativeStringSubstringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, vector<string> &constants) {
	constants.clear();
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}

	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (root.not_in || root.children.size() < 2 || !root.children[0] ||
		    !TryReadNativeSubstringReference(*root.children[0], source_index, substring_length)) {
			return false;
		}
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			auto &child = *root.children[child_idx];
			if (child.kind != ExecutionExpressionIRKind::CONSTANT || child.return_type.id() != LogicalTypeId::VARCHAR ||
			    child.constant.IsNull()) {
				return false;
			}
			auto constant = StringValue::Get(child.constant);
			if (constant.size() != substring_length || !IsNativeAsciiString(constant)) {
				return false;
			}
			constants.push_back(std::move(constant));
		}
		return !constants.empty();
	}

	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    root.conjunction_op != ExecutionExpressionConjunctionOp::OR || root.children.empty()) {
		return false;
	}

	bool initialized = false;
	for (auto &child : root.children) {
		idx_t child_source_index;
		idx_t child_substring_length;
		string constant;
		if (!TryReadNativeSubstringEqualConstant(*child, child_source_index, child_substring_length, constant)) {
			return false;
		}
		if (!initialized) {
			source_index = child_source_index;
			substring_length = child_substring_length;
			initialized = true;
		} else if (source_index != child_source_index || substring_length != child_substring_length) {
			return false;
		}
		constants.push_back(std::move(constant));
	}
	return initialized && !constants.empty();
}

static bool TryReadNativePredicateNullGuard(const ExecutionExpressionIR &node, vector<idx_t> &source_indices,
                                            bool &has_null_constant) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		source_indices.push_back(node.ref_index);
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		if (node.constant.IsNull()) {
			has_null_constant = true;
		}
		return true;
	}
	return false;
}

bool TryReadNativeNullCheck(const ExecutionExpressionIR &root, SljitNativeNullCheckOp &op, idx_t &source_index) {
	if (root.kind != ExecutionExpressionIRKind::UNARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || root.left->kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	switch (root.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
		op = SljitNativeNullCheckOp::IS_NULL;
		break;
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		op = SljitNativeNullCheckOp::IS_NOT_NULL;
		break;
	default:
		return false;
	}
	source_index = root.left->ref_index;
	return true;
}

bool ShouldTryNativePredicateRoot(const ExecutionExpressionIR &root) {
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (root.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::REFERENCE) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::CONSTANT) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		return true;
	}
	if (IsNativeCompareRoot(root)) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC) {
		switch (root.intrinsic) {
		case ExecutionExpressionIntrinsicKind::STRING_PREFIX:
		case ExecutionExpressionIntrinsicKind::STRING_SUFFIX:
		case ExecutionExpressionIntrinsicKind::STRING_CONTAINS:
		case ExecutionExpressionIntrinsicKind::STRING_LIKE:
			return true;
		default:
			break;
		}
	}
	return false;
}

static void AppendSljitNativePredicateSourceIndex(vector<idx_t> &source_indices, idx_t source_index) {
	source_indices.push_back(source_index);
}

static void CollectSljitNativePredicateSourceIndices(const SljitNativePredicate &predicate,
                                                     vector<idx_t> &source_indices) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.source_index);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.source_index);
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.right_source_index);
		return;
	case SljitNativePredicateKind::NOT:
		if (predicate.child) {
			CollectSljitNativePredicateSourceIndices(*predicate.child, source_indices);
		}
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child) {
				CollectSljitNativePredicateSourceIndices(*child, source_indices);
			}
		}
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto source_index : predicate.guard_source_indices) {
			AppendSljitNativePredicateSourceIndex(source_indices, source_index);
		}
		if (predicate.child) {
			CollectSljitNativePredicateSourceIndices(*predicate.child, source_indices);
		}
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

void FinalizeSljitNativePredicateSourceIndices(SljitNativePredicate &predicate) {
	vector<idx_t> source_indices;
	CollectSljitNativePredicateSourceIndices(predicate, source_indices);
	std::sort(source_indices.begin(), source_indices.end());
	source_indices.erase(std::unique(source_indices.begin(), source_indices.end()), source_indices.end());
	predicate.source_indices = std::move(source_indices);
}

static bool TryBuildNativePredicateInternal(const ExecutionExpressionIR &root,
                                            unique_ptr<SljitNativePredicate> &predicate) {
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}
	auto result = make_uniq<SljitNativePredicate>();
	result->return_type = root.return_type;

	if (root.kind == ExecutionExpressionIRKind::CONSTANT) {
		if (root.constant.IsNull()) {
			result->kind = SljitNativePredicateKind::CONSTANT;
			result->constant_is_null = true;
			predicate = std::move(result);
			return true;
		}
		if (root.constant.type().id() != LogicalTypeId::BOOLEAN) {
			return false;
		}
		result->kind = SljitNativePredicateKind::CONSTANT;
		result->constant_value = BooleanValue::Get(root.constant);
		predicate = std::move(result);
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::REFERENCE) {
		result->kind = SljitNativePredicateKind::REFERENCE;
		result->source_index = root.ref_index;
		predicate = std::move(result);
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT &&
	    root.left) {
		unique_ptr<SljitNativePredicate> child;
		if (!TryBuildNativePredicate(*root.left, child)) {
			return false;
		}
		result->kind = SljitNativePredicateKind::NOT;
		result->child = std::move(child);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeIntegerCompareNullConstant(root)) {
		result->kind = SljitNativePredicateKind::CONSTANT;
		result->constant_is_null = true;
		predicate = std::move(result);
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		if (root.children.empty()) {
			return false;
		}
		unique_ptr<SljitNativePredicate> child;
		if (!TryBuildNativePredicate(*root.children[0], child)) {
			return false;
		}
		result->kind = SljitNativePredicateKind::CONSTANT_OR_NULL;
		result->child = std::move(child);
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			if (!TryReadNativePredicateNullGuard(*root.children[child_idx], result->guard_source_indices,
			                                     result->guard_has_null_constant)) {
				return false;
			}
		}
		predicate = std::move(result);
		return true;
	}

	idx_t substring_in_list_source_index;
	idx_t substring_in_list_length;
	vector<string> substring_in_list_constants;
	if (TryReadNativeStringSubstringInListConstant(root, substring_in_list_source_index, substring_in_list_length,
	                                               substring_in_list_constants)) {
		result->kind = SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT;
		result->source_index = substring_in_list_source_index;
		result->substring_length = substring_in_list_length;
		result->string_constants = std::move(substring_in_list_constants);
		predicate = std::move(result);
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		if (root.children.empty()) {
			return false;
		}
		result->kind = SljitNativePredicateKind::CONJUNCTION;
		result->conjunction_op = root.conjunction_op;
		result->children.reserve(root.children.size());
		for (auto &child_node : root.children) {
			unique_ptr<SljitNativePredicate> child;
			if (!TryBuildNativePredicate(*child_node, child)) {
				return false;
			}
			result->children.push_back(std::move(child));
		}
		predicate = std::move(result);
		return true;
	}

	SljitNativeNullCheckOp null_check_op;
	idx_t source_index;
	if (TryReadNativeNullCheck(root, null_check_op, source_index)) {
		result->kind = SljitNativePredicateKind::NULL_CHECK;
		result->source_index = source_index;
		result->null_check_op = null_check_op;
		predicate = std::move(result);
		return true;
	}

	SljitNativeIntegerKind kind;
	SljitNativeIntegerCompareOp compare_op;
	idx_t right_source_index;
	SljitNativeDoubleSourceKind double_source_kind;
	SljitNativeDoubleSourceKind double_right_source_kind;
	double double_source_scale;
	double double_right_source_scale;
	double double_constant;
	uint64_t int128_lower;
	int64_t int128_upper;
	if (TryReadNativeIntegerCompareReferences(root, compare_op, kind, source_index, right_source_index)) {
		result->kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		result->integer_kind = kind;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeInt128CompareReferences(root, compare_op, source_index, right_source_index)) {
		result->kind = SljitNativePredicateKind::INT128_COMPARE_REFERENCES;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeDoubleCompareReferences(root, compare_op, double_source_kind, source_index, double_source_scale,
	                                         double_right_source_kind, right_source_index, double_right_source_scale)) {
		result->kind = SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		result->double_source_kind = double_source_kind;
		result->double_right_source_kind = double_right_source_kind;
		result->double_source_scale = double_source_scale;
		result->double_right_source_scale = double_right_source_scale;
		predicate = std::move(result);
		return true;
	}

	int64_t constant;
	bool constant_on_left;
	if (TryReadNativeIntegerCompareConstant(root, compare_op, kind, source_index, constant, constant_on_left)) {
		result->kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		result->integer_kind = kind;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->constant = constant;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeDoubleCompareConstant(root, compare_op, double_source_kind, source_index, double_source_scale,
	                                       double_constant, constant_on_left)) {
		result->kind = SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->double_source_kind = double_source_kind;
		result->double_source_scale = double_source_scale;
		result->double_constant = double_constant;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeInt128CompareConstant(root, compare_op, source_index, int128_lower, int128_upper,
	                                       constant_on_left)) {
		result->kind = SljitNativePredicateKind::INT128_COMPARE_CONSTANT;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->int128_constant_lower = int128_lower;
		result->int128_constant_upper = int128_upper;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}

	vector<int64_t> constants;
	bool list_has_null;
	bool not_in;
	if (TryReadNativeIntegerInList(root, kind, source_index, constants, list_has_null, not_in)) {
		result->kind = SljitNativePredicateKind::INTEGER_IN_LIST;
		result->integer_kind = kind;
		result->source_index = source_index;
		result->constants = std::move(constants);
		result->list_has_null = list_has_null;
		result->not_in = not_in;
		predicate = std::move(result);
		return true;
	}

	idx_t substring_length;
	vector<string> string_constants;
	string string_constant;
	if (TryReadNativeStringEqualConstant(root, source_index, string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_EQUAL_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeStringInListConstant(root, source_index, string_constants, list_has_null, not_in)) {
		result->kind = SljitNativePredicateKind::STRING_IN_LIST_CONSTANT;
		result->source_index = source_index;
		result->string_constants = std::move(string_constants);
		result->list_has_null = list_has_null;
		result->not_in = not_in;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeStringSubstringInListConstant(root, source_index, substring_length, string_constants)) {
		result->kind = SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT;
		result->source_index = source_index;
		result->substring_length = substring_length;
		result->string_constants = std::move(string_constants);
		predicate = std::move(result);
		return true;
	}

	int64_t lower;
	int64_t upper;
	bool lower_inclusive;
	bool upper_inclusive;
	bool not_between;
	if (TryReadNativeIntegerBetween(root, kind, source_index, lower, upper, lower_inclusive, upper_inclusive,
	                                not_between)) {
		result->kind = SljitNativePredicateKind::INTEGER_BETWEEN;
		result->integer_kind = kind;
		result->source_index = source_index;
		result->lower = lower;
		result->upper = upper;
		result->lower_inclusive = lower_inclusive;
		result->upper_inclusive = upper_inclusive;
		result->not_between = not_between;
		predicate = std::move(result);
		return true;
	}

	string prefix;
	if (TryReadNativeStringPrefixConstant(root, source_index, prefix)) {
		result->kind = SljitNativePredicateKind::STRING_PREFIX_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(prefix);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_SUFFIX, source_index,
	                                     string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_SUFFIX_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_CONTAINS, source_index,
	                                     string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_CONTAINS_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	vector<string> like_fragments;
	bool anchor_start;
	bool anchor_end;
	if (TryReadNativeStringLikeConstant(root, source_index, like_fragments, anchor_start, anchor_end)) {
		result->kind = SljitNativePredicateKind::STRING_LIKE_CONSTANT;
		result->source_index = source_index;
		result->string_constants = std::move(like_fragments);
		result->string_anchor_start = anchor_start;
		result->string_anchor_end = anchor_end;
		predicate = std::move(result);
		return true;
	}

	return false;
}

bool TryBuildNativePredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate) {
	if (!TryBuildNativePredicateInternal(root, predicate)) {
		return false;
	}
	FinalizeSljitNativePredicateSourceIndices(*predicate);
	return true;
}

} // namespace duckdb

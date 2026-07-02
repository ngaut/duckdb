//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_integer_plan_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_integer_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

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
		kind = SljitNativeIntegerKind::DATE;
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
	case SljitNativeIntegerKind::DATE:
		return "date";
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
	case SljitNativeIntegerKind::DATE:
		if (value.type().id() != LogicalTypeId::DATE || value.type().InternalType() != PhysicalType::INT32) {
			return false;
		}
		constant_value = value.GetValueUnsafe<date_t>().days;
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

} // namespace duckdb

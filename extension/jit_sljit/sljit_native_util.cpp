//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_util.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

string MaybeDumpIr(ClientContext &context, string ir) {
	if (!ExecutionRegionSettings::DumpIR(context)) {
		return string();
	}
	return ir;
}

string NativeSignedIntegerTypeName(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return "int8";
	case SljitNativeSignedIntegerWidth::INT16:
		return "int16";
	case SljitNativeSignedIntegerWidth::INT32:
		return "int32";
	case SljitNativeSignedIntegerWidth::INT64:
		return "int64";
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

string NativeUnsignedIntegerTypeName(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return "utinyint";
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return "usmallint";
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return "uinteger";
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

string NativeIntegerBinaryOverflowMessage(SljitNativeIntegerBinaryOp op) {
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "Overflow in addition";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "Overflow in subtraction";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "Overflow in multiplication";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

string NativeIntegerCastOverflowMessage(SljitNativeSignedIntegerWidth source_width,
                                        SljitNativeSignedIntegerWidth target_width) {
	return "Type " + StringUtil::Upper(NativeSignedIntegerTypeName(source_width)) +
	       " with value %lld can't be cast because the value is out of range for the destination type " +
	       StringUtil::Upper(NativeSignedIntegerTypeName(target_width));
}

string NativeSignedToUnsignedIntegerCastOverflowMessage(SljitNativeSignedIntegerWidth source_width,
                                                        SljitNativeUnsignedIntegerWidth target_width) {
	return "Type " + StringUtil::Upper(NativeSignedIntegerTypeName(source_width)) +
	       " with value %lld can't be cast because the value is out of range for the destination type " +
	       StringUtil::Upper(NativeUnsignedIntegerTypeName(target_width));
}

bool SljitFilteredAggregateUsesPayloadExpression(AggregatePrimitiveUpdateKind kind) {
	return kind == AggregatePrimitiveUpdateKind::SUM_INT64 || kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
}

bool SljitFilteredAggregateKindCanGenerate(AggregatePrimitiveUpdateKind kind) {
	return SljitFilteredAggregateUsesPayloadExpression(kind) || kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
}

} // namespace duckdb

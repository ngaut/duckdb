//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_util.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

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

void SljitBoundIntegerCastChain::ThrowCastError(int64_t source_value) const {
	if (!chain || !runtime) {
		throw InternalException("SLJIT cast error has no execution binding");
	}
	chain->ThrowCastError(source_value, SljitCastErrorBindings(*runtime.get_mutable()));
}

void SljitIntegerCastChain::ThrowCastError(int64_t source_value, const vector<SljitCastErrorIdentity> &errors) const {
	auto value = Value::BIGINT(source_value).DefaultCastAs(source_type);
	for (auto &step : steps) {
		string error;
		auto result = value.DefaultTryCastAs(step.target_type, &error);
		if (!result) {
			if (step.error_binding >= errors.size()) {
				throw InternalException("SLJIT cast error identity is not bound");
			}
			auto &identity = errors[step.error_binding];
			// the message DuckDB's cast raises for its expression (HandleCastError::AssignError)
			if (!identity.source_alias.empty()) {
				error += " when casting from source column " + identity.source_alias;
			}
			throw ConversionException(identity.query_location, error);
		}
		value = std::move(*result);
	}
	throw InternalException("SLJIT integer cast check failed on %lld, which DuckDB's casts accept", source_value);
}

bool SljitFilteredAggregateUsesPayloadExpression(AggregatePrimitiveUpdateKind kind) {
	return kind == AggregatePrimitiveUpdateKind::SUM_INT64 || kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT ||
	       kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE || kind == AggregatePrimitiveUpdateKind::COUNT;
}

bool SljitFilteredAggregateKindCanGenerate(AggregatePrimitiveUpdateKind kind) {
	return SljitFilteredAggregateUsesPayloadExpression(kind) || kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
}

} // namespace duckdb

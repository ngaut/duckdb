//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_value_abi.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

namespace duckdb {

enum class SljitAggregatePayloadValueABI : uint8_t {
	INVALID,
	VALIDITY_ONLY,
	SIGNED_MACHINE_WORD,
	SIGNED_DOUBLE_WORD,
	DOUBLE
};

static inline SljitAggregatePayloadValueABI SljitGetAggregatePayloadValueABI(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return SljitAggregatePayloadValueABI::SIGNED_MACHINE_WORD;
	case PhysicalType::INT128:
		return SljitAggregatePayloadValueABI::SIGNED_DOUBLE_WORD;
	case PhysicalType::DOUBLE:
		return SljitAggregatePayloadValueABI::DOUBLE;
	default:
		return SljitAggregatePayloadValueABI::INVALID;
	}
}

static inline bool SljitAggregatePayloadUsesRawVectorData(SljitAggregatePayloadValueABI abi) {
	return abi == SljitAggregatePayloadValueABI::SIGNED_DOUBLE_WORD || abi == SljitAggregatePayloadValueABI::DOUBLE;
}

} // namespace duckdb

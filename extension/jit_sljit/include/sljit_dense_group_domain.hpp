//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_dense_group_domain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

namespace duckdb {

static bool SljitTryReadDenseDomainKey(const Value &value, idx_t &key) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		key = value.GetValueUnsafe<bool>() ? 1 : 0;
		return true;
	case PhysicalType::UINT8:
		key = value.GetValueUnsafe<uint8_t>();
		return true;
	case PhysicalType::INT8: {
		const auto result = value.GetValueUnsafe<int8_t>();
		if (result < 0) {
			return false;
		}
		key = UnsafeNumericCast<idx_t>(result);
		return true;
	}
	case PhysicalType::UINT16:
		key = value.GetValueUnsafe<uint16_t>();
		return true;
	case PhysicalType::INT16: {
		const auto result = value.GetValueUnsafe<int16_t>();
		if (result < 0) {
			return false;
		}
		key = UnsafeNumericCast<idx_t>(result);
		return true;
	}
	case PhysicalType::UINT32:
		key = value.GetValueUnsafe<uint32_t>();
		return true;
	case PhysicalType::INT32: {
		const auto result = value.GetValueUnsafe<int32_t>();
		if (result < 0) {
			return false;
		}
		key = UnsafeNumericCast<idx_t>(result);
		return true;
	}
	case PhysicalType::UINT64: {
		const auto result = value.GetValueUnsafe<uint64_t>();
		if (result > NumericLimits<idx_t>::Maximum()) {
			return false;
		}
		key = UnsafeNumericCast<idx_t>(result);
		return true;
	}
	case PhysicalType::INT64: {
		const auto result = value.GetValueUnsafe<int64_t>();
		if (result < 0 || uint64_t(result) > NumericLimits<idx_t>::Maximum()) {
			return false;
		}
		key = UnsafeNumericCast<idx_t>(result);
		return true;
	}
	default:
		return false;
	}
}

static bool SljitTryBuildDenseGroupDomainFromStats(PhysicalType physical_type, idx_t distinct_count,
                                                   const Value &min_value, const Value &max_value,
                                                   ExecutionDenseGroupDomain &domain) {
	domain = ExecutionDenseGroupDomain();
	if (distinct_count == 0) {
		return false;
	}
	idx_t min_key;
	idx_t max_key;
	if (!SljitTryReadDenseDomainKey(min_value, min_key) || !SljitTryReadDenseDomainKey(max_value, max_key) ||
	    min_key > max_key) {
		return false;
	}
	domain.ready = true;
	domain.physical_type = physical_type;
	domain.min_key = min_key;
	domain.max_key = max_key;
	domain.distinct_count = distinct_count;
	return true;
}

} // namespace duckdb

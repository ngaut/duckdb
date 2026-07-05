//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_date_year_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

static bool SljitDateDaysAreFinite(int32_t days) {
	return days != date_t::infinity().days && days != date_t::ninfinity().days;
}

static int64_t SljitExtractDateYearFromDays(int32_t days) {
	int32_t year = Date::EPOCH_YEAR;
	while (days < 0) {
		days += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (days >= Date::DAYS_PER_YEAR_INTERVAL) {
		days -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	auto year_offset = days / 365;
	while (days < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	return year + year_offset;
}

template <class DST>
static bool SljitTryDateYearCompressedGroupKey(int32_t days, int64_t minimum, DST &result) {
	int64_t compressed_value;
	if (!TrySubtractOperator::Operation<int64_t, int64_t, int64_t>(SljitExtractDateYearFromDays(days), minimum,
	                                                               compressed_value)) {
		return false;
	}
	return TryCast::Operation<int64_t, DST>(compressed_value, result, false);
}

template <class DST>
static DST SljitDateYearCompressedGroupKeyOrThrow(int32_t days, int64_t minimum) {
	DST result;
	if (!SljitTryDateYearCompressedGroupKey<DST>(days, minimum, result)) {
		int64_t compressed_value;
		if (!TrySubtractOperator::Operation<int64_t, int64_t, int64_t>(SljitExtractDateYearFromDays(days), minimum,
		                                                               compressed_value)) {
			throw InvalidInputException("Overflow in date year group compression");
		}
		throw InvalidInputException(CastExceptionText<int64_t, DST>(compressed_value));
	}
	return result;
}

} // namespace duckdb

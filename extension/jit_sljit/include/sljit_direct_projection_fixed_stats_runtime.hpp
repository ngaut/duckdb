//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_fixed_stats_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_fixed_source_runtime.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

namespace duckdb {

static bool FixedDirectProjectionSignedStatsType(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

template <class T>
static void ScanFixedDirectProjectionSignedStats(const UnifiedVectorFormat &source_format, idx_t source_offset,
                                                 idx_t count, int64_t &min_value, int64_t &max_value) {
	D_ASSERT(count > 0);
	auto values = reinterpret_cast<const T *>(source_format.data) + source_offset;
	auto local_min = values[0];
	auto local_max = values[0];
	for (idx_t row_idx = 1; row_idx < count; row_idx++) {
		auto value = values[row_idx];
		local_min = MinValue<T>(local_min, value);
		local_max = MaxValue<T>(local_max, value);
	}
	min_value = int64_t(local_min);
	max_value = int64_t(local_max);
}

static bool TryScanFixedDirectProjectionSourceStats(DataChunk &input, idx_t source_index, idx_t source_offset,
                                                    idx_t count, vector<DirectAppendColumnStats> &source_stats,
                                                    optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache) {
	if (source_index >= input.ColumnCount() || source_index >= source_stats.size() || count == 0) {
		return false;
	}
	auto &cached_stats = source_stats[source_index];
	if (cached_stats.has_stats) {
		return true;
	}
	auto physical_type = input.data[source_index].GetType().InternalType();
	if (!FixedDirectProjectionSignedStatsType(physical_type)) {
		return false;
	}
	UnifiedVectorFormat local_source_format;
	UnifiedVectorFormat *source_format;
	if (!PrepareFixedDirectProjectionSource(input, source_index, source_offset, count, source_cache,
	                                        local_source_format, source_format)) {
		return false;
	}
	cached_stats.has_stats = true;
	cached_stats.physical_type = physical_type;
	switch (physical_type) {
	case PhysicalType::INT8:
		ScanFixedDirectProjectionSignedStats<int8_t>(*source_format, source_offset, count, cached_stats.signed_min,
		                                             cached_stats.signed_max);
		return true;
	case PhysicalType::INT16:
		ScanFixedDirectProjectionSignedStats<int16_t>(*source_format, source_offset, count, cached_stats.signed_min,
		                                              cached_stats.signed_max);
		return true;
	case PhysicalType::INT32:
		ScanFixedDirectProjectionSignedStats<int32_t>(*source_format, source_offset, count, cached_stats.signed_min,
		                                              cached_stats.signed_max);
		return true;
	case PhysicalType::INT64:
		ScanFixedDirectProjectionSignedStats<int64_t>(*source_format, source_offset, count, cached_stats.signed_min,
		                                              cached_stats.signed_max);
		return true;
	default:
		cached_stats = DirectAppendColumnStats();
		return false;
	}
}

static bool TryAddFixedStatsBound(int64_t left, int64_t right, int64_t &result) {
	if ((right > 0 && left > NumericLimits<int64_t>::Maximum() - right) ||
	    (right < 0 && left < NumericLimits<int64_t>::Minimum() - right)) {
		return false;
	}
	result = left + right;
	return true;
}

static bool TrySubtractFixedStatsBound(int64_t left, int64_t right, int64_t &result) {
	if ((right < 0 && left > NumericLimits<int64_t>::Maximum() + right) ||
	    (right > 0 && left < NumericLimits<int64_t>::Minimum() + right)) {
		return false;
	}
	result = left - right;
	return true;
}

static bool SetFixedSignedDirectProjectionStats(DirectAppendColumnStats &stats, PhysicalType physical_type,
                                                int64_t min_value, int64_t max_value) {
	if (!FixedDirectProjectionSignedStatsType(physical_type)) {
		return false;
	}
	stats = DirectAppendColumnStats();
	stats.has_stats = true;
	stats.physical_type = physical_type;
	stats.signed_min = min_value;
	stats.signed_max = max_value;
	return true;
}

static void TrySetDirectAppendDistinctCount(DirectAppendColumnStats &stats, idx_t source_index,
                                            const vector<idx_t> &source_distinct_counts) {
	if (source_index >= source_distinct_counts.size() || source_distinct_counts[source_index] == 0) {
		return;
	}
	stats.has_distinct_count = true;
	stats.distinct_count = source_distinct_counts[source_index];
}

static bool TryDeriveFixedBinaryConstantStats(const SljitNativeRegionExpressionPlan &plan,
                                              const DirectAppendColumnStats &source_stats,
                                              DirectAppendColumnStats &result_stats) {
	int64_t min_value;
	int64_t max_value;
	switch (plan.binary_op) {
	case SljitNativeIntegerBinaryOp::ADD:
		if (!TryAddFixedStatsBound(source_stats.signed_min, plan.constant, min_value) ||
		    !TryAddFixedStatsBound(source_stats.signed_max, plan.constant, max_value)) {
			return false;
		}
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (plan.constant_on_left) {
			if (!TrySubtractFixedStatsBound(plan.constant, source_stats.signed_max, min_value) ||
			    !TrySubtractFixedStatsBound(plan.constant, source_stats.signed_min, max_value)) {
				return false;
			}
		} else {
			if (!TrySubtractFixedStatsBound(source_stats.signed_min, plan.constant, min_value) ||
			    !TrySubtractFixedStatsBound(source_stats.signed_max, plan.constant, max_value)) {
				return false;
			}
		}
		break;
	default:
		return false;
	}
	return SetFixedSignedDirectProjectionStats(result_stats, plan.return_type.InternalType(), min_value, max_value);
}

static bool TryDeriveFixedBinaryReferenceStats(const SljitNativeRegionExpressionPlan &plan,
                                               const DirectAppendColumnStats &left_stats,
                                               const DirectAppendColumnStats &right_stats,
                                               DirectAppendColumnStats &result_stats) {
	int64_t min_value;
	int64_t max_value;
	switch (plan.binary_op) {
	case SljitNativeIntegerBinaryOp::ADD:
		if (!TryAddFixedStatsBound(left_stats.signed_min, right_stats.signed_min, min_value) ||
		    !TryAddFixedStatsBound(left_stats.signed_max, right_stats.signed_max, max_value)) {
			return false;
		}
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (!TrySubtractFixedStatsBound(left_stats.signed_min, right_stats.signed_max, min_value) ||
		    !TrySubtractFixedStatsBound(left_stats.signed_max, right_stats.signed_min, max_value)) {
			return false;
		}
		break;
	default:
		return false;
	}
	return SetFixedSignedDirectProjectionStats(result_stats, plan.return_type.InternalType(), min_value, max_value);
}

static bool TryComputeFixedDirectProjectionStats(const SljitExecutableRegionExpression &expr, DataChunk &input,
                                                 idx_t source_offset, idx_t count,
                                                 vector<DirectAppendColumnStats> &source_stats,
                                                 DirectAppendColumnStats &result_stats,
                                                 optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
                                                 const vector<idx_t> &source_distinct_counts) {
	auto &plan = expr.plan;
	if (!FixedDirectProjectionSignedStatsType(plan.return_type.InternalType())) {
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (!TryScanFixedDirectProjectionSourceStats(input, plan.source_index, source_offset, count, source_stats,
		                                             source_cache)) {
			return false;
		}
		if (source_stats[plan.source_index].physical_type != plan.return_type.InternalType()) {
			return false;
		}
		result_stats = source_stats[plan.source_index];
		TrySetDirectAppendDistinctCount(result_stats, plan.source_index, source_distinct_counts);
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		if (plan.check_arithmetic_overflow || plan.check_result_range ||
		    !TryScanFixedDirectProjectionSourceStats(input, plan.source_index, source_offset, count, source_stats,
		                                             source_cache)) {
			return false;
		}
		if (!TryDeriveFixedBinaryConstantStats(plan, source_stats[plan.source_index], result_stats)) {
			return false;
		}
		TrySetDirectAppendDistinctCount(result_stats, plan.source_index, source_distinct_counts);
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		if (plan.check_arithmetic_overflow || plan.check_result_range ||
		    !TryScanFixedDirectProjectionSourceStats(input, plan.source_index, source_offset, count, source_stats,
		                                             source_cache) ||
		    !TryScanFixedDirectProjectionSourceStats(input, plan.right_source_index, source_offset, count, source_stats,
		                                             source_cache)) {
			return false;
		}
		return TryDeriveFixedBinaryReferenceStats(plan, source_stats[plan.source_index],
		                                          source_stats[plan.right_source_index], result_stats);
	default:
		return false;
	}
}

} // namespace duckdb

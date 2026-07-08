//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_preaggregation_common_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation.hpp"

namespace duckdb {

static bool
SljitPreaggregatedPayloadSourcesReplayable(SljitPreaggregatedPrimitivePayloadSources &payload_sources,
                                           const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		if (!lane) {
			return false;
		}
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			if (payload_sources.SourceCanHaveNull(payload_idx)) {
				return false;
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool SljitPreaggregatedInputVectorGroupKeyReplayable(SljitPreaggregatedInputVectorGroupKeySource &group_source) {
	auto &source = *group_source.source;
	if (!source.all_valid && group_source.format.validity.CanHaveNull()) {
		return false;
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source.source_physical_type == source.target_physical_type;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source.unchecked_integral_cast;
	default:
		return false;
	}
}

template <class PAYLOAD_DISPATCH>
static bool SljitDispatchPreaggregatedInt64PayloadType(PhysicalType payload_type, PAYLOAD_DISPATCH &dispatch) {
	switch (payload_type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	case PhysicalType::UINT8:
		return dispatch.template Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.template Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.template Execute<uint32_t>();
	default:
		return false;
	}
}

template <class PAYLOAD_DISPATCH>
static bool SljitDispatchPreaggregatedHugeintPayloadType(PhysicalType payload_type, PAYLOAD_DISPATCH &dispatch) {
	switch (payload_type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	case PhysicalType::INT128:
		return dispatch.template Execute<hugeint_t>();
	case PhysicalType::UINT8:
		return dispatch.template Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.template Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.template Execute<uint32_t>();
	default:
		return false;
	}
}

template <class TARGET_TYPE, class LOAD_KEY>
static bool SljitTryInputVectorHasConsecutiveRepeat(idx_t count, LOAD_KEY &&load_key, bool &has_consecutive_repeat) {
	has_consecutive_repeat = false;
	if (count < 2) {
		return true;
	}
	TARGET_TYPE previous_key;
	if (!load_key(0, previous_key)) {
		return false;
	}
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		TARGET_TYPE key;
		if (!load_key(row_idx, key)) {
			return false;
		}
		if (key == previous_key) {
			has_consecutive_repeat = true;
			return true;
		}
		previous_key = key;
	}
	return true;
}

template <class LOAD_KEY>
static bool SljitInputVectorHasConsecutiveRepeat(idx_t count, LOAD_KEY &&load_key) {
	if (count < 2) {
		return false;
	}
	auto previous_key = load_key(0);
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		auto key = load_key(row_idx);
		if (key == previous_key) {
			return true;
		}
		previous_key = key;
	}
	return false;
}

template <class PAYLOAD_TYPE>
static int64_t SljitPreaggregatedPayloadAsInt64(PAYLOAD_TYPE value) {
	return NumericCast<int64_t>(value);
}

static hugeint_t SljitPreaggregatedPayloadAsHugeint(hugeint_t value) {
	return value;
}

template <class PAYLOAD_TYPE>
static hugeint_t SljitPreaggregatedPayloadAsHugeint(PAYLOAD_TYPE value) {
	return hugeint_t(NumericCast<int64_t>(value));
}

} // namespace duckdb

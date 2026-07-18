//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_preaggregated_primitive_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitPreaggregatedPrimitivePayloadDeltas {
	AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
	vector<int64_t> int64_values;
	vector<hugeint_t> hugeint_values;
	vector<uint8_t> value_is_set;
};

// One authority for how each primitive kind stores its per-group deltas: counts
// accumulate in int64_values, int64 sums add value_is_set, hugeint sums use
// hugeint_values with value_is_set. Shape-only sizing, reset, slice, copy, and
// append checks derive from these facts; typed accumulate and state-extract
// logic keeps explicit kind switches.
struct SljitPreaggregatedPayloadShape {
	bool supported = false;
	bool uses_int64_values = false;
	bool uses_hugeint_values = false;
	bool uses_value_is_set = false;
};

static inline SljitPreaggregatedPayloadShape SljitPreaggregatedPayloadShapeForKind(AggregatePrimitiveUpdateKind kind) {
	SljitPreaggregatedPayloadShape shape;
	switch (kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
		shape.supported = true;
		shape.uses_int64_values = true;
		break;
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		shape.supported = true;
		shape.uses_int64_values = true;
		shape.uses_value_is_set = true;
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		shape.supported = true;
		shape.uses_hugeint_values = true;
		shape.uses_value_is_set = true;
		break;
	default:
		break;
	}
	return shape;
}

enum class SljitPreaggregatedPrimitivePayloadLayout : uint8_t { PER_LANE, SHARED_AFFINE };

struct SljitPreaggregatedPrimitiveAggregateScratch {
	vector<SljitPreaggregatedPrimitivePayloadDeltas> payloads;
	vector<data_t> fused_state_storage;
	vector<uintptr_t> fused_row_state_addresses;
	vector<UnifiedVectorFormat> input_group_formats;
	vector<sel_t> group_rows;
	vector<idx_t> group_row_counts;
	// Shared affine values stay in the machine-word representation produced by
	// generated kernels. Only values that outgrow that representation are
	// promoted into the parallel hugeint storage.
	vector<int64_t> shared_int64_values;
	vector<hugeint_t> shared_hugeint_values;
	vector<uint8_t> shared_value_is_wide;
	vector<idx_t> shared_valid_counts;
	bool shared_valid_counts_are_row_counts = false;
	idx_t fused_state_stride = 0;
	SljitPreaggregatedPrimitivePayloadLayout payload_layout = SljitPreaggregatedPrimitivePayloadLayout::PER_LANE;

	void PrepareCommon(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		payloads.resize(lanes.size());
		fused_state_storage.clear();
		fused_row_state_addresses.clear();
		input_group_formats.clear();
		group_rows.clear();
		group_rows.reserve(capacity);
		group_row_counts.clear();
		group_row_counts.reserve(capacity);
		shared_int64_values.clear();
		shared_int64_values.reserve(capacity);
		shared_hugeint_values.clear();
		shared_hugeint_values.reserve(capacity);
		shared_value_is_wide.clear();
		shared_value_is_wide.reserve(capacity);
		shared_valid_counts.clear();
		shared_valid_counts.reserve(capacity);
		shared_valid_counts_are_row_counts = false;
		fused_state_stride = 0;
		payload_layout = SljitPreaggregatedPrimitivePayloadLayout::PER_LANE;
		for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			payloads[payload_idx].kind = lane ? lane->kind : AggregatePrimitiveUpdateKind::NONE;
		}
	}

	void Prepare(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		PrepareCommon(lanes, capacity);
		for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
			auto &payload = payloads[payload_idx];
			payload.int64_values.clear();
			payload.hugeint_values.clear();
			payload.value_is_set.clear();
			auto shape = SljitPreaggregatedPayloadShapeForKind(payload.kind);
			if (shape.uses_int64_values) {
				payload.int64_values.reserve(capacity);
			}
			if (shape.uses_hugeint_values) {
				payload.hugeint_values.reserve(capacity);
			}
			if (shape.uses_value_is_set) {
				payload.value_is_set.reserve(capacity);
			}
		}
	}

	void PrepareSharedAffine(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		PrepareCommon(lanes, capacity);
		for (auto &payload : payloads) {
			payload.int64_values = vector<int64_t>();
			payload.hugeint_values = vector<hugeint_t>();
			payload.value_is_set = vector<uint8_t>();
		}
		payload_layout = SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE;
	}

	bool PrepareFixed(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		Prepare(lanes, capacity);
		group_row_counts.resize(capacity);
		for (auto &payload : payloads) {
			auto shape = SljitPreaggregatedPayloadShapeForKind(payload.kind);
			if (!shape.supported) {
				return false;
			}
			if (shape.uses_int64_values) {
				payload.int64_values.resize(capacity);
			}
			if (shape.uses_hugeint_values) {
				payload.hugeint_values.resize(capacity);
			}
			if (shape.uses_value_is_set) {
				payload.value_is_set.resize(capacity);
			}
		}
		return true;
	}

	bool PrepareFixedSharedAffine(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		PrepareSharedAffine(lanes, capacity);
		group_row_counts.resize(capacity);
		shared_int64_values.resize(capacity);
		shared_hugeint_values.resize(capacity);
		shared_value_is_wide.resize(capacity);
		shared_valid_counts.resize(capacity);
		return true;
	}

	bool HasFixedCapacity(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) const {
		if (payloads.size() != lanes.size() || group_row_counts.size() != capacity) {
			return false;
		}
		for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			auto &payload = payloads[payload_idx];
			if (!lane || payload.kind != lane->kind) {
				return false;
			}
			auto shape = SljitPreaggregatedPayloadShapeForKind(payload.kind);
			if (!shape.supported) {
				return false;
			}
			if (shape.uses_int64_values && payload.int64_values.size() != capacity) {
				return false;
			}
			if (shape.uses_hugeint_values && payload.hugeint_values.size() != capacity) {
				return false;
			}
			if (shape.uses_value_is_set && payload.value_is_set.size() != capacity) {
				return false;
			}
		}
		return true;
	}

	bool HasFixedSharedAffineCapacity(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                  idx_t capacity) const {
		if (payload_layout != SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE ||
		    payloads.size() != lanes.size() || group_row_counts.size() != capacity ||
		    shared_int64_values.size() != capacity || shared_hugeint_values.size() != capacity ||
		    shared_value_is_wide.size() != capacity || shared_valid_counts.size() != capacity) {
			return false;
		}
		for (idx_t lane_idx = 0; lane_idx < lanes.size(); lane_idx++) {
			if (!lanes[lane_idx] || payloads[lane_idx].kind != lanes[lane_idx]->kind) {
				return false;
			}
		}
		return true;
	}
};

static bool SljitTryLoadSharedAffineInt64(const hugeint_t &value, int64_t &result) {
	if (value.upper == 0 && value.lower <= uint64_t(NumericLimits<int64_t>::Maximum())) {
		result = UnsafeNumericCast<int64_t>(value.lower);
		return true;
	}
	if (value.upper == -1 && value.lower >= uint64_t(NumericLimits<int64_t>::Minimum())) {
		result = -static_cast<int64_t>(NumericLimits<uint64_t>::Maximum() - value.lower) - 1;
		return true;
	}
	return false;
}

static bool SljitTryAddSharedAffineInt64(int64_t left, int64_t right, int64_t &result) {
#if defined(__GNUC__) || defined(__clang__)
	return !__builtin_add_overflow(left, right, &result);
#else
	return TryAddOperator::Operation(left, right, result);
#endif
}

static hugeint_t SljitLoadSharedAffineValue(const SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                            idx_t group_idx) {
	D_ASSERT(group_idx < scratch.shared_int64_values.size());
	D_ASSERT(group_idx < scratch.shared_hugeint_values.size());
	D_ASSERT(group_idx < scratch.shared_value_is_wide.size());
	return scratch.shared_value_is_wide[group_idx] ? scratch.shared_hugeint_values[group_idx]
	                                               : hugeint_t(scratch.shared_int64_values[group_idx]);
}

static bool SljitStoreSharedAffineValue(SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx,
                                        const hugeint_t &value) {
	if (group_idx >= scratch.shared_int64_values.size() || group_idx >= scratch.shared_hugeint_values.size() ||
	    group_idx >= scratch.shared_value_is_wide.size()) {
		return false;
	}
	int64_t int64_value;
	if (SljitTryLoadSharedAffineInt64(value, int64_value)) {
		scratch.shared_int64_values[group_idx] = int64_value;
		scratch.shared_value_is_wide[group_idx] = 0;
	} else {
		scratch.shared_hugeint_values[group_idx] = value;
		scratch.shared_value_is_wide[group_idx] = 1;
	}
	return true;
}

static bool SljitAddSharedAffineInt64(SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx,
                                      int64_t delta) {
	if (group_idx >= scratch.shared_int64_values.size() || group_idx >= scratch.shared_hugeint_values.size() ||
	    group_idx >= scratch.shared_value_is_wide.size()) {
		return false;
	}
	if (scratch.shared_value_is_wide[group_idx]) {
		scratch.shared_hugeint_values[group_idx] += hugeint_t(delta);
		return true;
	}
	int64_t result;
	if (SljitTryAddSharedAffineInt64(scratch.shared_int64_values[group_idx], delta, result)) {
		scratch.shared_int64_values[group_idx] = result;
		return true;
	}
	return SljitStoreSharedAffineValue(scratch, group_idx,
	                                   hugeint_t(scratch.shared_int64_values[group_idx]) + hugeint_t(delta));
}

static void SljitAppendSharedAffineValue(SljitPreaggregatedPrimitiveAggregateScratch &scratch, const hugeint_t &value) {
	int64_t int64_value;
	if (SljitTryLoadSharedAffineInt64(value, int64_value)) {
		scratch.shared_int64_values.push_back(int64_value);
		scratch.shared_hugeint_values.emplace_back(0);
		scratch.shared_value_is_wide.push_back(0);
	} else {
		scratch.shared_int64_values.push_back(0);
		scratch.shared_hugeint_values.push_back(value);
		scratch.shared_value_is_wide.push_back(1);
	}
}

static idx_t SljitLoadSharedAffineValidCount(const SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                             idx_t group_idx) {
	D_ASSERT(group_idx < scratch.group_row_counts.size());
	D_ASSERT(group_idx < scratch.shared_valid_counts.size());
	return scratch.shared_valid_counts_are_row_counts ? scratch.group_row_counts[group_idx]
	                                                  : scratch.shared_valid_counts[group_idx];
}

static bool SljitMaterializeSharedAffineValidCounts(SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	if (!scratch.shared_valid_counts_are_row_counts) {
		return true;
	}
	if (scratch.shared_valid_counts.size() < scratch.group_row_counts.size()) {
		return false;
	}
	std::copy(scratch.group_row_counts.begin(), scratch.group_row_counts.end(), scratch.shared_valid_counts.begin());
	scratch.shared_valid_counts_are_row_counts = false;
	return true;
}

static bool
ResetFixedPreaggregatedPrimitiveScratchGroup(SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                             const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                             idx_t group_idx) {
	if (scratch.payloads.size() != lanes.size() || group_idx >= scratch.group_row_counts.size()) {
		return false;
	}
	scratch.group_row_counts[group_idx] = 0;
	for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
		auto lane = lanes[payload_idx];
		auto &payload = scratch.payloads[payload_idx];
		if (!lane || payload.kind != lane->kind) {
			return false;
		}
		auto shape = SljitPreaggregatedPayloadShapeForKind(payload.kind);
		if (!shape.supported) {
			return false;
		}
		if (shape.uses_int64_values) {
			if (group_idx >= payload.int64_values.size()) {
				return false;
			}
			payload.int64_values[group_idx] = 0;
		}
		if (shape.uses_hugeint_values) {
			if (group_idx >= payload.hugeint_values.size()) {
				return false;
			}
			payload.hugeint_values[group_idx] = 0;
		}
		if (shape.uses_value_is_set) {
			if (group_idx >= payload.value_is_set.size()) {
				return false;
			}
			payload.value_is_set[group_idx] = 0;
		}
	}
	return true;
}

static bool CanSlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                  idx_t offset, idx_t count);

static bool CopyPreaggregatedPrimitiveScratchRangeToFixed(
    const SljitPreaggregatedPrimitiveAggregateScratch &source,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t source_offset, idx_t count,
    SljitPreaggregatedPrimitiveAggregateScratch &target, idx_t target_offset) {
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, source_offset, count) ||
	    target.payloads.size() != lanes.size() || target.group_row_counts.size() < target_offset + count) {
		return false;
	}
	const auto source_begin = UnsafeNumericCast<int64_t>(source_offset);
	const auto source_end = UnsafeNumericCast<int64_t>(source_offset + count);
	const auto target_begin = UnsafeNumericCast<int64_t>(target_offset);
	std::copy(source.group_row_counts.begin() + source_begin, source.group_row_counts.begin() + source_end,
	          target.group_row_counts.begin() + target_begin);
	if (source.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE) {
		if (target.payload_layout != SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE ||
		    target.shared_int64_values.size() < target_offset + count ||
		    target.shared_hugeint_values.size() < target_offset + count ||
		    target.shared_value_is_wide.size() < target_offset + count ||
		    target.shared_valid_counts.size() < target_offset + count) {
			return false;
		}
		std::copy(source.shared_int64_values.begin() + source_begin, source.shared_int64_values.begin() + source_end,
		          target.shared_int64_values.begin() + target_begin);
		std::copy(source.shared_hugeint_values.begin() + source_begin,
		          source.shared_hugeint_values.begin() + source_end,
		          target.shared_hugeint_values.begin() + target_begin);
		std::copy(source.shared_value_is_wide.begin() + source_begin, source.shared_value_is_wide.begin() + source_end,
		          target.shared_value_is_wide.begin() + target_begin);
		if (target_offset == 0) {
			target.shared_valid_counts_are_row_counts = source.shared_valid_counts_are_row_counts;
		} else if (target.shared_valid_counts_are_row_counts != source.shared_valid_counts_are_row_counts &&
		           !SljitMaterializeSharedAffineValidCounts(target)) {
			return false;
		}
		for (idx_t idx = 0; idx < count; idx++) {
			target.shared_valid_counts[target_offset + idx] =
			    SljitLoadSharedAffineValidCount(source, source_offset + idx);
		}
		return true;
	}
	if (target.payload_layout != SljitPreaggregatedPrimitivePayloadLayout::PER_LANE) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto &target_payload = target.payloads[payload_idx];
		if (!lanes[payload_idx] || source_payload.kind != lanes[payload_idx]->kind ||
		    target_payload.kind != source_payload.kind) {
			return false;
		}
		auto shape = SljitPreaggregatedPayloadShapeForKind(source_payload.kind);
		if (!shape.supported) {
			return false;
		}
		if (shape.uses_int64_values) {
			if (target_payload.int64_values.size() < target_offset + count) {
				return false;
			}
			std::copy(source_payload.int64_values.begin() + source_begin,
			          source_payload.int64_values.begin() + source_end,
			          target_payload.int64_values.begin() + target_begin);
		}
		if (shape.uses_hugeint_values) {
			if (target_payload.hugeint_values.size() < target_offset + count) {
				return false;
			}
			std::copy(source_payload.hugeint_values.begin() + source_begin,
			          source_payload.hugeint_values.begin() + source_end,
			          target_payload.hugeint_values.begin() + target_begin);
		}
		if (shape.uses_value_is_set) {
			if (target_payload.value_is_set.size() < target_offset + count) {
				return false;
			}
			std::copy(source_payload.value_is_set.begin() + source_begin,
			          source_payload.value_is_set.begin() + source_end,
			          target_payload.value_is_set.begin() + target_begin);
		}
	}
	return true;
}

static bool CanSlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                  idx_t offset, idx_t count) {
	if (source.payloads.size() != lanes.size() || source.group_row_counts.size() < offset + count) {
		return false;
	}
	if (!source.group_rows.empty() && source.group_rows.size() < offset + count) {
		return false;
	}
	if (source.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE) {
		if (source.shared_int64_values.size() < offset + count ||
		    source.shared_hugeint_values.size() < offset + count ||
		    source.shared_value_is_wide.size() < offset + count || source.shared_valid_counts.size() < offset + count) {
			return false;
		}
		for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			if (!lane ||
			    (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
			     lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
			    source.payloads[payload_idx].kind != lane->kind) {
				return false;
			}
		}
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto lane = lanes[payload_idx];
		if (!lane || source_payload.kind != lane->kind) {
			return false;
		}
		auto shape = SljitPreaggregatedPayloadShapeForKind(source_payload.kind);
		if (!shape.supported) {
			return false;
		}
		if (shape.uses_int64_values && source_payload.int64_values.size() < offset + count) {
			return false;
		}
		if (shape.uses_hugeint_values && source_payload.hugeint_values.size() < offset + count) {
			return false;
		}
		if (shape.uses_value_is_set && source_payload.value_is_set.size() < offset + count) {
			return false;
		}
	}
	return true;
}

static bool AppendPreaggregatedPrimitiveScratchRows(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                    idx_t offset, idx_t count,
                                                    SljitPreaggregatedPrimitiveAggregateScratch &target,
                                                    bool require_empty_target_for_group_rows) {
	const auto begin = UnsafeNumericCast<int64_t>(offset);
	const auto end = UnsafeNumericCast<int64_t>(offset + count);
	if (!source.group_rows.empty()) {
		if (require_empty_target_for_group_rows && (!target.group_rows.empty() || !target.group_row_counts.empty())) {
			return false;
		}
		target.group_rows.insert(target.group_rows.end(), source.group_rows.begin() + begin,
		                         source.group_rows.begin() + end);
	}
	target.group_row_counts.insert(target.group_row_counts.end(), source.group_row_counts.begin() + begin,
	                               source.group_row_counts.begin() + end);
	return true;
}

static bool AppendPreaggregatedPrimitivePayloadRange(const SljitPreaggregatedPrimitivePayloadDeltas &source_payload,
                                                     idx_t offset, idx_t count,
                                                     SljitPreaggregatedPrimitivePayloadDeltas &target_payload) {
	if (source_payload.kind != target_payload.kind) {
		return false;
	}
	const auto begin = UnsafeNumericCast<int64_t>(offset);
	const auto end = UnsafeNumericCast<int64_t>(offset + count);
	auto shape = SljitPreaggregatedPayloadShapeForKind(source_payload.kind);
	if (shape.supported) {
		if (shape.uses_int64_values) {
			target_payload.int64_values.insert(target_payload.int64_values.end(),
			                                   source_payload.int64_values.begin() + begin,
			                                   source_payload.int64_values.begin() + end);
		}
		if (shape.uses_hugeint_values) {
			target_payload.hugeint_values.insert(target_payload.hugeint_values.end(),
			                                     source_payload.hugeint_values.begin() + begin,
			                                     source_payload.hugeint_values.begin() + end);
		}
		if (shape.uses_value_is_set) {
			target_payload.value_is_set.insert(target_payload.value_is_set.end(),
			                                   source_payload.value_is_set.begin() + begin,
			                                   source_payload.value_is_set.begin() + end);
		}
		return true;
	}
	return false;
}

static bool SlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                               const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                               idx_t offset, idx_t count,
                                               SljitPreaggregatedPrimitiveAggregateScratch &target) {
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, offset, count)) {
		return false;
	}
	if (source.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE) {
		target.PrepareSharedAffine(lanes, count);
	} else {
		target.Prepare(lanes, count);
	}
	if (!AppendPreaggregatedPrimitiveScratchRows(source, offset, count, target, false)) {
		return false;
	}
	if (source.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE) {
		const auto begin = UnsafeNumericCast<int64_t>(offset);
		const auto end = UnsafeNumericCast<int64_t>(offset + count);
		target.shared_int64_values.insert(target.shared_int64_values.end(), source.shared_int64_values.begin() + begin,
		                                  source.shared_int64_values.begin() + end);
		target.shared_hugeint_values.insert(target.shared_hugeint_values.end(),
		                                    source.shared_hugeint_values.begin() + begin,
		                                    source.shared_hugeint_values.begin() + end);
		target.shared_value_is_wide.insert(target.shared_value_is_wide.end(),
		                                   source.shared_value_is_wide.begin() + begin,
		                                   source.shared_value_is_wide.begin() + end);
		target.shared_valid_counts_are_row_counts = source.shared_valid_counts_are_row_counts;
		for (idx_t idx = 0; idx < count; idx++) {
			target.shared_valid_counts.push_back(SljitLoadSharedAffineValidCount(source, offset + idx));
		}
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		if (!AppendPreaggregatedPrimitivePayloadRange(source.payloads[payload_idx], offset, count,
		                                              target.payloads[payload_idx])) {
			return false;
		}
	}
	return true;
}

static bool MergePreaggregatedPrimitiveScratchGroup(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                    idx_t source_idx,
                                                    SljitPreaggregatedPrimitiveAggregateScratch &target,
                                                    idx_t target_idx) {
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, source_idx, 1) ||
	    target.payloads.size() != lanes.size() || target.group_row_counts.size() <= target_idx) {
		return false;
	}
	if (source.payload_layout != target.payload_layout) {
		return false;
	}
	if (source.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE) {
		if (target.shared_int64_values.size() <= target_idx || target.shared_hugeint_values.size() <= target_idx ||
		    target.shared_value_is_wide.size() <= target_idx || target.shared_valid_counts.size() <= target_idx) {
			return false;
		}
		const auto target_valid_count = SljitLoadSharedAffineValidCount(target, target_idx);
		const auto source_valid_count = SljitLoadSharedAffineValidCount(source, source_idx);
		if (target.shared_valid_counts_are_row_counts != source.shared_valid_counts_are_row_counts &&
		    !SljitMaterializeSharedAffineValidCounts(target)) {
			return false;
		}
		target.group_row_counts[target_idx] += source.group_row_counts[source_idx];
		if (!SljitStoreSharedAffineValue(target, target_idx,
		                                 SljitLoadSharedAffineValue(target, target_idx) +
		                                     SljitLoadSharedAffineValue(source, source_idx))) {
			return false;
		}
		if (!target.shared_valid_counts_are_row_counts) {
			target.shared_valid_counts[target_idx] = target_valid_count + source_valid_count;
		}
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto &target_payload = target.payloads[payload_idx];
		if (source_payload.kind != target_payload.kind) {
			return false;
		}
		auto shape = SljitPreaggregatedPayloadShapeForKind(source_payload.kind);
		if (!shape.supported) {
			return false;
		}
		if (shape.uses_int64_values && target_payload.int64_values.size() <= target_idx) {
			return false;
		}
		if (shape.uses_hugeint_values && target_payload.hugeint_values.size() <= target_idx) {
			return false;
		}
		if (shape.uses_value_is_set && target_payload.value_is_set.size() <= target_idx) {
			return false;
		}
	}
	target.group_row_counts[target_idx] += source.group_row_counts[source_idx];
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto &target_payload = target.payloads[payload_idx];
		switch (source_payload.kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			target_payload.int64_values[target_idx] += source_payload.int64_values[source_idx];
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			if (source_payload.value_is_set[source_idx]) {
				target_payload.int64_values[target_idx] += source_payload.int64_values[source_idx];
				target_payload.value_is_set[target_idx] = 1;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			if (source_payload.value_is_set[source_idx]) {
				target_payload.hugeint_values[target_idx] += source_payload.hugeint_values[source_idx];
				target_payload.value_is_set[target_idx] = 1;
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool PreaggregatedPrimitiveRepresentedRowCount(const SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                                      idx_t offset, idx_t count, idx_t &row_count) {
	if (scratch.group_row_counts.size() < offset + count) {
		return false;
	}
	row_count = 0;
	for (idx_t group_idx = offset; group_idx < offset + count; group_idx++) {
		row_count += scratch.group_row_counts[group_idx];
	}
	return true;
}

static bool
SljitPrepareFusedPreaggregatedPrimitiveScratch(SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                               const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                               idx_t group_capacity, idx_t row_capacity) {
	idx_t state_stride = 0;
	for (auto lane : lanes) {
		if (!lane || !lane->ready || lane->state_size == 0) {
			return false;
		}
		state_stride = MaxValue<idx_t>(state_stride, lane->state_offset + lane->state_size);
	}
	if (state_stride == 0) {
		return false;
	}
	state_stride = AlignValue<idx_t>(state_stride);
	scratch.Prepare(lanes, group_capacity);
	scratch.fused_state_stride = state_stride;
	scratch.fused_state_storage.assign(group_capacity * state_stride, 0);
	scratch.fused_row_state_addresses.resize(row_capacity);
	return true;
}

static uintptr_t SljitFusedPreaggregatedPrimitiveStateAddress(SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                                              idx_t group_idx) {
	D_ASSERT(scratch.fused_state_stride > 0);
	D_ASSERT(group_idx * scratch.fused_state_stride < scratch.fused_state_storage.size());
	return reinterpret_cast<uintptr_t>(scratch.fused_state_storage.data() + group_idx * scratch.fused_state_stride);
}

static bool SljitFusedPreaggregatedPrimitiveStateBoundsValid(const SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                                             const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                             idx_t value_size, bool needs_state_is_set) {
	if (scratch.fused_state_stride == 0 || lane.state_offset > scratch.fused_state_stride ||
	    lane.state_value_offset > scratch.fused_state_stride - lane.state_offset ||
	    value_size > scratch.fused_state_stride - lane.state_offset - lane.state_value_offset) {
		return false;
	}
	if (!needs_state_is_set) {
		return true;
	}
	return lane.state_is_set_offset <= scratch.fused_state_stride - lane.state_offset &&
	       sizeof(bool) <= scratch.fused_state_stride - lane.state_offset - lane.state_is_set_offset;
}

static bool
SljitExtractFusedPreaggregatedPrimitiveDeltas(SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                              const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                              idx_t group_count) {
	if (scratch.payloads.size() != lanes.size() || scratch.group_row_counts.size() != group_count ||
	    scratch.fused_state_stride == 0 ||
	    scratch.fused_state_storage.size() < group_count * scratch.fused_state_stride) {
		return false;
	}
	auto state_base = scratch.fused_state_storage.data();
	for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
		auto lane = lanes[payload_idx];
		if (!lane || !lane->ready || lane->state_size == 0) {
			return false;
		}
		auto &payload = scratch.payloads[payload_idx];
		if (payload.kind != lane->kind) {
			return false;
		}
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			auto group_state = state_base + group_idx * scratch.fused_state_stride + lane->state_offset;
			auto value_ptr = group_state + lane->state_value_offset;
			switch (lane->kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
			case AggregatePrimitiveUpdateKind::COUNT:
				if (!SljitFusedPreaggregatedPrimitiveStateBoundsValid(scratch, *lane, sizeof(int64_t), false)) {
					return false;
				}
				payload.int64_values.push_back(*reinterpret_cast<int64_t *>(value_ptr));
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64: {
				if (!SljitFusedPreaggregatedPrimitiveStateBoundsValid(scratch, *lane, sizeof(int64_t), true)) {
					return false;
				}
				auto state_is_set = *reinterpret_cast<bool *>(group_state + lane->state_is_set_offset);
				payload.int64_values.push_back(state_is_set ? *reinterpret_cast<int64_t *>(value_ptr) : 0);
				payload.value_is_set.push_back(state_is_set ? 1 : 0);
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
				if (!SljitFusedPreaggregatedPrimitiveStateBoundsValid(scratch, *lane, sizeof(hugeint_t), true)) {
					return false;
				}
				auto state_is_set = *reinterpret_cast<bool *>(group_state + lane->state_is_set_offset);
				payload.hugeint_values.push_back(state_is_set ? *reinterpret_cast<hugeint_t *>(value_ptr)
				                                              : hugeint_t(0));
				payload.value_is_set.push_back(state_is_set ? 1 : 0);
				break;
			}
			default:
				return false;
			}
		}
	}
	return true;
}

} // namespace duckdb

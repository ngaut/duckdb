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

struct SljitPreaggregatedPrimitiveAggregateScratch {
	vector<SljitPreaggregatedPrimitivePayloadDeltas> payloads;
	vector<data_t> fused_state_storage;
	vector<uintptr_t> fused_row_state_addresses;
	vector<UnifiedVectorFormat> input_group_formats;
	vector<sel_t> group_rows;
	vector<idx_t> group_row_counts;
	idx_t fused_state_stride = 0;

	void Prepare(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
		payloads.resize(lanes.size());
		fused_state_storage.clear();
		fused_row_state_addresses.clear();
		input_group_formats.clear();
		group_rows.clear();
		group_rows.reserve(capacity);
		group_row_counts.clear();
		group_row_counts.reserve(capacity);
		fused_state_stride = 0;
		for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
			auto &payload = payloads[payload_idx];
			auto lane = lanes[payload_idx];
			payload.kind = lane ? lane->kind : AggregatePrimitiveUpdateKind::NONE;
			payload.int64_values.clear();
			payload.hugeint_values.clear();
			payload.value_is_set.clear();
			switch (payload.kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
			case AggregatePrimitiveUpdateKind::COUNT:
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				payload.int64_values.reserve(capacity);
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				payload.hugeint_values.reserve(capacity);
				break;
			default:
				break;
			}
			if (payload.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
			    payload.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				payload.value_is_set.reserve(capacity);
			}
		}
	}
};

static bool CanSlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                  idx_t offset, idx_t count) {
	if (source.payloads.size() != lanes.size() || source.group_row_counts.size() < offset + count) {
		return false;
	}
	if (!source.group_rows.empty() && source.group_rows.size() < offset + count) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto lane = lanes[payload_idx];
		if (!lane || source_payload.kind != lane->kind) {
			return false;
		}
		switch (source_payload.kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			if (source_payload.int64_values.size() < offset + count) {
				return false;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			if (source_payload.int64_values.size() < offset + count ||
			    source_payload.value_is_set.size() < offset + count) {
				return false;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			if (source_payload.hugeint_values.size() < offset + count ||
			    source_payload.value_is_set.size() < offset + count) {
				return false;
			}
			break;
		default:
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
	switch (source_payload.kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
		target_payload.int64_values.insert(target_payload.int64_values.end(),
		                                   source_payload.int64_values.begin() + begin,
		                                   source_payload.int64_values.begin() + end);
		return true;
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		target_payload.int64_values.insert(target_payload.int64_values.end(),
		                                   source_payload.int64_values.begin() + begin,
		                                   source_payload.int64_values.begin() + end);
		target_payload.value_is_set.insert(target_payload.value_is_set.end(),
		                                   source_payload.value_is_set.begin() + begin,
		                                   source_payload.value_is_set.begin() + end);
		return true;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		target_payload.hugeint_values.insert(target_payload.hugeint_values.end(),
		                                     source_payload.hugeint_values.begin() + begin,
		                                     source_payload.hugeint_values.begin() + end);
		target_payload.value_is_set.insert(target_payload.value_is_set.end(),
		                                   source_payload.value_is_set.begin() + begin,
		                                   source_payload.value_is_set.begin() + end);
		return true;
	default:
		return false;
	}
}

static bool PreaggregatedPrimitivePayloadsCanAppendRange(
    const SljitPreaggregatedPrimitiveAggregateScratch &source,
    const SljitPreaggregatedPrimitiveAggregateScratch &target) {
	if (source.payloads.size() != target.payloads.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto &target_payload = target.payloads[payload_idx];
		if (source_payload.kind != target_payload.kind) {
			return false;
		}
		switch (source_payload.kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool SlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                               const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                               idx_t offset, idx_t count,
                                               SljitPreaggregatedPrimitiveAggregateScratch &target) {
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, offset, count)) {
		return false;
	}
	target.Prepare(lanes, count);
	if (!AppendPreaggregatedPrimitiveScratchRows(source, offset, count, target, false)) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		if (!AppendPreaggregatedPrimitivePayloadRange(source.payloads[payload_idx], offset, count,
		                                             target.payloads[payload_idx])) {
			return false;
		}
	}
	return true;
}

static bool AppendPreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
                                                const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                                idx_t offset, idx_t count,
                                                SljitPreaggregatedPrimitiveAggregateScratch &target) {
	if (count == 0) {
		return true;
	}
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, offset, count)) {
		return false;
	}
	if (target.payloads.empty()) {
		target.Prepare(lanes, STANDARD_VECTOR_SIZE);
	}
	if (target.payloads.size() != lanes.size()) {
		return false;
	}
	if (!PreaggregatedPrimitivePayloadsCanAppendRange(source, target)) {
		return false;
	}
	if (!AppendPreaggregatedPrimitiveScratchRows(source, offset, count, target, true)) {
		return false;
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
	if (!CanSlicePreaggregatedPrimitiveScratch(source, lanes, source_idx, 1) || target.payloads.size() != lanes.size() ||
	    target.group_row_counts.size() <= target_idx) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
		auto &source_payload = source.payloads[payload_idx];
		auto &target_payload = target.payloads[payload_idx];
		if (source_payload.kind != target_payload.kind) {
			return false;
		}
		switch (source_payload.kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			if (target_payload.int64_values.size() <= target_idx) {
				return false;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			if (target_payload.int64_values.size() <= target_idx ||
			    target_payload.value_is_set.size() <= target_idx) {
				return false;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			if (target_payload.hugeint_values.size() <= target_idx ||
			    target_payload.value_is_set.size() <= target_idx) {
				return false;
			}
			break;
		default:
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

static bool SljitPrepareFusedPreaggregatedPrimitiveScratch(
    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t group_capacity, idx_t row_capacity) {
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

static uintptr_t SljitFusedPreaggregatedPrimitiveStateAddress(
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx) {
	D_ASSERT(scratch.fused_state_stride > 0);
	D_ASSERT(group_idx * scratch.fused_state_stride < scratch.fused_state_storage.size());
	return reinterpret_cast<uintptr_t>(scratch.fused_state_storage.data() +
	                                   group_idx * scratch.fused_state_stride);
}

static bool SljitFusedPreaggregatedPrimitiveStateBoundsValid(
    const SljitPreaggregatedPrimitiveAggregateScratch &scratch, const ExecutionPrimitiveAggregateUpdateLane &lane,
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

static bool SljitExtractFusedPreaggregatedPrimitiveDeltas(
    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t group_count) {
	if (scratch.payloads.size() != lanes.size() || scratch.group_row_counts.size() != group_count ||
	    scratch.fused_state_stride == 0 || scratch.fused_state_storage.size() < group_count * scratch.fused_state_stride) {
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

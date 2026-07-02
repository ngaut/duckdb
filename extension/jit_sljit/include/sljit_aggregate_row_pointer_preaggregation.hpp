//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_row_pointer_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"

namespace duckdb {

static bool SljitSameRowPointerIsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool SljitRowPointerGroupKeysEqual(data_ptr_t left, data_ptr_t right, bool same_row_pointer_is_equal,
                                          const vector<ExecutionRowPointerGroupKeySource> &group_sources);

static bool
SljitRowPointerDescriptorsHaveConsecutiveRepeat(Vector &row_pointers, idx_t count,
                                                const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const auto sample_count = MinValue<idx_t>(count, 64);
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(group_sources);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		if (SljitRowPointerGroupKeysEqual(row_pointer_data[row_idx - 1], row_pointer_data[row_idx],
		                                  same_row_pointer_is_equal, group_sources)) {
			return true;
		}
	}
	return false;
}

static bool SljitSameRowPointerIsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	bool same_row_pointer_is_equal = true;
	for (auto &source : group_sources) {
		same_row_pointer_is_equal = same_row_pointer_is_equal && source.all_valid;
	}
	return same_row_pointer_is_equal;
}

static bool SljitRowPointerGroupKeysEqual(data_ptr_t left, data_ptr_t right, bool same_row_pointer_is_equal,
                                          const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return (same_row_pointer_is_equal && left == right) ||
	       ExecutionRowPointerGroupKeysEqual(left, right, group_sources);
}

static bool TryPreaggregateConsecutiveRowPointerPrimitiveGroups(
    DataChunk &payload_input, Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t &compact_count) {
	compact_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	if (!SljitRowPointerDescriptorsHaveConsecutiveRepeat(row_pointers, count, group_sources)) {
		return false;
	}

	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(group_sources);
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(payload_input, payload_source_indices, payload_lanes)) {
		return false;
	}
	scratch.Prepare(payload_lanes, count);
	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);

	data_ptr_t active_row_pointer = nullptr;
	bool has_active_row_pointer = false;
	idx_t group_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		if (!has_active_row_pointer ||
		    !SljitRowPointerGroupKeysEqual(active_row_pointer, row_pointer, same_row_pointer_is_equal, group_sources)) {
			active_row_pointer = row_pointer;
			has_active_row_pointer = true;
			compact_row_pointer_data[group_count] = row_pointer;
			if (!SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes)) {
				return false;
			}
			group_count++;
		}
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                       group_count - 1)) {
			return false;
		}
	}
	if (group_count == count) {
		return false;
	}
	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}

static bool SljitTryPreaggregateConsecutiveRowPointerFusedPrimitiveGroups(
    SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    idx_t &compact_count) {
	compact_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources) ||
	    !op.aggregate_update.fused_payload_update_function) {
		return false;
	}
	if (!SljitRowPointerDescriptorsHaveConsecutiveRepeat(row_pointers, count, group_sources)) {
		return false;
	}

	idx_t state_stride = 0;
	for (auto lane : payload_lanes) {
		if (!lane || !lane->ready || lane->state_size == 0) {
			return false;
		}
		state_stride = MaxValue<idx_t>(state_stride, lane->state_offset + lane->state_size);
	}
	if (state_stride == 0) {
		return false;
	}
	state_stride = AlignValue<idx_t>(state_stride);
	scratch.Prepare(payload_lanes, count);
	scratch.fused_state_stride = state_stride;
	scratch.fused_state_storage.assign(count * state_stride, 0);
	scratch.fused_row_state_addresses.resize(count);

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto state_base = scratch.fused_state_storage.data();
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(group_sources);

	data_ptr_t active_row_pointer = nullptr;
	bool has_active_row_pointer = false;
	idx_t group_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		if (!has_active_row_pointer ||
		    !SljitRowPointerGroupKeysEqual(active_row_pointer, row_pointer, same_row_pointer_is_equal, group_sources)) {
			active_row_pointer = row_pointer;
			has_active_row_pointer = true;
			compact_row_pointer_data[group_count] = row_pointer;
			group_count++;
		}
		scratch.fused_row_state_addresses[row_idx] =
		    reinterpret_cast<uintptr_t>(state_base + (group_count - 1) * state_stride);
	}
	if (group_count == count) {
		return false;
	}

	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function,
	    op.aggregate_update.plan.sink_info.aggregates, op.aggregate_update.plan.sink_info.aggregate_contract,
	    payload_lanes, payload_input, scratch.fused_row_state_addresses.data(), nullptr, nullptr, false, count,
	    payload_scratch, optional_ptr<const vector<idx_t>>(&payload_source_indices));

	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		auto &payload = scratch.payloads[payload_idx];
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			auto group_state = state_base + group_idx * state_stride + lane->state_offset;
			auto value_ptr = group_state + lane->state_value_offset;
			switch (lane->kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
				payload.int64_values.push_back(*reinterpret_cast<int64_t *>(value_ptr));
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64: {
				auto state_is_set = *reinterpret_cast<bool *>(group_state + lane->state_is_set_offset);
				payload.int64_values.push_back(state_is_set ? *reinterpret_cast<int64_t *>(value_ptr) : 0);
				payload.value_is_set.push_back(state_is_set ? 1 : 0);
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
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

	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}
} // namespace duckdb

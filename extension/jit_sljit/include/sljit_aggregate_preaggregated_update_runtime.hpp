//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_preaggregated_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

namespace duckdb {

struct SljitPreaggregatedCountStarUpdateState {
	const ExecutionPrimitiveAggregateUpdateLane *lane = nullptr;
	const int64_t *counts = nullptr;
};

struct SljitPrimitiveCountOneUpdateState {
	const ExecutionPrimitiveAggregateUpdateLane *lane = nullptr;
};

static inline idx_t SljitSelectedGroupedStateAddressIndex(const sel_t *address_sel, const sel_t *execute_sel, idx_t idx,
                                                          idx_t row_idx) {
	if (address_sel) {
		return address_sel[row_idx];
	}
	return execute_sel ? idx : row_idx;
}

static void ExecuteSljitPreaggregatedCountStarUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                     const sel_t *execute_sel, idx_t count, void *state_p) {
	auto &state = *reinterpret_cast<SljitPreaggregatedCountStarUpdateState *>(state_p);
	if (!addresses || !state.lane || !state.counts || !state.lane->ready ||
	    state.lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR || state.lane->state_size == 0) {
		throw InternalException("SLJIT preaggregated count-star grouped update state is incomplete");
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		auto state_base = state_address + state.lane->state_offset;
		auto count_ptr = reinterpret_cast<int64_t *>(state_base + state.lane->state_value_offset);
		*count_ptr += state.counts[row_idx];
	}
}

static void ExecuteSljitPrimitiveCountOneUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                const sel_t *execute_sel, idx_t count, void *state_p) {
	auto &state = *reinterpret_cast<SljitPrimitiveCountOneUpdateState *>(state_p);
	if (!addresses || !state.lane || !state.lane->ready ||
	    (state.lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
	     state.lane->kind != AggregatePrimitiveUpdateKind::COUNT) ||
	    state.lane->state_size == 0) {
		throw InternalException("SLJIT primitive count-one grouped update state is incomplete");
	}
	if (!address_sel) {
		for (idx_t idx = 0; idx < count; idx++) {
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[idx]);
			auto state_base = state_address + state.lane->state_offset;
			auto count_ptr = reinterpret_cast<int64_t *>(state_base + state.lane->state_value_offset);
			*count_ptr += 1;
		}
		return;
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		auto state_base = state_address + state.lane->state_offset;
		auto count_ptr = reinterpret_cast<int64_t *>(state_base + state.lane->state_value_offset);
		*count_ptr += 1;
	}
}

static void ExecuteSljitPrimitiveCountOneTargetSpan(const ExecutionGroupedAggregateStateTargetSpan &span,
                                                    SljitPrimitiveCountOneUpdateState &state) {
	if (!span.HasTargets()) {
		return;
	}
	ExecuteSljitPrimitiveCountOneUpdate(span.addresses, span.address_sel, span.row_sel, span.count, &state);
}

static void ExecuteSljitPrimitiveCountOneTargetBatch(const ExecutionGroupedAggregateStateTargetBatch &targets,
                                                     SljitPrimitiveCountOneUpdateState &state) {
	for (auto &span : targets.Spans()) {
		ExecuteSljitPrimitiveCountOneTargetSpan(span, state);
	}
}

struct SljitPreaggregatedPrimitiveUpdateState {
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
	const vector<SljitPreaggregatedPrimitivePayloadDeltas> *payloads = nullptr;
};

static bool ExecuteSljitSingleLanePreaggregatedPrimitiveUpdate(
    const uintptr_t *addresses, const sel_t *address_sel, const sel_t *execute_sel, idx_t count,
    SljitPreaggregatedPrimitiveUpdateState &state) {
	auto &lanes = *state.lanes;
	auto &payloads = *state.payloads;
	if (lanes.size() != 1 || payloads.size() != 1) {
		return false;
	}
	auto lane = lanes[0];
	if (!lane || !lane->ready || lane->state_size == 0) {
		throw InternalException("SLJIT preaggregated primitive grouped update lane is incomplete");
	}
	auto &payload = payloads[0];
	auto state_value_offset = lane->state_offset + lane->state_value_offset;
	switch (lane->kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT: {
		auto values = payload.int64_values.data();
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			D_ASSERT(row_idx < payload.int64_values.size());
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			auto count_ptr = reinterpret_cast<int64_t *>(state_address + state_value_offset);
			*count_ptr += values[row_idx];
		}
		return true;
	}
	case AggregatePrimitiveUpdateKind::SUM_INT64: {
		auto state_is_set_offset = lane->state_offset + lane->state_is_set_offset;
		auto values = payload.int64_values.data();
		auto value_is_set = payload.value_is_set.data();
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			D_ASSERT(row_idx < payload.int64_values.size());
			D_ASSERT(row_idx < payload.value_is_set.size());
			if (!value_is_set[row_idx]) {
				continue;
			}
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			auto sum = reinterpret_cast<int64_t *>(state_address + state_value_offset);
			*sum += values[row_idx];
			auto state_is_set = reinterpret_cast<bool *>(state_address + state_is_set_offset);
			*state_is_set = true;
		}
		return true;
	}
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
		auto state_is_set_offset = lane->state_offset + lane->state_is_set_offset;
		auto values = payload.hugeint_values.data();
		auto value_is_set = payload.value_is_set.data();
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			D_ASSERT(row_idx < payload.hugeint_values.size());
			D_ASSERT(row_idx < payload.value_is_set.size());
			if (!value_is_set[row_idx]) {
				continue;
			}
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			auto sum = reinterpret_cast<hugeint_t *>(state_address + state_value_offset);
			*sum += values[row_idx];
			auto state_is_set = reinterpret_cast<bool *>(state_address + state_is_set_offset);
			*state_is_set = true;
		}
		return true;
	}
	default:
		return false;
	}
}

static void ExecuteSljitPreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                     const sel_t *execute_sel, idx_t count, void *state_p) {
	auto &state = *reinterpret_cast<SljitPreaggregatedPrimitiveUpdateState *>(state_p);
	if (!addresses || !state.lanes || !state.payloads || state.lanes->size() != state.payloads->size()) {
		throw InternalException("SLJIT preaggregated primitive grouped update state is incomplete");
	}
	if (ExecuteSljitSingleLanePreaggregatedPrimitiveUpdate(addresses, address_sel, execute_sel, count, state)) {
		return;
	}
	auto &lanes = *state.lanes;
	auto &payloads = *state.payloads;
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			if (!lane || !lane->ready || lane->state_size == 0) {
				throw InternalException("SLJIT preaggregated primitive grouped update lane is incomplete");
			}
			auto state_base = state_address + lane->state_offset;
			auto value_ptr = state_base + lane->state_value_offset;
			auto &payload = payloads[payload_idx];
			switch (lane->kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR: {
				if (row_idx >= payload.int64_values.size()) {
					throw InternalException("SLJIT preaggregated count-star delta is out of range");
				}
				auto count_ptr = reinterpret_cast<int64_t *>(value_ptr);
				*count_ptr += payload.int64_values[row_idx];
				break;
			}
			case AggregatePrimitiveUpdateKind::COUNT: {
				if (row_idx >= payload.int64_values.size()) {
					throw InternalException("SLJIT preaggregated count delta is out of range");
				}
				auto count_ptr = reinterpret_cast<int64_t *>(value_ptr);
				*count_ptr += payload.int64_values[row_idx];
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_INT64: {
				if (row_idx >= payload.int64_values.size()) {
					throw InternalException("SLJIT preaggregated int64 sum delta is out of range");
				}
				if (row_idx >= payload.value_is_set.size() || !payload.value_is_set[row_idx]) {
					break;
				}
				auto sum = reinterpret_cast<int64_t *>(value_ptr);
				*sum += payload.int64_values[row_idx];
				auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
				*state_is_set = true;
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
				if (row_idx >= payload.hugeint_values.size()) {
					throw InternalException("SLJIT preaggregated hugeint sum delta is out of range");
				}
				if (row_idx >= payload.value_is_set.size() || !payload.value_is_set[row_idx]) {
					break;
				}
				auto sum = reinterpret_cast<hugeint_t *>(value_ptr);
				*sum += payload.hugeint_values[row_idx];
				auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
				*state_is_set = true;
				break;
			}
			default:
				throw InternalException("Unsupported SLJIT preaggregated primitive update kind");
			}
		}
	}
}

static void ExecuteSljitPreaggregatedPrimitiveAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                            idx_t count, void *state_p) {
	ExecuteSljitPreaggregatedPrimitiveUpdate(addresses, address_sel, nullptr, count, state_p);
}

static void ExecuteSljitPreaggregatedPrimitiveTargetSpan(const ExecutionGroupedAggregateStateTargetSpan &span,
                                                         SljitPreaggregatedPrimitiveUpdateState &state) {
	if (!span.HasTargets()) {
		return;
	}
	ExecuteSljitPreaggregatedPrimitiveUpdate(span.addresses, span.address_sel, span.row_sel, span.count, &state);
}

static void ExecuteSljitPreaggregatedPrimitiveTargetBatch(const ExecutionGroupedAggregateStateTargetBatch &targets,
                                                          SljitPreaggregatedPrimitiveUpdateState &state) {
	for (auto &span : targets.Spans()) {
		ExecuteSljitPreaggregatedPrimitiveTargetSpan(span, state);
	}
}
} // namespace duckdb

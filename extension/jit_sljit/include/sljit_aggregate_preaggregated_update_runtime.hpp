//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_preaggregated_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
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

static bool
SljitTryBindPrimitiveCountOneUpdateState(const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                         const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                         bool count_one_payload, SljitPrimitiveCountOneUpdateState &state) {
	state = SljitPrimitiveCountOneUpdateState();
	if (payload_descriptors.size() != 1 || !count_one_payload || payload_lanes.size() != 1 || !payload_lanes[0] ||
	    !SljitAggregatePayloadDescriptorMatchesLane(payload_descriptors[0], *payload_lanes[0])) {
		return false;
	}
	auto lane = payload_lanes[0];
	if (lane->kind != AggregatePrimitiveUpdateKind::COUNT && lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return false;
	}
	state.lane = lane;
	return true;
}

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
	const vector<hugeint_t> *shared_affine_values = nullptr;
	const vector<idx_t> *shared_valid_counts = nullptr;
	const SljitExecutableFusedAffineRunUpdate *affine = nullptr;
	idx_t capture_row_idx = DConstants::INVALID_INDEX;
	uintptr_t captured_address = 0;
};

static SljitPreaggregatedPrimitiveUpdateState
SljitMakePreaggregatedPrimitiveUpdateState(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                           const vector<SljitPreaggregatedPrimitivePayloadDeltas> &payloads,
                                           idx_t capture_row_idx = DConstants::INVALID_INDEX) {
	SljitPreaggregatedPrimitiveUpdateState state;
	state.lanes = &lanes;
	state.payloads = &payloads;
	state.capture_row_idx = capture_row_idx;
	return state;
}

static SljitPreaggregatedPrimitiveUpdateState
SljitMakePreaggregatedPrimitiveUpdateState(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                           const SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                           optional_ptr<const SljitExecutableFusedAffineRunUpdate> affine,
                                           idx_t capture_row_idx = DConstants::INVALID_INDEX) {
	auto state = SljitMakePreaggregatedPrimitiveUpdateState(lanes, scratch.payloads, capture_row_idx);
	if (scratch.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE && affine &&
	    affine->Ready() &&
	    (affine->primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	     affine->primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		state.shared_affine_values = &scratch.shared_hugeint_values;
		state.shared_valid_counts = &scratch.shared_valid_counts;
		state.affine = affine.get();
	}
	return state;
}

static void SljitCapturePreaggregatedPrimitiveAddress(SljitPreaggregatedPrimitiveUpdateState &state, idx_t row_idx,
                                                      uintptr_t address) {
	if (state.capture_row_idx == row_idx) {
		state.captured_address = address;
	}
}

static bool ExecuteSljitSingleLanePreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                               const sel_t *execute_sel, idx_t count,
                                                               SljitPreaggregatedPrimitiveUpdateState &state,
                                                               bool initialize_states) {
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
			SljitCapturePreaggregatedPrimitiveAddress(state, row_idx, addresses[address_idx]);
			if (initialize_states) {
				ExecutionInitializeFreshPrimitiveAggregateState(state_address + lane->state_offset, *lane,
				                                                values[row_idx]);
			} else {
				auto count_ptr = reinterpret_cast<int64_t *>(state_address + state_value_offset);
				*count_ptr += values[row_idx];
			}
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
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			SljitCapturePreaggregatedPrimitiveAddress(state, row_idx, addresses[address_idx]);
			auto state_base = state_address + lane->state_offset;
			if (!value_is_set[row_idx]) {
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState<int64_t>(state_base, *lane, 0, false);
				}
				continue;
			}
			if (initialize_states) {
				ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, values[row_idx]);
				continue;
			}
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
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			SljitCapturePreaggregatedPrimitiveAddress(state, row_idx, addresses[address_idx]);
			auto state_base = state_address + lane->state_offset;
			if (!value_is_set[row_idx]) {
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState<hugeint_t>(state_base, *lane, hugeint_t(), false);
				}
				continue;
			}
			if (initialize_states) {
				ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, values[row_idx]);
				continue;
			}
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

static bool SljitTryComputeSharedAffineInt64Delta(int64_t shared_value, int64_t valid_count,
                                                  const SljitFusedAffineRunLane &lane, int64_t &result) {
	int64_t scaled_value;
	int64_t offset_value;
#if defined(__GNUC__) || defined(__clang__)
	return !__builtin_mul_overflow(shared_value, lane.scale, &scaled_value) &&
	       !__builtin_mul_overflow(valid_count, lane.offset, &offset_value) &&
	       !__builtin_add_overflow(scaled_value, offset_value, &result);
#else
	return TryMultiplyOperator::Operation(shared_value, lane.scale, scaled_value) &&
	       TryMultiplyOperator::Operation(valid_count, lane.offset, offset_value) &&
	       TryAddOperator::Operation(scaled_value, offset_value, result);
#endif
}

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

static bool SljitTryAddSharedAffineInt64Delta(int64_t left, int64_t right, int64_t &result) {
#if defined(__GNUC__) || defined(__clang__)
	return !__builtin_add_overflow(left, right, &result);
#else
	return TryAddOperator::Operation(left, right, result);
#endif
}

template <bool INITIALIZE_STATES>
static bool
ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdateTemplated(const uintptr_t *addresses, const sel_t *address_sel,
                                                              const sel_t *execute_sel, idx_t count,
                                                              SljitPreaggregatedPrimitiveUpdateState &state) {
	if (!state.affine) {
		return false;
	}
	if (!state.lanes || !state.shared_affine_values || !state.shared_valid_counts ||
	    (state.affine->primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     state.affine->primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    state.lanes->size() != state.affine->lanes.size() || state.shared_affine_values->size() < count ||
	    state.shared_valid_counts->size() < count) {
		throw InternalException("SLJIT shared affine grouped update state is incomplete");
	}
	auto &lanes = *state.lanes;
	auto &affine_lanes = state.affine->lanes;
	auto &shared_values = *state.shared_affine_values;
	auto &valid_counts = *state.shared_valid_counts;
	for (auto lane : lanes) {
		if (!lane || !lane->ready || lane->kind != state.affine->primitive_kind || lane->state_size == 0) {
			throw InternalException("SLJIT shared affine grouped update lane is incomplete");
		}
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		if (row_idx >= shared_values.size() || row_idx >= valid_counts.size()) {
			throw InternalException("SLJIT shared affine int64 grouped update row is out of range");
		}
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		SljitCapturePreaggregatedPrimitiveAddress(state, row_idx, addresses[address_idx]);
		const auto valid_count = valid_counts[row_idx];
		if (valid_count == 0) {
			if constexpr (INITIALIZE_STATES) {
				for (auto lane : lanes) {
					if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
						ExecutionInitializeFreshPrimitiveAggregateState<int64_t>(state_address + lane->state_offset,
						                                                         *lane, 0, false);
					} else {
						ExecutionInitializeFreshPrimitiveAggregateState<hugeint_t>(state_address + lane->state_offset,
						                                                           *lane, hugeint_t(0), false);
					}
				}
			}
			continue;
		}
		int64_t machine_word_shared_value;
		const bool machine_word_inputs =
		    valid_count <= NumericLimits<int64_t>::Maximum() &&
		    SljitTryLoadSharedAffineInt64(shared_values[row_idx], machine_word_shared_value);
		const auto machine_word_valid_count =
		    machine_word_inputs ? UnsafeNumericCast<int64_t>(valid_count) : int64_t(0);
		for (idx_t lane_idx = 0; lane_idx < lanes.size(); lane_idx++) {
			auto lane = lanes[lane_idx];
			auto state_base = state_address + lane->state_offset;
			auto &affine_lane = affine_lanes[lane_idx];
			if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				int64_t value;
				const bool machine_word_value =
				    machine_word_inputs && SljitTryComputeSharedAffineInt64Delta(
				                               machine_word_shared_value, machine_word_valid_count, affine_lane, value);
				if constexpr (INITIALIZE_STATES) {
					if (!machine_word_value) {
						const auto wide_value =
						    shared_values[row_idx] * hugeint_t(affine_lane.scale) +
						    hugeint_t(NumericCast<int64_t>(valid_count)) * hugeint_t(affine_lane.offset);
						value = NumericCast<int64_t>(wide_value);
					}
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, value);
				} else {
					auto sum = reinterpret_cast<int64_t *>(state_base + lane->state_value_offset);
					int64_t updated_sum;
					if (machine_word_value && SljitTryAddSharedAffineInt64Delta(*sum, value, updated_sum)) {
						*sum = updated_sum;
					} else {
						const auto wide_value =
						    machine_word_value
						        ? hugeint_t(value)
						        : shared_values[row_idx] * hugeint_t(affine_lane.scale) +
						              hugeint_t(NumericCast<int64_t>(valid_count)) * hugeint_t(affine_lane.offset);
						*sum = NumericCast<int64_t>(hugeint_t(*sum) + wide_value);
					}
					auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
					*state_is_set = true;
				}
			} else {
				const auto value = shared_values[row_idx] * hugeint_t(affine_lane.scale) +
				                   hugeint_t(NumericCast<int64_t>(valid_count)) * hugeint_t(affine_lane.offset);
				if constexpr (INITIALIZE_STATES) {
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, value);
				} else {
					auto sum = reinterpret_cast<hugeint_t *>(state_base + lane->state_value_offset);
					*sum += value;
					auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
					*state_is_set = true;
				}
			}
		}
	}
	return true;
}

static bool ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                                 const sel_t *execute_sel, idx_t count,
                                                                 SljitPreaggregatedPrimitiveUpdateState &state,
                                                                 bool initialize_states) {
	return initialize_states
	           ? ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdateTemplated<true>(addresses, address_sel,
	                                                                                 execute_sel, count, state)
	           : ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdateTemplated<false>(addresses, address_sel,
	                                                                                  execute_sel, count, state);
}

static void ExecuteSljitPreaggregatedPrimitiveUpdateInternal(const uintptr_t *addresses, const sel_t *address_sel,
                                                             const sel_t *execute_sel, idx_t count,
                                                             bool initialize_states, void *state_p) {
	auto &state = *reinterpret_cast<SljitPreaggregatedPrimitiveUpdateState *>(state_p);
	if (!addresses || !state.lanes || !state.payloads || state.lanes->size() != state.payloads->size()) {
		throw InternalException("SLJIT preaggregated primitive grouped update state is incomplete");
	}
	if (ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdate(addresses, address_sel, execute_sel, count, state,
	                                                         initialize_states)) {
		return;
	}
	if (ExecuteSljitSingleLanePreaggregatedPrimitiveUpdate(addresses, address_sel, execute_sel, count, state,
	                                                       initialize_states)) {
		return;
	}
	auto &lanes = *state.lanes;
	auto &payloads = *state.payloads;
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		SljitCapturePreaggregatedPrimitiveAddress(state, row_idx, addresses[address_idx]);
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
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, payload.int64_values[row_idx]);
				} else {
					auto count_ptr = reinterpret_cast<int64_t *>(value_ptr);
					*count_ptr += payload.int64_values[row_idx];
				}
				break;
			}
			case AggregatePrimitiveUpdateKind::COUNT: {
				if (row_idx >= payload.int64_values.size()) {
					throw InternalException("SLJIT preaggregated count delta is out of range");
				}
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, payload.int64_values[row_idx]);
				} else {
					auto count_ptr = reinterpret_cast<int64_t *>(value_ptr);
					*count_ptr += payload.int64_values[row_idx];
				}
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_INT64: {
				if (row_idx >= payload.int64_values.size()) {
					throw InternalException("SLJIT preaggregated int64 sum delta is out of range");
				}
				if (row_idx >= payload.value_is_set.size()) {
					throw InternalException("SLJIT preaggregated int64 sum validity is out of range");
				}
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, payload.int64_values[row_idx],
					                                                payload.value_is_set[row_idx]);
					break;
				}
				if (!payload.value_is_set[row_idx]) {
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
				if (row_idx >= payload.value_is_set.size()) {
					throw InternalException("SLJIT preaggregated hugeint sum validity is out of range");
				}
				if (initialize_states) {
					ExecutionInitializeFreshPrimitiveAggregateState(state_base, *lane, payload.hugeint_values[row_idx],
					                                                payload.value_is_set[row_idx]);
					break;
				}
				if (!payload.value_is_set[row_idx]) {
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

static void ExecuteSljitPreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                     const sel_t *execute_sel, idx_t count, void *state_p) {
	ExecuteSljitPreaggregatedPrimitiveUpdateInternal(addresses, address_sel, execute_sel, count, false, state_p);
}

static void ExecuteSljitPreaggregatedPrimitiveAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                            idx_t count,
                                                            ExecutionGroupedAggregateStateAddressUpdateMode mode,
                                                            void *state_p) {
	ExecuteSljitPreaggregatedPrimitiveUpdateInternal(
	    addresses, address_sel, nullptr, count,
	    mode == ExecutionGroupedAggregateStateAddressUpdateMode::INITIALIZE_AND_UPDATE, state_p);
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

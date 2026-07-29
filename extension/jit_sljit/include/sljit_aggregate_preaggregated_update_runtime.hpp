//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_preaggregated_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_preaggregated_primitive_update_codegen.hpp"
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
	const vector<int64_t> *shared_affine_int64_values = nullptr;
	const vector<hugeint_t> *shared_affine_hugeint_values = nullptr;
	const vector<uint8_t> *shared_affine_value_is_wide = nullptr;
	const vector<idx_t> *shared_valid_counts = nullptr;
	const SljitExecutableFusedAffineRunUpdate *affine = nullptr;
	idx_t shared_affine_state_offset = 0;
	idx_t shared_affine_state_stride = 0;
	bool shared_affine_canonical_states = false;
	ExecutionRegionRuntime *runtime = nullptr;
	SljitExecutablePrimitiveRunUpdate *primitive_run_update = nullptr;
	vector<SljitNativePreaggregatedPrimitiveLaneInput> native_lane_inputs;
	idx_t native_input_capacity = 0;
	idx_t generated_initialize_count = 0;
	idx_t generated_update_count = 0;
	idx_t capture_row_idx = DConstants::INVALID_INDEX;
	uintptr_t captured_address = 0;
};

static bool SljitBindPreaggregatedPrimitiveNativeInputs(SljitPreaggregatedPrimitiveUpdateState &state) {
	if (!state.lanes || !state.payloads || state.lanes->empty() || state.lanes->size() != state.payloads->size()) {
		return false;
	}
	state.native_lane_inputs.clear();
	state.native_lane_inputs.resize(state.lanes->size());
	state.native_input_capacity = NumericLimits<idx_t>::Maximum();
	for (idx_t lane_idx = 0; lane_idx < state.lanes->size(); lane_idx++) {
		auto lane = (*state.lanes)[lane_idx];
		if (!lane) {
			return false;
		}
		auto &payload = (*state.payloads)[lane_idx];
		auto &native = state.native_lane_inputs[lane_idx];
		native.state_offset = lane->state_offset;
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			native.int64_values = payload.int64_values.data();
			state.native_input_capacity =
			    MinValue(state.native_input_capacity, NumericCast<idx_t>(payload.int64_values.size()));
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			native.int64_values = payload.int64_values.data();
			native.value_is_set = payload.value_is_set.data();
			state.native_input_capacity =
			    MinValue(state.native_input_capacity, MinValue(NumericCast<idx_t>(payload.int64_values.size()),
			                                                   NumericCast<idx_t>(payload.value_is_set.size())));
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			native.hugeint_values = payload.hugeint_values.data();
			native.value_is_set = payload.value_is_set.data();
			state.native_input_capacity =
			    MinValue(state.native_input_capacity, MinValue(NumericCast<idx_t>(payload.hugeint_values.size()),
			                                                   NumericCast<idx_t>(payload.value_is_set.size())));
			break;
		default:
			return false;
		}
	}
	return state.native_input_capacity != NumericLimits<idx_t>::Maximum();
}

static SljitPreaggregatedPrimitiveUpdateState
SljitMakePreaggregatedPrimitiveUpdateState(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                           const vector<SljitPreaggregatedPrimitivePayloadDeltas> &payloads,
                                           idx_t capture_row_idx = DConstants::INVALID_INDEX) {
	SljitPreaggregatedPrimitiveUpdateState state;
	state.lanes = &lanes;
	state.payloads = &payloads;
	state.capture_row_idx = capture_row_idx;
	SljitBindPreaggregatedPrimitiveNativeInputs(state);
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
		state.shared_affine_int64_values = &scratch.shared_int64_values;
		state.shared_affine_hugeint_values = &scratch.shared_hugeint_values;
		state.shared_affine_value_is_wide = &scratch.shared_value_is_wide;
		state.shared_valid_counts =
		    scratch.shared_valid_counts_are_row_counts ? &scratch.group_row_counts : &scratch.shared_valid_counts;
		state.affine = affine.get();
		if (!lanes.empty()) {
			const auto state_value_size = AggregatePrimitiveUpdateStateValueSize(affine->primitive_kind);
			const auto state_stride = state_value_size + sizeof(uint64_t);
			const auto state_offset = lanes[0] ? lanes[0]->state_offset : 0;
			state.shared_affine_canonical_states = true;
			for (idx_t lane_idx = 0; lane_idx < lanes.size(); lane_idx++) {
				auto lane = lanes[lane_idx];
				if (!lane || lane->kind != affine->primitive_kind || lane->state_size != state_stride ||
				    lane->state_value_offset != 0 || lane->state_is_set_offset != state_value_size ||
				    lane->state_offset != state_offset + lane_idx * state_stride) {
					state.shared_affine_canonical_states = false;
					break;
				}
			}
			if (state.shared_affine_canonical_states) {
				state.shared_affine_state_offset = state_offset;
				state.shared_affine_state_stride = state_stride;
			}
		}
	}
	return state;
}

static void SljitCapturePreaggregatedPrimitiveAddress(SljitPreaggregatedPrimitiveUpdateState &state, idx_t row_idx,
                                                      uintptr_t address) {
	if (state.capture_row_idx == row_idx) {
		state.captured_address = address;
	}
}

static bool SljitPrepareGeneratedPreaggregatedPrimitiveSelection(const uintptr_t *addresses, const sel_t *address_sel,
                                                                 const sel_t *execute_sel, idx_t count,
                                                                 SljitPreaggregatedPrimitiveUpdateState &state) {
	if (!execute_sel) {
		if (count > state.native_input_capacity) {
			return false;
		}
		if (state.capture_row_idx < count) {
			const auto address_idx = address_sel ? address_sel[state.capture_row_idx] : state.capture_row_idx;
			state.captured_address = addresses[address_idx];
		}
		return true;
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel[idx];
		if (row_idx >= state.native_input_capacity) {
			return false;
		}
		if (row_idx == state.capture_row_idx) {
			const auto address_idx = address_sel ? address_sel[row_idx] : idx;
			state.captured_address = addresses[address_idx];
		}
	}
	return true;
}

static bool ExecuteSljitGeneratedPreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                              const sel_t *execute_sel, idx_t count,
                                                              bool initialize_states,
                                                              SljitPreaggregatedPrimitiveUpdateState &state) {
	if ((count != 0 && !addresses) || !state.lanes || !state.runtime || !state.primitive_run_update ||
	    state.native_lane_inputs.size() != state.lanes->size() ||
	    !SljitPrepareGeneratedPreaggregatedPrimitiveSelection(addresses, address_sel, execute_sel, count, state)) {
		return false;
	}
	auto function = SljitEnsureExecutablePreaggregatedPrimitiveUpdate(*state.runtime, *state.primitive_run_update,
	                                                                  *state.lanes, initialize_states,
	                                                                  address_sel != nullptr, execute_sel != nullptr);
	if (!function) {
		return false;
	}
	SljitNativePreaggregatedPrimitiveUpdateInput input;
	input.addresses = addresses;
	input.address_sel = address_sel;
	input.execute_sel = execute_sel;
	input.lane_inputs = state.native_lane_inputs.data();
	input.count = count;
	function(&input);
	if (initialize_states) {
		state.generated_initialize_count += count;
	} else {
		state.generated_update_count += count;
	}
	return true;
}

template <class VALUE_TYPE>
static inline void SljitInitializeCanonicalSumState(const ExecutionPrimitiveAggregateUpdateLane &lane, VALUE_TYPE value,
                                                    uint64_t state_is_set_word, data_ptr_t state_address) {
	auto state_base = state_address + lane.state_offset;
	Store<VALUE_TYPE>(state_is_set_word != 0 ? value : VALUE_TYPE(), state_base);
	Store<uint64_t>(state_is_set_word, state_base + lane.state_is_set_offset);
}

template <class VALUE_TYPE>
static bool ExecuteSljitSingleLaneCanonicalSumInitialization(const uintptr_t *addresses, const sel_t *address_sel,
                                                             const sel_t *execute_sel, idx_t count,
                                                             const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                             const vector<VALUE_TYPE> &values,
                                                             const vector<uint8_t> &value_is_set,
                                                             SljitPreaggregatedPrimitiveUpdateState &state) {
	if (lane.state_value_offset != 0 || lane.state_is_set_offset != sizeof(VALUE_TYPE) ||
	    lane.state_size != sizeof(VALUE_TYPE) + sizeof(uint64_t)) {
		return false;
	}
	uint64_t canonical_is_set_word = 0;
	Store<bool>(true, reinterpret_cast<data_ptr_t>(&canonical_is_set_word));
	if (!address_sel && !execute_sel) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			D_ASSERT(row_idx < values.size());
			D_ASSERT(row_idx < value_is_set.size());
			SljitInitializeCanonicalSumState(lane, values[row_idx],
			                                 value_is_set[row_idx] != 0 ? canonical_is_set_word : 0,
			                                 reinterpret_cast<data_ptr_t>(addresses[row_idx]));
		}
		if (state.capture_row_idx < count) {
			state.captured_address = addresses[state.capture_row_idx];
		}
		return true;
	}
	const auto capture_address = state.capture_row_idx != DConstants::INVALID_INDEX;
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		D_ASSERT(row_idx < values.size());
		D_ASSERT(row_idx < value_is_set.size());
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
		if (capture_address && state.capture_row_idx == row_idx) {
			state.captured_address = addresses[address_idx];
		}
		const auto state_is_set_word = value_is_set[row_idx] != 0 ? canonical_is_set_word : 0;
		SljitInitializeCanonicalSumState(lane, values[row_idx], state_is_set_word, state_address);
	}
	return true;
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
	if (initialize_states) {
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			if (ExecuteSljitSingleLaneCanonicalSumInitialization(addresses, address_sel, execute_sel, count, *lane,
			                                                     payload.int64_values, payload.value_is_set, state)) {
				return true;
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			if (ExecuteSljitSingleLaneCanonicalSumInitialization(addresses, address_sel, execute_sel, count, *lane,
			                                                     payload.hugeint_values, payload.value_is_set, state)) {
				return true;
			}
			break;
		default:
			break;
		}
	}
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

static bool SljitTryAddSharedAffineInt64Delta(int64_t left, int64_t right, int64_t &result) {
#if defined(__GNUC__) || defined(__clang__)
	return !__builtin_add_overflow(left, right, &result);
#else
	return TryAddOperator::Operation(left, right, result);
#endif
}

static bool SljitTryMultiplySharedAffineInt64Delta(int64_t left, int64_t right, int64_t &result) {
#if defined(__GNUC__) || defined(__clang__)
	return !__builtin_mul_overflow(left, right, &result);
#else
	return TryMultiplyOperator::Operation(left, right, result);
#endif
}

static inline hugeint_t SljitHugeintFromInt64Bits(int64_t value) {
	return hugeint_t(value < 0 ? -1 : 0, static_cast<uint64_t>(value));
}

static bool ExecuteSljitCanonicalSharedAffineInt64Initialization(
    const uintptr_t *addresses, idx_t count, SljitPreaggregatedPrimitiveUpdateState &state,
    const vector<int64_t> &shared_values, const vector<uint8_t> &wide_values, const vector<idx_t> &valid_counts,
    idx_t lane_count, int64_t lane_span, idx_t state_offset, idx_t state_stride, uint64_t canonical_is_set_word) {
	const auto wide_data = wide_values.data();
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (wide_data[row_idx] != 0) {
			return false;
		}
	}

	const auto shared_data = shared_values.data();
	const auto valid_count_data = valid_counts.data();
	auto &affine = *state.affine;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto valid_count = valid_count_data[row_idx];
		int64_t first_value = 0;
		int64_t value_step = 0;
		int64_t progression_last_value = 0;
		if (valid_count != 0) {
			if (valid_count > NumericLimits<int64_t>::Maximum()) {
				return false;
			}
			int64_t value_span;
			const auto machine_word_valid_count = UnsafeNumericCast<int64_t>(valid_count);
			if (!SljitTryComputeSharedAffineInt64Delta(shared_data[row_idx], machine_word_valid_count,
			                                           affine.lanes.front(), first_value) ||
			    !SljitTryComputeSharedAffineInt64Delta(shared_data[row_idx], machine_word_valid_count, affine.lane_step,
			                                           value_step) ||
			    !SljitTryMultiplySharedAffineInt64Delta(value_step, lane_span, value_span) ||
			    !SljitTryAddSharedAffineInt64Delta(first_value, value_span, progression_last_value)) {
				return false;
			}
		}

		auto state_base = reinterpret_cast<data_ptr_t>(addresses[row_idx]) + state_offset;
		const auto state_is_set_word = valid_count != 0 ? canonical_is_set_word : 0;
		auto value = first_value;
		for (idx_t lane_idx = 1; lane_idx < lane_count; lane_idx++) {
			Store<int64_t>(value, state_base);
			Store<uint64_t>(state_is_set_word, state_base + sizeof(int64_t));
			state_base += state_stride;
			value += value_step;
		}
		Store<int64_t>(value, state_base);
		Store<uint64_t>(state_is_set_word, state_base + sizeof(int64_t));
		D_ASSERT(value == progression_last_value);
	}
	if (state.capture_row_idx < count) {
		state.captured_address = addresses[state.capture_row_idx];
	}
	return true;
}

static hugeint_t SljitComputeSharedAffineHugeintDelta(const hugeint_t &shared_value, const hugeint_t &valid_count,
                                                      const SljitFusedAffineRunLane &lane) {
	const auto scaled_value = lane.scale == 0   ? hugeint_t(0)
	                          : lane.scale == 1 ? shared_value
	                                            : shared_value * hugeint_t(lane.scale);
	const auto offset_value = lane.offset == 0   ? hugeint_t(0)
	                          : lane.offset == 1 ? valid_count
	                                             : valid_count * hugeint_t(lane.offset);
	return scaled_value + offset_value;
}

static bool ExecuteSljitCanonicalSharedAffineHugeintInitialization(
    const uintptr_t *addresses, idx_t count, SljitPreaggregatedPrimitiveUpdateState &state,
    const vector<int64_t> &shared_int64_values, const vector<hugeint_t> &shared_hugeint_values,
    const vector<uint8_t> &shared_value_is_wide, const vector<idx_t> &valid_counts, idx_t lane_count,
    idx_t state_offset, idx_t state_stride, uint64_t canonical_is_set_word) {
	auto &affine = *state.affine;
	const auto lane_span = NumericCast<int64_t>(lane_count - 1);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto valid_count = valid_counts[row_idx];
		if (valid_count > NumericLimits<int64_t>::Maximum()) {
			return false;
		}
		const auto state_is_set_word = valid_count != 0 ? canonical_is_set_word : 0;
		int64_t first_machine_value = 0;
		int64_t machine_value_step = 0;
		int64_t last_machine_value = 0;
		bool machine_word_progression = valid_count == 0;
		if (!machine_word_progression && shared_value_is_wide[row_idx] == 0) {
			int64_t machine_value_span;
			const auto machine_word_valid_count = UnsafeNumericCast<int64_t>(valid_count);
			machine_word_progression =
			    SljitTryComputeSharedAffineInt64Delta(shared_int64_values[row_idx], machine_word_valid_count,
			                                          affine.lanes.front(), first_machine_value) &&
			    SljitTryComputeSharedAffineInt64Delta(shared_int64_values[row_idx], machine_word_valid_count,
			                                          affine.lane_step, machine_value_step) &&
			    SljitTryMultiplySharedAffineInt64Delta(machine_value_step, lane_span, machine_value_span) &&
			    SljitTryAddSharedAffineInt64Delta(first_machine_value, machine_value_span, last_machine_value);
		}
		if (machine_word_progression) {
			auto state_base = reinterpret_cast<data_ptr_t>(addresses[row_idx]) + state_offset;
			auto value = first_machine_value;
			for (idx_t lane_idx = 1; lane_idx < lane_count; lane_idx++) {
				Store<hugeint_t>(SljitHugeintFromInt64Bits(value), state_base);
				Store<uint64_t>(state_is_set_word, state_base + sizeof(hugeint_t));
				state_base += state_stride;
				value += machine_value_step;
			}
			Store<hugeint_t>(SljitHugeintFromInt64Bits(value), state_base);
			Store<uint64_t>(state_is_set_word, state_base + sizeof(hugeint_t));
			D_ASSERT(value == last_machine_value);
			continue;
		}
		const auto shared_value =
		    shared_value_is_wide[row_idx] ? shared_hugeint_values[row_idx] : hugeint_t(shared_int64_values[row_idx]);
		const auto wide_valid_count = hugeint_t(UnsafeNumericCast<int64_t>(valid_count));
		auto value = valid_count == 0
		                 ? hugeint_t(0)
		                 : SljitComputeSharedAffineHugeintDelta(shared_value, wide_valid_count, affine.lanes.front());
		const auto value_step =
		    valid_count == 0 ? hugeint_t(0)
		                     : SljitComputeSharedAffineHugeintDelta(shared_value, wide_valid_count, affine.lane_step);
		auto state_base = reinterpret_cast<data_ptr_t>(addresses[row_idx]) + state_offset;
		for (idx_t lane_idx = 1; lane_idx < lane_count; lane_idx++) {
			Store<hugeint_t>(value, state_base);
			Store<uint64_t>(state_is_set_word, state_base + sizeof(hugeint_t));
			state_base += state_stride;
			value += value_step;
		}
		Store<hugeint_t>(value, state_base);
		Store<uint64_t>(state_is_set_word, state_base + sizeof(hugeint_t));
	}
	if (state.capture_row_idx < count) {
		state.captured_address = addresses[state.capture_row_idx];
	}
	return true;
}

template <bool INITIALIZE_STATES>
static bool
ExecuteSljitSharedAffinePreaggregatedPrimitiveUpdateTemplated(const uintptr_t *addresses, const sel_t *address_sel,
                                                              const sel_t *execute_sel, idx_t count,
                                                              SljitPreaggregatedPrimitiveUpdateState &state) {
	if (!state.affine) {
		return false;
	}
	if (!state.lanes || !state.shared_affine_int64_values || !state.shared_affine_hugeint_values ||
	    !state.shared_affine_value_is_wide || !state.shared_valid_counts || !state.affine->Ready() ||
	    (state.affine->primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     state.affine->primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    state.lanes->size() != state.affine->lanes.size() || state.shared_affine_int64_values->size() < count ||
	    state.shared_affine_hugeint_values->size() < count || state.shared_affine_value_is_wide->size() < count ||
	    state.shared_valid_counts->size() < count) {
		throw InternalException("SLJIT shared affine grouped update state is incomplete");
	}
	auto &lanes = *state.lanes;
	auto &affine_lanes = state.affine->lanes;
	auto &shared_int64_values = *state.shared_affine_int64_values;
	auto &shared_hugeint_values = *state.shared_affine_hugeint_values;
	auto &shared_value_is_wide = *state.shared_affine_value_is_wide;
	auto &valid_counts = *state.shared_valid_counts;
	const auto lane_count = lanes.size();
	const auto lane_span = NumericCast<int64_t>(lane_count - 1);
	const auto shared_affine_state_offset = state.shared_affine_state_offset;
	const auto shared_affine_state_stride = state.shared_affine_state_stride;
	uint64_t canonical_is_set_word = 0;
	Store<bool>(true, reinterpret_cast<data_ptr_t>(&canonical_is_set_word));
	for (auto lane : lanes) {
		if (!lane || !lane->ready || lane->kind != state.affine->primitive_kind || lane->state_size == 0) {
			throw InternalException("SLJIT shared affine grouped update lane is incomplete");
		}
	}
	if constexpr (INITIALIZE_STATES) {
		if (!address_sel && !execute_sel && state.shared_affine_canonical_states &&
		    state.affine->lanes_form_arithmetic_progression) {
			if (state.affine->primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 &&
			    ExecuteSljitCanonicalSharedAffineInt64Initialization(
			        addresses, count, state, shared_int64_values, shared_value_is_wide, valid_counts, lane_count,
			        lane_span, shared_affine_state_offset, shared_affine_state_stride, canonical_is_set_word)) {
				return true;
			}
			if (state.affine->primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT &&
			    ExecuteSljitCanonicalSharedAffineHugeintInitialization(
			        addresses, count, state, shared_int64_values, shared_hugeint_values, shared_value_is_wide,
			        valid_counts, lane_count, shared_affine_state_offset, shared_affine_state_stride,
			        canonical_is_set_word)) {
				return true;
			}
		}
	}
	for (idx_t idx = 0; idx < count; idx++) {
		const auto row_idx = execute_sel ? execute_sel[idx] : idx;
		if (row_idx >= shared_int64_values.size() || row_idx >= shared_hugeint_values.size() ||
		    row_idx >= shared_value_is_wide.size() || row_idx >= valid_counts.size()) {
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
		const bool wide_shared_value = shared_value_is_wide[row_idx] != 0;
		const bool machine_word_value =
		    !wide_shared_value ||
		    SljitTryLoadSharedAffineInt64(shared_hugeint_values[row_idx], machine_word_shared_value);
		if (!wide_shared_value) {
			machine_word_shared_value = shared_int64_values[row_idx];
		}
		const bool machine_word_inputs = valid_count <= NumericLimits<int64_t>::Maximum() && machine_word_value;
		const auto machine_word_valid_count =
		    machine_word_inputs ? UnsafeNumericCast<int64_t>(valid_count) : int64_t(0);
		if constexpr (INITIALIZE_STATES) {
			if (machine_word_inputs && state.shared_affine_canonical_states &&
			    state.affine->primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 &&
			    state.affine->lanes_form_arithmetic_progression) {
				int64_t first_value;
				int64_t value_step;
				int64_t value_span;
				int64_t progression_last_value;
				// Planning already proves the lane coefficients form this progression. Runtime only has to prove that
				// the row-specific first value, step, and complete span fit in the canonical state type.
				if (SljitTryComputeSharedAffineInt64Delta(machine_word_shared_value, machine_word_valid_count,
				                                          affine_lanes.front(), first_value) &&
				    SljitTryComputeSharedAffineInt64Delta(machine_word_shared_value, machine_word_valid_count,
				                                          state.affine->lane_step, value_step) &&
				    SljitTryMultiplySharedAffineInt64Delta(value_step, lane_span, value_span) &&
				    SljitTryAddSharedAffineInt64Delta(first_value, value_span, progression_last_value)) {
					auto value = first_value;
					auto state_base = state_address + shared_affine_state_offset;
					for (idx_t lane_idx = 1; lane_idx < lane_count; lane_idx++) {
						Store<int64_t>(value, state_base);
						Store<uint64_t>(canonical_is_set_word, state_base + sizeof(int64_t));
						state_base += shared_affine_state_stride;
						value += value_step;
					}
					Store<int64_t>(value, state_base);
					Store<uint64_t>(canonical_is_set_word, state_base + sizeof(int64_t));
					D_ASSERT(value == progression_last_value);
					continue;
				}
			}
		}
		for (idx_t lane_idx = 0; lane_idx < lane_count; lane_idx++) {
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
						const auto shared_value = wide_shared_value ? shared_hugeint_values[row_idx]
						                                            : hugeint_t(shared_int64_values[row_idx]);
						const auto wide_value =
						    shared_value * hugeint_t(affine_lane.scale) +
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
						        : (wide_shared_value ? shared_hugeint_values[row_idx]
						                             : hugeint_t(shared_int64_values[row_idx])) *
						                  hugeint_t(affine_lane.scale) +
						              hugeint_t(NumericCast<int64_t>(valid_count)) * hugeint_t(affine_lane.offset);
						*sum = NumericCast<int64_t>(hugeint_t(*sum) + wide_value);
					}
					auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
					*state_is_set = true;
				}
			} else {
				const auto value =
				    (wide_shared_value ? shared_hugeint_values[row_idx] : hugeint_t(shared_int64_values[row_idx])) *
				        hugeint_t(affine_lane.scale) +
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
	if (ExecuteSljitGeneratedPreaggregatedPrimitiveUpdate(addresses, address_sel, execute_sel, count, initialize_states,
	                                                      state)) {
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

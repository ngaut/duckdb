//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_pending_preaggregation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_preaggregation_common_runtime.hpp"
#include "sljit_pending_preaggregated_group_batch_runtime.hpp"

namespace duckdb {

template <class TARGET_TYPE>
static bool SljitPendingPreaggregatedPrimitiveLastGroupMatches(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                               TARGET_TYPE key) {
	if (pending.Empty() || pending.groups.chunk.ColumnCount() != 1) {
		return false;
	}
	auto data = FlatVector::GetData<TARGET_TYPE>(pending.groups.chunk.data[0]);
	return data[pending.Count() - 1] == key;
}

template <class TARGET_TYPE>
static bool SljitPendingPreaggregatedPrimitiveContinuesTailStep(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                                TARGET_TYPE key) {
	if (pending.Count() < 2 || !pending.GeneratedAppendProof().groups_strictly_increasing) {
		return true;
	}
	auto data = FlatVector::GetData<TARGET_TYPE>(pending.groups.chunk.data[0]);
	const auto previous_key = data[pending.Count() - 2];
	const auto last_key = data[pending.Count() - 1];
	if (key <= last_key) {
		return true;
	}
	if constexpr (std::is_integral<TARGET_TYPE>::value && !std::is_same<TARGET_TYPE, bool>::value) {
		using UNSIGNED_TARGET_TYPE = typename std::make_unsigned<TARGET_TYPE>::type;
		const auto previous_step =
		    static_cast<UNSIGNED_TARGET_TYPE>(last_key) - static_cast<UNSIGNED_TARGET_TYPE>(previous_key);
		const auto next_step = static_cast<UNSIGNED_TARGET_TYPE>(key) - static_cast<UNSIGNED_TARGET_TYPE>(last_key);
		return next_step == previous_step;
	} else {
		return true;
	}
}

template <class TARGET_TYPE>
static bool SljitAppendPendingPreaggregatedPrimitiveGroup(
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, TARGET_TYPE key,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, TARGET_TYPE *key_data = nullptr) {
	if (!pending.groups.Initialized() || pending.groups.chunk.ColumnCount() != 1 ||
	    pending.Count() >= SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY || !pending.EnsureFixedScratch(payload_lanes)) {
		return false;
	}
	pending.InvalidateGeneratedAppendProof();
	const auto group_idx = pending.Count();
	if (!ResetFixedPreaggregatedPrimitiveScratchGroup(pending.scratch, payload_lanes, group_idx)) {
		return false;
	}
	if (!key_data) {
		auto &target = pending.groups.chunk.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		key_data = FlatVector::GetDataMutable<TARGET_TYPE>(target);
	}
	key_data[group_idx] = key;
	pending.count++;
	return true;
}

template <class TARGET_TYPE>
static bool SljitAppendPendingSingleLanePreaggregatedPrimitiveGroup(
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, TARGET_TYPE key,
    const ExecutionPrimitiveAggregateUpdateLane &lane, TARGET_TYPE *key_data = nullptr) {
	if (!pending.groups.Initialized() || pending.groups.chunk.ColumnCount() != 1 ||
	    pending.Count() >= SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY || pending.lanes.size() != 1 ||
	    pending.lanes[0] != &lane || pending.scratch.payloads.size() != 1 ||
	    !pending.scratch.HasFixedCapacity(pending.lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY)) {
		return false;
	}
	pending.InvalidateGeneratedAppendProof();
	auto &payload = pending.scratch.payloads[0];
	if (payload.kind != lane.kind) {
		return false;
	}
	const auto group_idx = pending.Count();
	if (!ResetFixedPreaggregatedPrimitiveScratchGroup(pending.scratch, pending.lanes, group_idx)) {
		return false;
	}
	if (!key_data) {
		auto &target = pending.groups.chunk.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		key_data = FlatVector::GetDataMutable<TARGET_TYPE>(target);
	}
	key_data[group_idx] = key;
	pending.count++;
	return true;
}

template <class TARGET_TYPE>
static bool SljitExecuteBoundGeneratedPrimitiveRunsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    SljitPreaggregatedInputVectorGroupKeySource &group_source,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    idx_t count, bool finish, optional_ptr<bool> deferred_grouped_finish, SljitNativePrimitiveRunInput &native_input,
    SljitNativePrimitiveRunFunction function, const char *path_name, bool shared_affine_output) {
	TARGET_TYPE first_key {};
	if (!pending.Empty()) {
		if (!SljitLoadPreaggregatedInputVectorGroupKey(group_source, 0, first_key)) {
			throw InternalException("SLJIT generated primitive run update lost its proven first group key");
		}
		if (!SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, first_key) &&
		    !SljitPendingPreaggregatedPrimitiveContinuesTailStep<TARGET_TYPE>(pending, first_key)) {
			if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
			                                                   deferred_grouped_finish)) {
				throw InternalException(
				    "SLJIT generated primitive run update could not preserve its progression boundary");
			}
			RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_progression_boundary_flush");
		}
	}
	const bool output_bound = shared_affine_output ? SljitBindGeneratedFusedAffinePrimitiveRunOutput<TARGET_TYPE>(
	                                                     pending.groups.chunk, pending.scratch, pending.Count(),
	                                                     SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY, native_input)
	                                               : SljitBindGeneratedPrimitiveRunOutput<TARGET_TYPE>(
	                                                     pending.groups.chunk, pending.scratch, pending.lanes,
	                                                     pending.generated_run_lane_inputs, pending.Count(),
	                                                     SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY, native_input);
	if (!output_bound) {
		return false;
	}
	pending.BeginGeneratedAppendProof();
	native_input.output_groups_strictly_increasing = pending.GeneratedAppendProof().groups_strictly_increasing ? 1 : 0;
	bool merge_existing_affine_group = false;
	if (!pending.Empty()) {
		if (SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, first_key)) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
			merge_existing_affine_group = shared_affine_output;
		}
	}

	auto generated_start = SljitRegionStageStart(runtime);
	while (native_input.input_offset < native_input.input_count) {
		const auto input_offset_before = native_input.input_offset;
		const auto output_count_before = native_input.output_count;
		hugeint_t existing_affine_value;
		if (merge_existing_affine_group) {
			if (output_count_before == 0 || pending.scratch.shared_int64_values.size() < output_count_before ||
			    pending.scratch.shared_hugeint_values.size() < output_count_before ||
			    pending.scratch.shared_value_is_wide.size() < output_count_before) {
				throw InternalException("SLJIT generated affine run lost its pending boundary delta slot");
			}
			existing_affine_value = SljitLoadSharedAffineValue(pending.scratch, output_count_before - 1);
			pending.scratch.shared_int64_values[output_count_before - 1] = 0;
		}
		function(&native_input);
		if (native_input.input_offset > native_input.input_count ||
		    native_input.output_count > native_input.output_capacity ||
		    (native_input.input_offset == input_offset_before && native_input.output_count == 0)) {
			throw InternalException("SLJIT generated primitive run update violated its streaming ABI");
		}
		if (shared_affine_output) {
			if (pending.scratch.shared_hugeint_values.size() < native_input.output_count ||
			    pending.scratch.shared_int64_values.size() < native_input.output_count ||
			    pending.scratch.shared_value_is_wide.size() < native_input.output_count) {
				throw InternalException("SLJIT generated affine run output is incomplete");
			}
			std::fill(pending.scratch.shared_value_is_wide.begin() + UnsafeNumericCast<int64_t>(output_count_before),
			          pending.scratch.shared_value_is_wide.begin() +
			              UnsafeNumericCast<int64_t>(native_input.output_count),
			          0);
			if (merge_existing_affine_group) {
				const auto boundary_idx = output_count_before - 1;
				if (!SljitStoreSharedAffineValue(pending.scratch, boundary_idx,
				                                 existing_affine_value +
				                                     hugeint_t(pending.scratch.shared_int64_values[boundary_idx]))) {
					throw InternalException("SLJIT generated affine run could not publish its boundary value");
				}
			}
			merge_existing_affine_group = false;
		}
		pending.UpdateGeneratedAppendProof(native_input.output_groups_strictly_increasing != 0);
		pending.count = native_input.output_count;
		pending.represented_row_count += native_input.input_offset - input_offset_before;
		if (native_input.input_offset == native_input.input_count) {
			break;
		}
		if (pending.Empty() || !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending,
		                                                                      grouped_state, deferred_grouped_finish)) {
			throw InternalException("SLJIT generated primitive run update could not flush a full output batch");
		}
		pending.BeginGeneratedAppendProof();
		native_input.output_groups_strictly_increasing = 1;
		const bool rebound = shared_affine_output ? SljitBindGeneratedFusedAffinePrimitiveRunOutput<TARGET_TYPE>(
		                                                pending.groups.chunk, pending.scratch, pending.Count(),
		                                                SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY, native_input)
		                                          : SljitBindGeneratedPrimitiveRunOutput<TARGET_TYPE>(
		                                                pending.groups.chunk, pending.scratch, pending.lanes,
		                                                pending.generated_run_lane_inputs, pending.Count(),
		                                                SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY, native_input);
		if (!rebound) {
			throw InternalException("SLJIT generated primitive run update lost its fixed output binding");
		}
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, path_name, generated_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, path_name, count);
	if (group_source.source->cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "generated_primitive_group_cast.integral_compress", count);
	}
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK, path_name,
	                              count);
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK, path_name,
	                              count);
	if (finish && !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
	                                                             deferred_grouped_finish)) {
		throw InternalException("SLJIT generated primitive run update final flush failed");
	}
	return true;
}

template <class TARGET_TYPE>
static bool TryExecuteGeneratedPrimitiveRunsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    SljitPreaggregatedInputVectorGroupKeySource &group_source,
    SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    idx_t count, bool finish, optional_ptr<bool> deferred_grouped_finish) {
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("generated_pending_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), count);
		}
		return false;
	};
	SljitNativePrimitiveRunInput native_input;
	SljitNativePrimitiveRunFunction function = nullptr;
	const char *blocker = nullptr;
	if (!SljitTryBindGeneratedPrimitiveRunSource(runtime, op.aggregate_update.primitive_run_update, group_source,
	                                             payload_sources, payload_lanes, count,
	                                             pending.generated_run_lane_inputs, native_input, function, blocker)) {
		return reject(blocker ? blocker : "binding");
	}
	if (!SljitExecuteBoundGeneratedPrimitiveRunsIntoPending<TARGET_TYPE>(
	        runtime, scratch, op_idx, op, group_source, payload_lanes, grouped_state, pending, count, finish,
	        deferred_grouped_finish, native_input, function, "generated_pending_primitive_group_runs", false)) {
		return reject("output");
	}
	return true;
}

template <class TARGET_TYPE>
static bool TryExecuteGeneratedFusedAffinePrimitiveRunsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, SljitPreaggregatedInputVectorGroupKeySource &group_source,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    idx_t count, bool finish, optional_ptr<bool> deferred_grouped_finish) {
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("generated_pending_fused_affine_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), count);
		}
		return false;
	};
	SljitPreaggregatedPrimitivePayloadSource payload_source;
	SljitNativePrimitiveRunInput native_input;
	SljitNativePrimitiveRunFunction function = nullptr;
	const char *blocker = nullptr;
	if (!SljitTryBindGeneratedFusedAffinePrimitiveRunSource(runtime, op, input, group_source, payload_source_indices,
	                                                        payload_lanes, count, payload_source, native_input,
	                                                        function, blocker)) {
		return reject(blocker ? blocker : "binding");
	}
	const bool valid_counts_are_row_counts = native_input.payload_validity == nullptr;
	if (!pending.Empty() && pending.scratch.shared_valid_counts_are_row_counts != valid_counts_are_row_counts) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			return reject("valid_count_representation");
		}
	}
	pending.scratch.shared_valid_counts_are_row_counts = valid_counts_are_row_counts;
	if (runtime.TraceRuntime()) {
		if (valid_counts_are_row_counts) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "generated_fused_affine.valid_count_row_alias", count);
		}
		if (op.aggregate_update.fused_affine_run_update.lanes_form_arithmetic_progression) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "generated_fused_affine.lane_progression", count);
		}
	}
	if (!SljitExecuteBoundGeneratedPrimitiveRunsIntoPending<TARGET_TYPE>(
	        runtime, scratch, op_idx, op, group_source, payload_lanes, grouped_state, pending, count, finish,
	        deferred_grouped_finish, native_input, function, "generated_pending_fused_affine_primitive_group_runs",
	        true)) {
		return reject("output");
	}
	return true;
}

struct SljitPendingSingleLaneCountAccumulator {
	bool BindOutput(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
	                const ExecutionPrimitiveAggregateUpdateLane &lane) {
		if ((lane.kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
		     lane.kind != AggregatePrimitiveUpdateKind::COUNT) ||
		    pending.scratch.payloads.size() != 1 ||
		    pending.scratch.payloads[0].int64_values.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY ||
		    pending.scratch.group_row_counts.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY) {
			return false;
		}
		values = pending.scratch.payloads[0].int64_values.data();
		row_counts = pending.scratch.group_row_counts.data();
		return true;
	}

	void StartPendingGroup(idx_t group_idx) const {
		values[group_idx] = 0;
		row_counts[group_idx] = 0;
	}

	struct GroupState {
		int64_t value = 0;
		idx_t row_count = 0;
	};

	void StartGroup(GroupState &state) const {
		state = GroupState();
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		(void)row_idx;
		state.value++;
		state.row_count++;
	}

	void MergeGroupState(idx_t group_idx, const GroupState &state) const {
		values[group_idx] += state.value;
		row_counts[group_idx] += state.row_count;
	}

	int64_t *values = nullptr;
	idx_t *row_counts = nullptr;
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitPendingSingleLaneInt64SumAccumulator {
	SljitPendingSingleLaneInt64SumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;
	int64_t *values = nullptr;
	uint8_t *value_is_set = nullptr;
	idx_t *row_counts = nullptr;

	bool BindOutput(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
	                const ExecutionPrimitiveAggregateUpdateLane &lane) {
		if (lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64 || pending.scratch.payloads.size() != 1) {
			return false;
		}
		auto &payload = pending.scratch.payloads[0];
		if (payload.int64_values.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY ||
		    payload.value_is_set.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY ||
		    pending.scratch.group_row_counts.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY) {
			return false;
		}
		values = payload.int64_values.data();
		value_is_set = payload.value_is_set.data();
		row_counts = pending.scratch.group_row_counts.data();
		return true;
	}

	void StartPendingGroup(idx_t group_idx) const {
		values[group_idx] = 0;
		value_is_set[group_idx] = 0;
		row_counts[group_idx] = 0;
	}

	struct GroupState {
		int64_t value = 0;
		idx_t row_count = 0;
	};

	void StartGroup(GroupState &state) const {
		state = GroupState();
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		const auto source_idx = HAS_SELECTION ? selection->get_index_unsafe(row_idx) : row_idx;
		state.value += SljitPreaggregatedPayloadAsInt64(data[source_idx]);
		state.row_count++;
	}

	void MergeGroupState(idx_t group_idx, const GroupState &state) const {
		values[group_idx] += state.value;
		value_is_set[group_idx] = 1;
		row_counts[group_idx] += state.row_count;
	}
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitPendingSingleLaneHugeintSumAccumulator {
	SljitPendingSingleLaneHugeintSumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;
	hugeint_t *values = nullptr;
	uint8_t *value_is_set = nullptr;
	idx_t *row_counts = nullptr;

	bool BindOutput(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
	                const ExecutionPrimitiveAggregateUpdateLane &lane) {
		if (lane.kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT || pending.scratch.payloads.size() != 1) {
			return false;
		}
		auto &payload = pending.scratch.payloads[0];
		if (payload.hugeint_values.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY ||
		    payload.value_is_set.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY ||
		    pending.scratch.group_row_counts.size() != SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY) {
			return false;
		}
		values = payload.hugeint_values.data();
		value_is_set = payload.value_is_set.data();
		row_counts = pending.scratch.group_row_counts.data();
		return true;
	}

	void StartPendingGroup(idx_t group_idx) const {
		values[group_idx] = 0;
		value_is_set[group_idx] = 0;
		row_counts[group_idx] = 0;
	}

	struct GroupState {
		hugeint_t value = 0;
		idx_t row_count = 0;
	};

	void StartGroup(GroupState &state) const {
		state = GroupState();
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		const auto source_idx = HAS_SELECTION ? selection->get_index_unsafe(row_idx) : row_idx;
		state.value += SljitPreaggregatedPayloadAsHugeint(data[source_idx]);
		state.row_count++;
	}

	void MergeGroupState(idx_t group_idx, const GroupState &state) const {
		values[group_idx] += state.value;
		value_is_set[group_idx] = 1;
		row_counts[group_idx] += state.row_count;
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION, class ACCUMULATOR>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish, ACCUMULATOR accumulator) {
	const auto count = input.size();
	if (payload_lanes.size() != 1 || !payload_lanes[0] || !accumulator.BindOutput(pending, *payload_lanes[0])) {
		return false;
	}
	pending.InvalidateGeneratedAppendProof();
	auto group_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(group_format);
	auto group_sel = group_format.sel;
	auto load_key = [&](idx_t row_idx) {
		const auto source_idx = GROUP_HAS_SELECTION ? group_sel->get_index_unsafe(row_idx) : row_idx;
		auto value = group_data[source_idx];
		if constexpr (CAST_KEY) {
			return static_cast<TARGET_TYPE>(value);
		} else {
			return value;
		}
	};
	TARGET_TYPE active_key {};
	bool has_active_key = false;
	idx_t active_group_idx = DConstants::INVALID_INDEX;
	typename ACCUMULATOR::GroupState active_state;
	auto &pending_key_vector = pending.groups.chunk.data[0];
	pending_key_vector.SetVectorType(VectorType::FLAT_VECTOR);
	auto pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
	auto flush_active_group = [&]() {
		if (has_active_key) {
			accumulator.MergeGroupState(active_group_idx, active_state);
		}
	};
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = load_key(row_idx);
		if (!has_active_key || !(key == active_key)) {
			const bool first_input_run = !has_active_key;
			flush_active_group();
			if (!first_input_run || !SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, key)) {
				if (pending.Count() == SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY &&
				    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
				                                                   deferred_grouped_finish)) {
					throw InternalException("SLJIT proven pending preaggregated group flush failed");
				}
				if (pending.Empty()) {
					pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
				}
				active_group_idx = pending.Count();
				pending_key_data[active_group_idx] = key;
				accumulator.StartPendingGroup(active_group_idx);
				pending.count++;
			} else {
				RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
				active_group_idx = pending.Count() - 1;
			}
			active_key = key;
			has_active_key = true;
			accumulator.StartGroup(active_state);
		}
		accumulator.AccumulateGroupState(active_state, row_idx);
	}
	flush_active_group();
	pending.represented_row_count += count;
	if (finish) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			throw InternalException("SLJIT proven pending preaggregated group flush failed");
		}
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayloadSelection(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	return SljitSelectPreaggregatedSingleLaneAccumulator<SljitPendingSingleLaneInt64SumAccumulator,
	                                                     SljitPendingSingleLaneHugeintSumAccumulator,
	                                                     SljitPendingSingleLaneCountAccumulator>(
	    payload_sources, payload_lanes, [&](auto &accumulator) {
		    return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE,
		                                                                               CAST_KEY, GROUP_HAS_SELECTION>(
		        runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		        deferred_grouped_finish, accumulator);
	    });
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayload(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	if (group_format.sel->IsSet()) {
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayloadSelection<
		    TARGET_TYPE, SOURCE_TYPE, CAST_KEY, true>(runtime, scratch, op_idx, op, input, group_format,
		                                              payload_sources, payload_lanes, grouped_state, pending, finish,
		                                              deferred_grouped_finish);
	}
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayloadSelection<
	    TARGET_TYPE, SOURCE_TYPE, CAST_KEY, false>(runtime, scratch, op_idx, op, input, group_format, payload_sources,
	                                               payload_lanes, grouped_state, pending, finish,
	                                               deferred_grouped_finish);
}

template <class TARGET_TYPE, class LOAD_KEY>
static bool SljitReplayInputVectorPrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    idx_t count, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish, LOAD_KEY &&load_key) {
	const bool use_single_lane = payload_lanes.size() == 1 && payload_lanes[0];
	TARGET_TYPE active_key {};
	bool has_active_key = false;
	auto &pending_key_vector = pending.groups.chunk.data[0];
	pending_key_vector.SetVectorType(VectorType::FLAT_VECTOR);
	auto pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		TARGET_TYPE key;
		if (!load_key(row_idx, key)) {
			throw InternalException("SLJIT proven input-vector group key replay failed");
		}
		if (!has_active_key || !(key == active_key)) {
			const bool first_input_run = !has_active_key;
			if (!first_input_run || !SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, key)) {
				if (pending.Count() == SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY &&
				    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
				                                                   deferred_grouped_finish)) {
					throw InternalException("SLJIT proven pending preaggregated group flush failed");
				}
				if (pending.Empty()) {
					pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
				}
				if (use_single_lane) {
					if (!SljitAppendPendingSingleLanePreaggregatedPrimitiveGroup<TARGET_TYPE>(
					        pending, key, *payload_lanes[0], pending_key_data)) {
						throw InternalException("SLJIT proven pending preaggregated group append failed");
					}
				} else if (!SljitAppendPendingPreaggregatedPrimitiveGroup<TARGET_TYPE>(pending, key, payload_lanes,
				                                                                       pending_key_data)) {
					throw InternalException("SLJIT proven pending preaggregated group append failed");
				}
			} else {
				RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
			}
			active_key = key;
			has_active_key = true;
		}
		const auto group_idx = pending.Count() - 1;
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, pending.scratch, payload_lanes, row_idx,
		                                                       group_idx)) {
			throw InternalException("SLJIT proven input-vector payload replay failed");
		}
		pending.represented_row_count++;
	}
	if (finish) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			throw InternalException("SLJIT proven pending preaggregated group flush failed");
		}
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithKeyData(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	const auto count = input.size();
	auto group_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(group_format);
	auto group_sel = group_format.sel;
	auto load_key = [&](idx_t row_idx) {
		auto value = group_data[group_sel->get_index(row_idx)];
		if constexpr (CAST_KEY) {
			return static_cast<TARGET_TYPE>(value);
		} else {
			return value;
		}
	};
	const bool use_single_lane = payload_lanes.size() == 1 && payload_lanes[0];
	if (use_single_lane &&
	    TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayload<TARGET_TYPE, SOURCE_TYPE,
	                                                                                   CAST_KEY>(
	        runtime, scratch, op_idx, op, input, group_format, payload_sources, payload_lanes, grouped_state, pending,
	        finish, deferred_grouped_finish)) {
		return true;
	}
	auto replay_key = [&](idx_t row_idx, TARGET_TYPE &key) {
		key = load_key(row_idx);
		return true;
	};
	return SljitReplayInputVectorPrimitiveGroupsIntoPending<TARGET_TYPE>(
	    runtime, scratch, op_idx, op, count, payload_sources, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, replay_key);
}

struct SljitPendingPreaggregationKeyDispatch {
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	idx_t op_idx;
	SljitExecutableRegionOp &op;
	DataChunk &input;
	SljitPreaggregatedPrimitivePayloadSources &payload_sources;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	ExecutionGroupedAggregateStateAddressBinding &grouped_state;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;
	bool finish;
	optional_ptr<bool> deferred_grouped_finish;

	template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
	bool Execute(UnifiedVectorFormat &group_format) {
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithKeyData<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    runtime, scratch, op_idx, op, input, group_format, payload_sources, payload_lanes, grouped_state, pending,
		    finish, deferred_grouped_finish);
	}
};

template <class TARGET_TYPE>
static bool
SljitTryPreparePendingPrimitiveRunInput(ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op, DataChunk &input,
                                        const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                        const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                        SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                        SljitPreaggregatedInputVectorGroupKeySource &group_source,
                                        SljitPreaggregatedPrimitivePayloadLayout payload_layout, const char *&blocker) {
	blocker = nullptr;
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (input.size() < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		blocker = "shape";
		return false;
	}
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source)) {
		blocker = "group_prepare";
		return false;
	}
	if (!SljitPreaggregatedInputVectorGroupRowsAllValid(group_source)) {
		blocker = "group_null";
		return false;
	}
	if (!SljitPreaggregatedInputVectorGroupKeyCastReplayable(group_source)) {
		blocker = "group_cast";
		return false;
	}
	if (pending.run_strategy == SljitPendingRunStrategy::BUFFERED) {
		blocker = "buffered_strategy";
		return false;
	}
	const auto &equivalence_type = SljitGroupKeyEquivalenceType(group_sources[0]);
	if (!pending.groups.Initialized()) {
		pending.groups.Ensure(runtime.GetAllocator(), vector<LogicalType> {equivalence_type});
	}
	if (pending.groups.chunk.ColumnCount() != 1 || pending.groups.chunk.data[0].GetType() != equivalence_type) {
		blocker = "group_output";
		return false;
	}
	if (pending.run_strategy == SljitPendingRunStrategy::UNDECIDED) {
		auto load_key = [&](idx_t row_idx, TARGET_TYPE &key) {
			return SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key);
		};
		bool profitable_pending_runs;
		if (!SljitTryInputVectorHasProfitablePrimitiveRuns<TARGET_TYPE>(input.size(), load_key,
		                                                                profitable_pending_runs)) {
			blocker = "sample";
			return false;
		}
		pending.run_strategy =
		    profitable_pending_runs ? SljitPendingRunStrategy::STREAMING : SljitPendingRunStrategy::BUFFERED;
		if (!profitable_pending_runs) {
			blocker = "economics";
			return false;
		}
	}
	if (!pending.EnsureFixedScratch(payload_lanes, payload_layout)) {
		blocker = "scratch";
		return false;
	}
	return true;
}

template <class TARGET_TYPE>
static bool TryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPendingTemplated(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("pending_fused_affine_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), input.size());
		}
		return false;
	};
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	const char *blocker = nullptr;
	if (!SljitTryPreparePendingPrimitiveRunInput<TARGET_TYPE>(
	        runtime, op, input, group_sources, payload_lanes, pending, group_source,
	        SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE, blocker)) {
		return reject(blocker ? blocker : "binding");
	}
	if (!TryExecuteGeneratedFusedAffinePrimitiveRunsIntoPending<TARGET_TYPE>(
	        runtime, scratch, op_idx, op, input, group_source, payload_source_indices, payload_lanes, grouped_state,
	        pending, input.size(), finish, deferred_grouped_finish)) {
		pending.InvalidateGeneratedAppendProof();
		return reject("generated");
	}
	return true;
}

template <class TARGET_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingTemplated(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	const auto count = input.size();
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("pending_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), count);
		}
		return false;
	};
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	const char *blocker = nullptr;
	if (!SljitTryPreparePendingPrimitiveRunInput<TARGET_TYPE>(
	        runtime, op, input, group_sources, payload_lanes, pending, group_source,
	        SljitPreaggregatedPrimitivePayloadLayout::PER_LANE, blocker)) {
		return reject(blocker ? blocker : "binding");
	}
	auto &payload_sources = pending.payload_sources;
	if (!payload_sources.Prepare(input, payload_source_indices, payload_lanes)) {
		return reject("payload_prepare");
	}
	if (!SljitPrimitiveAggregateLanesReplayable(payload_lanes)) {
		return reject("payload_replay");
	}
	if (TryExecuteGeneratedPrimitiveRunsIntoPending<TARGET_TYPE>(runtime, scratch, op_idx, op, group_source,
	                                                             payload_sources, payload_lanes, grouped_state, pending,
	                                                             count, finish, deferred_grouped_finish)) {
		return true;
	}
	pending.InvalidateGeneratedAppendProof();
	SljitPendingPreaggregationKeyDispatch dispatch {runtime,
	                                                scratch,
	                                                op_idx,
	                                                op,
	                                                input,
	                                                payload_sources,
	                                                payload_lanes,
	                                                grouped_state,
	                                                pending,
	                                                finish,
	                                                deferred_grouped_finish};
	if (SljitDispatchPreaggregatedInputVectorGroupKeyCast<TARGET_TYPE>(group_source, dispatch)) {
		return true;
	}
	auto load_replay_key = [&](idx_t row_idx, TARGET_TYPE &key) {
		return SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key);
	};
	return SljitReplayInputVectorPrimitiveGroupsIntoPending<TARGET_TYPE>(
	    runtime, scratch, op_idx, op, count, payload_sources, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, load_replay_key);
}

struct SljitPendingPreaggregationTargetDispatch {
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	idx_t op_idx;
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	ExecutionGroupedAggregateStateAddressBinding &grouped_state;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;
	bool finish;
	optional_ptr<bool> deferred_grouped_finish;

	template <class TARGET_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingTemplated<TARGET_TYPE>(
		    runtime, scratch, op_idx, op, input, group_sources, payload_source_indices, payload_lanes, grouped_state,
		    pending, finish, deferred_grouped_finish);
	}
};

struct SljitPendingFusedAffinePreaggregationTargetDispatch {
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	idx_t op_idx;
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	ExecutionGroupedAggregateStateAddressBinding &grouped_state;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;
	bool finish;
	optional_ptr<bool> deferred_grouped_finish;

	template <class TARGET_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPendingTemplated<TARGET_TYPE>(
		    runtime, scratch, op_idx, op, input, group_sources, payload_source_indices, payload_lanes, grouped_state,
		    pending, finish, deferred_grouped_finish);
	}
};

static bool TryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	if (group_sources.size() != 1 || !op.aggregate_update.fused_affine_run_update.Ready()) {
		return false;
	}
	SljitPendingFusedAffinePreaggregationTargetDispatch dispatch {runtime,
	                                                              scratch,
	                                                              op_idx,
	                                                              op,
	                                                              input,
	                                                              group_sources,
	                                                              payload_source_indices,
	                                                              payload_lanes,
	                                                              grouped_state,
	                                                              pending,
	                                                              finish,
	                                                              deferred_grouped_finish};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(SljitGroupKeyEquivalencePhysicalType(group_sources[0]),
	                                                            dispatch);
}

// A false result is an admission miss: no row from this input batch has been
// published into pending storage. Validated failures after publication throw.
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	if (group_sources.size() != 1) {
		return false;
	}
	SljitPendingPreaggregationTargetDispatch dispatch {runtime,
	                                                   scratch,
	                                                   op_idx,
	                                                   op,
	                                                   input,
	                                                   group_sources,
	                                                   payload_source_indices,
	                                                   payload_lanes,
	                                                   grouped_state,
	                                                   pending,
	                                                   finish,
	                                                   deferred_grouped_finish};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(SljitGroupKeyEquivalencePhysicalType(group_sources[0]),
	                                                            dispatch);
}

} // namespace duckdb

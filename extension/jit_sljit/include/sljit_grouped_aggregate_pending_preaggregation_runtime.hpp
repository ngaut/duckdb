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

static constexpr idx_t SLJIT_PENDING_RUN_MIN_COMPRESSION = 3;

template <class TARGET_TYPE, class LOAD_KEY>
static bool SljitTryInputVectorHasProfitablePendingRuns(idx_t count, LOAD_KEY &&load_key, bool &profitable) {
	profitable = false;
	if (count < 2) {
		return true;
	}
	TARGET_TYPE previous_key;
	if (!load_key(0, previous_key)) {
		return false;
	}
	const auto sample_count = MinValue<idx_t>(count, 64);
	idx_t transition_count = 0;
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		TARGET_TYPE key;
		if (!load_key(row_idx, key)) {
			return false;
		}
		if (!(key == previous_key)) {
			transition_count++;
		}
		previous_key = key;
	}
	// Transitions measure the distance between run starts without charging the
	// sample's partial leading and trailing runs as complete groups.
	profitable = transition_count == 0 || transition_count * SLJIT_PENDING_RUN_MIN_COMPRESSION <= sample_count - 1;
	return true;
}

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
static bool SljitAppendPendingPreaggregatedPrimitiveGroup(
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, TARGET_TYPE key,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, TARGET_TYPE *key_data = nullptr) {
	if (!pending.groups.Initialized() || pending.groups.chunk.ColumnCount() != 1 ||
	    pending.Count() >= SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY || !pending.EnsureFixedScratch(payload_lanes)) {
		return false;
	}
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
static bool SljitBindGeneratedPrimitiveRunOutput(SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                 const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                 SljitNativePrimitiveRunInput &native_input) {
	if (!pending.groups.Initialized() || pending.groups.chunk.ColumnCount() != 1 ||
	    pending.Count() > SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY || pending.lanes.size() != 1 ||
	    pending.lanes[0] != &lane || pending.scratch.payloads.size() != 1 ||
	    !pending.scratch.HasFixedCapacity(pending.lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY)) {
		return false;
	}
	auto &group_vector = pending.groups.chunk.data[0];
	group_vector.SetVectorType(VectorType::FLAT_VECTOR);
	native_input.output_group_data =
	    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<TARGET_TYPE>(group_vector));
	native_input.output_int64_values = nullptr;
	native_input.output_hugeint_values = nullptr;
	native_input.output_value_is_set = nullptr;
	native_input.output_row_counts = pending.scratch.group_row_counts.data();
	auto &payload = pending.scratch.payloads[0];
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		native_input.output_int64_values = payload.int64_values.data();
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		native_input.output_hugeint_values = payload.hugeint_values.data();
		break;
	default:
		return false;
	}
	if (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    lane.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		native_input.output_value_is_set = payload.value_is_set.data();
	}
	native_input.output_count = pending.Count();
	native_input.output_capacity = SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY;
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
	auto &run_update = op.aggregate_update.primitive_run_update;
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("generated_pending_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), count);
		}
		return false;
	};
	if (!run_update.IsExecutable()) {
		return reject("code");
	}
	if (count == 0 || payload_lanes.size() != 1 || !payload_lanes[0] || !group_source.source) {
		return reject("shape");
	}
	const bool exact_group_type =
	    group_source.source->cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
	    group_source.source->source_physical_type == group_source.source->target_physical_type;
	const bool proven_narrowing_group_cast = SljitGroupKeyNarrowingIntegralCast(group_source.source->cast_kind) &&
	                                         group_source.source->unchecked_integral_cast;
	if (!exact_group_type && !proven_narrowing_group_cast) {
		return reject("group_cast");
	}
	if (group_source.source->target_physical_type != run_update.group_type) {
		return reject("group_type");
	}
	if (group_source.format.sel->IsSet()) {
		return reject("group_selection");
	}
	if (!group_source.source->all_valid && group_source.format.validity.CanHaveNull()) {
		return reject("group_null");
	}
	auto &lane = *payload_lanes[0];
	PhysicalType payload_type = PhysicalType::INVALID;
	const_data_ptr_t payload_data = nullptr;
	if (lane.kind == AggregatePrimitiveUpdateKind::COUNT) {
		if (payload_sources.SourceCanHaveNull(0)) {
			return reject("payload_null");
		}
	} else if (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	           lane.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		auto source = payload_sources.GetSource(0);
		if (!source || payload_sources.SourceCanHaveNull(0)) {
			return reject("payload_null");
		}
		if (source->format.sel->IsSet()) {
			return reject("payload_selection");
		}
		payload_type = source->type;
		payload_data = source->format.data;
	} else if (lane.kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return reject("payload_kind");
	}
	if (run_update.group_type != group_source.source->target_physical_type || run_update.primitive_kind != lane.kind ||
	    run_update.payload_type != payload_type) {
		return reject("specialization");
	}
	auto function = run_update.Function(group_source.source->source_physical_type);
	if (!function) {
		return reject("specialization");
	}

	SljitNativePrimitiveRunInput native_input;
	native_input.group_data = group_source.format.data;
	native_input.payload_data = payload_data;
	native_input.input_count = count;
	if (!SljitBindGeneratedPrimitiveRunOutput<TARGET_TYPE>(pending, lane, native_input)) {
		return reject("output");
	}
	if (!pending.Empty()) {
		TARGET_TYPE first_key;
		if (!SljitLoadPreaggregatedInputVectorGroupKey(group_source, 0, first_key)) {
			throw InternalException("SLJIT generated primitive run update lost its proven first group key");
		}
		if (SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, first_key)) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
		}
	}

	auto generated_start = SljitRegionStageStart(runtime);
	while (native_input.input_offset < native_input.input_count) {
		const auto input_offset_before = native_input.input_offset;
		function(&native_input);
		if (native_input.input_offset > native_input.input_count ||
		    native_input.output_count > native_input.output_capacity ||
		    (native_input.input_offset == input_offset_before && native_input.output_count == 0)) {
			throw InternalException("SLJIT generated primitive run update violated its streaming ABI");
		}
		pending.count = native_input.output_count;
		pending.represented_row_count += native_input.input_offset - input_offset_before;
		if (native_input.input_offset == native_input.input_count) {
			break;
		}
		if (pending.Empty() || !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending,
		                                                                      grouped_state, deferred_grouped_finish)) {
			throw InternalException("SLJIT generated primitive run update could not flush a full output batch");
		}
		if (!SljitBindGeneratedPrimitiveRunOutput<TARGET_TYPE>(pending, lane, native_input)) {
			throw InternalException("SLJIT generated primitive run update lost its fixed output binding");
		}
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "generated_pending_primitive_group_runs", generated_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "generated_pending_primitive_group_runs", count);
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_STAGE_WORK,
	                              "generated_pending_primitive_group_runs", count);
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK,
	                              "generated_pending_primitive_group_runs", count);
	if (finish && !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
	                                                             deferred_grouped_finish)) {
		throw InternalException("SLJIT generated primitive run update final flush failed");
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

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION, class PAYLOAD_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithInt64Payload(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish, const SljitPreaggregatedPrimitivePayloadSource &source) {
	auto data = UnifiedVectorFormat::GetData<PAYLOAD_TYPE>(source.format);
	auto selection = source.format.sel;
	if (selection->IsSet()) {
		SljitPendingSingleLaneInt64SumAccumulator<PAYLOAD_TYPE, true> accumulator {data, selection};
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                           GROUP_HAS_SELECTION>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	SljitPendingSingleLaneInt64SumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
	                                                                           GROUP_HAS_SELECTION>(
	    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION, class PAYLOAD_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithHugeintPayload(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish, const SljitPreaggregatedPrimitivePayloadSource &source) {
	auto data = UnifiedVectorFormat::GetData<PAYLOAD_TYPE>(source.format);
	auto selection = source.format.sel;
	if (selection->IsSet()) {
		SljitPendingSingleLaneHugeintSumAccumulator<PAYLOAD_TYPE, true> accumulator {data, selection};
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                           GROUP_HAS_SELECTION>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	SljitPendingSingleLaneHugeintSumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
	                                                                           GROUP_HAS_SELECTION>(
	    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
struct SljitPendingInt64PayloadDispatch {
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	idx_t op_idx;
	SljitExecutableRegionOp &op;
	DataChunk &input;
	UnifiedVectorFormat &group_format;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	ExecutionGroupedAggregateStateAddressBinding &grouped_state;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;
	bool finish;
	optional_ptr<bool> deferred_grouped_finish;
	const SljitPreaggregatedPrimitivePayloadSource &source;

	template <class PAYLOAD_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithInt64Payload<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                            GROUP_HAS_SELECTION, PAYLOAD_TYPE>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
struct SljitPendingHugeintPayloadDispatch {
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	idx_t op_idx;
	SljitExecutableRegionOp &op;
	DataChunk &input;
	UnifiedVectorFormat &group_format;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	ExecutionGroupedAggregateStateAddressBinding &grouped_state;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;
	bool finish;
	optional_ptr<bool> deferred_grouped_finish;
	const SljitPreaggregatedPrimitivePayloadSource &source;

	template <class PAYLOAD_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithHugeintPayload<
		    TARGET_TYPE, SOURCE_TYPE, CAST_KEY, GROUP_HAS_SELECTION, PAYLOAD_TYPE>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayloadSelection(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	if (payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	auto &lane = *payload_lanes[0];
	if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    (lane.kind == AggregatePrimitiveUpdateKind::COUNT && !payload_sources.SourceCanHaveNull(0))) {
		SljitPendingSingleLaneCountAccumulator accumulator;
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                           GROUP_HAS_SELECTION>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	auto source = payload_sources.GetSource(0);
	if (!source || payload_sources.SourceCanHaveNull(0)) {
		return false;
	}
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64: {
		SljitPendingInt64PayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, GROUP_HAS_SELECTION> dispatch {
		    runtime,
		    scratch,
		    op_idx,
		    op,
		    input,
		    group_format,
		    payload_lanes,
		    grouped_state,
		    pending,
		    finish,
		    deferred_grouped_finish,
		    *source};
		return SljitDispatchPreaggregatedInt64PayloadType(source->type, dispatch);
	}
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
		SljitPendingHugeintPayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, GROUP_HAS_SELECTION> dispatch {
		    runtime,
		    scratch,
		    op_idx,
		    op,
		    input,
		    group_format,
		    payload_lanes,
		    grouped_state,
		    pending,
		    finish,
		    deferred_grouped_finish,
		    *source};
		return SljitDispatchPreaggregatedHugeintPayloadType(source->type, dispatch);
	}
	default:
		return false;
	}
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
					return false;
				}
				if (pending.Empty()) {
					pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
				}
				if (use_single_lane) {
					if (!SljitAppendPendingSingleLanePreaggregatedPrimitiveGroup<TARGET_TYPE>(
					        pending, key, *payload_lanes[0], pending_key_data)) {
						return false;
					}
				} else if (!SljitAppendPendingPreaggregatedPrimitiveGroup<TARGET_TYPE>(pending, key, payload_lanes,
				                                                                       pending_key_data)) {
					return false;
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
			return false;
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
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingTemplated(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish) {
	const auto count = input.size();
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto reject = [&](const char *blocker) {
		if (runtime.TraceRuntime()) {
			auto path = string("pending_primitive_group_runs_miss.") + blocker;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), count);
		}
		return false;
	};
	if (count < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		return reject("shape");
	}
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source)) {
		return reject("group_prepare");
	}
	if (!SljitPreaggregatedInputVectorGroupKeyReplayable(group_source)) {
		return reject("group_replay");
	}
	auto load_key = [&](idx_t row_idx, TARGET_TYPE &key) {
		return SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key);
	};
	if (pending.run_strategy == SljitPendingRunStrategy::BUFFERED) {
		return reject("buffered_strategy");
	}
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(input, payload_source_indices, payload_lanes)) {
		return reject("payload_prepare");
	}
	if (!SljitPreaggregatedPayloadSourcesReplayable(payload_sources, payload_lanes)) {
		return reject("payload_replay");
	}
	if (!pending.groups.Initialized()) {
		pending.groups.Ensure(runtime.GetAllocator(), vector<LogicalType> {sink_info.groups[0].type});
	}
	if (pending.groups.chunk.ColumnCount() != 1 || pending.groups.chunk.data[0].GetType() != sink_info.groups[0].type) {
		return reject("group_output");
	}
	if (pending.run_strategy == SljitPendingRunStrategy::UNDECIDED) {
		bool profitable_pending_runs;
		if (!SljitTryInputVectorHasProfitablePendingRuns<TARGET_TYPE>(count, load_key, profitable_pending_runs)) {
			return reject("sample");
		}
		pending.run_strategy =
		    profitable_pending_runs ? SljitPendingRunStrategy::STREAMING : SljitPendingRunStrategy::BUFFERED;
		if (!profitable_pending_runs) {
			return reject("economics");
		}
	}
	if (!pending.EnsureFixedScratch(payload_lanes)) {
		return reject("scratch");
	}
	if (TryExecuteGeneratedPrimitiveRunsIntoPending<TARGET_TYPE>(runtime, scratch, op_idx, op, group_source,
	                                                             payload_sources, payload_lanes, grouped_state, pending,
	                                                             count, finish, deferred_grouped_finish)) {
		return true;
	}
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
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_sources[0].target_physical_type, dispatch);
}

} // namespace duckdb

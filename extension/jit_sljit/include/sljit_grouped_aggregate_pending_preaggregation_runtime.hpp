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
	    pending.Count() >= STANDARD_VECTOR_SIZE) {
		return false;
	}
	if (!SljitStartPreaggregatedPrimitivePayloadGroup(pending.scratch, payload_lanes)) {
		return false;
	}
	if (!key_data) {
		auto &target = pending.groups.chunk.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		key_data = FlatVector::GetDataMutable<TARGET_TYPE>(target);
	}
	key_data[pending.Count()] = key;
	pending.count++;
	return true;
}

template <class TARGET_TYPE>
static bool SljitAppendPendingSingleLanePreaggregatedPrimitiveGroup(
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, TARGET_TYPE key,
    const ExecutionPrimitiveAggregateUpdateLane &lane, TARGET_TYPE *key_data = nullptr) {
	if (!pending.groups.Initialized() || pending.groups.chunk.ColumnCount() != 1 ||
	    pending.Count() >= STANDARD_VECTOR_SIZE || pending.scratch.payloads.size() != 1) {
		return false;
	}
	auto &payload = pending.scratch.payloads[0];
	if (payload.kind != lane.kind) {
		return false;
	}
	pending.scratch.group_row_counts.push_back(0);
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		payload.int64_values.push_back(0);
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		payload.hugeint_values.emplace_back(0);
		break;
	default:
		return false;
	}
	if (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    lane.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		payload.value_is_set.push_back(0);
	}
	if (!key_data) {
		auto &target = pending.groups.chunk.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		key_data = FlatVector::GetDataMutable<TARGET_TYPE>(target);
	}
	key_data[pending.Count()] = key;
	pending.count++;
	return true;
}

struct SljitPendingSingleLaneCountAccumulator {
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

	bool MergeGroupState(SljitPendingPreaggregatedPrimitiveGroupBatch &pending, idx_t group_idx,
	                     const GroupState &state) const {
		if (pending.scratch.payloads.size() != 1 || group_idx >= pending.scratch.group_row_counts.size()) {
			return false;
		}
		pending.scratch.payloads[0].int64_values[group_idx] += state.value;
		pending.scratch.group_row_counts[group_idx] += state.row_count;
		return true;
	}
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitPendingSingleLaneInt64SumAccumulator {
	SljitPendingSingleLaneInt64SumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;

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

	bool MergeGroupState(SljitPendingPreaggregatedPrimitiveGroupBatch &pending, idx_t group_idx,
	                     const GroupState &state) const {
		if (pending.scratch.payloads.size() != 1 || group_idx >= pending.scratch.group_row_counts.size()) {
			return false;
		}
		auto &payload = pending.scratch.payloads[0];
		payload.int64_values[group_idx] += state.value;
		payload.value_is_set[group_idx] = 1;
		pending.scratch.group_row_counts[group_idx] += state.row_count;
		return true;
	}
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitPendingSingleLaneHugeintSumAccumulator {
	SljitPendingSingleLaneHugeintSumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;

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

	bool MergeGroupState(SljitPendingPreaggregatedPrimitiveGroupBatch &pending, idx_t group_idx,
	                     const GroupState &state) const {
		if (pending.scratch.payloads.size() != 1 || group_idx >= pending.scratch.group_row_counts.size()) {
			return false;
		}
		auto &payload = pending.scratch.payloads[0];
		payload.hugeint_values[group_idx] += state.value;
		payload.value_is_set[group_idx] = 1;
		pending.scratch.group_row_counts[group_idx] += state.row_count;
		return true;
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class ACCUMULATOR>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish, optional_ptr<bool> deferred_grouped_finish, ACCUMULATOR accumulator) {
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
	TARGET_TYPE active_key {};
	bool has_active_key = false;
	idx_t active_group_idx = DConstants::INVALID_INDEX;
	typename ACCUMULATOR::GroupState active_state;
	auto &pending_key_vector = pending.groups.chunk.data[0];
	pending_key_vector.SetVectorType(VectorType::FLAT_VECTOR);
	auto pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
	auto flush_active_group = [&]() {
		if (!has_active_key) {
			return true;
		}
		return accumulator.MergeGroupState(pending, active_group_idx, active_state);
	};
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = load_key(row_idx);
		if (!has_active_key || !(key == active_key)) {
			const bool first_input_run = !has_active_key;
			if (!flush_active_group()) {
				throw InternalException("SLJIT proven pending preaggregated group merge failed");
			}
			if (!first_input_run || !SljitPendingPreaggregatedPrimitiveLastGroupMatches<TARGET_TYPE>(pending, key)) {
				if (pending.Count() == SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY &&
				    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
				                                                   deferred_grouped_finish)) {
					throw InternalException("SLJIT proven pending preaggregated group flush failed");
				}
				if (pending.Empty()) {
					pending_key_data = FlatVector::GetDataMutable<TARGET_TYPE>(pending_key_vector);
				}
				if (!SljitAppendPendingSingleLanePreaggregatedPrimitiveGroup<TARGET_TYPE>(
				        pending, key, *payload_lanes[0], pending_key_data)) {
					throw InternalException("SLJIT proven pending preaggregated group append failed");
				}
			} else {
				RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
			}
			active_key = key;
			has_active_key = true;
			active_group_idx = pending.Count() - 1;
			accumulator.StartGroup(active_state);
		}
		accumulator.AccumulateGroupState(active_state, row_idx);
	}
	if (!flush_active_group()) {
		throw InternalException("SLJIT proven pending preaggregated group merge failed");
	}
	pending.represented_row_count += count;
	if (finish) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state,
		                                                   deferred_grouped_finish)) {
			throw InternalException("SLJIT proven pending preaggregated group flush failed");
		}
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class PAYLOAD_TYPE>
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
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	SljitPendingSingleLaneInt64SumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
	    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class PAYLOAD_TYPE>
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
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	SljitPendingSingleLaneHugeintSumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
	    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
	    deferred_grouped_finish, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
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
		                                                                            PAYLOAD_TYPE>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
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
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithHugeintPayload<TARGET_TYPE, SOURCE_TYPE,
		                                                                              CAST_KEY, PAYLOAD_TYPE>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithTypedSingleLanePayload(
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
		return TryPreaggregateInputVectorPrimitiveGroupsIntoPendingWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    runtime, scratch, op_idx, op, input, group_format, payload_lanes, grouped_state, pending, finish,
		    deferred_grouped_finish, accumulator);
	}
	auto source = payload_sources.GetSource(0);
	if (!source || payload_sources.SourceCanHaveNull(0)) {
		return false;
	}
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64: {
		SljitPendingInt64PayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY> dispatch {runtime,
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
		SljitPendingHugeintPayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY> dispatch {runtime,
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
	if (count < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		return false;
	}
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source)) {
		return false;
	}
	if (!SljitPreaggregatedInputVectorGroupKeyReplayable(group_source)) {
		return false;
	}
	auto load_key = [&](idx_t row_idx, TARGET_TYPE &key) {
		return SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key);
	};
	if (pending.run_strategy == SljitPendingRunStrategy::BUFFERED) {
		return false;
	}
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(input, payload_source_indices, payload_lanes) ||
	    !SljitPreaggregatedPayloadSourcesReplayable(payload_sources, payload_lanes)) {
		return false;
	}
	if (!pending.groups.Initialized()) {
		pending.groups.Ensure(runtime.GetAllocator(), vector<LogicalType> {sink_info.groups[0].type});
	}
	if (pending.groups.chunk.ColumnCount() != 1 || pending.groups.chunk.data[0].GetType() != sink_info.groups[0].type) {
		return false;
	}
	if (pending.run_strategy == SljitPendingRunStrategy::UNDECIDED) {
		bool profitable_pending_runs;
		if (!SljitTryInputVectorHasProfitablePendingRuns<TARGET_TYPE>(count, load_key, profitable_pending_runs)) {
			return false;
		}
		pending.run_strategy =
		    profitable_pending_runs ? SljitPendingRunStrategy::STREAMING : SljitPendingRunStrategy::BUFFERED;
		if (!profitable_pending_runs) {
			return false;
		}
	}
	if (pending.Empty()) {
		pending.lanes = payload_lanes;
		pending.scratch.Prepare(payload_lanes, STANDARD_VECTOR_SIZE);
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_run_preaggregation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_source_indices.hpp"
#include "sljit_grouped_aggregate_preaggregation_common_runtime.hpp"

namespace duckdb {

struct SljitRunSingleLaneCountAccumulator {
	struct GroupState {
		int64_t value = 0;
		idx_t row_count = 0;
	};

	void StartGroup(GroupState &state) const {
		state.value = 0;
		state.row_count = 0;
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		(void)row_idx;
		state.value++;
		state.row_count++;
	}

	void FlushGroupState(SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx,
	                     const GroupState &state) const {
		scratch.payloads[0].int64_values[group_idx] = state.value;
		scratch.group_row_counts[group_idx] = state.row_count;
	}
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitRunSingleLaneInt64SumAccumulator {
	SljitRunSingleLaneInt64SumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;

	struct GroupState {
		int64_t value = 0;
		idx_t row_count = 0;
		uint8_t value_is_set = 0;
	};

	void StartGroup(GroupState &state) const {
		state.value = 0;
		state.row_count = 0;
		state.value_is_set = 0;
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		const auto source_idx = HAS_SELECTION ? selection->get_index_unsafe(row_idx) : row_idx;
		state.value += SljitPreaggregatedPayloadAsInt64(data[source_idx]);
		state.row_count++;
		state.value_is_set = 1;
	}

	void FlushGroupState(SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx,
	                     const GroupState &state) const {
		auto &payload = scratch.payloads[0];
		payload.int64_values[group_idx] = state.value;
		payload.value_is_set[group_idx] = state.value_is_set;
		scratch.group_row_counts[group_idx] = state.row_count;
	}
};

template <class PAYLOAD_TYPE, bool HAS_SELECTION>
struct SljitRunSingleLaneHugeintSumAccumulator {
	SljitRunSingleLaneHugeintSumAccumulator(const PAYLOAD_TYPE *data_p, const SelectionVector *selection_p)
	    : data(data_p), selection(selection_p) {
	}

	const PAYLOAD_TYPE *data;
	const SelectionVector *selection;

	struct GroupState {
		hugeint_t value = 0;
		idx_t row_count = 0;
		uint8_t value_is_set = 0;
	};

	void StartGroup(GroupState &state) const {
		state.value = 0;
		state.row_count = 0;
		state.value_is_set = 0;
	}

	void AccumulateGroupState(GroupState &state, idx_t row_idx) const {
		const auto source_idx = HAS_SELECTION ? selection->get_index_unsafe(row_idx) : row_idx;
		state.value += SljitPreaggregatedPayloadAsHugeint(data[source_idx]);
		state.row_count++;
		state.value_is_set = 1;
	}

	void FlushGroupState(SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t group_idx,
	                     const GroupState &state) const {
		auto &payload = scratch.payloads[0];
		payload.hugeint_values[group_idx] = state.value;
		payload.value_is_set[group_idx] = state.value_is_set;
		scratch.group_row_counts[group_idx] = state.row_count;
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION, class ACCUMULATOR>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulatorSelection(
    DataChunk &input, const SOURCE_TYPE *group_data, const SelectionVector *group_sel,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count,
    ACCUMULATOR accumulator) {
	const auto count = input.size();
	if (count < 2 || payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	TARGET_TYPE *run_key_data = nullptr;
	if (run_group_keys) {
		if (run_group_keys->ColumnCount() != 1) {
			return false;
		}
		run_key_data = PrepareFlatPreaggregatedGroupTarget<TARGET_TYPE>(*run_group_keys);
	}
	auto load_key = [&](idx_t row_idx) {
		const auto source_idx = GROUP_HAS_SELECTION ? group_sel->get_index_unsafe(row_idx) : row_idx;
		auto value = group_data[source_idx];
		if constexpr (CAST_KEY) {
			return static_cast<TARGET_TYPE>(value);
		} else {
			return value;
		}
	};
	if (!SljitInputVectorHasConsecutiveRepeat(count, load_key)) {
		return false;
	}

	scratch.Prepare(payload_lanes, count);
	if (scratch.payloads.size() != 1) {
		return false;
	}
	scratch.group_rows.resize(count);
	scratch.group_row_counts.resize(count);
	auto &payload = scratch.payloads[0];
	switch (payload.kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		payload.int64_values.resize(count);
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		payload.hugeint_values.resize(count);
		break;
	default:
		return false;
	}
	if (payload.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    payload.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		payload.value_is_set.resize(count);
	}
	TARGET_TYPE active_key {};
	bool has_active_key = false;
	typename ACCUMULATOR::GroupState active_state;
	group_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = load_key(row_idx);
		if (!has_active_key || !(key == active_key)) {
			if (has_active_key) {
				accumulator.FlushGroupState(scratch, group_count - 1, active_state);
			}
			const auto group_idx = group_count++;
			scratch.group_rows[group_idx] = static_cast<sel_t>(row_idx);
			active_key = key;
			has_active_key = true;
			accumulator.StartGroup(active_state);
			if (run_key_data) {
				run_key_data[group_idx] = key;
			}
		}
		accumulator.AccumulateGroupState(active_state, row_idx);
	}
	if (has_active_key) {
		accumulator.FlushGroupState(scratch, group_count - 1, active_state);
	}
	if (group_count == 0 || group_count == count) {
		group_count = 0;
		return false;
	}
	scratch.group_rows.resize(group_count);
	scratch.group_row_counts.resize(group_count);
	switch (payload.kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		payload.int64_values.resize(group_count);
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		payload.hugeint_values.resize(group_count);
		break;
	default:
		return false;
	}
	if (payload.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    payload.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		payload.value_is_set.resize(group_count);
	}
	if (run_group_keys) {
		FinishFlatPreaggregatedGroupTarget(*run_group_keys, group_count);
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class ACCUMULATOR>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator(
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count,
    ACCUMULATOR accumulator) {
	auto group_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(group_format);
	auto group_sel = group_format.sel;
	if (group_sel->IsSet()) {
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulatorSelection<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                            true>(
		    input, group_data, group_sel, payload_lanes, scratch, run_group_keys, group_count, accumulator);
	}
	return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulatorSelection<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
	                                                                            false>(
	    input, group_data, group_sel, payload_lanes, scratch, run_group_keys, group_count, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class PAYLOAD_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsWithInt64Payload(
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count,
    const SljitPreaggregatedPrimitivePayloadSource &source) {
	auto data = UnifiedVectorFormat::GetData<PAYLOAD_TYPE>(source.format);
	auto selection = source.format.sel;
	if (selection->IsSet()) {
		SljitRunSingleLaneInt64SumAccumulator<PAYLOAD_TYPE, true> accumulator {data, selection};
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, accumulator);
	}
	SljitRunSingleLaneInt64SumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
	    input, group_format, payload_lanes, scratch, run_group_keys, group_count, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, class PAYLOAD_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsWithHugeintPayload(
    DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count,
    const SljitPreaggregatedPrimitivePayloadSource &source) {
	auto data = UnifiedVectorFormat::GetData<PAYLOAD_TYPE>(source.format);
	auto selection = source.format.sel;
	if (selection->IsSet()) {
		SljitRunSingleLaneHugeintSumAccumulator<PAYLOAD_TYPE, true> accumulator {data, selection};
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, accumulator);
	}
	SljitRunSingleLaneHugeintSumAccumulator<PAYLOAD_TYPE, false> accumulator {data, selection};
	return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
	    input, group_format, payload_lanes, scratch, run_group_keys, group_count, accumulator);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
struct SljitRunInt64PayloadDispatch {
	DataChunk &input;
	UnifiedVectorFormat &group_format;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;
	const SljitPreaggregatedPrimitivePayloadSource &source;

	template <class PAYLOAD_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithInt64Payload<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                    PAYLOAD_TYPE>(
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
struct SljitRunHugeintPayloadDispatch {
	DataChunk &input;
	UnifiedVectorFormat &group_format;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;
	const SljitPreaggregatedPrimitivePayloadSource &source;

	template <class PAYLOAD_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithHugeintPayload<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
		                                                                      PAYLOAD_TYPE>(
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, source);
	}
};

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsWithTypedSingleLanePayload(
    DataChunk &input, UnifiedVectorFormat &group_format, SljitPreaggregatedPrimitivePayloadSources &payload_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	if (payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	auto &lane = *payload_lanes[0];
	if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    (lane.kind == AggregatePrimitiveUpdateKind::COUNT && !payload_sources.SourceCanHaveNull(0))) {
		SljitRunSingleLaneCountAccumulator accumulator;
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithAccumulator<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, accumulator);
	}
	auto source = payload_sources.GetSource(0);
	if (!source || payload_sources.SourceCanHaveNull(0)) {
		return false;
	}
	switch (lane.kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64: {
		SljitRunInt64PayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY> dispatch {
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, *source};
		return SljitDispatchPreaggregatedInt64PayloadType(source->type, dispatch);
	}
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
		SljitRunHugeintPayloadDispatch<TARGET_TYPE, SOURCE_TYPE, CAST_KEY> dispatch {
		    input, group_format, payload_lanes, scratch, run_group_keys, group_count, *source};
		return SljitDispatchPreaggregatedHugeintPayloadType(source->type, dispatch);
	}
	default:
		return false;
	}
}

struct SljitSingleLaneRunPreaggregationKeyDispatch {
	DataChunk &input;
	SljitPreaggregatedPrimitivePayloadSources &payload_sources;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;

	template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
	bool Execute(UnifiedVectorFormat &group_format) {
		return TryPreaggregateInputVectorPrimitiveGroupRunsWithTypedSingleLanePayload<TARGET_TYPE, SOURCE_TYPE,
		                                                                              CAST_KEY>(
		    input, group_format, payload_sources, payload_lanes, scratch, run_group_keys, group_count);
	}
};

template <class TARGET_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsFastTemplated(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (input.size() < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		return false;
	}
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source) ||
	    !SljitPreaggregatedInputVectorGroupKeyReplayable(group_source)) {
		return false;
	}
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(input, payload_source_indices, payload_lanes) ||
	    !SljitPrimitiveAggregateLanesReplayable(payload_lanes)) {
		return false;
	}
	SljitSingleLaneRunPreaggregationKeyDispatch dispatch {input,   payload_sources, payload_lanes,
	                                                      scratch, run_group_keys,  group_count};
	return SljitDispatchPreaggregatedInputVectorGroupKeyCast<TARGET_TYPE>(group_source, dispatch);
}

struct SljitSingleLaneRunPreaggregationTargetDispatch {
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;

	template <class TARGET_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupRunsFastTemplated<TARGET_TYPE>(
		    op, input, group_sources, payload_source_indices, payload_lanes, scratch, run_group_keys, group_count);
	}
};

static bool TryPreaggregateInputVectorPrimitiveGroupRunsFast(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	if (group_sources.size() != 1) {
		return false;
	}
	SljitSingleLaneRunPreaggregationTargetDispatch dispatch {
	    op, input, group_sources, payload_source_indices, payload_lanes, scratch, run_group_keys, group_count};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_sources[0].target_physical_type, dispatch);
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION, class START_GROUP,
          class VISIT_ROW>
static bool SljitBuildInputVectorPrimitiveRuns(idx_t count, const SOURCE_TYPE *group_data,
                                               const SelectionVector *group_sel,
                                               SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                               optional_ptr<DataChunk> run_group_keys, idx_t &group_count,
                                               START_GROUP &&start_group, VISIT_ROW &&visit_row) {
	group_count = 0;
	TARGET_TYPE *run_key_data = nullptr;
	if (run_group_keys) {
		if (run_group_keys->ColumnCount() != 1) {
			return false;
		}
		run_key_data = PrepareFlatPreaggregatedGroupTarget<TARGET_TYPE>(*run_group_keys);
	}
	auto load_key = [&](idx_t row_idx) {
		const auto source_idx = GROUP_HAS_SELECTION ? group_sel->get_index_unsafe(row_idx) : row_idx;
		auto value = group_data[source_idx];
		if constexpr (CAST_KEY) {
			return static_cast<TARGET_TYPE>(value);
		} else {
			return value;
		}
	};
	if (!SljitInputVectorHasConsecutiveRepeat(count, load_key)) {
		return false;
	}

	TARGET_TYPE active_key {};
	bool has_active_key = false;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = load_key(row_idx);
		if (!has_active_key || !(key == active_key)) {
			active_key = key;
			has_active_key = true;
			if (run_key_data) {
				run_key_data[group_count] = key;
			}
			scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
			if (!start_group(group_count)) {
				group_count = 0;
				return false;
			}
			group_count++;
		}
		const auto group_idx = group_count - 1;
		scratch.group_row_counts[group_idx]++;
		if (!visit_row(row_idx, group_idx)) {
			group_count = 0;
			return false;
		}
	}
	if (group_count == 0 || group_count == count) {
		group_count = 0;
		return false;
	}
	if (run_group_keys) {
		FinishFlatPreaggregatedGroupTarget(*run_group_keys, group_count);
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
static bool TryPreaggregateInputVectorFusedAffinePrimitiveGroupRunsWithSelection(
    SljitExecutableRegionOp &op, DataChunk &input, const SOURCE_TYPE *group_data, const SelectionVector *group_sel,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	auto &run_update = op.aggregate_update.fused_affine_run_update;
	const auto count = input.size();
	if (!run_update.Ready() || run_update.lanes.size() != payload_lanes.size() ||
	    run_update.source_position >= payload_source_indices.size()) {
		return false;
	}
	const auto source_idx = payload_source_indices[run_update.source_position];
	SljitPreaggregatedPrimitivePayloadSource source;
	if (!PrepareSljitPreaggregatedPrimitivePayloadSource(input, payload_lanes[0], source_idx, true, source) ||
	    source.type != run_update.source_type) {
		return false;
	}
	for (auto lane : payload_lanes) {
		if (!lane || lane->kind != run_update.primitive_kind) {
			return false;
		}
	}
	scratch.PrepareSharedAffine(payload_lanes, count);
	auto start_group = [&](idx_t group_idx) {
		D_ASSERT(group_idx == scratch.shared_valid_counts.size());
		scratch.group_row_counts.push_back(0);
		scratch.shared_hugeint_values.emplace_back(0);
		scratch.shared_valid_counts.push_back(0);
		return true;
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		if (run_update.primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			int64_t value;
			if (SljitLoadPreaggregatedInt64Payload(source, row_idx, value)) {
				scratch.shared_hugeint_values[group_idx] += hugeint_t(value);
				scratch.shared_valid_counts[group_idx]++;
			}
		} else {
			hugeint_t value;
			if (SljitLoadPreaggregatedHugeintPayload(source, row_idx, value)) {
				scratch.shared_hugeint_values[group_idx] += value;
				scratch.shared_valid_counts[group_idx]++;
			}
		}
		return true;
	};
	if (!SljitBuildInputVectorPrimitiveRuns<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, GROUP_HAS_SELECTION>(
	        count, group_data, group_sel, scratch, run_group_keys, group_count, start_group, visit_row)) {
		return false;
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY, bool GROUP_HAS_SELECTION>
static bool TryPreaggregateInputVectorFusedPrimitiveGroupRunsWithSelection(
    SljitExecutableRegionOp &op, DataChunk &input, const SOURCE_TYPE *group_data, const SelectionVector *group_sel,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	const auto count = input.size();
	group_count = 0;
	if (count < 2 ||
	    !SljitCanPreaggregateInputVectorFusedPrimitivePayloads(op, input, payload_source_indices, reduction_lanes)) {
		return false;
	}
	if (TryPreaggregateInputVectorFusedAffinePrimitiveGroupRunsWithSelection<TARGET_TYPE, SOURCE_TYPE, CAST_KEY,
	                                                                         GROUP_HAS_SELECTION>(
	        op, input, group_data, group_sel, payload_source_indices, payload_lanes, scratch, run_group_keys,
	        group_count)) {
		return true;
	}
	if (!SljitPrepareFusedPreaggregatedPrimitiveScratch(scratch, payload_lanes, count, count)) {
		return false;
	}
	auto start_group = [&](idx_t) {
		scratch.group_row_counts.push_back(0);
		return true;
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		scratch.fused_row_state_addresses[row_idx] = SljitFusedPreaggregatedPrimitiveStateAddress(scratch, group_idx);
		return true;
	};
	if (!SljitBuildInputVectorPrimitiveRuns<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, GROUP_HAS_SELECTION>(
	        count, group_data, group_sel, scratch, run_group_keys, group_count, start_group, visit_row)) {
		return false;
	}
	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update.Function(),
	    op.aggregate_update.plan.sink_info.aggregate_contract, op.aggregate_update.payload_descriptors, payload_lanes,
	    reduction_lanes, input, scratch.fused_row_state_addresses.data(), nullptr, nullptr, false, count,
	    payload_scratch, optional_ptr<const vector<idx_t>>(&payload_source_indices));
	if (!SljitExtractFusedPreaggregatedPrimitiveDeltas(scratch, payload_lanes, group_count)) {
		throw InternalException("SLJIT fused input-vector preaggregated primitive delta extraction failed");
	}
	return true;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool TryPreaggregateInputVectorFusedPrimitiveGroupRunsWithKeyData(
    SljitExecutableRegionOp &op, DataChunk &input, UnifiedVectorFormat &group_format,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	auto group_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(group_format);
	auto group_sel = group_format.sel;
	if (group_sel->IsSet()) {
		return TryPreaggregateInputVectorFusedPrimitiveGroupRunsWithSelection<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, true>(
		    op, input, group_data, group_sel, payload_source_indices, payload_lanes, reduction_lanes, scratch,
		    payload_scratch, run_group_keys, group_count);
	}
	return TryPreaggregateInputVectorFusedPrimitiveGroupRunsWithSelection<TARGET_TYPE, SOURCE_TYPE, CAST_KEY, false>(
	    op, input, group_data, group_sel, payload_source_indices, payload_lanes, reduction_lanes, scratch,
	    payload_scratch, run_group_keys, group_count);
}

struct SljitFusedRunPreaggregationKeyDispatch {
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	const vector<SljitGroupedReductionLaneBinding> &reduction_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	SljitAggregatePayloadAdapterScratch &payload_scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;

	template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
	bool Execute(UnifiedVectorFormat &group_format) {
		return TryPreaggregateInputVectorFusedPrimitiveGroupRunsWithKeyData<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(
		    op, input, group_format, payload_source_indices, payload_lanes, reduction_lanes, scratch, payload_scratch,
		    run_group_keys, group_count);
	}
};

template <class TARGET_TYPE>
static bool TryPreaggregateInputVectorFusedPrimitiveGroupRunsFastTemplated(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	group_count = 0;
	if (input.size() < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		return false;
	}
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source) ||
	    !SljitPreaggregatedInputVectorGroupKeyReplayable(group_source)) {
		return false;
	}
	SljitFusedRunPreaggregationKeyDispatch dispatch {
	    op,      input,           payload_source_indices, payload_lanes, reduction_lanes,
	    scratch, payload_scratch, run_group_keys,         group_count};
	return SljitDispatchPreaggregatedInputVectorGroupKeyCast<TARGET_TYPE>(group_source, dispatch);
}

struct SljitFusedRunPreaggregationTargetDispatch {
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	const vector<SljitGroupedReductionLaneBinding> &reduction_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	SljitAggregatePayloadAdapterScratch &payload_scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;

	template <class TARGET_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorFusedPrimitiveGroupRunsFastTemplated<TARGET_TYPE>(
		    op, input, group_sources, payload_source_indices, payload_lanes, reduction_lanes, scratch, payload_scratch,
		    run_group_keys, group_count);
	}
};

static bool TryPreaggregateInputVectorFusedPrimitiveGroupRunsFast(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	if (group_sources.size() != 1) {
		return false;
	}
	SljitFusedRunPreaggregationTargetDispatch dispatch {op,
	                                                    input,
	                                                    group_sources,
	                                                    payload_source_indices,
	                                                    payload_lanes,
	                                                    reduction_lanes,
	                                                    scratch,
	                                                    payload_scratch,
	                                                    run_group_keys,
	                                                    group_count};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_sources[0].target_physical_type, dispatch);
}

static bool TryPreaggregateInputVectorPrimitiveGroupRunsBest(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count, bool &fused_payloads) {
	fused_payloads = false;
	if (payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		if (!TryPreaggregateInputVectorFusedPrimitiveGroupRunsFast(op, input, group_sources, payload_source_indices,
		                                                           payload_lanes, reduction_lanes, scratch,
		                                                           payload_scratch, run_group_keys, group_count)) {
			return false;
		}
		fused_payloads = true;
		return true;
	}
	if (TryPreaggregateInputVectorPrimitiveGroupRunsFast(op, input, group_sources, payload_source_indices,
	                                                     payload_lanes, scratch, run_group_keys, group_count)) {
		return true;
	}
	return TryPreaggregateInputVectorPrimitiveGroupRuns(op, input, group_sources, payload_source_indices, payload_lanes,
	                                                    scratch, run_group_keys, group_count);
}

} // namespace duckdb

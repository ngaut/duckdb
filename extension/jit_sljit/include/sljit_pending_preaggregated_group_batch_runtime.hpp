//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pending_preaggregated_group_batch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_preaggregation_common_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"

#include <array>
#include <type_traits>

namespace duckdb {

enum class SljitPendingRunStrategy : uint8_t { UNDECIDED, BUFFERED, STREAMING };

// Keep one compact group unpublished between source invocations. If an input run crosses a
// vector boundary, its next delta merges into this carry before any hash-table append.
static constexpr idx_t SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY = STANDARD_VECTOR_SIZE - 1;

struct SljitPendingPreaggregatedPrimitiveGroupBatch {
	idx_t Count() const {
		return count;
	}

	bool Empty() const {
		return count == 0;
	}

	bool DenseSingleLaneEmpty() const {
		return dense_single_lane_represented_row_count == 0;
	}

	bool HasPending() const {
		return !Empty() || !DenseSingleLaneEmpty();
	}

	void Reset() {
		groups.Reset();
		published_groups.Reset();
		represented_row_count = 0;
		count = 0;
		InvalidateGeneratedAppendProof();
	}

	bool ConfigureGroupOutputTransform(const ExecutionRowPointerGroupKeySource &source) {
		if (!source.HasOutputTransform()) {
			return ClearGroupOutputTransform();
		}
		if (source.output_transform_kind != ExecutionGroupKeyOutputTransformKind::ADD_CONSTANT ||
		    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
		    !SljitSignedAffineGroupPhysicalType(source.source_physical_type) ||
		    !SljitSignedAffineGroupPhysicalType(source.target_physical_type)) {
			return false;
		}
		if (HasGroupOutputTransform()) {
			return group_output_transform_kind == source.output_transform_kind &&
			       group_output_transform_source_type == source.source_type &&
			       group_output_transform_target_type == source.target_type &&
			       group_output_transform_constant == source.output_transform_constant;
		}
		if (HasPending()) {
			return false;
		}
		group_output_transform_kind = source.output_transform_kind;
		group_output_transform_source_type = source.source_type;
		group_output_transform_target_type = source.target_type;
		group_output_transform_constant = source.output_transform_constant;
		return true;
	}

	bool ClearGroupOutputTransform() {
		if (HasGroupOutputTransform() && HasPending()) {
			return false;
		}
		group_output_transform_kind = ExecutionGroupKeyOutputTransformKind::NONE;
		group_output_transform_source_type = LogicalType();
		group_output_transform_target_type = LogicalType();
		group_output_transform_constant = 0;
		published_groups.Reset();
		return true;
	}

	bool HasGroupOutputTransform() const {
		return group_output_transform_kind != ExecutionGroupKeyOutputTransformKind::NONE;
	}

	void BeginGeneratedAppendProof() {
		if (!Empty()) {
			return;
		}
		generated_append_proof_available = true;
		generated_groups_strictly_increasing = true;
	}

	void UpdateGeneratedAppendProof(bool groups_strictly_increasing) {
		generated_groups_strictly_increasing = generated_append_proof_available && groups_strictly_increasing;
	}

	void InvalidateGeneratedAppendProof() {
		generated_append_proof_available = false;
		generated_groups_strictly_increasing = false;
	}

	ExecutionGroupedAggregateAppendProof GeneratedAppendProof() const {
		ExecutionGroupedAggregateAppendProof result;
		result.groups_strictly_increasing = generated_append_proof_available && generated_groups_strictly_increasing;
		return result;
	}

	bool EnsureFixedScratch(
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &new_lanes,
	    SljitPreaggregatedPrimitivePayloadLayout payload_layout = SljitPreaggregatedPrimitivePayloadLayout::PER_LANE) {
		const bool fixed_capacity =
		    payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE
		        ? scratch.HasFixedSharedAffineCapacity(lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY)
		        : scratch.payload_layout == SljitPreaggregatedPrimitivePayloadLayout::PER_LANE &&
		              scratch.HasFixedCapacity(lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY);
		if (lanes == new_lanes && fixed_capacity && generated_run_lane_inputs.size() == lanes.size()) {
			return true;
		}
		if (HasPending()) {
			return false;
		}
		lanes = new_lanes;
		const bool prepared = payload_layout == SljitPreaggregatedPrimitivePayloadLayout::SHARED_AFFINE
		                          ? scratch.PrepareFixedSharedAffine(lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY)
		                          : scratch.PrepareFixed(lanes, SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY);
		if (!prepared) {
			return false;
		}
		generated_run_lane_inputs.resize(lanes.size());
		return true;
	}

	void ResetDenseSingleLane() {
		dense_single_lane_initialized = false;
		dense_single_lane_domain = ExecutionDenseGroupDomain();
		dense_single_lane_group_type = LogicalType();
		dense_single_lane_lane = nullptr;
		dense_single_lane_deltas.clear();
		dense_single_lane_wide_deltas.clear();
		dense_single_lane_touched_offsets.clear();
		dense_single_lane_represented_row_count = 0;
	}

	uint64_t DenseSingleLaneDelta(idx_t offset) const {
		D_ASSERT(offset < dense_single_lane_deltas.size());
		return dense_single_lane_wide_deltas.empty() ? dense_single_lane_deltas[offset]
		                                             : dense_single_lane_wide_deltas[offset];
	}

	void PromoteDenseSingleLaneDeltas() {
		if (!dense_single_lane_wide_deltas.empty()) {
			return;
		}
		dense_single_lane_wide_deltas.assign(dense_single_lane_deltas.size(), 0);
		for (auto touched_offset : dense_single_lane_touched_offsets) {
			dense_single_lane_wide_deltas[touched_offset] = dense_single_lane_deltas[touched_offset];
		}
	}

	SljitDataChunkBatch groups;
	// Equivalence keys stay immutable. SQL-visible transformed keys are built in
	// this separate batch only for publication, so a failed update is retry-safe.
	SljitDataChunkBatch published_groups;
	SljitPreaggregatedPrimitiveAggregateScratch scratch;
	// Descriptors are rebound per chunk, but their storage belongs to the pipeline-local pending state.
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes;
	vector<SljitNativePrimitiveRunLaneInput> generated_run_lane_inputs;
	idx_t represented_row_count = 0;
	idx_t count = 0;

	bool dense_single_lane_initialized = false;
	ExecutionDenseGroupDomain dense_single_lane_domain;
	LogicalType dense_single_lane_group_type;
	const ExecutionPrimitiveAggregateUpdateLane *dense_single_lane_lane = nullptr;
	vector<uint32_t> dense_single_lane_deltas;
	vector<uint64_t> dense_single_lane_wide_deltas;
	vector<idx_t> dense_single_lane_touched_offsets;
	std::array<idx_t, STANDARD_VECTOR_SIZE> dense_single_lane_batch_offsets;
	idx_t dense_single_lane_represented_row_count = 0;
	SljitDataChunkBatch dense_single_lane_groups;
	SljitPreaggregatedPrimitiveAggregateScratch dense_single_lane_scratch;
	string dense_single_lane_blocker;

	bool proven_unique_append_active = false;
	bool proven_unique_append_failed = false;
	SljitPendingRunStrategy run_strategy = SljitPendingRunStrategy::UNDECIDED;
	bool generated_append_proof_available = false;
	bool generated_groups_strictly_increasing = false;
	ExecutionGroupKeyOutputTransformKind group_output_transform_kind = ExecutionGroupKeyOutputTransformKind::NONE;
	LogicalType group_output_transform_source_type;
	LogicalType group_output_transform_target_type;
	int64_t group_output_transform_constant = 0;
};

static bool SljitPreparePendingPublishedGroups(ExecutionRegionRuntime &runtime,
                                               SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                               DataChunk *&published_groups) {
	published_groups = &pending.groups.chunk;
	if (!pending.HasGroupOutputTransform()) {
		return true;
	}
	if (pending.group_output_transform_kind != ExecutionGroupKeyOutputTransformKind::ADD_CONSTANT ||
	    pending.groups.chunk.ColumnCount() != 1 ||
	    pending.groups.chunk.data[0].GetType() != pending.group_output_transform_source_type) {
		return false;
	}
	pending.published_groups.Ensure(runtime.GetAllocator(),
	                                vector<LogicalType> {pending.group_output_transform_target_type});
	auto &target = pending.published_groups.chunk;
	target.Reset();
	ExecutionRowPointerGroupKeySource source;
	SljitInitializeInputVectorGroupKeySource(0, pending.group_output_transform_source_type,
	                                         pending.group_output_transform_target_type, source);
	source.ready = true;
	source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	source.output_transform_kind = pending.group_output_transform_kind;
	source.output_transform_constant = pending.group_output_transform_constant;
	if (!SljitTryMaterializeInputVectorGroupSource(pending.groups.chunk, source, target.data[0], pending.Count(),
	                                               false)) {
		return false;
	}
	target.SetChildCardinality(pending.Count());
	published_groups = &target;
	return true;
}

static void SljitUpdateProvenUniqueAppendContract(ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op,
                                                  SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                  DataChunk &groups,
                                                  ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	if (pending.proven_unique_append_failed) {
		return;
	}
	const auto append_proof = pending.GeneratedAppendProof();
	if (!grouped_state.state->TryEnableProvenUniqueAppend(groups, append_proof)) {
		if (pending.proven_unique_append_active) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "proven_unique_append.final_combine_required");
		}
		pending.proven_unique_append_active = false;
		pending.proven_unique_append_failed = true;
		return;
	}
	if (!pending.proven_unique_append_active) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "proven_unique_append.enabled");
	}
	if (append_proof.groups_strictly_increasing) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "proven_unique_append.producer_order_proof", groups.size());
	}
	pending.proven_unique_append_active = true;
}

static void SljitInvalidateProvenUniqueAppendContract(ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op,
                                                      SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                      ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	grouped_state.state->RequireAppendFinalCombine();
	if (pending.proven_unique_append_active) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "proven_unique_append.final_combine_required");
	}
	pending.proven_unique_append_active = false;
	pending.proven_unique_append_failed = true;
}

static constexpr idx_t SLJIT_PENDING_DENSE_SINGLE_LANE_MEMORY_BUDGET = 32ULL * 1024ULL * 1024ULL;
static constexpr idx_t SLJIT_PENDING_DENSE_SINGLE_LANE_BYTES_PER_GROUP = sizeof(uint32_t) + sizeof(idx_t);
static constexpr idx_t SLJIT_PENDING_DENSE_SINGLE_LANE_MAX_RANGE =
    SLJIT_PENDING_DENSE_SINGLE_LANE_MEMORY_BUDGET / SLJIT_PENDING_DENSE_SINGLE_LANE_BYTES_PER_GROUP;
static constexpr idx_t SLJIT_PENDING_DENSE_SINGLE_LANE_MIN_COMPRESSION = 2;
template <class TARGET_TYPE>
static bool SljitPendingDenseSingleLaneKey(TARGET_TYPE value, idx_t &key) {
	if constexpr (std::is_same<TARGET_TYPE, bool>::value) {
		key = value ? 1 : 0;
		return true;
	} else {
		if constexpr (std::is_signed<TARGET_TYPE>::value) {
			if (value < 0) {
				return false;
			}
		}
		using UNSIGNED_TYPE = typename std::make_unsigned<TARGET_TYPE>::type;
		const auto unsigned_value = static_cast<UNSIGNED_TYPE>(value);
		if (unsigned_value > NumericLimits<idx_t>::Maximum()) {
			return false;
		}
		key = static_cast<idx_t>(unsigned_value);
		return true;
	}
}

static bool SljitPendingDenseSingleLaneProfitable(const SljitExecutableRegionOp &op,
                                                  const ExecutionDenseGroupDomain &dense_domain, idx_t &range) {
	if (!dense_domain.ready || dense_domain.min_key > dense_domain.max_key ||
	    dense_domain.max_key == NumericLimits<idx_t>::Maximum()) {
		return false;
	}
	range = dense_domain.max_key - dense_domain.min_key + 1;
	if (range == 0 || range > SLJIT_PENDING_DENSE_SINGLE_LANE_MAX_RANGE) {
		return false;
	}
	const auto estimated_input_count = op.aggregate_update.plan.estimated_input_count;
	return estimated_input_count / SLJIT_PENDING_DENSE_SINGLE_LANE_MIN_COMPRESSION >= range;
}

template <class TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
static bool SljitAccumulatePendingDenseSingleLaneKeyData(DataChunk &input, UnifiedVectorFormat &group_format,
                                                         const ExecutionDenseGroupDomain &dense_domain,
                                                         SljitPendingPreaggregatedPrimitiveGroupBatch &pending) {
	auto group_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(group_format);
	auto group_sel = group_format.sel;
	const bool can_have_null = group_format.validity.CanHaveNull();
	if (pending.dense_single_lane_wide_deltas.empty() &&
	    (pending.dense_single_lane_represented_row_count > NumericLimits<uint32_t>::Maximum() ||
	     input.size() > NumericLimits<uint32_t>::Maximum() - pending.dense_single_lane_represented_row_count)) {
		pending.PromoteDenseSingleLaneDeltas();
	}
	auto accumulate = [&](auto *deltas) {
		using DELTA_TYPE = typename std::remove_pointer<decltype(deltas)>::type;
		const auto touched_count_before_batch = pending.dense_single_lane_touched_offsets.size();
		auto rollback = [&](idx_t row_count) {
			for (idx_t rollback_idx = 0; rollback_idx < row_count; rollback_idx++) {
				auto &delta = deltas[pending.dense_single_lane_batch_offsets[rollback_idx]];
				D_ASSERT(delta > 0);
				delta--;
			}
			pending.dense_single_lane_touched_offsets.resize(touched_count_before_batch);
			pending.dense_single_lane_blocker = "key";
			return false;
		};
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			if (can_have_null && !group_format.validity.RowIsValid(source_idx)) {
				return rollback(row_idx);
			}
			TARGET_TYPE value;
			if constexpr (CAST_KEY) {
				value = static_cast<TARGET_TYPE>(group_data[source_idx]);
			} else {
				value = group_data[source_idx];
			}
			idx_t key;
			if (!SljitPendingDenseSingleLaneKey(value, key) || key < dense_domain.min_key ||
			    key > dense_domain.max_key) {
				return rollback(row_idx);
			}
			const auto offset = key - dense_domain.min_key;
			pending.dense_single_lane_batch_offsets[row_idx] = offset;
			auto &delta = deltas[offset];
			if (delta == 0) {
				pending.dense_single_lane_touched_offsets.push_back(offset);
			}
			if constexpr (std::is_same<DELTA_TYPE, uint64_t>::value) {
				if (delta == NumericLimits<int64_t>::Maximum()) {
					throw OutOfRangeException("Dense pending grouped count overflow");
				}
			}
			delta++;
		}
		pending.dense_single_lane_represented_row_count += input.size();
		pending.dense_single_lane_blocker.clear();
		return true;
	};
	if (pending.dense_single_lane_wide_deltas.empty()) {
		return accumulate(pending.dense_single_lane_deltas.data());
	}
	return accumulate(pending.dense_single_lane_wide_deltas.data());
}

template <class TARGET_TYPE>
struct SljitPendingDenseSingleLaneKeyDispatch {
	DataChunk &input;
	const ExecutionDenseGroupDomain &dense_domain;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;

	template <class DISPATCH_TARGET_TYPE, class SOURCE_TYPE, bool CAST_KEY>
	bool Execute(UnifiedVectorFormat &group_format) {
		static_assert(std::is_same<TARGET_TYPE, DISPATCH_TARGET_TYPE>::value,
		              "Dense single-lane key dispatch changed target type");
		return SljitAccumulatePendingDenseSingleLaneKeyData<TARGET_TYPE, SOURCE_TYPE, CAST_KEY>(input, group_format,
		                                                                                        dense_domain, pending);
	}
};

template <class TARGET_TYPE>
static bool SljitTryAccumulatePendingDenseSingleLaneGroupsTemplated(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const ExecutionDenseGroupDomain &dense_domain, SljitPendingPreaggregatedPrimitiveGroupBatch &pending) {
	auto reject = [&](const char *blocker) {
		pending.dense_single_lane_blocker = blocker;
		return false;
	};
	pending.dense_single_lane_blocker.clear();
	if (pending.HasGroupOutputTransform() || input.size() == 0 || input.size() > STANDARD_VECTOR_SIZE ||
	    group_sources.size() != 1 || payload_lanes.size() != 1 || !payload_lanes[0] || !pending.Empty()) {
		return reject("shape");
	}
	auto &lane = *payload_lanes[0];
	if (!lane.ready ||
	    (lane.kind != AggregatePrimitiveUpdateKind::COUNT && lane.kind != AggregatePrimitiveUpdateKind::COUNT_STAR)) {
		return reject("lane");
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1 || sink_info.aggregates.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != dense_domain.physical_type ||
	    group_sources[0].target_physical_type != dense_domain.physical_type) {
		return reject("sink");
	}
	idx_t range;
	if (!SljitPendingDenseSingleLaneProfitable(op, dense_domain, range)) {
		return reject("economics");
	}
	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source) ||
	    !SljitPreaggregatedInputVectorGroupKeyReplayable(group_source)) {
		return reject("source");
	}
	if (pending.dense_single_lane_initialized) {
		if (pending.dense_single_lane_domain.physical_type != dense_domain.physical_type ||
		    pending.dense_single_lane_domain.min_key != dense_domain.min_key ||
		    pending.dense_single_lane_domain.max_key != dense_domain.max_key ||
		    pending.dense_single_lane_group_type != sink_info.groups[0].type ||
		    pending.dense_single_lane_lane != payload_lanes[0] || pending.dense_single_lane_deltas.size() != range ||
		    (!pending.dense_single_lane_wide_deltas.empty() && pending.dense_single_lane_wide_deltas.size() != range)) {
			return reject("state");
		}
	} else {
		pending.dense_single_lane_initialized = true;
		pending.dense_single_lane_domain = dense_domain;
		pending.dense_single_lane_group_type = sink_info.groups[0].type;
		pending.dense_single_lane_lane = payload_lanes[0];
		pending.dense_single_lane_deltas.assign(range, 0);
		pending.dense_single_lane_touched_offsets.clear();
		pending.dense_single_lane_touched_offsets.reserve(MinValue(range, dense_domain.distinct_count));
	}

	SljitPendingDenseSingleLaneKeyDispatch<TARGET_TYPE> dispatch {input, dense_domain, pending};
	if (!SljitDispatchPreaggregatedInputVectorGroupKeyCast<TARGET_TYPE>(group_source, dispatch)) {
		if (pending.dense_single_lane_blocker.empty()) {
			return reject("key_dispatch");
		}
		return false;
	}
	return true;
}

struct SljitPendingDenseSingleLaneTargetDispatch {
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	const ExecutionDenseGroupDomain &dense_domain;
	SljitPendingPreaggregatedPrimitiveGroupBatch &pending;

	template <class TARGET_TYPE>
	bool Execute() {
		if constexpr (std::is_integral<TARGET_TYPE>::value) {
			return SljitTryAccumulatePendingDenseSingleLaneGroupsTemplated<TARGET_TYPE>(
			    op, input, group_sources, payload_lanes, dense_domain, pending);
		}
		return false;
	}
};

static bool SljitTryAccumulatePendingDenseSingleLaneGroups(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const ExecutionDenseGroupDomain &dense_domain, SljitPendingPreaggregatedPrimitiveGroupBatch &pending) {
	SljitPendingDenseSingleLaneTargetDispatch dispatch {op, input, group_sources, payload_lanes, dense_domain, pending};
	const auto accumulated = SljitDispatchPreaggregatedInputVectorGroupTargetType(dense_domain.physical_type, dispatch);
	if (!accumulated && pending.dense_single_lane_blocker.empty()) {
		pending.dense_single_lane_blocker = "target_type";
	}
	return accumulated;
}

static bool SljitPreaggregatedPrimitiveSingleGroupKeysMatch(DataChunk &left, idx_t left_idx, DataChunk &right,
                                                            idx_t right_idx) {
	if (left.ColumnCount() != 1 || right.ColumnCount() != 1 || left_idx >= left.size() || right_idx >= right.size() ||
	    left.data[0].GetType() != right.data[0].GetType()) {
		return false;
	}
	const auto physical_type = left.data[0].GetType().InternalType();
	if (!TypeIsConstantSize(physical_type)) {
		return false;
	}
	const auto type_size = GetTypeIdSize(physical_type);
	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;
	left.data[0].ToUnifiedFormat(left_format);
	right.data[0].ToUnifiedFormat(right_format);
	const auto left_source_idx = left_format.sel->get_index(left_idx);
	const auto right_source_idx = right_format.sel->get_index(right_idx);
	if (!left_format.validity.RowIsValid(left_source_idx) || !right_format.validity.RowIsValid(right_source_idx)) {
		return false;
	}
	return memcmp(left_format.data + left_source_idx * type_size, right_format.data + right_source_idx * type_size,
	              type_size) == 0;
}

static bool
SljitAppendPreaggregatedPrimitiveGroupRange(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                            idx_t op_idx, SljitExecutableRegionOp &op,
                                            SljitPendingPreaggregatedPrimitiveGroupBatch &pending, DataChunk &groups,
                                            SljitPreaggregatedPrimitiveAggregateScratch &source_scratch,
                                            const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                            idx_t offset, idx_t count, idx_t represented_row_count) {
	if (count == 0) {
		return true;
	}
	if (!pending.groups.Initialized()) {
		pending.groups.EnsureFromChunk(runtime.GetAllocator(), groups);
	}
	if (!pending.EnsureFixedScratch(payload_lanes, source_scratch.payload_layout)) {
		return false;
	}
	if (pending.groups.chunk.ColumnCount() != groups.ColumnCount() || pending.lanes.size() != payload_lanes.size()) {
		return false;
	}
	if (!CanSlicePreaggregatedPrimitiveScratch(source_scratch, payload_lanes, offset, count) ||
	    pending.scratch.payloads.size() != payload_lanes.size() ||
	    pending.Count() + count > SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		if (pending.scratch.payloads[payload_idx].kind != source_scratch.payloads[payload_idx].kind) {
			return false;
		}
	}
	pending.InvalidateGeneratedAppendProof();
	DataChunk *source_groups = &groups;
	auto &group_slice = scratch.AggregatePreaggregatedGroupSlice(op_idx);
	if (offset != 0 || count != groups.size()) {
		group_slice.Reset();
		group_slice.Slice(groups, offset, offset + count);
		source_groups = &group_slice;
	}
	const auto target_offset = pending.Count();
	if (!CopyPreaggregatedPrimitiveScratchRangeToFixed(source_scratch, payload_lanes, offset, count, pending.scratch,
	                                                   target_offset)) {
		return false;
	}
	auto append_start = SljitRegionStageStart(runtime);
	if (!SljitTryFastAppendFixedAllValid(pending.groups.chunk, *source_groups)) {
		pending.groups.chunk.Append(*source_groups);
	}
	pending.count += count;
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "pending_preaggregated_group_append", append_start);
	pending.represented_row_count += represented_row_count;
	return true;
}

template <class TARGET_TYPE>
static bool
SljitFlushPendingDenseSingleLaneGroupsTemplated(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                idx_t op_idx, SljitExecutableRegionOp &op,
                                                SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	if (pending.DenseSingleLaneEmpty()) {
		return true;
	}
	if (!pending.dense_single_lane_initialized || !pending.dense_single_lane_lane ||
	    pending.dense_single_lane_group_type.InternalType() != pending.dense_single_lane_domain.physical_type ||
	    pending.dense_single_lane_touched_offsets.empty()) {
		return false;
	}
	vector<const ExecutionPrimitiveAggregateUpdateLane *> lanes {pending.dense_single_lane_lane};
	pending.dense_single_lane_groups.Ensure(runtime.GetAllocator(),
	                                        vector<LogicalType> {pending.dense_single_lane_group_type});
	auto &groups = pending.dense_single_lane_groups.chunk;
	idx_t flushed_row_count = 0;
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state,
	                                      pending.dense_single_lane_touched_offsets.size());
	for (idx_t offset = 0; offset < pending.dense_single_lane_touched_offsets.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto group_count =
		    MinValue<idx_t>(STANDARD_VECTOR_SIZE, pending.dense_single_lane_touched_offsets.size() - offset);
		auto target_data = PrepareFlatPreaggregatedGroupTarget<TARGET_TYPE>(groups);
		auto &preaggregate_scratch = pending.dense_single_lane_scratch;
		preaggregate_scratch.Prepare(lanes, group_count);
		idx_t represented_row_count = 0;
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			const auto dense_offset = pending.dense_single_lane_touched_offsets[offset + group_idx];
			const auto key = pending.dense_single_lane_domain.min_key + dense_offset;
			target_data[group_idx] = static_cast<TARGET_TYPE>(key);
			switch (pending.dense_single_lane_lane->kind) {
			case AggregatePrimitiveUpdateKind::COUNT:
			case AggregatePrimitiveUpdateKind::COUNT_STAR: {
				const auto delta = pending.DenseSingleLaneDelta(dense_offset);
				if (delta == 0 || delta > NumericLimits<int64_t>::Maximum() ||
				    delta > NumericLimits<idx_t>::Maximum()) {
					throw InternalException("SLJIT pending dense single-lane count delta is invalid");
				}
				const auto row_count = UnsafeNumericCast<idx_t>(delta);
				preaggregate_scratch.group_row_counts.push_back(row_count);
				preaggregate_scratch.payloads[0].int64_values.push_back(UnsafeNumericCast<int64_t>(delta));
				represented_row_count += row_count;
				break;
			}
			default:
				throw InternalException("Unsupported SLJIT pending dense single-lane aggregate");
			}
		}
		FinishFlatPreaggregatedGroupTarget(groups, group_count);
		if (!TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(runtime, scratch, op_idx, op, groups,
		                                                                   preaggregate_scratch, lanes, grouped_state,
		                                                                   represented_row_count, false, true)) {
			throw InternalException("Validated SLJIT pending dense single-lane grouped update failed");
		}
		flushed_row_count += represented_row_count;
	}
	if (flushed_row_count != pending.dense_single_lane_represented_row_count) {
		throw InternalException("SLJIT pending dense single-lane flush row count mismatch");
	}
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "pending_dense_single_lane_grouped_update_flush",
	                                            pending.dense_single_lane_represented_row_count);
	pending.ResetDenseSingleLane();
	return true;
}

static bool SljitFlushPendingDenseSingleLaneGroups(ExecutionRegionRuntime &runtime,
                                                   SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                   SljitExecutableRegionOp &op,
                                                   SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                   ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	switch (pending.dense_single_lane_domain.physical_type) {
	case PhysicalType::BOOL:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<bool>(runtime, scratch, op_idx, op, pending,
		                                                             grouped_state);
	case PhysicalType::INT8:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<int8_t>(runtime, scratch, op_idx, op, pending,
		                                                               grouped_state);
	case PhysicalType::INT16:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<int16_t>(runtime, scratch, op_idx, op, pending,
		                                                                grouped_state);
	case PhysicalType::INT32:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<int32_t>(runtime, scratch, op_idx, op, pending,
		                                                                grouped_state);
	case PhysicalType::INT64:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<int64_t>(runtime, scratch, op_idx, op, pending,
		                                                                grouped_state);
	case PhysicalType::UINT8:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<uint8_t>(runtime, scratch, op_idx, op, pending,
		                                                                grouped_state);
	case PhysicalType::UINT16:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<uint16_t>(runtime, scratch, op_idx, op, pending,
		                                                                 grouped_state);
	case PhysicalType::UINT32:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<uint32_t>(runtime, scratch, op_idx, op, pending,
		                                                                 grouped_state);
	case PhysicalType::UINT64:
		return SljitFlushPendingDenseSingleLaneGroupsTemplated<uint64_t>(runtime, scratch, op_idx, op, pending,
		                                                                 grouped_state);
	default:
		return pending.DenseSingleLaneEmpty();
	}
}

static bool SljitFlushPendingPreaggregatedPrimitiveGroups(ExecutionRegionRuntime &runtime,
                                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                          SljitExecutableRegionOp &op,
                                                          SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
                                                          ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	if (!SljitFlushPendingDenseSingleLaneGroups(runtime, scratch, op_idx, op, pending, grouped_state)) {
		return false;
	}
	if (pending.Empty()) {
		return true;
	}
	pending.groups.chunk.SetChildCardinality(pending.Count());
	if (pending.Count() > pending.scratch.group_row_counts.size()) {
		return false;
	}
	DataChunk *published_groups;
	if (!SljitPreparePendingPublishedGroups(runtime, pending, published_groups)) {
		return false;
	}
	SljitUpdateProvenUniqueAppendContract(runtime, op, pending, *published_groups, grouped_state);
	if (!pending.proven_unique_append_active) {
		SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state, pending.Count());
	}
	if (!TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(runtime, scratch, op_idx, op, *published_groups,
	                                                                   pending.scratch, pending.lanes, grouped_state,
	                                                                   pending.represented_row_count, false, true)) {
		return false;
	}
	if (pending.HasGroupOutputTransform()) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "pending_group_output_transform.add_constant", pending.Count());
	}
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "pending_preaggregated_grouped_update_flush",
	                                            pending.represented_row_count);
	pending.Reset();
	return true;
}

static bool SljitBufferPreaggregatedPrimitiveGroups(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &groups, SljitPreaggregatedPrimitiveAggregateScratch &source_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    SljitPendingPreaggregatedPrimitiveGroupBatch &pending, bool finish) {
	if (groups.size() == 0 || groups.ColumnCount() != 1 ||
	    !TypeIsConstantSize(groups.data[0].GetType().InternalType()) ||
	    !CanSlicePreaggregatedPrimitiveScratch(source_scratch, payload_lanes, 0, groups.size())) {
		return false;
	}
	if (!pending.EnsureFixedScratch(payload_lanes, source_scratch.payload_layout)) {
		return false;
	}
	pending.InvalidateGeneratedAppendProof();
	idx_t offset = 0;
	if (!pending.Empty() &&
	    SljitPreaggregatedPrimitiveSingleGroupKeysMatch(pending.groups.chunk, pending.Count() - 1, groups, 0)) {
		idx_t merged_row_count;
		if (!PreaggregatedPrimitiveRepresentedRowCount(source_scratch, 0, 1, merged_row_count) ||
		    !MergePreaggregatedPrimitiveScratchGroup(source_scratch, payload_lanes, 0, pending.scratch,
		                                             pending.Count() - 1)) {
			return false;
		}
		pending.represented_row_count += merged_row_count;
		offset = 1;
		RecordSljitRegionRuntimePath(runtime, op.kind, "pending_preaggregated_group_boundary_merge");
	}
	auto append_count = groups.size() - offset;
	if (append_count > 0 && pending.Count() + append_count > SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY) {
		const auto fill_count = SLJIT_PENDING_PREAGGREGATED_GROUP_CAPACITY - pending.Count();
		if (fill_count > 0) {
			idx_t fill_row_count;
			if (!PreaggregatedPrimitiveRepresentedRowCount(source_scratch, offset, fill_count, fill_row_count) ||
			    !SljitAppendPreaggregatedPrimitiveGroupRange(runtime, scratch, op_idx, op, pending, groups,
			                                                 source_scratch, payload_lanes, offset, fill_count,
			                                                 fill_row_count)) {
				return false;
			}
			offset += fill_count;
			append_count -= fill_count;
		}
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state)) {
			return false;
		}
	}
	if (append_count > 0) {
		idx_t append_row_count;
		if (!PreaggregatedPrimitiveRepresentedRowCount(source_scratch, offset, append_count, append_row_count) ||
		    !SljitAppendPreaggregatedPrimitiveGroupRange(runtime, scratch, op_idx, op, pending, groups, source_scratch,
		                                                 payload_lanes, offset, append_count, append_row_count)) {
			return false;
		}
	}
	if (finish) {
		if (!SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, pending, grouped_state)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

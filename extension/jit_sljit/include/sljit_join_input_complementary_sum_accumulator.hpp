//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_input_complementary_sum_accumulator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_grouped_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_typed_local_group_index.hpp"

#include <array>

namespace duckdb {

static constexpr idx_t SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS =
    SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS;
static constexpr idx_t SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_ADMISSION_GROUPS =
    SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS / 2;
static constexpr idx_t SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_HOT_GROUPS = 8;

template <class T, bool PREDICATE_ALL_VALID = false>
struct SljitTypedJoinInputComplementarySumAccumulator final
    : public SljitJoinInputRowPointerComplementarySumAccumulator {
	SljitTypedJoinInputComplementarySumAccumulator()
	    : SljitJoinInputRowPointerComplementarySumAccumulator(GetTypeId<T>(), PREDICATE_ALL_VALID) {
	}

	bool Accumulate(bool group_is_valid, const T &key, bool predicate_is_valid, bool predicate_matches) {
		if constexpr (PREDICATE_ALL_VALID) {
			D_ASSERT(predicate_is_valid);
			return AccumulateAllValid(group_is_valid, key, predicate_matches);
		}
		idx_t group_idx;
		bool created;
		if (!groups.FindOrCreate(group_is_valid, key, group_idx, created)) {
			return false;
		}
		represented_rows[group_idx]++;
		if (!predicate_is_valid) {
			return true;
		}
		// Each valid predicate contributes to exactly one complementary lane. A
		// pointer select keeps that choice branchless while avoiding a second
		// read-modify-write that only adds zero to the opposite lane.
		auto *counts = predicate_matches ? matching_counts.data() : non_matching_counts.data();
		counts[group_idx]++;
		return true;
	}

	bool AccumulateAllValid(bool group_is_valid, const T &key, bool predicate_matches) {
		idx_t group_idx;
		bool created;
		if (!groups.FindOrCreate(group_is_valid, key, group_idx, created)) {
			return false;
		}
		// The all-valid predicate contract makes the represented row count the
		// sum of the two complementary lanes. Do not maintain a second per-row
		// counter solely for telemetry and later preaggregation accounting.
		auto *counts = predicate_matches ? matching_counts.data() : non_matching_counts.data();
		counts[group_idx]++;
		return true;
	}

	bool HasOneOrTwoGroups() const {
		return groups.Count() > 0 && groups.Count() <= 2;
	}

	bool MatchesKnownGroup(idx_t group_idx, bool group_is_valid, const T &key) const {
		D_ASSERT(group_idx < groups.Count());
		return groups.IsValid(group_idx) == group_is_valid &&
		       (!group_is_valid || SljitLocalGroupKeyOperations<T>::Equals(groups.Key(group_idx), key));
	}

	void AddAllValidKnownGroup(idx_t group_idx, int64_t matching_delta, int64_t non_matching_delta) {
		D_ASSERT(PREDICATE_ALL_VALID);
		D_ASSERT(group_idx < groups.Count());
		D_ASSERT(matching_delta >= 0);
		D_ASSERT(non_matching_delta >= 0);
		matching_counts[group_idx] += matching_delta;
		non_matching_counts[group_idx] += non_matching_delta;
	}

	bool Empty() const override {
		return groups.Count() == 0;
	}

	idx_t Count() const {
		return groups.Count();
	}

	idx_t RepresentedRows() const {
		idx_t result = 0;
		for (idx_t group_idx = 0; group_idx < groups.Count(); group_idx++) {
			result += RepresentedRows(group_idx);
		}
		return result;
	}

	idx_t RepresentedRows(idx_t group_idx) const {
		if constexpr (PREDICATE_ALL_VALID) {
			return UnsafeNumericCast<idx_t>(matching_counts[group_idx] + non_matching_counts[group_idx]);
		}
		return represented_rows[group_idx];
	}

	void Reset() override {
		groups.Reset();
		matching_counts = {};
		non_matching_counts = {};
		represented_rows = {};
	}

	// The index starts with a bounded direct tier and permanently switches to its
	// already-populated hash table when the ninth distinct runtime key appears.
	// This decision is exact even when catalog NDV statistics are approximate.
	SljitTypedLocalGroupIndex<T, SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS,
	                          SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_HOT_GROUPS>
	    groups;
	std::array<int64_t, SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS> matching_counts {};
	std::array<int64_t, SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS> non_matching_counts {};
	std::array<idx_t, SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_MAX_GROUPS> represented_rows {};
	std::array<T, STANDARD_VECTOR_SIZE> prepared_group_keys {};
	std::array<uint8_t, STANDARD_VECTOR_SIZE> prepared_group_validity {};
};

static bool SljitJoinInputComplementarySumAccumulatorEnabled(SljitDirectJoinOutputAggregateStrategy &strategy,
                                                             SljitJoinInputRowPointerComplementarySumPlan &plan) {
	if (plan.pipeline_accumulator_checked) {
		return plan.pipeline_accumulator_enabled;
	}
	plan.pipeline_accumulator_checked = true;
	if (!strategy.source_distinct_counts ||
	    plan.join_input_group_column_idx >= strategy.source_distinct_counts->size()) {
		return false;
	}
	const auto distinct_count = (*strategy.source_distinct_counts)[plan.join_input_group_column_idx];
	plan.pipeline_accumulator_enabled =
	    distinct_count > 0 && distinct_count <= SLJIT_JOIN_INPUT_COMPLEMENTARY_ACCUMULATOR_ADMISSION_GROUPS;
	return plan.pipeline_accumulator_enabled;
}

template <class T, bool PREDICATE_ALL_VALID = false>
static SljitTypedJoinInputComplementarySumAccumulator<T, PREDICATE_ALL_VALID> &
SljitGetJoinInputComplementarySumAccumulator(SljitDirectJoinOutputAggregateStrategy &strategy) {
	if (!strategy.join_input_complementary_sum_accumulator) {
		strategy.join_input_complementary_sum_accumulator =
		    make_uniq<SljitTypedJoinInputComplementarySumAccumulator<T, PREDICATE_ALL_VALID>>();
	}
	auto &accumulator = *strategy.join_input_complementary_sum_accumulator;
	if (accumulator.physical_type != GetTypeId<T>() || accumulator.predicate_all_valid != PREDICATE_ALL_VALID) {
		throw InternalException("SLJIT join-input complementary accumulator contract changed within one pipeline");
	}
	return static_cast<SljitTypedJoinInputComplementarySumAccumulator<T, PREDICATE_ALL_VALID> &>(accumulator);
}

template <class T, bool PREDICATE_ALL_VALID>
static bool SljitMaterializeJoinInputComplementarySumAccumulator(
    SljitTypedJoinInputComplementarySumAccumulator<T, PREDICATE_ALL_VALID> &accumulator,
    const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &represented_row_count) {
	const auto group_count = accumulator.Count();
	if (group_count == 0 || compact_groups.ColumnCount() != 1 ||
	    compact_groups.data[0].GetType().InternalType() != GetTypeId<T>()) {
		return false;
	}
	auto compact_group_data = PrepareFlatPreaggregatedGroupTarget<T>(compact_groups);
	auto &target_validity = FlatVector::ValidityMutable(compact_groups.data[0]);
	target_validity.Reset(group_count);
	target_validity.SetAllValid(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		compact_group_data[group_idx] = accumulator.groups.Key(group_idx);
		if (!accumulator.groups.IsValid(group_idx)) {
			target_validity.SetInvalid(group_idx);
		}
	}
	FlatVector::SetSize(compact_groups.data[0], count_t(group_count));
	compact_groups.SetChildCardinality(group_count);

	preaggregate_scratch.Prepare(payload_lanes, group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		preaggregate_scratch.group_row_counts.push_back(accumulator.RepresentedRows(group_idx));
	}
	for (idx_t payload_idx = 0; payload_idx < preaggregate_scratch.payloads.size(); payload_idx++) {
		auto &payload = preaggregate_scratch.payloads[payload_idx];
		const bool matching_payload = payload_idx == classification.matching_payload_idx;
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			const auto delta =
			    matching_payload ? accumulator.matching_counts[group_idx] : accumulator.non_matching_counts[group_idx];
			switch (payload.kind) {
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				payload.int64_values.push_back(delta);
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				payload.hugeint_values.emplace_back(delta);
				break;
			default:
				return false;
			}
			payload.value_is_set.push_back(1);
		}
	}
	represented_row_count = accumulator.RepresentedRows();
	return true;
}

template <class T, bool PREDICATE_ALL_VALID>
static bool SljitTryMaterializeTypedJoinInputComplementarySumAccumulator(
    SljitDirectJoinOutputAggregateStrategy &strategy, const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &represented_row_count) {
	auto &base = *strategy.join_input_complementary_sum_accumulator;
	if (base.physical_type != GetTypeId<T>()) {
		return false;
	}
	if (base.predicate_all_valid != PREDICATE_ALL_VALID) {
		return false;
	}
	return SljitMaterializeJoinInputComplementarySumAccumulator(
	    static_cast<SljitTypedJoinInputComplementarySumAccumulator<T, PREDICATE_ALL_VALID> &>(base), classification,
	    payload_lanes, compact_groups, preaggregate_scratch, represented_row_count);
}

struct SljitJoinInputComplementaryAccumulatorMaterializeDispatch {
	SljitDirectJoinOutputAggregateStrategy &strategy;
	const SljitStringSetComplementarySumDescriptor &classification;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	DataChunk &compact_groups;
	SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch;
	idx_t &represented_row_count;

	template <class T>
	bool Execute() {
		if (strategy.join_input_complementary_sum_accumulator->predicate_all_valid) {
			return SljitTryMaterializeTypedJoinInputComplementarySumAccumulator<T, true>(
			    strategy, classification, payload_lanes, compact_groups, preaggregate_scratch, represented_row_count);
		}
		return SljitTryMaterializeTypedJoinInputComplementarySumAccumulator<T, false>(
		    strategy, classification, payload_lanes, compact_groups, preaggregate_scratch, represented_row_count);
	}
};

static bool SljitTryMaterializeJoinInputComplementarySumAccumulator(
    SljitDirectJoinOutputAggregateStrategy &strategy, const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &represented_row_count) {
	if (!strategy.join_input_complementary_sum_accumulator) {
		return false;
	}
	SljitJoinInputComplementaryAccumulatorMaterializeDispatch dispatch {
	    strategy, classification, payload_lanes, compact_groups, preaggregate_scratch, represented_row_count};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(
	    strategy.join_input_complementary_sum_accumulator->physical_type, dispatch);
}

static bool SljitFlushJoinInputComplementarySumAccumulator(ExecutionRegionRuntime &runtime,
                                                           SljitExecutableRegionOp &aggregate_op,
                                                           SljitDirectJoinOutputAggregateStrategy &strategy) {
	auto accumulator = optional_ptr<SljitJoinInputRowPointerComplementarySumAccumulator>(
	    strategy.join_input_complementary_sum_accumulator.get());
	if (!accumulator || accumulator->Empty()) {
		return true;
	}
	if (!strategy.join_input_complementary_sum_scratch) {
		return false;
	}
	auto &scratch = *strategy.join_input_complementary_sum_scratch;
	auto &binding = scratch.SinkBinding(strategy.aggregate_idx);
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
	    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state) {
		return false;
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(
	    strategy.aggregate_idx, aggregate_op.aggregate_update.payload_descriptors, binding.aggregate_update.primitive);
	auto &compact_groups = scratch.AggregatePreaggregatedGroups(strategy.aggregate_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(strategy.aggregate_idx);
	idx_t represented_row_count;
	if (!SljitTryMaterializeJoinInputComplementarySumAccumulator(
	        strategy, strategy.join_input_complementary_sum_plan.classification, payload_lanes, compact_groups,
	        preaggregate_scratch, represented_row_count)) {
		return false;
	}
	if (!strategy.pending_preaggregated_groups) {
		strategy.pending_preaggregated_groups = make_shared_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch>();
	}
	strategy.pending_preaggregated_scratch = &scratch;
	strategy.pending_preaggregated_deferred_grouped_finish =
	    strategy.join_input_complementary_sum_deferred_grouped_finish;
	if (!SljitBufferPreaggregatedPrimitiveGroups(
	        runtime, scratch, strategy.aggregate_idx, aggregate_op, compact_groups, preaggregate_scratch, payload_lanes,
	        binding.aggregate_update.grouped_state, represented_row_count, *strategy.pending_preaggregated_groups,
	        false, strategy.join_input_complementary_sum_deferred_grouped_finish)) {
		return false;
	}
	RecordSljitRegionMaterializationElisionPath(
	    runtime, aggregate_op.kind, "join_input_pipeline_complementary_sum_accumulator_flush", represented_row_count);
	accumulator->Reset();
	return true;
}

} // namespace duckdb

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

template <class T>
struct SljitTypedJoinInputComplementarySumAccumulator final
    : public SljitJoinInputRowPointerComplementarySumAccumulator {
	SljitTypedJoinInputComplementarySumAccumulator()
	    : SljitJoinInputRowPointerComplementarySumAccumulator(GetTypeId<T>()) {
	}

	bool Accumulate(bool group_is_valid, const T &key, bool predicate_is_valid, bool predicate_matches) {
		idx_t group_idx;
		bool created;
		if (!groups.FindOrCreate(group_is_valid, key, group_idx, created)) {
			return false;
		}
		represented_rows[group_idx]++;
		if (predicate_is_valid) {
			matching_counts[group_idx] += predicate_matches ? 1 : 0;
			non_matching_counts[group_idx] += predicate_matches ? 0 : 1;
		}
		return true;
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
			result += represented_rows[group_idx];
		}
		return result;
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

template <class T>
static SljitTypedJoinInputComplementarySumAccumulator<T> &
SljitGetJoinInputComplementarySumAccumulator(SljitDirectJoinOutputAggregateStrategy &strategy) {
	if (!strategy.join_input_complementary_sum_accumulator) {
		strategy.join_input_complementary_sum_accumulator =
		    make_uniq<SljitTypedJoinInputComplementarySumAccumulator<T>>();
	}
	auto &accumulator = *strategy.join_input_complementary_sum_accumulator;
	if (accumulator.physical_type != GetTypeId<T>()) {
		throw InternalException("SLJIT join-input complementary accumulator type changed within one pipeline");
	}
	return static_cast<SljitTypedJoinInputComplementarySumAccumulator<T> &>(accumulator);
}

template <class T>
static bool SljitMaterializeJoinInputComplementarySumAccumulator(
    SljitTypedJoinInputComplementarySumAccumulator<T> &accumulator,
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
		preaggregate_scratch.group_row_counts.push_back(accumulator.represented_rows[group_idx]);
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

template <class T>
static bool SljitTryMaterializeTypedJoinInputComplementarySumAccumulator(
    SljitDirectJoinOutputAggregateStrategy &strategy, const SljitStringSetComplementarySumDescriptor &classification,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &represented_row_count) {
	auto &base = *strategy.join_input_complementary_sum_accumulator;
	if (base.physical_type != GetTypeId<T>()) {
		return false;
	}
	return SljitMaterializeJoinInputComplementarySumAccumulator(
	    static_cast<SljitTypedJoinInputComplementarySumAccumulator<T> &>(base), classification, payload_lanes,
	    compact_groups, preaggregate_scratch, represented_row_count);
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
		return SljitTryMaterializeTypedJoinInputComplementarySumAccumulator<T>(
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

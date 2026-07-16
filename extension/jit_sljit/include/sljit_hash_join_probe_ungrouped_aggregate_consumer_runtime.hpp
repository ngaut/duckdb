//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_ungrouped_aggregate_consumer_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_lane_runtime.hpp"
#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_hash_join_all_valid_probe_dispatch_runtime.hpp"
#include "sljit_hash_join_rhs_fixed_column_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static inline void SljitAccumulateHugeintInt64Unchecked(hugeint_t &sum, int64_t value) {
	const auto previous_lower = sum.lower;
	sum.lower += static_cast<uint64_t>(value);
	const auto carry = static_cast<uint64_t>(sum.lower < previous_lower);
	const auto sign_extension = value < 0 ? ~uint64_t(0) : uint64_t(0);
	sum.upper = static_cast<int64_t>(static_cast<uint64_t>(sum.upper) + sign_extension + carry);
}

//! One concrete terminal type keeps key-reader dispatch bounded while folding
//! probe, dictionary lookup, null handling, and reduction into the match loop.
//! It deliberately does not publish match selections or row-pointer batches.
struct SljitHashJoinDirectUngroupedAggregateProbeConsumer {
	SljitHashJoinDirectUngroupedAggregateProbeConsumer(SljitNativeRegularHashJoinProbeInput &input_p,
	                                                   AggregatePrimitiveUpdateKind primitive_kind_p,
	                                                   const ExecutionHashJoinRHSFixedColumnSource &source)
	    : input(input_p), primitive_kind(primitive_kind_p), storage_kind(source.storage_kind),
	      layout_column_idx(source.layout_column_idx), layout_column_count(source.layout_column_count),
	      layout_offset(source.layout_offset), dictionary_index_offset(source.dictionary_index_offset),
	      dictionary_data(source.dictionary_data), dictionary_validity(source.dictionary_validity),
	      dictionary_count(source.dictionary_count), all_valid(source.all_valid), local_sum(0, 0) {
	}

	inline void EmitNoChainMatch(idx_t, data_ptr_t row_pointer) {
		matched_count++;
		switch (primitive_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			aggregate_delta++;
			return;
		case AggregatePrimitiveUpdateKind::COUNT:
			aggregate_delta += SourceIsValid(row_pointer) ? 1 : 0;
			return;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
			int64_t value;
			if (TryLoadInt64(row_pointer, value)) {
				SljitAccumulateHugeintInt64Unchecked(local_sum, value);
				aggregate_delta++;
			}
			return;
		}
		default:
			throw InternalException("SLJIT direct hash-join aggregate consumer received an unsupported primitive");
		}
	}

	void Finish() {
		input.selected_count = matched_count;
		input.input_offset = input.count;
		input.resume_row_pointer = nullptr;
		input.finished = true;
	}

	idx_t MatchedCount() const {
		return matched_count;
	}

	idx_t AggregateDelta() const {
		return aggregate_delta;
	}

	const hugeint_t &LocalSum() const {
		return local_sum;
	}

private:
	inline idx_t DictionaryIndex(data_ptr_t row_pointer) const {
		if (!row_pointer || dictionary_index_offset == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT direct hash-join aggregate dictionary source is invalid");
		}
		const auto dictionary_idx = UnsafeNumericCast<idx_t>(Load<uint32_t>(row_pointer + dictionary_index_offset));
		if (dictionary_idx >= dictionary_count) {
			throw InternalException("SLJIT direct hash-join aggregate dictionary index is out of range");
		}
		return dictionary_idx;
	}

	inline bool DictionaryValueIsValid(idx_t dictionary_idx) const {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityMask::GetEntryIndex(dictionary_idx, entry_idx, idx_in_entry);
		return ValidityMask::RowIsValid(dictionary_validity[entry_idx], idx_in_entry);
	}

	inline bool RowValueIsValid(data_ptr_t row_pointer) const {
		idx_t entry_idx;
		idx_t idx_in_entry;
		SljitHashJoinRowValidityMask::GetEntryIndex(layout_column_idx, entry_idx, idx_in_entry);
		return SljitHashJoinRowValidityMask::RowIsValid(
		    SljitHashJoinRowValidityMask(row_pointer, layout_column_count).GetValidityEntryUnsafe(entry_idx),
		    idx_in_entry);
	}

	inline bool SourceIsValid(data_ptr_t row_pointer) const {
		if (!row_pointer) {
			throw InternalException("SLJIT direct hash-join aggregate row source is invalid");
		}
		if (all_valid) {
			return true;
		}
		switch (storage_kind) {
		case ExecutionHashJoinRHSFixedColumnStorageKind::ROW:
			return RowValueIsValid(row_pointer);
		case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY:
			return DictionaryValueIsValid(DictionaryIndex(row_pointer));
		default:
			throw InternalException("SLJIT direct hash-join aggregate storage source is invalid");
		}
	}

	inline bool TryLoadInt64(data_ptr_t row_pointer, int64_t &value) const {
		if (!row_pointer) {
			throw InternalException("SLJIT direct hash-join aggregate row source is invalid");
		}
		switch (storage_kind) {
		case ExecutionHashJoinRHSFixedColumnStorageKind::ROW:
			if (!all_valid && !RowValueIsValid(row_pointer)) {
				return false;
			}
			value = Load<int64_t>(row_pointer + layout_offset);
			return true;
		case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY: {
			const auto dictionary_idx = DictionaryIndex(row_pointer);
			if (!all_valid && !DictionaryValueIsValid(dictionary_idx)) {
				return false;
			}
			value = reinterpret_cast<const int64_t *>(dictionary_data)[dictionary_idx];
			return true;
		}
		default:
			throw InternalException("SLJIT direct hash-join aggregate storage source is invalid");
		}
	}

private:
	SljitNativeRegularHashJoinProbeInput &input;
	AggregatePrimitiveUpdateKind primitive_kind;
	ExecutionHashJoinRHSFixedColumnStorageKind storage_kind;
	idx_t layout_column_idx;
	idx_t layout_column_count;
	idx_t layout_offset;
	idx_t dictionary_index_offset;
	const_data_ptr_t dictionary_data;
	const validity_t *dictionary_validity;
	idx_t dictionary_count;
	bool all_valid;
	idx_t matched_count = 0;
	idx_t aggregate_delta = 0;
	hugeint_t local_sum;
};

template <bool SELECTED>
static bool SljitTryExecuteHashJoinDirectUngroupedAggregateProbe(
    const SljitNativeHashJoinProbePlan &probe_plan, SljitNativeRegularHashJoinProbeInput &native_input,
    SljitHashJoinDirectUngroupedAggregateProbeConsumer &aggregate_consumer) {
	return TryExecuteAllValidSingleKeyNoChainProbeWithConsumer<SELECTED>(probe_plan, native_input, aggregate_consumer);
}

static bool SljitTryExecuteHashJoinDirectUngroupedAggregateProbe(
    bool selected, const SljitNativeHashJoinProbePlan &probe_plan, SljitNativeRegularHashJoinProbeInput &native_input,
    SljitHashJoinDirectUngroupedAggregateProbeConsumer &aggregate_consumer) {
	return selected ? SljitTryExecuteHashJoinDirectUngroupedAggregateProbe<true>(probe_plan, native_input,
	                                                                             aggregate_consumer)
	                : SljitTryExecuteHashJoinDirectUngroupedAggregateProbe<false>(probe_plan, native_input,
	                                                                              aggregate_consumer);
}

static bool SljitTryExecuteHashJoinDirectUngroupedAggregateConsumer(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, const SljitNativeHashJoinProbePlan &probe_plan,
    SljitNativeRegularHashJoinProbeInput &native_input, bool selected, SljitDirectJoinOutputAggregateStrategy &strategy,
    SljitExecutableRegionOp &aggregate_op, DataChunk &probe_input, idx_t &matched_count) {
	auto &plan = strategy.descriptor.direct_ungrouped_aggregate;
	auto &hash_join_binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!plan.Ready()) {
		return false;
	}
	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (plan.primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
	    (!ExecutionGetHashJoinRHSFixedColumnSource(hash_join_binding, plan.rhs_output_idx, rhs_source) ||
	     !SljitHashJoinRHSFixedColumnSourceCanLoad(rhs_source) || rhs_source.type != plan.rhs_type ||
	     rhs_source.physical_type != plan.rhs_physical_type)) {
		return false;
	}
	strategy.descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = strategy.descriptor.input.chunk;
	aggregate_input.Reset();
	aggregate_input.SetChildCardinality(probe_input.size());
	auto &sink_binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, strategy.aggregate_idx, aggregate_op.kind, aggregate_input,
	    aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
	    "SLJIT direct hash-join ungrouped aggregate consumer");
	if (!sink_binding.ready || !sink_binding.aggregate_update.ready || !sink_binding.aggregate_update.primitive.ready) {
		throw InternalException("SLJIT direct hash-join ungrouped aggregate sink binding is incomplete");
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(strategy.aggregate_idx, aggregate_op.aggregate_update.payload_descriptors,
	                                  sink_binding.aggregate_update.primitive);
	if (payload_lanes.size() != 1 || !payload_lanes[0] ||
	    !SljitAggregatePayloadDescriptorMatchesLane(aggregate_op.aggregate_update.payload_descriptors[0],
	                                                *payload_lanes[0])) {
		throw InternalException("SLJIT direct hash-join ungrouped aggregate primitive lane is incomplete");
	}
	auto &lane = *payload_lanes[0];
	auto probe_start = SljitRegionStageStart(runtime);
	switch (plan.primitive_kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR: {
		SljitValidateUngroupedCountStarPrimitiveLane(lane, "SLJIT direct hash-join count-star lane is incomplete");
		break;
	}
	case AggregatePrimitiveUpdateKind::COUNT: {
		SljitValidateUngroupedCountStarPrimitiveLane(lane, "SLJIT direct hash-join count lane is incomplete");
		break;
	}
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
		if (!lane.ready || !lane.sum_hugeint_value || !lane.state_is_set || !lane.row_count) {
			SljitThrowIncompletePrimitiveLane(lane, "SLJIT direct hash-join hugeint sum lane is incomplete: %s",
			                                  "aggregate-primitive-lane-incomplete");
		}
		break;
	}
	default:
		return false;
	}
	SljitHashJoinDirectUngroupedAggregateProbeConsumer consumer(native_input, plan.primitive_kind, rhs_source);
	const auto executed =
	    SljitTryExecuteHashJoinDirectUngroupedAggregateProbe(selected, probe_plan, native_input, consumer);
	if (!executed) {
		return false;
	}
	matched_count = consumer.MatchedCount();
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	if (plan.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    plan.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		*lane.sum_int64_value += UnsafeNumericCast<int64_t>(consumer.AggregateDelta());
	} else if (consumer.AggregateDelta() != 0) {
		*lane.sum_hugeint_value = Hugeint::Add(*lane.sum_hugeint_value, consumer.LocalSum());
		*lane.state_is_set = true;
	}
	*lane.row_count += matched_count;
	if (native_runtime.RecordSinkResult(matched_count, SinkResultType::NEED_MORE_INPUT) !=
	    SinkResultType::NEED_MORE_INPUT) {
		throw InternalException("SLJIT direct hash-join ungrouped aggregate sink unexpectedly blocked");
	}
	RecordSljitRegionStageRuntimePath(
	    runtime, hash_join_idx, hash_join_op.kind,
	    selected ? "regular_probe.all_valid.selected.single_key.no_chain.direct_ungrouped_aggregate_consumer"
	             : "regular_probe.all_valid.flat.single_key.no_chain.direct_ungrouped_aggregate_consumer",
	    probe_start);
	RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind,
	                                            "join_output_probe_consumer_ungrouped_aggregate", matched_count);
	const char *source_path = "join_output_probe_consumer_ungrouped_aggregate.source_none";
	if (plan.primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		source_path = rhs_source.storage_kind == ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY
		                  ? "join_output_probe_consumer_ungrouped_aggregate.dictionary_source"
		                  : "join_output_probe_consumer_ungrouped_aggregate.row_source";
	}
	RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, source_path, matched_count);
	return true;
}

} // namespace duckdb

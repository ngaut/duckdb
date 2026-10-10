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
#include "sljit_hash_join_all_valid_probe_api.hpp"
#include "sljit_hash_join_direct_ungrouped_aggregate_probe_consumer_runtime.hpp"
#include "sljit_hash_join_rhs_fixed_column_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

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
	const auto executed = SljitTryExecuteAllValidSingleKeyNoChainDirectUngroupedAggregateProbe(selected, probe_plan,
	                                                                                           native_input, consumer);
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
	RecordSljitRegionMaterializationElision(runtime, aggregate_op.kind,
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

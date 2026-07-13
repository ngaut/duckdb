//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_primitive_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_direct_update_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitGroupedPrimitiveAggregateSinkKind(ExecutionRegionSinkKind kind) {
	return kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	       kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
}

static SljitBoundGroupedAggregateStrategy SljitBindGroupedAggregateStrategy(const SljitExecutableRegionOp &op) {
	if (op.aggregate_update.fused_payload_update_owns_group_lookup) {
		if (!op.aggregate_update.fused_payload_update.Function()) {
			throw InternalException("SLJIT perfect-hash grouped aggregate strategy has no fused payload function");
		}
		return SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED;
	}
	if (!op.aggregate_update.plan.use_grouped_state_addresses) {
		throw InternalException("SLJIT grouped aggregate strategy has no grouped-state ownership");
	}
	return op.aggregate_update.fused_payload_update.Function()
	           ? SljitBoundGroupedAggregateStrategy::GROUPED_STATE_FUSED
	           : SljitBoundGroupedAggregateStrategy::GROUPED_STATE_PER_PAYLOAD;
}

static void SljitBindGroupedPrimitiveAggregateUpdate(ExecutionOperatorRuntime &native_runtime,
                                                     SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                     SljitExecutableRegionOp &op, DataChunk &input,
                                                     SljitBoundGroupedPrimitiveAggregateUpdate &bound,
                                                     optional_ptr<bool> sink_bound = nullptr) {
	if (sink_bound) {
		*sink_bound = false;
	}
	if (bound.ready) {
		return;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (!SljitGroupedPrimitiveAggregateSinkKind(sink_info.kind) || !op.aggregate_update.plan.UsesPrimitivePayloads()) {
		throw InternalException("SLJIT grouped primitive aggregate update received a non-grouped aggregate");
	}
	auto &binding = SljitBindNativeSink(native_runtime, scratch, op_idx, input, sink_info,
	                                    "aggregate-update-runtime-binding-failed",
	                                    "SLJIT grouped aggregate update sink", sink_bound);
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT grouped aggregate update sink binding did not return a ready aggregate state");
	}
	auto &primitive = binding.aggregate_update.primitive;
	if (!primitive.ready) {
		auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
		throw InternalException("SLJIT grouped aggregate primitive update binding failed: %s", blocker.c_str());
	}
	auto &aggregates = sink_info.aggregates;
	if (aggregates.size() != op.aggregate_update.payloads.size()) {
		throw InternalException("SLJIT grouped aggregate primitive payload count mismatch");
	}
	if (!op.aggregate_update.fused_payload_update.Function() &&
	    aggregates.size() != op.aggregate_update.payload_updates.size()) {
		throw InternalException("SLJIT grouped aggregate primitive payload function count mismatch");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.aggregate_update.payload_descriptors, primitive);
	bound.reduction_lanes = &scratch.GroupedReductionLanes(op_idx, sink_info.aggregate_contract,
	                                                       op.aggregate_update.payload_descriptors, payload_lanes);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	auto strategy = SljitBindGroupedAggregateStrategy(op);
	const bool needs_grouped_state_address_plan = strategy != SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED;
	optional_ptr<ExecutionGroupedAggregateStateAddressBinding> grouped_state;
	if (needs_grouped_state_address_plan || op.aggregate_update.fused_payload_update_owns_group_lookup) {
		auto &state_binding = binding.aggregate_update.grouped_state;
		if (!state_binding.ready || !state_binding.state) {
			auto blocker =
			    state_binding.blocker.empty() ? "aggregate-grouped-state-binding-missing" : state_binding.blocker;
			throw InternalException("SLJIT aggregate grouped-state binding failed: %s", blocker.c_str());
		}
		grouped_state = &state_binding;
	}
	bound.ready = true;
	bound.op_idx = op_idx;
	bound.op = &op;
	bound.payload_descriptors = &op.aggregate_update.payload_descriptors;
	bound.payload_lanes = &payload_lanes;
	bound.payload_scratch = &payload_scratch;
	bound.grouped_state = grouped_state;
	bound.strategy = strategy;
}

static void SljitBindRecordedGroupedPrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                             ExecutionOperatorRuntime &native_runtime,
                                                             SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                             SljitExecutableRegionOp &op, DataChunk &input,
                                                             SljitBoundGroupedPrimitiveAggregateUpdate &bound) {
	auto bind_stage_start = SljitRegionStageStart(runtime);
	bool sink_bound = false;
	SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, op_idx, op, input, bound,
	                                         optional_ptr<bool>(&sink_bound));
	if (sink_bound) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
	}
}

static SinkResultType SljitExecuteBoundGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
    SljitBoundGroupedPrimitiveAggregateUpdate &bound, DataChunk &input, const SelectionVector *execute_sel, idx_t count,
    bool defer_grouped_finish = false, optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (!bound.ready || bound.strategy == SljitBoundGroupedAggregateStrategy::UNBOUND || !bound.op ||
	    !bound.payload_descriptors || !bound.payload_lanes || !bound.reduction_lanes || !bound.payload_scratch) {
		throw InternalException("SLJIT grouped primitive aggregate update executed before binding");
	}
	auto &op = *bound.op;
	auto &payload_descriptors = *bound.payload_descriptors;
	auto &payload_lanes = *bound.payload_lanes;
	auto &reduction_lanes = *bound.reduction_lanes;
	auto &payload_scratch = *bound.payload_scratch;
	optional_ptr<Vector> grouped_state_addresses;
	const bool owns_grouped_state_addresses = bound.strategy != SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED;
	if (owns_grouped_state_addresses) {
		if (!bound.grouped_state) {
			throw InternalException("SLJIT grouped primitive aggregate update is missing grouped state binding");
		}
		auto &grouped_state = *bound.grouped_state;
		if (TryExecuteDirectGroupedAggregateUpdate(runtime, scratch, bound.op_idx, op, input, payload_lanes,
		                                           reduction_lanes, execute_sel, count, grouped_state, payload_scratch,
		                                           defer_grouped_finish, deferred_grouped_finish)) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		grouped_state_addresses = &scratch.AggregateStateAddresses(bound.op_idx);
		if (SljitCanResolveDirectNewGroupedStateAddresses(scratch, bound.op_idx, op, input, execute_sel, count) &&
		    TryResolveDirectNewGroupedStateAddresses(runtime, scratch, bound.op_idx, op, input, grouped_state,
		                                             *grouped_state_addresses, !defer_grouped_finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		} else {
			auto resolve_stage_start = SljitRegionStageStart(runtime);
			ExecuteSljitRegionRecordedOperation(
			    runtime, bound.op_idx, op.kind, "resolve_grouped_state_addresses", resolve_stage_start,
			    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
				    grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses, recorder);
			    });
		}
		RecordSljitRegionRuntimePath(runtime, op.kind, "resolve_grouped_state_addresses");
	}

	switch (bound.strategy) {
	case SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED: {
		auto payload_stage_start = SljitRegionStageStart(runtime);
		if (!bound.grouped_state) {
			throw InternalException("SLJIT perfect-hash grouped primitive update is missing grouped state binding");
		}
		SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update.Function(),
		    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
		    op.aggregate_update.group_source_not_null, op.aggregate_update.plan.sink_info.aggregate_contract,
		    op.aggregate_update.payload_descriptors, payload_lanes, reduction_lanes,
		    bound.grouped_state->perfect_hash_layout, input, execute_sel, count, payload_scratch);
		RecordSljitRegionMaterializationElisionPath(runtime, op.kind,
		                                            "fused_payload_update_owns_perfect_hash_group_lookup");
		RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.kind, "primitive_payload_update_fused",
		                              payload_stage_start);
		break;
	}
	case SljitBoundGroupedAggregateStrategy::GROUPED_STATE_FUSED: {
		if (!grouped_state_addresses) {
			throw InternalException("SLJIT fused grouped aggregate update is missing state addresses");
		}
		auto payload_stage_start = SljitRegionStageStart(runtime);
		grouped_state_addresses->Flatten();
		const auto grouped_state_address_data = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
		SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update.Function(),
		    op.aggregate_update.plan.sink_info.aggregate_contract, op.aggregate_update.payload_descriptors,
		    payload_lanes, reduction_lanes, input, grouped_state_address_data, nullptr, execute_sel, false, count,
		    payload_scratch);
		RecordSljitRegionMaterializationElisionPath(runtime, op.kind,
		                                            "fused_payload_update_with_grouped_state_addresses");
		RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.kind, "primitive_payload_update_fused",
		                              payload_stage_start);
		break;
	}
	case SljitBoundGroupedAggregateStrategy::GROUPED_STATE_PER_PAYLOAD:
		for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
			auto &lane =
			    SljitRequireAggregatePayloadLane(payload_lanes, payload_descriptors, payload_idx,
			                                     "SLJIT grouped aggregate primitive lane invalid for aggregate %llu");
			auto payload_stage_start = SljitRegionStageStart(runtime);
			SljitExecutePrimitiveAggregatePayloadUpdate(
			    op.aggregate_update.payloads[payload_idx], op.aggregate_update.payload_updates[payload_idx].Function(),
			    lane, payload_descriptors[payload_idx], input, execute_sel, count,
			    scratch.ExpressionAdapterScratch(bound.op_idx, payload_idx), grouped_state_addresses);
			RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.kind, "primitive_payload_update",
			                              payload_stage_start);
		}
		RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "primitive_payload_update",
		                                            payload_descriptors.size());
		break;
	default:
		throw InternalException("SLJIT grouped primitive aggregate update has an invalid bound strategy");
	}
	if (owns_grouped_state_addresses) {
		if (defer_grouped_finish) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		} else {
			if (!bound.grouped_state) {
				throw InternalException("SLJIT grouped primitive aggregate update is missing grouped state binding");
			}
			FinishGroupedAggregateStateUpdates(runtime, bound.op_idx, *bound.grouped_state,
			                                   "finish_grouped_state_updates");
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb

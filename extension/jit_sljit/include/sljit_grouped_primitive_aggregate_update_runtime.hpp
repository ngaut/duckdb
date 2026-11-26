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
	if (op.AggregateUpdate().fused_payload_update_owns_group_lookup) {
		if (!op.AggregateUpdate().fused_payload_update.Function()) {
			throw InternalException("SLJIT perfect-hash grouped aggregate strategy has no fused payload function");
		}
		return SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED;
	}
	if (!op.AggregateUpdate().plan.use_grouped_state_addresses) {
		throw InternalException("SLJIT grouped aggregate strategy has no grouped-state ownership");
	}
	return op.AggregateUpdate().fused_payload_update.Function()
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
	if (bound.IsBound()) {
		return;
	}
	auto &sink_info = op.AggregateUpdate().plan.sink_info;
	if (!SljitGroupedPrimitiveAggregateSinkKind(sink_info.Kind()) ||
	    !op.AggregateUpdate().plan.UsesPrimitivePayloads()) {
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
	if (aggregates.size() != op.AggregateUpdate().payloads.size()) {
		throw InternalException("SLJIT grouped aggregate primitive payload count mismatch");
	}
	if (!op.AggregateUpdate().fused_payload_update.Function() &&
	    aggregates.size() != op.AggregateUpdate().payload_updates.size()) {
		throw InternalException("SLJIT grouped aggregate primitive payload function count mismatch");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.AggregateUpdate().payload_descriptors, primitive);
	bound.reduction_lanes = &scratch.GroupedReductionLanes(op_idx, sink_info.contract,
	                                                       op.AggregateUpdate().payload_descriptors, payload_lanes);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	auto strategy = SljitBindGroupedAggregateStrategy(op);
	auto &state_binding = binding.aggregate_update.grouped_state;
	if (!state_binding.ready || !state_binding.state) {
		auto blocker =
		    state_binding.blocker.empty() ? "aggregate-grouped-state-binding-missing" : state_binding.blocker;
		throw InternalException("SLJIT aggregate grouped-state binding failed: %s", blocker.c_str());
	}
	bound.op_idx = op_idx;
	bound.op = &op;
	bound.payload_descriptors = &op.AggregateUpdate().payload_descriptors;
	bound.payload_lanes = &payload_lanes;
	bound.payload_scratch = &payload_scratch;
	bound.grouped_state = &state_binding;
	// Publish the strategy last: it is the sole bound/unbound state, after every invariant is validated.
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
		RecordSljitRegionStageRuntime(runtime, op_idx, op.Kind(), "bind_sink_contract", bind_stage_start);
	}
}

static SinkResultType SljitExecuteBoundGroupedPrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                                       SljitRegionExecutionScratch &scratch,
                                                                       SljitBoundGroupedPrimitiveAggregateUpdate &bound,
                                                                       DataChunk &input,
                                                                       const SelectionVector *execute_sel, idx_t count,
                                                                       bool defer_grouped_finish = false) {
	if (!bound.IsBound()) {
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
		auto &grouped_state = *bound.grouped_state;
		if (TryExecuteDirectGroupedAggregateUpdate(runtime, scratch, bound.op_idx, op, input, payload_lanes,
		                                           reduction_lanes, execute_sel, count, grouped_state, payload_scratch,
		                                           defer_grouped_finish)) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (execute_sel) {
			// Resolving group addresses finds or creates a group for every row of the chunk it reads: the selected
			// rows go on as a chunk of their own, or every rejected row would leave an empty group behind
			DataChunk selected_input;
			selected_input.Initialize(runtime.GetAllocator(), input.GetTypes(), count);
			input.Copy(selected_input, *execute_sel, count);
			return SljitExecuteBoundGroupedPrimitiveAggregateUpdate(runtime, scratch, bound, selected_input, nullptr,
			                                                        count, defer_grouped_finish);
		}
		grouped_state_addresses = &scratch.AggregateStateAddresses(bound.op_idx);
		auto resolve_stage_start = SljitRegionStageStart(runtime);
		ExecuteSljitRegionRecordedOperation(
		    runtime, bound.op_idx, op.Kind(), "resolve_grouped_state_addresses", resolve_stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses, recorder);
		    });
		RecordSljitRegionRuntimePath(runtime, op.Kind(), "resolve_grouped_state_addresses");
	}

	switch (bound.strategy) {
	case SljitBoundGroupedAggregateStrategy::PERFECT_HASH_FUSED: {
		auto payload_stage_start = SljitRegionStageStart(runtime);
		SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
		    op.AggregateUpdate().payloads, op.AggregateUpdate().fused_payload_update.Function(),
		    op.AggregateUpdate().fused_payload_group_witness, op.AggregateUpdate().plan.sink_info.groups,
		    op.AggregateUpdate().plan.group_expressions, op.AggregateUpdate().group_source_not_null,
		    op.AggregateUpdate().plan.sink_info.contract, op.AggregateUpdate().payload_descriptors,
		    op.AggregateUpdate().payload_source_layout, op.AggregateUpdate().combined_payload_source_indices,
		    op.AggregateUpdate().combined_payload_source_not_null, payload_lanes, reduction_lanes,
		    bound.grouped_state->perfect_hash_layout, input, execute_sel, count, payload_scratch);
		RecordSljitRegionMaterializationElision(runtime, op.Kind(),
		                                        "fused_payload_update_owns_perfect_hash_group_lookup");
		RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.Kind(), "primitive_payload_update_fused",
		                              payload_stage_start);
		break;
	}
	case SljitBoundGroupedAggregateStrategy::GROUPED_STATE_FUSED: {
		if (!grouped_state_addresses) {
			throw InternalException("SLJIT fused grouped aggregate update is missing state addresses");
		}
		auto payload_stage_start = SljitRegionStageStart(runtime);
		// constant or dictionary groups resolve one address per distinct value, which each row reaches through the
		// selection
		UnifiedVectorFormat grouped_state_address_format;
		grouped_state_addresses->ToUnifiedFormat(grouped_state_address_format);
		const auto grouped_state_address_data = UnifiedVectorFormat::GetData<uintptr_t>(grouped_state_address_format);
		SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
		    op.AggregateUpdate().payloads, op.AggregateUpdate().fused_payload_update.Function(),
		    op.AggregateUpdate().plan.sink_info.contract, op.AggregateUpdate().payload_descriptors,
		    op.AggregateUpdate().payload_source_layout, op.AggregateUpdate().combined_payload_source_indices,
		    op.AggregateUpdate().combined_payload_source_not_null, payload_lanes, reduction_lanes, input,
		    grouped_state_address_data, grouped_state_address_format.sel->data(), execute_sel, false, count,
		    payload_scratch);
		RecordSljitRegionMaterializationElision(runtime, op.Kind(),
		                                        "fused_payload_update_with_grouped_state_addresses");
		RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.Kind(), "primitive_payload_update_fused",
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
			    op.AggregateUpdate().payloads[payload_idx],
			    op.AggregateUpdate().payload_updates[payload_idx].Function(), lane, payload_descriptors[payload_idx],
			    input, execute_sel, count, scratch.ExpressionAdapterScratch(bound.op_idx, payload_idx),
			    grouped_state_addresses);
			RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.Kind(), "primitive_payload_update",
			                              payload_stage_start);
		}
		RecordSljitRegionMaterializationElision(runtime, op.Kind(), "primitive_payload_update",
		                                        payload_descriptors.size());
		break;
	default:
		throw InternalException("SLJIT grouped primitive aggregate update has an invalid bound strategy");
	}
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb

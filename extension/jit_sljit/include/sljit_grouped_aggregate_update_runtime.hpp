//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_direct_update_runtime.hpp"

namespace duckdb {

static SinkResultType SljitExecuteNativeFilteredAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                                ExecutionOperatorRuntime &native_runtime,
                                                                SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                                SljitExecutableRegionOp &op, DataChunk &input) {
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	auto &primitive = binding.aggregate_update.primitive;
	if (!primitive.ready) {
		auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
		throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
	}
	auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
	if (aggregates.empty() || aggregates.size() != op.aggregate_update.payloads.size()) {
		throw InternalException("SLJIT filtered aggregate update requires matching primitive aggregate lanes");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
	if (payload_lanes.size() != aggregates.size()) {
		throw InternalException("SLJIT filtered aggregate primitive lane count mismatch");
	}

	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	if (op.aggregate_update.filtered_update.owns_perfect_hash_group_lookup) {
		auto &grouped_state = binding.aggregate_update.grouped_state;
		SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.filtered_update.payloads, op.aggregate_update.filtered_update.function, aggregates,
		    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
		    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, grouped_state.perfect_hash_layout,
		    input, nullptr, input.size(), payload_scratch);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_perfect_hash_update",
		                                  aggregate_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update", input.size());
	} else {
		SljitExecuteFilteredPrimitiveAggregateUpdate(op.aggregate_update.filtered_update, aggregates, payload_lanes,
		                                             input, input.size(), payload_scratch);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_primitive_update", aggregate_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

static SinkResultType
SljitExecutePrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                     SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                     DataChunk &input, const SelectionVector *execute_sel = nullptr,
                                     idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false,
                                     optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	if (!op.aggregate_update.plan.use_primitive_payloads) {
		throw InternalException("SLJIT primitive aggregate update requires primitive payloads");
	}
	{
		auto &primitive = binding.aggregate_update.primitive;
		if (!primitive.ready) {
			auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
			throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
		}
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		if (aggregates.size() != op.aggregate_update.payloads.size()) {
			throw InternalException("SLJIT aggregate primitive payload count mismatch");
		}
		if (!op.aggregate_update.fused_payload_update_function &&
		    aggregates.size() != op.aggregate_update.payload_update_functions.size()) {
			throw InternalException("SLJIT aggregate primitive payload function count mismatch");
		}
		auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
		auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
		const bool needs_grouped_state_address_plan = NeedsGroupedAggregateStateAddressPlan(op.aggregate_update);
		optional_ptr<Vector> grouped_state_addresses;
		if (needs_grouped_state_address_plan) {
			auto &grouped_state = binding.aggregate_update.grouped_state;
			if (!grouped_state.ready || !grouped_state.state) {
				auto blocker =
				    grouped_state.blocker.empty() ? "aggregate-grouped-state-binding-missing" : grouped_state.blocker;
				throw InternalException("SLJIT aggregate grouped-state binding failed: %s", blocker.c_str());
			}
			if (TryExecuteDirectGroupedAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes, execute_sel,
			                                           count, grouped_state, payload_scratch, defer_grouped_finish,
			                                           deferred_grouped_finish)) {
				return SinkResultType::NEED_MORE_INPUT;
			}
			grouped_state_addresses = &scratch.AggregateStateAddresses(op_idx);
			if (SljitCanResolveDirectNewGroupedStateAddresses(scratch, op_idx, op, input, execute_sel, count) &&
			    TryResolveDirectNewGroupedStateAddresses(runtime, scratch, op_idx, op, input, grouped_state,
			                                             *grouped_state_addresses, !defer_grouped_finish)) {
				MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			} else {
				auto resolve_stage_start = SljitRegionStageStart(runtime);
				ExecuteSljitRegionRecordedOperation(
				    runtime, op_idx, op.kind, "resolve_grouped_state_addresses", resolve_stage_start,
				    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
					    grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses, recorder);
				    });
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_resolve", input.size());
			}
			RecordSljitRegionRuntimePath(runtime, op.kind, "resolve_grouped_state_addresses");
		}
		if (op.aggregate_update.fused_payload_update_function) {
			auto payload_stage_start = SljitRegionStageStart(runtime);
			if (op.aggregate_update.fused_payload_update_owns_group_lookup) {
				auto &grouped_state = binding.aggregate_update.grouped_state;
				SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
				    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
				    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
				    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes,
				    grouped_state.perfect_hash_layout, input, execute_sel, count, payload_scratch);
				RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update_owns_perfect_hash_group_lookup");
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update", count);
			} else if (op.aggregate_update.plan.use_grouped_state_addresses) {
				if (!grouped_state_addresses) {
					throw InternalException("SLJIT fused grouped aggregate update is missing state addresses");
				}
				grouped_state_addresses->Flatten();
				const auto grouped_state_address_data = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
				SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
				    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
				    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, input,
				    grouped_state_address_data, nullptr, execute_sel, false, count, payload_scratch);
				RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update_with_grouped_state_addresses");
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_payload_update", count);
			} else {
				SljitExecuteFusedPrimitiveAggregatePayloadUpdate(
				    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
				    payload_lanes, input, execute_sel, count, payload_scratch);
				RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update");
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", count);
			}
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update_fused",
			                              payload_stage_start);
		} else {
			for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
				auto &aggregate = aggregates[payload_idx];
				auto lane = payload_lanes[payload_idx];
				if (!lane) {
					throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
					                        static_cast<unsigned long long>(aggregate.aggregate_index));
				}
				auto payload_stage_start = SljitRegionStageStart(runtime);
				SljitExecutePrimitiveAggregatePayloadUpdate(
				    op.aggregate_update.payloads[payload_idx],
				    op.aggregate_update.payload_update_functions[payload_idx], *lane, input, execute_sel, count,
				    scratch.ExpressionAdapterScratch(op_idx, payload_idx), grouped_state_addresses);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update",
				                              payload_stage_start);
			}
			RecordSljitRegionRuntimePath(runtime, op.kind, "primitive_payload_update", aggregates.size());
			RecordSljitRegionMaterializationBoundary(
			    runtime, op.kind, grouped_state_addresses ? "address_vector_payload_update" : "direct_state_update",
			    count);
		}
		if (needs_grouped_state_address_plan) {
			if (defer_grouped_finish) {
				MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			} else {
				FinishGroupedAggregateStateUpdates(runtime, op_idx, binding.aggregate_update.grouped_state,
				                                   "finish_grouped_state_updates");
			}
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
}

static SinkResultType
SljitExecuteDuckDBAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                  SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                  DataChunk &input, const SelectionVector *execute_sel = nullptr,
                                  idx_t count = DConstants::INVALID_INDEX) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	if (execute_sel != nullptr || count != input.size()) {
		throw InternalException("SLJIT native aggregate sink update requires a materialized input chunk");
	}
	if (!binding.aggregate_update.state) {
		throw InternalException("SLJIT aggregate update sink binding is missing native sink state");
	}
	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto sink_result = binding.aggregate_update.state->Sink(input);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "native_sink_update", aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "native_sink_update", input.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "native_sink_update", input.size());
	return sink_result;
}

static SinkResultType
SljitExecuteNativeAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                  SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                  DataChunk &input, const SelectionVector *execute_sel = nullptr,
                                  idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false,
                                  optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	if (op.aggregate_update.plan.use_primitive_payloads) {
		return SljitExecutePrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel,
		                                            count, defer_grouped_finish, deferred_grouped_finish);
	}
	return SljitExecuteDuckDBAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel, count);
}

} // namespace duckdb

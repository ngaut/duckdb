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

enum class SljitDistinctCountPointerPayloadStorageStrategy : uint8_t { ADAPTIVE_INLINE_GROUP_PAYLOADS, GLOBAL_PAIR_SET };

static SljitDistinctCountPointerPayloadStorageStrategy
SljitSelectDistinctCountPointerPayloadStorageStrategy(const SljitExecutableRegionOp &op, idx_t count,
                                                      idx_t &payload_set_target) {
	payload_set_target = DConstants::INVALID_INDEX;
	const auto estimated_payload_count = MaxValue(op.aggregate_update.plan.estimated_input_count, count);
	if (estimated_payload_count == 0) {
		return SljitDistinctCountPointerPayloadStorageStrategy::ADAPTIVE_INLINE_GROUP_PAYLOADS;
	}
	const auto inline_payload_capacity = EXECUTION_DISTINCT_COUNT_POINTER_INLINE_PAYLOAD_CAPACITY;
	auto &reserve = op.aggregate_update.plan.group_reserve;
	if (!reserve.CanReserve()) {
		return SljitDistinctCountPointerPayloadStorageStrategy::ADAPTIVE_INLINE_GROUP_PAYLOADS;
	}
	if (reserve.group_count > NumericLimits<idx_t>::Maximum() / inline_payload_capacity) {
		return SljitDistinctCountPointerPayloadStorageStrategy::ADAPTIVE_INLINE_GROUP_PAYLOADS;
	}
	const auto inline_target_payload_capacity = reserve.group_count * (inline_payload_capacity / 2);
	if (estimated_payload_count <= inline_target_payload_capacity) {
		return SljitDistinctCountPointerPayloadStorageStrategy::ADAPTIVE_INLINE_GROUP_PAYLOADS;
	}
	payload_set_target = estimated_payload_count;
	return SljitDistinctCountPointerPayloadStorageStrategy::GLOBAL_PAIR_SET;
}

static void SljitTryUseGlobalDistinctCountPointerPayloadSet(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                            const SljitExecutableRegionOp &op,
                                                            ExecutionDistinctCountPointerUpdateBinding &distinct,
                                                            idx_t count) {
	idx_t global_payload_set_target;
	if (!distinct.ready || !distinct.state ||
	    SljitSelectDistinctCountPointerPayloadStorageStrategy(op, count, global_payload_set_target) !=
	        SljitDistinctCountPointerPayloadStorageStrategy::GLOBAL_PAIR_SET) {
		return;
	}
	auto payload_set_stage_start = SljitRegionStageStart(runtime);
	if (distinct.state->UseGlobalPayloadSet(global_payload_set_target)) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_count_pointer_global_payload_set", count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "distinct_count_pointer_global_payload_set",
		                              payload_set_stage_start);
	}
}

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

static SinkResultType SljitExecuteDistinctCountPointerAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, bool defer_grouped_finish = false, optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT distinct count-pointer aggregate sink binding did not return a ready state");
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	auto &distinct = binding.aggregate_update.distinct_count_pointer;
	if (!grouped_state.ready || !grouped_state.state || !distinct.ready || !distinct.state_addresses ||
	    !distinct.state || distinct.payload_index >= input.ColumnCount() ||
	    distinct.state_value_offset == DConstants::INVALID_INDEX) {
		auto blocker = distinct.blocker.empty() ? "distinct-count-pointer-binding-missing" : distinct.blocker;
		throw InternalException("SLJIT distinct count-pointer aggregate binding failed: %s", blocker.c_str());
	}
	if (input.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	SljitTryUseGlobalDistinctCountPointerPayloadSet(runtime, op_idx, op, distinct, input.size());

	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	const bool finish = !defer_grouped_finish;
	struct DistinctCountPointerSelectedUpdateState {
		ExecutionDistinctCountPointerUpdateState *state = nullptr;
		Vector *payload = nullptr;
		idx_t state_value_offset = DConstants::INVALID_INDEX;
		bool success = true;
	};
	auto selected_payload_update = [](const uintptr_t *addresses, const sel_t *address_sel, const sel_t *execute_sel,
	                                  idx_t count, void *state_ptr) {
		auto &state = *reinterpret_cast<DistinctCountPointerSelectedUpdateState *>(state_ptr);
		state.success =
		    state.success && state.state->AddSelectedPayloads(addresses, address_sel, execute_sel, *state.payload,
		                                                      count, state.state_value_offset);
	};
	DistinctCountPointerSelectedUpdateState selected_update_state;
	selected_update_state.state = distinct.state.get();
	selected_update_state.payload = &input.data[distinct.payload_index];
	selected_update_state.state_value_offset = distinct.state_value_offset;
	auto selected_resolve_stage_start = SljitRegionStageStart(runtime);
	auto selected_resolved = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "distinct_count_pointer_selected_payload_update", selected_resolve_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        input, distinct.state_address_sink_info, selected_payload_update, &selected_update_state, recorder,
		        false);
	    });
	if (selected_resolved) {
		if (!selected_update_state.success) {
			throw InternalException("SLJIT distinct count-pointer selected payload update failed");
		}
		if (finish) {
			FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state,
			                                   "distinct_count_pointer_finish_state_update");
		} else {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		}
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_selected_payload_update",
		                                  selected_resolve_stage_start);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_aggregate_update",
		                                  aggregate_stage_start);
		RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_count_pointer_selected_payload_update", input.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "distinct_count_pointer_selected_payload_update",
		                                         input.size());
		return SinkResultType::NEED_MORE_INPUT;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_selected_payload_update_miss",
	                                  selected_resolve_stage_start);

	auto &state_addresses = *distinct.state_addresses;
	state_addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(state_addresses).SetAllValid(input.size());
	FlatVector::SetSize(state_addresses, input.size());

	auto resolve_stage_start = SljitRegionStageStart(runtime);
	auto resolved = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "distinct_count_pointer_resolve_addresses", resolve_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryResolveDistinctCountPointerAddresses(input, distinct.state_address_sink_info,
		                                                                        state_addresses, recorder, false);
	    });
	if (!resolved) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_resolve_addresses_miss",
		                                  resolve_stage_start);
		throw InternalException("SLJIT distinct count-pointer aggregate could not resolve grouped state addresses");
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_resolve_addresses",
	                                  resolve_stage_start);

	auto payload_stage_start = SljitRegionStageStart(runtime);
	if (!distinct.state->AddPayloads(state_addresses, input.data[distinct.payload_index], input.size(),
	                                 distinct.state_value_offset)) {
		throw InternalException("SLJIT distinct count-pointer aggregate failed to add payload values");
	}
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state,
		                                   "distinct_count_pointer_finish_state_update");
	} else {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_payload_set_update",
	                                  payload_stage_start);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_aggregate_update",
	                                  aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_count_pointer_payload_set_update", input.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "distinct_count_pointer_payload_set_update",
	                                         input.size());
	return SinkResultType::NEED_MORE_INPUT;
}

static bool SljitTryExecuteDistinctCountPointerGroupKeyAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &groups, Vector &payload, bool defer_grouped_finish = false,
    optional_ptr<bool> deferred_grouped_finish = nullptr,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT distinct count-pointer aggregate sink binding did not return a ready state");
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	auto &distinct = binding.aggregate_update.distinct_count_pointer;
	if (!grouped_state.ready || !grouped_state.state || !distinct.ready || !distinct.state ||
	    distinct.state_value_offset == DConstants::INVALID_INDEX) {
		auto blocker = distinct.blocker.empty() ? "distinct-count-pointer-binding-missing" : distinct.blocker;
		throw InternalException("SLJIT distinct count-pointer aggregate binding failed: %s", blocker.c_str());
	}
	if (groups.size() == 0) {
		return true;
	}

	struct DistinctCountPointerSelectedUpdateState {
		ExecutionDistinctCountPointerUpdateState *state = nullptr;
		Vector *payload = nullptr;
		idx_t state_value_offset = DConstants::INVALID_INDEX;
		bool success = true;
	};
	auto selected_payload_update = [](const uintptr_t *addresses, const sel_t *address_sel, const sel_t *execute_sel,
	                                  idx_t count, void *state_ptr) {
		auto &state = *reinterpret_cast<DistinctCountPointerSelectedUpdateState *>(state_ptr);
		state.success =
		    state.success && state.state->AddSelectedPayloads(addresses, address_sel, execute_sel, *state.payload,
		                                                      count, state.state_value_offset);
	};

	DistinctCountPointerSelectedUpdateState selected_update_state;
	selected_update_state.state = distinct.state.get();
	selected_update_state.payload = &payload;
	selected_update_state.state_value_offset = distinct.state_value_offset;

	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	const bool finish = !defer_grouped_finish;
	auto selected_resolve_stage_start = SljitRegionStageStart(runtime);
	auto selected_resolved = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "distinct_count_pointer_group_key_selected_payload_update",
	    selected_resolve_stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateGroupKeysWithSelectedStateAddresses(
		        groups, distinct.state_address_sink_info, selected_payload_update, &selected_update_state, recorder,
		        false, nullptr, dense_domain);
	    });
	if (!selected_resolved) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "distinct_count_pointer_group_key_selected_payload_update_miss",
		                                  selected_resolve_stage_start);
		return false;
	}
	if (!selected_update_state.success) {
		throw InternalException("SLJIT distinct count-pointer group-key payload update failed");
	}
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state,
		                                   "distinct_count_pointer_finish_state_update");
	} else {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "distinct_count_pointer_group_key_selected_payload_update",
	                                  selected_resolve_stage_start);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_aggregate_update",
	                                  aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_count_pointer_group_key_update", groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "distinct_count_pointer_group_key_update",
	                                         groups.size());
	return true;
}

static bool SljitTryExecuteDistinctCountPointerRowPointerGroupAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, Vector &payload, bool defer_grouped_finish = false,
    optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT distinct count-pointer aggregate sink binding did not return a ready state");
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	auto &distinct = binding.aggregate_update.distinct_count_pointer;
	if (!grouped_state.ready || !grouped_state.state || !distinct.ready || !distinct.state ||
	    distinct.state_value_offset == DConstants::INVALID_INDEX) {
		auto blocker = distinct.blocker.empty() ? "distinct-count-pointer-binding-missing" : distinct.blocker;
		throw InternalException("SLJIT distinct count-pointer aggregate binding failed: %s", blocker.c_str());
	}
	if (count == 0) {
		return true;
	}
	if (payload_input.size() != count || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    group_sources.empty()) {
		return false;
	}

	struct DistinctCountPointerTargetUpdateState {
		ExecutionDistinctCountPointerUpdateState *state = nullptr;
		Vector *payload = nullptr;
		idx_t state_value_offset = DConstants::INVALID_INDEX;
		bool success = true;
	};
	auto update_target_span = [](const ExecutionGroupedAggregateStateTargetSpan &span,
	                             DistinctCountPointerTargetUpdateState &state) {
		if (!span.HasTargets()) {
			return;
		}
		state.success =
		    state.success && state.state->AddSelectedPayloads(span.addresses, span.address_sel, span.row_sel,
		                                                      *state.payload, span.count, state.state_value_offset);
	};

	DistinctCountPointerTargetUpdateState update_state;
	update_state.state = distinct.state.get();
	update_state.payload = &payload;
	update_state.state_value_offset = distinct.state_value_offset;

	ExecutionGroupedAggregateStateTargetBatch targets;
	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	const bool finish = !defer_grouped_finish;
	auto target_stage_start = SljitRegionStageStart(runtime);
	auto found_targets = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "distinct_count_pointer_row_pointer_group_key_update", target_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, distinct.state_address_sink_info, targets, recorder);
	    });
	if (!found_targets) {
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "distinct_count_pointer_row_pointer_group_key_update_miss", target_stage_start);
		return false;
	}
	SljitTryUseGlobalDistinctCountPointerPayloadSet(runtime, op_idx, op, distinct, count);
	for (auto &span : targets.Spans()) {
		update_target_span(span, update_state);
	}
	if (!update_state.success) {
		throw InternalException("SLJIT distinct count-pointer row-pointer payload update failed");
	}
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state,
		                                   "distinct_count_pointer_finish_state_update");
	} else {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_row_pointer_group_key_update",
	                                  target_stage_start);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "distinct_count_pointer_aggregate_update",
	                                  aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_count_pointer_row_pointer_group_key_update", count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "distinct_count_pointer_row_pointer_group_key_update",
	                                         count);
	return true;
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
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	if (op.aggregate_update.plan.sink_info.aggregate_contract.distinct_count_pointer_keys) {
		if (execute_sel != nullptr || count != input.size()) {
			throw InternalException("SLJIT distinct count-pointer aggregate requires a materialized input chunk");
		}
		return SljitExecuteDistinctCountPointerAggregateUpdate(runtime, binding, op_idx, op, input,
		                                                       defer_grouped_finish, deferred_grouped_finish);
	}
	if (op.aggregate_update.plan.use_primitive_payloads) {
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

} // namespace duckdb

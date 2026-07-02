//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_two_join_direct_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_between_join_sidecar_runtime.hpp"
#include "sljit_between_join_sidecar_plan_runtime.hpp"
#include "sljit_final_projection_aggregate_descriptor.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

template <class AGGREGATE_SINK>
static SljitDirectFinalProjectionAggregateUpdateResult SljitTryDirectFinalProjectionAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitTwoJoinGroupedAggregateRouteLayout &layout,
    SljitTwoJoinGroupedAggregateRouteState &state, DataChunk &input, AGGREGATE_SINK &aggregate_sink) {
	SljitDirectFinalProjectionAggregateUpdateResult result;
	if (input.size() == 0) {
		return result;
	}

	layout.AssertOperatorBounds(ops.size());
	auto &second_join_projection_op = ops[layout.second_projection_idx];
	auto &final_projection_op = ops[layout.final_projection_idx];
	auto &aggregate_op = ops[layout.aggregate_idx];
	auto &second_join_batch = state.second_join_batch;
	auto &between_join_sidecars = state.between_join_sidecars;
	auto &final_aggregate = state.final_aggregate;
	auto &projection_skips = state.projection_skips;
	auto &second_join_omissions = state.second_join_omissions;

	auto record_split_payload_unsupported = [&](const string &reason, idx_t count) {
		auto path = string("direct_projected_group_payload_update_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), count);
	};

	if (!SljitBuildFinalSplitPayloadDescriptor(final_aggregate, final_projection_op, aggregate_op, input)) {
		record_split_payload_unsupported(final_aggregate.split_payload_descriptor.blocker, input.size());
		return result;
	}
	if (aggregate_sink.FlushProjectedBatch()) {
		result.stop_pipeline = true;
		return result;
	}
	auto second_join_projection_column_is_omitted = [&](idx_t projection_idx) {
		return projection_skips.SecondJoinProjectionColumnIsOmitted(second_join_omissions, projection_idx);
	};
	if (SljitBuildFinalRowPointerGroupDescriptor(final_aggregate, final_projection_op, aggregate_op, input,
	                                             second_join_projection_column_is_omitted)) {
		if (SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
		        runtime, native_runtime, scratch, layout.aggregate_idx, aggregate_op, input,
		        scratch.HashJoinRowPointers(layout.second_hash_join_idx),
		        final_aggregate.row_pointer_aggregate.group_sources,
		        final_aggregate.row_pointer_aggregate.payload_source_indices, true,
		        aggregate_sink.DeferredGroupedFinishPtr())) {
			result.processed_batch = true;
			result.handled = true;
			return result;
		}
		record_split_payload_unsupported("row_pointer_grouped_lookup_update", input.size());
	} else if (!final_aggregate.row_pointer_aggregate.Blocker().empty()) {
		auto path = string("direct_final_row_pointer_grouped_lookup_update_unsupported.") +
		            final_aggregate.row_pointer_aggregate.Blocker();
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), input.size());
	}

	final_aggregate.group_key_batch.Ensure(runtime.GetAllocator(), final_aggregate.group_key_types);
	final_aggregate.group_key_batch.Reset();
	auto &final_group_keys = final_aggregate.group_key_batch.chunk;
	vector<SljitDirectProjectionBatchPassthrough> final_group_key_passthroughs;
	optional_ptr<const vector<SljitDirectProjectionBatchPassthrough>> final_group_key_passthroughs_ptr;
	if (SljitBuildFinalGroupKeyPassthroughs(scratch, layout.second_hash_join_idx, between_join_sidecars,
	                                        final_aggregate, second_join_projection_op, final_projection_op,
	                                        second_join_batch.size(), final_group_key_passthroughs)) {
		final_group_key_passthroughs_ptr = &final_group_key_passthroughs;
	}
	if (!SljitTryMaterializeSelectedProjectionToBatch(
	        runtime, scratch, layout.final_projection_idx, final_projection_op, input, final_group_keys,
	        final_aggregate.group_projection_indices, optional_ptr<Vector>(&final_aggregate.group_key_hashes),
	        final_group_key_passthroughs_ptr)) {
		record_split_payload_unsupported("group_key_projection", input.size());
		return result;
	}

	auto &binding =
	    SljitBindRecordedNativeSink(runtime, native_runtime, scratch, layout.aggregate_idx, aggregate_op.kind,
	                                final_group_keys, aggregate_op.aggregate_update.plan.sink_info,
	                                "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
	    !binding.aggregate_update.grouped_state.state || !binding.aggregate_update.primitive.ready) {
		record_split_payload_unsupported("sink_binding", input.size());
		return result;
	}

	auto &aggregates = aggregate_op.aggregate_update.plan.sink_info.aggregates;
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(layout.aggregate_idx, aggregates, binding.aggregate_update.primitive);
	const bool finish = false;
	bool updated = false;
	if (final_aggregate.split_payload_uses_fused_update) {
		if (!SljitCanExecuteDirectGroupedFusedPayloadUpdate(scratch, layout.aggregate_idx, aggregate_op, input,
		                                                    payload_lanes, nullptr, input.size())) {
			record_split_payload_unsupported("fused_payload_update_shape", input.size());
			return result;
		}
		auto &payload_scratch = scratch.AggregatePayloadScratch(layout.aggregate_idx);
		updated = TryExecuteDirectProjectedGroupedFusedPayloadUpdate(
		    runtime, scratch, layout.aggregate_idx, aggregate_op, final_group_keys, input,
		    final_aggregate.payload_source_indices, payload_lanes, binding.aggregate_update.grouped_state,
		    payload_scratch, finish, optional_ptr<Vector>(&final_aggregate.group_key_hashes));
	} else {
		auto stage_start = SljitRegionStageStart(runtime);
		updated = ExecuteSljitRegionRecordedOperation(
		    runtime, layout.aggregate_idx, aggregate_op.kind, "direct_projected_group_payload_update", stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return binding.aggregate_update.grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
			        final_group_keys, input, final_aggregate.payload_source_indices,
			        aggregate_op.aggregate_update.plan.sink_info, payload_lanes, recorder, finish,
			        optional_ptr<Vector>(&final_aggregate.group_key_hashes));
		    });
		if (updated) {
			RecordSljitRegionStageRuntime(runtime, layout.aggregate_idx, aggregate_op.kind,
			                              "direct_projected_group_payload_update", stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind, "projected_group_payload_update",
			                                         input.size());
		} else {
			RecordSljitRegionStageRuntimePath(runtime, layout.aggregate_idx, aggregate_op.kind,
			                                  "direct_projected_group_payload_update_miss", stage_start);
		}
	}
	if (!updated) {
		return result;
	}
	MarkDeferredGroupedFinish(true, aggregate_sink.DeferredGroupedFinishPtr());
	RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_group_payload_update", input.size());
	result.processed_batch = true;
	result.handled = true;
	return result;
}

template <class AGGREGATE_SINK>
static SljitDirectSecondJoinProjectionResult
SljitTryDirectSecondJoinProjection(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                   vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
                                   const SljitTwoJoinGroupedAggregateRouteLayout &layout,
                                   SljitTwoJoinGroupedAggregateRouteState &state, DataChunk &second_join_input,
                                   DataChunk &second_join_output, AGGREGATE_SINK &aggregate_sink) {
	SljitDirectSecondJoinProjectionResult result;
	if (second_join_output.size() == 0 || layout.aggregate_idx <= layout.second_projection_idx) {
		return result;
	}

	layout.AssertOperatorBounds(ops.size());
	auto &second_join_projection_op = ops[layout.second_projection_idx];
	auto &final_projection_op = ops[layout.final_projection_idx];
	auto &second_join_batch = state.second_join_batch;
	auto &between_join_sidecars = state.between_join_sidecars;
	auto &projection_skips = state.projection_skips;
	auto &second_join_omissions = state.second_join_omissions;
	auto &direct_projection = scratch.TemporaryChunk(layout.second_projection_idx);
	direct_projection.Reset();

	vector<uint8_t> second_join_projection_skip;
	optional_ptr<const vector<uint8_t>> second_join_projection_skip_ptr;
	if (!projection_skips.BuildSecondProjectionSkip(second_join_omissions, second_join_projection_op.projections.size(),
	                                                second_join_projection_skip, second_join_projection_skip_ptr)) {
		return result;
	}
	if (!SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
	        runtime, ops, scratch, layout.second_hash_join_idx, layout.second_projection_idx, second_join_projection_op,
	        second_join_input, scratch.FilterSelection(layout.second_hash_join_idx),
	        scratch.HashJoinRowPointers(layout.second_hash_join_idx), second_join_output, direct_projection, nullptr,
	        nullptr, second_join_projection_skip_ptr)) {
		if (second_join_omissions.Any()) {
			throw InternalException("SLJIT second-join projection skip became unsupported after descriptor preflight");
		}
		return result;
	}
	if (!SljitCopySecondJoinPrecomputedPayloadsToProjection(
	        runtime, scratch, layout.second_hash_join_idx, layout.second_projection_idx, second_join_projection_op.kind,
	        between_join_sidecars, second_join_omissions.precomputed_payload, second_join_batch.size(),
	        direct_projection, second_join_output.size())) {
		throw InternalException("SLJIT precomputed payload passthrough became unsupported");
	}

	result.handled = true;
	RecordSljitRegionRuntimePath(runtime, second_join_projection_op.kind, "direct_second_join_projection",
	                             second_join_output.size());
	auto final_aggregate_update = SljitTryDirectFinalProjectionAggregateUpdate(
	    runtime, native_runtime, ops, scratch, layout, state, direct_projection, aggregate_sink);
	if (final_aggregate_update.stop_pipeline) {
		result.stop_pipeline = true;
		return result;
	}
	result.processed_batch = final_aggregate_update.processed_batch;
	if (final_aggregate_update.handled) {
		return result;
	}
	if (second_join_omissions.compressed_group_key) {
		throw InternalException("SLJIT compressed group-key aggregate update skip became unsupported");
	}

	bool final_projection_handled = false;
	if (aggregate_sink.TryAppendDirectProjectedBatch(
	        direct_projection, final_projection_handled, [&](DataChunk &batch) {
		        return SljitTryDirectMaterializeFixedProjectionToBatch(runtime, scratch, layout.final_projection_idx,
		                                                               final_projection_op, direct_projection, batch);
	        })) {
		result.stop_pipeline = true;
		return result;
	}
	if (final_projection_handled) {
		return result;
	}
	SljitProjectOptionalPostJoinProjectionChain(
	    runtime, scratch, ops, layout.post_second_projection_idx, layout.final_projection_idx, direct_projection,
	    result.projected, "post_second_join_reference_projection", "post_second_join_batch_projection");
	return result;
}

template <class FLUSH_SECOND_JOIN_BATCH>
static SljitDirectBetweenJoinProjectionAppendResult SljitTryAppendDirectBetweenJoinProjection(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitTwoJoinGroupedAggregateRouteLayout &layout,
    SljitTwoJoinGroupedAggregateRouteState &state, DataChunk &first_join_input, DataChunk &first_join_output,
    idx_t selected_count, FLUSH_SECOND_JOIN_BATCH &&flush_second_join_batch) {
	SljitDirectBetweenJoinProjectionAppendResult result;
	if (selected_count == 0) {
		return result;
	}

	layout.AssertOperatorBounds(ops.size());
	auto &first_hash_join_op = ops[layout.first_hash_join_idx];
	auto &between_join_projection_op = ops[layout.between_projection_idx];
	auto &second_join_batch = state.second_join_batch;
	auto &between_join_sidecars = state.between_join_sidecars;
	auto &projection_skips = state.projection_skips;
	auto &second_join_omissions = state.second_join_omissions;

	bool use_compressed_group_key_skip = SljitBuildCompressedGroupKeyProjectionSkips(
	    runtime, native_runtime, scratch, ops, layout.first_hash_join_idx, layout.between_projection_idx,
	    layout.second_hash_join_idx, layout.second_projection_idx, layout.final_projection_idx, layout.aggregate_idx,
	    state);
	bool use_precomputed_payload_skip = SljitBuildPrecomputedPayloadProjectionSkips(
	    runtime, native_runtime, scratch, ops, layout.first_hash_join_idx, layout.between_projection_idx,
	    layout.second_hash_join_idx, layout.second_projection_idx, layout.final_projection_idx, layout.aggregate_idx,
	    state);
	if (second_join_batch.size() > 0 &&
	    second_join_omissions.Differs(use_compressed_group_key_skip, use_precomputed_payload_skip)) {
		if (flush_second_join_batch()) {
			result.stop_pipeline = true;
			return result;
		}
	}
	if (second_join_batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
		if (flush_second_join_batch()) {
			result.stop_pipeline = true;
			return result;
		}
	}

	const auto target_offset = second_join_batch.size();
	vector<uint8_t> between_projection_skip;
	optional_ptr<const vector<uint8_t>> between_projection_skip_ptr;
	if (!projection_skips.BuildBetweenProjectionSkip(between_join_projection_op.projections.size(),
	                                                 use_compressed_group_key_skip, use_precomputed_payload_skip,
	                                                 between_projection_skip, between_projection_skip_ptr)) {
		return result;
	}
	if (!SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
	        runtime, ops, scratch, layout.first_hash_join_idx, layout.between_projection_idx,
	        between_join_projection_op, first_join_input, scratch.FilterSelection(layout.first_hash_join_idx),
	        scratch.HashJoinRowPointers(layout.first_hash_join_idx), first_join_output, second_join_batch, nullptr,
	        nullptr, between_projection_skip_ptr)) {
		if (use_compressed_group_key_skip || use_precomputed_payload_skip) {
			throw InternalException("SLJIT between-join projection skip became unsupported after descriptor preflight");
		}
		return result;
	}
	second_join_omissions.Set(use_compressed_group_key_skip, use_precomputed_payload_skip);
	if (!SljitAppendBetweenJoinCompressedPassthroughs(
	        runtime, scratch, layout.first_hash_join_idx, layout.between_projection_idx, between_join_projection_op,
	        first_hash_join_op, between_join_sidecars, first_join_input,
	        scratch.FilterSelection(layout.first_hash_join_idx),
	        scratch.HashJoinRowPointers(layout.first_hash_join_idx), target_offset, selected_count)) {
		if (use_compressed_group_key_skip) {
			throw InternalException("SLJIT compressed group-key sidecar became unsupported after skip");
		}
	}
	if (use_precomputed_payload_skip &&
	    !SljitAppendBetweenJoinPrecomputedPayloads(
	        runtime, scratch, layout.first_hash_join_idx, layout.between_projection_idx, layout.second_projection_idx,
	        between_join_projection_op, between_join_sidecars, first_join_input,
	        scratch.FilterSelection(layout.first_hash_join_idx),
	        scratch.HashJoinRowPointers(layout.first_hash_join_idx), target_offset, selected_count)) {
		throw InternalException("SLJIT precomputed payload sidecar became unsupported after skip");
	}

	result.handled = true;
	RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, "direct_between_join_projection",
	                             selected_count);
	if (use_compressed_group_key_skip) {
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
		                             "direct_between_join_compressed_group_key_skip_projection", selected_count);
	}
	if (use_precomputed_payload_skip) {
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
		                             "direct_between_join_precomputed_payload_skip_projection", selected_count);
	}
	if (second_join_batch.size() == STANDARD_VECTOR_SIZE && flush_second_join_batch()) {
		result.stop_pipeline = true;
	}
	return result;
}

} // namespace duckdb

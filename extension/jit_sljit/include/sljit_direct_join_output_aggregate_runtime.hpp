//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_batch_runtime.hpp"
#include "sljit_direct_join_output_aggregate_trace.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_hash_join_projection_aggregate_input_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_descriptor.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_ungrouped_aggregate_payload_update_runtime.hpp"

namespace duckdb {

static bool SljitTryExecuteDirectJoinOutputPerfectHashAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &aggregate_input, optional_ptr<string> failure_reason) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (!aggregate_update.fused_payload_update_function || !aggregate_update.fused_payload_update_owns_group_lookup ||
	    !plan.use_primitive_payloads || !plan.use_perfect_hash_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size()) {
		return false;
	}
	if (sink_info.aggregates.empty()) {
		if (failure_reason) {
			*failure_reason = "shape";
		}
		return false;
	}
	auto &binding = SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, aggregate_input,
	                                            sink_info, "aggregate-update-runtime-binding-failed",
	                                            "SLJIT direct join-output perfect-hash aggregate update");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
		if (failure_reason) {
			*failure_reason = "binding";
		}
		return false;
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	if (!grouped_state.perfect_hash_layout.ready) {
		if (failure_reason) {
			*failure_reason = grouped_state.perfect_hash_layout.blocker.empty()
			                      ? string("perfect_hash_layout")
			                      : grouped_state.perfect_hash_layout.blocker;
		}
		return false;
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(op_idx, sink_info.aggregates, binding.aggregate_update.primitive);
	if (payload_lanes.size() != sink_info.aggregates.size()) {
		if (failure_reason) {
			*failure_reason = "payload_lanes";
		}
		return false;
	}
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	auto payload_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
	    aggregate_update.payloads, aggregate_update.fused_payload_update_function, sink_info.aggregates,
	    sink_info.groups, plan.group_expressions, sink_info.aggregate_contract, payload_lanes,
	    grouped_state.perfect_hash_layout, aggregate_input, nullptr, aggregate_input.size(), payload_scratch);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update_fused", payload_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "direct_join_output_perfect_hash_payload_update",
	                             aggregate_input.size());
	RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update_owns_perfect_hash_group_lookup",
	                             aggregate_input.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update",
	                                         aggregate_input.size());
	return true;
}

static bool SljitTryExecuteDirectJoinOutputAggregate(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy_ptr,
    SljitPostJoinProjectionStrategy &post_join_projection, DataChunk &join_input,
    const SelectionVector &match_selection, const SelectionVector &build_selection, Vector &row_pointers,
    DataChunk &join_output, optional_ptr<bool> deferred_grouped_finish,
    bool source_key0_int64_to_int32_unchecked = false, optional_ptr<const vector<idx_t>> output_column_map = nullptr,
    idx_t output_projection_idx = DConstants::INVALID_INDEX) {
	if (!strategy_ptr || strategy_ptr->disabled) {
		return false;
	}
	auto &strategy = *strategy_ptr;
	auto &descriptor = strategy.descriptor;
	strategy.last_failure.clear();
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	const bool has_projection_chain = post_join_projection.HasProjectionChain();
	const bool descriptor_ready =
	    has_projection_chain
	        ? SljitTryBuildPostJoinProjectionAggregateDescriptor(ops, scratch, post_join_projection,
	                                                             strategy.aggregate_idx, descriptor, output_column_map,
	                                                             output_projection_idx)
	        : SljitTryBuildSelectedJoinAggregateInputDescriptor(ops, scratch, post_join_projection.hash_join_idx,
	                                                            strategy.aggregate_idx, descriptor);
	if (!descriptor_ready) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
		                                                          descriptor.Blocker(), join_output.size());
		strategy.last_failure = descriptor.Blocker();
		strategy.disabled = true;
		return false;
	}
	descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = descriptor.input.chunk;
	aggregate_input.Reset();
	auto &aggregate_op = ops[strategy.aggregate_idx];
	if (has_projection_chain && descriptor.projection_idx == DConstants::INVALID_INDEX) {
		throw InternalException("SLJIT direct row-pointer aggregate descriptor has no projection index");
	}
	SljitApplyJoinProjectionGroupCastProofs(descriptor.group_sources, source_key0_int64_to_int32_unchecked);
	const auto aggregate_projection_idx = descriptor.projection_idx == DConstants::INVALID_INDEX
	                                          ? post_join_projection.trace_projection_idx
	                                          : descriptor.projection_idx;
	if (descriptor.output_to_projection.empty()) {
		aggregate_input.SetChildCardinality(join_output.size());
	} else if (SljitTryReferenceHashJoinProjectionAggregateInputsToChunk(
	               runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
	               descriptor.Projection(), join_input, match_selection, descriptor.input_sources, join_output.size(),
	               aggregate_input)) {
	} else {
		string materialize_failure;
		if (SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk(
		        runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx, descriptor.Projection(),
		        join_input, match_selection, build_selection, row_pointers, descriptor.input_sources,
		        join_output.size(), aggregate_input, optional_ptr<string>(&materialize_failure))) {
		} else if (!SljitJoinProjectionAggregateInputsUseOnlyProjectionOutputs(descriptor) ||
		           !SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
		               runtime, ops, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
		               descriptor.Projection(), join_input, match_selection, row_pointers, join_output, aggregate_input,
		               optional_ptr<const vector<idx_t>>(&descriptor.output_to_projection))) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection, "materialize",
			                                                          join_output.size());
			strategy.last_failure =
			    materialize_failure.empty() ? string("materialize") : string("materialize_") + materialize_failure;
			SljitFlushDirectJoinOutputAggregate(runtime, ops, strategy_ptr);
			return false;
		}
	}
	if (aggregate_input.size() != join_output.size()) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection, "cardinality",
		                                                          join_output.size());
		strategy.last_failure = "cardinality";
		SljitFlushDirectJoinOutputAggregate(runtime, ops, strategy_ptr);
		return false;
	}
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		if (descriptor.remapped_payloads.size() != sink_info.aggregates.size()) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
			                                                          "ungrouped_payloads", join_output.size());
			strategy.last_failure = "ungrouped_payloads";
			return false;
		}
		SljitExecuteNativeUngroupedAggregateUpdateWithPayloads(
		    runtime, runtime.ExecutionOperators(), scratch, strategy.aggregate_idx, aggregate_op, aggregate_input,
		    descriptor.remapped_payloads, "direct_join_output_ungrouped_payload_update",
		    "direct_join_output_ungrouped_payload_update", "direct_join_output_ungrouped_state_update");
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_join_output_ungrouped_update",
		                             aggregate_input.size());
		return true;
	}
	auto batch_group_sources = descriptor.group_sources;
	SljitApplyInputVectorGroupBatchCastProofs(aggregate_input, batch_group_sources, aggregate_input.size());
	SljitApplyRowPointerGroupBatchCastProofs(row_pointers, batch_group_sources, aggregate_input.size());
	SljitRecordJoinProjectionAggregateDescriptorShape(runtime, aggregate_op.kind, descriptor, batch_group_sources,
	                                                  join_output.size());
	string perfect_hash_failure;
	if (SljitTryExecuteDirectJoinOutputPerfectHashAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch,
	                                                              strategy.aggregate_idx, aggregate_op, aggregate_input,
	                                                              optional_ptr<string>(&perfect_hash_failure))) {
		return true;
	}
	if (!perfect_hash_failure.empty()) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
		                                                          string("perfect_hash_update_") + perfect_hash_failure,
		                                                          join_output.size());
	}

	if (!SljitDescriptorUsesRowPointerGroupSource(descriptor) &&
	    SljitGroupSourcesCanMaterializeFromInputVectors(aggregate_input, batch_group_sources)) {
		SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
		                                          strategy.pending_batch);
		SljitAppendPendingInputVectorAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, strategy, scratch,
		                                            deferred_grouped_finish, aggregate_input,
		                                            source_key0_int64_to_int32_unchecked);
		return true;
	}

	SljitFlushDirectJoinOutputAggregate(runtime, ops, strategy_ptr);
	SljitAppendPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                           strategy.pending_batch, scratch, deferred_grouped_finish,
	                                           aggregate_input, row_pointers, source_key0_int64_to_int32_unchecked);
	return true;
}

} // namespace duckdb

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
#include "sljit_hash_join_projection_aggregate_input_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_join_input_row_pointer_complementary_sum_runtime.hpp"
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
	if (!aggregate_update.fused_payload_update.Function() || !aggregate_update.fused_payload_update_owns_group_lookup ||
	    !plan.UsesPrimitivePayloads() || !plan.use_perfect_hash_group_lookup ||
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
	    scratch.AggregatePayloadLanes(op_idx, aggregate_update.payload_descriptors, binding.aggregate_update.primitive);
	if (payload_lanes.size() != sink_info.aggregates.size()) {
		if (failure_reason) {
			*failure_reason = "payload_lanes";
		}
		return false;
	}
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, sink_info.aggregate_contract,
	                                                      aggregate_update.payload_descriptors, payload_lanes);
	auto payload_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
	    aggregate_update.payloads, aggregate_update.fused_payload_update.Function(), sink_info.groups,
	    plan.group_expressions, aggregate_update.group_source_not_null, sink_info.aggregate_contract,
	    aggregate_update.payload_descriptors, payload_lanes, reduction_lanes, grouped_state.perfect_hash_layout,
	    aggregate_input, nullptr, aggregate_input.size(), payload_scratch);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update_fused", payload_stage_start);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "join_output_perfect_hash_payload_update",
	                                            aggregate_input.size());
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "fused_payload_update_owns_perfect_hash_group_lookup",
	                                            aggregate_input.size());
	return true;
}

static bool SljitDirectJoinOutputAggregatePayloadSourcesValid(const SljitJoinProjectionAggregateDescriptor &descriptor,
                                                              const SljitExecutableRegionOp &aggregate_op,
                                                              DataChunk &aggregate_input,
                                                              optional_ptr<string> failure_reason) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return true;
	}
	auto fused_override_status = SljitGetFusedTypedPayloadSourceOverrideStatus(aggregate_update, aggregate_input,
	                                                                           descriptor.payload_source_indices);
	if (fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::READY) {
		return true;
	}
	if (fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::INVALID) {
		if (failure_reason) {
			*failure_reason = "fused_payload_sources";
		}
		return false;
	}
	if (descriptor.payload_source_indices.size() != sink_info.aggregates.size() ||
	    descriptor.payload_source_indices.size() != aggregate_update.payload_descriptors.size()) {
		if (failure_reason) {
			*failure_reason = "payload_source_count_" + to_string(descriptor.payload_source_indices.size()) + "_" +
			                  to_string(sink_info.aggregates.size());
		}
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < descriptor.payload_source_indices.size(); payload_idx++) {
		auto source_idx = descriptor.payload_source_indices[payload_idx];
		auto &aggregate = sink_info.aggregates[payload_idx];
		auto &payload_descriptor = aggregate_update.payload_descriptors[payload_idx];
		if (source_idx == DConstants::INVALID_INDEX) {
			if (payload_descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
			    payload_descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
				continue;
			}
			if (failure_reason) {
				*failure_reason = "payload_source_invalid_" + to_string(payload_idx);
			}
			return false;
		}
		if (source_idx >= aggregate_input.ColumnCount()) {
			if (failure_reason) {
				*failure_reason = "payload_source_bounds_" + to_string(payload_idx) + "_" + to_string(source_idx) +
				                  "_" + to_string(aggregate_input.ColumnCount());
			}
			return false;
		}
		if (aggregate.child_types.size() == 1 &&
		    aggregate_input.data[source_idx].GetType() != aggregate.child_types[0]) {
			if (failure_reason) {
				*failure_reason = "payload_source_type_" + to_string(payload_idx);
			}
			return false;
		}
	}
	return true;
}

static bool SljitTryExecuteDirectJoinOutputAggregate(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy_ptr,
    SljitPostJoinProjectionStrategy &post_join_projection, DataChunk &join_input,
    const SelectionVector &match_selection, const SelectionVector &build_selection, Vector &row_pointers,
    DataChunk &join_output, optional_ptr<bool> deferred_grouped_finish,
    ExecutionHashJoinProbeOutputProof output_proof = {}, optional_ptr<const vector<idx_t>> output_column_map = nullptr,
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
	                                                            strategy.aggregate_idx, descriptor, output_column_map,
	                                                            output_projection_idx);
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
	string complementary_sum_failure;
	if (SljitTryExecuteJoinInputRowPointerComplementarySumUpdate(
	        runtime, runtime.ExecutionOperators(), scratch, post_join_projection.hash_join_idx, aggregate_op, strategy,
	        join_input, match_selection, row_pointers, join_output.size(), deferred_grouped_finish,
	        output_proof.source_key0_int64_to_int32, optional_ptr<string>(&complementary_sum_failure))) {
		return true;
	}
	if (!complementary_sum_failure.empty()) {
		auto &plan = strategy.join_input_complementary_sum_plan;
		if (!plan.blocker_recorded) {
			SljitRecordDirectJoinOutputAggregateProjectionUnsupported(
			    runtime, ops, post_join_projection, string("complementary_sum_") + complementary_sum_failure,
			    join_output.size());
			plan.blocker_recorded = true;
		}
	}
	aggregate_input.Reset();
	if (has_projection_chain && descriptor.projection_idx == DConstants::INVALID_INDEX) {
		throw InternalException("SLJIT direct row-pointer aggregate descriptor has no projection index");
	}
	SljitApplyJoinProjectionGroupCastProofs(descriptor.group_sources, output_proof.source_key0_int64_to_int32);
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
		if (!output_proof.ExactSourceFilterMatches() &&
		    SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk(
		        runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx, descriptor.Projection(),
		        join_input, match_selection, build_selection, row_pointers, descriptor.input_sources,
		        join_output.size(), aggregate_input, optional_ptr<string>(&materialize_failure))) {
		} else if (!SljitJoinProjectionAggregateInputsUseOnlyProjectionOutputs(descriptor) ||
		           !SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
		               runtime, ops, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
		               descriptor.Projection(), join_input, match_selection, row_pointers, join_output, aggregate_input,
		               optional_ptr<const vector<idx_t>>(&descriptor.output_to_projection), nullptr, nullptr,
		               output_proof)) {
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
		    descriptor.remapped_payloads, "join_output_ungrouped_payload_update",
		    "join_output_ungrouped_payload_update");
		RecordSljitRegionMaterializationElisionPath(runtime, aggregate_op.kind, "join_output_ungrouped_update",
		                                            aggregate_input.size());
		return true;
	}
	string payload_source_failure;
	if (!SljitDirectJoinOutputAggregatePayloadSourcesValid(descriptor, aggregate_op, aggregate_input,
	                                                       optional_ptr<string>(&payload_source_failure))) {
		const auto reason = payload_source_failure.empty() ? string("payload_sources")
		                                                   : string("payload_sources_") + payload_source_failure;
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection, reason,
		                                                          join_output.size());
		strategy.last_failure = reason;
		SljitFlushDirectJoinOutputAggregate(runtime, ops, strategy_ptr);
		return false;
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
		                                            output_proof.source_key0_int64_to_int32);
		return true;
	}

	// Switching from input-vector grouping to row-pointer grouping must flush the
	// former, but the row-pointer batch itself remains valid across join output
	// chunks and owns capacity/cast-proof transitions internally.
	SljitFlushPendingDirectInputVectorAggregate(runtime, aggregate_op, strategy);
	auto string_set_classification =
	    SljitGetDirectJoinOutputStringSetClassification(strategy, aggregate_op, aggregate_input);
	SljitAppendPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                           strategy.pending_batch, scratch, deferred_grouped_finish,
	                                           aggregate_input, row_pointers, output_proof.source_key0_int64_to_int32,
	                                           string_set_classification);
	return true;
}

} // namespace duckdb

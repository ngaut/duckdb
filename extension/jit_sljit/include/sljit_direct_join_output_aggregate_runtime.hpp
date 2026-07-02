//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_grouped_aggregate_descriptor.hpp"
#include "sljit_grouped_aggregate_direct_update_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitTryBuildProjectionChainRowPointerAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	if (aggregate_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	if (!SljitTryBuildPostJoinProjectionDescriptor(ops, post_join_projection)) {
		return descriptor.Block(post_join_projection.descriptor.Blocker().c_str());
	}
	auto hash_join_idx = post_join_projection.hash_join_idx;
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return descriptor.Block("hash_join_binding");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD || !binding.hash_table) {
		return descriptor.Block("hash_join_shape");
	}
	descriptor.BorrowProjection(post_join_projection.descriptor.projection_idx,
	                            post_join_projection.descriptor.Projection());
	return SljitTryBuildProjectionRowPointerAggregateDescriptor(binding, ops[aggregate_idx], descriptor,
	                                                            post_join_projection.descriptor.OutputMap());
}

static bool SljitTryBuildSingleProjectionRowPointerAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    idx_t projection_idx, idx_t aggregate_idx, SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	if (projection_idx >= ops.size() || aggregate_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return descriptor.Block("hash_join_binding");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD || !binding.hash_table) {
		return descriptor.Block("hash_join_shape");
	}
	descriptor.BorrowProjection(projection_idx, ops[projection_idx]);
	return SljitTryBuildProjectionRowPointerAggregateDescriptor(binding, ops[aggregate_idx], descriptor);
}

static bool SljitFlushPendingRowPointerAggregateBatch(ExecutionRegionRuntime &runtime, idx_t aggregate_idx,
                                                      SljitExecutableRegionOp &aggregate_op,
                                                      SljitJoinProjectionAggregateDescriptor &descriptor,
                                                      SljitPendingRowPointerAggregateBatch &batch) {
	const auto pending_count = batch.Count();
	if (pending_count == 0) {
		return false;
	}
	if (!batch.scratch) {
		throw InternalException("SLJIT batched direct row-pointer aggregate has no scratch state");
	}
	if (!SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
	        runtime, runtime.ExecutionOperators(), *batch.scratch, aggregate_idx, aggregate_op, batch.input,
	        batch.row_pointers, descriptor.group_sources, descriptor.payload_source_indices, true,
	        batch.deferred_grouped_finish)) {
		throw InternalException("SLJIT batched direct row-pointer aggregate update failed");
	}
	RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projection_row_pointer_grouped_update",
	                             pending_count);
	batch.Reset();
	return false;
}

static void SljitAppendPendingRowPointerAggregateBatch(ExecutionRegionRuntime &runtime, idx_t aggregate_idx,
                                                       SljitExecutableRegionOp &aggregate_op,
                                                       SljitJoinProjectionAggregateDescriptor &descriptor,
                                                       SljitPendingRowPointerAggregateBatch &batch,
                                                       SljitRegionExecutionScratch &scratch,
                                                       optional_ptr<bool> deferred_grouped_finish,
                                                       DataChunk &aggregate_input, Vector &row_pointers) {
	batch.scratch = &scratch;
	batch.deferred_grouped_finish = deferred_grouped_finish;
	batch.Ensure(runtime.GetAllocator(), descriptor.input_types);
	if (batch.Count() + aggregate_input.size() > STANDARD_VECTOR_SIZE) {
		SljitFlushPendingRowPointerAggregateBatch(runtime, aggregate_idx, aggregate_op, descriptor, batch);
	}
	if (batch.input.ColumnCount() == 0) {
		batch.input.SetChildCardinality(batch.Count() + aggregate_input.size());
	} else {
		batch.input.Append(aggregate_input, VectorAppendMode::ERROR_ON_NO_SPACE);
	}
	batch.row_pointers.Append(row_pointers, aggregate_input.size(), VectorAppendMode::ERROR_ON_NO_SPACE);
}

static const char *
SljitDirectJoinOutputAggregateUnsupportedPrefix(const SljitDirectJoinOutputAggregateStrategy &strategy) {
	if (strategy.mode == SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN) {
		return "direct_projection_chain_row_pointer_aggregate_unsupported.";
	}
	return "direct_projection_row_pointer_aggregate_unsupported.";
}

static const char *SljitDirectJoinOutputAggregateUpdatePath(const SljitDirectJoinOutputAggregateStrategy &strategy) {
	if (strategy.mode == SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN) {
		return "direct_projection_chain_row_pointer_grouped_update";
	}
	return "direct_projection_row_pointer_grouped_update";
}

static void SljitRecordDirectJoinOutputAggregateProjectionUnsupported(
    ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops,
    const SljitDirectJoinOutputAggregateStrategy &strategy, const SljitPostJoinProjectionStrategy &post_join_projection,
    const string &reason, idx_t count) {
	if (post_join_projection.trace_projection_idx == DConstants::INVALID_INDEX ||
	    post_join_projection.trace_projection_idx >= ops.size()) {
		return;
	}
	auto path = string(SljitDirectJoinOutputAggregateUnsupportedPrefix(strategy)) + reason;
	RecordSljitRegionRuntimePath(runtime, ops[post_join_projection.trace_projection_idx].kind, path.c_str(), count);
}

static bool SljitFlushDirectJoinOutputAggregate(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                                SljitDirectJoinOutputAggregatePolicy &policy) {
	if (!policy.UsesPendingBatch()) {
		return false;
	}
	auto &strategy = policy.Strategy();
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	return SljitFlushPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, ops[strategy.aggregate_idx],
	                                                 strategy.descriptor, strategy.pending_batch);
}

static bool SljitTryBuildDirectJoinOutputAggregateDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                             SljitRegionExecutionScratch &scratch,
                                                             SljitDirectJoinOutputAggregateStrategy &strategy,
                                                             SljitPostJoinProjectionStrategy &post_join_projection) {
	if (strategy.mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION) {
		if (post_join_projection.first_projection_idx != post_join_projection.final_projection_idx) {
			return strategy.descriptor.Block("projection_strategy");
		}
		return SljitTryBuildSingleProjectionRowPointerAggregateDescriptor(
		    ops, scratch, post_join_projection.hash_join_idx, post_join_projection.first_projection_idx,
		    strategy.aggregate_idx, strategy.descriptor);
	}
	if (strategy.mode == SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN) {
		return SljitTryBuildProjectionChainRowPointerAggregateDescriptor(ops, scratch, post_join_projection,
		                                                                 strategy.aggregate_idx, strategy.descriptor);
	}
	return false;
}

static bool SljitTryExecuteDirectJoinOutputAggregate(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitDirectJoinOutputAggregatePolicy &policy, SljitPostJoinProjectionStrategy &post_join_projection,
    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, DataChunk &join_output,
    optional_ptr<bool> deferred_grouped_finish) {
	if (!policy.Enabled()) {
		return false;
	}
	auto &strategy = policy.Strategy();
	auto &descriptor = strategy.descriptor;
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	if (!SljitTryBuildDirectJoinOutputAggregateDescriptor(ops, scratch, strategy, post_join_projection)) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, strategy, post_join_projection,
		                                                          descriptor.Blocker(), join_output.size());
		if (strategy.mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION) {
			strategy.disabled = true;
		}
		return false;
	}
	if (strategy.mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION &&
	    !SljitDescriptorUsesRowPointerGroupSource(descriptor)) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, strategy, post_join_projection,
		                                                          "no_row_pointer_group", join_output.size());
		strategy.disabled = true;
		return false;
	}

	descriptor.EnsureInput(runtime.GetAllocator());
	auto &aggregate_input = descriptor.input.chunk;
	aggregate_input.Reset();
	bool aggregate_input_references_join_input = false;
	auto &aggregate_op = ops[strategy.aggregate_idx];
	auto &aggregate_update = aggregate_op.aggregate_update;
	if (descriptor.projection_idx == DConstants::INVALID_INDEX) {
		throw InternalException("SLJIT direct row-pointer aggregate descriptor has no projection index");
	}
	const auto aggregate_projection_idx = descriptor.projection_idx;
	const auto can_update_referenced_aggregate_input =
	    strategy.mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION &&
	    aggregate_update.fused_payload_update_function &&
	    SljitFusedGroupedAggregatePayloadsUseRuntimeInputAdapter(aggregate_update.payloads,
	                                                             aggregate_update.plan.sink_info.aggregates);
	if (descriptor.output_to_projection.empty()) {
		aggregate_input.SetChildCardinality(join_output.size());
	} else if (can_update_referenced_aggregate_input &&
	           SljitTryReferenceHashJoinLHSProjectionSourcesToChunk(
	               runtime, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
	               descriptor.Projection(), join_input, match_selection,
	               optional_ptr<const vector<idx_t>>(&descriptor.output_to_projection), join_output.size(),
	               aggregate_input)) {
		aggregate_input_references_join_input = true;
	} else if (!SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
	               runtime, ops, scratch, post_join_projection.hash_join_idx, aggregate_projection_idx,
	               descriptor.Projection(), join_input, match_selection, row_pointers, join_output, aggregate_input,
	               optional_ptr<const vector<idx_t>>(&descriptor.output_to_projection))) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, strategy, post_join_projection,
		                                                          "materialize", join_output.size());
		SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
		return false;
	}
	if (aggregate_input.size() != join_output.size()) {
		SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, strategy, post_join_projection,
		                                                          "cardinality", join_output.size());
		SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
		return false;
	}

	if (strategy.mode == SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN ||
	    aggregate_input_references_join_input) {
		SljitFlushDirectJoinOutputAggregate(runtime, ops, policy);
		if (!SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
		        runtime, runtime.ExecutionOperators(), scratch, strategy.aggregate_idx, aggregate_op, aggregate_input,
		        row_pointers, descriptor.group_sources, descriptor.payload_source_indices, true,
		        deferred_grouped_finish)) {
			if (strategy.mode == SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION) {
				throw InternalException("SLJIT direct row-pointer aggregate view update failed");
			}
			auto path = string(SljitDirectJoinOutputAggregateUnsupportedPrefix(strategy)) + "update";
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), join_output.size());
			return false;
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, SljitDirectJoinOutputAggregateUpdatePath(strategy),
		                             aggregate_input.size());
		return true;
	}

	SljitAppendPendingRowPointerAggregateBatch(runtime, strategy.aggregate_idx, aggregate_op, descriptor,
	                                           strategy.pending_batch, scratch, deferred_grouped_finish,
	                                           aggregate_input, row_pointers);
	return true;
}

} // namespace duckdb

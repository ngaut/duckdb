//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_two_join_drain_helpers_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_projected_grouped_aggregate_sink.hpp"
#include "sljit_two_join_direct_projection_runtime.hpp"

#include <utility>

namespace duckdb {

template <class EXECUTE_HASH_JOIN_PROBE, class AGGREGATE_SINK>
static bool SljitDrainSecondHashJoinProjectionAggregateRoute(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitTwoJoinGroupedAggregateRouteLayout &layout,
    SljitTwoJoinGroupedAggregateRouteState &state,
    const SljitHashJoinSelectionOnlyMaterializationBoundaries &materialization_boundaries, DataChunk &second_join_input,
    AGGREGATE_SINK &aggregate_sink, EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe) {
	if (second_join_input.size() == 0) {
		return false;
	}
	layout.AssertOperatorBounds(ops.size());
	auto &second_hash_join_op = ops[layout.second_hash_join_idx];
	SljitHashJoinProbeDrainState second_state;
	auto &second_join_output = scratch.TemporaryChunk(layout.second_hash_join_idx);
	do {
		second_join_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(layout.second_hash_join_idx, second_hash_join_op, second_join_input,
		                                           second_join_output, second_state, deferred_reason, false, true);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return aggregate_sink.DeferAfterFinish(deferred_reason);
		}
		if (second_join_output.size() == 0) {
			continue;
		}

		auto direct_second_projection =
		    SljitTryDirectSecondJoinProjection(runtime, native_runtime, ops, scratch, layout, state, second_join_input,
		                                       second_join_output, aggregate_sink);
		if (direct_second_projection.stop_pipeline) {
			return true;
		}
		if (direct_second_projection.processed_batch) {
			aggregate_sink.Charge(second_join_output.size());
		}
		auto projected = direct_second_projection.projected;
		if (!direct_second_projection.handled) {
			if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(
			        runtime, scratch, layout.second_hash_join_idx, second_hash_join_op, second_join_input,
			        scratch.FilterSelection(layout.second_hash_join_idx),
			        scratch.HashJoinBuildSelection(layout.second_hash_join_idx),
			        scratch.HashJoinRowPointers(layout.second_hash_join_idx), second_join_output.size(),
			        second_join_output, materialization_boundaries)) {
				throw InternalException("SLJIT direct second-join fallback materialization failed");
			}
			SljitProjectOptionalPostJoinProjectionChain(
			    runtime, scratch, ops, layout.second_projection_idx, layout.final_projection_idx, second_join_output,
			    projected, "post_second_join_reference_projection", "post_second_join_batch_projection");
		}
		if (projected && aggregate_sink.AppendProjectedBatch(*projected)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
	return false;
}

template <class EXECUTE_HASH_JOIN_PROBE, class AGGREGATE_SINK>
static bool SljitDrainSecondHashJoinProjectionChain(ExecutionRegionRuntime &runtime,
                                                    vector<SljitExecutableRegionOp> &ops,
                                                    SljitRegionExecutionScratch &scratch, idx_t second_hash_join_idx,
                                                    idx_t first_projection_idx, idx_t final_projection_idx,
                                                    DataChunk &second_join_input, AGGREGATE_SINK &aggregate_sink,
                                                    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe,
                                                    const char *reference_phase = "post_join_reference_projection",
                                                    const char *batch_phase = "post_join_batch_projection") {
	if (second_join_input.size() == 0) {
		return false;
	}
	D_ASSERT(second_hash_join_idx < ops.size());
	D_ASSERT(first_projection_idx <= final_projection_idx);
	D_ASSERT(final_projection_idx < ops.size());
	auto &second_hash_join_op = ops[second_hash_join_idx];
	SljitHashJoinProbeDrainState second_state;
	auto &second_join_output = scratch.TemporaryChunk(second_hash_join_idx);
	auto &projected = scratch.TemporaryChunk(final_projection_idx);
	do {
		second_join_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(second_hash_join_idx, second_hash_join_op, second_join_input,
		                                           second_join_output, second_state, deferred_reason, false, false);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return aggregate_sink.DeferAfterFinish(deferred_reason);
		}
		if (second_join_output.size() == 0) {
			continue;
		}
		SljitProjectPostJoinProjectionChain(runtime, scratch, ops, first_projection_idx, final_projection_idx,
		                                    second_join_output, projected, reference_phase, batch_phase);
		if (aggregate_sink.AppendProjectedBatch(projected)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
	return false;
}

template <class EXECUTE_HASH_JOIN_PROBE, class AGGREGATE_SINK>
static bool SljitDrainSelectionOnlySecondHashJoinProjection(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    idx_t second_hash_join_idx, idx_t projection_idx, DataChunk &second_join_input,
    const SljitHashJoinSelectionOnlyMaterializationBoundaries &materialization_boundaries,
    AGGREGATE_SINK &aggregate_sink, EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe,
    const char *reference_phase = "post_second_join_reference_projection",
    const char *batch_phase = "post_second_join_batch_projection") {
	if (second_join_input.size() == 0) {
		return false;
	}
	D_ASSERT(second_hash_join_idx < ops.size());
	D_ASSERT(projection_idx < ops.size());
	auto &second_hash_join_op = ops[second_hash_join_idx];
	auto &projection_op = ops[projection_idx];
	SljitHashJoinProbeDrainState second_state;
	auto &second_join_output = scratch.TemporaryChunk(second_hash_join_idx);
	auto &projected = scratch.TemporaryChunk(projection_idx);
	do {
		second_join_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(second_hash_join_idx, second_hash_join_op, second_join_input,
		                                           second_join_output, second_state, deferred_reason, false, true);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return aggregate_sink.DeferAfterFlushAndFinish(deferred_reason);
		}
		if (second_join_output.size() == 0) {
			continue;
		}

		bool direct_projected = false;
		if (aggregate_sink.TryAppendDirectProjectedBatch(second_join_output, direct_projected, [&](DataChunk &batch) {
			    return SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
			        runtime, ops, scratch, second_hash_join_idx, projection_idx, projection_op, second_join_input,
			        scratch.FilterSelection(second_hash_join_idx), scratch.HashJoinRowPointers(second_hash_join_idx),
			        second_join_output, batch);
		    })) {
			return true;
		}
		if (direct_projected) {
			RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_second_join_projection",
			                             second_join_output.size());
			continue;
		}

		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(
		        runtime, scratch, second_hash_join_idx, second_hash_join_op, second_join_input,
		        scratch.FilterSelection(second_hash_join_idx), scratch.HashJoinBuildSelection(second_hash_join_idx),
		        scratch.HashJoinRowPointers(second_hash_join_idx), second_join_output.size(), second_join_output,
		        materialization_boundaries)) {
			throw InternalException("SLJIT direct second-join fallback materialization failed");
		}
		SljitProjectPostJoinProjectionChain(runtime, scratch, ops, projection_idx, projection_idx, second_join_output,
		                                    projected, reference_phase, batch_phase);
		if (aggregate_sink.AppendProjectedBatch(projected)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
	return false;
}

template <class EXECUTE_HASH_JOIN_PROBE, class FLUSH_SECOND_JOIN_BATCH, class AGGREGATE_SINK>
static bool SljitExecuteTwoJoinGroupedAggregateSourceChunk(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, const SljitTwoJoinGroupedAggregateRouteLayout &layout,
    SljitTwoJoinGroupedAggregateRouteState &state,
    const SljitHashJoinSelectionOnlyMaterializationBoundaries &materialization_boundaries, DataChunk &source_chunk,
    bool have_more_output, FLUSH_SECOND_JOIN_BATCH &&flush_second_join_batch, AGGREGATE_SINK &aggregate_sink,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe) {
	if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
		return aggregate_sink.StopAfterFinish(ExecutionRegionResult::INTERRUPTED);
	}
	layout.AssertOperatorBounds(ops.size());
	auto &first_hash_join_op = ops[layout.first_hash_join_idx];
	auto &between_join_projection_op = ops[layout.between_projection_idx];
	SljitHashJoinProbeDrainState first_state;
	auto &first_join_output = scratch.TemporaryChunk(layout.first_hash_join_idx);
	do {
		first_join_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(layout.first_hash_join_idx, first_hash_join_op, source_chunk,
		                                           first_join_output, first_state, deferred_reason, false, true);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return aggregate_sink.DeferAfterFinish(deferred_reason);
		}
		if (first_join_output.size() == 0) {
			continue;
		}

		auto direct_projection_append = SljitTryAppendDirectBetweenJoinProjection(
		    runtime, native_runtime, ops, scratch, layout, state, source_chunk, first_join_output,
		    first_join_output.size(), std::forward<FLUSH_SECOND_JOIN_BATCH>(flush_second_join_batch));
		if (direct_projection_append.stop_pipeline) {
			return true;
		}
		if (direct_projection_append.handled) {
			continue;
		}
		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(
		        runtime, scratch, layout.first_hash_join_idx, first_hash_join_op, source_chunk,
		        scratch.FilterSelection(layout.first_hash_join_idx),
		        scratch.HashJoinBuildSelection(layout.first_hash_join_idx),
		        scratch.HashJoinRowPointers(layout.first_hash_join_idx), first_join_output.size(), first_join_output,
		        materialization_boundaries)) {
			throw InternalException("SLJIT direct between-join fallback materialization failed");
		}

		auto &second_join_input = scratch.TemporaryChunk(layout.between_projection_idx);
		second_join_input.Reset();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryReferenceProjection(second_join_input, first_join_output, between_join_projection_op)) {
			RecordSljitRegionStageRuntime(runtime, layout.between_projection_idx, between_join_projection_op.kind,
			                              "between_join_reference_projection", projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
			                                         "reference_post_join_projection", second_join_input.size());
		} else {
			SljitExecuteProjection(scratch, layout.between_projection_idx, between_join_projection_op,
			                       first_join_output, second_join_input);
			RecordSljitRegionStageRuntime(runtime, layout.between_projection_idx, between_join_projection_op.kind,
			                              "between_join_batch_projection", projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
			                                         "copied_post_join_projection", second_join_input.size());
		}
		if (SljitDrainSecondHashJoinProjectionAggregateRoute(runtime, native_runtime, ops, scratch, layout, state,
		                                                     materialization_boundaries, second_join_input,
		                                                     aggregate_sink, execute_hash_join_probe)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
	return false;
}

} // namespace duckdb

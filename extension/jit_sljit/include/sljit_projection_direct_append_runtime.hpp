//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_direct_append_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

enum class SljitProjectionDirectAppendReservationStatus : uint8_t { READY, UNAVAILABLE };

static ExecutionSinkBinding &SljitBindProjectionDirectAppendSink(ExecutionRegionRuntime &runtime,
                                                                 ExecutionOperatorRuntime &native_runtime,
                                                                 SljitRegionExecutionScratch &scratch, idx_t sink_idx,
                                                                 SljitExecutableRegionOp &sink_op, DataChunk &input) {
	auto &binding = SljitBindRecordedNativeSink(runtime, native_runtime, scratch, sink_idx, sink_op.kind, input,
	                                            sink_op.append_sink.plan.sink_info,
	                                            "append-sink-runtime-binding-failed", "SLJIT append sink");
	if (!binding.ready || !binding.append_sink.ready) {
		throw InternalException("SLJIT append sink binding did not return a ready append state");
	}
	return binding;
}

static void SljitValidateProjectionDirectAppendReservation(SljitExecutableRegionOp &op, DataChunk &input,
                                                           idx_t source_offset, DirectAppendReservation &reservation) {
	if (reservation.slices.size() != 1) {
		throw InternalException("SLJIT direct append expected exactly one storage reservation slice");
	}
	auto &slice = reservation.slices[0];
	slice.source_offset = source_offset;
	if (slice.count == 0 || source_offset + slice.count > input.size()) {
		throw InternalException("SLJIT direct append reservation slice is out of range");
	}
	if (slice.targets.size() != op.projections.size()) {
		throw InternalException("SLJIT direct append target count mismatch");
	}
	if (slice.sources.size() != op.projections.size()) {
		throw InternalException("SLJIT direct append source count mismatch");
	}
}

static SljitProjectionDirectAppendReservationStatus SljitPrepareProjectionDirectAppendReservation(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, SljitRegionExecutionScratch &scratch,
    idx_t sink_idx, SljitExecutableRegionOp &op, SljitExecutableRegionOp &sink_op, DataChunk &input,
    idx_t source_offset, SljitDirectProjectionStageTimers &direct_stage_timers, SinkResultType &sink_result,
    DirectAppendProfile &direct_append_profile) {
	string blocker;
	auto &reservation = scratch.direct_append_reservation;
	auto prepare_stage_start = SljitRegionStageStart(runtime);
	auto direct_result =
	    ExecutionPrepareDirectAppend(binding.append_sink, op.output_types, input.size() - source_offset, reservation,
	                                 blocker, &direct_append_profile);
	direct_stage_timers.AddPrepare(prepare_stage_start);
	if (direct_result != ExecutionOperatorBindResult::READY) {
		if (source_offset > 0) {
			throw InternalException("SLJIT direct append became unavailable after a partial commit: %s",
			                        blocker.c_str());
		}
		RecordSljitDirectAppendProfile(runtime, sink_idx, sink_op.kind, direct_append_profile);
		return SljitProjectionDirectAppendReservationStatus::UNAVAILABLE;
	}
	SljitValidateProjectionDirectAppendReservation(op, input, source_offset, reservation);
	return SljitProjectionDirectAppendReservationStatus::READY;
}

static bool SljitCommitProjectionDirectAppendSlice(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, SljitRegionExecutionScratch &scratch,
    idx_t sink_idx, SljitExecutableRegionOp &op, SljitExecutableRegionOp &sink_op, DataChunk &input,
    const SljitDirectProjectionCandidate &direct_candidate, const vector<idx_t> &direct_distinct_counts,
    optional_ptr<SljitFixedDirectProjectionSourceCache> fixed_source_cache,
    SljitProjectionAdapterScratch &projection_scratch, SljitDirectProjectionStageTimers &direct_stage_timers,
    DirectAppendProfile &direct_append_profile, SinkResultType &sink_result) {
	auto &reservation = scratch.direct_append_reservation;
	auto &slice = reservation.slices[0];
	auto generated_stage_start = SljitRegionStageStart(runtime);
	if (!SljitTryMaterializeDirectProjectionSlice(runtime, direct_candidate, op, input, slice, fixed_source_cache,
	                                              projection_scratch, direct_stage_timers)) {
		throw InternalException(direct_candidate.ShapeChangedMessage());
	}
	direct_stage_timers.AddGenerated(generated_stage_start);
	auto stats_stage_start = SljitRegionStageStart(runtime);
	SljitFinishDirectProjectionStats(direct_candidate, op, input, slice, direct_distinct_counts, fixed_source_cache,
	                                 projection_scratch);
	direct_stage_timers.AddStats(stats_stage_start);
	auto commit_stage_start = SljitRegionStageStart(runtime);
	sink_result = ExecutionCommitDirectAppend(binding.append_sink, reservation, &direct_append_profile);
	direct_stage_timers.AddCommit(commit_stage_start);
	RecordSljitDirectAppendProfile(runtime, sink_idx, sink_op.kind, direct_append_profile);
	return sink_result != SinkResultType::NEED_MORE_INPUT;
}

static bool SljitTryExecuteProjectionDirectAppendLoop(
    ExecutionRegionRuntime &runtime, ExecutionSinkBinding &binding, SljitRegionExecutionScratch &scratch, idx_t op_idx,
    idx_t sink_idx, SljitExecutableRegionOp &op, SljitExecutableRegionOp &sink_op, DataChunk &input,
    const SljitDirectProjectionCandidate &direct_candidate, const vector<idx_t> &source_distinct_counts,
    optional_ptr<SljitFixedDirectProjectionSourceCache> fixed_source_cache,
    SljitProjectionAdapterScratch &projection_scratch, SljitDirectProjectionStageTimers &direct_stage_timers,
    SinkResultType &sink_result) {
	idx_t source_offset = 0;
	vector<idx_t> empty_distinct_counts;
	const auto &direct_distinct_counts = op_idx == 0 ? source_distinct_counts : empty_distinct_counts;
	while (source_offset < input.size()) {
		DirectAppendProfile direct_append_profile;
		const auto reservation_status = SljitPrepareProjectionDirectAppendReservation(
		    runtime, binding, scratch, sink_idx, op, sink_op, input, source_offset, direct_stage_timers, sink_result,
		    direct_append_profile);
		if (reservation_status == SljitProjectionDirectAppendReservationStatus::UNAVAILABLE) {
			direct_stage_timers.Flush();
			return false;
		}
		auto &slice = scratch.direct_append_reservation.slices[0];
		if (SljitCommitProjectionDirectAppendSlice(
		        runtime, binding, scratch, sink_idx, op, sink_op, input, direct_candidate, direct_distinct_counts,
		        fixed_source_cache, projection_scratch, direct_stage_timers, direct_append_profile, sink_result)) {
			direct_stage_timers.Flush();
			return true;
		}
		source_offset += slice.count;
	}
	direct_stage_timers.Flush();
	return true;
}

static bool
SljitTryExecuteProjectionDirectAppend(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                      SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
                                      const vector<idx_t> &source_distinct_counts, idx_t op_idx,
                                      SljitExecutableRegionOp &op, DataChunk &input, SinkResultType &sink_result) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op_idx + 1 >= ops.size()) {
		return false;
	}
	const auto sink_idx = op_idx + 1;
	auto &sink_op = ops[sink_idx];
	if (sink_op.kind != SljitNativeRegionOpKind::APPEND_SINK || sink_idx + 1 != ops.size()) {
		return false;
	}
	auto &projection_scratch = scratch.ProjectionScratch(op_idx);
	SljitFixedDirectProjectionSourceCache fixed_source_cache;
	fixed_source_cache.Reset(input.ColumnCount());
	auto fixed_source_cache_ptr = optional_ptr<SljitFixedDirectProjectionSourceCache>(&fixed_source_cache);
	SljitDirectProjectionCandidate direct_candidate;
	if (!SljitTrySelectDirectProjectionCandidate(runtime, op_idx, op, input, fixed_source_cache_ptr, projection_scratch,
	                                             direct_candidate)) {
		return false;
	}
	D_ASSERT(direct_candidate.IsSet());

	auto &binding = SljitBindProjectionDirectAppendSink(runtime, native_runtime, scratch, sink_idx, sink_op, input);
	SljitDirectProjectionStageTimers direct_stage_timers(runtime, op_idx, op.kind, sink_idx, sink_op.kind,
	                                                     direct_candidate);
	return SljitTryExecuteProjectionDirectAppendLoop(runtime, binding, scratch, op_idx, sink_idx, op, sink_op, input,
	                                                 direct_candidate, source_distinct_counts, fixed_source_cache_ptr,
	                                                 projection_scratch, direct_stage_timers, sink_result);
}

} // namespace duckdb

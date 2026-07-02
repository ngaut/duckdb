//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_filtered_aggregate_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_native_sink_runtime.hpp"
#include "sljit_nested_loop_join_runtime.hpp"
#include "sljit_projection_direct_append_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"

namespace duckdb {

template <class KERNEL>
static SinkResultType
SljitExecuteNativeFullPipelineFrom(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                   vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
                                   SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input);

template <class KERNEL>
static SinkResultType SljitExecuteNativeFullPipeline(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                                     vector<SljitExecutableRegionOp> &ops,
                                                     const vector<idx_t> &source_distinct_counts,
                                                     SljitRegionExecutionScratch &scratch, DataChunk &input);

template <class KERNEL>
struct SljitNativePipelineExecutor {
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	const vector<idx_t> &source_distinct_counts;

	SinkResultType operator()(SljitRegionExecutionScratch &scratch, DataChunk &input) {
		return SljitExecuteNativeFullPipeline(kernel, runtime, ops, source_distinct_counts, scratch, input);
	}

	SinkResultType operator()(SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input) {
		return SljitExecuteNativeFullPipelineFrom(kernel, runtime, ops, source_distinct_counts, scratch, start_op_idx,
		                                          input);
	}
};

template <class KERNEL>
static SljitNativePipelineExecutor<KERNEL>
SljitMakeNativePipelineExecutor(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                const vector<idx_t> &source_distinct_counts) {
	return {kernel, runtime, ops, source_distinct_counts};
}

template <class KERNEL>
static bool SljitTryExecuteNativeHashJoinFilteredUngroupedAggregateUpdate(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    SljitExecutableRegionOp &hash_join_op, DataChunk &join_input, SinkResultType &sink_result) {
	auto execute_hash_join_probe =
	    SljitMakeFixedScratchRecordedHashJoinProbeCallback(kernel, runtime, native_runtime, scratch);
	return SljitTryExecuteHashJoinFilteredUngroupedAggregateUpdate(runtime, native_runtime, ops, scratch, hash_join_idx,
	                                                               hash_join_op, join_input, sink_result,
	                                                               execute_hash_join_probe);
}

template <class KERNEL>
static bool SljitTryExecuteNativeHashJoinProbeDirectHashJoinBuild(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
    DataChunk &join_input, DataChunk &join_output, SinkResultType &sink_result) {
	auto execute_hash_join_probe =
	    SljitMakeFixedScratchRecordedHashJoinProbeCallback(kernel, runtime, native_runtime, scratch);
	auto execute_native_full_pipeline_from = [&](idx_t start_op_idx, DataChunk &next_input) {
		return SljitExecuteNativeFullPipelineFrom(kernel, runtime, ops, source_distinct_counts, scratch, start_op_idx,
		                                          next_input);
	};
	auto execute_native_hash_join_build = [&](idx_t sink_idx, SljitExecutableRegionOp &sink_op, DataChunk &input) {
		return SljitExecuteNativeHashJoinBuild(
		    runtime, native_runtime, scratch, sink_idx, sink_op, input, scratch.HashJoinBuildSourceChunk(sink_idx),
		    scratch.HashJoinBuildHashValues(sink_idx), scratch.HashJoinBuildSelection(sink_idx));
	};
	return SljitTryExecuteHashJoinProbeDirectHashJoinBuild(
	    runtime, native_runtime, ops, scratch, hash_join_idx, hash_join_op, join_input, join_output, sink_result,
	    execute_hash_join_probe, execute_native_full_pipeline_from, execute_native_hash_join_build);
}

template <class KERNEL>
static SinkResultType
SljitDrainNativeHashJoinProbe(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                              const vector<idx_t> &source_distinct_counts, SljitRegionExecutionScratch &scratch,
                              idx_t op_idx, DataChunk &input, DataChunk &output) {
	auto &op = ops[op_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SinkResultType direct_hash_build_result;
	if (SljitTryExecuteNativeHashJoinProbeDirectHashJoinBuild(kernel, runtime, native_runtime, ops,
	                                                          source_distinct_counts, scratch, op_idx, op, input,
	                                                          output, direct_hash_build_result)) {
		return direct_hash_build_result;
	}
	SljitHashJoinProbeDrainState state;
	do {
		output.Reset();
		string deferred_reason;
		auto stage_start = SljitRegionStageStart(runtime);
		auto bind_result = SljitExecuteNativeHashJoinProbeWithScratch(kernel, runtime, native_runtime, scratch, op_idx,
		                                                              op, input, output, state, deferred_reason);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return SljitDeferBlockedSinkResult(runtime, deferred_reason);
		}
		if (output.size() == 0) {
			continue;
		}
		auto sink_result = SljitExecuteNativeFullPipelineFrom(kernel, runtime, ops, source_distinct_counts, scratch,
		                                                      op_idx + 1, output);
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return sink_result;
		}
	} while (!SljitHashJoinProbeDrainFinished(op.hash_join_probe.plan.output_mode, state));
	return SinkResultType::NEED_MORE_INPUT;
}

template <class KERNEL>
static SinkResultType SljitDrainNativeNestedLoopJoinProbe(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                                          vector<SljitExecutableRegionOp> &ops,
                                                          const vector<idx_t> &source_distinct_counts,
                                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                          DataChunk &input, DataChunk &output) {
	auto &op = ops[op_idx];
	SljitNestedLoopJoinProbeDrainState state;
	auto &native_runtime = runtime.ExecutionOperators();
	auto &left_condition = scratch.NestedLoopLeftConditionChunk(op_idx);
	do {
		output.Reset();
		string deferred_reason;
		auto stage_start = SljitRegionStageStart(runtime);
		auto bind_result = SljitExecuteNativeNestedLoopJoinProbeWithScratch(
		    native_runtime, scratch, op_idx, op, input, left_condition, output, scratch.NestedLoopLeftSelection(op_idx),
		    scratch.NestedLoopRightSelection(op_idx), state, deferred_reason);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return SljitDeferBlockedSinkResult(runtime, deferred_reason);
		}
		if (output.size() == 0) {
			continue;
		}
		auto sink_result = SljitExecuteNativeFullPipelineFrom(kernel, runtime, ops, source_distinct_counts, scratch,
		                                                      op_idx + 1, output);
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return sink_result;
		}
	} while (!state.finished);
	return SinkResultType::NEED_MORE_INPUT;
}

template <class KERNEL>
static SinkResultType
SljitExecuteNativeFullPipelineFrom(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                   vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
                                   SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input) {
	if (input.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &native_runtime = runtime.ExecutionOperators();
	DataChunk *current = &input;
	for (idx_t op_idx = start_op_idx; op_idx < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		SinkResultType terminal_sink_result;
		if (SljitTryExecuteNativeTerminalSink(runtime, native_runtime, scratch, op_idx, op, *current,
		                                      op_idx + 1 == ops.size(), terminal_sink_result)) {
			return native_runtime.RecordSinkResult(*current, terminal_sink_result);
		}
		SinkResultType filter_aggregate_result;
		bool record_filter_aggregate_result = false;
		if (SljitTryExecuteNativeFilterAggregateUpdate(runtime, native_runtime, scratch, ops, op_idx, *current,
		                                               filter_aggregate_result, record_filter_aggregate_result)) {
			if (record_filter_aggregate_result) {
				return native_runtime.RecordSinkResult(*current, filter_aggregate_result);
			}
			return filter_aggregate_result;
		}
		bool filter_projection_needs_input = false;
		if (SljitTryExecuteFullPipelineFilterProjection(runtime, scratch, ops, op_idx, current,
		                                                filter_projection_needs_input)) {
			if (filter_projection_needs_input) {
				return SinkResultType::NEED_MORE_INPUT;
			}
			continue;
		}
		SinkResultType direct_append_result;
		if (SljitTryExecuteProjectionDirectAppend(runtime, native_runtime, scratch, ops, source_distinct_counts, op_idx,
		                                          op, *current, direct_append_result)) {
			if (direct_append_result == SinkResultType::BLOCKED && !runtime.DeferredReason().empty()) {
				return direct_append_result;
			}
			return native_runtime.RecordSinkResult(*current, direct_append_result);
		}
		SinkResultType direct_hash_join_filtered_aggregate_result;
		if (SljitTryExecuteNativeHashJoinFilteredUngroupedAggregateUpdate(kernel, runtime, native_runtime, ops, scratch,
		                                                                  op_idx, op, *current,
		                                                                  direct_hash_join_filtered_aggregate_result)) {
			return direct_hash_join_filtered_aggregate_result;
		}
		bool single_transform_needs_input = false;
		if (SljitTryExecuteFullPipelineSingleOperatorTransform(runtime, scratch, op_idx, op, current,
		                                                       single_transform_needs_input)) {
			if (single_transform_needs_input) {
				return SinkResultType::NEED_MORE_INPUT;
			}
			continue;
		}
		switch (op.kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
			auto &output = scratch.TemporaryChunk(op_idx);
			output.Reset();
			return SljitDrainNativeHashJoinProbe(kernel, runtime, ops, source_distinct_counts, scratch, op_idx,
			                                     *current, output);
		}
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE: {
			auto &output = scratch.TemporaryChunk(op_idx);
			output.Reset();
			return SljitDrainNativeNestedLoopJoinProbe(kernel, runtime, ops, source_distinct_counts, scratch, op_idx,
			                                           *current, output);
		}
		default:
			throw InternalException("Invalid SLJIT full pipeline operator before sink");
		}
	}
	throw InternalException("SLJIT full pipeline region has no native sink operator");
}

template <class KERNEL>
static SinkResultType SljitExecuteNativeFullPipeline(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                                     vector<SljitExecutableRegionOp> &ops,
                                                     const vector<idx_t> &source_distinct_counts,
                                                     SljitRegionExecutionScratch &scratch, DataChunk &input) {
	return SljitExecuteNativeFullPipelineFrom(kernel, runtime, ops, source_distinct_counts, scratch, 0, input);
}

} // namespace duckdb

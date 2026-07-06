//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_native_sink_runtime.hpp"
#include "sljit_nested_loop_join_runtime.hpp"
#include "sljit_projection_direct_append_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_runtime_batch_runtime.hpp"

namespace duckdb {

struct SljitNativePipelineGroupedFinishState {
	SljitNativePipelineGroupedFinishState(ExecutionOperatorRuntime &native_runtime_p,
	                                      vector<SljitExecutableRegionOp> &ops_p)
	    : native_runtime(native_runtime_p), ops(ops_p) {
	}

	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	bool deferred = false;
	SljitDataChunkBatch pending_aggregate_update;
	idx_t pending_aggregate_update_idx = DConstants::INVALID_INDEX;

	optional_ptr<bool> Prepare(idx_t aggregate_idx_p) {
		aggregate_idx = aggregate_idx_p;
		return optional_ptr<bool>(&deferred);
	}

	bool TryExecuteBatchedTerminalAggregateUpdate(ExecutionRegionRuntime &runtime,
	                                              ExecutionOperatorRuntime &native_runtime,
	                                              SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                              SljitExecutableRegionOp &op, DataChunk &input,
	                                              SinkResultType &sink_result) {
		if (!CanBatchTerminalAggregateUpdate(op, input)) {
			return false;
		}
		if (pending_aggregate_update.Empty() && input.size() >= STANDARD_VECTOR_SIZE / 2) {
			return false;
		}
		pending_aggregate_update.Ensure(runtime.GetAllocator(), input.GetTypes());
		pending_aggregate_update_idx = op_idx;
		auto execute_batch = [&](DataChunk &batch) {
			sink_result = ExecuteAggregateUpdateBatch(runtime, native_runtime, scratch, op_idx, op, batch);
			if (SljitSinkResultStopsPipeline(sink_result)) {
				SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, op_idx, deferred);
				return true;
			}
			RecordSljitRegionRuntimePath(runtime, op.kind, "pending_grouped_aggregate_update_flush", batch.size());
			return false;
		};
		auto flush_batch = [&]() {
			return SljitFlushDataChunkBatch(pending_aggregate_update.chunk, execute_batch);
		};
		auto trace_op = optional_ptr<const SljitExecutableRegionOp>(&op);
		if (SljitAppendChunkToInitializedBatch(runtime, pending_aggregate_update.chunk, input, op_idx, trace_op,
		                                       "pending_grouped_aggregate_input_append",
		                                       "pending_grouped_aggregate_input", flush_batch, execute_batch)) {
			return true;
		}
		sink_result = SinkResultType::NEED_MORE_INPUT;
		return true;
	}

	void Finish(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch) {
		FlushPendingAggregateUpdate(runtime, scratch);
		if (!deferred) {
			return;
		}
		if (aggregate_idx == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT native pipeline deferred grouped finish has no aggregate index");
		}
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, aggregate_idx, deferred);
		aggregate_idx = DConstants::INVALID_INDEX;
	}

private:
	ExecutionOperatorRuntime &native_runtime;
	vector<SljitExecutableRegionOp> &ops;

	static bool CanBatchTerminalAggregateUpdate(SljitExecutableRegionOp &op, DataChunk &input) {
		if (input.size() == 0 || op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
		    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !op.aggregate_update.plan.use_grouped_state_addresses) {
			return false;
		}
		for (auto &type : input.GetTypes()) {
			if (!TypeIsConstantSize(type.InternalType())) {
				return false;
			}
		}
		return true;
	}

	SinkResultType ExecuteAggregateUpdateBatch(ExecutionRegionRuntime &runtime,
	                                           ExecutionOperatorRuntime &native_runtime,
	                                           SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                           SljitExecutableRegionOp &op, DataChunk &batch) {
		auto deferred_grouped_finish = Prepare(op_idx);
		auto result = SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, batch, nullptr,
		                                                DConstants::INVALID_INDEX, true, deferred_grouped_finish);
		return native_runtime.RecordSinkResult(batch, result);
	}

	void FlushPendingAggregateUpdate(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch) {
		if (pending_aggregate_update.Empty()) {
			return;
		}
		if (pending_aggregate_update_idx >= ops.size()) {
			throw InternalException("SLJIT pending grouped aggregate update has no operator");
		}
		auto &op = ops[pending_aggregate_update_idx];
		auto execute_batch = [&](DataChunk &batch) {
			auto result =
			    ExecuteAggregateUpdateBatch(runtime, native_runtime, scratch, pending_aggregate_update_idx, op, batch);
			if (SljitSinkResultStopsPipeline(result)) {
				SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, pending_aggregate_update_idx, deferred);
				return true;
			}
			RecordSljitRegionRuntimePath(runtime, op.kind, "pending_grouped_aggregate_update_flush", batch.size());
			return false;
		};
		SljitFlushDataChunkBatch(pending_aggregate_update.chunk, execute_batch);
		pending_aggregate_update_idx = DConstants::INVALID_INDEX;
	}
};

static bool SljitNativePipelineAggregateCanDeferGroupedFinish(const SljitExecutableRegionOp &op) {
	return op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	       op.aggregate_update.plan.use_grouped_state_addresses;
}

struct SljitNativePipelineTerminalPolicy {
	static bool CanDeferGroupedFinish() {
		return true;
	}

	static bool TryExecuteTerminalSink(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                                   SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
	                                   DataChunk &input, bool is_final_operator, SinkResultType &sink_result,
	                                   optional_ptr<bool> deferred_grouped_finish = nullptr) {
		return SljitTryExecuteNativeTerminalSink(runtime, native_runtime, scratch, op_idx, op, input, is_final_operator,
		                                         sink_result, deferred_grouped_finish);
	}
};

struct SljitNativeTailHandoffTerminalPolicy {
	static bool CanDeferGroupedFinish() {
		return false;
	}

	static bool TryExecuteTerminalSink(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                                   SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
	                                   DataChunk &input, bool is_final_operator, SinkResultType &sink_result,
	                                   optional_ptr<bool> deferred_grouped_finish = nullptr) {
		return SljitTryExecuteNativeTailTerminalSink(runtime, native_runtime, scratch, op_idx, op, input,
		                                             is_final_operator, sink_result, deferred_grouped_finish);
	}
};

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType
SljitExecuteNativeFullPipelineFrom(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                   vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
                                   SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input,
                                   optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish = nullptr);

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType
SljitExecuteNativeFullPipeline(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                               const vector<idx_t> &source_distinct_counts, SljitRegionExecutionScratch &scratch,
                               DataChunk &input,
                               optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish = nullptr);

template <class KERNEL, class TERMINAL_POLICY>
struct SljitNativePipelineExecutor {
	SljitNativePipelineExecutor(KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p,
	                            vector<SljitExecutableRegionOp> &ops_p, const vector<idx_t> &source_distinct_counts_p)
	    : kernel(kernel_p), runtime(runtime_p), ops(ops_p), source_distinct_counts(source_distinct_counts_p),
	      grouped_finish(runtime_p.ExecutionOperators(), ops_p) {
	}

	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	const vector<idx_t> &source_distinct_counts;

	SinkResultType operator()(SljitRegionExecutionScratch &scratch, DataChunk &input) {
		return SljitExecuteNativeFullPipeline<KERNEL, TERMINAL_POLICY>(kernel, runtime, ops, source_distinct_counts,
		                                                               scratch, input, &grouped_finish);
	}

	SinkResultType operator()(SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input) {
		return SljitExecuteNativeFullPipelineFrom<KERNEL, TERMINAL_POLICY>(
		    kernel, runtime, ops, source_distinct_counts, scratch, start_op_idx, input, &grouped_finish);
	}

	void Finalize(SljitRegionExecutionScratch &scratch) {
		grouped_finish.Finish(runtime, scratch);
	}

private:
	SljitNativePipelineGroupedFinishState grouped_finish;
};

template <class KERNEL>
static SljitNativePipelineExecutor<KERNEL, SljitNativePipelineTerminalPolicy>
SljitMakeNativePipelineExecutor(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                                const vector<idx_t> &source_distinct_counts) {
	return SljitNativePipelineExecutor<KERNEL, SljitNativePipelineTerminalPolicy>(kernel, runtime, ops,
	                                                                              source_distinct_counts);
}

template <class KERNEL>
static SljitNativePipelineExecutor<KERNEL, SljitNativeTailHandoffTerminalPolicy>
SljitMakeNativeTailPipelineExecutor(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                    vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts) {
	return SljitNativePipelineExecutor<KERNEL, SljitNativeTailHandoffTerminalPolicy>(kernel, runtime, ops,
	                                                                                 source_distinct_counts);
}

template <class KERNEL, class TERMINAL_POLICY>
static bool SljitTryExecuteNativeHashJoinProbeDirectHashJoinBuild(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
    DataChunk &join_input, DataChunk &join_output, SinkResultType &sink_result,
    optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish = nullptr) {
	auto execute_hash_join_probe =
	    SljitMakeFixedScratchRecordedHashJoinProbeCallback(kernel, runtime, native_runtime, scratch);
	auto execute_native_full_pipeline_from = [&](idx_t start_op_idx, DataChunk &next_input) {
		return SljitExecuteNativeFullPipelineFrom<KERNEL, TERMINAL_POLICY>(
		    kernel, runtime, ops, source_distinct_counts, scratch, start_op_idx, next_input, grouped_finish);
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

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType
SljitDrainNativeHashJoinProbe(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                              const vector<idx_t> &source_distinct_counts, SljitRegionExecutionScratch &scratch,
                              idx_t op_idx, DataChunk &input, DataChunk &output,
                              optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish = nullptr) {
	auto &op = ops[op_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SinkResultType direct_hash_build_result;
	if (SljitTryExecuteNativeHashJoinProbeDirectHashJoinBuild<KERNEL, TERMINAL_POLICY>(
	        kernel, runtime, native_runtime, ops, source_distinct_counts, scratch, op_idx, op, input, output,
	        direct_hash_build_result, grouped_finish)) {
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
		auto sink_result = SljitExecuteNativeFullPipelineFrom<KERNEL, TERMINAL_POLICY>(
		    kernel, runtime, ops, source_distinct_counts, scratch, op_idx + 1, output, grouped_finish);
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return sink_result;
		}
	} while (!SljitHashJoinProbeDrainFinished(op.hash_join_probe.plan.output_mode, state));
	return SinkResultType::NEED_MORE_INPUT;
}

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType SljitDrainNativeNestedLoopJoinProbe(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
    const vector<idx_t> &source_distinct_counts, SljitRegionExecutionScratch &scratch, idx_t op_idx, DataChunk &input,
    DataChunk &output, optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish = nullptr) {
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
		auto sink_result = SljitExecuteNativeFullPipelineFrom<KERNEL, TERMINAL_POLICY>(
		    kernel, runtime, ops, source_distinct_counts, scratch, op_idx + 1, output, grouped_finish);
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return sink_result;
		}
	} while (!state.finished);
	return SinkResultType::NEED_MORE_INPUT;
}

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType
SljitExecuteNativeFullPipelineFrom(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                   vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
                                   SljitRegionExecutionScratch &scratch, idx_t start_op_idx, DataChunk &input,
                                   optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish) {
	if (input.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &native_runtime = runtime.ExecutionOperators();
	DataChunk *current = &input;
	for (idx_t op_idx = start_op_idx; op_idx < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		SinkResultType terminal_sink_result;
		optional_ptr<bool> deferred_grouped_finish;
		if (TERMINAL_POLICY::CanDeferGroupedFinish() && grouped_finish &&
		    SljitNativePipelineAggregateCanDeferGroupedFinish(op)) {
			deferred_grouped_finish = grouped_finish->Prepare(op_idx);
		}
		SinkResultType batched_terminal_sink_result;
		if (TERMINAL_POLICY::CanDeferGroupedFinish() && grouped_finish && op_idx + 1 == ops.size() &&
		    grouped_finish->TryExecuteBatchedTerminalAggregateUpdate(runtime, native_runtime, scratch, op_idx, op,
		                                                             *current, batched_terminal_sink_result)) {
			return batched_terminal_sink_result;
		}
		if (TERMINAL_POLICY::TryExecuteTerminalSink(runtime, native_runtime, scratch, op_idx, op, *current,
		                                            op_idx + 1 == ops.size(), terminal_sink_result,
		                                            deferred_grouped_finish)) {
			return native_runtime.RecordSinkResult(*current, terminal_sink_result);
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
			return SljitDrainNativeHashJoinProbe<KERNEL, TERMINAL_POLICY>(
			    kernel, runtime, ops, source_distinct_counts, scratch, op_idx, *current, output, grouped_finish);
		}
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE: {
			auto &output = scratch.TemporaryChunk(op_idx);
			output.Reset();
			return SljitDrainNativeNestedLoopJoinProbe<KERNEL, TERMINAL_POLICY>(
			    kernel, runtime, ops, source_distinct_counts, scratch, op_idx, *current, output, grouped_finish);
		}
		default:
			throw InternalException("Invalid SLJIT full pipeline operator before sink");
		}
	}
	throw InternalException("SLJIT full pipeline region has no native sink operator");
}

template <class KERNEL, class TERMINAL_POLICY>
static SinkResultType
SljitExecuteNativeFullPipeline(KERNEL &kernel, ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
                               const vector<idx_t> &source_distinct_counts, SljitRegionExecutionScratch &scratch,
                               DataChunk &input, optional_ptr<SljitNativePipelineGroupedFinishState> grouped_finish) {
	return SljitExecuteNativeFullPipelineFrom<KERNEL, TERMINAL_POLICY>(kernel, runtime, ops, source_distinct_counts,
	                                                                   scratch, 0, input, grouped_finish);
}

} // namespace duckdb

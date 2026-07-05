//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_primitive_contract.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_generated_filter_primitive.hpp"
#include "sljit_mark_probe_filter_boundary.hpp"
#include "sljit_projection_chain_runtime.hpp"

namespace duckdb {

static bool SljitFullPipelinePrimitiveStepHasOpCount(const SljitFullPipelinePrimitiveStep &step, idx_t expected_count) {
	return step.op_count == expected_count;
}

static bool SljitFullPipelineSourcePrimitiveIsExecutable(const SljitFullPipelinePrimitiveStep &step) {
	return step.kind == SljitFullPipelinePrimitiveKind::SOURCE_FETCH &&
	       SljitFullPipelinePrimitiveStepHasOpCount(step, 0);
}

static bool SljitFullPipelinePrimitiveOwnsSourceBatchAdvance(const SljitFullPipelinePrimitiveStep &step) {
	switch (step.kind) {
	case SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY:
		return true;
	default:
		return false;
	}
}

static bool SljitFullPipelineSourceFetchOwnsSinkAdvance(const SljitFullPipelinePrimitiveSequence &primitive_sequence) {
	if (primitive_sequence.Count() < 2 || !SljitFullPipelineSourcePrimitiveIsExecutable(primitive_sequence.Step(0))) {
		throw InternalException("SLJIT source-fetch sink ownership requires an executable primitive sequence");
	}
	return !SljitFullPipelinePrimitiveOwnsSourceBatchAdvance(primitive_sequence.Step(1));
}

static bool SljitNativeTailHandoffCanConsumeTail(const vector<SljitExecutableRegionOp> &ops, idx_t tail_start_idx) {
	if (tail_start_idx >= ops.size()) {
		return false;
	}
	auto &tail = ops[tail_start_idx];
	if (tail.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE && tail.aggregate_update.plan.use_primitive_payloads) {
		return false;
	}
	return true;
}

static bool SljitFullPipelineIntermediatePrimitiveIsExecutable(const vector<SljitExecutableRegionOp> &ops,
                                                               const SljitFullPipelinePrimitiveStep &step) {
	switch (step.kind) {
	case SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) && step.Op(0) < ops.size();
	case SljitFullPipelinePrimitiveKind::GENERATED_FILTER:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) && step.generated_filter.filter_idx == step.Op(0) &&
		       SljitCanBindGeneratedFilterPrimitive(ops, step.generated_filter.filter_idx);
	case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) &&
		       SljitCanBindHashJoinProbeMaterializePrimitive(ops, step.Op(0));
	case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) &&
		       SljitCanBindHashJoinProbeSelectionPrimitive(ops, step.Op(0));
	case SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 2) &&
		       step.mark_probe_filter_boundary.hash_join_idx == step.Op(0) &&
		       step.mark_probe_filter_boundary.filter_idx == step.Op(1) &&
		       SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, step.mark_probe_filter_boundary);
	case SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 2) &&
		       step.projection_chain.first_projection_idx == step.Op(0) &&
		       step.projection_chain.final_projection_idx == step.Op(1) &&
		       SljitCanBindProjectionChainPrimitive(ops, step.projection_chain.first_projection_idx,
		                                            step.projection_chain.final_projection_idx);
	default:
		return false;
	}
}

static bool SljitFullPipelineTerminalPrimitiveIsExecutable(const vector<SljitExecutableRegionOp> &ops,
                                                           const SljitFullPipelinePrimitiveStep &step) {
	switch (step.kind) {
	case SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 2) &&
		       SljitCanBindJoinProjectionAggregateUpdatePrimitive(ops, step.join_projection_aggregate_update);
	case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) &&
		       step.ungrouped_aggregate_update.aggregate_idx == step.Op(0) &&
		       SljitCanBindUngroupedAggregateUpdatePrimitive(ops, step.ungrouped_aggregate_update);
	case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
		if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, step.grouped_aggregate_update)) {
			return false;
		}
		switch (step.grouped_aggregate_update.input_kind) {
		case SljitGroupedAggregateUpdateInputKind::MATERIALIZED:
			return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) &&
			       step.grouped_aggregate_update.aggregate_idx == step.Op(0);
		case SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT:
			return SljitFullPipelinePrimitiveStepHasOpCount(step, 3) &&
			       step.grouped_aggregate_update.first_projection_idx == step.Op(0) &&
			       step.grouped_aggregate_update.final_projection_idx == step.Op(1) &&
			       step.grouped_aggregate_update.aggregate_idx == step.Op(2);
		}
		return false;
	case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
		if (!SljitCanBindDelimJoinSinkPrimitive(ops, step.delim_join_sink)) {
			return false;
		}
		if (step.delim_join_sink.HasProjection()) {
			return SljitFullPipelinePrimitiveStepHasOpCount(step, 3) &&
			       step.delim_join_sink.first_projection_idx == step.Op(0) &&
			       step.delim_join_sink.final_projection_idx == step.Op(1) &&
			       step.delim_join_sink.sink_idx == step.Op(2);
		}
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) && step.delim_join_sink.sink_idx == step.Op(0);
	case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF:
		return SljitFullPipelinePrimitiveStepHasOpCount(step, 1) &&
		       SljitNativeTailHandoffCanConsumeTail(ops, step.Op(0));
	default:
		return false;
	}
}

static bool SljitFullPipelinePrimitiveSequenceIsExecutable(const vector<SljitExecutableRegionOp> &ops,
                                                           const SljitFullPipelinePrimitiveSequence &sequence) {
	if (sequence.Count() < 2 || !SljitFullPipelineSourcePrimitiveIsExecutable(sequence.Step(0))) {
		return false;
	}
	for (idx_t step_idx = 1; step_idx < sequence.Count(); step_idx++) {
		auto &step = sequence.Step(step_idx);
		const bool terminal_step = step_idx == sequence.Count() - 1;
		if (terminal_step) {
			return SljitFullPipelineTerminalPrimitiveIsExecutable(ops, step);
		}
		if (!SljitFullPipelineIntermediatePrimitiveIsExecutable(ops, step)) {
			return false;
		}
	}
	return false;
}

static const SljitFullPipelinePrimitiveStep &
SljitFullPipelinePrimitiveSequenceTerminalStep(const SljitFullPipelinePrimitiveSequence &sequence) {
	D_ASSERT(sequence.Count() > 0);
	return sequence.Step(sequence.Count() - 1);
}

} // namespace duckdb

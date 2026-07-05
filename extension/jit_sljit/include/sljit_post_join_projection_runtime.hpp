//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

namespace duckdb {

static bool SljitTryFastProjectJoinOutput(ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops,
                                          const SljitPostJoinProjectionStrategy &strategy, DataChunk &join_output,
                                          DataChunk &projected) {
	if (!strategy.HasFastPath()) {
		return false;
	}
	if (strategy.fast_path == SljitPostJoinProjectionFastPath::STRING_SET_CASE_GROUPED_PAYLOAD) {
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (!SljitTryFastProjectStringSetCaseGroupedPayload(strategy.string_set_case_projection, join_output,
		                                                    projected)) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, strategy.final_projection_idx, ops[strategy.final_projection_idx].kind,
		                              "post_join_string_set_case_projection", projection_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, ops[strategy.final_projection_idx].kind,
		                                         "copied_post_join_projection", projected.size());
		return true;
	}
	throw InternalException("Unsupported SLJIT post-join projection fast path");
}

static void SljitProjectProjectionStep(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                       vector<SljitExecutableRegionOp> &ops, idx_t projection_idx, DataChunk &input,
                                       DataChunk &output, const char *reference_phase, const char *batch_phase) {
	auto &projection_op = ops[projection_idx];
	output.Reset();
	auto projection_stage_start = SljitRegionStageStart(runtime);
	if (SljitTryReferenceProjection(output, input, projection_op)) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, reference_phase,
		                              projection_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "reference_post_join_projection",
		                                         output.size());
		return;
	}
	SljitExecuteProjection(scratch, projection_idx, projection_op, input, output);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, batch_phase, projection_stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "copied_post_join_projection", output.size());
}

static void SljitProjectPostJoinProjectionChain(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                idx_t final_projection_idx, DataChunk &input, DataChunk &projected,
                                                const char *reference_phase = "post_join_reference_projection",
                                                const char *batch_phase = "post_join_batch_projection") {
	DataChunk *current = &input;
	for (idx_t projection_idx = first_projection_idx; projection_idx <= final_projection_idx; projection_idx++) {
		auto &projection_output =
		    projection_idx == final_projection_idx ? projected : scratch.TemporaryChunk(projection_idx);
		SljitProjectProjectionStep(runtime, scratch, ops, projection_idx, *current, projection_output, reference_phase,
		                           batch_phase);
		current = &projection_output;
		if (current->size() == 0) {
			projected.Reset();
			return;
		}
	}
}

static void SljitRecordDirectProjectionChainUnsupported(ExecutionRegionRuntime &runtime,
                                                        const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t trace_projection_idx, const char *reason, idx_t count) {
	if (trace_projection_idx == DConstants::INVALID_INDEX || trace_projection_idx >= ops.size()) {
		return;
	}
	auto path = string("direct_projection_chain_unsupported.") + reason;
	RecordSljitRegionRuntimePath(runtime, ops[trace_projection_idx].kind, path.c_str(), count);
}

static bool SljitTryBuildPostJoinProjectionDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                      SljitPostJoinProjectionStrategy &strategy) {
	auto &descriptor = strategy.descriptor;
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	auto first_projection_idx = strategy.first_projection_idx;
	auto final_projection_idx = strategy.final_projection_idx;
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx ||
	    ops[first_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    ops[final_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		return descriptor.Block("shape");
	}
	if (strategy.direct_projection_disabled_reason) {
		return descriptor.Block(strategy.direct_projection_disabled_reason);
	}
	if (first_projection_idx == final_projection_idx) {
		descriptor.BorrowProjection(first_projection_idx, ops[first_projection_idx]);
		descriptor.MarkReady();
		return true;
	}
	if (final_projection_idx == first_projection_idx + 1 &&
	    SljitBuildReferenceProjectionOutputMap(ops[first_projection_idx], ops[final_projection_idx],
	                                           descriptor.output_to_projection)) {
		descriptor.BorrowProjection(first_projection_idx, ops[first_projection_idx]);
		descriptor.MarkReady();
		return true;
	}

	auto composed_projection = make_uniq<SljitExecutableRegionOp>();
	string compose_blocker;
	if (!SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
	                                                 *composed_projection, optional_ptr<string>(&compose_blocker))) {
		if (compose_blocker.empty()) {
			compose_blocker = "compose";
		} else {
			compose_blocker = "compose_" + compose_blocker;
		}
		return descriptor.Block(compose_blocker.c_str());
	}
	descriptor.OwnProjection(final_projection_idx, std::move(*composed_projection));
	descriptor.MarkReady();
	return true;
}

static bool SljitTryDirectMaterializeJoinProjectionChainToBatch(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &strategy, DataChunk *join_input, const SelectionVector *match_selection,
    Vector *row_pointers, DataChunk &join_output, DataChunk &batch) {
	auto hash_join_idx = strategy.hash_join_idx;
	if (join_output.size() == 0) {
		return false;
	}
	if (!SljitTryBuildPostJoinProjectionDescriptor(ops, strategy)) {
		SljitRecordDirectProjectionChainUnsupported(runtime, ops, strategy.trace_projection_idx,
		                                            strategy.descriptor.Blocker().c_str(), join_output.size());
		return false;
	}
	auto &descriptor = strategy.descriptor;
	auto &projection_op = descriptor.Projection();
	auto output_map = descriptor.OutputMap();

	if (join_input && match_selection && row_pointers) {
		if (SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
		        runtime, ops, scratch, hash_join_idx, descriptor.projection_idx, projection_op, *join_input,
		        *match_selection, *row_pointers, join_output, batch, output_map)) {
			return true;
		}
		SljitRecordDirectProjectionChainUnsupported(runtime, ops, strategy.trace_projection_idx, "probe_materialize",
		                                            join_output.size());
		return false;
	}

	if (SljitTryDirectMaterializeFixedProjectionToBatch(runtime, scratch, descriptor.projection_idx, projection_op,
	                                                    join_output, batch, output_map)) {
		return true;
	}
	SljitRecordDirectProjectionChainUnsupported(runtime, ops, strategy.trace_projection_idx, "materialized_projection",
	                                            join_output.size());
	return false;
}

} // namespace duckdb

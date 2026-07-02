//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_between_join_compressed_group_key_plan_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_between_join_sidecar_plan_common_runtime.hpp"

namespace duckdb {

static bool SljitCollectFinalGroupCompressedPassthroughSources(
    SljitBetweenJoinSidecars &sidecars, SljitFinalProjectionAggregateBridge &bridge,
    SljitExecutableRegionOp &second_join_projection_op, SljitExecutableRegionOp &final_projection_op,
    const ExecutionHashJoinProbeBinding &second_binding, vector<SljitFinalGroupCompressedPassthroughSource> &sources) {
	sources.clear();
	if (sidecars.compressed_passthrough_batch.ColumnCount() == 0 || bridge.group_projection_indices.empty() ||
	    !second_binding.ready ||
	    second_binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    second_binding.lhs_output_column_indices.empty() ||
	    second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < bridge.group_projection_indices.size(); output_idx++) {
		const auto projection_idx = bridge.group_projection_indices[output_idx];
		if (projection_idx >= final_projection_op.projections.size()) {
			return false;
		}
		auto &final_plan = final_projection_op.projections[projection_idx].plan;
		if (final_plan.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		    final_plan.source_index >= second_join_projection_op.projections.size()) {
			continue;
		}
		SljitExecutableRegionExpression remapped_expr;
		idx_t second_join_output_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(
		        second_join_projection_op.projections[final_plan.source_index], remapped_expr,
		        second_join_output_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
		    second_join_output_source_idx >= second_binding.lhs_output_column_indices.size()) {
			continue;
		}
		const auto second_join_input_col = second_binding.lhs_output_column_indices[second_join_output_source_idx];
		if (second_join_input_col >= sidecars.compressed_passthrough_by_between_projection.size()) {
			continue;
		}
		const auto sidecar_idx = sidecars.compressed_passthrough_by_between_projection[second_join_input_col];
		if (sidecar_idx == DConstants::INVALID_INDEX ||
		    sidecar_idx >= sidecars.compressed_passthrough_batch.ColumnCount()) {
			continue;
		}
		auto &source = sidecars.compressed_passthrough_batch.chunk.data[sidecar_idx];
		if (source.GetType() != final_projection_op.output_types[projection_idx] ||
		    source.GetType() != final_plan.return_type) {
			continue;
		}
		SljitFinalGroupCompressedPassthroughSource passthrough;
		passthrough.final_group_output_idx = output_idx;
		passthrough.final_projection_idx = projection_idx;
		passthrough.second_join_projection_idx = final_plan.source_index;
		passthrough.second_join_input_col = second_join_input_col;
		passthrough.sidecar_idx = sidecar_idx;
		sources.push_back(passthrough);
	}
	return !sources.empty();
}

static bool SljitBuildFinalGroupKeyPassthroughs(SljitRegionExecutionScratch &scratch, idx_t second_hash_join_idx,
                                                SljitBetweenJoinSidecars &sidecars,
                                                SljitFinalProjectionAggregateBridge &bridge,
                                                SljitExecutableRegionOp &second_join_projection_op,
                                                SljitExecutableRegionOp &final_projection_op,
                                                idx_t second_join_batch_count,
                                                vector<SljitDirectProjectionBatchPassthrough> &passthroughs) {
	passthroughs.clear();
	if (sidecars.compressed_passthrough_batch.Count() != second_join_batch_count ||
	    !scratch.HasOperatorBinding(second_hash_join_idx)) {
		return false;
	}
	auto &second_binding = scratch.OperatorBinding(second_hash_join_idx).hash_join_probe;
	vector<SljitFinalGroupCompressedPassthroughSource> sources;
	if (!SljitCollectFinalGroupCompressedPassthroughSources(sidecars, bridge, second_join_projection_op,
	                                                        final_projection_op, second_binding, sources)) {
		return false;
	}
	for (auto &source_info : sources) {
		auto &source = sidecars.compressed_passthrough_batch.chunk.data[source_info.sidecar_idx];
		SljitDirectProjectionBatchPassthrough passthrough;
		passthrough.output_idx = source_info.final_group_output_idx;
		passthrough.source = &source;
		passthrough.selection = &scratch.FilterSelection(second_hash_join_idx);
		passthrough.trace_phase = "direct_batch_expression.compressed_passthrough";
		passthroughs.push_back(passthrough);
	}
	return !passthroughs.empty();
}

static bool SljitFinalPayloadUsesSecondJoinInputCol(SljitExecutableRegionOp &second_join_projection_op,
                                                    SljitFinalProjectionAggregateBridge &bridge,
                                                    const ExecutionHashJoinProbeBinding &second_binding,
                                                    idx_t input_col) {
	if (second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return true;
	}
	for (auto payload_source_idx : bridge.payload_source_indices) {
		if (payload_source_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (payload_source_idx >= second_join_projection_op.projections.size()) {
			return true;
		}
		if (SljitProjectionMayReferenceSecondJoinInputCol(second_join_projection_op.projections[payload_source_idx],
		                                                  second_binding, input_col)) {
			return true;
		}
	}
	return false;
}

static const char *SljitBuildCompressedGroupKeyPlanFromSources(
    SljitExecutableRegionOp &second_hash_join_op, const ExecutionHashJoinProbeBinding &second_binding,
    SljitExecutableRegionOp &second_join_projection_op, SljitFinalProjectionAggregateBridge &final_aggregate,
    const vector<SljitFinalGroupCompressedPassthroughSource> &sources, SljitBetweenJoinCompressedGroupKeyPlan &plan) {
	bool skipped_any = false;
	for (auto &source_info : sources) {
		if (source_info.second_join_input_col >= plan.between_skip.size() ||
		    source_info.second_join_projection_idx >= plan.second_skip.size()) {
			return "source_index";
		}
		if (SljitSecondJoinInputColIsProbeKey(second_hash_join_op, second_binding, source_info.second_join_input_col)) {
			return "probe_key";
		}
		if (SljitFinalPayloadUsesSecondJoinInputCol(second_join_projection_op, final_aggregate, second_binding,
		                                            source_info.second_join_input_col)) {
			return "payload_dependency";
		}
		plan.between_skip[source_info.second_join_input_col] = 1;
		plan.second_skip[source_info.second_join_projection_idx] = 1;
		skipped_any = true;
	}
	return skipped_any ? nullptr : "no_skipped_projection";
}

} // namespace duckdb

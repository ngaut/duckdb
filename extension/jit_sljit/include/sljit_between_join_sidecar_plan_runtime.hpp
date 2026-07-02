//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_between_join_sidecar_plan_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_between_join_compressed_group_key_plan_runtime.hpp"
#include "sljit_between_join_precomputed_payload_plan_runtime.hpp"

namespace duckdb {

static bool SljitBuildPrecomputedPayloadProjectionSkips(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    vector<SljitExecutableRegionOp> &ops, idx_t first_hash_join_idx, idx_t between_projection_idx,
    idx_t second_hash_join_idx, idx_t second_projection_idx, idx_t final_projection_idx, idx_t aggregate_idx,
    SljitTwoJoinGroupedAggregateRouteState &state) {
	auto &projection_skips = state.projection_skips;
	if (projection_skips.precomputed_payload.Built()) {
		return projection_skips.precomputed_payload.Ready();
	}
	auto &between_join_projection_op = ops[between_projection_idx];
	auto &second_hash_join_op = ops[second_hash_join_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &between_join_sidecars = state.between_join_sidecars;
	auto &final_aggregate = state.final_aggregate;
	auto &second_join_batch = state.second_join_batch;
	auto record_skip_blocker = [&](const char *reason) {
		auto path = string("direct_between_join_precomputed_payload_skip_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), 1);
	};
	auto block_skip = [&](const char *reason) {
		record_skip_blocker(reason);
		return projection_skips.precomputed_payload.Block(reason);
	};
	SljitBetweenJoinPrecomputedPayloadPlan plan;
	const auto between_projection_count = between_join_projection_op.projections.size();
	plan.between_skip.assign(between_projection_count, 0);
	if (ops.size() <= second_projection_idx) {
		return block_skip("operator_count");
	}
	auto &second_join_projection_op = ops[second_projection_idx];
	plan.second_skip.assign(second_join_projection_op.projections.size(), 0);
	if (second_hash_join_op.hash_join_probe.plan.residual_predicate) {
		return block_skip("second_join_residual");
	}

	DataChunk descriptor_input;
	descriptor_input.InitializeEmpty(second_join_projection_op.output_types);
	if (!SljitBuildFinalSplitPayloadDescriptor(final_aggregate, final_projection_op, aggregate_op, descriptor_input)) {
		return block_skip("final_payload_descriptor");
	}
	if (final_aggregate.payload_source_indices.empty()) {
		return block_skip("no_payload_sources");
	}

	if (!SljitEnsureNativeHashJoinProbeBinding(native_runtime, scratch, second_hash_join_idx, second_hash_join_op,
	                                           second_join_batch)) {
		return block_skip("second_join_bind");
	}
	auto &second_binding = scratch.OperatorBinding(second_hash_join_idx).hash_join_probe;
	if (!second_binding.ready ||
	    second_binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    second_binding.lhs_output_column_indices.empty()) {
		return block_skip("second_join_binding");
	}

	auto &first_hash_join_op = ops[first_hash_join_idx];
	vector<SljitBetweenJoinPrecomputedPayload> candidates;
	SljitCollectPrecomputedPayloadCandidates(first_hash_join_op, between_join_projection_op, second_join_projection_op,
	                                         second_join_batch, final_aggregate, second_binding, candidates,
	                                         record_skip_blocker);
	if (candidates.empty()) {
		return block_skip("no_candidate");
	}
	if (!SljitBuildPrecomputedPayloadPlanFromCandidates(second_hash_join_op, second_binding, second_join_projection_op,
	                                                    projection_skips, between_projection_count, candidates, plan)) {
		return block_skip("no_dead_between_projection");
	}
	projection_skips.between_precomputed_payload = std::move(plan.between_skip);
	projection_skips.second_precomputed_payload = std::move(plan.second_skip);
	between_join_sidecars.precomputed_payloads = std::move(plan.payloads);
	between_join_sidecars.precomputed_payload_batch.Ensure(runtime.GetAllocator(), plan.payload_types);
	projection_skips.precomputed_payload.MarkReady();
	return true;
}

static bool SljitBuildCompressedGroupKeyProjectionSkips(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    vector<SljitExecutableRegionOp> &ops, idx_t first_hash_join_idx, idx_t between_projection_idx,
    idx_t second_hash_join_idx, idx_t second_projection_idx, idx_t final_projection_idx, idx_t aggregate_idx,
    SljitTwoJoinGroupedAggregateRouteState &state) {
	auto &projection_skips = state.projection_skips;
	if (projection_skips.compressed_group_key.Built()) {
		return projection_skips.compressed_group_key.Ready();
	}
	auto &first_hash_join_op = ops[first_hash_join_idx];
	auto &between_join_projection_op = ops[between_projection_idx];
	auto &second_hash_join_op = ops[second_hash_join_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &between_join_sidecars = state.between_join_sidecars;
	auto &final_aggregate = state.final_aggregate;
	auto &second_join_batch = state.second_join_batch;
	auto record_skip_blocker = [&](const char *reason) {
		auto path = string("direct_between_join_compressed_group_key_skip_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), 1);
	};
	auto block_skip = [&](const char *reason) {
		record_skip_blocker(reason);
		return projection_skips.compressed_group_key.Block(reason);
	};
	SljitBetweenJoinCompressedGroupKeyPlan plan;
	plan.between_skip.assign(between_join_projection_op.projections.size(), 0);
	if (ops.size() <= second_projection_idx) {
		return block_skip("operator_count");
	}
	auto &second_join_projection_op = ops[second_projection_idx];
	plan.second_skip.assign(second_join_projection_op.projections.size(), 0);
	if (second_hash_join_op.hash_join_probe.plan.residual_predicate) {
		return block_skip("second_join_residual");
	}
	SljitInitializeBetweenJoinCompressedPassthroughs(runtime.GetAllocator(), between_join_projection_op,
	                                                 first_hash_join_op, between_join_sidecars);
	if (between_join_sidecars.compressed_passthroughs.empty()) {
		return block_skip("no_compressed_passthrough_source");
	}

	DataChunk descriptor_input;
	descriptor_input.InitializeEmpty(second_join_projection_op.output_types);
	if (!SljitBuildFinalSplitPayloadDescriptor(final_aggregate, final_projection_op, aggregate_op, descriptor_input)) {
		return block_skip("final_payload_descriptor");
	}

	if (!SljitEnsureNativeHashJoinProbeBinding(native_runtime, scratch, second_hash_join_idx, second_hash_join_op,
	                                           second_join_batch)) {
		return block_skip("second_join_bind");
	}
	auto &second_binding = scratch.OperatorBinding(second_hash_join_idx).hash_join_probe;
	vector<SljitFinalGroupCompressedPassthroughSource> sources;
	if (!SljitCollectFinalGroupCompressedPassthroughSources(between_join_sidecars, final_aggregate,
	                                                        second_join_projection_op, final_projection_op,
	                                                        second_binding, sources)) {
		return block_skip("final_group_passthrough");
	}
	if (const char *blocker = SljitBuildCompressedGroupKeyPlanFromSources(
	        second_hash_join_op, second_binding, second_join_projection_op, final_aggregate, sources, plan)) {
		return block_skip(blocker);
	}
	projection_skips.between_compressed_group_key = std::move(plan.between_skip);
	projection_skips.second_compressed_group_key = std::move(plan.second_skip);
	projection_skips.compressed_group_key.MarkReady();
	return true;
}

} // namespace duckdb

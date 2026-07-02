//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_between_join_precomputed_payload_plan_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_between_join_sidecar_plan_common_runtime.hpp"

namespace duckdb {

static void SljitAddUniqueIndex(vector<idx_t> &values, idx_t value) {
	for (auto existing : values) {
		if (existing == value) {
			return;
		}
	}
	values.push_back(value);
}

static void SljitBuildPrecomputedPayloadLiveSecondProjectionSkip(
    const SljitBetweenJoinProjectionSkips &projection_skips, idx_t second_projection_count,
    const vector<SljitBetweenJoinPrecomputedPayload> &candidates, const vector<uint8_t> &selected_candidate,
    vector<uint8_t> &live_second_projection_skip) {
	live_second_projection_skip.assign(second_projection_count, 0);
	projection_skips.ApplySecondCompressedGroupKeySkips(live_second_projection_skip);
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		if (!selected_candidate[candidate_idx]) {
			continue;
		}
		auto projection_idx = candidates[candidate_idx].second_projection_idx;
		if (projection_idx < live_second_projection_skip.size()) {
			live_second_projection_skip[projection_idx] = 1;
		}
	}
}

static bool SljitPrecomputedPayloadCanSkipBetweenInput(SljitExecutableRegionOp &second_hash_join_op,
                                                       const ExecutionHashJoinProbeBinding &second_binding,
                                                       SljitExecutableRegionOp &second_join_projection_op,
                                                       idx_t between_projection_count,
                                                       idx_t between_input_projection_idx,
                                                       const vector<uint8_t> &live_second_projection_skip) {
	if (between_input_projection_idx >= between_projection_count) {
		return false;
	}
	if (SljitSecondJoinInputColIsProbeKey(second_hash_join_op, second_binding, between_input_projection_idx)) {
		return false;
	}
	return !SljitSecondJoinInputColUsedByLiveProjection(second_join_projection_op, second_binding,
	                                                    between_input_projection_idx, live_second_projection_skip);
}

template <class RECORD_BLOCKER>
static bool SljitTryBuildPrecomputedPayloadCandidate(
    SljitExecutableRegionOp &first_hash_join_op, SljitExecutableRegionOp &between_join_projection_op,
    SljitExecutableRegionOp &second_join_projection_op, DataChunk &second_join_batch,
    const ExecutionHashJoinProbeBinding &second_binding, idx_t payload_source_idx,
    SljitBetweenJoinPrecomputedPayload &candidate, RECORD_BLOCKER &record_skip_blocker) {
	auto &payload_projection = second_join_projection_op.projections[payload_source_idx];
	auto &payload_type = second_join_projection_op.output_types[payload_source_idx];
	if (payload_projection.plan.return_type != payload_type || !SljitDirectProjectionBatchSupportsType(payload_type)) {
		record_skip_blocker("payload_type");
		return false;
	}
	vector<uint8_t> referenced_sources;
	if (!SljitTryCollectHashJoinProjectionExpressionSources(payload_projection, second_binding.output_types.size(),
	                                                        referenced_sources)) {
		record_skip_blocker("payload_sources");
		return false;
	}
	vector<idx_t> source_map(second_binding.output_types.size(), DConstants::INVALID_INDEX);
	vector<idx_t> between_projection_indices;
	bool supported = false;
	for (idx_t source_idx = 0; source_idx < referenced_sources.size(); source_idx++) {
		if (!referenced_sources[source_idx]) {
			continue;
		}
		supported = true;
		if (source_idx >= second_binding.lhs_output_column_indices.size()) {
			record_skip_blocker("rhs_dependency");
			return false;
		}
		const auto between_input_projection_idx = second_binding.lhs_output_column_indices[source_idx];
		if (between_input_projection_idx >= between_join_projection_op.projections.size() ||
		    between_input_projection_idx >= between_join_projection_op.output_types.size() ||
		    between_input_projection_idx >= second_join_batch.ColumnCount() ||
		    second_binding.output_types[source_idx] !=
		        between_join_projection_op.output_types[between_input_projection_idx]) {
			record_skip_blocker("between_projection_index");
			return false;
		}
		SljitExecutableRegionExpression remapped_between_expr;
		idx_t first_join_output_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(
		        between_join_projection_op.projections[between_input_projection_idx], remapped_between_expr,
		        first_join_output_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_between_expr.plan) ||
		    first_join_output_source_idx >= first_hash_join_op.output_types.size() ||
		    first_hash_join_op.output_types[first_join_output_source_idx] !=
		        between_join_projection_op.output_types[between_input_projection_idx]) {
			record_skip_blocker("between_projection_source");
			return false;
		}
		source_map[source_idx] = first_join_output_source_idx;
		SljitAddUniqueIndex(between_projection_indices, between_input_projection_idx);
	}
	if (!supported || between_projection_indices.empty()) {
		return false;
	}
	candidate.second_projection_idx = payload_source_idx;
	candidate.between_projection_indices = std::move(between_projection_indices);
	SljitBuildBorrowedProjectionExpression(payload_projection, candidate.first_join_expr);
	if (!SljitTryRemapHashJoinProjectionPlanSources(source_map, candidate.first_join_expr.plan)) {
		record_skip_blocker("payload_remap_plan");
		return false;
	}
	if (!SljitTryRemapHashJoinProjectionExpressionInputSources(source_map, candidate.first_join_expr) ||
	    candidate.first_join_expr.plan.return_type != payload_type) {
		record_skip_blocker("payload_remap_sources");
		return false;
	}
	if (candidate.first_join_expr.plan.expression_tree) {
		SljitTryBuildRuntimeDecimal64DiscountedAmountProgram(*candidate.first_join_expr.plan.expression_tree,
		                                                     candidate.first_join_expr.input_source_indices.size(),
		                                                     candidate.decimal64_discounted_amount_program);
	}
	if (!candidate.decimal64_discounted_amount_program.ready) {
		record_skip_blocker("payload_program");
		return false;
	}
	return true;
}

template <class RECORD_BLOCKER>
static void SljitCollectPrecomputedPayloadCandidates(
    SljitExecutableRegionOp &first_hash_join_op, SljitExecutableRegionOp &between_join_projection_op,
    SljitExecutableRegionOp &second_join_projection_op, DataChunk &second_join_batch,
    const SljitFinalProjectionAggregateBridge &final_aggregate, const ExecutionHashJoinProbeBinding &second_binding,
    vector<SljitBetweenJoinPrecomputedPayload> &candidates, RECORD_BLOCKER &record_skip_blocker) {
	candidates.clear();
	vector<uint8_t> candidate_seen(second_join_projection_op.projections.size(), 0);
	for (auto payload_source_idx : final_aggregate.payload_source_indices) {
		if (payload_source_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (payload_source_idx >= second_join_projection_op.projections.size() ||
		    payload_source_idx >= second_join_projection_op.output_types.size()) {
			record_skip_blocker("payload_source_index");
			continue;
		}
		if (candidate_seen[payload_source_idx]) {
			continue;
		}
		candidate_seen[payload_source_idx] = 1;
		SljitBetweenJoinPrecomputedPayload candidate;
		if (SljitTryBuildPrecomputedPayloadCandidate(first_hash_join_op, between_join_projection_op,
		                                             second_join_projection_op, second_join_batch, second_binding,
		                                             payload_source_idx, candidate, record_skip_blocker)) {
			candidates.push_back(std::move(candidate));
		}
	}
}

static bool SljitBuildPrecomputedPayloadPlanFromCandidates(SljitExecutableRegionOp &second_hash_join_op,
                                                           const ExecutionHashJoinProbeBinding &second_binding,
                                                           SljitExecutableRegionOp &second_join_projection_op,
                                                           const SljitBetweenJoinProjectionSkips &projection_skips,
                                                           idx_t between_projection_count,
                                                           vector<SljitBetweenJoinPrecomputedPayload> &candidates,
                                                           SljitBetweenJoinPrecomputedPayloadPlan &plan) {
	plan.between_skip.assign(between_projection_count, 0);
	plan.second_skip.assign(second_join_projection_op.projections.size(), 0);
	plan.payloads.clear();
	plan.payload_types.clear();

	auto candidate_skips_input = [&](const SljitBetweenJoinPrecomputedPayload &candidate,
	                                 const vector<uint8_t> &live_second_projection_skip) {
		for (auto between_input_projection_idx : candidate.between_projection_indices) {
			if (SljitPrecomputedPayloadCanSkipBetweenInput(second_hash_join_op, second_binding,
			                                               second_join_projection_op, between_projection_count,
			                                               between_input_projection_idx, live_second_projection_skip)) {
				return true;
			}
		}
		return false;
	};

	vector<uint8_t> selected_candidate(candidates.size(), 1);
	bool changed = true;
	while (changed) {
		changed = false;
		vector<uint8_t> live_second_projection_skip(second_join_projection_op.projections.size(), 0);
		SljitBuildPrecomputedPayloadLiveSecondProjectionSkip(projection_skips,
		                                                     second_join_projection_op.projections.size(), candidates,
		                                                     selected_candidate, live_second_projection_skip);
		for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
			if (!selected_candidate[candidate_idx]) {
				continue;
			}
			if (!candidate_skips_input(candidates[candidate_idx], live_second_projection_skip)) {
				selected_candidate[candidate_idx] = 0;
				changed = true;
			}
		}
	}

	vector<uint8_t> live_second_projection_skip;
	SljitBuildPrecomputedPayloadLiveSecondProjectionSkip(projection_skips, second_join_projection_op.projections.size(),
	                                                     candidates, selected_candidate, live_second_projection_skip);
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		if (!selected_candidate[candidate_idx]) {
			continue;
		}
		auto &candidate = candidates[candidate_idx];
		bool skipped_between_input = false;
		for (auto between_input_projection_idx : candidate.between_projection_indices) {
			if (SljitPrecomputedPayloadCanSkipBetweenInput(second_hash_join_op, second_binding,
			                                               second_join_projection_op, between_projection_count,
			                                               between_input_projection_idx, live_second_projection_skip)) {
				plan.between_skip[between_input_projection_idx] = 1;
				skipped_between_input = true;
			}
		}
		if (!skipped_between_input) {
			continue;
		}
		candidate.sidecar_idx = plan.payload_types.size();
		plan.second_skip[candidate.second_projection_idx] = 1;
		plan.payload_types.push_back(second_join_projection_op.output_types[candidate.second_projection_idx]);
		plan.payloads.push_back(std::move(candidate));
	}
	return !plan.payloads.empty() && SljitProjectionSkipHasAny(plan.between_skip);
}

} // namespace duckdb

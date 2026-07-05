//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_filtered_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_rhs_projection_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_ungrouped_aggregate_payload_update_runtime.hpp"

namespace duckdb {

static bool SljitCanExecuteHashJoinUngroupedAggregateUpdate(const vector<SljitExecutableRegionOp> &ops,
                                                            idx_t hash_join_idx, idx_t first_projection_idx) {
	if (hash_join_idx + 1 >= ops.size() || first_projection_idx > ops.size() ||
	    ops[hash_join_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
	    ops.back().kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &hash_join_op = ops[hash_join_idx];
	if (hash_join_op.hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}
	const auto aggregate_idx = ops.size() - 1;
	auto &aggregate_op = ops[aggregate_idx];
	if (aggregate_op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    !aggregate_op.aggregate_update.plan.use_primitive_payloads ||
	    aggregate_op.aggregate_update.plan.use_grouped_state_addresses) {
		return false;
	}
	if (!aggregate_op.aggregate_update.fused_payload_update_function &&
	    aggregate_op.aggregate_update.payloads.size() !=
	        aggregate_op.aggregate_update.payload_update_functions.size()) {
		return false;
	}
	for (idx_t op_idx = first_projection_idx; op_idx < aggregate_idx; op_idx++) {
		if (ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	for (auto &payload : aggregate_op.aggregate_update.payloads) {
		idx_t join_output_source_idx;
		LogicalType source_type;
		if (!SljitTryResolveReferenceThroughProjectionChain(ops, first_projection_idx, aggregate_idx, payload,
		                                                    join_output_source_idx, source_type)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryPrepareHashJoinFilteredUngroupedPayloadInput(
    const vector<SljitExecutableRegionOp> &ops, const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
    const SelectionVector &match_selection, Vector &row_pointers, const SelectionVector &filter_selection,
    idx_t selected_count, idx_t first_projection_idx, idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
    DataChunk &payload_input, vector<Vector> &payload_sources,
    vector<SljitExecutableRegionExpression> &remapped_payloads, SelectionVector &compact_match_selection) {
	auto &payloads = aggregate_op.aggregate_update.payloads;
	if (!binding.ready || !binding.hash_table ||
	    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
		const auto match_idx = filter_selection.get_index(selected_idx);
		compact_match_selection.set_index(selected_idx, match_selection.get_index(match_idx));
	}

	vector<LogicalType> payload_types;
	payload_types.reserve(payloads.size());
	payload_sources.clear();
	payload_sources.reserve(payloads.size());
	remapped_payloads.clear();
	remapped_payloads.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &payload = payloads[payload_idx];
		idx_t join_output_source_idx;
		LogicalType source_type;
		if (!SljitTryResolveReferenceThroughProjectionChain(ops, first_projection_idx, aggregate_idx, payload,
		                                                    join_output_source_idx, source_type) ||
		    join_output_source_idx >= binding.output_types.size() ||
		    binding.output_types[join_output_source_idx] != source_type) {
			return false;
		}

		payload_types.push_back(source_type);
		payload_sources.emplace_back(source_type);
		auto &source = payload_sources.back();
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (join_output_source_idx < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[join_output_source_idx];
			if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != source_type) {
				return false;
			}
			source.Slice(join_input.data[input_col], compact_match_selection, selected_count);
		} else {
			const auto rhs_col_idx = join_output_source_idx - lhs_column_count;
			if (rhs_col_idx >= binding.rhs_output_column_count) {
				return false;
			}
			binding.hash_table->GatherRHSColumn(row_pointers, filter_selection, selected_count, rhs_col_idx, source);
		}

		remapped_payloads.emplace_back();
		auto &remapped_payload = remapped_payloads.back();
		idx_t remapped_join_output_source_idx;
		if (!SljitTryBuildRemappedPayloadReference(payload, payload_idx, remapped_payload,
		                                           remapped_join_output_source_idx) ||
		    remapped_join_output_source_idx != join_output_source_idx) {
			return false;
		}
	}

	payload_input.InitializeEmpty(payload_types);
	for (idx_t payload_idx = 0; payload_idx < payload_sources.size(); payload_idx++) {
		payload_input.data[payload_idx].Reference(payload_sources[payload_idx]);
	}
	payload_input.SetChildCardinality(selected_count);
	vector<idx_t> payload_input_source_indices;
	payload_input_source_indices.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		payload_input_source_indices.push_back(payload_idx);
	}
	vector<bool> payload_input_source_not_null(payload_input_source_indices.size(), false);
	for (auto &remapped_payload : remapped_payloads) {
		remapped_payload.input_source_indices = payload_input_source_indices;
		remapped_payload.input_source_not_null = payload_input_source_not_null;
	}
	return true;
}

template <class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteHashJoinFilteredUngroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
    DataChunk &join_input, SinkResultType &sink_result, EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe) {
	if (hash_join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return false;
	}
	const bool has_post_join_filter =
	    hash_join_idx + 1 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::FILTER;
	const auto first_projection_idx = has_post_join_filter ? hash_join_idx + 2 : hash_join_idx + 1;
	if (!SljitCanExecuteHashJoinUngroupedAggregateUpdate(ops, hash_join_idx, first_projection_idx)) {
		return false;
	}
	const auto filter_idx = hash_join_idx + 1;
	const auto aggregate_idx = ops.size() - 1;
	auto &aggregate_op = ops[aggregate_idx];
	auto &join_selection_output = scratch.TemporaryChunk(hash_join_idx);
	auto &match_selection = scratch.FilterSelection(hash_join_idx);
	auto &build_selection = scratch.HashJoinBuildSelection(hash_join_idx);
	auto &row_pointers = scratch.HashJoinRowPointers(hash_join_idx);
	auto &compact_match_selection = build_selection;
	SljitHashJoinProbeDrainState state;
	bool updated_aggregate = false;
	auto fail = [&](const char *reason) -> bool {
		auto path = string("direct_filtered_ungrouped_aggregate_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, path.c_str());
		if (updated_aggregate || hash_join_op.hash_join_probe.plan.mark_build_match) {
			throw InternalException(
			    "SLJIT direct filtered aggregate path failed after entering stateful join/aggregate path: %s", reason);
		}
		return false;
	};

	do {
		join_selection_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(hash_join_idx, hash_join_op, join_input, join_selection_output,
		                                           state, deferred_reason, false,
		                                           SljitHashJoinProbeOutputContract::SELECTED_VIEW);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return SljitDeferBlockedSinkResult(runtime, deferred_reason, sink_result);
		}

		const auto match_count = join_selection_output.size();
		if (match_count == 0) {
			continue;
		}
		if (!scratch.HasOperatorBinding(hash_join_idx)) {
			return fail("operator_binding");
		}
		auto &join_binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		if (!join_binding.ready || join_binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
		    !join_binding.hash_table) {
			return fail("non_regular_hash_table");
		}

		const SelectionVector *selected_row_indices = FlatVector::IncrementalSelectionVector();
		auto selected_count = match_count;
		if (has_post_join_filter) {
			auto &filter_op = ops[filter_idx];
			SljitExecutableRegionExpression remapped_filter;
			DataChunk filter_input;
			vector<Vector> filter_sources;
			auto filter_input_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryBuildHashJoinProjectionExpressionInput(join_binding, filter_op.filter, join_input,
			                                                    match_selection, row_pointers, match_count,
			                                                    remapped_filter, filter_input, filter_sources)) {
				return fail("filter_input");
			}
			RecordSljitRegionStageRuntime(runtime, filter_idx, filter_op.kind, "direct_hash_join_filter_input",
			                              filter_input_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "filtered_expression_input",
			                                         match_count);

			auto filter_stage_start = SljitRegionStageStart(runtime);
			auto &filter_selection = scratch.FilterSelection(filter_idx);
			selected_count = SljitSelectExpression(remapped_filter, filter_input, filter_selection,
			                                       scratch.ExpressionAdapterScratch(filter_idx, 0));
			selected_row_indices = &filter_selection;
			RecordSljitRegionStageRuntime(runtime, filter_idx, filter_op.kind, "direct_hash_join_selection",
			                              filter_stage_start);
		}
		if (selected_count == 0) {
			continue;
		}

		DataChunk payload_input;
		vector<Vector> payload_sources;
		vector<SljitExecutableRegionExpression> remapped_payloads;
		auto payload_input_stage_start = SljitRegionStageStart(runtime);
		if (!SljitTryPrepareHashJoinFilteredUngroupedPayloadInput(
		        ops, join_binding, join_input, match_selection, row_pointers, *selected_row_indices, selected_count,
		        first_projection_idx, aggregate_idx, aggregate_op, payload_input, payload_sources, remapped_payloads,
		        compact_match_selection)) {
			return fail("payload_input");
		}
		RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
		                              "direct_hash_join_filtered_payload_input", payload_input_stage_start);

		updated_aggregate = true;
		sink_result = SljitExecuteNativeUngroupedAggregateUpdateWithPayloads(
		    runtime, native_runtime, scratch, aggregate_idx, aggregate_op, payload_input, remapped_payloads,
		    "direct_hash_join_filtered_payload_update", "direct_hash_join_filtered_payload_update",
		    "direct_hash_join_filtered_state_update");
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));

	sink_result = SinkResultType::NEED_MORE_INPUT;
	return true;
}

} // namespace duckdb

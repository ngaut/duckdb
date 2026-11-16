//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_output_source_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitBuildHashJoinBuildRequiredInputColumns(const ExecutionRegionHashJoinInfo &sink_info,
                                                        idx_t column_count, vector<uint8_t> &required_columns) {
	if (!sink_info.contract.present) {
		return false;
	}
	required_columns.assign(column_count, 0);
	for (auto &key : sink_info.keys) {
		if (!key.supported_reference || key.input_index >= column_count) {
			return false;
		}
		required_columns[key.input_index] = 1;
	}
	auto &contract = sink_info.contract;
	for (auto input_index : contract.payload_column_indices) {
		if (input_index >= column_count) {
			return false;
		}
		required_columns[input_index] = 1;
	}
	return true;
}

static bool SljitMaterializeSelectionOnlyHashJoinProbeOutput(
    ExecutionRegionRuntime &runtime, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
    const SljitHashJoinOutputSource &source, const SelectionVector &match_selection,
    const SelectionVector &build_selection, Vector &row_pointers, idx_t count, DataChunk &join_output) {
	auto &binding = source.binding;
	auto &join_input = source.input;
	if (!binding.ready) {
		return false;
	}
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, hash_join_idx, hash_join_op.Kind(), "materialize_selection_output");
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
		ExecutionMaterializeHashJoinProbe(binding, join_input, row_pointers, match_selection, count, join_output,
		                                  runtime.TraceRuntime() ? &recorder : nullptr, source.output_proof);
	} else if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		ExecutionMaterializePerfectHashJoinProbe(binding, join_input, match_selection, build_selection, count,
		                                         join_output, runtime.TraceRuntime() ? &recorder : nullptr);
	} else {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.Kind(), "materialize_selection_output",
	                              materialize_stage_start);
	return true;
}

static bool SljitTryMaterializeHashJoinRequiredSources(ExecutionRegionRuntime &runtime, idx_t hash_join_idx,
                                                       SljitExecutableRegionOp &hash_join_op,
                                                       const SljitHashJoinOutputSource &source,
                                                       const SelectionVector &match_selection,
                                                       const SelectionVector &build_selection, Vector &row_pointers,
                                                       idx_t count, const vector<uint8_t> &required_columns,
                                                       DataChunk &sink_input) {
	if (count == 0 || sink_input.ColumnCount() != required_columns.size()) {
		return false;
	}
	auto &binding = source.binding;
	auto &join_input = source.input;
	auto &output_proof = source.output_proof;
	auto stage_start = SljitRegionStageStart(runtime);
	auto materialized = ExecuteSljitRegionRecordedOperation(
	    runtime, hash_join_idx, hash_join_op.Kind(), "materialize_probe_required_sources", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return ExecutionMaterializeHashJoinProbeProjectionSources(
		        binding, join_input, row_pointers, match_selection, count, required_columns, sink_input, recorder,
		        optional_ptr<const SelectionVector>(&build_selection), output_proof);
	    });
	if (!materialized) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.Kind(), "materialize_probe_required_sources",
	                              stage_start);
	return true;
}

static bool SljitTryMaterializeHashJoinRequiredProjectionViews(
    ExecutionRegionRuntime &runtime, idx_t projection_idx, SljitExecutableRegionOp &projection_op,
    const SljitHashJoinOutputSource &source, const SelectionVector &match_selection,
    const SelectionVector &build_selection, Vector &row_pointers, idx_t count, const vector<uint8_t> &required_columns,
    DataChunk &projected) {
	if (count == 0 || required_columns.size() != projection_op.Projections().size() ||
	    projected.ColumnCount() != projection_op.Projections().size()) {
		return false;
	}
	auto &binding = source.binding;
	auto &join_input = source.input;
	auto &output_proof = source.output_proof;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
		return false;
	}

	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	const auto rhs_column_count =
	    binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD
	        ? (regular_hash_join ? binding.rhs_output_column_count : binding.perfect_layout.rhs_output_column_count)
	        : 0;
	const auto join_output_column_count = lhs_column_count + rhs_column_count;
	auto stage_start = SljitRegionStageStart(runtime);
	for (idx_t projected_idx = 0; projected_idx < projection_op.Projections().size(); projected_idx++) {
		if (!required_columns[projected_idx]) {
			continue;
		}
		auto &expr = projection_op.Projections()[projected_idx];
		idx_t join_output_source_index;
		if (!SljitTryGetSingleSourceReferenceProjectionIndex(expr, join_output_source_index) ||
		    join_output_source_index >= join_output_column_count) {
			return false;
		}
		auto &target = projected.data[projected_idx];
		if (target.GetType() != expr.plan.return_type) {
			return false;
		}
		if (join_output_source_index < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
			if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != target.GetType()) {
				return false;
			}
			target.Slice(join_input.data[input_col], match_selection, count);
		} else {
			if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
				return false;
			}
			const auto rhs_col_idx = join_output_source_index - lhs_column_count;
			if (regular_hash_join) {
				if (output_proof.ExactSourceFilterMatches()) {
					auto &rhs_aliases = output_proof.ExactRHSOutputProbeInputIndices();
					if (rhs_col_idx >= rhs_aliases.size()) {
						return false;
					}
					const auto input_col = rhs_aliases[rhs_col_idx];
					if (input_col == DConstants::INVALID_INDEX || input_col >= join_input.ColumnCount() ||
					    join_input.data[input_col].GetType() != target.GetType()) {
						return false;
					}
					target.Slice(join_input.data[input_col], match_selection, count);
				} else {
					SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, target);
				}
			} else {
				if (binding.perfect_layout.rhs_dictionary_buffers.size() !=
				        binding.perfect_layout.rhs_output_column_count ||
				    rhs_col_idx >= binding.perfect_layout.rhs_output_column_count ||
				    target.GetType() != binding.perfect_layout.rhs_output_types[rhs_col_idx]) {
					return false;
				}
				target.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_col_idx], build_selection, count);
			}
		}
	}
	projected.SetChildCardinality(count);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "required_projection_views",
	                              stage_start);
	return true;
}

} // namespace duckdb

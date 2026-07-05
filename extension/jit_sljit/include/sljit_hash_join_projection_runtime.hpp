//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_projection_materialization_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitBuildHashJoinBuildRequiredInputColumns(const ExecutionRegionSinkInfo &sink_info, idx_t column_count,
                                                        vector<uint8_t> &required_columns) {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_JOIN_BUILD || !sink_info.hash_join_contract.present) {
		return false;
	}
	required_columns.assign(column_count, 0);
	for (auto &key : sink_info.hash_join_keys) {
		if (!key.supported_reference || key.input_index >= column_count) {
			return false;
		}
		required_columns[key.input_index] = 1;
	}
	auto &contract = sink_info.hash_join_contract;
	for (auto input_index : contract.payload_column_indices) {
		if (input_index >= column_count) {
			return false;
		}
		required_columns[input_index] = 1;
	}
	return true;
}

static bool SljitRequiredColumnsAreStrictSubset(const vector<uint8_t> &required_columns) {
	bool has_required = false;
	bool has_dead = false;
	for (auto required : required_columns) {
		has_required = has_required || required;
		has_dead = has_dead || !required;
	}
	return has_required && has_dead;
}

struct SljitHashJoinSelectionOnlyMaterializationBoundaries {
	SljitHashJoinSelectionOnlyMaterializationBoundaries(const char *regular_first_p = "row_pointer_selection_reference",
	                                                    const char *regular_second_p = "final_output",
	                                                    const char *perfect_first_p = "perfect_selection_reference",
	                                                    const char *perfect_second_p = "final_output")
	    : regular_first(regular_first_p), regular_second(regular_second_p), perfect_first(perfect_first_p),
	      perfect_second(perfect_second_p) {
	}

	const char *regular_first;
	const char *regular_second;
	const char *perfect_first;
	const char *perfect_second;
};

static void SljitRecordHashJoinSelectionOnlyMaterializationBoundary(ExecutionRegionRuntime &runtime,
                                                                    SljitNativeRegionOpKind op_kind, const char *phase,
                                                                    idx_t count) {
	if (phase) {
		RecordSljitRegionMaterializationBoundary(runtime, op_kind, phase, count);
	}
}

static bool SljitMaterializeSelectionOnlyHashJoinProbeOutput(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    SljitExecutableRegionOp &hash_join_op, DataChunk &join_input, const SelectionVector &match_selection,
    const SelectionVector &build_selection, Vector &row_pointers, idx_t count, DataChunk &join_output,
    const SljitHashJoinSelectionOnlyMaterializationBoundaries &boundaries =
        SljitHashJoinSelectionOnlyMaterializationBoundaries()) {
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!binding.ready) {
		return false;
	}
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, hash_join_idx, hash_join_op.kind, "materialize_output_fallback");
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
		SljitRecordHashJoinSelectionOnlyMaterializationBoundary(runtime, hash_join_op.kind, boundaries.regular_first,
		                                                        count);
		SljitRecordHashJoinSelectionOnlyMaterializationBoundary(runtime, hash_join_op.kind, boundaries.regular_second,
		                                                        count);
		ExecutionMaterializeHashJoinProbe(binding, join_input, row_pointers, match_selection, count, join_output,
		                                  runtime.TraceRuntime() ? &recorder : nullptr);
	} else if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		SljitRecordHashJoinSelectionOnlyMaterializationBoundary(runtime, hash_join_op.kind, boundaries.perfect_first,
		                                                        count);
		SljitRecordHashJoinSelectionOnlyMaterializationBoundary(runtime, hash_join_op.kind, boundaries.perfect_second,
		                                                        count);
		ExecutionMaterializePerfectHashJoinProbe(binding, join_input, match_selection, build_selection, count,
		                                         join_output, runtime.TraceRuntime() ? &recorder : nullptr);
	} else {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_output_fallback",
	                              materialize_stage_start);
	return true;
}

static bool SljitTryMaterializeHashJoinRequiredSources(ExecutionRegionRuntime &runtime,
                                                       SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
                                                       SljitExecutableRegionOp &hash_join_op, DataChunk &join_input,
                                                       const SelectionVector &match_selection,
                                                       const SelectionVector &build_selection, Vector &row_pointers,
                                                       idx_t count, const vector<uint8_t> &required_columns,
                                                       DataChunk &sink_input) {
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) ||
	    sink_input.ColumnCount() != required_columns.size()) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	auto stage_start = SljitRegionStageStart(runtime);
	auto materialized = ExecuteSljitRegionRecordedOperation(
	    runtime, hash_join_idx, hash_join_op.kind, "materialize_required_sources", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return ExecutionMaterializeHashJoinProbeProjectionSources(
		        binding, join_input, row_pointers, match_selection, count, required_columns, sink_input, recorder,
		        optional_ptr<const SelectionVector>(&build_selection));
	    });
	if (!materialized) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_required_sources",
	                              stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "required_sink_sources", count);
	return true;
}

static bool SljitTryMaterializeHashJoinRequiredProjectionViews(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    const SelectionVector &build_selection, Vector &row_pointers, idx_t count, const vector<uint8_t> &required_columns,
    DataChunk &projected) {
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) ||
	    required_columns.size() != projection_op.projections.size() ||
	    projected.ColumnCount() != projection_op.projections.size()) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
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
	for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
		if (!required_columns[projected_idx]) {
			continue;
		}
		auto &expr = projection_op.projections[projected_idx];
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
				SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, target);
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
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, "required_projection_views",
	                              stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "required_projection_view", count);
	return true;
}

static bool SljitBuildRequiredProjectionSkip(const vector<uint8_t> &required_columns, idx_t projection_count,
                                             vector<uint8_t> &skip_projection) {
	if (required_columns.size() != projection_count) {
		return false;
	}
	skip_projection.assign(projection_count, 0);
	bool has_required = false;
	for (idx_t projection_idx = 0; projection_idx < projection_count; projection_idx++) {
		const bool required = required_columns[projection_idx] != 0;
		has_required = has_required || required;
		skip_projection[projection_idx] = required ? 0 : 1;
	}
	return has_required;
}

static bool SljitTryMaterializeHashJoinRequiredProjectionOutputs(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, idx_t projection_idx, SljitExecutableRegionOp &projection_op, DataChunk &join_input,
    const SelectionVector &match_selection, Vector &row_pointers, DataChunk &join_source, DataChunk &projected,
    const vector<uint8_t> &required_columns) {
	if (join_source.size() == 0 || projected.ColumnCount() != projection_op.projections.size()) {
		return false;
	}
	vector<uint8_t> skip_projection;
	if (!SljitBuildRequiredProjectionSkip(required_columns, projection_op.projections.size(), skip_projection)) {
		return false;
	}
	return SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
	    runtime, ops, scratch, hash_join_idx, projection_idx, projection_op, join_input, match_selection, row_pointers,
	    join_source, projected, nullptr, nullptr, optional_ptr<const vector<uint8_t>>(&skip_projection));
}

template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_NATIVE_HASH_JOIN_BUILD>
static bool SljitTryExecuteHashJoinProbeDirectHashJoinBuild(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, vector<SljitExecutableRegionOp> &ops,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
    DataChunk &join_input, DataChunk &join_output, SinkResultType &sink_result,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from,
    EXECUTE_NATIVE_HASH_JOIN_BUILD &&execute_native_hash_join_build) {
	if (hash_join_op.hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    hash_join_op.hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return false;
	}

	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;
	if (hash_join_idx + 1 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD &&
	    hash_join_idx + 2 == ops.size()) {
		sink_idx = hash_join_idx + 1;
	} else if (hash_join_idx + 2 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION &&
	           ops[hash_join_idx + 2].kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD &&
	           hash_join_idx + 3 == ops.size()) {
		projection_idx = hash_join_idx + 1;
		sink_idx = hash_join_idx + 2;
	} else {
		return false;
	}

	auto &sink_op = ops[sink_idx];
	auto sink_input_column_count = projection_idx == DConstants::INVALID_INDEX
	                                   ? hash_join_op.output_types.size()
	                                   : ops[projection_idx].output_types.size();
	vector<uint8_t> required_columns;
	if (!SljitBuildHashJoinBuildRequiredInputColumns(sink_op.hash_join_build.plan.sink_info, sink_input_column_count,
	                                                 required_columns)) {
		return false;
	}
	if (projection_idx == DConstants::INVALID_INDEX && !SljitRequiredColumnsAreStrictSubset(required_columns)) {
		return false;
	}

	SljitHashJoinProbeDrainState state;
	auto &match_selection = scratch.FilterSelection(hash_join_idx);
	auto &build_selection = scratch.HashJoinBuildSelection(hash_join_idx);
	auto &row_pointers = scratch.HashJoinRowPointers(hash_join_idx);
	do {
		join_output.Reset();
		string deferred_reason;
		auto bind_result = execute_hash_join_probe(hash_join_idx, hash_join_op, join_input, join_output, state,
		                                           deferred_reason, false,
		                                           SljitHashJoinProbeOutputContract::SELECTED_VIEW);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return SljitDeferBlockedSinkResult(runtime, deferred_reason, sink_result);
		}
		if (join_output.size() == 0) {
			continue;
		}

		DataChunk *sink_input = nullptr;
		if (projection_idx == DConstants::INVALID_INDEX) {
			if (SljitTryMaterializeHashJoinRequiredSources(runtime, scratch, hash_join_idx, hash_join_op, join_input,
			                                               match_selection, build_selection, row_pointers,
			                                               join_output.size(), required_columns, join_output)) {
				sink_input = &join_output;
				RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, "direct_required_hash_build_sources",
				                             join_output.size());
			}
		} else {
			auto &projected = scratch.TemporaryChunk(projection_idx);
			projected.Reset();
			if (SljitTryMaterializeHashJoinRequiredProjectionViews(
			        runtime, scratch, hash_join_idx, projection_idx, ops[projection_idx], join_input, match_selection,
			        build_selection, row_pointers, join_output.size(), required_columns, projected)) {
				sink_input = &projected;
				RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, "direct_projected_hash_build_views",
				                             projected.size());
			} else if (SljitTryMaterializeHashJoinRequiredProjectionOutputs(
			               runtime, ops, scratch, hash_join_idx, projection_idx, ops[projection_idx], join_input,
			               match_selection, row_pointers, join_output, projected, required_columns)) {
				sink_input = &projected;
				RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, "direct_projected_hash_build_outputs",
				                             projected.size());
			}
		}

		if (!sink_input) {
			if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op,
			                                                      join_input, match_selection, build_selection,
			                                                      row_pointers, join_output.size(), join_output)) {
				return false;
			}
			sink_result = execute_native_full_pipeline_from(hash_join_idx + 1, join_output);
		} else {
			auto build_result = execute_native_hash_join_build(sink_idx, sink_op, *sink_input);
			sink_result = native_runtime.RecordSinkResult(*sink_input, build_result);
		}
		if (SljitSinkResultStopsPipeline(sink_result)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
	sink_result = SinkResultType::NEED_MORE_INPUT;
	return true;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_mark_probe_filter_boundary_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_mark_probe_filter_boundary.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

#include <array>

namespace duckdb {

class SljitMarkProbeFilterBoundaryRuntime {
public:
	SljitMarkProbeFilterBoundaryRuntime(ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops_p,
	                                    SljitRegionExecutionScratch &scratch_p,
	                                    SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs_p)
	    : runtime(runtime_p), ops(ops_p), scratch(scratch_p), selected_hash_join_inputs(selected_hash_join_inputs_p) {
	}

	template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NEXT_STEP>
	bool Execute(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe, EXECUTE_NEXT_STEP &&execute_next_step) {
		const auto &primitive = step.mark_probe_filter_boundary;
		auto &join_input =
		    selected_hash_join_inputs.BindProbeInput(primitive.hash_join_idx, input, "SLJIT MARK probe boundary");
		if (join_input.size() == 0) {
			return false;
		}

		const auto hash_join_idx = primitive.hash_join_idx;
		const auto filter_idx = primitive.filter_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &filter_op = ops[filter_idx];
		const auto mark_filter_mode = primitive.filter_mode;
		const auto marker_mode = primitive.marker_mode;
		const bool use_filtered_mark_selection_probe =
		    primitive.strategy != SljitMarkProbeFilterBoundaryStrategy::MATERIALIZE;
		auto probe_output_contract = SljitHashJoinProbeOutputContract::SELECTED_VIEW;
		if (use_filtered_mark_selection_probe) {
			probe_output_contract = mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
			                            ? SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES
			                            : SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES;
		}
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);

		auto handle_output = [&](DataChunk &output) {
			const auto mark_count = output.size();
			if (use_filtered_mark_selection_probe) {
				const bool materialize =
				    primitive.strategy == SljitMarkProbeFilterBoundaryStrategy::FILTERED_MATERIALIZED;
				RecordSljitRegionRuntimePath(
				    runtime, filter_op.Kind(),
				    mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
				        ? (materialize ? "mark_probe_match_materialized" : "mark_probe_match_selection")
				        : (materialize ? "mark_probe_nonmatch_materialized" : "mark_probe_nonmatch_selection"),
				    mark_count);
			}
			auto &boundary_output = BoundaryOutputChunk(step_idx, primitive);
			if (primitive.strategy == SljitMarkProbeFilterBoundaryStrategy::FILTERED_MATERIALIZED) {
				return ExecuteFilteredMaterializedBoundary(hash_join_idx, join_input, boundary_output, marker_mode,
				                                           mark_count, execute_next_step);
			}
			if (use_filtered_mark_selection_probe) {
				return ExecuteFilteredMarkerOmittedBoundary(hash_join_idx, join_input, boundary_output, marker_mode,
				                                            mark_count, execute_next_step);
			}
			return ExecuteMaterializedBoundary(primitive.apply_filter_selection, hash_join_idx, filter_idx, filter_op,
			                                   join_input, boundary_output, marker_mode, mark_filter_mode, mark_count,
			                                   execute_next_step);
		};
		return SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output,
		                                      execute_hash_join_probe, handle_output, false, probe_output_contract);
	}

private:
	template <class EXECUTE_NEXT_STEP>
	bool ExecuteFilteredMarkerOmittedBoundary(idx_t hash_join_idx, DataChunk &join_input, DataChunk &boundary_output,
	                                          SljitMarkProbeFilterBoundaryMarkerMode marker_mode, idx_t mark_count,
	                                          EXECUTE_NEXT_STEP &execute_next_step) {
		SljitBuiltMarkProbeFilterBoundary mark_boundary;
		const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
		    scratch, hash_join_idx, join_input, boundary_output, join_input.size(), marker_mode, mark_boundary);
		if (!built_mark_boundary) {
			throw InternalException("SLJIT filtered MARK probe boundary could not build its marker-omitted output");
		}
		auto selected_output = SljitRuntimeBatchViewFromChunk(mark_boundary.OutputChunk(),
		                                                      &scratch.FilterSelection(hash_join_idx), mark_count);
		return execute_next_step(selected_output);
	}

	template <class EXECUTE_NEXT_STEP>
	bool ExecuteFilteredMaterializedBoundary(idx_t hash_join_idx, DataChunk &join_input, DataChunk &boundary_output,
	                                         SljitMarkProbeFilterBoundaryMarkerMode marker_mode, idx_t mark_count,
	                                         EXECUTE_NEXT_STEP &execute_next_step) {
		SljitMarkProbeFilterBoundary mark_boundary;
		mark_boundary.input = SljitRuntimeBatchViewFromChunk(join_input);
		mark_boundary.output = &boundary_output;
		mark_boundary.mark_flags = &scratch.FilterSelection(hash_join_idx);
		mark_boundary.count = join_input.size();
		SljitMarkProbeFilterBoundaryResult mark_result;
		if (!SljitTryBuildSelectedMarkProbeFilterBoundary(scratch.OperatorBinding(hash_join_idx).hash_join_probe,
		                                                  mark_boundary, scratch.FilterSelection(hash_join_idx),
		                                                  mark_count, marker_mode, mark_result)) {
			throw InternalException("SLJIT filtered MARK probe boundary could not build materialized output");
		}
		return execute_next_step(mark_result.output);
	}

	template <class EXECUTE_NEXT_STEP>
	bool ExecuteMaterializedBoundary(bool apply_filter_selection, idx_t hash_join_idx, idx_t filter_idx,
	                                 SljitExecutableRegionOp &filter_op, DataChunk &join_input,
	                                 DataChunk &boundary_output, SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
	                                 SljitMarkProbeFilterMode mark_filter_mode, idx_t mark_count,
	                                 EXECUTE_NEXT_STEP &execute_next_step) {
		SljitBuiltMarkProbeFilterBoundary mark_boundary;
		const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
		    scratch, hash_join_idx, join_input, boundary_output, mark_count, marker_mode, mark_boundary);
		if (!built_mark_boundary) {
			throw InternalException("SLJIT MARK probe filter boundary primitive could not build its bound output");
		}
		if (!apply_filter_selection) {
			return execute_next_step(mark_boundary.result.output);
		}
		SljitSelectMarkProbeFilterProjectionBoundaryRows(runtime, scratch, hash_join_idx, filter_idx, filter_op,
		                                                 mark_filter_mode, mark_boundary);
		if (mark_boundary.SelectedCount() == 0) {
			return false;
		}
		auto selected_output = SljitRuntimeBatchViewFromChunk(
		    mark_boundary.OutputChunk(), mark_boundary.ExecuteSelection(), mark_boundary.SelectedCount());
		return execute_next_step(selected_output);
	}

	DataChunk &BoundaryOutputChunk(idx_t step_idx, const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
		if (!SljitMarkProbeFilterBoundaryOmitsMarker(primitive.marker_mode)) {
			return scratch.TemporaryChunk(primitive.hash_join_idx);
		}
		auto &lhs_output = lhs_boundary_outputs[step_idx];
		lhs_output.Ensure(runtime.GetAllocator(), *primitive.output_layout);
		lhs_output.Reset();
		return lhs_output.chunk;
	}

private:
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> lhs_boundary_outputs;
};

} // namespace duckdb

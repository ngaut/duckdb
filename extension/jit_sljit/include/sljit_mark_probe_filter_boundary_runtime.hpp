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
	SljitMarkProbeFilterBoundaryRuntime(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                    vector<SljitExecutableRegionOp> &ops_p, SljitRegionExecutionScratch &scratch_p,
	                                    SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), scratch(scratch_p),
	      selected_hash_join_inputs(selected_hash_join_inputs_p) {
	}

	template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NEXT_STEP>
	bool Execute(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe, EXECUTE_NEXT_STEP &&execute_next_step) {
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		const auto &primitive = step.mark_probe_filter_boundary;
		if (input.HasHashJoinSelection()) {
			if (!selected_hash_join_inputs.TryPrepareMarkProbeInput(primitive.hash_join_idx, input, join_input_ptr,
			                                                        deferred_reason)) {
				if (!deferred_reason.empty()) {
					return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
				}
				throw InternalException("SLJIT MARK probe boundary could not prepare selected hash-join input");
			}
		} else {
			join_input_ptr = &SljitBindMaterializedRuntimeBatchInput(input, "SLJIT MARK probe boundary");
		}
		auto &join_input = *join_input_ptr;
		if (join_input.size() == 0) {
			return false;
		}

		const auto hash_join_idx = primitive.hash_join_idx;
		const auto filter_idx = primitive.filter_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &filter_op = ops[filter_idx];
		const auto mark_filter_mode = SljitMarkProbeMarkerFilterMode(hash_join_op, filter_op);
		auto marker_mode = SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS;
		if (primitive.downstream_projection_idx != DConstants::INVALID_INDEX) {
			marker_mode =
			    SljitChooseMarkProbeFilterBoundaryMarkerMode(hash_join_op, ops[primitive.downstream_projection_idx],
			                                                 mark_filter_mode, primitive.allow_marker_omission);
		} else if (primitive.materialize_filter_selection) {
			marker_mode = SljitChooseMaterializedMarkProbeFilterBoundaryMarkerMode(mark_filter_mode);
		}
		const bool use_filtered_mark_selection_probe =
		    (SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode) || primitive.materialize_filter_selection) &&
		    (mark_filter_mode == SljitMarkProbeFilterMode::MATCHES ||
		     mark_filter_mode == SljitMarkProbeFilterMode::NON_MATCHES) &&
		    !hash_join_op.hash_join_probe.plan.perfect_hash_probe &&
		    !hash_join_op.hash_join_probe.plan.residual_predicate;
		auto probe_output_contract = SljitHashJoinProbeOutputContract::SELECTED_VIEW;
		if (use_filtered_mark_selection_probe) {
			probe_output_contract = mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
			                            ? SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES
			                            : SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES;
		}
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);

		auto handle_output = [&](DataChunk &output) {
			const auto mark_count = output.size();
			if (use_filtered_mark_selection_probe && primitive.materialize_filter_selection) {
				return ExecuteFilteredMaterializedBoundary(step_idx, hash_join_idx, hash_join_op, filter_op, join_input,
				                                           marker_mode, mark_filter_mode, mark_count,
				                                           execute_next_step);
			}
			if (use_filtered_mark_selection_probe) {
				return ExecuteFilteredMarkerOmittedBoundary(step_idx, hash_join_idx, hash_join_op, filter_op,
				                                            join_input, marker_mode, mark_filter_mode, mark_count,
				                                            execute_next_step);
			}
			return ExecuteMaterializedBoundary(step_idx, primitive.apply_filter_selection, hash_join_idx, hash_join_op,
			                                   filter_idx, filter_op, join_input, marker_mode, mark_filter_mode,
			                                   mark_count, execute_next_step);
		};
		auto handle_defer = [&](string &deferred_reason) {
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output,
		                                      execute_hash_join_probe, handle_output, handle_defer, false,
		                                      probe_output_contract);
	}

private:
	template <class EXECUTE_NEXT_STEP>
	bool ExecuteFilteredMarkerOmittedBoundary(idx_t step_idx, idx_t hash_join_idx,
	                                          const SljitExecutableRegionOp &hash_join_op,
	                                          SljitExecutableRegionOp &filter_op, DataChunk &join_input,
	                                          SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
	                                          SljitMarkProbeFilterMode mark_filter_mode, idx_t mark_count,
	                                          EXECUTE_NEXT_STEP &execute_next_step) {
		RecordSljitRegionRuntimePath(runtime, filter_op.kind,
		                             mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
		                                 ? "direct_mark_probe_match_selection"
		                                 : "direct_mark_probe_nonmatch_selection",
		                             mark_count);
		if (mark_count == 0) {
			return false;
		}
		auto &boundary_output = BoundaryOutputChunk(step_idx, hash_join_idx, hash_join_op, marker_mode);
		SljitBuiltMarkProbeFilterBoundary mark_boundary;
		const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
		    scratch, hash_join_idx, join_input, boundary_output, join_input.size(), marker_mode, mark_boundary);
		if (!built_mark_boundary) {
			throw InternalException("SLJIT filtered MARK probe boundary could not build its marker-omitted output");
		}
		RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "mark_filter_lhs_selected_view",
		                                         mark_count);
		auto selected_output = SljitRuntimeBatchViewFromChunk(mark_boundary.OutputChunk(),
		                                                      &scratch.FilterSelection(hash_join_idx), mark_count);
		return execute_next_step(selected_output);
	}

	template <class EXECUTE_NEXT_STEP>
	bool ExecuteFilteredMaterializedBoundary(idx_t step_idx, idx_t hash_join_idx,
	                                         const SljitExecutableRegionOp &hash_join_op,
	                                         SljitExecutableRegionOp &filter_op, DataChunk &join_input,
	                                         SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
	                                         SljitMarkProbeFilterMode mark_filter_mode, idx_t mark_count,
	                                         EXECUTE_NEXT_STEP &execute_next_step) {
		RecordSljitRegionRuntimePath(runtime, filter_op.kind,
		                             mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
		                                 ? "direct_mark_probe_match_materialized"
		                                 : "direct_mark_probe_nonmatch_materialized",
		                             mark_count);
		if (mark_count == 0) {
			return false;
		}
		auto &boundary_output = BoundaryOutputChunk(step_idx, hash_join_idx, hash_join_op, marker_mode);
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
		RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "mark_filter_materialized_view",
		                                         mark_count);
		return execute_next_step(mark_result.output);
	}

	template <class EXECUTE_NEXT_STEP>
	bool ExecuteMaterializedBoundary(idx_t step_idx, bool apply_filter_selection, idx_t hash_join_idx,
	                                 const SljitExecutableRegionOp &hash_join_op, idx_t filter_idx,
	                                 SljitExecutableRegionOp &filter_op, DataChunk &join_input,
	                                 SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
	                                 SljitMarkProbeFilterMode mark_filter_mode, idx_t mark_count,
	                                 EXECUTE_NEXT_STEP &execute_next_step) {
		auto &boundary_output = BoundaryOutputChunk(step_idx, hash_join_idx, hash_join_op, marker_mode);
		SljitBuiltMarkProbeFilterBoundary mark_boundary;
		const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
		    scratch, hash_join_idx, join_input, boundary_output, mark_count, marker_mode, mark_boundary);
		if (!built_mark_boundary) {
			throw InternalException("SLJIT MARK probe filter boundary primitive could not build its bound output");
		}
		RecordSljitRegionMaterializationBoundary(
		    runtime, hash_join_op.kind,
		    SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode) ? "mark_filter_lhs_view" : "mark_filter_vector",
		    mark_count);
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

	DataChunk &BoundaryOutputChunk(idx_t step_idx, idx_t hash_join_idx, const SljitExecutableRegionOp &hash_join_op,
	                               SljitMarkProbeFilterBoundaryMarkerMode marker_mode) {
		if (!SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode)) {
			return scratch.TemporaryChunk(hash_join_idx);
		}
		if (hash_join_op.output_types.empty()) {
			throw InternalException("SLJIT MARK probe filtered LHS boundary has no output columns");
		}
		auto &lhs_output_types = lhs_boundary_output_types[step_idx];
		if (!lhs_boundary_output_types_initialized[step_idx]) {
			lhs_output_types.reserve(hash_join_op.output_types.size() - 1);
			for (idx_t col_idx = 0; col_idx + 1 < hash_join_op.output_types.size(); col_idx++) {
				lhs_output_types.push_back(hash_join_op.output_types[col_idx]);
			}
			lhs_boundary_output_types_initialized[step_idx] = true;
		}
		auto &lhs_output = lhs_boundary_outputs[step_idx];
		lhs_output.Ensure(runtime.GetAllocator(), lhs_output_types);
		lhs_output.Reset();
		return lhs_output.chunk;
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	SljitSelectedHashJoinInputRuntime &selected_hash_join_inputs;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> lhs_boundary_outputs;
	std::array<vector<LogicalType>, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> lhs_boundary_output_types;
	std::array<bool, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> lhs_boundary_output_types_initialized {};
};

} // namespace duckdb

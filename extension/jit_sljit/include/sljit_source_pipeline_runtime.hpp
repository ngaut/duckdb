//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_source_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_full_pipeline_terminal_runtime.hpp"
#include "sljit_generated_filter_projection_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_mark_probe_filter_boundary.hpp"
#include "sljit_native_tail_handoff_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_reference_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitSourceBatchBoundaryShouldBatch(idx_t pending_count, idx_t chunk_count) {
	if (pending_count != 0) {
		return true;
	}
	return chunk_count < STANDARD_VECTOR_SIZE / 2;
}

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineNativeOnly(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                  vector<SljitExecutableRegionOp> &ops,
                                                  bool uses_extended_source_fetch_budget,
                                                  EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t processed_chunks = 0;
	idx_t fetched_chunks = 0;
	const auto max_chunks = uses_extended_source_fetch_budget
	                            ? SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())
	                            : runtime.MaxChunks();
	auto execute_native_batch = [&](DataChunk &batch, bool have_more_output) {
		if (SljitAdvanceSinkBatchBlocked(runtime, batch, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		if (batch.size() > 0) {
			auto sink_result = execute_native_full_pipeline(scratch, batch);
			if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
				execute_native_full_pipeline.Finalize(scratch);
				return true;
			}
			processed_chunks++;
		}
		return false;
	};
	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
		return execute_native_batch(source_chunk, have_more_output);
	};
	auto stop_after_flush = [&](ExecutionRegionResult stop_result) {
		execute_native_full_pipeline.Finalize(scratch);
		return SljitStopFullPipeline(result, stop_result);
	};
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks, [&]() { return processed_chunks >= max_chunks; }, execute_source_chunk,
	    [&]() { return stop_after_flush(ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return stop_after_flush(ExecutionRegionResult::INTERRUPTED); },
	    [&]() { return stop_after_flush(ExecutionRegionResult::FINISHED); });
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_HASH_JOIN_PROBE>
class SljitFullPipelinePrimitiveSequenceBatchExecutor {
public:
	SljitFullPipelinePrimitiveSequenceBatchExecutor(
	    ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p, vector<SljitExecutableRegionOp> &ops_p,
	    const SljitFullPipelineRecipe &recipe_p, EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from_p,
	    EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe_p, const vector<idx_t> &source_distinct_counts_p,
	    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), recipe(recipe_p),
	      execute_native_full_pipeline_from(execute_native_full_pipeline_from_p),
	      execute_hash_join_probe(execute_hash_join_probe_p),
	      terminal_runtime(execute_hash_join_probe_p, source_distinct_counts_p, source_min_values_p,
	                       source_max_values_p),
	      scratch(runtime.GetAllocator(), ops) {
	}

	bool Execute() {
		if (!PrimitiveSequenceIsExecutable()) {
			throw InternalException("SLJIT primitive sequence executor received an invalid recipe");
		}
		if (!terminal_runtime.Prepare(runtime, ops, scratch, TerminalStep())) {
			throw InternalException("SLJIT primitive sequence terminal preparation failed");
		}
		idx_t fetched_chunks = 0;
		const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget
		                                    ? SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())
		                                    : runtime.MaxChunks();

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			return ExecuteSourceChunk(source_chunk, have_more_output);
		};

		return SljitRunFullPipelineSourceContractLoop(
		    runtime, fetched_chunks,
		    [&]() {
			    return processed_batches >= max_recipe_batches ||
			           terminal_runtime.BudgetReached(runtime, TerminalStep(), max_recipe_batches) ||
			           fetched_chunks >= max_recipe_batches;
		    },
		    execute_source_chunk,
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::NOT_FINISHED,
			                                           [&]() { return FlushPendingBatch(); });
		    },
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::INTERRUPTED,
			                                           [&]() { return FlushPendingBatch(); });
		    },
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED,
			                                           [&]() { return FlushPendingBatch(); });
		    });
	}

private:
	bool PrimitiveSequenceIsExecutable() const {
		return SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence);
	}

	const SljitFullPipelinePrimitiveStep &TerminalStep() const {
		return SljitFullPipelinePrimitiveSequenceTerminalStep(recipe.primitive_sequence);
	}

	bool IsTerminalStep(idx_t step_idx) const {
		return step_idx + 1 == recipe.primitive_sequence.count;
	}

	bool ExecuteSourceChunk(DataChunk &source_chunk, bool have_more_output) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (SljitFullPipelineSourceFetchOwnsSinkAdvance(recipe.primitive_sequence) &&
		    SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		auto input_view = SljitRuntimeBatchViewFromChunk(source_chunk);
		return ExecuteStep(1, input_view, have_more_output);
	}

	bool ExecuteStep(idx_t step_idx, const SljitRuntimeBatchView &input, bool have_more_output) {
		if (step_idx >= recipe.primitive_sequence.count) {
			throw InternalException("SLJIT primitive sequence reached the end without a consumer");
		}
		auto &step = recipe.primitive_sequence.steps[step_idx];
		if (IsTerminalStep(step_idx)) {
			return ExecuteTerminalStep(step, input, have_more_output);
		}
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY:
			return ExecuteSourceBatchBoundary(step_idx, step, input, have_more_output);
		case SljitFullPipelinePrimitiveKind::GENERATED_FILTER:
			return ExecuteGeneratedFilter(step_idx, step, input, have_more_output);
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE:
			return ExecuteHashJoinProbeMaterialize(step_idx, step, input);
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION:
			return ExecuteHashJoinProbeSelection(step_idx, step, input);
		case SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY:
			return ExecuteMarkProbeFilterBoundary(step_idx, step, input);
		case SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN:
			return ExecuteProjectionChain(step_idx, step, input);
		default:
			throw InternalException("SLJIT primitive sequence contains an unsupported intermediate primitive");
		}
	}

	bool ExecuteTerminalStep(const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	                         bool have_more_output) {
		if (step.kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF) {
			auto stopped = SljitExecuteNativeTailHandoffBatch(runtime, result, scratch, step.Op(0), input,
			                                                  processed_batches, execute_native_full_pipeline_from);
			if (stopped) {
				execute_native_full_pipeline_from.Finalize(scratch);
			}
			return stopped;
		}
		return terminal_runtime.Execute(runtime, result, ops, scratch, step, input, have_more_output,
		                                processed_batches);
	}

	bool ExecuteMaterializedBatch(idx_t step_idx, DataChunk &batch, bool have_more_output = true) {
		if (batch.size() == 0) {
			return false;
		}
		auto batch_view = SljitRuntimeBatchViewFromChunk(batch);
		return ExecuteStep(step_idx, batch_view, have_more_output);
	}

	bool ExecuteGeneratedFilter(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                            const SljitRuntimeBatchView &input, bool have_more_output) {
		SljitRuntimeBatchView filtered_input;
		if (!SljitExecuteGeneratedFilterPrimitive(runtime, scratch, ops, step.generated_filter, input,
		                                          filtered_input)) {
			return false;
		}
		return ExecuteStep(step_idx + 1, filtered_input, have_more_output);
	}

	bool ExecuteHashJoinProbeMaterialize(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                     const SljitRuntimeBatchView &input) {
		auto &join_input = SljitBindNativeTailHandoffInput(input);
		if (join_input.size() == 0) {
			return false;
		}
		auto &primitive = step.hash_join_probe_materialize;
		const auto hash_join_idx = primitive.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		const auto next_step_idx = step_idx + 1;
		auto handle_output = [&](DataChunk &output) {
			return AppendHashJoinMaterializeBatch(step_idx, step, output, next_step_idx);
		};
		auto handle_defer = [&](string &deferred_reason) {
			if (FlushHashJoinMaterializeBatch(step_idx)) {
				return true;
			}
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output,
		                                      execute_hash_join_probe, handle_output, handle_defer,
		                                      primitive.source_key0_int64_to_int32_unchecked);
	}

	bool ExecuteHashJoinProbeSelection(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                   const SljitRuntimeBatchView &input) {
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		if (input.HasHashJoinSelection()) {
			if (!TryPrepareSelectedHashJoinOutputForHashProbeInput(step.Op(0), input, join_input_ptr,
			                                                       deferred_reason)) {
				if (!deferred_reason.empty()) {
					return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
				}
				throw InternalException("SLJIT hash probe could not prepare selected upstream hash-join input");
			}
		} else {
			join_input_ptr = &SljitBindNativeTailHandoffInput(input);
		}
		auto &join_input = *join_input_ptr;
		if (join_input.size() == 0) {
			return false;
		}
		auto &primitive = step.hash_join_probe_selection;
		const auto hash_join_idx = primitive.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		const auto next_step_idx = step_idx + 1;
		const auto output_column_map = primitive.HasOutputColumnMap() ? &primitive.output_column_map : nullptr;
		auto handle_output = [&](DataChunk &output, SljitHashJoinProbeDrainState &state) {
			auto output_view = SljitRuntimeBatchViewFromHashJoinSelection(
			    join_input, scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
			    scratch.HashJoinRowPointers(hash_join_idx), output.size(), hash_join_idx,
			    state.source_key0_int64_to_int32_matches_are_proven, output_column_map,
			    primitive.output_projection_idx);
			return ExecuteStep(next_step_idx, output_view, true);
		};
		auto handle_defer = [&](string &deferred_reason) {
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputsWithState(
		    scratch, hash_join_idx, hash_join_op, join_input, join_output, execute_hash_join_probe, handle_output,
		    handle_defer, primitive.source_key0_int64_to_int32_unchecked,
		    SljitHashJoinProbeOutputContract::SELECTED_VIEW,
		    optional_ptr<const SljitHashJoinProbeInputRemap>(&primitive.input_remap));
	}

	bool ExecuteMarkProbeFilterBoundary(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                    const SljitRuntimeBatchView &input) {
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		const auto &primitive = step.mark_probe_filter_boundary;
		const bool preserve_selected_hash_join = input.HasHashJoinSelection() && primitive.preserve_selected_hash_join;
		if (input.HasHashJoinSelection()) {
			if (!TryPrepareSelectedHashJoinOutputForMarkProbeInput(
			        primitive.hash_join_idx, input, !preserve_selected_hash_join, join_input_ptr, deferred_reason)) {
				if (!deferred_reason.empty()) {
					return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
				}
				throw InternalException("SLJIT MARK probe boundary could not prepare selected hash-join input");
			}
		} else {
			join_input_ptr = &SljitBindNativeTailHandoffInput(input);
		}
		auto &join_input = *join_input_ptr;
		if (join_input.size() == 0) {
			return false;
		}
		const auto hash_join_idx = primitive.hash_join_idx;
		const auto filter_idx = primitive.filter_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &filter_op = ops[filter_idx];
		const auto next_step_idx = step_idx + 1;
		const auto mark_filter_mode = SljitMarkProbeMarkerFilterMode(hash_join_op, filter_op);
		auto marker_mode = SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS;
		if (primitive.downstream_projection_idx != DConstants::INVALID_INDEX) {
			marker_mode =
			    SljitChooseMarkProbeFilterBoundaryMarkerMode(hash_join_op, ops[primitive.downstream_projection_idx],
			                                                 mark_filter_mode, primitive.allow_marker_omission);
		}
		const bool use_filtered_mark_selection_probe = SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode) &&
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
			if (preserve_selected_hash_join) {
				if (use_filtered_mark_selection_probe) {
					RecordSljitRegionRuntimePath(runtime, filter_op.kind,
					                             mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
					                                 ? "direct_mark_probe_match_selection"
					                                 : "direct_mark_probe_nonmatch_selection",
					                             mark_count);
					if (mark_count == 0) {
						return false;
					}
					auto selected_view =
					    BuildSelectedHashJoinMarkOutputView(input, &scratch.FilterSelection(hash_join_idx), mark_count);
					return ExecuteStep(next_step_idx, selected_view, true);
				}
				SljitMarkProbeFilterBoundary mark_boundary;
				mark_boundary.input = SljitRuntimeBatchViewFromChunk(join_input, nullptr, mark_count);
				mark_boundary.mark_flags = &scratch.FilterSelection(hash_join_idx);
				mark_boundary.count = mark_count;
				SljitMarkProbeFilterBoundaryResult mark_result;
				auto selection_stage_start = SljitRegionStageStart(runtime);
				SljitSelectMarkProbeFilterBoundaryRows(scratch.OperatorBinding(hash_join_idx).hash_join_probe,
				                                       mark_boundary, mark_filter_mode,
				                                       scratch.FilterSelection(filter_idx), mark_result);
				RecordSljitRegionStageRuntimePath(runtime, filter_idx, filter_op.kind,
				                                  SljitMarkProbeFilterSelectionPath(mark_filter_mode),
				                                  selection_stage_start);
				if (mark_result.selected_count == 0) {
					return false;
				}
				auto selected_view =
				    BuildSelectedHashJoinMarkOutputView(input, mark_result.selection, mark_result.selected_count);
				return ExecuteStep(next_step_idx, selected_view, true);
			}
			if (use_filtered_mark_selection_probe) {
				RecordSljitRegionRuntimePath(runtime, filter_op.kind,
				                             mark_filter_mode == SljitMarkProbeFilterMode::MATCHES
				                                 ? "direct_mark_probe_match_selection"
				                                 : "direct_mark_probe_nonmatch_selection",
				                             mark_count);
				if (mark_count == 0) {
					return false;
				}
				auto &boundary_output =
				    MarkProbeFilterBoundaryOutputChunk(step_idx, hash_join_idx, hash_join_op, marker_mode);
				SljitBuiltMarkProbeFilterBoundary mark_boundary;
				const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
				    scratch, hash_join_idx, join_input, boundary_output, join_input.size(), marker_mode, mark_boundary);
				if (!built_mark_boundary) {
					throw InternalException(
					    "SLJIT filtered MARK probe boundary could not build its marker-omitted output");
				}
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "mark_filter_lhs_selected_view",
				                                         mark_count);
				auto selected_output = SljitRuntimeBatchViewFromChunk(
				    mark_boundary.OutputChunk(), &scratch.FilterSelection(hash_join_idx), mark_count);
				return ExecuteStep(next_step_idx, selected_output, true);
			}
			auto &boundary_output =
			    MarkProbeFilterBoundaryOutputChunk(step_idx, hash_join_idx, hash_join_op, marker_mode);
			SljitBuiltMarkProbeFilterBoundary mark_boundary;
			const bool built_mark_boundary = SljitTryBuildMarkProbeFilterBoundaryFromInput(
			    scratch, hash_join_idx, join_input, boundary_output, mark_count, marker_mode, mark_boundary);
			if (built_mark_boundary) {
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind,
				                                         SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode)
				                                             ? "mark_filter_lhs_view"
				                                             : "mark_filter_vector",
				                                         mark_count);
				if (!primitive.apply_filter_selection) {
					return ExecuteStep(next_step_idx, mark_boundary.result.output, true);
				}
				SljitSelectMarkProbeFilterProjectionBoundaryRows(runtime, scratch, hash_join_idx, filter_idx, filter_op,
				                                                 mark_filter_mode, mark_boundary);
				if (mark_boundary.SelectedCount() == 0) {
					return false;
				}
				auto selected_output = SljitRuntimeBatchViewFromChunk(
				    mark_boundary.OutputChunk(), mark_boundary.ExecuteSelection(), mark_boundary.SelectedCount());
				return ExecuteStep(next_step_idx, selected_output, true);
			}
			throw InternalException("SLJIT MARK probe filter boundary primitive could not build its bound output");
		};
		auto handle_defer = [&](string &deferred_reason) {
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output,
		                                      execute_hash_join_probe, handle_output, handle_defer, false,
		                                      probe_output_contract);
	}

	DataChunk &MarkProbeFilterBoundaryOutputChunk(idx_t step_idx, idx_t hash_join_idx,
	                                              const SljitExecutableRegionOp &hash_join_op,
	                                              SljitMarkProbeFilterBoundaryMarkerMode marker_mode) {
		if (!SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode)) {
			return scratch.TemporaryChunk(hash_join_idx);
		}
		if (hash_join_op.output_types.empty()) {
			throw InternalException("SLJIT MARK probe filtered LHS boundary has no output columns");
		}
		auto &lhs_output_types = mark_probe_lhs_boundary_output_types[step_idx];
		if (!mark_probe_lhs_boundary_output_types_initialized[step_idx]) {
			lhs_output_types.reserve(hash_join_op.output_types.size() - 1);
			for (idx_t col_idx = 0; col_idx + 1 < hash_join_op.output_types.size(); col_idx++) {
				lhs_output_types.push_back(hash_join_op.output_types[col_idx]);
			}
			mark_probe_lhs_boundary_output_types_initialized[step_idx] = true;
		}
		auto &lhs_output = mark_probe_lhs_boundary_outputs[step_idx];
		lhs_output.Ensure(runtime.GetAllocator(), lhs_output_types);
		lhs_output.Reset();
		return lhs_output.chunk;
	}

	bool TryPrepareSelectedHashJoinOutputForMarkProbeInput(idx_t mark_hash_join_idx,
	                                                       const SljitRuntimeBatchView &selected_input,
	                                                       bool include_lhs_output_columns, DataChunk *&join_input,
	                                                       string &deferred_reason) {
		join_input = nullptr;
		if (!selected_input.HasHashJoinSelection()) {
			return false;
		}
		const auto source_hash_join_idx = selected_input.hash_join_idx;
		if (source_hash_join_idx >= ops.size() || mark_hash_join_idx >= ops.size()) {
			return false;
		}
		if (!scratch.HasOperatorBinding(source_hash_join_idx)) {
			throw InternalException("SLJIT selected MARK input has no source hash-join binding");
		}
		auto &source_binding = scratch.OperatorBinding(source_hash_join_idx).hash_join_probe;
		if (!source_binding.ready || source_binding.output_types.empty()) {
			throw InternalException("SLJIT selected MARK input has an incomplete source hash-join binding");
		}

		selected_hash_join_mark_input.Ensure(runtime.GetAllocator(), source_binding.output_types);
		auto &compact_input = selected_hash_join_mark_input.chunk;
		compact_input.Reset();

		auto &mark_hash_join_op = ops[mark_hash_join_idx];
		ExecutionOperatorBinding *mark_binding_ptr = nullptr;
		auto bind_result = SljitBindRecordedNativeOperator(
		    runtime, runtime.ExecutionOperators(), scratch, mark_hash_join_idx, mark_hash_join_op, compact_input,
		    mark_hash_join_op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred",
		    "SLJIT selected MARK probe input", mark_binding_ptr, deferred_reason);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return false;
		}
		auto &mark_binding = mark_binding_ptr->hash_join_probe;
		if (!mark_binding.ready || mark_binding.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
			throw InternalException("SLJIT selected MARK probe input received an invalid MARK binding");
		}

		vector<uint8_t> referenced_columns(source_binding.output_types.size(), 0);
		auto mark_referenced_column = [&](idx_t column_idx) {
			if (column_idx >= referenced_columns.size()) {
				throw InternalException("SLJIT selected MARK probe input column index out of range");
			}
			referenced_columns[column_idx] = 1;
		};
		for (auto column_idx : mark_binding.probe_key_input_indices) {
			mark_referenced_column(column_idx);
		}
		if (include_lhs_output_columns) {
			for (auto column_idx : mark_binding.lhs_output_column_indices) {
				mark_referenced_column(column_idx);
			}
		}
		for (auto &residual_source : mark_binding.residual_sources) {
			if (residual_source.kind == ExecutionHashJoinResidualSourceKind::PROBE) {
				mark_referenced_column(residual_source.input_index);
			}
		}

		auto materialize_stage_start = SljitRegionStageStart(runtime);
		auto materialized = ExecuteSljitRegionRecordedOperation(
		    runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, "materialize_selected_mark_input",
		    materialize_stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    (void)recorder;
			    return SljitTryMaterializeSelectedHashJoinOutputColumns(source_binding, selected_input,
			                                                            referenced_columns, compact_input);
		    });
		if (!materialized) {
			RecordSljitRegionStageRuntimePath(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
			                                  "materialize_selected_mark_input_miss", materialize_stage_start);
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
		                              "materialize_selected_mark_input", materialize_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, ops[source_hash_join_idx].kind, "selected_mark_probe_input",
		                                         selected_input.count);
		join_input = &compact_input;
		return true;
	}

	bool TryPrepareSelectedHashJoinOutputForHashProbeInput(idx_t target_hash_join_idx,
	                                                       const SljitRuntimeBatchView &selected_input,
	                                                       DataChunk *&join_input, string &deferred_reason) {
		join_input = nullptr;
		if (!selected_input.HasHashJoinSelection()) {
			return false;
		}
		const auto source_hash_join_idx = selected_input.hash_join_idx;
		if (source_hash_join_idx >= ops.size() || target_hash_join_idx >= ops.size()) {
			return false;
		}
		if (!scratch.HasOperatorBinding(source_hash_join_idx)) {
			throw InternalException("SLJIT selected hash probe input has no source hash-join binding");
		}
		auto &source_binding = scratch.OperatorBinding(source_hash_join_idx).hash_join_probe;
		if (!source_binding.ready || source_binding.output_types.empty()) {
			throw InternalException("SLJIT selected hash probe input has an incomplete source hash-join binding");
		}

		selected_hash_join_probe_input.Ensure(runtime.GetAllocator(), source_binding.output_types);
		auto &compact_input = selected_hash_join_probe_input.chunk;
		compact_input.Reset();

		auto &target_hash_join_op = ops[target_hash_join_idx];
		ExecutionOperatorBinding *target_binding_ptr = nullptr;
		auto bind_result = SljitBindRecordedNativeOperator(
		    runtime, runtime.ExecutionOperators(), scratch, target_hash_join_idx, target_hash_join_op, compact_input,
		    target_hash_join_op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred",
		    "SLJIT selected hash probe input", target_binding_ptr, deferred_reason);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return false;
		}
		auto &target_binding = target_binding_ptr->hash_join_probe;
		if (!target_binding.ready) {
			throw InternalException("SLJIT selected hash probe input received an invalid target binding");
		}

		vector<uint8_t> referenced_columns(source_binding.output_types.size(), 0);
		auto mark_referenced_column = [&](idx_t column_idx) {
			if (column_idx >= referenced_columns.size()) {
				throw InternalException("SLJIT selected hash probe input column index out of range");
			}
			referenced_columns[column_idx] = 1;
		};
		for (auto column_idx : target_binding.probe_key_input_indices) {
			mark_referenced_column(column_idx);
		}
		for (auto column_idx : target_binding.lhs_output_column_indices) {
			mark_referenced_column(column_idx);
		}
		for (auto &residual_source : target_binding.residual_sources) {
			if (residual_source.kind == ExecutionHashJoinResidualSourceKind::PROBE) {
				mark_referenced_column(residual_source.input_index);
			}
		}

		auto materialize_stage_start = SljitRegionStageStart(runtime);
		auto materialized = ExecuteSljitRegionRecordedOperation(
		    runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, "materialize_selected_hash_probe_input",
		    materialize_stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    (void)recorder;
			    return SljitTryMaterializeSelectedHashJoinOutputColumns(source_binding, selected_input,
			                                                            referenced_columns, compact_input);
		    });
		if (!materialized) {
			RecordSljitRegionStageRuntimePath(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
			                                  "materialize_selected_hash_probe_input_miss", materialize_stage_start);
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
		                              "materialize_selected_hash_probe_input", materialize_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, ops[source_hash_join_idx].kind, "selected_hash_probe_input",
		                                         selected_input.count);
		join_input = &compact_input;
		return true;
	}

	SljitRuntimeBatchView BuildSelectedHashJoinMarkOutputView(const SljitRuntimeBatchView &input,
	                                                          const SelectionVector *mark_selection,
	                                                          idx_t selected_count) {
		if (!mark_selection) {
			return SljitRuntimeBatchViewFromHashJoinSelection(
			    input.Chunk(), *input.selection, *input.hash_join_build_selection, *input.hash_join_row_pointers,
			    selected_count, input.hash_join_idx, input.source_key0_int64_to_int32_matches_are_proven,
			    input.hash_join_output_column_map, input.hash_join_output_projection_idx);
		}

		if (!selected_mark_source_selection) {
			selected_mark_source_selection = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!selected_mark_build_selection) {
			selected_mark_build_selection = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!selected_mark_row_pointers) {
			selected_mark_row_pointers = make_uniq<Vector>(LogicalType::POINTER);
		}
		auto &row_pointers = *selected_mark_row_pointers;
		row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
		FlatVector::ValidityMutable(row_pointers).SetAllValid(selected_count);
		FlatVector::SetSize(row_pointers, selected_count);
		auto target_row_pointers = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
		auto source_row_pointers = FlatVector::GetData<data_ptr_t>(*input.hash_join_row_pointers);
		auto mark_sel = mark_selection->data();
		auto source_sel = input.selection->data();
		auto build_sel = input.hash_join_build_selection->data();
		auto target_source_sel = selected_mark_source_selection->data();
		auto target_build_sel = selected_mark_build_selection->data();
		for (idx_t row_idx = 0; row_idx < selected_count; row_idx++) {
			const auto selected_idx = mark_sel[row_idx];
			target_source_sel[row_idx] = source_sel[selected_idx];
			target_build_sel[row_idx] = build_sel[selected_idx];
			target_row_pointers[row_idx] = source_row_pointers[selected_idx];
		}
		return SljitRuntimeBatchViewFromHashJoinSelection(
		    input.Chunk(), *selected_mark_source_selection, *selected_mark_build_selection, row_pointers,
		    selected_count, input.hash_join_idx, input.source_key0_int64_to_int32_matches_are_proven,
		    input.hash_join_output_column_map, input.hash_join_output_projection_idx);
	}

	bool ExecuteProjectionChain(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                            const SljitRuntimeBatchView &input) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		auto &projection_chain_batch = projection_chain_batches[step_idx];
		auto &selected_hash_join_input = selected_hash_join_projection_inputs[step_idx];
		return SljitExecuteProjectionChainPrimitive(runtime, scratch, ops, step.projection_chain, input,
		                                            projection_chain_batch, selected_hash_join_input,
		                                            execute_output_batch);
	}

	bool ExecuteSourceBatchBoundary(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                const SljitRuntimeBatchView &input, bool have_more_output) {
		return ExecuteBatchBoundary(step_idx, step, input, have_more_output, "source_batch_boundary",
		                            "source_batch_boundary_append", "source_batch", true);
	}

	bool ExecuteBatchBoundary(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                          const SljitRuntimeBatchView &input, bool have_more_output, const char *runtime_path,
	                          const char *append_stage, const char *boundary_counter, bool adaptive_source_batch) {
		auto &chunk = SljitBindNativeTailHandoffInput(input);
		if (chunk.size() == 0) {
			return false;
		}
		const auto next_step_idx = step_idx + 1;

		auto execute_output_batch = [&](DataChunk &batch, bool batch_has_more_output) -> bool {
			if (SljitAdvanceSinkBatchBlocked(runtime, batch, batch_has_more_output)) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
			}
			return ExecuteMaterializedBatch(next_step_idx, batch, batch_has_more_output);
		};
		auto flush_batch = [&](bool batch_has_more_output) -> bool {
			auto &boundary_batch = source_boundary_batches[step_idx];
			if (boundary_batch.Empty()) {
				return false;
			}
			return SljitFlushDataChunkBatch(boundary_batch.chunk, [&](DataChunk &batch) {
				return execute_output_batch(batch, batch_has_more_output);
			});
		};

		auto &boundary_batch = source_boundary_batches[step_idx];
		boundary_batch.Ensure(runtime.GetAllocator(), chunk.GetTypes());
		auto &batch = boundary_batch.chunk;
		const auto op_idx = step.Op(0);
		auto &trace_op = ops[op_idx];
		RecordSljitRegionRuntimePath(runtime, trace_op.kind, runtime_path, chunk.size());
		if (batch.size() + chunk.size() > STANDARD_VECTOR_SIZE && flush_batch(true)) {
			return true;
		}
		if (adaptive_source_batch && !SljitSourceBatchBoundaryShouldBatch(batch.size(), chunk.size())) {
			return execute_output_batch(chunk, have_more_output);
		}
		if (chunk.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
			return execute_output_batch(chunk, have_more_output);
		}
		auto stage_start = SljitRegionStageStart(runtime);
		if (!SljitTryFastAppendFixedFlatAllValid(batch, chunk)) {
			batch.Append(chunk);
		}
		RecordSljitRegionStageRuntime(runtime, op_idx, trace_op.kind, append_stage, stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, trace_op.kind, boundary_counter, chunk.size());
		if (batch.size() == STANDARD_VECTOR_SIZE && flush_batch(have_more_output)) {
			return true;
		}
		return false;
	}

	bool FlushPendingBatch() {
		for (idx_t step_idx = 1; step_idx + 1 < recipe.primitive_sequence.count; step_idx++) {
			auto &step = recipe.primitive_sequence.steps[step_idx];
			switch (step.kind) {
			case SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY:
				if (FlushSourceBoundaryBatch(step_idx)) {
					return true;
				}
				break;
			case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE:
				if (FlushHashJoinMaterializeBatch(step_idx)) {
					return true;
				}
				break;
			case SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN:
				if (FlushProjectionChainBatch(step_idx)) {
					return true;
				}
				break;
			default:
				break;
			}
		}
		auto &terminal_step = TerminalStep();
		if (terminal_step.kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF) {
			execute_native_full_pipeline_from.Finalize(scratch);
			return false;
		}
		return terminal_runtime.Flush(runtime, result, ops, scratch, terminal_step, processed_batches);
	}

	bool FlushProjectionChainBatch(idx_t step_idx) {
		auto &projection_chain_batch = projection_chain_batches[step_idx];
		if (projection_chain_batch.Empty()) {
			return false;
		}
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return SljitFlushDataChunkBatch(projection_chain_batch.chunk, execute_output_batch);
	}

	bool FlushSourceBoundaryBatch(idx_t step_idx) {
		auto &boundary_batch = source_boundary_batches[step_idx];
		if (boundary_batch.Empty()) {
			return false;
		}
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			if (SljitAdvanceSinkBatchBlocked(runtime, batch, false)) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
			}
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return SljitFlushDataChunkBatch(boundary_batch.chunk, execute_output_batch);
	}

	bool AppendHashJoinMaterializeBatch(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, DataChunk &output,
	                                    idx_t next_step_idx) {
		auto &hash_join_materialize_batch = hash_join_materialize_batches[step_idx];
		hash_join_materialize_batch.Ensure(runtime.GetAllocator(), output.GetTypes());
		auto &batch = hash_join_materialize_batch.chunk;
		auto execute_output_batch = [&](DataChunk &materialized) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, materialized);
		};
		auto flush_batch = [&]() -> bool {
			return FlushHashJoinMaterializeBatch(step_idx);
		};
		const auto op_idx = step.Op(0);
		return SljitAppendChunkToInitializedBatch(
		    runtime, batch, output, op_idx, optional_ptr<const SljitExecutableRegionOp>(&ops[op_idx]),
		    "hash_join_materialize_batch_append", "hash_join_materialize_batch", flush_batch, execute_output_batch);
	}

	bool FlushHashJoinMaterializeBatch(idx_t step_idx) {
		auto &hash_join_materialize_batch = hash_join_materialize_batches[step_idx];
		if (hash_join_materialize_batch.Empty()) {
			return false;
		}
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return SljitFlushDataChunkBatch(hash_join_materialize_batch.chunk, execute_output_batch);
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipe &recipe;
	EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from;
	EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe;
	SljitFullPipelineTerminalRuntime<EXECUTE_HASH_JOIN_PROBE> terminal_runtime;
	SljitRegionExecutionScratch scratch;
	SljitDataChunkBatch selected_hash_join_mark_input;
	SljitDataChunkBatch selected_hash_join_probe_input;
	unique_ptr<SelectionVector> selected_mark_source_selection;
	unique_ptr<SelectionVector> selected_mark_build_selection;
	unique_ptr<Vector> selected_mark_row_pointers;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> hash_join_materialize_batches;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> mark_probe_lhs_boundary_outputs;
	std::array<vector<LogicalType>, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> mark_probe_lhs_boundary_output_types;
	std::array<bool, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> mark_probe_lhs_boundary_output_types_initialized {};
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> selected_hash_join_projection_inputs;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> projection_chain_batches;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> source_boundary_batches;
	idx_t processed_batches = 0;
};

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteFullPipelinePrimitiveSequenceBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    const SljitFullPipelineRecipe &recipe, EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, const vector<idx_t> &source_distinct_counts,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values) {
	auto executor =
	    SljitFullPipelinePrimitiveSequenceBatchExecutor<EXECUTE_NATIVE_FULL_PIPELINE_FROM, EXECUTE_HASH_JOIN_PROBE>(
	        runtime, result, ops, recipe, execute_native_full_pipeline_from, execute_hash_join_probe,
	        source_distinct_counts, source_min_values, source_max_values);
	return executor.Execute();
}

} // namespace duckdb

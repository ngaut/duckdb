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
#include "sljit_generated_filter_primitive_runtime.hpp"
#include "sljit_hash_join_probe_materialize_primitive_runtime.hpp"
#include "sljit_hash_join_probe_selection_primitive_runtime.hpp"
#include "sljit_mark_probe_filter_boundary_runtime.hpp"
#include "sljit_native_only_pipeline_runtime.hpp"
#include "sljit_projection_chain_primitive_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"
#include "sljit_source_fetch_primitive_runtime.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_HASH_JOIN_PROBE>
class SljitFullPipelinePrimitiveSequenceBatchExecutor {
public:
	SljitFullPipelinePrimitiveSequenceBatchExecutor(
	    ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p, vector<SljitExecutableRegionOp> &ops_p,
	    const SljitFullPipelineRecipe &recipe_p, EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from_p,
	    EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe_p, const vector<idx_t> &source_distinct_counts_p,
	    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	    SljitRegionExecutionScratch &scratch_p, SljitFullPipelineTerminalRuntimeState &terminal_state_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), recipe(recipe_p),
	      execute_hash_join_probe(execute_hash_join_probe_p),
	      terminal_runtime(execute_native_full_pipeline_from_p, source_distinct_counts_p, source_min_values_p,
	                       source_max_values_p, terminal_state_p),
	      scratch(scratch_p), selected_hash_join_inputs(runtime, ops, scratch),
	      source_fetch(runtime, result, recipe.primitive_sequence), generated_filter(runtime, ops, scratch),
	      hash_join_materialize(runtime, result, ops, scratch),
	      hash_join_selection(runtime, result, ops, scratch, selected_hash_join_inputs),
	      mark_probe_filter_boundary(runtime, result, ops, scratch, selected_hash_join_inputs),
	      projection_chain(runtime, ops, scratch) {
	}

	bool Execute() {
		if (!PrimitiveSequenceIsExecutable()) {
			throw InternalException("SLJIT primitive sequence executor received an invalid recipe");
		}
		if (SljitFullPipelineIsSelectedHashJoinSinkSequence(recipe.primitive_sequence)) {
			return ExecuteLoweredSelectedHashJoinSinkRecipe();
		}
		if (!terminal_runtime.Prepare(runtime, ops, scratch, TerminalStep())) {
			throw InternalException("SLJIT primitive sequence terminal preparation failed");
		}
		idx_t fetched_chunks = 0;
		const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget
		                                    ? SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())
		                                    : runtime.MaxChunks();

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			return ExecuteSourceContractChunk(source_chunk, have_more_output);
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
			                                           [&]() { return FlushRuntimeState(true, false); });
		    },
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::INTERRUPTED,
			                                           [&]() { return FlushRuntimeState(true, true); });
		    },
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED,
			                                           [&]() { return FlushRuntimeState(false, true); });
		    });
	}

private:
	bool ExecuteLoweredSelectedHashJoinSinkRecipe() {
		auto &hash_join_step = recipe.primitive_sequence.Step(1);
		auto &terminal_step = recipe.primitive_sequence.Step(2);
		if (!terminal_runtime.Prepare(runtime, ops, scratch, terminal_step)) {
			throw InternalException("SLJIT lowered selected hash-join terminal preparation failed");
		}
		idx_t fetched_chunks = 0;
		const auto max_chunks = runtime.MaxChunks();
		auto execute_source_batch = [&](const SljitRuntimeBatchView &input, bool have_more_output) {
			auto execute_terminal = [&](const SljitRuntimeBatchView &selected_output) {
				return terminal_runtime.Execute(runtime, result, ops, scratch, terminal_step, selected_output,
				                                have_more_output, processed_batches);
			};
			return hash_join_selection.Execute(hash_join_step, input, execute_hash_join_probe, execute_terminal);
		};
		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
			return source_fetch.Execute(source_chunk, have_more_output, execute_source_batch);
		};
		auto stop_after_flush = [&](ExecutionRegionResult stop_result, bool have_more_output, bool flush_terminal) {
			if (source_fetch.Flush(have_more_output, execute_source_batch)) {
				return true;
			}
			if (flush_terminal &&
			    terminal_runtime.Flush(runtime, result, ops, scratch, terminal_step, processed_batches)) {
				return true;
			}
			return SljitStopFullPipeline(result, stop_result);
		};
		return SljitRunFullPipelineSourceContractLoop(
		    runtime, fetched_chunks, [&]() { return fetched_chunks >= max_chunks; }, execute_source_chunk,
		    [&]() { return stop_after_flush(ExecutionRegionResult::NOT_FINISHED, true, false); },
		    [&]() { return stop_after_flush(ExecutionRegionResult::INTERRUPTED, true, true); },
		    [&]() { return stop_after_flush(ExecutionRegionResult::FINISHED, false, true); });
	}

	bool PrimitiveSequenceIsExecutable() const {
		return SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence);
	}

	const SljitFullPipelinePrimitiveStep &TerminalStep() const {
		return SljitFullPipelinePrimitiveSequenceTerminalStep(recipe.primitive_sequence);
	}

	bool IsTerminalStep(idx_t step_idx) const {
		return step_idx + 1 == recipe.primitive_sequence.Count();
	}

	bool ExecuteSourceContractChunk(DataChunk &source_chunk, bool have_more_output) {
		auto execute_next_step = [&](const SljitRuntimeBatchView &input, bool source_have_more_output) {
			return ExecuteStep(1, input, source_have_more_output);
		};
		return source_fetch.Execute(source_chunk, have_more_output, execute_next_step);
	}

	bool ExecuteStep(idx_t step_idx, const SljitRuntimeBatchView &input, bool have_more_output) {
		if (step_idx >= recipe.primitive_sequence.Count()) {
			throw InternalException("SLJIT primitive sequence reached the end without a consumer");
		}
		auto &step = recipe.primitive_sequence.Step(step_idx);
		if (IsTerminalStep(step_idx)) {
			return ExecuteTerminalStep(step, input, have_more_output);
		}
		switch (step.kind) {
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
		auto execute_output_view = [&](const SljitRuntimeBatchView &output) {
			return ExecuteStep(step_idx + 1, output, have_more_output);
		};
		return generated_filter.Execute(step, input, execute_output_view);
	}

	bool ExecuteHashJoinProbeMaterialize(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                     const SljitRuntimeBatchView &input) {
		const auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		const auto direct_handoff =
		    HashJoinMaterializeCanDirectHandoffTo(recipe.primitive_sequence.Step(next_step_idx));
		return hash_join_materialize.Execute(step_idx, step, input, direct_handoff, execute_hash_join_probe,
		                                     execute_output_batch);
	}

	bool HashJoinMaterializeCanDirectHandoffTo(const SljitFullPipelinePrimitiveStep &step) const {
		return step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE ||
		       step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION;
	}

	bool ExecuteHashJoinProbeSelection(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                   const SljitRuntimeBatchView &input) {
		const auto next_step_idx = step_idx + 1;
		auto execute_next_step = [&](const SljitRuntimeBatchView &output) {
			return ExecuteStep(next_step_idx, output, true);
		};
		return hash_join_selection.Execute(step, input, execute_hash_join_probe, execute_next_step);
	}

	bool ExecuteMarkProbeFilterBoundary(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                    const SljitRuntimeBatchView &input) {
		auto execute_next_step = [&](const SljitRuntimeBatchView &output) {
			return ExecuteStep(step_idx + 1, output, true);
		};
		return mark_probe_filter_boundary.Execute(step_idx, step, input, execute_hash_join_probe, execute_next_step);
	}

	bool ExecuteProjectionChain(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                            const SljitRuntimeBatchView &input) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		const auto direct_handoff = IsTerminalStep(next_step_idx) &&
		                            ProjectionChainCanDirectHandoffTo(recipe.primitive_sequence.Step(next_step_idx));
		return projection_chain.Execute(step_idx, step, input, direct_handoff, execute_output_batch);
	}

	bool ProjectionChainCanDirectHandoffTo(const SljitFullPipelinePrimitiveStep &step) const {
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK:
			return true;
		default:
			return false;
		}
	}

	bool FlushRuntimeState(bool source_contract_have_more_output, bool flush_terminal) {
		auto execute_next_step = [&](const SljitRuntimeBatchView &input, bool source_have_more_output) {
			return ExecuteStep(1, input, source_have_more_output);
		};
		if (source_fetch.Flush(source_contract_have_more_output, execute_next_step)) {
			return true;
		}
		return FlushPendingBatch(flush_terminal);
	}

	bool FlushPendingBatch(bool flush_terminal) {
		for (idx_t step_idx = 1; step_idx + 1 < recipe.primitive_sequence.Count(); step_idx++) {
			auto &step = recipe.primitive_sequence.Step(step_idx);
			if (FlushMaterializingStep(step_idx, step)) {
				return true;
			}
		}
		if (!flush_terminal) {
			return false;
		}
		auto &terminal_step = TerminalStep();
		return terminal_runtime.Flush(runtime, result, ops, scratch, terminal_step, processed_batches);
	}

	bool FlushMaterializingStep(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE:
			return hash_join_materialize.Flush(step_idx, execute_output_batch);
		case SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN:
			return projection_chain.Flush(step_idx, execute_output_batch);
		default:
			return false;
		}
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipe &recipe;
	EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe;
	SljitFullPipelineTerminalRuntime<EXECUTE_NATIVE_FULL_PIPELINE_FROM> terminal_runtime;
	SljitRegionExecutionScratch &scratch;
	SljitSelectedHashJoinInputRuntime selected_hash_join_inputs;
	SljitSourceFetchPrimitiveRuntime source_fetch;
	SljitGeneratedFilterPrimitiveRuntime generated_filter;
	SljitHashJoinProbeMaterializePrimitiveRuntime hash_join_materialize;
	SljitHashJoinProbeSelectionPrimitiveRuntime hash_join_selection;
	SljitMarkProbeFilterBoundaryRuntime mark_probe_filter_boundary;
	SljitProjectionChainPrimitiveRuntime projection_chain;
	idx_t processed_batches = 0;
};

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteFullPipelinePrimitiveSequenceBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    const SljitFullPipelineRecipe &recipe, EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, const vector<idx_t> &source_distinct_counts,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state) {
	auto executor =
	    SljitFullPipelinePrimitiveSequenceBatchExecutor<EXECUTE_NATIVE_FULL_PIPELINE_FROM, EXECUTE_HASH_JOIN_PROBE>(
	        runtime, result, ops, recipe, execute_native_full_pipeline_from, execute_hash_join_probe,
	        source_distinct_counts, source_min_values, source_max_values, scratch, terminal_state);
	return executor.Execute();
}

} // namespace duckdb

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
#include "sljit_hash_join_probe_materialize_primitive_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_mark_probe_filter_boundary_runtime.hpp"
#include "sljit_native_tail_handoff_runtime.hpp"
#include "sljit_projection_chain_primitive_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"
#include "sljit_source_batch_boundary_runtime.hpp"

namespace duckdb {

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
	      scratch(runtime.GetAllocator(), ops), selected_hash_join_inputs(runtime, ops, scratch),
	      hash_join_materialize(runtime, result, ops, scratch),
	      mark_probe_filter_boundary(runtime, result, ops, scratch, selected_hash_join_inputs),
	      source_batch_boundary(runtime, result, ops), projection_chain(runtime, ops, scratch) {
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
		const auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		return hash_join_materialize.Execute(step_idx, step, input, execute_hash_join_probe, execute_output_batch);
	}

	bool ExecuteHashJoinProbeSelection(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                   const SljitRuntimeBatchView &input) {
		string deferred_reason;
		DataChunk *join_input_ptr = nullptr;
		if (input.HasHashJoinSelection()) {
			if (!selected_hash_join_inputs.TryPrepareHashProbeInput(step.Op(0), input, join_input_ptr,
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
		return projection_chain.Execute(step_idx, step, input, execute_output_batch);
	}

	bool ExecuteSourceBatchBoundary(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                const SljitRuntimeBatchView &input, bool have_more_output) {
		const auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch, bool batch_has_more_output) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, batch_has_more_output);
		};
		return source_batch_boundary.Execute(step_idx, step, input, have_more_output, execute_output_batch);
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
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return projection_chain.Flush(step_idx, execute_output_batch);
	}

	bool FlushSourceBoundaryBatch(idx_t step_idx) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return source_batch_boundary.Flush(step_idx, execute_output_batch);
	}

	bool FlushHashJoinMaterializeBatch(idx_t step_idx) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch, false);
		};
		return hash_join_materialize.Flush(step_idx, execute_output_batch);
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
	SljitSelectedHashJoinInputRuntime selected_hash_join_inputs;
	SljitHashJoinProbeMaterializePrimitiveRuntime hash_join_materialize;
	SljitMarkProbeFilterBoundaryRuntime mark_probe_filter_boundary;
	SljitSourceBatchBoundaryRuntime source_batch_boundary;
	SljitProjectionChainPrimitiveRuntime projection_chain;
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

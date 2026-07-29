//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_source_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_full_pipeline_recipe_state.hpp"
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
	    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications_p,
	    SljitRegionExecutionScratch &scratch_p, SljitFullPipelineTerminalRuntimeState &terminal_state_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), recipe(recipe_p),
	      execute_hash_join_probe(execute_hash_join_probe_p),
	      terminal_runtime(execute_native_full_pipeline_from_p, source_distinct_counts_p, source_min_values_p,
	                       source_max_values_p, shared_predicate_classifications_p, terminal_state_p),
	      scratch(scratch_p), selected_hash_join_inputs(runtime, ops, scratch), source_fetch(runtime, result, recipe),
	      generated_filter(runtime, ops, scratch), hash_join_materialize(runtime, result, ops, scratch),
	      hash_join_selection(runtime, result, ops, scratch, selected_hash_join_inputs),
	      mark_probe_filter_boundary(runtime, result, ops, scratch, selected_hash_join_inputs),
	      projection_chain(runtime, ops, scratch),
	      direct_aggregate_consumer_dispatch(terminal_state_p.direct_aggregate_consumer_dispatch) {
	}

	bool Execute() {
		if (recipe.UsesPrimitiveAggregateStateSource()) {
			return ExecutePrimitiveAggregateStateSourceRecipe();
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
			    return SljitStopFullPipelineAfterFlush(result, SljitBlockedSourceStopResult(runtime),
			                                           [&]() { return FlushRuntimeState(true, true); });
		    },
		    [&]() {
			    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED,
			                                           [&]() { return FlushRuntimeState(false, true); });
		    });
	}

private:
	bool ExecutePrimitiveAggregateStateSourceRecipe() {
		if (recipe.primitive_sequence.Count() != 2 ||
		    TerminalStep().kind != SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE) {
			throw InternalException(
			    "SLJIT primitive aggregate-state source requires a direct ungrouped aggregate terminal");
		}
		auto &primitive = TerminalStep().ungrouped_aggregate_update;
		if (primitive.strategy != SljitUngroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE ||
		    primitive.aggregate_idx >= ops.size()) {
			throw InternalException("SLJIT primitive aggregate-state source has an invalid terminal binding");
		}
		auto &aggregate_op = ops[primitive.aggregate_idx];
		DataChunk binding_input;
		auto &binding = SljitBindRecordedNativeSink(
		    runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx, aggregate_op.kind, binding_input,
		    aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
		    "SLJIT primitive aggregate-state source sink");
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
			throw InternalException("SLJIT primitive aggregate-state source sink binding is incomplete");
		}

		idx_t fetched_batches = 0;
		while (fetched_batches < runtime.MaxChunks()) {
			ExecutionAggregateStateScanBatch *batch = nullptr;
			auto source_result = runtime.FetchPrimitiveAggregateStateSourceContract(batch);
			if (source_result == SourceResultType::BLOCKED) {
				return SljitStopFullPipeline(result, SljitBlockedSourceStopResult(runtime));
			}
			fetched_batches++;
			if (batch) {
				if (batch->Count() == 0) {
					throw InternalException("SLJIT primitive aggregate-state source returned an empty batch");
				}
				string blocker;
				auto combine_start = SljitRegionStageStart(runtime);
				if (!batch->CombinePrimitive(recipe.primitive_aggregate_state_source_lanes,
				                             binding.aggregate_update.primitive, blocker)) {
					throw InternalException("SLJIT primitive aggregate-state combine failed: %s",
					                        blocker.empty() ? "unknown" : blocker.c_str());
				}
				RecordSljitRegionStageRuntime(runtime, primitive.aggregate_idx, aggregate_op.kind,
				                              "primitive_state_source_combine", combine_start);
				runtime.RecordJitRuntimePath("source.hash_aggregate.primitive_state_combine", batch->Count());
				runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK, batch->Count());
				runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION, batch->Count());
				runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, batch->Count());
				auto sink_result =
				    runtime.ExecutionOperators().RecordSinkResult(batch->Count(), SinkResultType::NEED_MORE_INPUT);
				if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
					return true;
				}
				processed_batches++;
			}
			if (source_result == SourceResultType::FINISHED) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::FINISHED);
			}
		}
		return SljitStopFullPipeline(result, ExecutionRegionResult::NOT_FINISHED);
	}

	const SljitFullPipelinePrimitiveStep &TerminalStep() const {
		return recipe.primitive_sequence.Step(recipe.primitive_sequence.Count() - 1);
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
			return ExecuteHashJoinProbeSelection(step_idx, step, input, have_more_output);
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
	                                   const SljitRuntimeBatchView &input, bool have_more_output) {
		const auto next_step_idx = step_idx + 1;
		auto execute_next_step = [&](const SljitRuntimeBatchView &output) {
			return ExecuteStep(next_step_idx, output, have_more_output);
		};
		const auto direct_consumer_contract =
		    recipe.direct_aggregate_consumer.probe_step_idx == step_idx
		        ? optional_ptr<const SljitHashJoinDirectAggregateConsumerContract>(&recipe.direct_aggregate_consumer)
		        : nullptr;
		auto try_execute_direct_consumer = [&](const SljitHashJoinDirectAggregateConsumerContract &contract,
		                                       const SljitHashJoinProbeSelectionPrimitive &probe_primitive,
		                                       DataChunk &join_input, auto &probe_executor) {
			D_ASSERT(contract.probe_step_idx == step_idx);
			D_ASSERT(contract.terminal_step_idx + 1 == recipe.primitive_sequence.Count());
			return terminal_runtime.TryExecuteHashJoinProbeConsumer(runtime, ops, scratch, contract, probe_primitive,
			                                                        join_input, probe_executor);
		};
		return hash_join_selection.Execute(step, input, execute_hash_join_probe, execute_next_step,
		                                   direct_consumer_contract, direct_aggregate_consumer_dispatch,
		                                   try_execute_direct_consumer);
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
	SljitHashJoinAggregateConsumerDispatch &direct_aggregate_consumer_dispatch;
	idx_t processed_batches = 0;
};

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM, class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteFullPipelinePrimitiveSequenceBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    const SljitFullPipelineRecipe &recipe, EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, const vector<idx_t> &source_distinct_counts,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications,
    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state) {
	auto executor =
	    SljitFullPipelinePrimitiveSequenceBatchExecutor<EXECUTE_NATIVE_FULL_PIPELINE_FROM, EXECUTE_HASH_JOIN_PROBE>(
	        runtime, result, ops, recipe, execute_native_full_pipeline_from, execute_hash_join_probe,
	        source_distinct_counts, source_min_values, source_max_values, shared_predicate_classifications, scratch,
	        terminal_state);
	return executor.Execute();
}

} // namespace duckdb

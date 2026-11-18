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
#include "sljit_projection_chain_primitive_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_selected_hash_join_input_runtime.hpp"

namespace duckdb {

template <class EXECUTE_HASH_JOIN_PROBE>
class SljitFullPipelinePrimitiveSequenceBatchExecutor {
public:
	SljitFullPipelinePrimitiveSequenceBatchExecutor(
	    ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p, vector<SljitExecutableRegionOp> &ops_p,
	    const SljitFullPipelineRecipe &recipe_p, EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe_p,
	    const vector<idx_t> &source_distinct_counts_p, const vector<Value> &source_min_values_p,
	    const vector<Value> &source_max_values_p,
	    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications_p,
	    SljitRegionExecutionScratch &scratch_p, SljitFullPipelineTerminalRuntimeState &terminal_state_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), recipe(recipe_p), sequence(&recipe_p.primitive_sequence),
	      execute_hash_join_probe(execute_hash_join_probe_p),
	      terminal_runtime(source_distinct_counts_p, source_min_values_p, source_max_values_p,
	                       shared_predicate_classifications_p, terminal_state_p),
	      scratch(scratch_p), selected_hash_join_inputs(runtime, ops, scratch), generated_filter(runtime, ops, scratch),
	      hash_join_materialize(runtime, ops, scratch, selected_hash_join_inputs),
	      hash_join_selection(runtime, ops, scratch, selected_hash_join_inputs),
	      mark_probe_filter_boundary(runtime, ops, scratch, selected_hash_join_inputs),
	      projection_chain(runtime, ops, scratch),
	      direct_aggregate_consumer_dispatch(terminal_state_p.direct_aggregate_consumer_dispatch) {
	}

	//! Chunk step: whether the terminal leaves the pipeline to DuckDB's operators, decided on the first source chunk
	bool HandsOverToNative(DataChunk &source_chunk, const SljitExecutableNativeContinuation &continuation) {
		return terminal_runtime.HandsOverToNative(ops, scratch, TerminalStep(), source_chunk, continuation);
	}

	//! Chunk step: binds the terminal once, before the executor hands over the first chunk
	void PrepareChunks() {
		if (!terminal_runtime.Prepare(runtime, ops, scratch, TerminalStep())) {
			throw InternalException("SLJIT primitive sequence terminal preparation failed");
		}
	}

	//! Chunk step: runs one source chunk, which the pipeline executor fetched and advanced the sink's batch for,
	//! through the steps. Rows the steps batch wait for later chunks. Returns true when the pipeline stopped. A chunk
	//! whose zone maps prove the source's filter for every row takes the steps that skip it
	bool ExecuteChunk(DataChunk &source_chunk, bool source_filters_hold,
	                  optional_ptr<ExecutionDeferredColumns> deferred_columns) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, source_chunk.size());
		}
		const auto skips_source_filter = source_filters_hold && recipe.filters_hold_sequence.Count() > 0;
		if (skips_source_filter && runtime.TraceRuntime()) {
			runtime.RecordJitRuntimePath("source.storage_scan.zone_maps_hold_filter", source_chunk.size());
		}
		sequence = skips_source_filter ? &recipe.filters_hold_sequence : &recipe.primitive_sequence;
		if (deferred_columns) {
			if (!FiltersBeforeReading(recipe.primitive_sequence)) {
				// every chunk's first step reads all of its columns
				deferred_columns->StopDeferring();
			}
			if (!FiltersBeforeReading(*sequence)) {
				deferred_columns->ReadAll(source_chunk);
				deferred_columns = nullptr;
			}
		}
		source_deferred_columns = deferred_columns;
		const auto stopped = ExecuteStep(1, SljitRuntimeBatchViewFromChunk(source_chunk));
		source_deferred_columns = nullptr;
		return stopped;
	}

	//! Chunk step: every batched row and the terminal's state sink, because the sink moves to another partition or
	//! the source is exhausted. The steps after the source's filter are the same in both step lists
	bool FlushChunks() {
		sequence = &recipe.primitive_sequence;
		return FlushRuntimeState();
	}

	//! The aggregate-state source recipe: combines the aggregate-state batches the source scans straight into the
	//! terminal ungrouped aggregate
	bool ExecuteAggregateStateSource() {
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
		    runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx, aggregate_op.Kind(), binding_input,
		    aggregate_op.AggregateUpdate().plan.sink_info, "aggregate-update-runtime-binding-failed",
		    "SLJIT primitive aggregate-state source sink");
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
			throw InternalException("SLJIT primitive aggregate-state source sink binding is incomplete");
		}

		idx_t fetched_batches = 0;
		while (fetched_batches < runtime.MaxChunks()) {
			ExecutionAggregateStateScanBatch *batch = nullptr;
			auto source_result = runtime.FetchPrimitiveAggregateStateSourceContract(batch);
			if (source_result == SourceResultType::BLOCKED) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
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
				RecordSljitRegionStageRuntime(runtime, primitive.aggregate_idx, aggregate_op.Kind(),
				                              "primitive_state_source_combine", combine_start);
				runtime.RecordJitRuntimePath("source.hash_aggregate.primitive_state_combine", batch->Count());
				RecordSljitRegionBackendWork(runtime, batch->Count());
				runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION, batch->Count());
				runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, batch->Count());
				auto sink_result =
				    runtime.ExecutionOperators().RecordSinkResult(batch->Count(), SinkResultType::NEED_MORE_INPUT);
				if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
					return true;
				}
			}
			if (source_result == SourceResultType::FINISHED) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::FINISHED);
			}
		}
		return SljitStopFullPipeline(result, ExecutionRegionResult::NOT_FINISHED);
	}

private:
	const SljitFullPipelinePrimitiveStep &TerminalStep() const {
		return recipe.primitive_sequence.Step(recipe.primitive_sequence.Count() - 1);
	}

	bool IsTerminalStep(idx_t step_idx) const {
		return step_idx + 1 == sequence->Count();
	}

	//! Whether a step list's first step is the source's generated filter, which reads only the source's filtered
	//! columns before it keeps rows
	bool FiltersBeforeReading(const SljitFullPipelinePrimitiveSequence &steps) const {
		return steps.Count() > 2 && steps.Step(1).kind == SljitFullPipelinePrimitiveKind::GENERATED_FILTER &&
		       steps.Step(1).generated_filter.filter_idx == recipe.source_filter_idx;
	}

	bool ExecuteStep(idx_t step_idx, const SljitRuntimeBatchView &input) {
		if (step_idx >= sequence->Count()) {
			throw InternalException("SLJIT primitive sequence reached the end without a consumer");
		}
		auto &step = sequence->Step(step_idx);
		if (IsTerminalStep(step_idx)) {
			return ExecuteTerminalStep(step, input);
		}
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::GENERATED_FILTER:
			return ExecuteGeneratedFilter(step_idx, step, input);
		case SljitFullPipelinePrimitiveKind::HELD_FILTER:
			return ExecuteStep(step_idx + 1, input);
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

	bool ExecuteTerminalStep(const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input) {
		return terminal_runtime.Execute(runtime, result, ops, scratch, step, input);
	}

	bool ExecuteMaterializedBatch(idx_t step_idx, DataChunk &batch) {
		if (batch.size() == 0) {
			return false;
		}
		auto batch_view = SljitRuntimeBatchViewFromChunk(batch);
		return ExecuteStep(step_idx, batch_view);
	}

	bool ExecuteGeneratedFilter(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                            const SljitRuntimeBatchView &input) {
		auto execute_output_view = [&](const SljitRuntimeBatchView &output) {
			if (step_idx == 1 && source_deferred_columns) {
				// the source's columns the filter did not read, for the rows it kept
				source_deferred_columns->Read(*output.chunk, output.selection, output.count);
				source_deferred_columns = nullptr;
			}
			return ExecuteStep(step_idx + 1, output);
		};
		return generated_filter.Execute(step, input, execute_output_view);
	}

	bool ExecuteHashJoinProbeMaterialize(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                     const SljitRuntimeBatchView &input) {
		const auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		const auto direct_handoff = HashJoinMaterializeCanDirectHandoffTo(sequence->Step(next_step_idx));
		return hash_join_materialize.Execute(step_idx, step, input, direct_handoff, execute_hash_join_probe,
		                                     execute_output_batch);
	}

	//! DuckDB's operators take each output chunk as their own pipeline would produce it: batching would flatten the
	//! global dictionaries a dictionary-narrowing hash join build requires
	bool HashJoinMaterializeCanDirectHandoffTo(const SljitFullPipelinePrimitiveStep &step) const {
		return step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE ||
		       step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION ||
		       step.kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION;
	}

	bool ExecuteHashJoinProbeSelection(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                                   const SljitRuntimeBatchView &input) {
		const auto next_step_idx = step_idx + 1;
		auto execute_next_step = [&](const SljitRuntimeBatchView &output) {
			return ExecuteStep(next_step_idx, output);
		};
		const auto direct_consumer_contract =
		    recipe.direct_aggregate_consumer.probe_step_idx == step_idx
		        ? optional_ptr<const SljitHashJoinDirectAggregateConsumerContract>(&recipe.direct_aggregate_consumer)
		        : nullptr;
		auto try_execute_direct_consumer = [&](const SljitHashJoinDirectAggregateConsumerContract &contract,
		                                       const SljitHashJoinProbeSelectionPrimitive &probe_primitive,
		                                       DataChunk &join_input, auto &probe_executor) {
			D_ASSERT(contract.probe_step_idx == step_idx);
			D_ASSERT(contract.terminal_step_idx + 1 == sequence->Count());
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
			return ExecuteStep(step_idx + 1, output);
		};
		return mark_probe_filter_boundary.Execute(step_idx, step, input, execute_hash_join_probe, execute_next_step);
	}

	bool ExecuteProjectionChain(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step,
	                            const SljitRuntimeBatchView &input) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
		};
		const auto direct_handoff =
		    IsTerminalStep(next_step_idx) && ProjectionChainCanDirectHandoffTo(sequence->Step(next_step_idx));
		return projection_chain.Execute(step_idx, step, input, direct_handoff, execute_output_batch);
	}

	bool ProjectionChainCanDirectHandoffTo(const SljitFullPipelinePrimitiveStep &step) const {
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK:
		case SljitFullPipelinePrimitiveKind::PRIMITIVE_AGGREGATE_UPDATE:
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION:
			return true;
		default:
			return false;
		}
	}

	bool FlushRuntimeState() {
		return FlushPendingBatch();
	}

	bool FlushPendingBatch() {
		for (idx_t step_idx = 1; step_idx + 1 < recipe.primitive_sequence.Count(); step_idx++) {
			auto &step = recipe.primitive_sequence.Step(step_idx);
			if (FlushMaterializingStep(step_idx, step)) {
				return true;
			}
		}
		auto &terminal_step = TerminalStep();
		return terminal_runtime.Flush(runtime, result, ops, scratch, terminal_step);
	}

	bool FlushMaterializingStep(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step) {
		auto next_step_idx = step_idx + 1;
		auto execute_output_batch = [&](DataChunk &batch) -> bool {
			return ExecuteMaterializedBatch(next_step_idx, batch);
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
	//! The steps the current source chunk runs: the recipe's, or those that skip the source's filter
	const SljitFullPipelinePrimitiveSequence *sequence;
	EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe;
	SljitFullPipelineTerminalRuntime terminal_runtime;
	//! The source chunk's columns the source left unread, until the source's filter kept its rows
	optional_ptr<ExecutionDeferredColumns> source_deferred_columns;
	SljitRegionExecutionScratch &scratch;
	SljitSelectedHashJoinInputRuntime selected_hash_join_inputs;
	SljitGeneratedFilterPrimitiveRuntime generated_filter;
	SljitHashJoinProbeMaterializePrimitiveRuntime hash_join_materialize;
	SljitHashJoinProbeSelectionPrimitiveRuntime hash_join_selection;
	SljitMarkProbeFilterBoundaryRuntime mark_probe_filter_boundary;
	SljitProjectionChainPrimitiveRuntime projection_chain;
	SljitHashJoinAggregateConsumerDispatch &direct_aggregate_consumer_dispatch;
};

} // namespace duckdb

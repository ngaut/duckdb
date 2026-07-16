//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_terminal_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_append_sink_runtime.hpp"
#include "sljit_delim_join_sink_runtime.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime_state.hpp"
#include "sljit_hash_join_build_sink_runtime.hpp"
#include "sljit_native_tail_delegation_runtime.hpp"
#include "sljit_post_join_projection_aggregate_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_ungrouped_aggregate_update_primitive.hpp"

namespace duckdb {

struct SljitFullPipelineTerminalRuntimeState {
	bool prepared = false;
	SljitFullPipelinePrimitiveKind kind = SljitFullPipelinePrimitiveKind::INVALID;
	SljitUngroupedAggregateUpdateRuntimeState ungrouped_aggregate_update;
	SljitGroupedAggregateUpdateRuntimeState grouped_aggregate_update;
	SljitPostJoinProjectionAggregateRuntimeState post_join_projection_aggregate;
	SljitHashJoinBuildSinkRuntimeState hash_join_build_sink;
	SljitDelimJoinSinkRuntimeState delim_join_sink;
	SljitAppendSinkRuntimeState append_sink;
};

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
class SljitFullPipelineTerminalRuntime {
public:
	SljitFullPipelineTerminalRuntime(EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from_p,
	                                 const vector<idx_t> &source_distinct_counts_p,
	                                 const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                                 SljitFullPipelineTerminalRuntimeState &state_p)
	    : source_distinct_counts(source_distinct_counts_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p), state(state_p),
	      native_tail_delegation(execute_native_full_pipeline_from_p) {
	}

	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step) {
		if (state.prepared) {
			if (state.kind != terminal_step.kind) {
				throw InternalException("SLJIT pipeline-local terminal state changed primitive kind");
			}
			BeginInvocation(terminal_step.kind);
			return true;
		}
		bool prepared = false;
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			prepared = state.ungrouped_aggregate_update.Prepare(runtime, ops, scratch,
			                                                    terminal_step.ungrouped_aggregate_update);
			break;
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			prepared =
			    state.grouped_aggregate_update.Prepare(runtime, ops, scratch, terminal_step.grouped_aggregate_update);
			break;
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			prepared = state.post_join_projection_aggregate.Prepare(ops, terminal_step.post_join_projection_aggregate,
			                                                        source_distinct_counts, source_min_values,
			                                                        source_max_values);
			break;
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK:
			prepared = true;
			break;
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			prepared = state.delim_join_sink.Prepare(runtime, ops, terminal_step.delim_join_sink);
			break;
		case SljitFullPipelinePrimitiveKind::APPEND_SINK:
			prepared = state.append_sink.Prepare(runtime, ops, terminal_step.append_sink);
			break;
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION:
			prepared = true;
			break;
		default:
			return false;
		}
		if (prepared) {
			state.prepared = true;
			state.kind = terminal_step.kind;
			BeginInvocation(terminal_step.kind);
		}
		return prepared;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step,
	             const SljitRuntimeBatchView &input, bool have_more_output, idx_t &processed_batches) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			return state.ungrouped_aggregate_update.Execute(
			    runtime, result, ops, scratch, terminal_step.ungrouped_aggregate_update, input, processed_batches);
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return ExecuteGroupedAggregateUpdate(runtime, result, ops, scratch, terminal_step, input,
			                                     processed_batches);
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			(void)have_more_output;
			return state.post_join_projection_aggregate.Execute(runtime, result, ops, scratch,
			                                                    terminal_step.post_join_projection_aggregate, input);
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK:
			(void)have_more_output;
			return ExecuteHashJoinBuildSink(runtime, result, ops, scratch, terminal_step, input, processed_batches);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return state.delim_join_sink.Execute(runtime, result, ops, scratch, terminal_step.delim_join_sink, input,
			                                     processed_batches);
		case SljitFullPipelinePrimitiveKind::APPEND_SINK:
			return state.append_sink.Execute(runtime, result, ops, scratch, terminal_step.append_sink, input,
			                                 processed_batches);
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION:
			return native_tail_delegation.Execute(runtime, result, ops, scratch, terminal_step, input,
			                                      processed_batches);
		default:
			throw InternalException("SLJIT primitive sequence contains an unsupported terminal primitive");
		}
	}

	template <class EXECUTE_HASH_JOIN_PROBE>
	SljitHashJoinAggregateConsumerResult
	TryExecuteHashJoinProbeConsumer(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                SljitRegionExecutionScratch &scratch,
	                                const SljitHashJoinDirectAggregateConsumerContract &contract,
	                                const SljitHashJoinProbeSelectionPrimitive &probe_primitive, DataChunk &join_input,
	                                EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe) {
		D_ASSERT(contract.IsBound());
		return state.post_join_projection_aggregate.TryExecuteHashJoinProbeConsumer(
		    runtime, ops, scratch, contract, probe_primitive, join_input, execute_hash_join_probe,
		    contract.probe_input_filter_idx);
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step,
	           idx_t &processed_batches) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			return state.ungrouped_aggregate_update.Flush(runtime, ops, scratch,
			                                              terminal_step.ungrouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return state.grouped_aggregate_update.Flush(runtime, ops, scratch, terminal_step.grouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			return state.post_join_projection_aggregate.Flush(runtime, result, ops, scratch);
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_BUILD_SINK:
			return false;
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return state.delim_join_sink.Flush(runtime, result, ops, scratch, terminal_step.delim_join_sink,
			                                   processed_batches);
		case SljitFullPipelinePrimitiveKind::APPEND_SINK:
			return state.append_sink.Flush(runtime, result, ops, scratch, terminal_step.append_sink, processed_batches);
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_DELEGATION:
			native_tail_delegation.Finalize(scratch);
			return false;
		default:
			throw InternalException("SLJIT primitive sequence has an unsupported terminal flush primitive");
		}
	}

	bool BudgetReached(ExecutionRegionRuntime &runtime, const SljitFullPipelinePrimitiveStep &terminal_step,
	                   idx_t max_recipe_batches) const {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			return state.post_join_projection_aggregate.BudgetReached(runtime, max_recipe_batches);
		default:
			return false;
		}
	}

private:
	void BeginInvocation(SljitFullPipelinePrimitiveKind kind) {
		if (kind == SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE) {
			state.post_join_projection_aggregate.BeginInvocation();
		}
	}

	bool ExecuteGroupedAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                   vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                   const SljitFullPipelinePrimitiveStep &terminal_step,
	                                   const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		return state.grouped_aggregate_update.Execute(runtime, result, ops, scratch,
		                                              terminal_step.grouped_aggregate_update, input, processed_batches);
	}

	bool ExecuteHashJoinBuildSink(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                              vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                              const SljitFullPipelinePrimitiveStep &terminal_step,
	                              const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		return state.hash_join_build_sink.Execute(runtime, result, ops, scratch, terminal_step.hash_join_build_sink,
		                                          input, processed_batches);
	}

	const vector<idx_t> &source_distinct_counts;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	SljitFullPipelineTerminalRuntimeState &state;
	SljitNativeTailDelegationRuntimeState<EXECUTE_NATIVE_FULL_PIPELINE_FROM> native_tail_delegation;
};

} // namespace duckdb

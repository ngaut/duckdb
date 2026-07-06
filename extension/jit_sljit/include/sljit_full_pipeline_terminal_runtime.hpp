//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_terminal_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_delim_join_sink_runtime.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime_state.hpp"
#include "sljit_post_join_projection_aggregate_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"
#include "sljit_ungrouped_aggregate_update_primitive.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
class SljitFullPipelineTerminalRuntime {
public:
	SljitFullPipelineTerminalRuntime(EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from_p,
	                                 const vector<idx_t> &source_distinct_counts_p,
	                                 const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : execute_native_full_pipeline_from(execute_native_full_pipeline_from_p),
	      source_distinct_counts(source_distinct_counts_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p) {
	}

	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			return ungrouped_aggregate_update.Prepare(runtime, ops, scratch, terminal_step.ungrouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return grouped_aggregate_update.Prepare(runtime, ops, scratch, terminal_step.grouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			return post_join_projection_aggregate.Prepare(runtime, ops, terminal_step.post_join_projection_aggregate,
			                                              source_distinct_counts, source_min_values, source_max_values);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return delim_join_sink.Prepare(runtime, ops, terminal_step.delim_join_sink);
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF:
			return true;
		default:
			return false;
		}
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step,
	             const SljitRuntimeBatchView &input, bool have_more_output, idx_t &processed_batches) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			return ungrouped_aggregate_update.Execute(
			    runtime, result, ops, scratch, terminal_step.ungrouped_aggregate_update, input, processed_batches);
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return ExecuteGroupedAggregateUpdate(runtime, result, ops, scratch, terminal_step, input,
			                                     processed_batches);
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			(void)have_more_output;
			return post_join_projection_aggregate.Execute(runtime, result, ops, scratch,
			                                              terminal_step.post_join_projection_aggregate, input);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return delim_join_sink.Execute(runtime, result, ops, scratch, terminal_step.delim_join_sink, input,
			                               processed_batches);
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF:
			return ExecuteNativeTailHandoff(runtime, result, scratch, terminal_step, input, processed_batches);
		default:
			throw InternalException("SLJIT primitive sequence contains an unsupported terminal primitive");
		}
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step,
	           idx_t &processed_batches) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE:
			return ungrouped_aggregate_update.Flush(runtime, ops, scratch, terminal_step.ungrouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return grouped_aggregate_update.Flush(runtime, ops, scratch, terminal_step.grouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			return post_join_projection_aggregate.Flush(runtime, result, ops, scratch);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return false;
		case SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF:
			execute_native_full_pipeline_from.Finalize(scratch);
			return false;
		default:
			throw InternalException("SLJIT primitive sequence has an unsupported terminal flush primitive");
		}
	}

	bool BudgetReached(ExecutionRegionRuntime &runtime, const SljitFullPipelinePrimitiveStep &terminal_step,
	                   idx_t max_recipe_batches) const {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE:
			return post_join_projection_aggregate.BudgetReached(runtime, max_recipe_batches);
		default:
			return false;
		}
	}

private:
	bool ExecuteGroupedAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                   vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                   const SljitFullPipelinePrimitiveStep &terminal_step,
	                                   const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		return grouped_aggregate_update.Execute(runtime, result, ops, scratch, terminal_step.grouped_aggregate_update,
		                                        input, processed_batches);
	}

	bool ExecuteNativeTailHandoff(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                              SljitRegionExecutionScratch &scratch,
	                              const SljitFullPipelinePrimitiveStep &terminal_step,
	                              const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		auto &chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT native tail handoff");
		auto sink_result = execute_native_full_pipeline_from(scratch, terminal_step.Op(0), chunk);
		auto stopped = SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
		if (stopped) {
			execute_native_full_pipeline_from.Finalize(scratch);
			return true;
		}
		processed_batches++;
		return false;
	}

	EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from;
	const vector<idx_t> &source_distinct_counts;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	SljitUngroupedAggregateUpdateRuntimeState ungrouped_aggregate_update;
	SljitGroupedAggregateUpdateRuntimeState grouped_aggregate_update;
	SljitPostJoinProjectionAggregateRuntimeState post_join_projection_aggregate;
	SljitDelimJoinSinkRuntimeState delim_join_sink;
};

} // namespace duckdb

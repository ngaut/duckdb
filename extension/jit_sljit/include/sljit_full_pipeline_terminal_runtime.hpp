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
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_join_projection_aggregate_update_runtime.hpp"

namespace duckdb {

template <class EXECUTE_HASH_JOIN_PROBE>
class SljitFullPipelineTerminalRuntime {
public:
	SljitFullPipelineTerminalRuntime(EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe_p,
	                                 const vector<idx_t> &source_distinct_counts_p,
	                                 const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : execute_hash_join_probe(execute_hash_join_probe_p), source_distinct_counts(source_distinct_counts_p),
	      source_min_values(source_min_values_p), source_max_values(source_max_values_p) {
	}

	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return grouped_aggregate_update.Prepare(runtime, ops, scratch, terminal_step.grouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE:
			return join_projection_aggregate_update.Prepare(
			    runtime, ops, terminal_step.join_projection_aggregate_update, source_distinct_counts, source_min_values,
			    source_max_values);
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
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return ExecuteGroupedAggregateUpdate(runtime, result, ops, scratch, terminal_step, input,
			                                     processed_batches);
		case SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE:
			return join_projection_aggregate_update.Execute(runtime, result, ops, scratch,
			                                                terminal_step.join_projection_aggregate_update, input,
			                                                have_more_output, execute_hash_join_probe);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return delim_join_sink.Execute(runtime, result, ops, scratch, terminal_step.delim_join_sink, input,
			                               processed_batches);
		default:
			throw InternalException("SLJIT primitive sequence contains an unsupported terminal primitive");
		}
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &terminal_step,
	           idx_t &processed_batches) {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE:
			return grouped_aggregate_update.Flush(runtime, scratch, terminal_step.grouped_aggregate_update);
		case SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE:
			return join_projection_aggregate_update.Flush(
			    runtime, result, ops, scratch, terminal_step.join_projection_aggregate_update, execute_hash_join_probe);
		case SljitFullPipelinePrimitiveKind::DELIM_JOIN_SINK:
			return false;
		default:
			throw InternalException("SLJIT primitive sequence has an unsupported terminal flush primitive");
		}
	}

	bool BudgetReached(ExecutionRegionRuntime &runtime, const SljitFullPipelinePrimitiveStep &terminal_step,
	                   idx_t max_recipe_batches) const {
		switch (terminal_step.kind) {
		case SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE:
			return join_projection_aggregate_update.BudgetReached(
			    runtime, terminal_step.join_projection_aggregate_update, max_recipe_batches);
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

	EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe;
	const vector<idx_t> &source_distinct_counts;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	SljitGroupedAggregateUpdateRuntimeState grouped_aggregate_update;
	SljitJoinProjectionAggregateUpdateRuntimeState join_projection_aggregate_update;
	SljitDelimJoinSinkRuntimeState delim_join_sink;
};

} // namespace duckdb

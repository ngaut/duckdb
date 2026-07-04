//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projected_aggregate_runtime.hpp"
#include "sljit_join_projection_aggregate_update_primitive.hpp"

namespace duckdb {

struct SljitJoinProjectionAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             const SljitJoinProjectionAggregateUpdatePrimitive &primitive,
	             const vector<idx_t> &source_distinct_counts, const vector<Value> &source_min_values,
	             const vector<Value> &source_max_values) {
		return selected_join_output.Prepare(runtime, ops, primitive.selected_join_output, source_distinct_counts,
		                                    source_min_values, source_max_values);
	}

	template <class EXECUTE_HASH_JOIN_PROBE>
	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitJoinProjectionAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, bool have_more_output,
	             EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe) {
		(void)have_more_output;
		(void)execute_hash_join_probe;
		return selected_join_output.Execute(runtime, result, ops, scratch, primitive.selected_join_output, input);
	}

	template <class EXECUTE_HASH_JOIN_PROBE>
	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitJoinProjectionAggregateUpdatePrimitive &primitive,
	           EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe) {
		(void)primitive;
		(void)execute_hash_join_probe;
		return selected_join_output.Flush(runtime, result, ops, scratch);
	}

	bool BudgetReached(ExecutionRegionRuntime &runtime, const SljitJoinProjectionAggregateUpdatePrimitive &primitive,
	                   idx_t max_recipe_batches) const {
		(void)primitive;
		return selected_join_output.BudgetReached(runtime, max_recipe_batches);
	}

private:
	SljitPostJoinProjectionAggregateRuntimeState selected_join_output;
};

} // namespace duckdb

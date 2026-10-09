//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_state_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_native_pipeline_runtime.hpp"
#include "sljit_source_pipeline_runtime.hpp"

namespace duckdb {

//! Recipes over source chunks run as chunk steps inside the pipeline executor. The aggregate-state source recipe reads
//! the aggregate states a hash aggregate source scans, not chunks, so it alone keeps its own fetch loop
template <class KERNEL>
static bool SljitTryExecuteAggregateStateSourceRecipe(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
    vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    const SljitFullPipelineRecipePlan &recipe_plan,
    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications,
    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state) {
	if (recipe_plan.SupportsChunkStep()) {
		throw InternalException("SLJIT recipe over source chunks entered the aggregate-state fetch loop");
	}
	auto execute_native_full_pipeline_from = SljitMakeNativeTailPipelineExecutor(kernel, runtime, ops);
	auto execute_hash_join_probe =
	    SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, runtime.ExecutionOperators());
	SljitFullPipelinePrimitiveSequenceBatchExecutor<decltype(execute_native_full_pipeline_from),
	                                                decltype(execute_hash_join_probe)>
	    executor(runtime, result, ops, recipe_plan.Recipe(), execute_native_full_pipeline_from, execute_hash_join_probe,
	             source_distinct_counts, source_min_values, source_max_values, shared_predicate_classifications,
	             scratch, terminal_state);
	return executor.ExecuteAggregateStateSource();
}

} // namespace duckdb

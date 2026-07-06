//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_native_pipeline_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_source_pipeline_runtime.hpp"

#include <utility>

namespace duckdb {

template <class KERNEL>
class SljitFullPipelineRuntimeDispatcher {
public:
	SljitFullPipelineRuntimeDispatcher(KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p,
	                                   ExecutionRegionResult &result_p, vector<SljitExecutableRegionOp> &ops_p,
	                                   const vector<idx_t> &source_distinct_counts_p,
	                                   const vector<Value> &source_min_values_p,
	                                   const vector<Value> &source_max_values_p,
	                                   const SljitFullPipelineRecipePlan &recipe_plan_p)
	    : kernel(kernel_p), runtime(runtime_p), result(result_p), ops(ops_p),
	      source_distinct_counts(source_distinct_counts_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p), recipe_plan(recipe_plan_p) {
	}

	bool TryExecute() {
		if (recipe_plan.has_recipe) {
			return TryExecutePrimitiveSequenceBatched();
		}
		return TryExecuteNativeOnly();
	}

private:
	auto NativePipelineExecutor() {
		return SljitMakeNativePipelineExecutor(kernel, runtime, ops, source_distinct_counts);
	}

	auto NativeTailPipelineExecutor() {
		return SljitMakeNativeTailPipelineExecutor(kernel, runtime, ops, source_distinct_counts);
	}

	auto RecordedHashJoinProbeExecutor() {
		return SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, runtime.ExecutionOperators());
	}

	bool TryExecuteNativeOnly() {
		auto execute_native_full_pipeline = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineNativeOnly(
		    runtime, result, ops, recipe_plan.uses_extended_source_fetch_budget, execute_native_full_pipeline);
	}

	bool TryExecutePrimitiveSequenceBatched() {
		auto execute_native_full_pipeline_from = NativeTailPipelineExecutor();
		auto execute_hash_join_probe = RecordedHashJoinProbeExecutor();
		return SljitTryExecuteFullPipelinePrimitiveSequenceBatched(
		    runtime, result, ops, recipe_plan.recipe, execute_native_full_pipeline_from, execute_hash_join_probe,
		    source_distinct_counts, source_min_values, source_max_values);
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	const vector<idx_t> &source_distinct_counts;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	const SljitFullPipelineRecipePlan &recipe_plan;
};

template <class KERNEL>
static bool
SljitTryExecuteFullPipelineRecipe(KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                  vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
                                  const vector<Value> &source_min_values, const vector<Value> &source_max_values,
                                  const SljitFullPipelineRecipePlan &recipe_plan) {
	SljitFullPipelineRuntimeDispatcher<KERNEL> dispatcher(kernel, runtime, result, ops, source_distinct_counts,
	                                                      source_min_values, source_max_values, recipe_plan);
	return dispatcher.TryExecute();
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_native_pipeline_runtime.hpp"
#include "sljit_source_pipeline_runtime.hpp"

namespace duckdb {

template <class KERNEL>
class SljitFullPipelineRuntimeDispatcher {
public:
	SljitFullPipelineRuntimeDispatcher(
	    KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	    vector<SljitExecutableRegionOp> &ops_p, const vector<idx_t> &source_distinct_counts_p,
	    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	    const SljitFullPipelineRecipePlan &recipe_plan_p,
	    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications_p,
	    SljitRegionExecutionScratch &scratch_p, SljitFullPipelineTerminalRuntimeState &terminal_state_p)
	    : kernel(kernel_p), runtime(runtime_p), result(result_p), ops(ops_p),
	      source_distinct_counts(source_distinct_counts_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p), recipe_plan(recipe_plan_p),
	      shared_predicate_classifications(shared_predicate_classifications_p), scratch(scratch_p),
	      terminal_state(terminal_state_p) {
	}

	bool TryExecute() {
		switch (recipe_plan.Kind()) {
		case SljitFullPipelineRecipePlanKind::PRIMITIVE_RECIPE:
			return TryExecutePrimitiveSequenceBatched();
		case SljitFullPipelineRecipePlanKind::NATIVE_ONLY:
			runtime.RecordJitRuntimePath(recipe_plan.NativeOnlyRuntimePath().c_str());
			return TryExecuteNativeOnly();
		case SljitFullPipelineRecipePlanKind::INVALID:
			throw InternalException("SLJIT full-pipeline runtime received an invalid recipe plan");
		}
		throw InternalException("Unknown SLJIT full-pipeline recipe plan kind");
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
		return SljitTryExecuteFullPipelineNativeOnly(runtime, result, ops, execute_native_full_pipeline, scratch);
	}

	bool TryExecutePrimitiveSequenceBatched() {
		auto execute_native_full_pipeline_from = NativeTailPipelineExecutor();
		auto execute_hash_join_probe = RecordedHashJoinProbeExecutor();
		return SljitTryExecuteFullPipelinePrimitiveSequenceBatched(
		    runtime, result, ops, recipe_plan.Recipe(), execute_native_full_pipeline_from, execute_hash_join_probe,
		    source_distinct_counts, source_min_values, source_max_values, shared_predicate_classifications, scratch,
		    terminal_state);
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
	vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications;
	SljitRegionExecutionScratch &scratch;
	SljitFullPipelineTerminalRuntimeState &terminal_state;
};

template <class KERNEL>
static bool SljitTryExecuteFullPipelineRecipe(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
    vector<SljitExecutableRegionOp> &ops, const vector<idx_t> &source_distinct_counts,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    const SljitFullPipelineRecipePlan &recipe_plan,
    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications,
    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state) {
	SljitFullPipelineRuntimeDispatcher<KERNEL> dispatcher(kernel, runtime, result, ops, source_distinct_counts,
	                                                      source_min_values, source_max_values, recipe_plan,
	                                                      shared_predicate_classifications, scratch, terminal_state);
	return dispatcher.TryExecute();
}

} // namespace duckdb

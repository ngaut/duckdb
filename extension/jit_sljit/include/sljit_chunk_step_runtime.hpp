//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_chunk_step_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_kernel.hpp"

#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_native_continuation_runtime.hpp"
#include "sljit_source_pipeline_runtime.hpp"

namespace duckdb {

//! A chunk step stops mid-chunk only because the sink finished: the executor owns the source, so there is nothing to
//! defer or yield on
static SinkResultType SljitChunkStepResult(bool stopped, ExecutionRegionResult result) {
	if (!stopped) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (result != ExecutionRegionResult::FINISHED) {
		throw InternalException("SLJIT chunk step stopped mid-chunk for a reason other than its sink");
	}
	return SinkResultType::FINISHED;
}

//! A primitive recipe bound to one pipeline executor: its step runtimes, and the rows they batch, live as long as the
//! binding
template <class KERNEL>
class SljitPrimitiveRecipeChunkStep final : public ExecutionRegionChunkStep {
	using HashJoinProbeExecutor = SljitRecordedHashJoinProbeCallback<KERNEL>;

public:
	SljitPrimitiveRecipeChunkStep(
	    KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops,
	    const SljitFullPipelineRecipe &recipe, const vector<idx_t> &source_distinct_counts,
	    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
	    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications,
	    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state,
	    optional_ptr<SljitExecutableNativeContinuation> native_continuation_plan)
	    : kernel(kernel_p), runtime(runtime_p),
	      hash_join_probe(SljitMakeRecordedHashJoinProbeCallback(kernel_p, runtime_p, runtime_p.ExecutionOperators())),
	      executor(runtime_p, result, ops, recipe, hash_join_probe, source_distinct_counts, source_min_values,
	               source_max_values, shared_predicate_classifications, scratch, terminal_state),
	      native_continuation(runtime_p, native_continuation_plan) {
		executor.PrepareChunks();
	}

	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, ExecutionSourceChunkProof proof) override {
		return native_continuation.ContinuesNatively(
		    input, proof.table_filters_hold,
		    [&](const SljitExecutableNativeContinuation &plan) { return executor.HandsOverToNative(input, plan); },
		    [&]() { SljitChunkStepResult(executor.FlushChunks(), result); });
	}

	SinkResultType ExecuteChunk(DataChunk &input, ExecutionSourceChunkProof proof) override {
		return SljitChunkStepResult(executor.ExecuteChunk(input, proof.table_filters_hold), result);
	}

	void FlushChunks() override {
		SljitChunkStepResult(executor.FlushChunks(), result);
	}

	void FinishChunks() override {
		SljitChunkStepResult(executor.FlushChunks(), result);
		kernel.RecordExecutedFilterRows(runtime);
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult result = ExecutionRegionResult::NOT_FINISHED;
	HashJoinProbeExecutor hash_join_probe;
	SljitFullPipelinePrimitiveSequenceBatchExecutor<HashJoinProbeExecutor> executor;
	SljitNativeContinuationDecision native_continuation;
};

} // namespace duckdb

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
#include "sljit_native_pipeline_runtime.hpp"
#include "sljit_source_fetch_primitive_runtime.hpp"
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
	using NativeTailExecutor = SljitNativePipelineExecutor<KERNEL, SljitNativeTailDelegationTerminalPolicy>;
	using HashJoinProbeExecutor = SljitRecordedHashJoinProbeCallback<KERNEL>;

public:
	SljitPrimitiveRecipeChunkStep(
	    KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops,
	    const SljitFullPipelineRecipe &recipe, const vector<idx_t> &source_distinct_counts,
	    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
	    vector<SljitSharedPerfectHashPredicateClassificationCache> &shared_predicate_classifications,
	    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state)
	    : kernel(kernel_p), runtime(runtime_p), native_tail(kernel_p, runtime_p, ops),
	      hash_join_probe(SljitMakeRecordedHashJoinProbeCallback(kernel_p, runtime_p, runtime_p.ExecutionOperators())),
	      executor(runtime_p, result, ops, recipe, native_tail, hash_join_probe, source_distinct_counts,
	               source_min_values, source_max_values, shared_predicate_classifications, scratch, terminal_state) {
		executor.PrepareChunks();
	}

	SinkResultType ExecuteChunk(DataChunk &input) override {
		return SljitChunkStepResult(executor.ExecuteChunk(input), result);
	}

	void FlushChunks() override {
		SljitChunkStepResult(executor.FlushChunks(), result);
	}

	void FinishChunks() override {
		SljitChunkStepResult(executor.FinishChunks(), result);
		kernel.RecordExecutedFilterRows(runtime);
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult result = ExecutionRegionResult::NOT_FINISHED;
	NativeTailExecutor native_tail;
	HashJoinProbeExecutor hash_join_probe;
	SljitFullPipelinePrimitiveSequenceBatchExecutor<NativeTailExecutor, HashJoinProbeExecutor> executor;
};

//! A recipe of native operators only, bound to one pipeline executor. Small source chunks (a selective scan filter)
//! batch before the operators run, so they pay the per-chunk operator and sink cost once per batch
template <class KERNEL>
class SljitNativeOnlyChunkStep final : public ExecutionRegionChunkStep {
public:
	SljitNativeOnlyChunkStep(KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops,
	                         SljitRegionExecutionScratch &scratch_p)
	    : kernel(kernel_p), runtime(runtime_p), scratch(scratch_p), native(kernel_p, runtime_p, ops),
	      source_fetch(runtime_p) {
	}

	SinkResultType ExecuteChunk(DataChunk &input) override {
		return SljitChunkStepResult(source_fetch.Execute(input, true, ExecuteBatchFunction()), result);
	}

	void FlushChunks() override {
		SljitChunkStepResult(Flush(), result);
	}

	void FinishChunks() override {
		SljitChunkStepResult(Flush(), result);
		kernel.RecordExecutedFilterRows(runtime);
	}

private:
	auto ExecuteBatchFunction() {
		return [this](const SljitRuntimeBatchView &input, bool) {
			auto &batch = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT native-only source batch");
			auto sink_result = native(scratch, batch);
			if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
				native.Finalize(scratch);
				return true;
			}
			return false;
		};
	}

	bool Flush() {
		if (source_fetch.Flush(false, ExecuteBatchFunction())) {
			return true;
		}
		native.Finalize(scratch);
		return false;
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	ExecutionRegionResult result = ExecutionRegionResult::NOT_FINISHED;
	SljitNativePipelineExecutor<KERNEL, SljitNativePipelineTerminalPolicy> native;
	SljitSourceFetchPrimitiveRuntime source_fetch;
};

} // namespace duckdb

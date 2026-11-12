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
#include "sljit_native_pipeline_runtime.hpp"
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
	    SljitRegionExecutionScratch &scratch, SljitFullPipelineTerminalRuntimeState &terminal_state,
	    optional_ptr<SljitExecutableNativeContinuation> native_continuation_plan)
	    : kernel(kernel_p), runtime(runtime_p), native_tail(kernel_p, runtime_p, ops),
	      hash_join_probe(SljitMakeRecordedHashJoinProbeCallback(kernel_p, runtime_p, runtime_p.ExecutionOperators())),
	      executor(runtime_p, result, ops, recipe, native_tail, hash_join_probe, source_distinct_counts,
	               source_min_values, source_max_values, shared_predicate_classifications, scratch, terminal_state) {
		executor.PrepareChunks();
		if (native_continuation_plan) {
			native_continuation = make_uniq<SljitNativeContinuation>(*native_continuation_plan);
		}
	}

	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input) override {
		if (!native_continuation_decided) {
			native_continuation_decided = true;
			if (native_continuation && !executor.HandsOverToNative(input, native_continuation->Plan())) {
				native_continuation.reset();
			}
		}
		if (!native_continuation) {
			return nullptr;
		}
		return &native_continuation->Prepare(runtime, input);
	}

	SinkResultType ExecuteChunk(DataChunk &input) override {
		return SljitChunkStepResult(executor.ExecuteChunk(input), result);
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
	NativeTailExecutor native_tail;
	HashJoinProbeExecutor hash_join_probe;
	SljitFullPipelinePrimitiveSequenceBatchExecutor<NativeTailExecutor, HashJoinProbeExecutor> executor;
	unique_ptr<SljitNativeContinuation> native_continuation;
	bool native_continuation_decided = false;
};

//! Native-contract operators consume the core runner's source batches.
template <class KERNEL>
class SljitNativeOnlyChunkStep final : public ExecutionRegionChunkStep {
public:
	SljitNativeOnlyChunkStep(KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops,
	                         SljitRegionExecutionScratch &scratch_p,
	                         optional_ptr<SljitExecutableNativeContinuation> native_continuation_plan)
	    : kernel(kernel_p), runtime(runtime_p), scratch(scratch_p), native(kernel_p, runtime_p, ops) {
		// without a recipe no generated step updates a hash aggregate: DuckDB's operators run the whole pipeline
		if (native_continuation_plan) {
			native_continuation = make_uniq<SljitNativeContinuation>(*native_continuation_plan);
		}
	}

	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input) override {
		if (!native_continuation) {
			return nullptr;
		}
		return &native_continuation->Prepare(runtime, input);
	}

	SinkResultType ExecuteChunk(DataChunk &input) override {
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, input.size());
		}
		return SljitChunkStepResult(ExecuteBatchFunction()(SljitRuntimeBatchViewFromChunk(input)), result);
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
		return [this](const SljitRuntimeBatchView &input) {
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
		native.Finalize(scratch);
		return false;
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	SljitRegionExecutionScratch &scratch;
	ExecutionRegionResult result = ExecutionRegionResult::NOT_FINISHED;
	SljitNativePipelineExecutor<KERNEL, SljitNativePipelineTerminalPolicy> native;
	unique_ptr<SljitNativeContinuation> native_continuation;
};

} // namespace duckdb

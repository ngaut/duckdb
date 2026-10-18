//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_pipeline_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_kernel.hpp"

#include "sljit_chunk_step_runtime.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_aggregate_state_source_runtime.hpp"
#include "sljit_join_probe_codegen.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>

namespace duckdb {

struct SljitLazyCodegenTiming {
	int64_t codegen_time_us = 0;
	int64_t machine_codegen_time_us = 0;
};

template <class BUILD>
static SljitLazyCodegenTiming TimeSljitLazyCodegen(BUILD build) {
	ExecutionRegionCompileTimings timings;
	auto codegen_start = std::chrono::steady_clock::now();
	{
		SljitCodegenTimingScope codegen_timing_scope(&timings);
		build();
	}
	SljitLazyCodegenTiming result;
	result.codegen_time_us = SljitRegionElapsedMicros(codegen_start);
	result.machine_codegen_time_us = timings.machine_codegen_time_us;
	return result;
}

static void RecordSljitLazyHashJoinProbeCodegen(ExecutionRegionRuntime &runtime, const SljitLazyCodegenTiming &timing,
                                                idx_t code_size) {
	ExecutionRegionLazyCodegenMetrics metrics;
	metrics.codegen_time_us = timing.codegen_time_us;
	metrics.machine_codegen_time_us = timing.machine_codegen_time_us;
	metrics.code_size = code_size;
	runtime.RecordLazyCodegen(metrics);
}

class SljitNativeRegionLocalState : public ExecutionRegionLocalState {
public:
	SljitNativeRegionLocalState(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops,
	                            vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings)
	    : scratch(allocator, ops, std::move(exact_source_filter_bindings)) {
	}

	SljitRegionExecutionScratch scratch;
	// Mutable terminal strategies and staged fallible group transforms stay below
	// this execution-local boundary.
	SljitFullPipelineTerminalRuntimeState terminal;
};

template <class FUNCTION, class BUILD>
FUNCTION SljitNativeRegionKernel::EnsureLazyHashJoinProbeCode(ExecutionRegionRuntime &runtime,
                                                              SljitLazyCompiledFunction<FUNCTION> &artifact,
                                                              const string &failure_message, BUILD build) {
	return artifact.Ensure([&]() {
		string error;
		FUNCTION function = nullptr;
		unique_ptr<ExecutionRegionCodeHandle> code;
		auto timing = TimeSljitLazyCodegen([&]() { code = build(function, error); });
		if (!code || !function) {
			throw InternalException("%s: %s", failure_message.c_str(), error.empty() ? "unknown error" : error);
		}
		auto code_size = code->CodeSize();
		RecordSljitLazyHashJoinProbeCodegen(runtime, timing, code_size);
		return SljitCompiledFunction<FUNCTION>(std::move(code), function);
	});
}

SljitNativePerfectHashJoinProbeFunction
SljitNativeRegionKernel::EnsurePerfectHashJoinProbeCode(ExecutionRegionRuntime &runtime,
                                                        SljitExecutableHashJoinProbe &probe,
                                                        bool prefer_identity_selection, bool direct_consumer) {
	SljitPerfectHashJoinProbeCodegenConfig config;
	config.emit_match_selection = !prefer_identity_selection;
	config.emit_build_selection = !direct_consumer;
	return EnsureLazyHashJoinProbeCode(runtime,
	                                   probe.perfect.SelectionKernel(prefer_identity_selection, direct_consumer),
	                                   "SLJIT native perfect hash join probe lazy code generation failed",
	                                   [&](SljitNativePerfectHashJoinProbeFunction &function, string &error) {
		                                   return BuildSljitPerfectHashJoinProbe(probe.plan, function, error, config);
	                                   });
}

SljitNativeRegularHashJoinProbeFunction
SljitNativeRegionKernel::EnsureRegularHashJoinProbeCode(ExecutionRegionRuntime &runtime,
                                                        SljitExecutableHashJoinProbe &probe, bool uses_bloom_filter,
                                                        SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto key = SljitHashJoinProbeSpecializationKey::General(uses_bloom_filter, mark_selection_mode);
	auto &specialization = probe.regular.Specialization(key);
	SljitHashJoinProbeCodegenConfig config;
	config.uses_bloom_filter = uses_bloom_filter;
	config.mark_selection_mode = mark_selection_mode;
	return EnsureLazyHashJoinProbeCode(runtime, specialization,
	                                   "SLJIT native hash join probe lazy code generation failed",
	                                   [&](SljitNativeRegularHashJoinProbeFunction &function, string &error) {
		                                   return BuildSljitRegularHashJoinProbe(probe.plan, function, error, config);
	                                   });
}

SljitNativeRegularHashJoinProbeFunction SljitNativeRegionKernel::EnsureAllValidRegularHashJoinProbeCode(
    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe,
    const SljitHashJoinProbeAllValidSpecializationKey &key, SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto specialization_key = SljitHashJoinProbeSpecializationKey::AllValid(key, mark_selection_mode);
	auto &specialization = probe.regular.Specialization(specialization_key);
	auto config = SljitHashJoinProbeCodegenConfig::ForAllValidSpecialization(key);
	config.mark_selection_mode = mark_selection_mode;
	auto failure_message = string("SLJIT native ") + (key.selected ? "selected" : "flat") +
	                       (mark_selection_mode == SljitHashJoinMarkSelectionMode::NONE
	                            ? " all-valid hash join probe codegen failed"
	                            : (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES
	                                   ? " all-valid MARK-match hash join probe codegen failed"
	                                   : " all-valid MARK-nonmatch hash join probe codegen failed"));
	return EnsureLazyHashJoinProbeCode(runtime, specialization, failure_message,
	                                   [&](SljitNativeRegularHashJoinProbeFunction &function, string &error) {
		                                   return BuildSljitRegularHashJoinProbe(probe.plan, function, error, config);
	                                   });
}

unique_ptr<ExecutionRegionLocalState> SljitNativeRegionKernel::CreateLocalState(Allocator &allocator) const {
	return make_uniq<SljitNativeRegionLocalState>(allocator, artifact->ops, exact_source_filter_bindings);
}

bool SljitNativeRegionKernel::SupportsChunkStep() const {
	return artifact->full_pipeline_recipe_plan.SupportsChunkStep();
}

//! A native-delegated join probe whose runtime state is not ready (external-partitioned table layout, unfinalized
//! build) leaves the pipeline to the vectorized loop: the executor asks before its first fetch, where nothing is in
//! flight
bool SljitNativeRegionKernel::EntryReady(ExecutionRegionRuntime &runtime, string &blocker) {
	for (auto &op : artifact->ops) {
		if (op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			continue;
		}
		auto readiness =
		    runtime.ExecutionOperators().GetOperatorReadiness(op.operator_index, op.hash_join_probe.plan.operator_info);
		if (readiness.status == ExecutionOperatorReadinessStatus::NOT_READY) {
			blocker = readiness.blocker.empty() ? string("native operator not ready") : readiness.blocker;
			return false;
		}
	}
	return true;
}

unique_ptr<ExecutionRegionChunkStep> SljitNativeRegionKernel::CreateChunkStep(ExecutionRegionRuntime &runtime) {
	auto &local_state = runtime.LocalState().Cast<SljitNativeRegionLocalState>();
	auto &recipe_plan = artifact->full_pipeline_recipe_plan;
	if (!recipe_plan.HasRecipe()) {
		runtime.RecordJitRuntimePath(recipe_plan.NativeOnlyRuntimePath().c_str());
		return make_uniq<SljitNativeOnlyChunkStep<SljitNativeRegionKernel>>(*this, runtime, artifact->ops,
		                                                                    local_state.scratch);
	}
	return make_uniq<SljitPrimitiveRecipeChunkStep<SljitNativeRegionKernel>>(
	    *this, runtime, artifact->ops, recipe_plan.Recipe(), artifact->source_distinct_counts,
	    artifact->source_min_values, artifact->source_max_values, shared_predicate_classifications, local_state.scratch,
	    local_state.terminal);
}

void SljitNativeRegionKernel::RecordExecutedFilterRows(ExecutionRegionRuntime &runtime) const {
	auto filter_rows = executed_filter_rows.exchange(0, std::memory_order_relaxed);
	if (filter_rows > 0) {
		runtime.RecordJitRuntimePath("source.storage_scan.compiled_filter", filter_rows);
		runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK, filter_rows);
	}
}

bool SljitNativeRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
	auto &local_state = runtime.LocalState().Cast<SljitNativeRegionLocalState>();
	auto executed = SljitTryExecuteAggregateStateSourceRecipe(
	    *this, runtime, result, artifact->ops, artifact->source_distinct_counts, artifact->source_min_values,
	    artifact->source_max_values, artifact->full_pipeline_recipe_plan, shared_predicate_classifications,
	    local_state.scratch, local_state.terminal);
	RecordExecutedFilterRows(runtime);
	return executed;
}

} // namespace duckdb

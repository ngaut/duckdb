//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_pipeline_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_kernel.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_full_pipeline_dispatch_runtime.hpp"
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
	SljitNativeRegionLocalState(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops)
	    : scratch(allocator, ops) {
	}

	SljitRegionExecutionScratch scratch;
	// Mutable terminal strategies and staged fallible group transforms stay below
	// this execution-local boundary.
	SljitFullPipelineTerminalRuntimeState terminal;
};

//! Entry prologue for genuine native-fallback states: a native-delegated join
//! probe whose runtime state is not ready (external-partitioned table layout,
//! unfinalized build) defers to the vectorized continuation BEFORE any source
//! fetch, where a handoff is trivially legal. After the first fetch the runtime
//! rejects deferral outright, so not-ready states discovered any later fail
//! loudly instead of abandoning in-flight rows.
static bool SljitTryDeferNotReadyNativeJoinProbesAtEntry(ExecutionRegionRuntime &runtime,
                                                         vector<SljitExecutableRegionOp> &ops,
                                                         ExecutionRegionResult &result) {
	for (auto &op : ops) {
		const ExecutionRegionOperatorInfo *operator_info = nullptr;
		if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			operator_info = &op.hash_join_probe.plan.operator_info;
		} else if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
			operator_info = &op.nested_loop_join_probe.plan.operator_info;
		}
		if (!operator_info) {
			continue;
		}
		auto readiness = runtime.ExecutionOperators().GetOperatorReadiness(op.operator_index, *operator_info);
		if (readiness.status != ExecutionOperatorReadinessStatus::NOT_READY) {
			continue;
		}
		runtime.Defer(readiness.blocker.empty() ? string("native operator not ready at kernel entry")
		                                        : readiness.blocker);
		result = ExecutionRegionResult::DEFERRED;
		return true;
	}
	return false;
}

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
	return make_uniq<SljitNativeRegionLocalState>(allocator, artifact->ops);
}

bool SljitNativeRegionKernel::SupportsRunnerHandoff() const {
	if (!artifact->full_pipeline_recipe_plan.HasRecipe()) {
		return true;
	}
	if (artifact->full_pipeline_recipe_plan.Recipe().UsesPrimitiveAggregateStateSource()) {
		return false;
	}
	auto &sequence = artifact->full_pipeline_recipe_plan.Recipe().primitive_sequence;
	for (idx_t step_idx = 0; step_idx < sequence.Count(); step_idx++) {
		if (!SljitFullPipelinePrimitiveStepSupportsRunnerHandoff(sequence.Step(step_idx))) {
			return false;
		}
	}
	return true;
}

bool SljitNativeRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
	if (runtime.CanDeferAtEntry() && SljitTryDeferNotReadyNativeJoinProbesAtEntry(runtime, artifact->ops, result)) {
		return true;
	}
	auto &local_state = runtime.LocalState().Cast<SljitNativeRegionLocalState>();
	auto executed = SljitTryExecuteFullPipelineRecipe(
	    *this, runtime, result, artifact->ops, artifact->source_distinct_counts, artifact->source_min_values,
	    artifact->source_max_values, artifact->full_pipeline_recipe_plan, shared_predicate_classifications,
	    local_state.scratch, local_state.terminal);
	auto filter_rows = executed_filter_rows.exchange(0, std::memory_order_relaxed);
	if (filter_rows > 0) {
		runtime.RecordJitRuntimePath("source.storage_scan.compiled_filter", filter_rows);
		runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK, filter_rows);
	}
	return executed;
}

} // namespace duckdb

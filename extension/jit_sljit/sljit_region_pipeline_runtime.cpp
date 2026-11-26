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
	                            vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings,
	                            const vector<SljitAggregateInstanceParameters> &aggregate_parameters,
	                            const vector<SljitCastErrorIdentity> &cast_errors_p)
	    : scratch(allocator, ops, std::move(exact_source_filter_bindings), aggregate_parameters),
	      cast_errors(cast_errors_p) {
	}

	SljitRegionExecutionScratch scratch;
	// Mutable terminal strategies and staged fallible group transforms stay below
	// this execution-local boundary.
	SljitFullPipelineTerminalRuntimeState terminal;
	const vector<SljitCastErrorIdentity> &cast_errors;
};

const vector<SljitCastErrorIdentity> &SljitCastErrorBindings(ExecutionRegionRuntime &runtime) {
	return runtime.LocalState().Cast<SljitNativeRegionLocalState>().cast_errors;
}

template <class FUNCTION, class BUILD>
FUNCTION SljitNativeRegionKernel::EnsureLazyHashJoinProbeCode(ExecutionRegionRuntime &runtime,
                                                              SljitLazyCompiledFunction<FUNCTION> &artifact,
                                                              const char *failure_message, BUILD build) {
	return artifact.Ensure([&]() {
		string error;
		FUNCTION function = nullptr;
		unique_ptr<ExecutionRegionCodeHandle> code;
		auto timing = TimeSljitLazyCodegen([&]() { code = build(function, error); });
		if (!code || !function) {
			throw InternalException("%s: %s", failure_message, error.empty() ? "unknown error" : error);
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
	// the message is a literal: the call runs per chunk, and only a failed compile reads it
	const char *failure_message;
	switch (mark_selection_mode) {
	case SljitHashJoinMarkSelectionMode::NONE:
		failure_message = key.selected ? "SLJIT native selected all-valid hash join probe codegen failed"
		                               : "SLJIT native flat all-valid hash join probe codegen failed";
		break;
	case SljitHashJoinMarkSelectionMode::MATCHES:
		failure_message = key.selected ? "SLJIT native selected all-valid MARK-match hash join probe codegen failed"
		                               : "SLJIT native flat all-valid MARK-match hash join probe codegen failed";
		break;
	case SljitHashJoinMarkSelectionMode::NON_MATCHES:
		failure_message = key.selected ? "SLJIT native selected all-valid MARK-nonmatch hash join probe codegen failed"
		                               : "SLJIT native flat all-valid MARK-nonmatch hash join probe codegen failed";
		break;
	default:
		throw InternalException("SLJIT hash join probe has an unknown MARK selection mode");
	}
	return EnsureLazyHashJoinProbeCode(runtime, specialization, failure_message,
	                                   [&](SljitNativeRegularHashJoinProbeFunction &function, string &error) {
		                                   return BuildSljitRegularHashJoinProbe(probe.plan, function, error, config);
	                                   });
}

unique_ptr<ExecutionRegionLocalState> SljitNativeRegionKernel::CreateLocalState(Allocator &allocator) const {
	return make_uniq<SljitNativeRegionLocalState>(allocator, artifact->ops, bindings.exact_source_filters,
	                                              bindings.aggregate_parameters, bindings.cast_errors);
}

bool SljitNativeRegionKernel::SupportsChunkStep() const {
	return artifact->full_pipeline_recipe_plan.SupportsChunkStep();
}

//! A native-delegated join probe whose runtime state is not ready (external-partitioned table layout, unfinalized
//! build) leaves the pipeline to the vectorized loop: the executor asks before its first fetch, where nothing is in
//! flight
bool SljitNativeRegionKernel::EntryReady(ExecutionRegionRuntime &runtime, string &blocker) {
	for (auto &op : artifact->ops) {
		if (op.Kind() != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			continue;
		}
		auto readiness =
		    runtime.ExecutionOperators().GetOperatorReadiness(op.operator_index, op.HashJoinProbe().plan.operator_info);
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
	auto native_continuation = optional_ptr<SljitExecutableNativeContinuation>(artifact->native_continuation.get());
	return make_uniq<SljitPrimitiveRecipeChunkStep<SljitNativeRegionKernel>>(
	    *this, runtime, artifact->ops, recipe_plan.Recipe(), bindings.source_distinct_counts,
	    bindings.source_min_values, bindings.source_max_values, shared_predicate_classifications, local_state.scratch,
	    local_state.terminal, native_continuation);
}

//! A step ending in a generated ungrouped aggregate costs about as much as copying a few hundred rows: the source
//! batches only the chunks under a quarter vector. A step ending anywhere else, such as DuckDB's own sink, pays a cost
//! per call that batching saves up to half a vector
idx_t SljitNativeRegionKernel::MinPassthroughRows() const {
	if (!SupportsChunkStep()) {
		return ExecutionRegionKernel::MinPassthroughRows();
	}
	auto &steps = artifact->full_pipeline_recipe_plan.Recipe().primitive_sequence;
	auto &terminal = steps.Step(steps.Count() - 1);
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	if (terminal.kind == SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE) {
		aggregate_idx = terminal.ungrouped_aggregate_update.aggregate_idx;
	} else if (terminal.kind == SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE) {
		aggregate_idx = terminal.post_join_projection_aggregate.aggregate_idx;
	}
	auto &ops = artifact->ops;
	if (aggregate_idx < ops.size() && ops[aggregate_idx].Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	    ops[aggregate_idx].AggregateUpdate().plan.sink_info.Kind() ==
	        ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return STANDARD_VECTOR_SIZE / 4;
	}
	return ExecutionRegionKernel::MinPassthroughRows();
}

void SljitNativeRegionKernel::RecordExecutedFilterRows(ExecutionRegionRuntime &runtime) const {
	auto filter_rows = executed_filter_rows.exchange(0, std::memory_order_relaxed);
	if (filter_rows > 0) {
		runtime.RecordJitRuntimePath("source.storage_scan.compiled_filter", filter_rows);
		RecordSljitRegionBackendWork(runtime, filter_rows);
	}
	auto compressed_segments = compressed_string_match_segments.exchange(0, std::memory_order_relaxed);
	if (compressed_segments > 0) {
		runtime.RecordJitRuntimePath("source.storage_scan.compressed_string_match_segments", compressed_segments);
	}
}

bool SljitNativeRegionKernel::TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
	auto &local_state = runtime.LocalState().Cast<SljitNativeRegionLocalState>();
	auto executed = SljitTryExecuteAggregateStateSourceRecipe(
	    *this, runtime, result, artifact->ops, bindings.source_distinct_counts, bindings.source_min_values,
	    bindings.source_max_values, artifact->full_pipeline_recipe_plan, shared_predicate_classifications,
	    local_state.scratch, local_state.terminal);
	RecordExecutedFilterRows(runtime);
	return executed;
}

} // namespace duckdb

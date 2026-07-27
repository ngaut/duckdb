//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_runtime.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_full_pipeline_dispatch_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_join_probe_codegen.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <utility>

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

class SljitTableFilterKernelState : public TableFilterKernelState {
public:
	SljitTableFilterKernelState(const SljitExecutableScanFilter &scan_filter_p, atomic<idx_t> &executed_rows_p)
	    : scan_filter(scan_filter_p), executed_rows(executed_rows_p), filter_selection(STANDARD_VECTOR_SIZE),
	      alternate_selection(STANDARD_VECTOR_SIZE) {
		input.InitializeEmpty({scan_filter.input_type});
	}

	bool TrySelect(Vector &vector, UnifiedVectorFormat &format, SelectionVector &selection, idx_t scan_count,
	               idx_t &approved_tuple_count) override {
		(void)format;
		if (!scan_filter.filter || scan_count > STANDARD_VECTOR_SIZE || approved_tuple_count > scan_count) {
			return false;
		}
		if (approved_tuple_count == 0) {
			return true;
		}
		input.data[0].Reference(vector);
		input.SetChildCardinality(scan_count);
		auto input_count = approved_tuple_count;
		const SelectionVector *execute_selection = input_count == scan_count ? nullptr : &selection;
		auto output_selection = &filter_selection;
		if (execute_selection && execute_selection->data() == output_selection->data()) {
			output_selection = &alternate_selection;
		}
		idx_t selected_count;
		auto &filter = *scan_filter.filter;
		if (filter.batch_select) {
			selected_count = SljitSelectNativeStringLikeBatch(*filter.batch_select, vector, *output_selection,
			                                                  execute_selection, input_count);
		} else {
			selected_count = SljitSelectExpression(filter.expression, input, *output_selection, expression_scratch,
			                                       execute_selection, input_count, false);
		}
		if (selected_count < input_count) {
			selection.Initialize(*output_selection);
		}
		approved_tuple_count = selected_count;
		executed_rows.fetch_add(input_count, std::memory_order_relaxed);
		return true;
	}

private:
	const SljitExecutableScanFilter &scan_filter;
	atomic<idx_t> &executed_rows;
	DataChunk input;
	SelectionVector filter_selection;
	SelectionVector alternate_selection;
	SljitExpressionAdapterScratch expression_scratch;
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

//! Backend-owned artifact shared by database-local plans with identical
//! semantic and compile-affecting binding keys. Eager code handles and
//! concurrency-safe lazy specialization cells live here; counters and adaptive
//! policy remain on each kernel wrapper.
struct SljitNativeRegionArtifact : public ExecutionRegionArtifact {
	SljitNativeRegionArtifact(string backend_name_p, vector<SljitExecutableRegionOp> ops_p,
	                          vector<SljitExecutableScanFilter> scan_filters_p, bool uses_scan_filters_p,
	                          vector<idx_t> source_distinct_counts_p, vector<Value> source_min_values_p,
	                          vector<Value> source_max_values_p, SljitFullPipelineRecipePlan recipe_plan_p,
	                          ExecutionRegionABI abi_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)), scan_filters(std::move(scan_filters_p)),
	      uses_scan_filters(uses_scan_filters_p), source_distinct_counts(std::move(source_distinct_counts_p)),
	      source_min_values(std::move(source_min_values_p)), source_max_values(std::move(source_max_values_p)),
	      full_pipeline_recipe_plan(std::move(recipe_plan_p)), abi(abi_p) {
	}

	string backend_name;
	//! Lazy code cells are synchronized and publish monotonically. The semantic
	//! operation graph itself is immutable after artifact construction.
	mutable vector<SljitExecutableRegionOp> ops;
	vector<SljitExecutableScanFilter> scan_filters;
	bool uses_scan_filters;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	SljitFullPipelineRecipePlan full_pipeline_recipe_plan;
	ExecutionRegionABI abi;
};

class SljitNativeRegionKernel : public ExecutionRegionKernel {
public:
	SljitNativeRegionKernel(shared_ptr<const SljitNativeRegionArtifact> artifact_p,
	                        vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings_p)
	    : artifact(std::move(artifact_p)), exact_source_filter_bindings(std::move(exact_source_filter_bindings_p)),
	      shared_predicate_classifications(artifact->ops.size()) {
	}

	const string &BackendName() const override {
		return artifact->backend_name;
	}

	idx_t CodeSize() const override {
		idx_t result = 0;
		for (auto &op : artifact->ops) {
			result += op.CodeSize();
		}
		for (auto &scan_filter : artifact->scan_filters) {
			result += scan_filter.CodeSize();
		}
		return result;
	}

	bool HasTableFilterKernels() const override {
		return !artifact->scan_filters.empty();
	}

	bool HasTableFilterKernel(idx_t filter_index) const override {
		for (auto &scan_filter : artifact->scan_filters) {
			if (scan_filter.filter_index == filter_index) {
				return true;
			}
		}
		return false;
	}

	unique_ptr<TableFilterKernelState> CreateTableFilterKernelState(idx_t filter_index) const override {
		for (auto &scan_filter : artifact->scan_filters) {
			if (scan_filter.filter_index == filter_index) {
				return make_uniq<SljitTableFilterKernelState>(scan_filter, executed_filter_rows);
			}
		}
		return nullptr;
	}

	bool HasExecutableBody() const override {
		for (auto &op : artifact->ops) {
			if (op.HasExecutableBody()) {
				return true;
			}
		}
		return artifact->uses_scan_filters && artifact->full_pipeline_recipe_plan.HasRecipe() &&
		       artifact->full_pipeline_recipe_plan.Recipe().has_scan_filter_executable_body;
	}

	unique_ptr<ExecutionRegionLocalState> CreateLocalState(Allocator &allocator) const override {
		return make_uniq<SljitNativeRegionLocalState>(allocator, artifact->ops);
	}

	bool SupportsRunnerHandoff() const override {
		if (!artifact->full_pipeline_recipe_plan.HasRecipe()) {
			return true;
		}
		auto &sequence = artifact->full_pipeline_recipe_plan.Recipe().primitive_sequence;
		for (idx_t step_idx = 0; step_idx < sequence.Count(); step_idx++) {
			if (!SljitFullPipelinePrimitiveStepSupportsRunnerHandoff(sequence.Step(step_idx))) {
				return false;
			}
		}
		return true;
	}

	bool CanExecuteFullPipeline() const override {
		return ExecutionRegionABIIsFullPipeline(artifact->abi);
	}

	const shared_ptr<ExecutionRuntimeFilterIdentity> &ExactSourceFilterIdentity(idx_t binding) const {
		static const shared_ptr<ExecutionRuntimeFilterIdentity> no_identity;
		return binding < exact_source_filter_bindings.size() ? exact_source_filter_bindings[binding] : no_identity;
	}

	void RecordLazyHashJoinProbeCodegen(ExecutionRegionRuntime &runtime, const SljitLazyCodegenTiming &timing,
	                                    idx_t code_size) {
		ExecutionRegionLazyCodegenMetrics metrics;
		metrics.codegen_time_us = timing.codegen_time_us;
		metrics.machine_codegen_time_us = timing.machine_codegen_time_us;
		metrics.code_size = code_size;
		runtime.RecordLazyCodegen(metrics);
	}

	template <class FUNCTION, class BUILD>
	FUNCTION EnsureLazyHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitLazyCompiledFunction<FUNCTION> &artifact,
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
			RecordLazyHashJoinProbeCodegen(runtime, timing, code_size);
			return SljitCompiledFunction<FUNCTION>(std::move(code), function);
		});
	}

	SljitNativePerfectHashJoinProbeFunction EnsurePerfectHashJoinProbeCode(ExecutionRegionRuntime &runtime,
	                                                                       SljitExecutableHashJoinProbe &probe,
	                                                                       bool prefer_identity_selection,
	                                                                       bool direct_consumer = false) {
		SljitPerfectHashJoinProbeCodegenConfig config;
		config.emit_match_selection = !prefer_identity_selection;
		config.emit_build_selection = !direct_consumer;
		return EnsureLazyHashJoinProbeCode(
		    runtime, probe.perfect.SelectionKernel(prefer_identity_selection, direct_consumer),
		    "SLJIT native perfect hash join probe lazy code generation failed",
		    [&](SljitNativePerfectHashJoinProbeFunction &function, string &error) {
			    return BuildSljitPerfectHashJoinProbe(probe.plan, function, error, config);
		    });
	}

	SljitNativeRegularHashJoinProbeFunction EnsureRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe, bool uses_bloom_filter = false,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE) {
		auto key = SljitHashJoinProbeSpecializationKey::General(uses_bloom_filter, mark_selection_mode);
		auto &specialization = probe.regular.Specialization(key);
		SljitHashJoinProbeCodegenConfig config;
		config.uses_bloom_filter = uses_bloom_filter;
		config.mark_selection_mode = mark_selection_mode;
		return EnsureLazyHashJoinProbeCode(
		    runtime, specialization, "SLJIT native hash join probe lazy code generation failed",
		    [&](SljitNativeRegularHashJoinProbeFunction &function, string &error) {
			    return BuildSljitRegularHashJoinProbe(probe.plan, function, error, config);
		    });
	}

	SljitNativeRegularHashJoinProbeFunction EnsureAllValidRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe,
	    const SljitHashJoinProbeAllValidSpecializationKey &key,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE) {
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
			                                   return BuildSljitRegularHashJoinProbe(probe.plan, function, error,
			                                                                         config);
		                                   });
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) override {
		if (!ExecutionRegionABIIsFullPipeline(artifact->abi)) {
			throw InternalException("SLJIT full pipeline kernel entered without full-pipeline ABI");
		}
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

private:
	shared_ptr<const SljitNativeRegionArtifact> artifact;
	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings;
	vector<SljitSharedPerfectHashPredicateClassificationCache> shared_predicate_classifications;
	mutable atomic<idx_t> executed_filter_rows {0};
};

shared_ptr<const ExecutionRegionArtifact> CreateSljitNativeRegionArtifact(string backend_name,
                                                                          SljitExecutableRegion &&region,
                                                                          SljitFullPipelineRecipePlan recipe_plan,
                                                                          ExecutionRegionABI abi) {
	return make_shared_ptr<SljitNativeRegionArtifact>(
	    std::move(backend_name), std::move(region.ops), std::move(region.scan_filters), region.uses_scan_filters,
	    std::move(region.source_distinct_counts), std::move(region.source_min_values),
	    std::move(region.source_max_values), std::move(recipe_plan), abi);
}

unique_ptr<ExecutionRegionKernel>
InstantiateSljitNativeRegionArtifact(const shared_ptr<const ExecutionRegionArtifact> &artifact,
                                     const ExecutionRegionCompilationInput &input) {
	auto sljit_artifact_ptr = dynamic_cast<const SljitNativeRegionArtifact *>(artifact.get());
	if (!sljit_artifact_ptr || !input.lowering_plan || !input.lowering_plan->backend_plan) {
		return nullptr;
	}
	shared_ptr<const SljitNativeRegionArtifact> sljit_artifact(artifact, sljit_artifact_ptr);
	auto sljit_plan = dynamic_cast<const SljitRegionBackendPlan *>(input.lowering_plan->backend_plan.get());
	if (!sljit_plan || !sljit_plan->native_region) {
		return nullptr;
	}
	vector<shared_ptr<ExecutionRuntimeFilterIdentity>> exact_source_filter_bindings(sljit_artifact->ops.size());
	for (auto &current_op : sljit_plan->native_region->ops) {
		if (current_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			continue;
		}
		auto binding = current_op.hash_join_probe.exact_source_filter_binding;
		if (binding == DConstants::INVALID_INDEX) {
			continue;
		}
		if (binding >= exact_source_filter_bindings.size() ||
		    !current_op.hash_join_probe.exact_source_filter_identity) {
			return nullptr;
		}
		exact_source_filter_bindings[binding] = current_op.hash_join_probe.exact_source_filter_identity;
	}
	return make_uniq<SljitNativeRegionKernel>(std::move(sljit_artifact), std::move(exact_source_filter_bindings));
}

} // namespace duckdb

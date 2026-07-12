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
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_join_probe_codegen.hpp"
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

class SljitNativeRegionKernel : public ExecutionRegionKernel {
public:
	SljitNativeRegionKernel(string backend_name_p, vector<SljitExecutableRegionOp> ops_p, bool uses_scan_filters_p,
	                        vector<LogicalType> source_output_types_p, vector<idx_t> source_distinct_counts_p,
	                        vector<Value> source_min_values_p, vector<Value> source_max_values_p,
	                        ExecutionRegionABI abi_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)), uses_scan_filters(uses_scan_filters_p),
	      source_output_types(std::move(source_output_types_p)),
	      source_distinct_counts(std::move(source_distinct_counts_p)),
	      source_min_values(std::move(source_min_values_p)), source_max_values(std::move(source_max_values_p)),
	      full_pipeline_recipe_plan(
	          BuildSljitFullPipelineRecipePlan(ops, source_output_types, source_min_values, source_max_values)),
	      abi(abi_p) {
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		idx_t result = 0;
		for (auto &op : ops) {
			result += op.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const override {
		for (auto &op : ops) {
			if (op.HasExecutableBody()) {
				return true;
			}
		}
		return uses_scan_filters && full_pipeline_recipe_plan.has_recipe &&
		       (SljitFullPipelineHasDirectSourceHashBuild(full_pipeline_recipe_plan.recipe.primitive_sequence) ||
		        SljitFullPipelineHasExactFilterProbeHashBuild(ops,
		                                                      full_pipeline_recipe_plan.recipe.primitive_sequence));
	}

	bool CanExecuteFullPipeline() const override {
		return ExecutionRegionABIIsFullPipeline(abi);
	}

	void RecordLazyHashJoinProbeCodegen(ExecutionRegionRuntime &runtime, const SljitLazyCodegenTiming &timing,
	                                    idx_t code_size) {
		ExecutionRegionLazyCodegenMetrics metrics;
		metrics.codegen_time_us = timing.codegen_time_us;
		metrics.machine_codegen_time_us = timing.machine_codegen_time_us;
		metrics.code_size = code_size;
		runtime.RecordLazyCodegen(metrics);
		AddTraceCodeSize(code_size);
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
	                                                                       SljitExecutableHashJoinProbe &probe) {
		return EnsureLazyHashJoinProbeCode(runtime, probe.perfect.compiled,
		                                   "SLJIT native perfect hash join probe lazy code generation failed",
		                                   [&](SljitNativePerfectHashJoinProbeFunction &function, string &error) {
			                                   return BuildSljitPerfectHashJoinProbe(probe.plan, function, error);
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
		if (!ExecutionRegionABIIsFullPipeline(abi)) {
			throw InternalException("SLJIT full pipeline kernel entered without full-pipeline ABI");
		}
		return SljitTryExecuteFullPipelineRecipe(*this, runtime, result, ops, source_distinct_counts, source_min_values,
		                                         source_max_values, full_pipeline_recipe_plan);
	}

private:
	string backend_name;
	vector<SljitExecutableRegionOp> ops;
	bool uses_scan_filters;
	vector<LogicalType> source_output_types;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	SljitFullPipelineRecipePlan full_pipeline_recipe_plan;
	ExecutionRegionABI abi;
};

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                                SljitExecutableRegion &&region,
                                                                ExecutionRegionABI abi) {
	(void)context;
	return make_uniq<SljitNativeRegionKernel>(
	    std::move(backend_name), std::move(region.ops), region.uses_scan_filters, std::move(region.source_output_types),
	    std::move(region.source_distinct_counts), std::move(region.source_min_values),
	    std::move(region.source_max_values), abi);
}

} // namespace duckdb

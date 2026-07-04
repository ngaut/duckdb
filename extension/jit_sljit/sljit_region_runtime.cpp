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
#include "sljit_hash_join_probe_executor_runtime.hpp"
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
	SljitNativeRegionKernel(string backend_name_p, vector<SljitExecutableRegionOp> ops_p,
	                        vector<idx_t> source_distinct_counts_p, vector<Value> source_min_values_p,
	                        vector<Value> source_max_values_p, ExecutionRegionABI abi_p, bool uses_scan_filters_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)),
	      source_distinct_counts(std::move(source_distinct_counts_p)),
	      source_min_values(std::move(source_min_values_p)), source_max_values(std::move(source_max_values_p)),
	      full_pipeline_recipe_plan(
	          BuildSljitFullPipelineRecipePlan(ops, source_min_values, source_max_values, uses_scan_filters_p)),
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
		return false;
	}

	bool CanExecuteFullPipeline() const override {
		return ExecutionRegionABIIsFullPipeline(abi);
	}

	void RefreshTraceCodeSize() {
		SetTraceInfo(TraceId(), ExecutionMode(), TraceCompileReason(), TraceCompileTime(), CodeSize());
	}

	void RecordLazyHashJoinProbeCodegen(ExecutionRegionRuntime &runtime, const SljitLazyCodegenTiming &timing,
	                                    idx_t code_size) {
		ExecutionRegionLazyCodegenMetrics metrics;
		metrics.codegen_time_us = timing.codegen_time_us;
		metrics.machine_codegen_time_us = timing.machine_codegen_time_us;
		metrics.code_size = code_size;
		runtime.RecordLazyCodegen(metrics);
		RefreshTraceCodeSize();
	}

	template <class FUNCTION, class BUILD>
	void EnsureLazyHashJoinProbeCode(ExecutionRegionRuntime &runtime, unique_ptr<ExecutionRegionCodeHandle> &code,
	                                 FUNCTION &function, const string &failure_message, BUILD build) {
		if (function) {
			return;
		}
		lock_guard<mutex> guard(codegen_lock);
		if (function) {
			return;
		}
		string error;
		auto timing = TimeSljitLazyCodegen([&]() { code = build(error); });
		if (!code || !function) {
			throw InternalException("%s: %s", failure_message.c_str(), error.empty() ? "unknown error" : error);
		}
		RecordLazyHashJoinProbeCodegen(runtime, timing, code->CodeSize());
	}

	void EnsurePerfectHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe) {
		auto &perfect = probe.perfect;
		if (perfect.function) {
			return;
		}
		EnsureLazyHashJoinProbeCode(
		    runtime, perfect.code, perfect.function, "SLJIT native perfect hash join probe lazy code generation failed",
		    [&](string &error) { return BuildSljitPerfectHashJoinProbe(probe.plan, perfect.function, error); });
	}

	void EnsureRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe, bool uses_bloom_filter = false,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE) {
		auto &regular = probe.regular;
		unique_ptr<ExecutionRegionCodeHandle> *code_ptr;
		SljitNativeRegularHashJoinProbeFunction *function_ptr;
		switch (mark_selection_mode) {
		case SljitHashJoinMarkSelectionMode::NONE:
			code_ptr = uses_bloom_filter ? &regular.bloom_code : &regular.code;
			function_ptr = uses_bloom_filter ? &regular.bloom_function : &regular.function;
			break;
		case SljitHashJoinMarkSelectionMode::MATCHES:
			code_ptr =
			    uses_bloom_filter ? &regular.mark_match_selection_bloom_code : &regular.mark_match_selection_code;
			function_ptr = uses_bloom_filter ? &regular.mark_match_selection_bloom_function
			                                 : &regular.mark_match_selection_function;
			break;
		case SljitHashJoinMarkSelectionMode::NON_MATCHES:
			code_ptr =
			    uses_bloom_filter ? &regular.mark_nonmatch_selection_bloom_code : &regular.mark_nonmatch_selection_code;
			function_ptr = uses_bloom_filter ? &regular.mark_nonmatch_selection_bloom_function
			                                 : &regular.mark_nonmatch_selection_function;
			break;
		default:
			throw InternalException("Unsupported SLJIT MARK selection mode");
		}
		SljitHashJoinProbeCodegenConfig config;
		config.uses_bloom_filter = uses_bloom_filter;
		config.mark_selection_mode = mark_selection_mode;
		EnsureLazyHashJoinProbeCode(
		    runtime, *code_ptr, *function_ptr, "SLJIT native hash join probe lazy code generation failed",
		    [&](string &error) { return BuildSljitRegularHashJoinProbe(probe.plan, *function_ptr, error, config); });
	}

	SljitNativeRegularHashJoinProbeFunction EnsureAllValidRegularHashJoinProbeCode(
	    ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe,
	    const SljitHashJoinProbeAllValidSpecializationKey &key,
	    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE) {
		SljitExecutableRegularHashJoinProbeCode::AllValidSpecialization *specialization_ptr;
		switch (mark_selection_mode) {
		case SljitHashJoinMarkSelectionMode::NONE:
			specialization_ptr = &probe.regular.AllValidSpecializationFor(key);
			break;
		case SljitHashJoinMarkSelectionMode::MATCHES:
			specialization_ptr = &probe.regular.MarkMatchAllValidSpecializationFor(key);
			break;
		case SljitHashJoinMarkSelectionMode::NON_MATCHES:
			specialization_ptr = &probe.regular.MarkNonMatchAllValidSpecializationFor(key);
			break;
		default:
			throw InternalException("Unsupported SLJIT all-valid MARK selection mode");
		}
		auto &specialization = *specialization_ptr;
		if (specialization.function) {
			return specialization.function;
		}
		auto config = SljitHashJoinProbeCodegenConfig::ForAllValidSpecialization(key);
		config.mark_selection_mode = mark_selection_mode;
		auto failure_message = string("SLJIT native ") + (key.selected ? "selected" : "flat") +
		                       (mark_selection_mode == SljitHashJoinMarkSelectionMode::NONE
		                            ? " all-valid hash join probe codegen failed"
		                            : (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES
		                                   ? " all-valid MARK-match hash join probe codegen failed"
		                                   : " all-valid MARK-nonmatch hash join probe codegen failed"));
		EnsureLazyHashJoinProbeCode(
		    runtime, specialization.code, specialization.function, failure_message, [&](string &error) {
			    return BuildSljitRegularHashJoinProbe(probe.plan, specialization.function, error, config);
		    });
		return specialization.function;
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
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	SljitFullPipelineRecipePlan full_pipeline_recipe_plan;
	ExecutionRegionABI abi;
	mutex codegen_lock;
};

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                                SljitExecutableRegion &&region, ExecutionRegionABI abi,
                                                                bool uses_scan_filters) {
	(void)context;
	return make_uniq<SljitNativeRegionKernel>(
	    std::move(backend_name), std::move(region.ops), std::move(region.source_distinct_counts),
	    std::move(region.source_min_values), std::move(region.source_max_values), abi, uses_scan_filters);
}

} // namespace duckdb

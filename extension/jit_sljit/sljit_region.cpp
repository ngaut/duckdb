//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_util.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_region_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_settings.hpp"

#include <chrono>

namespace duckdb {

static int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static string AttachCoreRegionIR(string backend_ir, const ExecutionRegionIR &region_ir) {
	return std::move(backend_ir) + ";core=(" + region_ir.ir + ")";
}

ExecutionRegionLoweringPlan AnalyzeSljitRegion(const ExecutionRegionCompilationInput &input) {
	return BuildSljitRegionPlan(input.region_ir, input.candidate,
	                            ExecutionRegionSettings::ShouldRecordDetailedTelemetry(input.context));
}

ExecutionRegionCompileResult CompileSljitRegion(const string &backend_name,
                                                const ExecutionRegionCompilationInput &input) {
	if (!input.lowering_plan || !input.lowering_plan->backend_plan) {
		throw InternalException("SLJIT region compile requires an analyzed backend region plan");
	}
	auto sljit_plan = dynamic_cast<const SljitRegionBackendPlan *>(input.lowering_plan->backend_plan.get());
	if (!sljit_plan) {
		throw InternalException("SLJIT region compile received a backend plan from another execution region backend");
	}
	const auto &lowering_plan = *input.lowering_plan;
	auto reason = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(input.context)
	                  ? lowering_plan.EventReason()
	                  : lowering_plan.CompactEventReason();
	auto native_region = sljit_plan->native_region.get();
	auto error = sljit_plan->error;
	auto execution_mode = lowering_plan.ExpectedCompiledExecutionMode();
	auto &contract = input.candidate.contract;
	if (!ExecutionRegionExecutionModeIsCompiled(execution_mode)) {
		throw InternalException("SLJIT region compile requires compiled execution mode");
	}
	if (native_region) {
		if (ExecutionRegionABIIsFullPipeline(contract.abi) && !native_region->UsesSourceContract()) {
			return ExecutionRegionCompileResult::Error("SLJIT full-pipeline native regions require a source contract");
		}
		if (native_region->UsesSourceContract()) {
			reason += ";source-execution:source-contract";
		}
		if (!native_region->summary.generates_machine_code) {
			throw InternalException("SLJIT compiled region has no executable body classification");
		}
		auto shape = DescribeNativeRegionShape(*native_region);
		string ir;
		if (ExecutionRegionSettings::DumpIR(input.context)) {
			ir = AttachCoreRegionIR(DescribeNativeRegion(*native_region, "native.region"), input.region_ir);
		}
		ExecutionRegionCompileTimings timings;
		SljitExecutableRegion executable_region;
		auto executable_build_start = std::chrono::steady_clock::now();
		{
			SljitCodegenTimingScope codegen_timing_scope(&timings);
			if (!BuildSljitExecutableRegion(*native_region, executable_region, error)) {
				timings.executable_build_time_us = SljitRegionElapsedMicros(executable_build_start);
				auto result = ExecutionRegionCompileResult::Error(std::move(error));
				result.timings = timings;
				return result;
			}
		}
		timings.executable_build_time_us = SljitRegionElapsedMicros(executable_build_start);
		if (executable_region.ops.empty()) {
			throw InternalException(
			    "SLJIT compiled region reached code generation without executable region operators");
		}
		if (execution_mode != ExecutionRegionExecutionMode::NATIVE) {
			throw InternalException("SLJIT executable mode native does not match analyzed mode %s",
			                        ExecutionRegionExecutionModeToString(execution_mode));
		}
		reason += ";execution:native-sljit-region-" + shape;
		if (ExecutionRegionSettings::Verify(input.context)) {
			reason += ";verify:region";
		}
		auto kernel_build_start = std::chrono::steady_clock::now();
		auto kernel =
		    CreateSljitNativeRegionKernel(input.context, backend_name, std::move(executable_region), contract.abi);
		timings.kernel_build_time_us = SljitRegionElapsedMicros(kernel_build_start);
		auto result = ExecutionRegionCompileResult::Compiled(std::move(kernel), execution_mode, std::move(reason),
		                                                     MaybeDumpIr(input.context, std::move(ir)));
		result.timings = timings;
		return result;
	}
	if (!error.empty()) {
		return ExecutionRegionCompileResult::Error(std::move(error));
	}
	reason += ";execution:unsupported";
	auto result = ExecutionRegionCompileResult::Unsupported(std::move(reason));
	result.ir = input.region_ir.ir;
	return result;
}

} // namespace duckdb

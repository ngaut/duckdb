//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_util.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_region_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static string AttachCoreRegionIR(string backend_ir, const JitRegionIR &region_ir) {
	return std::move(backend_ir) + ";core=(" + region_ir.ir + ")";
}

JitRegionLoweringPlan AnalyzeSljitRegion(const JitRegionCompilationInput &input) {
	return BuildSljitRegionPlan(input.region_ir, input.candidate).lowering_plan;
}

JitRegionCompileResult CompileSljitRegion(const string &backend_name, const JitRegionCompilationInput &input) {
	if (!input.lowering_plan || !input.lowering_plan->backend_plan) {
		throw InternalException("SLJIT region compile requires an analyzed backend region plan");
	}
	auto sljit_plan = dynamic_cast<const SljitRegionBackendPlan *>(input.lowering_plan->backend_plan.get());
	if (!sljit_plan) {
		throw InternalException("SLJIT region compile received a backend plan from another JIT backend");
	}
	const auto &lowering_plan = *input.lowering_plan;
	auto reason = lowering_plan.EventReason();
	auto native_region = sljit_plan->native_region ? CopySljitNativeRegion(*sljit_plan->native_region) : nullptr;
	auto error = sljit_plan->error;
	auto execution_mode = lowering_plan.ExpectedCompiledExecutionMode();
	auto &contract = input.candidate.contract;
	if (execution_mode != JitExecutionMode::NATIVE) {
		throw InternalException("SLJIT region compile requires native compiled execution mode");
	}
	if (native_region) {
		if (JitRegionABIIsFullPipeline(contract.abi) && !native_region->native_source) {
			return JitRegionCompileResult::Error(
			    "SLJIT full-pipeline native regions require a native-source protocol");
		}
		if (native_region->elided_identity_projections > 0) {
			reason += ";elided:identity-projection=" + std::to_string(native_region->elided_identity_projections);
		}
		if (native_region->fused_projection_chains > 0) {
			reason += ";fused:projection-chain=" + std::to_string(native_region->fused_projection_chains);
		}
		if (native_region->fused_arithmetic_projection_chains > 0) {
			reason += ";fused:arithmetic-projection-chain=" +
			          std::to_string(native_region->fused_arithmetic_projection_chains);
		}
		if (native_region->runtime_combined_filter_projections > 0) {
			reason += ";runtime-fused:filter-projection=" +
			          std::to_string(native_region->runtime_combined_filter_projections);
		}
		if (native_region->native_source) {
			reason += ";source-execution:native-source";
		}
		SljitExecutableRegion executable_region;
		if (!BuildSljitExecutableRegion(*native_region, executable_region, error)) {
			return JitRegionCompileResult::Error(std::move(error));
		}
		auto shape = DescribeNativeRegionShape(*native_region);
		auto ir = AttachCoreRegionIR(DescribeNativeRegion(*native_region, "native.region"), input.region_ir);
		reason += ";execution:native-sljit-region-" + shape;
		if (Settings::Get<JitVerifySetting>(input.context)) {
			reason += ";verify:region";
		}
		return JitRegionCompileResult::Compiled(
		    CreateSljitNativeRegionKernel(input.context, backend_name, std::move(executable_region), contract.abi,
		                                  native_region->native_source),
		    execution_mode, std::move(reason), MaybeDumpIr(input.context, std::move(ir)));
	}
	if (!error.empty()) {
		return JitRegionCompileResult::Error(std::move(error));
	}
	reason += ";execution:unsupported";
	auto result = JitRegionCompileResult::Unsupported(std::move(reason));
	result.ir = input.region_ir.ir;
	return result;
}

} // namespace duckdb

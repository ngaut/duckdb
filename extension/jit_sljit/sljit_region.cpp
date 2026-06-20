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
#include "duckdb/execution/execution_region_settings.hpp"

namespace duckdb {

static string AttachCoreRegionIR(string backend_ir, const ExecutionRegionIR &region_ir) {
	return std::move(backend_ir) + ";core=(" + region_ir.ir + ")";
}

ExecutionRegionLoweringPlan AnalyzeSljitRegion(const ExecutionRegionCompilationInput &input) {
	return BuildSljitRegionPlan(input.region_ir, input.candidate).lowering_plan;
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
	auto native_region = sljit_plan->native_region ? CopySljitNativeRegion(*sljit_plan->native_region) : nullptr;
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
		if (native_region->fused_primitive_aggregate_updates > 0) {
			reason +=
			    ";fused:primitive-aggregate-update=" + std::to_string(native_region->fused_primitive_aggregate_updates);
		}
		if (native_region->runtime_combined_filter_projections > 0) {
			reason += ";runtime-fused:filter-projection=" +
			          std::to_string(native_region->runtime_combined_filter_projections);
		}
		if (native_region->UsesSourceContract()) {
			reason += ";source-execution:source-contract";
		}
		SljitExecutableRegion executable_region;
		if (!BuildSljitExecutableRegion(*native_region, executable_region, error)) {
			return ExecutionRegionCompileResult::Error(std::move(error));
		}
		if (executable_region.ops.empty()) {
			throw InternalException(
			    "SLJIT compiled region reached code generation without executable region operators");
		}
		const auto execution_body = SljitNativeRegionExecutionBody(*native_region, contract);
		if (execution_body == ExecutionRegionExecutionBody::NONE) {
			throw InternalException("SLJIT compiled region has no executable body classification");
		}
		auto expected_execution_body = lowering_plan.ExpectedExecutionBody();
		if (expected_execution_body != ExecutionRegionExecutionBody::NONE &&
		    expected_execution_body != execution_body) {
			throw InternalException("SLJIT executable body %s does not match analyzed body %s",
			                        ExecutionRegionExecutionBodyToString(execution_body),
			                        ExecutionRegionExecutionBodyToString(expected_execution_body));
		}
		if (execution_mode != ExecutionRegionExecutionMode::NATIVE) {
			throw InternalException("SLJIT executable mode native does not match analyzed mode %s",
			                        ExecutionRegionExecutionModeToString(execution_mode));
		}
		auto shape = DescribeNativeRegionShape(*native_region);
		string ir;
		if (ExecutionRegionSettings::DumpIR(input.context)) {
			ir = AttachCoreRegionIR(DescribeNativeRegion(*native_region, "native.region"), input.region_ir);
		}
		reason += ";execution:native-sljit-region-" + shape;
		reason += ";execution-body=" + string(ExecutionRegionExecutionBodyToString(execution_body));
		if (ExecutionRegionSettings::Verify(input.context)) {
			reason += ";verify:region";
		}
		return ExecutionRegionCompileResult::Compiled(
		    CreateSljitNativeRegionKernel(input.context, backend_name, std::move(executable_region), contract.abi),
		    execution_mode, std::move(reason), MaybeDumpIr(input.context, std::move(ir)), execution_body);
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_util.hpp"
#include "sljit_region_codegen.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_region_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

static string AttachCoreRegionIR(string backend_ir, const JitRegionIR &region_ir) {
	return std::move(backend_ir) + ";core=(" + region_ir.ir + ")";
}

static unique_ptr<JitRegionKernel> TryBuildFusedFilterProjectionRegion(const string &backend_name,
                                                                         SljitNativeRegionPlan &&region,
                                                                         const JitRegionIR &region_ir, string &ir,
                                                                         string &error, JitRegionABI abi) {
	if (!CanFuseNativeFilterProjectionRegion(region)) {
		return nullptr;
	}

	auto &filter = region.ops[0].filter;
	auto &projection = region.ops[1].projections[0];
	auto projection_overflow_message = NativeIntegerBinaryOverflowMessage(projection.binary_op);
	SljitFusedFilterProjectionFunction function = nullptr;
	auto code = BuildSljitFusedIntegerFilterProjection(filter.integer_kind, filter.compare_op, filter.constant_on_left,
	                                                   projection.binary_op, projection.constant_on_left, function,
	                                                   error);
	if (!code) {
		return nullptr;
	}

	ir = AttachCoreRegionIR(DescribeNativeRegion(region, "native.region.fused"), region_ir);
	return CreateSljitFusedFilterProjectionKernel(backend_name, std::move(filter), std::move(projection),
	                                             std::move(code), function,
	                                             std::move(projection_overflow_message), abi,
	                                             region.native_source);
}

static unique_ptr<JitRegionKernel> TryBuildFusedFilterProjectionUngroupedSumRegion(const string &backend_name,
                                                                                   SljitNativeRegionPlan &&region,
                                                                                   const JitRegionIR &region_ir,
                                                                                   string &ir, string &error,
                                                                                   JitRegionABI abi) {
	if (!JitRegionABIIsFullPipeline(abi) || !CanFuseNativeFilterProjectionUngroupedSumRegion(region)) {
		return nullptr;
	}

	auto &filter = region.ops[0].filter;
	auto &projection = region.ops[1].projections[0];
	auto &update = region.ops[2].native_ungrouped_aggregate_updates[0];
	auto projection_overflow_message = projection.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES
	                                       ? NativeIntegerBinaryOverflowMessage(projection.binary_op)
	                                       : string();
	SljitFusedUngroupedAggregateFunction function = nullptr;
	auto code = BuildSljitFusedFilterProjectionUngroupedSum(filter, projection, update, function, error);
	if (!code || !function) {
		return nullptr;
	}

	ir = AttachCoreRegionIR(DescribeNativeRegion(region, "native.region.fused"), region_ir);
	return CreateSljitFusedFilterProjectionUngroupedSumKernel(
	    backend_name, std::move(filter), std::move(projection), std::move(update), std::move(code), function,
	    std::move(projection_overflow_message), region.native_source);
}

static unique_ptr<JitRegionKernel> TryBuildFusedProjectionUngroupedSumRegion(const string &backend_name,
                                                                             SljitNativeRegionPlan &&region,
                                                                             const JitRegionIR &region_ir,
                                                                             string &ir, string &error,
                                                                             JitRegionABI abi) {
	if (!JitRegionABIIsFullPipeline(abi) || !CanFuseNativeProjectionUngroupedSumRegion(region)) {
		return nullptr;
	}

	auto &projection = region.ops[0].projections[0];
	auto &update = region.ops[1].native_ungrouped_aggregate_updates[0];
	auto projection_overflow_message = projection.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES
	                                       ? NativeIntegerBinaryOverflowMessage(projection.binary_op)
	                                       : string();
	SljitFusedUngroupedAggregateFunction function = nullptr;
	auto code = BuildSljitFusedProjectionUngroupedSum(projection, update, function, error);
	if (!code || !function) {
		return nullptr;
	}

	ir = AttachCoreRegionIR(DescribeNativeRegion(region, "native.region.fused"), region_ir);
	return CreateSljitFusedProjectionUngroupedSumKernel(backend_name, std::move(projection), std::move(update),
	                                                    std::move(code), function,
	                                                    std::move(projection_overflow_message),
	                                                    region.native_source);
}

static unique_ptr<JitRegionKernel> TryBuildFusedPerfectHashAggregateRegion(const string &backend_name,
                                                                           SljitNativeRegionPlan &&region,
                                                                           const JitRegionIR &region_ir, string &ir,
                                                                           string &error, JitRegionABI abi) {
	if (!JitRegionABIIsFullPipeline(abi) || !CanFuseNativePerfectHashAggregateRegion(region)) {
		return nullptr;
	}

	unique_ptr<JitCodeHandle> code;
	SljitFusedPerfectHashAggregateFunction function = nullptr;
	string overflow_message;
	if (!BuildSljitFusedDirectPerfectHashAggregate(region, code, function, overflow_message, error)) {
		return nullptr;
	}
	if (!code || !function) {
		return nullptr;
	}

	ir = AttachCoreRegionIR(DescribeNativeRegion(region, "native.region.fused"), region_ir);
	auto native_source = region.native_source;
	return CreateSljitFusedDirectPerfectHashAggregateKernel(backend_name, std::move(region), std::move(code),
	                                                        function, std::move(overflow_message), native_source);
}

static unique_ptr<JitRegionKernel>
TryBuildOperatorStageKernel(const string &backend_name, SljitNativeRegionPlan &&region,
                            const JitRegionIR &region_ir, string &ir, string &error,
                            const JitRegionContract &contract, const SljitOperatorStageRegionPlan &stage_plan) {
	switch (stage_plan.kernel_kind) {
	case SljitOperatorKernelKind::FILTER_PROJECTION:
		return TryBuildFusedFilterProjectionRegion(backend_name, std::move(region), region_ir, ir, error,
		                                           contract.abi);
	case SljitOperatorKernelKind::FILTER_PROJECTION_UNGROUPED_SUM:
		return TryBuildFusedFilterProjectionUngroupedSumRegion(backend_name, std::move(region), region_ir, ir, error,
		                                                       contract.abi);
	case SljitOperatorKernelKind::PROJECTION_UNGROUPED_SUM:
		return TryBuildFusedProjectionUngroupedSumRegion(backend_name, std::move(region), region_ir, ir, error,
		                                                 contract.abi);
	case SljitOperatorKernelKind::PERFECT_HASH_AGGREGATE:
		return TryBuildFusedPerfectHashAggregateRegion(backend_name, std::move(region), region_ir, ir, error,
		                                               contract.abi);
	default:
		return nullptr;
	}
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
		if (native_region->runtime_fused_filter_projections > 0) {
			reason += ";runtime-fused:filter-projection=" +
			          std::to_string(native_region->runtime_fused_filter_projections);
		}
		if (native_region->native_source) {
			reason += ";source-execution:native-source";
		}
		auto stage_plan =
		    BuildSljitOperatorStageRegionPlan(*native_region, contract, input.candidate.stage_plan);
		if (stage_plan.HasImplementedKernel()) {
			string kernel_ir;
			auto stage_region = CopySljitNativeRegion(*native_region);
			auto stage_kernel = TryBuildOperatorStageKernel(backend_name, std::move(*stage_region),
			                                                input.region_ir, kernel_ir, error, contract, stage_plan);
			if (stage_kernel) {
				reason += ";" + stage_plan.stage_ir;
				reason += ";" + stage_plan.execution_reason;
				if (Settings::Get<JitVerifySetting>(input.context)) {
					reason += ";verify:region";
				}
				return JitRegionCompileResult::Compiled(std::move(stage_kernel), execution_mode, std::move(reason),
				                                          MaybeDumpIr(input.context, std::move(kernel_ir)));
			}
			if (stage_plan.kernel_kind == SljitOperatorKernelKind::PERFECT_HASH_AGGREGATE) {
				reason += ";execution:unsupported;fused-perfect-hash-codegen=" +
				          (error.empty() ? string("unsupported shape") : error);
				return JitRegionCompileResult::Unsupported(std::move(reason));
			}
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

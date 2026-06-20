#pragma once

#include "test_jit_helpers.hpp"

#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <atomic>

namespace {

class UnitTestExecutionRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "unit_test_jit_backend";
	}

	string Description() const override {
		return "unit test execution region backend";
	}
};

class ContractTestRegionKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) override {
		throw InternalException("contract test region kernel should not execute");
	}

private:
	string backend_name = "contract_test_region_jit_backend";
};

static bool IsMaximalTransformCandidate(const ExecutionRegionCompilationInput &input) {
	return input.candidate.contract.abi == ExecutionRegionABI::FULL_PIPELINE &&
	       input.candidate.end_operator_index > input.candidate.start_operator_index;
}

static bool IsMaximalTransformCandidate(const ExecutionRegionCandidate &candidate) {
	return candidate.contract.abi == ExecutionRegionABI::FULL_PIPELINE &&
	       candidate.end_operator_index > candidate.start_operator_index;
}

static ExecutionRegionLoweringPlan UnsupportedContractBoundaryPlan() {
	ExecutionRegionLoweringPlan plan;
	plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
	plan.AddNode("boundary", "CONTRACT_BOUNDARY", ExecutionRegionLoweringKind::BOUNDARY,
	             "contract backend only compiles maximal transform candidates");
	return plan;
}

static void SetGeneratedFusedRegion(ExecutionRegionLoweringPlan &plan) {
	plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
	plan.SetRegionExecutionForm(ExecutionRegionForm::FUSED);
	plan.SetExecutionBody(ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE);
	plan.SetGeneratedStageCount(1);
}

class ZeroCodeRegionKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) override {
		throw InternalException("zero-code region kernel should not execute");
	}

private:
	string backend_name = "contract_test_zero_code_region_jit_backend";
};

class ZeroCodeRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_zero_code_region_jit_backend";
	}

	string Description() const override {
		return "contract test zero-code region execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("source", "CONTRACT_SOURCE", ExecutionRegionLoweringKind::BOUNDARY, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", ExecutionRegionLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", ExecutionRegionLoweringKind::BOUNDARY, "contract sink boundary");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		return ExecutionRegionCompileResult::Compiled(
		    make_uniq<ZeroCodeRegionKernel>(), ExecutionRegionExecutionMode::NATIVE, "contract-test-zero-code-region");
	}
};

class NonCompiledKernelResultBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_non_compiled_kernel_result_jit_backend";
	}

	string Description() const override {
		return "contract test non-compiled kernel result execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("source", "CONTRACT_SOURCE", ExecutionRegionLoweringKind::BOUNDARY, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", ExecutionRegionLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", ExecutionRegionLoweringKind::BOUNDARY, "contract sink boundary");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		ExecutionRegionCompileResult result;
		result.status = ExecutionRegionCompileStatus::UNSUPPORTED;
		result.execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
		result.reason = "contract-test-non-compiled-kernel-result";
		result.kernel = make_uniq<ContractTestRegionKernel>();
		return result;
	}
};

class AutoRejectedCountingBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_auto_reject_jit_backend";
	}

	string Description() const override {
		return "contract test auto-rejected execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		region_analyze_count++;
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("source", "CONTRACT_SOURCE", ExecutionRegionLoweringKind::BOUNDARY, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", ExecutionRegionLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", ExecutionRegionLoweringKind::BOUNDARY, "contract sink boundary");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		region_compile_count++;
		return ExecutionRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-region-compiled");
	}

	atomic<idx_t> region_compile_count {0};
	atomic<idx_t> region_analyze_count {0};
};

class AutoMissingExecutionFormBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_auto_missing_execution_form_jit_backend";
	}

	string Description() const override {
		return "contract test auto missing execution form execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		region_analyze_count++;
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		plan.shape_key = "contract:auto-missing-execution-form";
		plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
		plan.SetRegionExecutionForm(ExecutionRegionForm::NONE);
		plan.AddNode("source", "CONTRACT_SOURCE", ExecutionRegionLoweringKind::BOUNDARY, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", ExecutionRegionLoweringKind::NATIVE,
		             "contract native node without execution form");
		plan.AddNode("sink", "CONTRACT_SINK", ExecutionRegionLoweringKind::BOUNDARY, "contract sink boundary");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		region_compile_count++;
		return ExecutionRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-auto-missing-execution-form-compiled");
	}

	atomic<idx_t> region_analyze_count {0};
	atomic<idx_t> region_compile_count {0};
};

class ImplicitModeRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_implicit_mode_region_jit_backend";
	}

	string Description() const override {
		return "contract test implicit-mode region execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &) override {
		ExecutionRegionLoweringPlan plan;
		plan.AddNode("op", "CONTRACT_OPERATOR", ExecutionRegionLoweringKind::NATIVE,
		             "contract native node without explicit region mode");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		region_compile_count++;
		return ExecutionRegionCompileResult::Compiled(make_uniq<ContractTestRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-implicit-mode-region");
	}

	atomic<idx_t> region_compile_count {0};
};

class ThrowingVerifiedRegionKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) override {
		throw InternalException("contract test region runtime failure");
	}

private:
	string backend_name = "contract_test_throwing_region_jit_backend";
};

class ThrowingVerifiedRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_throwing_region_jit_backend";
	}

	string Description() const override {
		return "contract test throwing region execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		if (!IsMaximalTransformCandidate(input)) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("source", "CONTRACT_SOURCE", ExecutionRegionLoweringKind::BOUNDARY, "contract source boundary");
		plan.AddNode("op0", "CONTRACT_FILTER", ExecutionRegionLoweringKind::NATIVE, "contract native node");
		plan.AddNode("sink", "CONTRACT_SINK", ExecutionRegionLoweringKind::BOUNDARY, "contract sink boundary");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		return ExecutionRegionCompileResult::Compiled(make_uniq<ThrowingVerifiedRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-throwing-region");
	}
};

class FullPipelineAbiRejectRegionKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

private:
	string backend_name = "contract_test_full_pipeline_abi_region_jit_backend";
};

class FullPipelineAbiRejectRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_full_pipeline_abi_region_jit_backend";
	}

	string Description() const override {
		return "contract test full-pipeline ABI region execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		if (input.candidate.contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("full", "CONTRACT_FULL_PIPELINE", ExecutionRegionLoweringKind::NATIVE,
		             "contract full pipeline node without full-pipeline executable ABI");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		return ExecutionRegionCompileResult::Compiled(make_uniq<FullPipelineAbiRejectRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-full-pipeline-abi-region");
	}
};

class FalseReturningFullPipelineRegionKernel : public ExecutionRegionKernel {
public:
	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		return 1;
	}

	bool CanExecuteFullPipeline() const override {
		return true;
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &, ExecutionRegionResult &) override {
		return false;
	}

private:
	string backend_name = "contract_test_false_returning_full_pipeline_region_jit_backend";
};

class FalseReturningFullPipelineRegionBackend : public ExecutionRegionBackend {
public:
	string Name() const override {
		return "contract_test_false_returning_full_pipeline_region_jit_backend";
	}

	string Description() const override {
		return "contract test false-returning full-pipeline region execution backend";
	}

	bool SupportsRegions() const override {
		return true;
	}

	ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input) override {
		if (input.candidate.contract.abi != ExecutionRegionABI::FULL_PIPELINE) {
			return UnsupportedContractBoundaryPlan();
		}
		ExecutionRegionLoweringPlan plan;
		SetGeneratedFusedRegion(plan);
		plan.AddNode("full", "CONTRACT_FULL_PIPELINE", ExecutionRegionLoweringKind::NATIVE,
		             "contract false-returning full pipeline node");
		return plan;
	}

	ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &) override {
		return ExecutionRegionCompileResult::Compiled(make_uniq<FalseReturningFullPipelineRegionKernel>(),
		                                              ExecutionRegionExecutionMode::NATIVE,
		                                              "contract-test-false-returning-full-pipeline-region");
	}
};

class CountingCodeHandle : public ExecutionRegionCodeHandle {
public:
	explicit CountingCodeHandle(bool &destroyed_p) : destroyed(destroyed_p) {
	}

	~CountingCodeHandle() override {
		destroyed = true;
	}

	idx_t CodeSize() const override {
		return 17;
	}

private:
	bool &destroyed;
};

} // namespace

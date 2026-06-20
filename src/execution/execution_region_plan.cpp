#include "duckdb/execution/execution_region_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionPlan::ExecutionRegionPlan() {
}

ExecutionRegionPlan::~ExecutionRegionPlan() {
}

optional_ptr<ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() {
	for (auto &kernel : kernels) {
		D_ASSERT(kernel);
		if (!ExecutionRegionABIIsFullPipeline(kernel->ExecutionABI()) || !kernel->CanExecuteFullPipeline()) {
			continue;
		}
		return *kernel;
	}
	return nullptr;
}

optional_ptr<const ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() const {
	for (auto &kernel : kernels) {
		D_ASSERT(kernel);
		if (!ExecutionRegionABIIsFullPipeline(kernel->ExecutionABI()) || !kernel->CanExecuteFullPipeline()) {
			continue;
		}
		return *kernel;
	}
	return nullptr;
}

bool ExecutionRegionPlan::HasExecutableFullPipeline() const {
	return GetExecutableFullPipelineKernel() != nullptr;
}

void ExecutionRegionPlan::SelectRunner(ExecutionRunnerKind runner) {
	if (runner == ExecutionRunnerKind::COMPILED_VECTORIZED && !HasExecutableFullPipeline()) {
		throw InternalException("compiled-vectorized execution selected without an executable full-pipeline region");
	}
	selected_runner = runner;
}

} // namespace duckdb

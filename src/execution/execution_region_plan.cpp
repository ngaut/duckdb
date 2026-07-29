#include "duckdb/execution/execution_region_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionPlan::ExecutionRegionPlan() {
}

ExecutionRegionPlan::~ExecutionRegionPlan() {
}

optional_ptr<ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() {
	if (!kernel || !ExecutionRegionABIIsFullPipeline(kernel->ExecutionABI()) || !kernel->CanExecuteFullPipeline()) {
		return nullptr;
	}
	return *kernel;
}

optional_ptr<const ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() const {
	if (!kernel || !ExecutionRegionABIIsFullPipeline(kernel->ExecutionABI()) || !kernel->CanExecuteFullPipeline()) {
		return nullptr;
	}
	return *kernel;
}

bool ExecutionRegionPlan::HasExecutableFullPipeline() const {
	return GetExecutableFullPipelineKernel() != nullptr;
}

void ExecutionRegionPlan::SelectRunner(ExecutionRunnerKind runner) {
	if (runner != ExecutionRunnerKind::VECTORIZED && !HasExecutableFullPipeline()) {
		throw InternalException("compiled execution selected without an executable full-pipeline region");
	}
	selected_runner = runner;
}

} // namespace duckdb

#include "duckdb/execution/execution_region_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionPlan::ExecutionRegionPlan() {
}

ExecutionRegionPlan::~ExecutionRegionPlan() {
}

optional_ptr<ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() {
	return kernel ? optional_ptr<ExecutionRegionKernel>(*kernel) : nullptr;
}

optional_ptr<const ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() const {
	return kernel ? optional_ptr<const ExecutionRegionKernel>(*kernel) : nullptr;
}

bool ExecutionRegionPlan::HasExecutableFullPipeline() const {
	return kernel != nullptr;
}

void ExecutionRegionPlan::SelectRunner(ExecutionRunnerKind runner) {
	if (runner != ExecutionRunnerKind::VECTORIZED && !HasExecutableFullPipeline()) {
		throw InternalException("compiled execution selected without an executable full-pipeline region");
	}
	selected_runner = runner;
}

} // namespace duckdb

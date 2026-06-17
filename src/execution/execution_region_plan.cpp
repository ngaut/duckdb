#include "duckdb/execution/execution_region_plan.hpp"

#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

ExecutionRegionPlan::ExecutionRegionPlan() {
}

ExecutionRegionPlan::~ExecutionRegionPlan() {
}

optional_ptr<ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() {
	for (auto &kernel : kernels) {
		D_ASSERT(kernel);
		if (!kernel->HasTraceCandidate()) {
			continue;
		}
		auto &contract = kernel->TraceCandidateContract();
		if (!ExecutionRegionABIIsFullPipeline(contract.abi) || !kernel->CanExecuteFullPipeline()) {
			continue;
		}
		return *kernel;
	}
	return nullptr;
}

optional_ptr<const ExecutionRegionKernel> ExecutionRegionPlan::GetExecutableFullPipelineKernel() const {
	for (auto &kernel : kernels) {
		D_ASSERT(kernel);
		if (!kernel->HasTraceCandidate()) {
			continue;
		}
		auto &contract = kernel->TraceCandidateContract();
		if (!ExecutionRegionABIIsFullPipeline(contract.abi) || !kernel->CanExecuteFullPipeline()) {
			continue;
		}
		return *kernel;
	}
	return nullptr;
}

bool ExecutionRegionPlan::HasExecutableFullPipeline() const {
	return GetExecutableFullPipelineKernel() != nullptr;
}

} // namespace duckdb

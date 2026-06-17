#include "duckdb/execution/execution_region_backend.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

ExecutionRegionCompilationInput::ExecutionRegionCompilationInput(ClientContext &context_p,
                                                                 const ExecutionRegionIR &region_ir_p,
                                                                 const ExecutionRegionCandidate &candidate_p)
    : context(context_p), region_ir(region_ir_p), candidate(candidate_p) {
}

ExecutionRegionCompileResult ExecutionRegionCompileResult::Compiled(unique_ptr<ExecutionRegionKernel> kernel,
                                                                    ExecutionRegionExecutionMode execution_mode,
                                                                    string reason, string ir) {
	if (!kernel) {
		throw InternalException("compiled region result marked compiled without a kernel");
	}
	if (!ExecutionRegionExecutionModeIsCompiled(execution_mode)) {
		throw InternalException("compiled region result uses invalid compiled execution mode");
	}
	ExecutionRegionCompileResult result;
	result.status = ExecutionRegionCompileStatus::COMPILED;
	result.execution_mode = execution_mode;
	result.reason = std::move(reason);
	result.ir = std::move(ir);
	result.kernel = std::move(kernel);
	return result;
}

ExecutionRegionCompileResult ExecutionRegionCompileResult::Unsupported(string reason) {
	ExecutionRegionCompileResult result;
	result.status = ExecutionRegionCompileStatus::UNSUPPORTED;
	result.execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	result.reason = std::move(reason);
	return result;
}

ExecutionRegionCompileResult ExecutionRegionCompileResult::Unavailable(string reason) {
	ExecutionRegionCompileResult result;
	result.status = ExecutionRegionCompileStatus::UNAVAILABLE;
	result.execution_mode = ExecutionRegionExecutionMode::NONE;
	result.reason = std::move(reason);
	return result;
}

ExecutionRegionCompileResult ExecutionRegionCompileResult::Error(string reason) {
	ExecutionRegionCompileResult result;
	result.status = ExecutionRegionCompileStatus::ERROR;
	result.execution_mode = ExecutionRegionExecutionMode::NONE;
	result.reason = std::move(reason);
	return result;
}

ExecutionRegionBackend::~ExecutionRegionBackend() {
}

bool ExecutionRegionBackend::IsAvailable() const {
	return true;
}

bool ExecutionRegionBackend::SupportsRegions() const {
	return false;
}

bool ExecutionRegionBackend::HasAutoAdmissionRules(ExecutionRegionCompileTarget) const {
	return false;
}

ExecutionRegionLoweringPlan ExecutionRegionBackend::AnalyzeRegion(const ExecutionRegionCompilationInput &) {
	ExecutionRegionLoweringPlan plan;
	plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
	plan.AddNode("region", "unknown", ExecutionRegionLoweringKind::BOUNDARY, "backend does not analyze regions");
	return plan;
}

ExecutionRegionCompileResult ExecutionRegionBackend::CompileRegion(const ExecutionRegionCompilationInput &) {
	return ExecutionRegionCompileResult::Unsupported("backend does not compile regions");
}

bool ExecutionRegionBackend::GetAutoAdmissionRule(ExecutionRegionCompileTarget, const ExecutionRegionCandidate &,
                                                  ExecutionRegionAdmissionRule &) const {
	return false;
}

bool ExecutionRegionBackend::GetAutoAdmissionRule(ExecutionRegionCompileTarget,
                                                  const ExecutionRegionPipelineInventory &,
                                                  ExecutionRegionAdmissionRule &) const {
	return false;
}

bool ExecutionRegionBackend::GetAutoAdmissionRule(ExecutionRegionCompileTarget, const ExecutionRegionCandidate &,
                                                  const ExecutionRegionLoweringPlan &,
                                                  ExecutionRegionAdmissionRule &) const {
	return false;
}

ExecutionRegionBackendPlan::~ExecutionRegionBackendPlan() {
}

} // namespace duckdb

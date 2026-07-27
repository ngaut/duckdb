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

ExecutionRegionCompileResult
ExecutionRegionCompileResult::CompiledArtifact(shared_ptr<const ExecutionRegionArtifact> artifact,
                                               ExecutionRegionExecutionMode execution_mode, string reason, string ir) {
	if (!artifact) {
		throw InternalException("compiled region result marked compiled without an artifact");
	}
	if (!ExecutionRegionExecutionModeIsCompiled(execution_mode)) {
		throw InternalException("compiled artifact result uses invalid compiled execution mode");
	}
	ExecutionRegionCompileResult result;
	result.status = ExecutionRegionCompileStatus::COMPILED;
	result.execution_mode = execution_mode;
	result.reason = std::move(reason);
	result.ir = std::move(ir);
	result.artifact = std::move(artifact);
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

ExecutionRegionArtifact::~ExecutionRegionArtifact() {
}

ExecutionRunnerKind ExecutionRegionBackend::RunnerKind() const {
	return ExecutionRunnerKind::COMPILED_VECTORIZED;
}

bool ExecutionRegionBackend::IsAvailable() const {
	return true;
}

bool ExecutionRegionBackend::SupportsRegions() const {
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

unique_ptr<ExecutionRegionKernel>
ExecutionRegionBackend::InstantiateRegionArtifact(const shared_ptr<const ExecutionRegionArtifact> &,
                                                  const ExecutionRegionCompilationInput &) {
	return nullptr;
}

ExecutionRegionBackendPlan::~ExecutionRegionBackendPlan() {
}

string ExecutionRegionBackendPlan::ArtifactCacheKey() const {
	return string();
}

} // namespace duckdb

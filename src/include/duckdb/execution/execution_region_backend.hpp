//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_admission.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

class ClientContext;

struct ExecutionRegionBackendInfo {
	string name;
	string description;
	bool available = false;
	bool supports_regions = false;
	bool selected = false;
};

class ExecutionRegionBackendPlan {
public:
	virtual ~ExecutionRegionBackendPlan();
};

struct ExecutionRegionCompilationInput {
	ExecutionRegionCompilationInput(ClientContext &context, const ExecutionRegionIR &region_ir,
	                                const ExecutionRegionCandidate &candidate);

	ClientContext &context;
	const ExecutionRegionIR &region_ir;
	const ExecutionRegionCandidate &candidate;
	const ExecutionRegionLoweringPlan *lowering_plan = nullptr;
};

struct ExecutionRegionCompileResult {
	static ExecutionRegionCompileResult Compiled(unique_ptr<ExecutionRegionKernel> kernel,
	                                             ExecutionRegionExecutionMode execution_mode, string reason = string(),
	                                             string ir = string());
	static ExecutionRegionCompileResult Unsupported(string reason);
	static ExecutionRegionCompileResult Unavailable(string reason);
	static ExecutionRegionCompileResult Error(string reason);

	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::UNSUPPORTED;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	string reason;
	string ir;
	unique_ptr<ExecutionRegionKernel> kernel;
};

class ExecutionRegionBackend {
public:
	virtual ~ExecutionRegionBackend();

	virtual string Name() const = 0;
	virtual string Description() const = 0;
	virtual bool IsAvailable() const;
	virtual bool SupportsRegions() const;
	virtual bool HasAutoAdmissionRules(ExecutionRegionCompileTarget target) const;
	virtual ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input);
	virtual ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input);
	virtual bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target,
	                                  const ExecutionRegionPipelineInventory &inventory,
	                                  ExecutionRegionAdmissionRule &rule) const;
	virtual bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionCandidate &candidate,
	                                  ExecutionRegionAdmissionRule &rule) const;
	virtual bool GetAutoAdmissionRule(ExecutionRegionCompileTarget target, const ExecutionRegionCandidate &candidate,
	                                  const ExecutionRegionLoweringPlan &lowering_plan,
	                                  ExecutionRegionAdmissionRule &rule) const;
};

} // namespace duckdb

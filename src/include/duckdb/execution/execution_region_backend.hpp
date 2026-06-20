//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_backend.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_kernel.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

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
	static ExecutionRegionCompileResult
	Compiled(unique_ptr<ExecutionRegionKernel> kernel, ExecutionRegionExecutionMode execution_mode,
	         string reason = string(), string ir = string(),
	         ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE);
	static ExecutionRegionCompileResult Unsupported(string reason);
	static ExecutionRegionCompileResult Unavailable(string reason);
	static ExecutionRegionCompileResult Error(string reason);

	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::UNSUPPORTED;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE;
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
	virtual ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input);
	virtual ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input);
};

DUCKDB_API void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend);

} // namespace duckdb

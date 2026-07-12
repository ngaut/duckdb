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
	ExecutionRunnerKind runner_kind = ExecutionRunnerKind::COMPILED_VECTORIZED;
	bool available = false;
	bool supports_regions = false;
	bool selected = false;
};

class DUCKDB_API ExecutionRegionBackendPlan {
public:
	virtual ~ExecutionRegionBackendPlan();
};

struct DUCKDB_API ExecutionRegionCompilationInput {
	ExecutionRegionCompilationInput(ClientContext &context, const ExecutionRegionIR &region_ir,
	                                const ExecutionRegionCandidate &candidate);

	ClientContext &context;
	const ExecutionRegionIR &region_ir;
	const ExecutionRegionCandidate &candidate;
	const ExecutionRegionLoweringPlan *lowering_plan = nullptr;
};

struct ExecutionRegionCompileTimings {
	int64_t executable_build_time_us = 0;
	int64_t machine_codegen_time_us = 0;
	int64_t kernel_build_time_us = 0;
};

struct DUCKDB_API ExecutionRegionCompileResult {
	static ExecutionRegionCompileResult Compiled(unique_ptr<ExecutionRegionKernel> kernel,
	                                             ExecutionRegionExecutionMode execution_mode, string reason = string(),
	                                             string ir = string());
	static ExecutionRegionCompileResult Unsupported(string reason);
	static ExecutionRegionCompileResult Unavailable(string reason);
	static ExecutionRegionCompileResult Error(string reason);

	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::UNSUPPORTED;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	ExecutionRegionCompileTimings timings;
	string reason;
	string ir;
	unique_ptr<ExecutionRegionKernel> kernel;
};

class DUCKDB_API ExecutionRegionBackend {
public:
	virtual ~ExecutionRegionBackend();

	virtual string Name() const = 0;
	virtual string Description() const = 0;
	virtual ExecutionRunnerKind RunnerKind() const;
	virtual bool IsAvailable() const;
	virtual bool SupportsRegions() const;
	virtual ExecutionRegionLoweringPlan AnalyzeRegion(const ExecutionRegionCompilationInput &input);
	virtual ExecutionRegionCompileResult CompileRegion(const ExecutionRegionCompilationInput &input);
};

DUCKDB_API void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend);

} // namespace duckdb

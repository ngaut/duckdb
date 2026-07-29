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
class ExecutionRegionArtifact;

//! C++ ABI contract shared by DuckDB and loadable execution-region backends.
//! Increment this whenever any execution-region backend, plan, kernel, local-state, or runtime interface changes
//! layout or virtual method order. The version is passed explicitly at registration so extension package versions
//! are never used as a proxy for C++ ABI compatibility.
static constexpr uint64_t EXECUTION_REGION_BACKEND_ABI_VERSION = 7;

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
	//! A complete semantic identity for a reusable compiled artifact. Empty
	//! disables caching. The database-local cache can reuse artifacts across
	//! equivalent plans, so the key must cover every embedded semantic value
	//! and the artifact must not retain catalog or query-local objects.
	virtual string ArtifactCacheKey() const;
};

struct DUCKDB_API ExecutionRegionCompilationInput {
	ExecutionRegionCompilationInput(ClientContext &context, const ExecutionRegionIR &region_ir,
	                                const ExecutionRegionCandidate &candidate);

	ClientContext &context;
	const ExecutionRegionIR &region_ir;
	const ExecutionRegionCandidate &candidate;
	const ExecutionRegionLoweringPlan *lowering_plan = nullptr;
};

//! Backend-owned semantic code artifact. Artifacts may contain internally
//! synchronized lazy code-publication cells, but must not own query-local
//! counters, runtime bindings, adaptive state, or trace state.
class DUCKDB_API ExecutionRegionArtifact {
public:
	virtual ~ExecutionRegionArtifact();
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
	static ExecutionRegionCompileResult CompiledArtifact(shared_ptr<const ExecutionRegionArtifact> artifact,
	                                                     ExecutionRegionExecutionMode execution_mode,
	                                                     string reason = string(), string ir = string());
	static ExecutionRegionCompileResult Unsupported(string reason);
	static ExecutionRegionCompileResult Unavailable(string reason);
	static ExecutionRegionCompileResult Error(string reason);

	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::UNSUPPORTED;
	ExecutionRegionExecutionMode execution_mode = ExecutionRegionExecutionMode::UNSUPPORTED;
	ExecutionRegionCompileTimings timings;
	string reason;
	string ir;
	unique_ptr<ExecutionRegionKernel> kernel;
	shared_ptr<const ExecutionRegionArtifact> artifact;
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
	//! Bind a shared semantic artifact to the current execution. A backend that
	//! publishes an artifact must instantiate it successfully for its complete
	//! cache key; nullptr is a backend contract failure.
	virtual unique_ptr<ExecutionRegionKernel>
	InstantiateRegionArtifact(const shared_ptr<const ExecutionRegionArtifact> &artifact,
	                          const ExecutionRegionCompilationInput &input);
};

DUCKDB_API void RegisterExecutionRegionBackend(DatabaseInstance &db, unique_ptr<ExecutionRegionBackend> backend,
                                               uint64_t backend_abi_version);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_runner.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/execution_region_common.hpp"
#include "duckdb/parallel/pipeline_execution.hpp"

namespace duckdb {

class ExecutionRegionPipelineAdapter;

enum class ExecutionRunnerResultKind : uint8_t { EXECUTED, CONTINUE_VECTORIZED };

struct ExecutionRunnerResult {
	static ExecutionRunnerResult Executed(PipelineExecuteResult result);
	static ExecutionRunnerResult ContinueVectorized();

	ExecutionRunnerResultKind kind = ExecutionRunnerResultKind::EXECUTED;
	PipelineExecuteResult result = PipelineExecuteResult::FINISHED;
};

class ExecutionRunner {
public:
	virtual ~ExecutionRunner();

	virtual ExecutionRunnerResult Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) = 0;
};

enum class CompiledVectorizedRunStatus : uint8_t {
	EXECUTED,
	VECTORIZED_SUPPRESSED,
	VECTORIZED_CONTINUATION,
	VECTORIZED_DEFERRED
};

class VectorizedRunner : public ExecutionRunner {
public:
	ExecutionRunnerResult Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) override;
};

class ExecutionRegionKernel;
struct ExecutionRegionAdaptiveAbState;

class CompiledVectorizedRunner : public ExecutionRunner {
public:
	ExecutionRunnerResult Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) override;

	static CompiledVectorizedRunStatus
	ExecuteCompiledRegion(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks, PipelineExecuteResult &result,
	                      optional_ptr<ExecutionRegionAdaptiveAbState> adaptive_ab = nullptr,
	                      idx_t adaptive_margin_basis_points = 0);

private:
	static ExecutionRunnerResult ExecuteAdaptive(ExecutionRegionPipelineAdapter &pipeline,
	                                             ExecutionRegionKernel &kernel, idx_t max_chunks);
};

ExecutionRunner &GetExecutionRunner(ExecutionRunnerKind kind);
PipelineExecuteResult ExecuteExecutionRunner(ExecutionRunnerKind kind, ExecutionRegionPipelineAdapter &pipeline,
                                             idx_t max_chunks);

} // namespace duckdb

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

class CompiledVectorizedRunner : public ExecutionRunner {
public:
	ExecutionRunnerResult Execute(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks) override;

	static CompiledVectorizedRunStatus ExecuteCompiledRegion(ExecutionRegionPipelineAdapter &pipeline, idx_t max_chunks,
	                                                         PipelineExecuteResult &result);
	//! A pipeline fed by its producers' pushes never enters a runner: its executor binds the chunk step once, before
	//! the first push, and every push runs the step
	static void BindPushedInputStep(ExecutionRegionPipelineAdapter &pipeline);
	static void RecordPushedInputStepFinished(ExecutionRegionPipelineAdapter &pipeline,
	                                          const ExecutionRegionKernel &kernel);
};

ExecutionRunner &GetExecutionRunner(ExecutionRunnerKind kind);
PipelineExecuteResult ExecuteExecutionRunner(ExecutionRunnerKind kind, ExecutionRegionPipelineAdapter &pipeline,
                                             idx_t max_chunks);

} // namespace duckdb

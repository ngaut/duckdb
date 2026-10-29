//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_plan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

class ExecutionRegionKernel;

struct ExecutionRegionPlan {
	ExecutionRegionPlan();
	~ExecutionRegionPlan();

	ExecutionRegionOpenRequest source_open_request;
	unique_ptr<ExecutionRegionKernel> kernel;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;

	bool HasExecutableFullPipeline() const;
	void SelectRunner(ExecutionRunnerKind runner);
	ExecutionRunnerKind SelectedRunner() const {
		return selected_runner;
	}
	optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel();
	optional_ptr<const ExecutionRegionKernel> GetExecutableFullPipelineKernel() const;
	bool RequiresSourceContract() const {
		return source_open_request.UsesSourceContract();
	}
	const ExecutionRegionOpenRequest &OpenRequest() const {
		return source_open_request;
	}
};

} // namespace duckdb

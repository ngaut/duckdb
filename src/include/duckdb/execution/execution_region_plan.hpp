//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_plan.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_region_open_request.hpp"

namespace duckdb {

class ExecutionRegionKernel;

struct ExecutionRegionPlan {
	ExecutionRegionPlan();
	~ExecutionRegionPlan();

	string backend_name;
	ExecutionRegionOpenRequest source_open_request;
	vector<unique_ptr<ExecutionRegionKernel>> kernels;
	bool operator_readiness_refresh = false;

	bool HasExecutableRegions() const {
		return !kernels.empty();
	}
	bool HasExecutableFullPipeline() const;
	optional_ptr<ExecutionRegionKernel> GetExecutableFullPipelineKernel();
	optional_ptr<const ExecutionRegionKernel> GetExecutableFullPipelineKernel() const;
	bool RequiresOperatorReadinessRefresh() const {
		return operator_readiness_refresh && !HasExecutableFullPipeline();
	}
	vector<unique_ptr<ExecutionRegionKernel>> &Kernels() {
		return kernels;
	}
	const vector<unique_ptr<ExecutionRegionKernel>> &Kernels() const {
		return kernels;
	}
	bool RequiresCompiledSourceInput() const {
		return source_open_request.present && source_open_request.UsesGeneratedFilters();
	}
	bool RequiresSourceContract() const {
		return source_open_request.present && source_open_request.UsesSourceContract();
	}
	const vector<LogicalType> &SourceInputTypes(const vector<LogicalType> &input_types) const {
		return RequiresCompiledSourceInput() ? source_open_request.input_types : input_types;
	}
	const ExecutionRegionOpenRequest &OpenRequest() const {
		return source_open_request;
	}
};

} // namespace duckdb

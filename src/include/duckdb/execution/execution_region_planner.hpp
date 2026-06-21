//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_planner.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_plan.hpp"

namespace duckdb {

class ClientContext;
class ExecutionRegionBackend;
struct ExecutionRegionIR;
class Pipeline;

class ExecutionRegionPlanner {
private:
	struct SelectedCandidate;

public:
	DUCKDB_API static unique_ptr<ExecutionRegionPlan> Build(ClientContext &context, Pipeline &pipeline);

private:
	static void Compile(ClientContext &context, ExecutionRegionBackend &backend, const string &backend_name,
	                    ExecutionRegionPlan &plan, ExecutionRegionIR &region_ir,
	                    vector<SelectedCandidate> &selected_regions);
};

} // namespace duckdb

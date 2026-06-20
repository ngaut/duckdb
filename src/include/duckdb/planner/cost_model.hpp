//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/cost_model.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class Expression;
class TableFilterSet;
class TableFilter;

struct PhysicalRunnerCostInput {
	idx_t estimated_cardinality = 0;
	idx_t expression_cost = 0;
	idx_t accelerated_stage_count = 0;
	bool full_pipeline = false;
	idx_t node_count = 0;
	idx_t stage_count = 0;
	idx_t expression_node_count = 0;
	idx_t operator_count = 0;
	bool has_accelerated_work = false;
};

struct PhysicalRunnerCostProfile {
	bool present = false;
	int64_t rows = 0;
	int64_t batches = 0;
	int64_t expression_cost = 0;
	int64_t accelerated_stage_count = 0;
	int64_t saved_work_per_batch = 0;
	int64_t accelerated_runner_benefit = 0;
	int64_t startup_cost = 0;
	int64_t required_benefit = 0;
	int64_t net_benefit = 0;
	bool selected_accelerated_runner = false;
};

class DuckDBCostModel {
public:
	static idx_t ExpressionCost(const Expression &expr);
	static idx_t FilterCost(const TableFilter &filter);
	static vector<idx_t> InitialFilterOrder(const TableFilterSet &table_filters);

	static PhysicalRunnerCostProfile SelectPhysicalRunner(const PhysicalRunnerCostInput &input);
};

} // namespace duckdb

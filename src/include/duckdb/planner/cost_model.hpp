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

enum class PhysicalRunnerGeneratedWorkClass : uint8_t { NONE, PROJECTION_GLUE, HIGH_COST_PROJECTION, COMPUTE };

enum class PhysicalRunnerNativeProtocolClass : uint8_t { NONE, STATEFUL_SOURCE_SINK_PROTOCOL };

struct PhysicalRunnerCostInput {
	idx_t estimated_cardinality = 0;
	idx_t expression_cost = 0;
	idx_t generated_stage_count = 0;
	idx_t materialization_elision_count = 0;
	idx_t native_join_stage_count = 0;
	idx_t native_aggregate_stage_count = 0;
	idx_t native_grouped_aggregate_stage_count = 0;
	idx_t native_sort_stage_count = 0;
	bool full_pipeline = false;
	idx_t node_count = 0;
	idx_t stage_count = 0;
	idx_t expression_node_count = 0;
	idx_t operator_count = 0;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	bool has_accelerated_work = false;
};

struct PhysicalRunnerCostParameters {
	idx_t generated_stage_benefit = 0;
	idx_t native_operator_stage_benefit = 0;
	idx_t materialization_elision_benefit = 0;
	idx_t full_pipeline_benefit = 0;
	idx_t startup_base_cost = 0;
	idx_t startup_margin_basis_points = 0;
};

struct PhysicalRunnerCostProfile {
	bool present = false;
	int64_t rows = 0;
	int64_t batches = 0;
	int64_t expression_cost = 0;
	int64_t generated_stage_count = 0;
	int64_t materialization_elision_count = 0;
	int64_t native_join_stage_count = 0;
	int64_t native_aggregate_stage_count = 0;
	int64_t native_grouped_aggregate_stage_count = 0;
	int64_t native_sort_stage_count = 0;
	bool full_pipeline = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	int64_t generated_expression_work = 0;
	int64_t generated_stage_work = 0;
	int64_t native_operator_work = 0;
	int64_t materialization_elision_work = 0;
	int64_t full_pipeline_work = 0;
	int64_t stateful_protocol_penalty = 0;
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

	static PhysicalRunnerCostProfile SelectPhysicalRunner(const PhysicalRunnerCostInput &input,
	                                                      const PhysicalRunnerCostParameters &parameters);
};

DUCKDB_API const char *PhysicalRunnerGeneratedWorkClassToString(PhysicalRunnerGeneratedWorkClass work_class);
DUCKDB_API const char *PhysicalRunnerNativeProtocolClassToString(PhysicalRunnerNativeProtocolClass protocol_class);

} // namespace duckdb

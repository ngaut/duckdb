//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/cost_model.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/execution_region_common.hpp"

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
	idx_t materialization_source_append_count = 0;
	idx_t native_join_stage_count = 0;
	idx_t perfect_hash_join_probe_count = 0;
	idx_t hash_join_build_payload_column_count = 0;
	idx_t native_aggregate_stage_count = 0;
	idx_t native_grouped_aggregate_stage_count = 0;
	idx_t grouped_aggregate_group_count = 0;
	idx_t grouped_aggregate_varchar_group_count = 0;
	idx_t blocked_hash_aggregate_lookup_count = 0;
	idx_t native_sort_stage_count = 0;
	idx_t source_filter_count = 0;
	idx_t source_projected_column_count = 0;
	idx_t reference_varchar_projection_count = 0;
	bool full_pipeline = false;
	bool uses_scan_filters = false;
	bool sort_sink = false;
	idx_t node_count = 0;
	idx_t stage_count = 0;
	idx_t expression_node_count = 0;
	idx_t operator_count = 0;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	bool has_accelerated_work = false;
};

struct PhysicalRunnerCostParameters {
	bool compiled_vectorized_runner_available = true;
	idx_t generated_stage_benefit = 0;
	idx_t native_operator_stage_benefit = 0;
	idx_t materialization_elision_benefit = 0;
	idx_t full_pipeline_benefit = 0;
	idx_t startup_base_cost = 0;
	idx_t startup_margin_basis_points = 0;
	bool gpu_runner_available = false;
	idx_t gpu_generated_stage_benefit = 0;
	idx_t gpu_native_operator_stage_benefit = 0;
	idx_t gpu_materialization_elision_benefit = 0;
	idx_t gpu_full_pipeline_benefit = 0;
	idx_t gpu_startup_base_cost = 0;
	idx_t gpu_startup_margin_basis_points = 0;
	idx_t gpu_transfer_cost_per_batch = 0;
};

struct PhysicalRunnerCostProfile {
	bool present = false;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	int64_t rows = 0;
	int64_t batches = 0;
	int64_t expression_cost = 0;
	int64_t generated_stage_count = 0;
	int64_t materialization_elision_count = 0;
	int64_t materialization_source_append_count = 0;
	int64_t native_join_stage_count = 0;
	int64_t native_aggregate_stage_count = 0;
	int64_t native_grouped_aggregate_stage_count = 0;
	int64_t native_sort_stage_count = 0;
	int64_t source_filter_count = 0;
	bool full_pipeline = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	int64_t generated_expression_work = 0;
	int64_t generated_stage_work = 0;
	int64_t native_operator_work = 0;
	int64_t materialization_elision_work = 0;
	int64_t materialization_source_append_penalty = 0;
	int64_t full_pipeline_work = 0;
	int64_t stateful_protocol_penalty = 0;
	int64_t saved_work_per_batch = 0;
	int64_t compiled_vectorized_runner_benefit = 0;
	int64_t compiled_vectorized_startup_cost = 0;
	int64_t compiled_vectorized_required_benefit = 0;
	int64_t compiled_vectorized_net_benefit = 0;
	int64_t gpu_runner_benefit = 0;
	int64_t gpu_transfer_cost = 0;
	int64_t gpu_startup_cost = 0;
	int64_t gpu_required_benefit = 0;
	int64_t gpu_net_benefit = 0;
	int64_t accelerated_runner_benefit = 0;
	int64_t startup_cost = 0;
	int64_t required_benefit = 0;
	int64_t net_benefit = 0;
	bool selected_accelerated_runner = false;
	bool selected_compiled_vectorized_runner = false;
	bool selected_gpu_runner = false;
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

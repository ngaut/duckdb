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

enum class PhysicalRunnerCostInputScope : uint8_t { EXECUTION_REGION_CANDIDATE, PHYSICAL_PIPELINE };

struct PhysicalRunnerCostInput {
	PhysicalRunnerCostInputScope input_scope = PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE;
	idx_t estimated_cardinality = 0;
	idx_t expression_cost = 0;
	idx_t source_contract_input_cardinality = 0;
	bool source_contract_output_cardinality_unknown = false;
	bool finalized_dynamic_filter_cardinality_estimate = false;
	idx_t generated_stage_count = 0;
	idx_t generated_backend_stage_count = 0;
	idx_t generated_grouped_aggregate_stage_count = 0;
	idx_t native_grouped_state_address_lookup_count = 0;
	idx_t grouped_aggregate_estimated_cardinality = 0;
	idx_t materialization_elision_count = 0;
	idx_t selected_hash_join_filter_materialization_count = 0;
	idx_t native_join_stage_count = 0;
	idx_t native_hash_join_build_sink_count = 0;
	bool native_delim_join_sink = false;
	idx_t native_aggregate_stage_count = 0;
	idx_t native_grouped_aggregate_stage_count = 0;
	idx_t native_sort_stage_count = 0;
	idx_t source_filter_count = 0;
	bool full_pipeline = false;
	bool uses_scan_filters = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	bool has_accelerated_work = false;
	bool vectorized_execution_preferred = false;
};

//! The cost knobs of one accelerated runner. Every accelerated runner is priced by the
//! same model over these knobs; runner-specific pricing differences are expressed as
//! knob values (an in-memory runner simply has transfer_cost_per_batch = 0), never as
//! runner-specific code paths.
struct RunnerCostAxis {
	bool available = false;
	idx_t generated_stage_benefit = 0;
	idx_t native_operator_stage_benefit = 0;
	idx_t materialization_elision_benefit = 0;
	idx_t full_pipeline_benefit = 0;
	idx_t startup_base_cost = 0;
	idx_t startup_margin_basis_points = 0;
	//! Per-batch cost of moving data into the runner's memory domain.
	idx_t transfer_cost_per_batch = 0;

	bool HasEnabledBenefit() const {
		return available && (generated_stage_benefit > 0 || native_operator_stage_benefit > 0 ||
		                     materialization_elision_benefit > 0 || full_pipeline_benefit > 0);
	}
};

struct PhysicalRunnerCostParameters {
	//! Inputs shared by every axis: properties of the plan and the host, not of a runner.
	idx_t source_contract_scan_filter_penalty = 4096;
	idx_t vectorized_parallelism = 1;
	//! Axis order is the selection tie-break: an earlier axis keeps a tied net benefit.
	//! The first axis is the reference axis; it is costed even when unavailable so
	//! telemetry can report the hypothetical compiled-vectorized economics.
	RunnerCostAxis compiled_vectorized;
	RunnerCostAxis gpu;

	static constexpr idx_t AXIS_COUNT = 2;
	RunnerCostAxis &AxisAt(idx_t axis_idx) {
		return axis_idx == 0 ? compiled_vectorized : gpu;
	}
	const RunnerCostAxis &AxisAt(idx_t axis_idx) const {
		return axis_idx == 0 ? compiled_vectorized : gpu;
	}
	static ExecutionRunnerKind AxisRunner(idx_t axis_idx) {
		return axis_idx == 0 ? ExecutionRunnerKind::COMPILED_VECTORIZED : ExecutionRunnerKind::COMPILED_GPU;
	}
};

//! One accelerated runner's selection economics inside a cost profile.
struct PhysicalRunnerAxisCostBreakdown {
	bool available = false;
	//! Whether this axis passed selection analysis on its own; the winning runner is
	//! the selected axis with the highest net benefit.
	bool selected = false;
	string selection_reason;
	int64_t runner_benefit = 0;
	int64_t transfer_cost = 0;
	int64_t startup_cost = 0;
	int64_t required_benefit = 0;
	int64_t net_benefit = 0;
};

struct PhysicalRunnerCostProfile {
	bool present = false;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	PhysicalRunnerCostInputScope input_scope = PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE;
	int64_t rows = 0;
	int64_t batches = 0;
	int64_t costed_batches = 0;
	int64_t expression_cost = 0;
	int64_t source_contract_input_rows = 0;
	int64_t source_contract_input_batches = 0;
	bool source_contract_output_cardinality_unknown = false;
	int64_t generated_stage_count = 0;
	int64_t generated_backend_stage_count = 0;
	int64_t generated_grouped_aggregate_stage_count = 0;
	int64_t native_grouped_state_address_lookup_count = 0;
	int64_t grouped_aggregate_estimated_cardinality = 0;
	int64_t materialization_elision_count = 0;
	int64_t selected_hash_join_filter_materialization_count = 0;
	int64_t native_join_stage_count = 0;
	int64_t native_hash_join_build_sink_count = 0;
	int64_t native_aggregate_stage_count = 0;
	int64_t native_grouped_aggregate_stage_count = 0;
	int64_t native_sort_stage_count = 0;
	int64_t source_filter_count = 0;
	bool full_pipeline = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	string admission_class;
	string selection_reason;
	int64_t generated_expression_work = 0;
	int64_t generated_stage_work = 0;
	int64_t generated_backend_stage_work = 0;
	int64_t native_operator_work = 0;
	int64_t materialization_elision_work = 0;
	int64_t selected_hash_join_filter_materialization_penalty = 0;
	int64_t source_contract_scan_penalty = 0;
	int64_t full_pipeline_work = 0;
	int64_t stateful_protocol_penalty = 0;
	int64_t saved_work_per_batch = 0;
	int64_t accelerated_runner_benefit = 0;
	int64_t startup_cost = 0;
	int64_t required_benefit = 0;
	int64_t net_benefit = 0;
	ExecutionRegionJitRuntimeProofMask required_runtime_proofs = 0;
	//! Per-axis selection economics; filled for every axis regardless of which one won.
	PhysicalRunnerAxisCostBreakdown compiled_vectorized;
	PhysicalRunnerAxisCostBreakdown gpu;

	bool SelectedAcceleratedRunner() const {
		return selected_runner != ExecutionRunnerKind::VECTORIZED;
	}
	bool SelectedCompiledVectorizedRunner() const {
		return selected_runner == ExecutionRunnerKind::COMPILED_VECTORIZED;
	}
	bool SelectedGpuRunner() const {
		return selected_runner == ExecutionRunnerKind::COMPILED_GPU;
	}
	PhysicalRunnerAxisCostBreakdown &AxisAt(idx_t axis_idx) {
		return axis_idx == 0 ? compiled_vectorized : gpu;
	}
	const PhysicalRunnerAxisCostBreakdown &AxisAt(idx_t axis_idx) const {
		return axis_idx == 0 ? compiled_vectorized : gpu;
	}
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
DUCKDB_API const char *PhysicalRunnerCostInputScopeToString(PhysicalRunnerCostInputScope input_scope);

} // namespace duckdb

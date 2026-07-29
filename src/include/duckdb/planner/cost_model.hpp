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

#define DUCKDB_PHYSICAL_RUNNER_COST_SHAPE_FIELDS(FIELD)                                                                \
	FIELD(rows)                                                                                                        \
	FIELD(batches)                                                                                                     \
	FIELD(costed_batches)                                                                                              \
	FIELD(expression_cost)                                                                                             \
	FIELD(source_contract_input_rows)                                                                                  \
	FIELD(source_contract_input_batches)                                                                               \
	FIELD(generated_stage_count)                                                                                       \
	FIELD(generated_backend_stage_count)                                                                               \
	FIELD(generated_grouped_aggregate_stage_count)                                                                     \
	FIELD(native_grouped_state_address_lookup_count)                                                                   \
	FIELD(materialization_elision_count)                                                                               \
	FIELD(selected_hash_join_filter_materialization_count)                                                             \
	FIELD(native_join_stage_count)                                                                                     \
	FIELD(native_hash_join_build_sink_count)                                                                           \
	FIELD(native_aggregate_stage_count)                                                                                \
	FIELD(native_grouped_aggregate_stage_count)                                                                        \
	FIELD(native_sort_stage_count)                                                                                     \
	FIELD(grouped_aggregate_estimated_cardinality)

#define DUCKDB_PHYSICAL_RUNNER_COST_WORK_FIELDS(FIELD)                                                                 \
	FIELD(generated_expression_work)                                                                                   \
	FIELD(generated_stage_work)                                                                                        \
	FIELD(generated_backend_stage_work)                                                                                \
	FIELD(native_operator_work)                                                                                        \
	FIELD(materialization_elision_work)                                                                                \
	FIELD(selected_hash_join_filter_materialization_penalty)                                                           \
	FIELD(source_contract_scan_penalty)                                                                                \
	FIELD(full_pipeline_work)                                                                                          \
	FIELD(stateful_protocol_penalty)                                                                                   \
	FIELD(saved_work_per_batch)                                                                                        \
	FIELD(accelerated_runner_benefit)                                                                                  \
	FIELD(startup_cost)                                                                                                \
	FIELD(required_benefit)                                                                                            \
	FIELD(net_benefit)

#define DUCKDB_PHYSICAL_RUNNER_AXIS_COST_FIELDS(FIELD)                                                                 \
	FIELD(runner_benefit)                                                                                              \
	FIELD(transfer_cost)                                                                                               \
	FIELD(startup_cost)                                                                                                \
	FIELD(required_benefit)                                                                                            \
	FIELD(net_benefit)

//! Numeric runner-cost storage shared by live profiles and accumulated totals.
//! The field lists above also generate every schema walk below.
struct PhysicalRunnerCostNumericFields {
#define DUCKDB_DECLARE_RUNNER_COST_FIELD(name) int64_t name = 0;
	DUCKDB_PHYSICAL_RUNNER_COST_SHAPE_FIELDS(DUCKDB_DECLARE_RUNNER_COST_FIELD)
	DUCKDB_PHYSICAL_RUNNER_COST_WORK_FIELDS(DUCKDB_DECLARE_RUNNER_COST_FIELD)
#undef DUCKDB_DECLARE_RUNNER_COST_FIELD
};

struct PhysicalRunnerAxisCostValues {
#define DUCKDB_DECLARE_RUNNER_AXIS_COST_FIELD(name) int64_t name = 0;
	DUCKDB_PHYSICAL_RUNNER_AXIS_COST_FIELDS(DUCKDB_DECLARE_RUNNER_AXIS_COST_FIELD)
#undef DUCKDB_DECLARE_RUNNER_AXIS_COST_FIELD
};

//! One accelerated runner's selection economics inside a cost profile.
struct PhysicalRunnerAxisCostBreakdown : PhysicalRunnerAxisCostValues {
	bool available = false;
	//! Whether this axis passed selection analysis on its own; the winning runner is
	//! the selected axis with the highest net benefit.
	bool selected = false;
	string selection_reason;
};

struct PhysicalRunnerCostProfile : PhysicalRunnerCostNumericFields {
	bool present = false;
	ExecutionRunnerKind selected_runner = ExecutionRunnerKind::VECTORIZED;
	PhysicalRunnerCostInputScope input_scope = PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE;
	bool source_contract_output_cardinality_unknown = false;
	bool full_pipeline = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
	string admission_class;
	string selection_reason;
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

//! The single authority over the summable runner-cost schema. Every surface that
//! enumerates these fields — totals accumulation, system-table columns and appenders,
//! profiler emission — derives from these two walks, so a new field is one line here.
//! Visit order is the external column order of the corresponding system-table group.
//! The paired form exists so accumulation can walk a profile and its totals in
//! lockstep; single-object consumers pass the same object twice. Fields with
//! non-summable types (bools, strings, the proof mask, selection state) are handled
//! by each surface explicitly and are not part of the numeric schema.
template <class A, class B, class FN>
void ForEachPhysicalRunnerCostShapeField(A &a, B &b, FN &&fn) {
#define DUCKDB_VISIT_RUNNER_COST_SHAPE_FIELD(name) fn(#name, a.name, b.name);
	DUCKDB_PHYSICAL_RUNNER_COST_SHAPE_FIELDS(DUCKDB_VISIT_RUNNER_COST_SHAPE_FIELD)
#undef DUCKDB_VISIT_RUNNER_COST_SHAPE_FIELD
}

template <class A, class B, class FN>
void ForEachPhysicalRunnerCostWorkField(A &a, B &b, FN &&fn) {
#define DUCKDB_VISIT_RUNNER_COST_WORK_FIELD(name) fn(#name, a.name, b.name);
	DUCKDB_PHYSICAL_RUNNER_COST_WORK_FIELDS(DUCKDB_VISIT_RUNNER_COST_WORK_FIELD)
#undef DUCKDB_VISIT_RUNNER_COST_WORK_FIELD
#define DUCKDB_VISIT_COMPILED_RUNNER_AXIS_COST_FIELD(name)                                                             \
	fn("compiled_vectorized_" #name, a.compiled_vectorized.name, b.compiled_vectorized.name);
	DUCKDB_PHYSICAL_RUNNER_AXIS_COST_FIELDS(DUCKDB_VISIT_COMPILED_RUNNER_AXIS_COST_FIELD)
#undef DUCKDB_VISIT_COMPILED_RUNNER_AXIS_COST_FIELD
#define DUCKDB_VISIT_GPU_RUNNER_AXIS_COST_FIELD(name) fn("gpu_" #name, a.gpu.name, b.gpu.name);
	DUCKDB_PHYSICAL_RUNNER_AXIS_COST_FIELDS(DUCKDB_VISIT_GPU_RUNNER_AXIS_COST_FIELD)
#undef DUCKDB_VISIT_GPU_RUNNER_AXIS_COST_FIELD
}

#undef DUCKDB_PHYSICAL_RUNNER_COST_SHAPE_FIELDS
#undef DUCKDB_PHYSICAL_RUNNER_COST_WORK_FIELDS
#undef DUCKDB_PHYSICAL_RUNNER_AXIS_COST_FIELDS

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

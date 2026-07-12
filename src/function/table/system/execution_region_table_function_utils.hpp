//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_table_function_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/execution_region_telemetry.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ExecutionRegionTraceColumn {
	const char *name;
	LogicalTypeId type;
};

template <class ENTRY>
struct ExecutionRegionTableFunctionState : public GlobalTableFunctionState {
	vector<ENTRY> entries;
	vector<column_t> column_ids;
	idx_t offset = 0;
};

static inline void AddExecutionRegionTableFunctionColumn(vector<LogicalType> &return_types, vector<string> &names,
                                                         const char *name, LogicalType type) {
	names.emplace_back(name);
	return_types.emplace_back(type);
}

static inline void AddExecutionRegionTableFunctionColumns(vector<LogicalType> &return_types, vector<string> &names,
                                                          const ExecutionRegionTraceColumn *columns,
                                                          idx_t column_count) {
	for (idx_t column_idx = 0; column_idx < column_count; column_idx++) {
		AddExecutionRegionTableFunctionColumn(return_types, names, columns[column_idx].name,
		                                      LogicalType(columns[column_idx].type));
	}
}

static constexpr ExecutionRegionTraceColumn EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMNS[] = {
    {"runner_cost_profile", LogicalTypeId::BOOLEAN},
    {"runner_cost_rows", LogicalTypeId::BIGINT},
    {"runner_cost_batches", LogicalTypeId::BIGINT},
    {"runner_cost_costed_batches", LogicalTypeId::BIGINT},
    {"runner_cost_expression_cost", LogicalTypeId::BIGINT},
    {"runner_cost_source_contract_input_rows", LogicalTypeId::BIGINT},
    {"runner_cost_source_contract_input_batches", LogicalTypeId::BIGINT},
    {"runner_cost_source_contract_output_cardinality_unknown", LogicalTypeId::BOOLEAN},
    {"runner_cost_generated_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_generated_backend_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_generated_grouped_aggregate_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_grouped_state_address_lookup_count", LogicalTypeId::BIGINT},
    {"runner_cost_materialization_elision_count", LogicalTypeId::BIGINT},
    {"runner_cost_selected_hash_join_filter_materialization_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_join_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_hash_join_build_sink_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_aggregate_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_grouped_aggregate_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_native_sort_stage_count", LogicalTypeId::BIGINT},
    {"runner_cost_full_pipeline", LogicalTypeId::BOOLEAN},
    {"runner_cost_input_scope", LogicalTypeId::VARCHAR},
    {"runner_cost_admission_class", LogicalTypeId::VARCHAR},
    {"runner_cost_selection_reason", LogicalTypeId::VARCHAR},
    {"runner_cost_grouped_aggregate_estimated_cardinality", LogicalTypeId::BIGINT},
    {"runner_cost_required_runtime_proofs", LogicalTypeId::VARCHAR},
};

static constexpr idx_t EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMN_COUNT =
    sizeof(EXECUTION_REGION_RUNNER_COST_PROFILE_COLUMNS) / sizeof(ExecutionRegionTraceColumn);

static constexpr ExecutionRegionTraceColumn EXECUTION_REGION_RUNNER_COST_WORK_COLUMNS[] = {
    {"runner_cost_generated_expression_work", LogicalTypeId::BIGINT},
    {"runner_cost_generated_stage_work", LogicalTypeId::BIGINT},
    {"runner_cost_generated_backend_stage_work", LogicalTypeId::BIGINT},
    {"runner_cost_native_operator_work", LogicalTypeId::BIGINT},
    {"runner_cost_materialization_elision_work", LogicalTypeId::BIGINT},
    {"runner_cost_selected_hash_join_filter_materialization_penalty", LogicalTypeId::BIGINT},
    {"runner_cost_source_contract_scan_penalty", LogicalTypeId::BIGINT},
    {"runner_cost_full_pipeline_work", LogicalTypeId::BIGINT},
    {"runner_cost_stateful_protocol_penalty", LogicalTypeId::BIGINT},
    {"runner_cost_saved_work_per_batch", LogicalTypeId::BIGINT},
    {"runner_cost_accelerated_runner_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_startup_cost", LogicalTypeId::BIGINT},
    {"runner_cost_required_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_net_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_compiled_vectorized_runner_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_compiled_vectorized_startup_cost", LogicalTypeId::BIGINT},
    {"runner_cost_compiled_vectorized_required_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_compiled_vectorized_net_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_gpu_runner_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_gpu_transfer_cost", LogicalTypeId::BIGINT},
    {"runner_cost_gpu_startup_cost", LogicalTypeId::BIGINT},
    {"runner_cost_gpu_required_benefit", LogicalTypeId::BIGINT},
    {"runner_cost_gpu_net_benefit", LogicalTypeId::BIGINT},
};

static constexpr idx_t EXECUTION_REGION_RUNNER_COST_WORK_COLUMN_COUNT =
    sizeof(EXECUTION_REGION_RUNNER_COST_WORK_COLUMNS) / sizeof(ExecutionRegionTraceColumn);

static constexpr ExecutionRegionTraceColumn EXECUTION_REGION_STAGE_TIMING_COLUMNS[] = {
    {"ir_lowering_time_us", LogicalTypeId::BIGINT},      {"backend_analysis_time_us", LogicalTypeId::BIGINT},
    {"codegen_time_us", LogicalTypeId::BIGINT},          {"pipeline_cbo_time_us", LogicalTypeId::BIGINT},
    {"graph_build_time_us", LogicalTypeId::BIGINT},      {"candidate_cbo_time_us", LogicalTypeId::BIGINT},
    {"executable_build_time_us", LogicalTypeId::BIGINT}, {"machine_codegen_time_us", LogicalTypeId::BIGINT},
    {"kernel_build_time_us", LogicalTypeId::BIGINT},
};

static constexpr idx_t EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT =
    sizeof(EXECUTION_REGION_STAGE_TIMING_COLUMNS) / sizeof(ExecutionRegionTraceColumn);

static constexpr ExecutionRegionTraceColumn EXECUTION_REGION_CANDIDATE_TRACE_COLUMNS[] = {
    {"candidate_signature_context", LogicalTypeId::VARCHAR},
    {"candidate_signature_shape", LogicalTypeId::VARCHAR},
    {"candidate_signature_feature_shape", LogicalTypeId::VARCHAR},
    {"candidate_signature_context_feature_shape", LogicalTypeId::VARCHAR},
    {"candidate_contract_shape", LogicalTypeId::VARCHAR},
    {"candidate_contract_abi", LogicalTypeId::VARCHAR},
    {"candidate_owns_source", LogicalTypeId::BOOLEAN},
    {"candidate_owns_transform", LogicalTypeId::BOOLEAN},
    {"candidate_owns_sink", LogicalTypeId::BOOLEAN},
    {"candidate_owns_state_scan", LogicalTypeId::BOOLEAN},
    {"candidate_has_source", LogicalTypeId::BOOLEAN},
    {"candidate_has_sink", LogicalTypeId::BOOLEAN},
    {"candidate_source_kind", LogicalTypeId::VARCHAR},
    {"candidate_source_execution", LogicalTypeId::VARCHAR},
    {"candidate_sink_kind", LogicalTypeId::VARCHAR},
    {"candidate_source_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_filter_expression_count", LogicalTypeId::UBIGINT},
    {"candidate_source_conjunction_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_operator_count", LogicalTypeId::UBIGINT},
    {"candidate_arithmetic_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_reference_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_source_ownership", LogicalTypeId::VARCHAR},
    {"candidate_state_scan_ownership", LogicalTypeId::VARCHAR},
    {"candidate_transform_ownership", LogicalTypeId::VARCHAR},
    {"candidate_sink_ownership", LogicalTypeId::VARCHAR},
    {"candidate_generated_operator_count", LogicalTypeId::UBIGINT},
    {"candidate_source_boundary_count", LogicalTypeId::UBIGINT},
    {"candidate_missing_contract_count", LogicalTypeId::UBIGINT},
    {"candidate_required_capabilities", LogicalTypeId::VARCHAR},
    {"candidate_fusion_blockers", LogicalTypeId::VARCHAR},
};

static constexpr idx_t EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT =
    sizeof(EXECUTION_REGION_CANDIDATE_TRACE_COLUMNS) / sizeof(ExecutionRegionTraceColumn);

static inline string FormatExecutionRegionStringList(const vector<string> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += values[value_idx];
	}
	result += "]";
	return result;
}

static inline void AppendExecutionRegionNullableString(Vector &output, const string &value) {
	if (value.empty()) {
		output.Append(Value(LogicalType::VARCHAR));
	} else {
		output.Append(Value(value));
	}
}

static inline void AppendExecutionRegionRunnerCostInputScope(Vector &output, PhysicalRunnerCostInputScope input_scope) {
	output.Append(Value(PhysicalRunnerCostInputScopeToString(input_scope)));
}

static inline void AppendExecutionRegionRunnerCostInputScope(Vector &output, const string &input_scope) {
	AppendExecutionRegionNullableString(output, input_scope);
}

static inline void AddExecutionRegionStageTimingColumns(vector<LogicalType> &return_types, vector<string> &names) {
	AddExecutionRegionTableFunctionColumns(return_types, names, EXECUTION_REGION_STAGE_TIMING_COLUMNS,
	                                       EXECUTION_REGION_STAGE_TIMING_COLUMN_COUNT);
}

static inline void AppendExecutionRegionStageTimingColumn(Vector &output, idx_t column_id,
                                                          const ExecutionRegionStageTimings &timings) {
	switch (column_id) {
	case 0:
		output.Append(Value::BIGINT(timings.ir_lowering_time_us));
		return;
	case 1:
		output.Append(Value::BIGINT(timings.backend_analysis_time_us));
		return;
	case 2:
		output.Append(Value::BIGINT(timings.codegen_time_us));
		return;
	case 3:
		output.Append(Value::BIGINT(timings.pipeline_cbo_time_us));
		return;
	case 4:
		output.Append(Value::BIGINT(timings.graph_build_time_us));
		return;
	case 5:
		output.Append(Value::BIGINT(timings.candidate_cbo_time_us));
		return;
	case 6:
		output.Append(Value::BIGINT(timings.executable_build_time_us));
		return;
	case 7:
		output.Append(Value::BIGINT(timings.machine_codegen_time_us));
		return;
	case 8:
		output.Append(Value::BIGINT(timings.kernel_build_time_us));
		return;
	default:
		throw InternalException("Unsupported execution region stage timing column index");
	}
}

template <class RUNNER_COST>
static inline void AppendExecutionRegionRunnerCostProfileColumn(Vector &output, idx_t column_id,
                                                                const RUNNER_COST &cost) {
	switch (column_id) {
	case 0:
		output.Append(Value::BOOLEAN(cost.present));
		return;
	case 1:
		output.Append(Value::BIGINT(cost.rows));
		return;
	case 2:
		output.Append(Value::BIGINT(cost.batches));
		return;
	case 3:
		output.Append(Value::BIGINT(cost.costed_batches));
		return;
	case 4:
		output.Append(Value::BIGINT(cost.expression_cost));
		return;
	case 5:
		output.Append(Value::BIGINT(cost.source_contract_input_rows));
		return;
	case 6:
		output.Append(Value::BIGINT(cost.source_contract_input_batches));
		return;
	case 7:
		output.Append(Value::BOOLEAN(cost.source_contract_output_cardinality_unknown));
		return;
	case 8:
		output.Append(Value::BIGINT(cost.generated_stage_count));
		return;
	case 9:
		output.Append(Value::BIGINT(cost.generated_backend_stage_count));
		return;
	case 10:
		output.Append(Value::BIGINT(cost.generated_grouped_aggregate_stage_count));
		return;
	case 11:
		output.Append(Value::BIGINT(cost.native_grouped_state_address_lookup_count));
		return;
	case 12:
		output.Append(Value::BIGINT(cost.materialization_elision_count));
		return;
	case 13:
		output.Append(Value::BIGINT(cost.selected_hash_join_filter_materialization_count));
		return;
	case 14:
		output.Append(Value::BIGINT(cost.native_join_stage_count));
		return;
	case 15:
		output.Append(Value::BIGINT(cost.native_hash_join_build_sink_count));
		return;
	case 16:
		output.Append(Value::BIGINT(cost.native_aggregate_stage_count));
		return;
	case 17:
		output.Append(Value::BIGINT(cost.native_grouped_aggregate_stage_count));
		return;
	case 18:
		output.Append(Value::BIGINT(cost.native_sort_stage_count));
		return;
	case 19:
		output.Append(Value::BOOLEAN(cost.full_pipeline));
		return;
	case 20:
		AppendExecutionRegionRunnerCostInputScope(output, cost.input_scope);
		return;
	case 21:
		AppendExecutionRegionNullableString(output, cost.admission_class);
		return;
	case 22:
		AppendExecutionRegionNullableString(output, cost.selection_reason);
		return;
	case 23:
		output.Append(Value::BIGINT(cost.grouped_aggregate_estimated_cardinality));
		return;
	case 24:
		AppendExecutionRegionNullableString(
		    output, RenderExecutionRegionJitRuntimeProofRequirements(cost.required_runtime_proofs));
		return;
	default:
		throw InternalException("Unsupported execution region runner cost profile column index");
	}
}

template <class RUNNER_COST>
static inline void AppendExecutionRegionRunnerCostWorkColumn(Vector &output, idx_t column_id, const RUNNER_COST &cost) {
	switch (column_id) {
	case 0:
		output.Append(Value::BIGINT(cost.generated_expression_work));
		return;
	case 1:
		output.Append(Value::BIGINT(cost.generated_stage_work));
		return;
	case 2:
		output.Append(Value::BIGINT(cost.generated_backend_stage_work));
		return;
	case 3:
		output.Append(Value::BIGINT(cost.native_operator_work));
		return;
	case 4:
		output.Append(Value::BIGINT(cost.materialization_elision_work));
		return;
	case 5:
		output.Append(Value::BIGINT(cost.selected_hash_join_filter_materialization_penalty));
		return;
	case 6:
		output.Append(Value::BIGINT(cost.source_contract_scan_penalty));
		return;
	case 7:
		output.Append(Value::BIGINT(cost.full_pipeline_work));
		return;
	case 8:
		output.Append(Value::BIGINT(cost.stateful_protocol_penalty));
		return;
	case 9:
		output.Append(Value::BIGINT(cost.saved_work_per_batch));
		return;
	case 10:
		output.Append(Value::BIGINT(cost.accelerated_runner_benefit));
		return;
	case 11:
		output.Append(Value::BIGINT(cost.startup_cost));
		return;
	case 12:
		output.Append(Value::BIGINT(cost.required_benefit));
		return;
	case 13:
		output.Append(Value::BIGINT(cost.net_benefit));
		return;
	case 14:
		output.Append(Value::BIGINT(cost.compiled_vectorized_runner_benefit));
		return;
	case 15:
		output.Append(Value::BIGINT(cost.compiled_vectorized_startup_cost));
		return;
	case 16:
		output.Append(Value::BIGINT(cost.compiled_vectorized_required_benefit));
		return;
	case 17:
		output.Append(Value::BIGINT(cost.compiled_vectorized_net_benefit));
		return;
	case 18:
		output.Append(Value::BIGINT(cost.gpu_runner_benefit));
		return;
	case 19:
		output.Append(Value::BIGINT(cost.gpu_transfer_cost));
		return;
	case 20:
		output.Append(Value::BIGINT(cost.gpu_startup_cost));
		return;
	case 21:
		output.Append(Value::BIGINT(cost.gpu_required_benefit));
		return;
	case 22:
		output.Append(Value::BIGINT(cost.gpu_net_benefit));
		return;
	default:
		throw InternalException("Unsupported execution region runner cost work column index");
	}
}

template <class ENTRY, class APPENDER>
static inline void EmitExecutionRegionTableFunctionRows(ExecutionRegionTableFunctionState<ENTRY> &data,
                                                        DataChunk &output, APPENDER append) {
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		for (idx_t col = 0; col < data.column_ids.size(); col++) {
			append(output.data[col], NumericCast<idx_t>(data.column_ids[col]), entry);
		}
		count++;
	}
	if (data.column_ids.empty()) {
		output.SetChildCardinality(count);
	} else {
		output.CheckCardinality(count);
	}
}

static inline void AppendNullExecutionRegionCandidateTraceColumn(Vector &output, idx_t column_id) {
	if (column_id >= EXECUTION_REGION_CANDIDATE_TRACE_COLUMN_COUNT) {
		throw InternalException("Unsupported execution region candidate trace column index");
	}
	output.Append(Value(LogicalType(EXECUTION_REGION_CANDIDATE_TRACE_COLUMNS[column_id].type)));
}

static inline void AppendExecutionRegionCandidateTraceColumn(Vector &output, idx_t column_id,
                                                             const ExecutionRegionSignature &signature,
                                                             const ExecutionRegionCandidateTraits &traits,
                                                             const ExecutionRegionContract &contract) {
	switch (column_id) {
	case 0:
		output.Append(Value(signature.context));
		return;
	case 1:
		output.Append(Value(signature.shape));
		return;
	case 2:
		output.Append(Value(signature.feature_shape));
		return;
	case 3:
		output.Append(Value(signature.context_feature_shape));
		return;
	case 4:
		output.Append(Value(signature.contract_shape));
		return;
	case 5:
		output.Append(Value(ExecutionRegionABIToString(contract.abi)));
		return;
	case 6:
		output.Append(Value::BOOLEAN(contract.OwnsSource()));
		return;
	case 7:
		output.Append(Value::BOOLEAN(contract.OwnsTransform()));
		return;
	case 8:
		output.Append(Value::BOOLEAN(contract.OwnsSink()));
		return;
	case 9:
		output.Append(Value::BOOLEAN(contract.OwnsStateScan()));
		return;
	case 10:
		output.Append(Value::BOOLEAN(traits.HasSource()));
		return;
	case 11:
		output.Append(Value::BOOLEAN(traits.HasSink()));
		return;
	case 12:
		output.Append(Value(ExecutionRegionSourceKindToString(traits.source_kind)));
		return;
	case 13:
		output.Append(Value(ExecutionRegionSourceExecutionKindToString(traits.source_execution)));
		return;
	case 14:
		output.Append(Value(ExecutionRegionSinkKindToString(traits.sink_kind)));
		return;
	case 15:
		output.Append(Value::UBIGINT(traits.source_filter_count));
		return;
	case 16:
		output.Append(Value::UBIGINT(traits.source_filter_expression_count));
		return;
	case 17:
		output.Append(Value::UBIGINT(traits.source_conjunction_filter_count));
		return;
	case 18:
		output.Append(Value::UBIGINT(traits.filter_count));
		return;
	case 19:
		output.Append(Value::UBIGINT(traits.projection_count));
		return;
	case 20:
		output.Append(Value::UBIGINT(traits.operator_count));
		return;
	case 21:
		output.Append(Value::UBIGINT(traits.arithmetic_projection_count));
		return;
	case 22:
		output.Append(Value::UBIGINT(traits.reference_projection_count));
		return;
	case 23:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.source_ownership)));
		return;
	case 24:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.state_scan_ownership)));
		return;
	case 25:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.transform_ownership)));
		return;
	case 26:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.sink_ownership)));
		return;
	case 27:
		output.Append(Value::UBIGINT(contract.generated_operator_count));
		return;
	case 28:
		output.Append(Value::UBIGINT(contract.source_boundary_count));
		return;
	case 29:
		output.Append(Value::UBIGINT(contract.missing_contract_count));
		return;
	case 30:
		output.Append(Value(FormatExecutionRegionStringList(contract.required_capabilities)));
		return;
	case 31:
		output.Append(Value(FormatExecutionRegionStringList(contract.blockers)));
		return;
	default:
		throw InternalException("Unsupported execution region candidate trace column index");
	}
}

static inline void AddExecutionRegionCandidateTraceColumns(vector<LogicalType> &return_types, vector<string> &names) {
	for (const auto &column : EXECUTION_REGION_CANDIDATE_TRACE_COLUMNS) {
		names.emplace_back(column.name);
		return_types.emplace_back(column.type);
	}
}

} // namespace duckdb

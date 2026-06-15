//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_jit_table_function_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/jit/region.hpp"

namespace duckdb {

static inline string FormatJitTableFunctionStringList(const vector<string> &values) {
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

static inline void AddJitCandidateTraceColumns(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("candidate_contract_abi");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_contract_first_node");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_contract_node_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_contract_start_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_contract_end_operator_index");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_owns_source");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_owns_transform");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_owns_sink");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_owns_state_scan");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_has_source");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_has_sink");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_source_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_source_execution");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_sink_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_has_table_scan_source");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_has_stateful_source");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_expression_traits_known");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_source_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_filter_expression_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_filter_fallback_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_integer_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_non_integer_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_conjunction_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_projected_column_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_returned_column_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_projection_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_operator_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_core_expression_operator_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_arithmetic_projection_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_integer_arithmetic_projection_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_non_integer_arithmetic_projection_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_reference_projection_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_integer_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_non_integer_comparison_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_conjunction_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_expression_fallback_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_operator_fallback_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_scan_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_sink_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_state_scan_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_transform_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_sink_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_executor_boundary_free");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_native_fusion_ready");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_generated_operator_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_typed_helper_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_executor_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_missing_protocol_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_required_capabilities");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_fusion_blockers");
	return_types.emplace_back(LogicalType::VARCHAR);
}

static inline idx_t AppendNullJitCandidateTraceColumns(DataChunk &output, idx_t column_offset) {
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	for (idx_t count_idx = 0; count_idx < 5; count_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::UBIGINT));
	}
	for (idx_t boolean_idx = 0; boolean_idx < 4; boolean_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	}
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	for (idx_t count_idx = 0; count_idx < 25; count_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::UBIGINT));
	}
	for (idx_t string_idx = 0; string_idx < 4; string_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	}
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	for (idx_t count_idx = 0; count_idx < 4; count_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::UBIGINT));
	}
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	return column_offset;
}

static inline idx_t AppendJitCandidateTraceColumns(DataChunk &output, idx_t column_offset,
                                                   const JitRegionCandidateTraits &traits,
                                                   const JitRegionContract &contract) {
	output.data[column_offset++].Append(Value(JitRegionABIToString(contract.abi)));
	output.data[column_offset++].Append(Value::UBIGINT(contract.first_node));
	output.data[column_offset++].Append(Value::UBIGINT(contract.node_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.start_operator_index));
	output.data[column_offset++].Append(Value::UBIGINT(contract.end_operator_index));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.owns_source));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.owns_transform));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.owns_sink));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.owns_state_scan));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.has_source));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.has_sink));
	output.data[column_offset++].Append(Value(JitRegionSourceKindToString(traits.source_kind)));
	output.data[column_offset++].Append(Value(JitRegionSourceExecutionKindToString(traits.source_execution)));
	output.data[column_offset++].Append(Value(JitRegionSinkKindToString(traits.sink_kind)));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.has_table_scan_source));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.has_stateful_source));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.expression_traits_known));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_expression_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_fallback_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_integer_comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_non_integer_comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_conjunction_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_projected_column_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_returned_column_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.projection_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.operator_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.core_expression_operator_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.arithmetic_projection_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.integer_arithmetic_projection_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.non_integer_arithmetic_projection_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.reference_projection_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.integer_comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.non_integer_comparison_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.conjunction_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.expression_fallback_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.operator_fallback_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.scan_boundary_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.sink_boundary_count));
	output.data[column_offset++].Append(Value(JitRegionOwnershipKindToString(contract.source_ownership)));
	output.data[column_offset++].Append(Value(JitRegionOwnershipKindToString(contract.state_scan_ownership)));
	output.data[column_offset++].Append(Value(JitRegionOwnershipKindToString(contract.transform_ownership)));
	output.data[column_offset++].Append(Value(JitRegionOwnershipKindToString(contract.sink_ownership)));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.executor_boundary_free));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.native_fusion_ready));
	output.data[column_offset++].Append(Value::UBIGINT(contract.generated_operator_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.source_boundary_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.typed_helper_boundary_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.executor_boundary_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.missing_protocol_count));
	output.data[column_offset++].Append(Value(FormatJitTableFunctionStringList(contract.required_capabilities)));
	output.data[column_offset++].Append(Value(FormatJitTableFunctionStringList(contract.blockers)));
	return column_offset;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_table_function_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

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

static inline void AddExecutionRegionCandidateTraceColumns(vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("candidate_signature_context");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_signature_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_signature_feature_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_signature_context_feature_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_contract_shape");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_signature_ir");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_contract_abi");
	return_types.emplace_back(LogicalType::VARCHAR);
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
	names.emplace_back("candidate_expression_traits_known");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("candidate_source_filter_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_filter_expression_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_filter_missing_count");
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
	names.emplace_back("candidate_expression_missing_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_operator_missing_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_state_scan_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_transform_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_sink_ownership");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_generated_operator_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_source_boundary_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_missing_contract_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("candidate_required_capabilities");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("candidate_fusion_blockers");
	return_types.emplace_back(LogicalType::VARCHAR);
}

static inline idx_t AppendNullExecutionRegionCandidateTraceColumns(DataChunk &output, idx_t column_offset) {
	for (idx_t string_idx = 0; string_idx < 6; string_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	}
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	for (idx_t boolean_idx = 0; boolean_idx < 4; boolean_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	}
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::BOOLEAN));
	for (idx_t count_idx = 0; count_idx < 23; count_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::UBIGINT));
	}
	for (idx_t string_idx = 0; string_idx < 4; string_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	}
	for (idx_t count_idx = 0; count_idx < 3; count_idx++) {
		output.data[column_offset++].Append(Value(LogicalType::UBIGINT));
	}
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	return column_offset;
}

static inline idx_t AppendExecutionRegionCandidateTraceColumns(DataChunk &output, idx_t column_offset,
                                                   const ExecutionRegionSignature &signature,
                                                   const ExecutionRegionCandidateTraits &traits,
                                                   const ExecutionRegionContract &contract) {
	output.data[column_offset++].Append(Value(signature.context));
	output.data[column_offset++].Append(Value(signature.shape));
	output.data[column_offset++].Append(Value(signature.feature_shape));
	output.data[column_offset++].Append(Value(signature.context_feature_shape));
	output.data[column_offset++].Append(Value(signature.contract_shape));
	if (signature.ir.empty()) {
		output.data[column_offset++].Append(Value(LogicalType::VARCHAR));
	} else {
		output.data[column_offset++].Append(Value(signature.ir));
	}
	output.data[column_offset++].Append(Value(ExecutionRegionABIToString(contract.abi)));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.OwnsSource()));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.OwnsTransform()));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.OwnsSink()));
	output.data[column_offset++].Append(Value::BOOLEAN(contract.OwnsStateScan()));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.HasSource()));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.HasSink()));
	output.data[column_offset++].Append(Value(ExecutionRegionSourceKindToString(traits.source_kind)));
	output.data[column_offset++].Append(Value(ExecutionRegionSourceExecutionKindToString(traits.source_execution)));
	output.data[column_offset++].Append(Value(ExecutionRegionSinkKindToString(traits.sink_kind)));
	output.data[column_offset++].Append(Value::BOOLEAN(traits.expression_traits_known));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_expression_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.source_filter_missing_count));
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
	output.data[column_offset++].Append(Value::UBIGINT(traits.expression_missing_count));
	output.data[column_offset++].Append(Value::UBIGINT(traits.operator_missing_count));
	output.data[column_offset++].Append(Value(ExecutionRegionOwnershipKindToString(contract.source_ownership)));
	output.data[column_offset++].Append(Value(ExecutionRegionOwnershipKindToString(contract.state_scan_ownership)));
	output.data[column_offset++].Append(Value(ExecutionRegionOwnershipKindToString(contract.transform_ownership)));
	output.data[column_offset++].Append(Value(ExecutionRegionOwnershipKindToString(contract.sink_ownership)));
	output.data[column_offset++].Append(Value::UBIGINT(contract.generated_operator_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.source_boundary_count));
	output.data[column_offset++].Append(Value::UBIGINT(contract.missing_contract_count));
	output.data[column_offset++].Append(Value(FormatExecutionRegionStringList(contract.required_capabilities)));
	output.data[column_offset++].Append(Value(FormatExecutionRegionStringList(contract.blockers)));
	return column_offset;
}

} // namespace duckdb

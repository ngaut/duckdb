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

static constexpr ExecutionRegionTraceColumn EXECUTION_REGION_CANDIDATE_TRACE_COLUMNS[] = {
    {"candidate_signature_context", LogicalTypeId::VARCHAR},
    {"candidate_signature_shape", LogicalTypeId::VARCHAR},
    {"candidate_signature_feature_shape", LogicalTypeId::VARCHAR},
    {"candidate_signature_context_feature_shape", LogicalTypeId::VARCHAR},
    {"candidate_contract_shape", LogicalTypeId::VARCHAR},
    {"candidate_signature_ir", LogicalTypeId::VARCHAR},
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
    {"candidate_expression_traits_known", LogicalTypeId::BOOLEAN},
    {"candidate_source_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_filter_expression_count", LogicalTypeId::UBIGINT},
    {"candidate_source_filter_missing_count", LogicalTypeId::UBIGINT},
    {"candidate_source_contract_input_column_count", LogicalTypeId::UBIGINT},
    {"candidate_source_contract_filter_prune_required", LogicalTypeId::BOOLEAN},
    {"candidate_source_projection_pushdown", LogicalTypeId::BOOLEAN},
    {"candidate_source_filter_pushdown", LogicalTypeId::BOOLEAN},
    {"candidate_source_filter_prune", LogicalTypeId::BOOLEAN},
    {"candidate_source_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_integer_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_non_integer_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_conjunction_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_source_projected_column_count", LogicalTypeId::UBIGINT},
    {"candidate_source_returned_column_count", LogicalTypeId::UBIGINT},
    {"candidate_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_operator_count", LogicalTypeId::UBIGINT},
    {"candidate_core_expression_operator_count", LogicalTypeId::UBIGINT},
    {"candidate_arithmetic_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_integer_arithmetic_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_non_integer_arithmetic_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_reference_projection_count", LogicalTypeId::UBIGINT},
    {"candidate_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_integer_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_non_integer_comparison_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_conjunction_filter_count", LogicalTypeId::UBIGINT},
    {"candidate_expression_missing_count", LogicalTypeId::UBIGINT},
    {"candidate_operator_missing_count", LogicalTypeId::UBIGINT},
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
		if (signature.ir.empty()) {
			output.Append(Value(LogicalType::VARCHAR));
		} else {
			output.Append(Value(signature.ir));
		}
		return;
	case 6:
		output.Append(Value(ExecutionRegionABIToString(contract.abi)));
		return;
	case 7:
		output.Append(Value::BOOLEAN(contract.OwnsSource()));
		return;
	case 8:
		output.Append(Value::BOOLEAN(contract.OwnsTransform()));
		return;
	case 9:
		output.Append(Value::BOOLEAN(contract.OwnsSink()));
		return;
	case 10:
		output.Append(Value::BOOLEAN(contract.OwnsStateScan()));
		return;
	case 11:
		output.Append(Value::BOOLEAN(traits.HasSource()));
		return;
	case 12:
		output.Append(Value::BOOLEAN(traits.HasSink()));
		return;
	case 13:
		output.Append(Value(ExecutionRegionSourceKindToString(traits.source_kind)));
		return;
	case 14:
		output.Append(Value(ExecutionRegionSourceExecutionKindToString(traits.source_execution)));
		return;
	case 15:
		output.Append(Value(ExecutionRegionSinkKindToString(traits.sink_kind)));
		return;
	case 16:
		output.Append(Value::BOOLEAN(traits.expression_traits_known));
		return;
	case 17:
		output.Append(Value::UBIGINT(traits.source_filter_count));
		return;
	case 18:
		output.Append(Value::UBIGINT(traits.source_filter_expression_count));
		return;
	case 19:
		output.Append(Value::UBIGINT(traits.source_filter_missing_count));
		return;
	case 20:
		output.Append(Value::UBIGINT(traits.source_contract_input_column_count));
		return;
	case 21:
		output.Append(Value::BOOLEAN(traits.source_contract_filter_prune_required));
		return;
	case 22:
		output.Append(Value::BOOLEAN(traits.source_projection_pushdown));
		return;
	case 23:
		output.Append(Value::BOOLEAN(traits.source_filter_pushdown));
		return;
	case 24:
		output.Append(Value::BOOLEAN(traits.source_filter_prune));
		return;
	case 25:
		output.Append(Value::UBIGINT(traits.source_comparison_filter_count));
		return;
	case 26:
		output.Append(Value::UBIGINT(traits.source_integer_comparison_filter_count));
		return;
	case 27:
		output.Append(Value::UBIGINT(traits.source_non_integer_comparison_filter_count));
		return;
	case 28:
		output.Append(Value::UBIGINT(traits.source_conjunction_filter_count));
		return;
	case 29:
		output.Append(Value::UBIGINT(traits.source_projected_column_count));
		return;
	case 30:
		output.Append(Value::UBIGINT(traits.source_returned_column_count));
		return;
	case 31:
		output.Append(Value::UBIGINT(traits.filter_count));
		return;
	case 32:
		output.Append(Value::UBIGINT(traits.projection_count));
		return;
	case 33:
		output.Append(Value::UBIGINT(traits.operator_count));
		return;
	case 34:
		output.Append(Value::UBIGINT(traits.core_expression_operator_count));
		return;
	case 35:
		output.Append(Value::UBIGINT(traits.arithmetic_projection_count));
		return;
	case 36:
		output.Append(Value::UBIGINT(traits.integer_arithmetic_projection_count));
		return;
	case 37:
		output.Append(Value::UBIGINT(traits.non_integer_arithmetic_projection_count));
		return;
	case 38:
		output.Append(Value::UBIGINT(traits.reference_projection_count));
		return;
	case 39:
		output.Append(Value::UBIGINT(traits.comparison_filter_count));
		return;
	case 40:
		output.Append(Value::UBIGINT(traits.integer_comparison_filter_count));
		return;
	case 41:
		output.Append(Value::UBIGINT(traits.non_integer_comparison_filter_count));
		return;
	case 42:
		output.Append(Value::UBIGINT(traits.conjunction_filter_count));
		return;
	case 43:
		output.Append(Value::UBIGINT(traits.expression_missing_count));
		return;
	case 44:
		output.Append(Value::UBIGINT(traits.operator_missing_count));
		return;
	case 45:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.source_ownership)));
		return;
	case 46:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.state_scan_ownership)));
		return;
	case 47:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.transform_ownership)));
		return;
	case 48:
		output.Append(Value(ExecutionRegionOwnershipKindToString(contract.sink_ownership)));
		return;
	case 49:
		output.Append(Value::UBIGINT(contract.generated_operator_count));
		return;
	case 50:
		output.Append(Value::UBIGINT(contract.source_boundary_count));
		return;
	case 51:
		output.Append(Value::UBIGINT(contract.missing_contract_count));
		return;
	case 52:
		output.Append(Value(FormatExecutionRegionStringList(contract.required_capabilities)));
		return;
	case 53:
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

#include "execution_region_description.hpp"

#include "execution_region_source_contract.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/execution_contract.hpp"

namespace duckdb {

static string ExecutionRegionBool(bool value) {
	return value ? "true" : "false";
}

static string BuildExecutionRegionLogicalTypeList(const vector<LogicalType> &types) {
	string result = "[";
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += types[type_idx].ToString();
	}
	result += "]";
	return result;
}

static string ExecutionRegionTypeDescriptor(const LogicalType &type) {
	return "logical=" + type.ToString() + ",physical=" + TypeIdToString(type.InternalType());
}

static string BuildExecutionRegionIdxList(const vector<idx_t> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += std::to_string(values[value_idx]);
	}
	result += "]";
	return result;
}

static string ExecutionHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode) {
	switch (mode) {
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
		return "matched_probe_and_build";
	case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD:
		return "left_probe_and_build";
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
		return "matched_probe_only";
	case ExecutionHashJoinProbeOutputMode::MARK_PROBE:
		return "mark_probe";
	case ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
		return "mark_build_only";
	default:
		return "none";
	}
}

static string ExecutionHashJoinResidualSourceKindToString(ExecutionHashJoinResidualSourceKind kind) {
	switch (kind) {
	case ExecutionHashJoinResidualSourceKind::PROBE:
		return "probe";
	case ExecutionHashJoinResidualSourceKind::BUILD:
		return "build";
	default:
		return "unknown";
	}
}

static string DescribeExecutionHashJoinResidualSources(const vector<ExecutionHashJoinResidualSource> &sources) {
	string result = "[";
	for (idx_t source_idx = 0; source_idx < sources.size(); source_idx++) {
		if (source_idx > 0) {
			result += "|";
		}
		auto &source = sources[source_idx];
		result += std::to_string(source.source_index);
		result += ":";
		result += ExecutionHashJoinResidualSourceKindToString(source.kind);
		result += ":input=" + std::to_string(source.input_index);
		result += ":not_null=" + ExecutionRegionBool(source.not_null);
		result += ":";
		const auto &source_logical_type = source.type;
		result += ExecutionRegionTypeDescriptor(source_logical_type);
	}
	result += "]";
	return result;
}

static string BuildExecutionRegionValueList(const vector<Value> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += values[value_idx].ToString();
	}
	result += "]";
	return result;
}

static string BuildExecutionRegionStringList(const vector<string> &values) {
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

static string BuildExecutionRegionComparisonTypeList(const vector<ExecutionRegionComparisonType> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += ExecutionRegionComparisonTypeToString(values[value_idx]);
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionSourceFilter(const ExecutionRegionSourceFilter &filter) {
	string result = "filter<index=" + std::to_string(filter.filter_index);
	result += ",scan_column=" + std::to_string(filter.scan_column_index);
	result += ",table_column=" + std::to_string(filter.table_column_index);
	result += ",generated_source_stage_candidate=";
	result += ExecutionRegionBool(filter.generated_source_stage_candidate);
	result += ">(";
	if (filter.expression) {
		result += filter.expression->ir;
	} else {
		result += "blocker:" + filter.reason;
	}
	result += ")";
	return result;
}

static string DescribeExecutionRegionContractFields(const vector<ExecutionRegionContractField> &fields) {
	string result = "[";
	for (idx_t field_idx = 0; field_idx < fields.size(); field_idx++) {
		if (field_idx > 0) {
			result += "|";
		}
		result += fields[field_idx].name;
		result += "=";
		result += fields[field_idx].value;
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionTableScanContract(const ExecutionRegionTableScanContract &contract) {
	if (!contract.present) {
		return string();
	}
	string result = "table_scan_contract<function=" + contract.function_name;
	result += ",estimated_source_cardinality=" + std::to_string(contract.estimated_source_cardinality);
	result += ",output_columns=" + std::to_string(contract.output_column_count);
	result += ",returned_columns=" + std::to_string(contract.returned_column_count);
	result += ",column_ids=" + std::to_string(contract.column_id_count);
	result += ",projected_columns=" + std::to_string(contract.projected_column_count);
	result += ",column_id_bindings=" + BuildExecutionRegionIdxList(contract.column_ids);
	result += ",projection_ids=" + BuildExecutionRegionIdxList(contract.projection_ids);
	result += ",source_contract_input_columns=" + std::to_string(contract.source_contract_input_column_count);
	result +=
	    ",source_contract_input_types=" + BuildExecutionRegionLogicalTypeList(contract.source_contract_input_types);
	result += ",source_contract_input_distinct_counts=" +
	          BuildExecutionRegionIdxList(contract.source_contract_input_distinct_counts);
	result +=
	    ",source_contract_input_min_values=" + BuildExecutionRegionValueList(contract.source_contract_input_min_values);
	result +=
	    ",source_contract_input_max_values=" + BuildExecutionRegionValueList(contract.source_contract_input_max_values);
	result += ",source_contract_output_projection_map=" +
	          BuildExecutionRegionIdxList(contract.source_contract_output_projection_map);
	result +=
	    ",source_contract_filter_prune_required=" + ExecutionRegionBool(contract.source_contract_filter_prune_required);
	result += ",projection_pushdown=" + ExecutionRegionBool(contract.projection_pushdown);
	result += ",filter_pushdown=" + ExecutionRegionBool(contract.filter_pushdown);
	result += ",filter_prune=" + ExecutionRegionBool(contract.filter_prune);
	result += ",dynamic_filters=" + ExecutionRegionBool(contract.dynamic_filters);
	result += ",in_out_function=" + ExecutionRegionBool(contract.in_out_function);
	result += ",filter_count=" + std::to_string(contract.filter_count);
	result += ">";
	return result;
}

static string DescribeExecutionRegionNativeOperatorContract(const ExecutionRegionNativeOperatorContract &contract,
                                                            const string &prefix);
static void AppendExecutionRegionContractIR(string &result, const string &ir);

static string DescribeExecutionRegionExpressionFragmentIR(const ExecutionExpressionFragment &fragment) {
	if (!fragment.ir.empty()) {
		return fragment.ir;
	}
	if (!fragment.root) {
		return string();
	}
	return "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*fragment.root);
}

static string DescribeExecutionRegionHashJoinContract(const ExecutionRegionHashJoinContract &contract) {
	if (!contract.present) {
		return string();
	}
	string result = "hash_join_contract<join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	result += ",condition_count=" + std::to_string(contract.condition_count);
	result += ",equality_condition_count=" + std::to_string(contract.equality_condition_count);
	result += ",non_equality_condition_count=" + std::to_string(contract.non_equality_condition_count);
	result += ",null_equal_condition_count=" + std::to_string(contract.null_equal_condition_count);
	result += ",condition_types=" + BuildExecutionRegionLogicalTypeList(contract.condition_types);
	result += ",comparison_ops=" + BuildExecutionRegionComparisonTypeList(contract.comparison_types);
	result += ",payload_columns=" + std::to_string(contract.payload_column_count);
	result += ",payload_column_indices=" + BuildExecutionRegionIdxList(contract.payload_column_indices);
	result += ",payload_types=" + BuildExecutionRegionLogicalTypeList(contract.payload_types);
	result += ",lhs_output_columns=" + std::to_string(contract.lhs_output_column_count);
	result += ",lhs_output_column_indices=" + BuildExecutionRegionIdxList(contract.lhs_output_column_indices);
	result += ",lhs_output_types=" + BuildExecutionRegionLogicalTypeList(contract.lhs_output_types);
	result += ",rhs_output_columns=" + std::to_string(contract.rhs_output_column_count);
	result += ",rhs_output_types=" + BuildExecutionRegionLogicalTypeList(contract.rhs_output_types);
	result += ",lhs_probe_columns=" + std::to_string(contract.lhs_probe_column_count);
	result += ",lhs_probe_column_indices=" + BuildExecutionRegionIdxList(contract.lhs_probe_column_indices);
	result += ",lhs_probe_types=" + BuildExecutionRegionLogicalTypeList(contract.lhs_probe_types);
	result += ",lhs_output_in_probe=" + std::to_string(contract.lhs_output_in_probe_count);
	result += ",delim_types=" + std::to_string(contract.delim_type_count);
	result += ",correlated_mark_counts_required=" + ExecutionRegionBool(contract.correlated_mark_counts_required);
	result += ",residual_predicate=" + ExecutionRegionBool(contract.residual_predicate);
	result += ",residual_info=" + ExecutionRegionBool(contract.residual_info);
	result += ",residual_expression_ready=" + ExecutionRegionBool(contract.residual_expression_ready);
	result += ",residual_expression_blocker=" + contract.residual_expression_blocker;
	result += ",residual_sources=" + DescribeExecutionHashJoinResidualSources(contract.residual_sources);
	if (contract.residual_expression_ready) {
		auto expression_ir = DescribeExecutionRegionExpressionFragmentIR(contract.residual_expression);
		if (!expression_ir.empty()) {
			result += ",residual_expression_ir=(" + expression_ir + ")";
		}
	}
	result += ",filter_pushdown=" + ExecutionRegionBool(contract.filter_pushdown);
	result += ",filter_pushdown_condition_count=" + std::to_string(contract.filter_pushdown_condition_count);
	result += ",filter_pushdown_probe_count=" + std::to_string(contract.filter_pushdown_probe_count);
	result += ",build_side_has_filter=" + ExecutionRegionBool(contract.build_side_has_filter);
	result += ",source_produces_rows=" + ExecutionRegionBool(contract.source_produces_rows);
	result += ",regular_hash_table_layout_ready=" + ExecutionRegionBool(contract.regular_hash_table_layout_ready);
	result += ",native_probe_shape_ready=" + ExecutionRegionBool(contract.native_probe_shape_ready);
	result += ",native_probe_shape_blocker=" + contract.native_probe_shape_blocker;
	result +=
	    ",native_probe_output_mode=" + ExecutionHashJoinProbeOutputModeToString(contract.native_probe_output_mode);
	result += ",build_sink_shape_ready=" + ExecutionRegionBool(contract.build_sink_shape_ready);
	result += ",build_sink_shape_blocker=" + contract.build_sink_shape_blocker;
	result += ",hash_join_layout_column_count=" + std::to_string(contract.layout_column_count);
	result += ",hash_join_layout_offsets=" + BuildExecutionRegionIdxList(contract.layout_offsets);
	result += ",hash_join_tuple_size=" + std::to_string(contract.tuple_size);
	result += ",hash_join_entry_size=" + std::to_string(contract.entry_size);
	result += ",hash_join_pointer_offset=" + std::to_string(contract.pointer_offset);
	result += ",hash_join_hash_column_index=" + std::to_string(contract.hash_column_index);
	result += ",hash_join_found_match_column_present=" + ExecutionRegionBool(contract.found_match_column_present);
	result += ",hash_join_found_match_column_index=" + std::to_string(contract.found_match_column_index);
	result += ",hash_join_native_contract_blocker=" + contract.native_contract_blocker;
	AppendExecutionRegionContractIR(result, contract.native_probe_contract.ir);
	AppendExecutionRegionContractIR(result, contract.native_build_contract.ir);
	result += ">";
	return result;
}

static string
DescribeExecutionRegionNestedLoopJoinCondition(const ExecutionRegionNestedLoopJoinConditionInput &condition) {
	string result = "condition";
	result += std::to_string(condition.condition_index);
	result += "<type=" + condition.type.ToString();
	result += ",comparison=" + string(ExecutionRegionComparisonTypeToString(condition.comparison_type));
	result += ",lhs_ready=" + ExecutionRegionBool(condition.lhs_expression_ready);
	if (!condition.lhs_expression_blocker.empty()) {
		result += ",lhs_blocker=" + condition.lhs_expression_blocker;
	}
	if (condition.lhs_expression_ready) {
		auto lhs_ir = DescribeExecutionRegionExpressionFragmentIR(condition.lhs_expression);
		if (!lhs_ir.empty()) {
			result += ",lhs_ir=(" + lhs_ir + ")";
		}
	}
	result += ",rhs_ready=" + ExecutionRegionBool(condition.rhs_expression_ready);
	if (!condition.rhs_expression_blocker.empty()) {
		result += ",rhs_blocker=" + condition.rhs_expression_blocker;
	}
	if (condition.rhs_expression_ready) {
		auto rhs_ir = DescribeExecutionRegionExpressionFragmentIR(condition.rhs_expression);
		if (!rhs_ir.empty()) {
			result += ",rhs_ir=(" + rhs_ir + ")";
		}
	}
	result += ">";
	return result;
}

static string
DescribeExecutionRegionNestedLoopJoinConditions(const vector<ExecutionRegionNestedLoopJoinConditionInput> &conditions) {
	string result = "[";
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += "|";
		}
		result += conditions[condition_idx].ir;
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionNestedLoopJoinContract(const ExecutionRegionNestedLoopJoinContract &contract) {
	if (!contract.present) {
		return string();
	}
	string result =
	    "nested_loop_join_contract<join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	result += ",condition_count=" + std::to_string(contract.condition_count);
	result += ",comparison_condition_count=" + std::to_string(contract.comparison_condition_count);
	result += ",condition_types=" + BuildExecutionRegionLogicalTypeList(contract.condition_types);
	result += ",comparison_ops=" + BuildExecutionRegionComparisonTypeList(contract.comparison_types);
	result += ",lhs_input_types=" + BuildExecutionRegionLogicalTypeList(contract.lhs_input_types);
	result += ",rhs_input_types=" + BuildExecutionRegionLogicalTypeList(contract.rhs_input_types);
	result += ",output_types=" + BuildExecutionRegionLogicalTypeList(contract.output_types);
	result += ",simple_join=" + ExecutionRegionBool(contract.simple_join);
	result += ",complex_join=" + ExecutionRegionBool(contract.complex_join);
	result += ",source_produces_rows=" + ExecutionRegionBool(contract.source_produces_rows);
	result += ",residual_predicate=" + ExecutionRegionBool(contract.residual_predicate);
	result += ",filter_pushdown=" + ExecutionRegionBool(contract.filter_pushdown);
	result += ",conditions_ready=" + ExecutionRegionBool(contract.conditions_ready);
	result += ",condition_blocker=" + contract.condition_blocker;
	result += ",native_probe_shape_ready=" + ExecutionRegionBool(contract.native_probe_shape_ready);
	result += ",native_probe_shape_blocker=" + contract.native_probe_shape_blocker;
	result += ",build_sink_shape_ready=" + ExecutionRegionBool(contract.build_sink_shape_ready);
	result += ",build_sink_shape_blocker=" + contract.build_sink_shape_blocker;
	result += ",conditions=" + DescribeExecutionRegionNestedLoopJoinConditions(contract.conditions);
	AppendExecutionRegionContractIR(result, contract.native_probe_contract.ir);
	AppendExecutionRegionContractIR(result, contract.native_build_contract.ir);
	result += ">";
	return result;
}

static string
DescribeExecutionRegionNativeGroupedStateContract(const ExecutionRegionNativeGroupedStateContract &contract) {
	if (contract.status == ExecutionRegionStateContractStatus::NONE) {
		return string();
	}
	string result =
	    "native_grouped_state_contract_status=" + string(ExecutionRegionStateContractStatusToString(contract.status));
	result += ",native_grouped_state_required_capability=" + contract.required_capability;
	result += ",native_grouped_state_contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		result += ",native_grouped_state_blocker=" + contract.blocker;
	}
	return result;
}

static string DescribeExecutionRegionNativeStateScanContract(const ExecutionRegionNativeStateScanContract &contract) {
	if (contract.status == ExecutionRegionStateContractStatus::NONE) {
		return string();
	}
	string result =
	    "native_state_scan_contract_status=" + string(ExecutionRegionStateContractStatusToString(contract.status));
	result += ",native_state_scan_required_capability=" + contract.required_capability;
	result += ",native_state_scan_contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		result += ",native_state_scan_blocker=" + contract.blocker;
	}
	return result;
}

static string DescribeExecutionRegionNativeOperatorContract(const ExecutionRegionNativeOperatorContract &contract,
                                                            const string &prefix) {
	if (contract.status == ExecutionRegionStateContractStatus::NONE) {
		return string();
	}
	string result = prefix + "_contract_status=" + string(ExecutionRegionStateContractStatusToString(contract.status));
	result += "," + prefix + "_required_capability=" + contract.required_capability;
	result += "," + prefix + "_contract_version=" + contract.contract_version;
	result += "," + prefix + "_blocker=" + contract.blocker;
	return result;
}

static void AppendExecutionRegionContractIR(string &result, const string &ir) {
	if (!ir.empty()) {
		result += "," + ir;
	}
}

static string DescribeExecutionRegionAggregateContract(const ExecutionRegionAggregateContract &contract,
                                                       bool include_update_contracts) {
	if (!contract.present) {
		return string();
	}
	string result = "aggregate_contract<aggregate_operator_kind=";
	result += ExecutionRegionAggregateOperatorKindToString(contract.kind);
	result += ",group_count=" + std::to_string(contract.group_count);
	result += ",group_types=" + BuildExecutionRegionLogicalTypeList(contract.group_types);
	result += ",aggregate_count=" + std::to_string(contract.aggregate_count);
	result += ",aggregate_functions=" + BuildExecutionRegionStringList(contract.aggregate_functions);
	result += ",aggregate_return_types=" + BuildExecutionRegionLogicalTypeList(contract.aggregate_return_types);
	result += ",aggregate_child_counts=" + BuildExecutionRegionIdxList(contract.aggregate_child_counts);
	result += ",aggregate_types=" + BuildExecutionRegionStringList(contract.aggregate_types);
	result += ",aggregate_filter_count=" + std::to_string(contract.aggregate_filter_count);
	result += ",aggregate_order_count=" + std::to_string(contract.aggregate_order_count);
	result += ",payload_type_count=" + std::to_string(contract.payload_type_count);
	result += ",payload_types=" + BuildExecutionRegionLogicalTypeList(contract.payload_types);
	result += ",grouping_set_count=" + std::to_string(contract.grouping_set_count);
	result += ",grouping_function_count=" + std::to_string(contract.grouping_function_count);
	result += ",radix_table_count=" + std::to_string(contract.radix_table_count);
	result += ",distinct_aggregate_count=" + std::to_string(contract.distinct_aggregate_count);
	result += ",distinct_table_count=" + std::to_string(contract.distinct_table_count);
	result += ",distinct_child_count=" + std::to_string(contract.distinct_child_count);
	result += ",distinct_count_pointer_keys=" + ExecutionRegionBool(contract.distinct_count_pointer_keys);
	result += ",input_group_type_count=" + std::to_string(contract.input_group_type_count);
	result += ",input_group_types=" + BuildExecutionRegionLogicalTypeList(contract.input_group_types);
	result += ",non_distinct_filter_count=" + std::to_string(contract.non_distinct_filter_count);
	result += ",distinct_filter_count=" + std::to_string(contract.distinct_filter_count);
	result += ",perfect_required_bits_count=" + std::to_string(contract.perfect_required_bits_count);
	result += ",perfect_required_bits_total=" + std::to_string(contract.perfect_required_bits_total);
	result += ",perfect_required_bits=" + BuildExecutionRegionIdxList(contract.perfect_required_bits);
	result += ",perfect_group_minima_count=" + std::to_string(contract.perfect_group_minima_count);
	result += ",perfect_group_minima=" + BuildExecutionRegionValueList(contract.perfect_group_minima);
	result += ",grouped_state_layout_ready=" + ExecutionRegionBool(contract.grouped_state_layout_ready);
	result += ",grouped_state_offsets=" + BuildExecutionRegionIdxList(contract.grouped_state_offsets);
	result += ",grouped_state_payload_sizes=" + BuildExecutionRegionIdxList(contract.grouped_state_payload_sizes);
	result += ",hash_lookup_layout_present=" + ExecutionRegionBool(contract.hash_lookup_layout_present);
	result += ",hash_lookup_layout_blocker=" + contract.hash_lookup_layout_blocker;
	result += ",hash_lookup_layout_row_compare_blocker=" + contract.hash_lookup_layout_row_compare_blocker;
	result += ",hash_lookup_layout_backend_lowering_blocker=" + contract.hash_lookup_layout_backend_lowering_blocker;
	if (include_update_contracts) {
		AppendExecutionRegionContractIR(result, contract.hash_lookup_layout_ir);
		AppendExecutionRegionContractIR(result, contract.native_grouped_state_contract.ir);
		AppendExecutionRegionContractIR(result, contract.native_hash_lookup_contract.ir);
		AppendExecutionRegionContractIR(result, contract.native_state_update_contract.ir);
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionOrderKeyInput(const ExecutionRegionOrderKeyInput &key) {
	string result = "order_key";
	result += std::to_string(key.key_index);
	result += "<type=" + key.type.ToString();
	result += ",physical_type=" + TypeIdToString(key.physical_type);
	result += ",order_type=" + EnumUtil::ToString(key.order_type);
	result += ",null_order=" + EnumUtil::ToString(key.null_order);
	result += ",expression_ready=" + ExecutionRegionBool(key.expression_ready);
	result += ",expression_blocker=" + key.expression_blocker;
	if (key.expression_ready) {
		auto expression_ir = DescribeExecutionRegionExpressionFragmentIR(key.expression);
		if (!expression_ir.empty()) {
			result += ",expression_ir=(" + expression_ir + ")";
		}
	}
	if (!key.reason.empty()) {
		result += ",reason=" + key.reason;
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionOrderKeyInputs(const vector<ExecutionRegionOrderKeyInput> &keys) {
	string result = "[";
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if (key_idx > 0) {
			result += "|";
		}
		result += keys[key_idx].ir.empty() ? DescribeExecutionRegionOrderKeyInput(keys[key_idx]) : keys[key_idx].ir;
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionOrderContract(const ExecutionRegionOrderContract &contract) {
	if (!contract.present) {
		return string();
	}
	string result = "order_contract<operator_kind=";
	result += ExecutionRegionOperatorKindToString(contract.kind);
	result += ",order_count=" + std::to_string(contract.order_count);
	result += ",payload_type_count=" + std::to_string(contract.payload_type_count);
	result += ",payload_types=" + BuildExecutionRegionLogicalTypeList(contract.payload_types);
	result += ",projection_count=" + std::to_string(contract.projection_count);
	result += ",projection_ids=" + BuildExecutionRegionIdxList(contract.projection_ids);
	result += ",has_limit=" + ExecutionRegionBool(contract.has_limit);
	result += ",limit=" + std::to_string(contract.limit);
	result += ",offset=" + std::to_string(contract.offset);
	result += ",dynamic_filter=" + ExecutionRegionBool(contract.dynamic_filter);
	result += ",is_index_sort=" + ExecutionRegionBool(contract.is_index_sort);
	result += ",all_order_keys_ready=" + ExecutionRegionBool(contract.all_order_keys_ready);
	result += ",order_key_blocker=" + contract.order_key_blocker;
	result += ",order_keys=" + DescribeExecutionRegionOrderKeyInputs(contract.order_keys);
	result += ">";
	return result;
}

static string DescribeExecutionRegionPrimitiveUpdateKind(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::NONE:
		return "none";
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		return "sum_int64";
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return "sum_hugeint";
	case AggregatePrimitiveUpdateKind::SUM_DOUBLE:
		return "sum_double";
	case AggregatePrimitiveUpdateKind::COUNT:
		return "count";
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
		return "count_star";
	default:
		return "unknown";
	}
}

static string DescribeExecutionRegionAggregateInput(const ExecutionRegionAggregateInput &aggregate) {
	string result = "aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "<function=" + aggregate.function_name;
	result += ",payload_index=" + std::to_string(aggregate.payload_index);
	result += ",child_count=" + std::to_string(aggregate.child_count);
	result += ",child_indices=" + BuildExecutionRegionIdxList(aggregate.child_indices);
	result += ",child_types=" + BuildExecutionRegionLogicalTypeList(aggregate.child_types);
	result += ",payload_expressions_ready=" + ExecutionRegionBool(aggregate.payload_expressions_ready);
	if (aggregate.payload_expressions_ready && !aggregate.child_expressions.empty()) {
		result += ",child_expression_ir=[";
		for (idx_t child_idx = 0; child_idx < aggregate.child_expressions.size(); child_idx++) {
			if (child_idx > 0) {
				result += "|";
			}
			result += DescribeExecutionRegionExpressionFragmentIR(aggregate.child_expressions[child_idx]);
		}
		result += "]";
	}
	if (!aggregate.payload_expression_blocker.empty()) {
		result += ",payload_expression_blocker=" + aggregate.payload_expression_blocker;
	}
	result += ",return_type=" + aggregate.return_type.ToString();
	result += ",distinct=" + ExecutionRegionBool(aggregate.distinct);
	result += ",filter=" + ExecutionRegionBool(aggregate.has_filter);
	result += ",order_bys=" + ExecutionRegionBool(aggregate.has_order_bys);
	result += ",order_dependent=" + ExecutionRegionBool(aggregate.order_dependent);
	result += ",state_update=" + ExecutionRegionBool(aggregate.has_state_update);
	result += ",primitive_update_ready=" + ExecutionRegionBool(aggregate.primitive_update_ready);
	result += ",primitive_update_kind=" + DescribeExecutionRegionPrimitiveUpdateKind(aggregate.primitive_update_kind);
	result += ",primitive_update_input_type=" + TypeIdToString(aggregate.primitive_update_input_type);
	result += ",primitive_update_state_size=" + std::to_string(aggregate.primitive_update_state_size);
	result += ",primitive_update_state_value_offset=" + std::to_string(aggregate.primitive_update_state_value_offset);
	result += ",primitive_update_state_is_set_offset=" + std::to_string(aggregate.primitive_update_state_is_set_offset);
	if (!aggregate.primitive_update_blocker.empty()) {
		result += ",primitive_update_blocker=" + aggregate.primitive_update_blocker;
	}
	result += ",supported_payload_references=" + ExecutionRegionBool(aggregate.supported_payload_references);
	if (!aggregate.reason.empty()) {
		result += ",reason=" + aggregate.reason;
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionAggregateInputs(const vector<ExecutionRegionAggregateInput> &aggregates) {
	string result = "[";
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		if (aggregate_idx > 0) {
			result += "|";
		}
		result += aggregates[aggregate_idx].ir;
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionHashJoinKeyInput(const ExecutionRegionHashJoinKeyInput &key) {
	string result = "key";
	result += std::to_string(key.key_index);
	result += "<input_index=" + std::to_string(key.input_index);
	result += ",type=" + key.type.ToString();
	result += ",supported_reference=" + ExecutionRegionBool(key.supported_reference);
	if (!key.reason.empty()) {
		result += ",reason=" + key.reason;
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionHashJoinKeyInputs(const vector<ExecutionRegionHashJoinKeyInput> &keys) {
	string result = "[";
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if (key_idx > 0) {
			result += "|";
		}
		result += keys[key_idx].ir;
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionGroupInput(const ExecutionRegionGroupInput &group) {
	string result = "group";
	result += std::to_string(group.group_index);
	result += "<input_index=" + std::to_string(group.input_index);
	result += ",type=" + group.type.ToString();
	result += ",supported_reference=" + ExecutionRegionBool(group.supported_reference);
	result += ",expression_ready=" + ExecutionRegionBool(group.expression_ready);
	if (group.expression_ready) {
		auto expression_ir = DescribeExecutionRegionExpressionFragmentIR(group.expression);
		if (!expression_ir.empty()) {
			result += ",expression_ir=(" + expression_ir + ")";
		}
	}
	if (!group.expression_blocker.empty()) {
		result += ",expression_blocker=" + group.expression_blocker;
	}
	if (!group.reason.empty()) {
		result += ",reason=" + group.reason;
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionGroupInputs(const vector<ExecutionRegionGroupInput> &groups) {
	string result = "[";
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		if (group_idx > 0) {
			result += "|";
		}
		result += groups[group_idx].ir;
	}
	result += "]";
	return result;
}

static string ExecutionRegionSourceBoundaryMarker(ExecutionRegionSourceKind kind,
                                                  ExecutionRegionSourceExecutionKind execution) {
	switch (kind) {
	case ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN:
	case ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN:
	case ExecutionRegionSourceKind::GENERIC_SCAN:
		if (execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			return "DuckDB table scan source contract";
		}
		if (execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
			return "DuckDB table scan source boundary;source-contract-blocker:requires-source-contract;"
			       "source_execution=duckdb-source-boundary";
		}
		return "DuckDB table scan source boundary;source_execution=none";
	default:
		return string();
	}
}

static vector<ExecutionRegionContractField>
BuildExecutionRegionEffectiveSourceFields(const ExecutionRegionSourceInfo &source,
                                          ExecutionRegionSourceExecutionKind execution) {
	auto result = source.fields;
	auto marker = ExecutionRegionSourceBoundaryMarker(source.kind, execution);
	if (marker.empty()) {
		return result;
	}
	for (auto &field : result) {
		if (field.name == "marker") {
			field.value = std::move(marker);
			return result;
		}
	}
	ExecutionRegionContractField marker_field;
	marker_field.name = "marker";
	marker_field.value = std::move(marker);
	result.insert(result.begin(), std::move(marker_field));
	return result;
}

string DescribeExecutionRegionSourceInfo(const ExecutionRegionSourceInfo &source,
                                         ExecutionRegionSourceExecutionKind execution) {
	if (execution == ExecutionRegionSourceExecutionKind::NONE) {
		execution = source.execution;
	}
	auto source_contract = execution == source.execution ? source.source_contract
	                                                     : BuildExecutionSourceProtocolContract(source.kind, execution);
	auto fields = BuildExecutionRegionEffectiveSourceFields(source, execution);
	string result = "source<kind=" + string(ExecutionRegionSourceKindToString(source.kind));
	result += ",execution=" + string(ExecutionRegionSourceExecutionKindToString(execution));
	if (!source_contract.ir.empty()) {
		result += "," + source_contract.ir;
	}
	if (!source.native_state_scan_contract.ir.empty()) {
		result += "," + source.native_state_scan_contract.ir;
	}
	result += ",function=" + source.function_name;
	result += ",estimated_source_cardinality=" + std::to_string(source.estimated_source_cardinality);
	result += ",estimated_source_cardinality_exact=" + ExecutionRegionBool(source.estimated_source_cardinality_exact);
	result += ",fields=" + DescribeExecutionRegionContractFields(fields);
	result += ",output_columns=" + std::to_string(source.output_column_count);
	result += ",returned_columns=" + std::to_string(source.returned_column_count);
	result += ",column_ids=" + BuildExecutionRegionIdxList(source.column_ids);
	result += ",projection_ids=" + BuildExecutionRegionIdxList(source.projection_ids);
	result += ",projection_pushdown=" + ExecutionRegionBool(source.projection_pushdown);
	result += ",filter_pushdown=" + ExecutionRegionBool(source.filter_pushdown);
	result += ",filter_prune=" + ExecutionRegionBool(source.filter_prune);
	result += ",dynamic_filters=" + ExecutionRegionBool(source.dynamic_filters);
	result += ",in_out_function=" + ExecutionRegionBool(source.in_out_function);
	result += ",filter_count=" + std::to_string(source.filters.size());
	if (!source.table_scan_contract.ir.empty()) {
		result += "," + source.table_scan_contract.ir;
	}
	if (!source.hash_join_contract.ir.empty()) {
		result += "," + source.hash_join_contract.ir;
	}
	if (!source.nested_loop_join_contract.ir.empty()) {
		result += "," + source.nested_loop_join_contract.ir;
	}
	if (!source.aggregate_contract.ir.empty()) {
		result += "," + source.aggregate_contract.ir;
	}
	if (!source.order_contract.ir.empty()) {
		result += "," + source.order_contract.ir;
	}
	if (!source.aggregates.empty()) {
		result += ",aggregates=" + DescribeExecutionRegionAggregateInputs(source.aggregates);
	}
	if (!source.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeExecutionRegionHashJoinKeyInputs(source.hash_join_keys);
	}
	if (!source.groups.empty()) {
		result += ",groups=" + DescribeExecutionRegionGroupInputs(source.groups);
	}
	result += ">";
	for (auto &filter : source.filters) {
		result += ";";
		result += DescribeExecutionRegionSourceFilter(filter);
	}
	return result;
}

static string DescribeExecutionRegionOperatorInfo(const ExecutionRegionOperatorInfo &operator_info) {
	string result = "operator<kind=" + string(ExecutionRegionOperatorContractKindToString(operator_info.kind));
	result += ",fields=" + DescribeExecutionRegionContractFields(operator_info.fields);
	if (!operator_info.hash_join_contract.ir.empty()) {
		result += "," + operator_info.hash_join_contract.ir;
	}
	if (!operator_info.nested_loop_join_contract.ir.empty()) {
		result += "," + operator_info.nested_loop_join_contract.ir;
	}
	if (!operator_info.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeExecutionRegionHashJoinKeyInputs(operator_info.hash_join_keys);
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionSinkInfo(const ExecutionRegionSinkInfo &sink) {
	string result = "sink<kind=" + string(ExecutionRegionSinkKindToString(sink.kind));
	result += ",fields=" + DescribeExecutionRegionContractFields(sink.fields);
	if (!sink.native_sink_contract.ir.empty()) {
		result += "," + sink.native_sink_contract.ir;
	}
	if (!sink.hash_join_contract.ir.empty()) {
		result += "," + sink.hash_join_contract.ir;
	}
	if (!sink.nested_loop_join_contract.ir.empty()) {
		result += "," + sink.nested_loop_join_contract.ir;
	}
	if (!sink.aggregate_contract.ir.empty()) {
		result += "," + sink.aggregate_contract.ir;
	}
	if (!sink.order_contract.ir.empty()) {
		result += "," + sink.order_contract.ir;
	}
	if (!sink.aggregates.empty()) {
		result += ",aggregates=" + DescribeExecutionRegionAggregateInputs(sink.aggregates);
	}
	if (!sink.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeExecutionRegionHashJoinKeyInputs(sink.hash_join_keys);
	}
	if (!sink.groups.empty()) {
		result += ",groups=" + DescribeExecutionRegionGroupInputs(sink.groups);
	}
	result += ">";
	return result;
}

static bool ExecutionRegionShouldRenderDiagnostics(ExecutionRegionIRMode mode) {
	return mode == ExecutionRegionIRMode::TRACE;
}

static void MaterializeExecutionRegionDiagnosticFields(vector<ExecutionRegionContractField> &fields,
                                                       const string &reason) {
	if (!fields.empty() || reason.empty()) {
		return;
	}
	fields = BuildExecutionContractFields(reason);
}

static string DescribeExecutionRegionTypeList(const vector<LogicalType> &types) {
	string result = "[";
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += ExecutionRegionTypeDescriptor(types[type_idx]);
	}
	result += "]";
	return result;
}

static string DescribeExecutionRegionNodeHeader(const ExecutionRegionNode &node) {
	string result = node.label + ":" + string(ExecutionRegionNodeKindToString(node.kind)) + ":" + node.operator_name;
	result += "<outputs=" + DescribeExecutionRegionTypeList(node.output_types);
	result += ",input_format=" + string(ExecutionRegionVectorFormatKindToString(node.input_format));
	result += ",output_format=" + string(ExecutionRegionVectorFormatKindToString(node.output_format));
	result += ",vector_source=" + string(ExecutionRegionVectorSourceKindToString(node.vector_source));
	result += ",selection_source=" + string(ExecutionRegionSelectionSourceKindToString(node.selection_source));
	result += ",boundary=" + string(ExecutionRegionBoundaryKindToString(node.boundary));
	result += ",estimated_cardinality=" + std::to_string(node.estimated_cardinality);
	result += ",estimated_cardinality_exact=" + ExecutionRegionBool(node.estimated_cardinality_exact);
	if (node.compiled_contract.Present()) {
		result += ",compiled_contract=" + node.compiled_contract.ir;
	}
	result += ">";
	return result;
}

static string DescribeExecutionRegionNode(const ExecutionRegionNode &node) {
	string result = DescribeExecutionRegionNodeHeader(node);
	switch (node.kind) {
	case ExecutionRegionNodeKind::FILTER:
		result += "(";
		result += node.filter ? node.filter->ir : "blocker:" + node.blocker_reason;
		result += ")";
		break;
	case ExecutionRegionNodeKind::PROJECTION:
		result += "(";
		for (idx_t expr_idx = 0; expr_idx < node.projections.size(); expr_idx++) {
			if (expr_idx > 0) {
				result += ",";
			}
			result += node.projections[expr_idx]->ir;
		}
		if (node.projections.empty() && !node.blocker_reason.empty()) {
			result += "blocker:" + node.blocker_reason;
		}
		result += ")";
		break;
	default:
		if (node.sink) {
			result += "(" + node.sink->ir;
			if (!node.blocker_reason.empty()) {
				result += ";blocker:" + node.blocker_reason;
			}
			result += ")";
			break;
		}
		if (node.source) {
			result += "(" + node.source->ir;
			if (!node.blocker_reason.empty()) {
				result += ";blocker:" + node.blocker_reason;
			}
			result += ")";
			break;
		}
		if (node.operator_info) {
			result += "(" + node.operator_info->ir;
			if (!node.blocker_reason.empty()) {
				result += ";blocker:" + node.blocker_reason;
			}
			result += ")";
			break;
		}
		if (!node.blocker_reason.empty()) {
			result += "(" + node.blocker_reason + ")";
		}
		break;
	}
	return result;
}

static string DescribeExecutionRegionCandidateTraits(const ExecutionRegionCandidateTraits &traits) {
	string result = "traits<source=" + ExecutionRegionBool(traits.HasSource());
	result += ",sink=" + ExecutionRegionBool(traits.HasSink());
	result += ",source_kind=" + string(ExecutionRegionSourceKindToString(traits.source_kind));
	result += ",source_execution=" + string(ExecutionRegionSourceExecutionKindToString(traits.source_execution));
	result += ",sink_kind=" + string(ExecutionRegionSinkKindToString(traits.sink_kind));
	result += ",source_filters=" + std::to_string(traits.source_filter_count);
	result += ",source_filter_expressions=" + std::to_string(traits.source_filter_expression_count);
	result += ",source_conjunction_filters=" + std::to_string(traits.source_conjunction_filter_count);
	result += ",filters=" + std::to_string(traits.filter_count);
	result += ",projections=" + std::to_string(traits.projection_count);
	result += ",operators=" + std::to_string(traits.operator_count);
	result += ",hash_join_operators=" + std::to_string(traits.hash_join_operator_count);
	result += ",aggregates=" + std::to_string(traits.aggregate_count);
	result += ",arithmetic_projections=" + std::to_string(traits.arithmetic_projection_count);
	result += ",high_cost_projections=" + std::to_string(traits.high_cost_projection_count);
	result += ",reference_projections=" + std::to_string(traits.reference_projection_count);
	result += ",reference_varchar_projections=" + std::to_string(traits.reference_varchar_projection_count);
	result += ",predicate_expressions=" + std::to_string(traits.predicate_expression_count);
	result += ",control_expressions=" + std::to_string(traits.control_expression_count);
	result += ",expression_cost=" + std::to_string(traits.expression_cost);
	result += ">";
	return result;
}

static string DescribeExecutionRegionStagePlan(const ExecutionRegionStagePlan &plan) {
	string result = "duckdb.operator-stage-region<shape=" + plan.shape;
	result += ",stages=[";
	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto &stage = plan.stages[stage_idx];
		if (stage_idx > 0) {
			result += "|";
		}
		result += ExecutionRegionStageKindToString(stage.kind);
		result += ":";
		result += ExecutionRegionStageExecutionKindToString(stage.execution);
		result += ":";
		result += ExecutionRegionOwnershipKindToString(stage.ownership);
		result += ":operation=";
		result += ExecutionCompiledContractKindToString(stage.operation);
		result += ":drain=";
		result += ExecutionCompiledDrainKindToString(stage.drain);
		result += ":executable_work=";
		result += stage.executable_work ? "true" : "false";
		result += "#node";
		result += std::to_string(stage.node_index);
		if (stage.operator_index != DConstants::INVALID_INDEX) {
			result += "#op";
			result += std::to_string(stage.operator_index);
		}
		if (stage.filter_index != DConstants::INVALID_INDEX) {
			result += "#filter";
			result += std::to_string(stage.filter_index);
		}
		if (!stage.operator_name.empty()) {
			result += "(";
			result += stage.operator_name;
			result += ")";
		}
		if (!stage.required_capability.empty()) {
			result += "[capability=";
			result += stage.required_capability;
			result += "]";
		}
		if (!stage.reason.empty()) {
			result += "{";
			result += stage.reason;
			result += "}";
		}
	}
	result += "]>";
	return result;
}

static string DescribeExecutionRegionCandidate(const ExecutionRegionCandidate &candidate) {
	string result = "candidate" + std::to_string(candidate.candidate_id);
	result += "<first_node=" + std::to_string(candidate.first_node);
	result += ",node_count=" + std::to_string(candidate.node_count);
	result += ",start_operator_index=" + std::to_string(candidate.start_operator_index);
	result += ",end_operator_index=" + std::to_string(candidate.end_operator_index);
	result += ",estimated_cardinality=" + std::to_string(candidate.estimated_cardinality);
	if (candidate.source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		result += ",source_execution=";
		result += ExecutionRegionSourceExecutionKindToString(candidate.source_execution);
	}
	if (candidate.uses_scan_filters) {
		result += ",uses_scan_filters=true";
	}
	result += ",inputs=" + DescribeExecutionRegionTypeList(candidate.input_types);
	result += ",outputs=" + DescribeExecutionRegionTypeList(candidate.output_types);
	result += ",shape=" + candidate.shape;
	if (!candidate.stage_plan.ir.empty()) {
		result += ",";
		result += candidate.stage_plan.ir;
	}
	if (candidate.context_has_missing_operator_contract) {
		result += ",context_has_missing_operator_contract=true";
	}
	result += ",pipeline_shape=" + candidate.pipeline_shape;
	result += "," + candidate.traits.ir;
	result += "," + candidate.contract.ir;
	result += ">";
	return result;
}

string DescribeExecutionRegionCandidateSpan(idx_t first_node, idx_t node_count, idx_t start_operator_index,
                                            idx_t end_operator_index,
                                            ExecutionRegionSourceExecutionKind source_execution) {
	string result = "span[first_node=" + std::to_string(first_node);
	result += ",node_count=" + std::to_string(node_count);
	result +=
	    ",operator_range=[" + std::to_string(start_operator_index) + "," + std::to_string(end_operator_index) + ")";
	if (source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		result += ",source_execution=";
		result += ExecutionRegionSourceExecutionKindToString(source_execution);
	}
	result += "]";
	return result;
}

void FinalizeExecutionRegionSourceInfo(ExecutionRegionSourceInfo &source, ExecutionRegionIRMode mode) {
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return;
	}
	MaterializeExecutionRegionDiagnosticFields(source.fields, source.reason);
	for (auto &aggregate : source.aggregates) {
		aggregate.ir = DescribeExecutionRegionAggregateInput(aggregate);
	}
	for (auto &key : source.hash_join_keys) {
		key.ir = DescribeExecutionRegionHashJoinKeyInput(key);
	}
	for (auto &condition : source.nested_loop_join_contract.conditions) {
		condition.ir = DescribeExecutionRegionNestedLoopJoinCondition(condition);
	}
	for (auto &group : source.groups) {
		group.ir = DescribeExecutionRegionGroupInput(group);
	}
	source.table_scan_contract.ir = DescribeExecutionRegionTableScanContract(source.table_scan_contract);
	source.hash_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.hash_join_contract.native_probe_contract, "native_hash_join_probe");
	source.hash_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.hash_join_contract.native_build_contract, "native_hash_join_build");
	source.hash_join_contract.ir = DescribeExecutionRegionHashJoinContract(source.hash_join_contract);
	source.nested_loop_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.nested_loop_join_contract.native_probe_contract, "native_nested_loop_join_probe");
	source.nested_loop_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.nested_loop_join_contract.native_build_contract, "native_nested_loop_join_build");
	source.nested_loop_join_contract.ir =
	    DescribeExecutionRegionNestedLoopJoinContract(source.nested_loop_join_contract);
	source.aggregate_contract.native_grouped_state_contract.ir =
	    DescribeExecutionRegionNativeGroupedStateContract(source.aggregate_contract.native_grouped_state_contract);
	source.aggregate_contract.native_hash_lookup_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.aggregate_contract.native_hash_lookup_contract, "native_hash_aggregate_lookup");
	source.aggregate_contract.native_state_update_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    source.aggregate_contract.native_state_update_contract, "native_aggregate_state_update");
	source.aggregate_contract.ir = DescribeExecutionRegionAggregateContract(source.aggregate_contract, false);
	for (auto &order_key : source.order_contract.order_keys) {
		order_key.ir = DescribeExecutionRegionOrderKeyInput(order_key);
	}
	source.order_contract.ir = DescribeExecutionRegionOrderContract(source.order_contract);
	source.source_contract.ir = DescribeExecutionSourceProtocolContract(source.source_contract);
	source.native_state_scan_contract.ir =
	    DescribeExecutionRegionNativeStateScanContract(source.native_state_scan_contract);
	source.ir = DescribeExecutionRegionSourceInfo(source);
}

void FinalizeExecutionRegionOperatorInfo(ExecutionRegionOperatorInfo &operator_info, ExecutionRegionIRMode mode) {
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return;
	}
	MaterializeExecutionRegionDiagnosticFields(operator_info.fields, operator_info.reason);
	for (auto &key : operator_info.hash_join_keys) {
		key.ir = DescribeExecutionRegionHashJoinKeyInput(key);
	}
	for (auto &condition : operator_info.nested_loop_join_contract.conditions) {
		condition.ir = DescribeExecutionRegionNestedLoopJoinCondition(condition);
	}
	operator_info.hash_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    operator_info.hash_join_contract.native_probe_contract, "native_hash_join_probe");
	operator_info.hash_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    operator_info.hash_join_contract.native_build_contract, "native_hash_join_build");
	operator_info.hash_join_contract.ir = DescribeExecutionRegionHashJoinContract(operator_info.hash_join_contract);
	operator_info.nested_loop_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    operator_info.nested_loop_join_contract.native_probe_contract, "native_nested_loop_join_probe");
	operator_info.nested_loop_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    operator_info.nested_loop_join_contract.native_build_contract, "native_nested_loop_join_build");
	operator_info.nested_loop_join_contract.ir =
	    DescribeExecutionRegionNestedLoopJoinContract(operator_info.nested_loop_join_contract);
	operator_info.ir = DescribeExecutionRegionOperatorInfo(operator_info);
}

void FinalizeExecutionRegionSinkInfo(ExecutionRegionSinkInfo &sink, ExecutionRegionIRMode mode) {
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return;
	}
	MaterializeExecutionRegionDiagnosticFields(sink.fields, sink.reason);
	for (auto &aggregate : sink.aggregates) {
		aggregate.ir = DescribeExecutionRegionAggregateInput(aggregate);
	}
	for (auto &key : sink.hash_join_keys) {
		key.ir = DescribeExecutionRegionHashJoinKeyInput(key);
	}
	for (auto &condition : sink.nested_loop_join_contract.conditions) {
		condition.ir = DescribeExecutionRegionNestedLoopJoinCondition(condition);
	}
	for (auto &group : sink.groups) {
		group.ir = DescribeExecutionRegionGroupInput(group);
	}
	sink.native_sink_contract.ir = DescribeExecutionRegionNativeOperatorContract(sink.native_sink_contract, "sink");
	sink.hash_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.hash_join_contract.native_probe_contract, "native_hash_join_probe");
	sink.hash_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.hash_join_contract.native_build_contract, "native_hash_join_build");
	sink.hash_join_contract.ir = DescribeExecutionRegionHashJoinContract(sink.hash_join_contract);
	sink.nested_loop_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.nested_loop_join_contract.native_probe_contract, "native_nested_loop_join_probe");
	sink.nested_loop_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.nested_loop_join_contract.native_build_contract, "native_nested_loop_join_build");
	sink.nested_loop_join_contract.ir = DescribeExecutionRegionNestedLoopJoinContract(sink.nested_loop_join_contract);
	sink.aggregate_contract.native_grouped_state_contract.ir =
	    DescribeExecutionRegionNativeGroupedStateContract(sink.aggregate_contract.native_grouped_state_contract);
	sink.aggregate_contract.native_hash_lookup_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.aggregate_contract.native_hash_lookup_contract, "native_hash_aggregate_lookup");
	sink.aggregate_contract.native_state_update_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.aggregate_contract.native_state_update_contract, "native_aggregate_state_update");
	sink.aggregate_contract.ir = DescribeExecutionRegionAggregateContract(sink.aggregate_contract, true);
	for (auto &order_key : sink.order_contract.order_keys) {
		order_key.ir = DescribeExecutionRegionOrderKeyInput(order_key);
	}
	sink.order_contract.ir = DescribeExecutionRegionOrderContract(sink.order_contract);
	sink.ir = DescribeExecutionRegionSinkInfo(sink);
}

void FinalizeExecutionRegionCandidateTraits(ExecutionRegionCandidateTraits &traits, ExecutionRegionIRMode mode) {
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		traits.ir = DescribeExecutionRegionCandidateTraits(traits);
	}
}

void FinalizeExecutionRegionStagePlan(ExecutionRegionStagePlan &plan, ExecutionRegionIRMode mode) {
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		plan.ir = DescribeExecutionRegionStagePlan(plan);
	}
}

void FinalizeExecutionRegionCandidate(ExecutionRegionCandidate &candidate, ExecutionRegionIRMode mode) {
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		candidate.ir = DescribeExecutionRegionCandidate(candidate);
	}
}

void FinalizeExecutionRegionIR(ExecutionRegionIR &region_ir, ExecutionRegionIRMode mode) {
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return;
	}
	region_ir.ir = "duckdb.region typed-vector-ir";
	for (auto &candidate : region_ir.candidates) {
		region_ir.ir += ";";
		region_ir.ir += candidate.ir;
	}
	for (auto &blocker : region_ir.candidate_blockers) {
		region_ir.ir += ";candidate-blocker:";
		region_ir.ir += blocker;
	}
	for (auto &node : region_ir.nodes) {
		region_ir.ir += ";";
		region_ir.ir += DescribeExecutionRegionNode(node);
	}
}

} // namespace duckdb

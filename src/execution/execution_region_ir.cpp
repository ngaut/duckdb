#include "duckdb/execution/execution_region_lowering.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_contract.hpp"

#include <algorithm>

namespace duckdb {

static void AddExecutionRegionFeature(vector<string> &features, string feature) {
	if (feature.empty()) {
		return;
	}
	for (auto &entry : features) {
		if (entry == feature) {
			return;
		}
	}
	features.push_back(std::move(feature));
}

static string BuildExecutionRegionFeatureSetShape(vector<string> features) {
	std::sort(features.begin(), features.end());
	features.erase(std::unique(features.begin(), features.end()), features.end());
	return StringUtil::Join(features, "+");
}

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

static void AddExecutionRegionUniqueString(vector<string> &values, string value) {
	if (value.empty()) {
		return;
	}
	for (auto &entry : values) {
		if (entry == value) {
			return;
		}
	}
	values.push_back(std::move(value));
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
		AppendExecutionRegionContractIR(result, contract.native_distinct_state_update_contract.ir);
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

static string DescribeExecutionSourceProtocolContract(const ExecutionSourceProtocolContract &contract) {
	if (contract.status == ExecutionRegionSourceContractStatus::NONE) {
		return string();
	}
	string result = "source_contract<status=" + string(ExecutionRegionSourceContractStatusToString(contract.status));
	result += ",required_capability=" + contract.required_capability;
	result += ",contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		result += ",blocker=" + contract.blocker;
	}
	result += ">";
	return result;
}

ExecutionSourceProtocolContract BuildExecutionSourceProtocolContract(ExecutionRegionSourceKind kind,
                                                                     ExecutionRegionSourceExecutionKind execution) {
	ExecutionSourceProtocolContract result;
	if (kind == ExecutionRegionSourceKind::NONE) {
		return result;
	}
	result.status = execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
	                    ? ExecutionRegionSourceContractStatus::READY
	                    : ExecutionRegionSourceContractStatus::BLOCKED;
	result.contract_version = "v1";
	switch (kind) {
	case ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN:
		result.required_capability = "duckdb-table-scan-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "duckdb-table-scan-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN:
		result.required_capability = "table-function-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "table-function-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::GENERIC_SCAN:
		result.required_capability = "generic-scan-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "generic-scan-source-boundary";
		}
		break;
	case ExecutionRegionSourceKind::STATEFUL_OPERATOR:
		result.required_capability = "stateful-operator-source-contract";
		if (execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT) {
			result.blocker = "stateful-source-contract-boundary";
		}
		break;
	default:
		break;
	}
	result.ir = DescribeExecutionSourceProtocolContract(result);
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

static void AddExecutionRegionSourceFilters(const ExecutionSourceContract &descriptor,
                                            ExecutionRegionSourceInfo &source,
                                            ExecutionExpressionIRMode expression_mode,
                                            ExecutionExpressionAnalysisCache *expression_cache) {
	for (auto &entry : descriptor.filters) {
		ExecutionRegionSourceFilter filter;
		filter.filter_index = entry.filter_index;
		filter.scan_column_index = entry.scan_column_index;
		filter.table_column_index = entry.table_column_index;
		if (!entry.expression) {
			filter.reason = entry.reason.empty() ? "source filter has no expression descriptor" : entry.reason;
			source.filters.push_back(std::move(filter));
			continue;
		}
		filter.expression = expression_cache
		                        ? expression_cache->Copy(*entry.expression, filter.filter_index, expression_mode)
		                        : TryLowerExecutionExpression(*entry.expression, filter.filter_index, expression_mode);
		if (!filter.expression) {
			filter.reason = DescribeExecutionExpressionLoweringFailure(*entry.expression);
		}
		source.filters.push_back(std::move(filter));
	}
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

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionSourceInfo(const ExecutionSourceContract &descriptor, ExecutionExpressionIRMode expression_mode,
                               ExecutionRegionIRMode mode, ExecutionExpressionAnalysisCache *expression_cache) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = descriptor.kind;
	result->execution = descriptor.execution;
	result->function_name = descriptor.function_name;
	result->estimated_source_cardinality = descriptor.estimated_source_cardinality;
	result->output_column_count = descriptor.output_column_count;
	result->returned_column_count = descriptor.returned_column_count;
	result->column_ids = descriptor.column_ids;
	result->projection_ids = descriptor.projection_ids;
	result->projection_pushdown = descriptor.projection_pushdown;
	result->filter_pushdown = descriptor.filter_pushdown;
	result->filter_prune = descriptor.filter_prune;
	result->dynamic_filters = descriptor.dynamic_filters;
	result->in_out_function = descriptor.in_out_function;
	result->table_scan_contract = descriptor.table_scan_contract;
	result->hash_join_contract = descriptor.hash_join_contract;
	result->nested_loop_join_contract = descriptor.nested_loop_join_contract;
	result->aggregate_contract = descriptor.aggregate_contract;
	result->order_contract = descriptor.order_contract;
	result->aggregates = descriptor.aggregates;
	result->hash_join_keys = descriptor.hash_join_keys;
	result->groups = descriptor.groups;
	result->source_contract = descriptor.source_contract;
	result->native_state_scan_contract = descriptor.native_state_scan_contract;
	result->reason = descriptor.reason;
	AddExecutionRegionSourceFilters(descriptor, *result, expression_mode, expression_cache);
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return result;
	}
	result->fields = descriptor.fields;
	MaterializeExecutionRegionDiagnosticFields(result->fields, result->reason);
	for (auto &aggregate : result->aggregates) {
		aggregate.ir = DescribeExecutionRegionAggregateInput(aggregate);
	}
	for (auto &key : result->hash_join_keys) {
		key.ir = DescribeExecutionRegionHashJoinKeyInput(key);
	}
	for (auto &condition : result->nested_loop_join_contract.conditions) {
		condition.ir = DescribeExecutionRegionNestedLoopJoinCondition(condition);
	}
	for (auto &group : result->groups) {
		group.ir = DescribeExecutionRegionGroupInput(group);
	}
	result->table_scan_contract.ir = DescribeExecutionRegionTableScanContract(result->table_scan_contract);
	result->hash_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->hash_join_contract.native_probe_contract, "native_hash_join_probe");
	result->hash_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->hash_join_contract.native_build_contract, "native_hash_join_build");
	result->hash_join_contract.ir = DescribeExecutionRegionHashJoinContract(result->hash_join_contract);
	result->nested_loop_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->nested_loop_join_contract.native_probe_contract, "native_nested_loop_join_probe");
	result->nested_loop_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->nested_loop_join_contract.native_build_contract, "native_nested_loop_join_build");
	result->nested_loop_join_contract.ir =
	    DescribeExecutionRegionNestedLoopJoinContract(result->nested_loop_join_contract);
	result->aggregate_contract.native_grouped_state_contract.ir =
	    DescribeExecutionRegionNativeGroupedStateContract(result->aggregate_contract.native_grouped_state_contract);
	result->aggregate_contract.native_hash_lookup_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->aggregate_contract.native_hash_lookup_contract, "native_hash_aggregate_lookup");
	result->aggregate_contract.native_state_update_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->aggregate_contract.native_state_update_contract, "native_aggregate_state_update");
	result->aggregate_contract.native_distinct_state_update_contract.ir =
	    DescribeExecutionRegionNativeOperatorContract(result->aggregate_contract.native_distinct_state_update_contract,
	                                                  "native_hash_aggregate_distinct_state_update");
	result->aggregate_contract.ir = DescribeExecutionRegionAggregateContract(result->aggregate_contract, false);
	for (auto &order_key : result->order_contract.order_keys) {
		order_key.ir = DescribeExecutionRegionOrderKeyInput(order_key);
	}
	result->order_contract.ir = DescribeExecutionRegionOrderContract(result->order_contract);
	result->source_contract.ir = DescribeExecutionSourceProtocolContract(result->source_contract);
	result->native_state_scan_contract.ir =
	    DescribeExecutionRegionNativeStateScanContract(result->native_state_scan_contract);
	result->ir = DescribeExecutionRegionSourceInfo(*result);
	return result;
}

static unique_ptr<ExecutionRegionOperatorInfo>
BuildExecutionRegionOperatorInfo(const ExecutionRegionOperatorInfo &descriptor, ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionOperatorInfo>(descriptor);
	if (!ExecutionRegionShouldRenderDiagnostics(mode)) {
		return result;
	}
	MaterializeExecutionRegionDiagnosticFields(result->fields, result->reason);
	for (auto &key : result->hash_join_keys) {
		key.ir = DescribeExecutionRegionHashJoinKeyInput(key);
	}
	for (auto &condition : result->nested_loop_join_contract.conditions) {
		condition.ir = DescribeExecutionRegionNestedLoopJoinCondition(condition);
	}
	result->hash_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->hash_join_contract.native_probe_contract, "native_hash_join_probe");
	result->hash_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->hash_join_contract.native_build_contract, "native_hash_join_build");
	result->hash_join_contract.ir = DescribeExecutionRegionHashJoinContract(result->hash_join_contract);
	result->nested_loop_join_contract.native_probe_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->nested_loop_join_contract.native_probe_contract, "native_nested_loop_join_probe");
	result->nested_loop_join_contract.native_build_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    result->nested_loop_join_contract.native_build_contract, "native_nested_loop_join_build");
	result->nested_loop_join_contract.ir =
	    DescribeExecutionRegionNestedLoopJoinContract(result->nested_loop_join_contract);
	result->ir = DescribeExecutionRegionOperatorInfo(*result);
	return result;
}

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionGenericScanSourceInfo(const ExecutionRegionOperatorEntry &entry, const string &reason,
                                          ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = ExecutionRegionSourceKind::GENERIC_SCAN;
	result->execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result->function_name = StringUtil::Lower(entry.operator_name);
	result->output_column_count = entry.output_types.size();
	result->returned_column_count = entry.output_types.size();
	result->source_contract.status = ExecutionRegionSourceContractStatus::BLOCKED;
	result->source_contract.required_capability = "generic-scan-source-contract";
	result->source_contract.contract_version = "v1";
	result->source_contract.blocker = "generic-scan-source-boundary";
	result->reason = reason;
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		MaterializeExecutionRegionDiagnosticFields(result->fields, result->reason);
		result->source_contract.ir = DescribeExecutionSourceProtocolContract(result->source_contract);
		result->ir = DescribeExecutionRegionSourceInfo(*result);
	}
	return result;
}

static unique_ptr<ExecutionRegionSourceInfo>
BuildExecutionRegionStatefulSourceInfo(const ExecutionRegionOperatorEntry &entry, const string &reason,
                                       ExecutionRegionIRMode mode) {
	auto result = make_uniq<ExecutionRegionSourceInfo>();
	result->kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	result->execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result->function_name = StringUtil::Lower(entry.operator_name);
	result->output_column_count = entry.output_types.size();
	result->returned_column_count = entry.output_types.size();
	result->source_contract.status = ExecutionRegionSourceContractStatus::BLOCKED;
	result->source_contract.required_capability = "stateful-operator-source-contract";
	result->source_contract.contract_version = "v1";
	result->source_contract.blocker = "stateful-source-contract-boundary";
	result->reason = reason;
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		MaterializeExecutionRegionDiagnosticFields(result->fields, result->reason);
		result->source_contract.ir = DescribeExecutionSourceProtocolContract(result->source_contract);
		result->ir = DescribeExecutionRegionSourceInfo(*result);
	}
	return result;
}

static string BuildExecutionRegionGenericSinkContractReason(const ExecutionRegionOperatorEntry &entry) {
	if (entry.IsSortSink()) {
		return "DuckDB ordered sink contract missing;operator=" + entry.operator_name;
	}
	if (entry.IsMaterializationSink()) {
		return "DuckDB materialization sink contract missing;operator=" + entry.operator_name;
	}
	return "DuckDB sink operator missing;operator=" + entry.operator_name;
}

static void FinalizeExecutionRegionSinkInfo(ExecutionRegionSinkInfo &sink, ExecutionRegionIRMode mode) {
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
	sink.aggregate_contract.native_distinct_state_update_contract.ir = DescribeExecutionRegionNativeOperatorContract(
	    sink.aggregate_contract.native_distinct_state_update_contract, "native_hash_aggregate_distinct_state_update");
	sink.aggregate_contract.ir = DescribeExecutionRegionAggregateContract(sink.aggregate_contract, true);
	for (auto &order_key : sink.order_contract.order_keys) {
		order_key.ir = DescribeExecutionRegionOrderKeyInput(order_key);
	}
	sink.order_contract.ir = DescribeExecutionRegionOrderContract(sink.order_contract);
	sink.ir = DescribeExecutionRegionSinkInfo(sink);
}

static unique_ptr<ExecutionRegionSinkInfo> BuildExecutionRegionSinkInfo(const ExecutionRegionOperatorEntry &entry,
                                                                        const ExecutionRegionSinkInfo &sink_payload,
                                                                        bool has_sink_contract,
                                                                        ExecutionRegionIRMode mode) {
	auto result =
	    has_sink_contract ? make_uniq<ExecutionRegionSinkInfo>(sink_payload) : make_uniq<ExecutionRegionSinkInfo>();
	if (!has_sink_contract) {
		result->reason = BuildExecutionRegionGenericSinkContractReason(entry);
	}
	FinalizeExecutionRegionSinkInfo(*result, mode);
	return result;
}

static string BuildExecutionRegionSourceBoundaryReason(const ExecutionRegionOperatorEntry &entry) {
	if (!entry.source_boundary_reason.empty()) {
		return entry.source_boundary_reason;
	}
	if (entry.IsScanSource()) {
		return "DuckDB scan source boundary;operator=" + entry.operator_name;
	}
	return "DuckDB stateful source boundary;operator=" + entry.operator_name;
}

static string BuildExecutionRegionSinkBoundaryReason(const ExecutionRegionOperatorEntry &entry) {
	if (entry.HasSinkContract() && !entry.sink_payload.reason.empty()) {
		return entry.sink_payload.reason;
	}
	return BuildExecutionRegionGenericSinkContractReason(entry);
}

static ExecutionRegionNode BuildExecutionRegionBoundaryNode(string label, const ExecutionRegionOperatorEntry &entry,
                                                            ExecutionRegionNodeKind kind, string blocker_reason) {
	ExecutionRegionNode node;
	node.label = std::move(label);
	node.operator_name = entry.operator_name;
	node.operator_kind = entry.operator_kind;
	node.operator_index = entry.operator_index;
	node.kind = kind;
	node.output_types = entry.output_types;
	node.estimated_cardinality = entry.estimated_cardinality;
	node.input_format = ExecutionRegionVectorFormatKind::BOUNDARY;
	node.output_format = ExecutionRegionVectorFormatKind::BOUNDARY;
	node.vector_source = ExecutionRegionVectorSourceKind::BOUNDARY;
	node.selection_source = ExecutionRegionSelectionSourceKind::BOUNDARY;
	switch (kind) {
	case ExecutionRegionNodeKind::SOURCE:
		node.boundary =
		    entry.IsScanSource() ? ExecutionRegionBoundaryKind::SCAN : ExecutionRegionBoundaryKind::OPERATOR_MISSING;
		break;
	case ExecutionRegionNodeKind::SINK:
		node.boundary = ExecutionRegionBoundaryKind::SINK;
		break;
	default:
		node.boundary = ExecutionRegionBoundaryKind::OPERATOR_MISSING;
		break;
	}
	node.blocker_reason = std::move(blocker_reason);
	return node;
}

struct ExecutionRegionDataflowState {
	ExecutionRegionVectorSourceKind vector_source = ExecutionRegionVectorSourceKind::REGION_INPUT;
	ExecutionRegionSelectionSourceKind selection_source = ExecutionRegionSelectionSourceKind::INPUT_SELECTION;
};

static void SetExecutionRegionInputDataflow(ExecutionRegionNode &node, const ExecutionRegionDataflowState &state) {
	node.vector_source = state.vector_source;
	node.selection_source = state.selection_source;
}

static void SetExecutionRegionBoundaryDataflow(ExecutionRegionDataflowState &state) {
	state.vector_source = ExecutionRegionVectorSourceKind::BOUNDARY;
	state.selection_source = ExecutionRegionSelectionSourceKind::BOUNDARY;
}

static ExecutionRegionNode BuildExecutionRegionOperatorNode(string label, const ExecutionRegionOperatorEntry &entry,
                                                            ExecutionRegionDataflowState &state,
                                                            ExecutionExpressionIRMode expression_mode,
                                                            ExecutionRegionIRMode mode,
                                                            ExecutionExpressionAnalysisCache *expression_cache) {
	if (entry.IsFilter()) {
		if (!entry.filter_expression) {
			throw InternalException("Execution region filter descriptor missing expression");
		}
		auto &filter_expression = *entry.filter_expression;
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.kind = ExecutionRegionNodeKind::FILTER;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = ExecutionRegionVectorFormatKind::SELECTION_VECTOR;
		SetExecutionRegionInputDataflow(node, state);
		node.filter = expression_cache ? expression_cache->Copy(filter_expression, 0, expression_mode)
		                               : TryLowerExecutionExpression(filter_expression, 0, expression_mode);
		if (!node.filter) {
			node.boundary = ExecutionRegionBoundaryKind::EXPRESSION_MISSING;
			node.blocker_reason = "core filter expression lowering unsupported;" +
			                      DescribeExecutionExpressionLoweringFailure(filter_expression);
			SetExecutionRegionBoundaryDataflow(state);
			return node;
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::FILTER_SELECTION;
		return node;
	}
	if (entry.IsProjection()) {
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.kind = ExecutionRegionNodeKind::PROJECTION;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = ExecutionRegionVectorFormatKind::FLAT_VECTOR;
		SetExecutionRegionInputDataflow(node, state);
		for (idx_t expr_idx = 0; expr_idx < entry.projection_expressions.size(); expr_idx++) {
			auto &projection_expression = *entry.projection_expressions[expr_idx];
			auto fragment = expression_cache
			                    ? expression_cache->Copy(projection_expression, expr_idx, expression_mode)
			                    : TryLowerExecutionExpression(projection_expression, expr_idx, expression_mode);
			if (!fragment) {
				node.projections.clear();
				node.boundary = ExecutionRegionBoundaryKind::EXPRESSION_MISSING;
				node.blocker_reason =
				    "core projection expression lowering unsupported;expression_index=" + std::to_string(expr_idx) +
				    ";" + DescribeExecutionExpressionLoweringFailure(projection_expression);
				SetExecutionRegionBoundaryDataflow(state);
				return node;
			}
			node.projections.push_back(std::move(fragment));
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::NONE;
		return node;
	}
	if (entry.HasOperatorContract()) {
		ExecutionRegionNode node;
		node.label = std::move(label);
		node.operator_name = entry.operator_name;
		node.operator_kind = entry.operator_kind;
		node.operator_index = entry.operator_index;
		node.compiled_contract = entry.operator_contract;
		node.kind = ExecutionRegionNodeKind::OPERATOR;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = ExecutionRegionVectorFormatKind::DATA_CHUNK;
		node.output_format = ExecutionRegionVectorFormatKind::DATA_CHUNK;
		SetExecutionRegionInputDataflow(node, state);
		node.operator_info = BuildExecutionRegionOperatorInfo(entry.operator_payload, mode);
		if (entry.HasNativeOperator()) {
			node.boundary = ExecutionRegionBoundaryKind::OPERATOR_NATIVE;
		} else {
			node.boundary = ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY;
		}
		state.vector_source = ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = ExecutionRegionSelectionSourceKind::NONE;
		return node;
	}
	auto node = BuildExecutionRegionBoundaryNode(std::move(label), entry, ExecutionRegionNodeKind::OPERATOR,
	                                             "DuckDB physical operator outside generated execution region");
	SetExecutionRegionBoundaryDataflow(state);
	return node;
}

static string ExecutionRegionOperatorKindSignatureSegment(ExecutionRegionOperatorKind kind) {
	if (kind == ExecutionRegionOperatorKind::GENERIC) {
		return "generic";
	}
	return ExecutionRegionOperatorKindToString(kind);
}

static string DescribeExecutionSourceProtocolContractShape(ExecutionRegionSourceKind kind,
                                                           ExecutionRegionSourceExecutionKind execution,
                                                           idx_t filter_count, idx_t projected_column_count,
                                                           idx_t returned_column_count, bool dynamic_filters,
                                                           bool in_out_function) {
	string result = "source(kind=";
	result += ExecutionRegionSourceKindToString(kind);
	result += ",execution=";
	result += ExecutionRegionSourceExecutionKindToString(execution);
	result += ",filters=" + std::to_string(filter_count);
	result += ",projected=" + std::to_string(projected_column_count);
	result += ",returned=" + std::to_string(returned_column_count);
	result += ",dynamic=" + ExecutionRegionBool(dynamic_filters);
	result += ",inout=" + ExecutionRegionBool(in_out_function);
	result += ")";
	return result;
}

static string DescribeExecutionRegionHashJoinContractShape(const ExecutionRegionHashJoinContract &contract) {
	if (!contract.present) {
		return "hash_join(absent)";
	}
	string result = "hash_join(join=";
	result += ExecutionRegionJoinTypeToString(contract.join_type);
	result += ",conditions=" + std::to_string(contract.condition_count);
	result += ",equality=" + std::to_string(contract.equality_condition_count);
	result += ",payload=" + std::to_string(contract.payload_column_count);
	result += ",lhs=" + std::to_string(contract.lhs_output_column_count);
	result += ",rhs=" + std::to_string(contract.rhs_output_column_count);
	result += ",probe=" + ExecutionHashJoinProbeOutputModeToString(contract.native_probe_output_mode);
	result += ",build_filter=" + ExecutionRegionBool(contract.build_side_has_filter);
	result += ",filter_conditions=" + std::to_string(contract.filter_pushdown_condition_count);
	result += ",delim=" + std::to_string(contract.delim_type_count);
	result += ",residual=" + ExecutionRegionBool(contract.residual_predicate || contract.residual_info);
	result += ",residual_ready=" + ExecutionRegionBool(contract.residual_expression_ready);
	result += ")";
	return result;
}

static string
DescribeExecutionRegionNestedLoopJoinContractShape(const ExecutionRegionNestedLoopJoinContract &contract) {
	if (!contract.present) {
		return "nested_loop_join(absent)";
	}
	string result = "nested_loop_join(join=";
	result += ExecutionRegionJoinTypeToString(contract.join_type);
	result += ",conditions=" + std::to_string(contract.condition_count);
	result += ",comparisons=" + std::to_string(contract.comparison_condition_count);
	result += ",simple=" + ExecutionRegionBool(contract.simple_join);
	result += ",complex=" + ExecutionRegionBool(contract.complex_join);
	result += ",source=" + ExecutionRegionBool(contract.source_produces_rows);
	result += ",conditions_ready=" + ExecutionRegionBool(contract.conditions_ready);
	result += ")";
	return result;
}

static string DescribeExecutionRegionAggregateContractShape(const ExecutionRegionAggregateContract &contract) {
	if (!contract.present) {
		return "aggregate(absent)";
	}
	string result = "aggregate(kind=";
	result += ExecutionRegionAggregateOperatorKindToString(contract.kind);
	result += ",groups=" + std::to_string(contract.group_count);
	result += ",input_groups=" + std::to_string(contract.input_group_type_count);
	result += ",aggregates=" + std::to_string(contract.aggregate_count);
	result += ",functions=" + BuildExecutionRegionStringList(contract.aggregate_functions);
	result += ",payloads=" + std::to_string(contract.payload_type_count);
	result += ",distinct=" + std::to_string(contract.distinct_aggregate_count);
	result += ",filters=" + std::to_string(contract.aggregate_filter_count);
	result += ",orders=" + std::to_string(contract.aggregate_order_count);
	result += ",state_layout=" + ExecutionRegionBool(contract.grouped_state_layout_ready);
	result += ",hash_lookup_layout=" + ExecutionRegionBool(contract.hash_lookup_layout_present);
	result += ")";
	return result;
}

static string DescribeExecutionRegionOrderContractShape(const ExecutionRegionOrderContract &contract) {
	if (!contract.present) {
		return "order(absent)";
	}
	string result = "order(kind=";
	result += ExecutionRegionOperatorKindToString(contract.kind);
	result += ",keys=" + std::to_string(contract.order_count);
	result += ",payloads=" + std::to_string(contract.payload_type_count);
	result += ",projections=" + std::to_string(contract.projection_count);
	result += ",limit=" + ExecutionRegionBool(contract.has_limit);
	if (contract.has_limit) {
		result += ":" + std::to_string(contract.limit);
		result += ":offset=" + std::to_string(contract.offset);
	}
	result += ",dynamic_filter=" + ExecutionRegionBool(contract.dynamic_filter);
	result += ",index_sort=" + ExecutionRegionBool(contract.is_index_sort);
	result += ",keys_ready=" + ExecutionRegionBool(contract.all_order_keys_ready);
	result += ")";
	return result;
}

static void AppendExecutionRegionContractShapeEntry(string &result, const string &entry) {
	if (entry.empty()) {
		return;
	}
	if (!result.empty()) {
		result += ";";
	}
	result += entry;
}

static void AppendExecutionRegionContractShapePart(vector<ExecutionRegionContractShapePart> &parts,
                                                   ExecutionRegionNodeKind node_kind, string entry) {
	if (entry.empty()) {
		return;
	}
	ExecutionRegionContractShapePart part;
	part.node_kind = node_kind;
	part.shape = std::move(entry);
	parts.push_back(std::move(part));
}

static string DescribeExecutionRegionContractShapeParts(const vector<ExecutionRegionContractShapePart> &parts) {
	string result;
	for (auto &part : parts) {
		AppendExecutionRegionContractShapeEntry(result, part.shape);
	}
	return result;
}

static string DescribeExecutionSourceProtocolContractShape(const ExecutionSourceContract &source) {
	return DescribeExecutionSourceProtocolContractShape(source.kind, source.execution, source.filters.size(),
	                                                    source.projection_ids.size(), source.returned_column_count,
	                                                    source.dynamic_filters, source.in_out_function);
}

static string DescribeExecutionSourceProtocolContractShape(const ExecutionRegionSourceInfo &source) {
	return DescribeExecutionSourceProtocolContractShape(source.kind, source.execution, source.filters.size(),
	                                                    source.projection_ids.size(), source.returned_column_count,
	                                                    source.dynamic_filters, source.in_out_function);
}

static string DescribeExecutionRegionOperatorContractShape(const ExecutionRegionOperatorInfo &op) {
	if (op.hash_join_contract.present) {
		return "op(" + DescribeExecutionRegionHashJoinContractShape(op.hash_join_contract) + ")";
	}
	if (op.nested_loop_join_contract.present) {
		return "op(" + DescribeExecutionRegionNestedLoopJoinContractShape(op.nested_loop_join_contract) + ")";
	}
	return "op(kind=" + string(ExecutionRegionOperatorContractKindToString(op.kind)) + ")";
}

static string DescribeExecutionRegionSinkContractShape(const ExecutionRegionSinkInfo &sink) {
	if (sink.hash_join_contract.present) {
		return "sink(" + DescribeExecutionRegionHashJoinContractShape(sink.hash_join_contract) + ")";
	}
	if (sink.nested_loop_join_contract.present) {
		return "sink(" + DescribeExecutionRegionNestedLoopJoinContractShape(sink.nested_loop_join_contract) + ")";
	}
	if (sink.aggregate_contract.present) {
		return "sink(" + DescribeExecutionRegionAggregateContractShape(sink.aggregate_contract) + ")";
	}
	if (sink.order_contract.present) {
		return "sink(" + DescribeExecutionRegionOrderContractShape(sink.order_contract) + ")";
	}
	return "sink(kind=" + string(ExecutionRegionSinkKindToString(sink.kind)) + ")";
}

bool ExecutionRegionAggregateFunctionIsCount(const string &function_name) {
	return function_name == "count" || function_name == "count_star";
}

bool ExecutionRegionAggregateFunctionIsSum(const string &function_name) {
	return function_name == "sum" || function_name == "sum_no_overflow";
}

void AccumulateExecutionRegionHashJoinKind(ExecutionRegionJoinType join_type, idx_t &hash_join_count,
                                           idx_t &right_hash_join_count, idx_t &inner_hash_join_count) {
	hash_join_count++;
	switch (join_type) {
	case ExecutionRegionJoinType::RIGHT:
		right_hash_join_count++;
		break;
	case ExecutionRegionJoinType::INNER:
		inner_hash_join_count++;
		break;
	default:
		break;
	}
}

void AccumulateExecutionRegionAggregateFunctionKinds(const ExecutionRegionAggregateContract &contract,
                                                     idx_t &aggregate_count, idx_t &count_function_count,
                                                     idx_t &sum_function_count, idx_t &other_function_count) {
	if (!contract.present) {
		return;
	}
	aggregate_count += contract.aggregate_count;
	for (auto &function_name : contract.aggregate_functions) {
		if (ExecutionRegionAggregateFunctionIsCount(function_name)) {
			count_function_count++;
		} else if (ExecutionRegionAggregateFunctionIsSum(function_name)) {
			sum_function_count++;
		} else {
			other_function_count++;
		}
	}
}

static bool ExecutionRegionOrderDependencyCoveredByPrimitiveUpdate(const ExecutionRegionAggregateInput &aggregate) {
	return aggregate.primitive_update_ready &&
	       aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE;
}

string ExecutionRegionAggregateNativeStateUpdateBlocker(const ExecutionRegionAggregateContract &contract,
                                                        const vector<ExecutionRegionAggregateInput> &aggregates,
                                                        const vector<ExecutionRegionGroupInput> &groups) {
	if (!contract.present) {
		return "aggregate-state-update-contract-missing";
	}
	if (contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	    contract.distinct_child_count != 0 || contract.distinct_filter_count != 0) {
		return "aggregate-state-update-distinct-state";
	}
	if (contract.aggregate_filter_count != 0) {
		return "aggregate-state-update-filtered-aggregate";
	}
	if (contract.aggregate_order_count != 0) {
		return "aggregate-state-update-ordered-aggregate";
	}

	switch (contract.kind) {
	case ExecutionRegionAggregateOperatorKind::HASH:
	case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
		if (groups.size() != contract.group_count) {
			return "aggregate-state-update-group-binding-count";
		}
		if (contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY) {
			return contract.native_grouped_state_contract.blocker.empty()
			           ? "aggregate-state-update-grouped-state-contract"
			           : contract.native_grouped_state_contract.blocker;
		}
		for (auto &group : groups) {
			if (!group.supported_reference) {
				return group.reason.empty() ? "aggregate-state-update-group-reference" : group.reason;
			}
		}
		break;
	case ExecutionRegionAggregateOperatorKind::UNGROUPED:
		if (!groups.empty()) {
			return "ungrouped-aggregate-state-update-unexpected-groups";
		}
		break;
	default:
		return "aggregate-state-update-operator-kind";
	}

	for (auto &aggregate : aggregates) {
		if (!aggregate.reason.empty()) {
			return aggregate.reason;
		}
		if (aggregate.distinct) {
			return "aggregate-state-update-distinct-aggregate";
		}
		if (aggregate.has_filter) {
			return "aggregate-state-update-filtered-aggregate";
		}
		if (aggregate.has_order_bys ||
		    (aggregate.order_dependent && !ExecutionRegionOrderDependencyCoveredByPrimitiveUpdate(aggregate))) {
			return "aggregate-state-update-ordered-aggregate";
		}
		if (!aggregate.has_state_update) {
			return "aggregate-state-update-callback-missing";
		}
		if (!aggregate.payload_expressions_ready) {
			return aggregate.payload_expression_blocker.empty() ? "aggregate-state-update-payload-expression"
			                                                    : aggregate.payload_expression_blocker;
		}
		if (!aggregate.supported_payload_references) {
			return "aggregate-state-update-payload-reference";
		}
	}
	return string();
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

static string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir) {
	string result = "pipeline";
	for (auto &node : region_ir.nodes) {
		result += ";";
		result += node.label;
		result += ":";
		result += StringUtil::Lower(string(ExecutionRegionNodeKindToString(node.kind)));
		result += ":";
		result += node.operator_name;
		result += ":";
		result += StringUtil::Lower(string(ExecutionRegionBoundaryKindToString(node.boundary)));
	}
	return result;
}

static string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir, idx_t first_node,
                                                   idx_t node_count) {
	string result = "pipeline";
	auto end_node = first_node + node_count;
	for (idx_t node_idx = first_node; node_idx < end_node && node_idx < region_ir.nodes.size(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		result += ";";
		result += node.label;
		result += ":";
		result += StringUtil::Lower(string(ExecutionRegionNodeKindToString(node.kind)));
		result += ":";
		result += node.operator_name;
		result += ":";
		result += StringUtil::Lower(string(ExecutionRegionBoundaryKindToString(node.boundary)));
	}
	return result;
}

static string DescribeExecutionRegionCandidateTraits(const ExecutionRegionCandidateTraits &traits) {
	string result = "traits<source=" + ExecutionRegionBool(traits.HasSource());
	result += ",sink=" + ExecutionRegionBool(traits.HasSink());
	result += ",source_kind=" + string(ExecutionRegionSourceKindToString(traits.source_kind));
	result += ",source_execution=" + string(ExecutionRegionSourceExecutionKindToString(traits.source_execution));
	result += ",sink_kind=" + string(ExecutionRegionSinkKindToString(traits.sink_kind));
	result += ",expression_traits_known=" + ExecutionRegionBool(traits.expression_traits_known);
	result += ",estimated_source_cardinality=" + std::to_string(traits.estimated_source_cardinality);
	result += ",source_filters=" + std::to_string(traits.source_filter_count);
	result += ",source_filter_expressions=" + std::to_string(traits.source_filter_expression_count);
	result += ",source_filter_missing=" + std::to_string(traits.source_filter_missing_count);
	result += ",source_contract_input_columns=" + std::to_string(traits.source_contract_input_column_count);
	result +=
	    ",source_contract_filter_prune_required=" + ExecutionRegionBool(traits.source_contract_filter_prune_required);
	result += ",source_projection_pushdown=" + ExecutionRegionBool(traits.source_projection_pushdown);
	result += ",source_filter_pushdown=" + ExecutionRegionBool(traits.source_filter_pushdown);
	result += ",source_filter_prune=" + ExecutionRegionBool(traits.source_filter_prune);
	result += ",source_comparison_filters=" + std::to_string(traits.source_comparison_filter_count);
	result += ",source_integer_comparison_filters=" + std::to_string(traits.source_integer_comparison_filter_count);
	result +=
	    ",source_non_integer_comparison_filters=" + std::to_string(traits.source_non_integer_comparison_filter_count);
	result += ",source_conjunction_filters=" + std::to_string(traits.source_conjunction_filter_count);
	result += ",source_projected_columns=" + std::to_string(traits.source_projected_column_count);
	result += ",source_returned_columns=" + std::to_string(traits.source_returned_column_count);
	result += ",filters=" + std::to_string(traits.filter_count);
	result += ",projections=" + std::to_string(traits.projection_count);
	result += ",operators=" + std::to_string(traits.operator_count);
	result += ",hash_join_operators=" + std::to_string(traits.hash_join_operator_count);
	result += ",right_hash_join_operators=" + std::to_string(traits.right_hash_join_operator_count);
	result += ",inner_hash_join_operators=" + std::to_string(traits.inner_hash_join_operator_count);
	result += ",aggregates=" + std::to_string(traits.aggregate_count);
	result += ",aggregate_count_functions=" + std::to_string(traits.aggregate_count_function_count);
	result += ",aggregate_sum_functions=" + std::to_string(traits.aggregate_sum_function_count);
	result += ",aggregate_other_functions=" + std::to_string(traits.aggregate_other_function_count);
	result += ",core_expression_operators=" + std::to_string(traits.core_expression_operator_count);
	result += ",arithmetic_projections=" + std::to_string(traits.arithmetic_projection_count);
	result += ",integer_arithmetic_projections=" + std::to_string(traits.integer_arithmetic_projection_count);
	result += ",non_integer_arithmetic_projections=" + std::to_string(traits.non_integer_arithmetic_projection_count);
	result += ",high_cost_projections=" + std::to_string(traits.high_cost_projection_count);
	result += ",reference_projections=" + std::to_string(traits.reference_projection_count);
	result += ",comparison_filters=" + std::to_string(traits.comparison_filter_count);
	result += ",integer_comparison_filters=" + std::to_string(traits.integer_comparison_filter_count);
	result += ",non_integer_comparison_filters=" + std::to_string(traits.non_integer_comparison_filter_count);
	result += ",conjunction_filters=" + std::to_string(traits.conjunction_filter_count);
	result += ",expression_nodes=" + std::to_string(traits.expression_node_count);
	result += ",predicate_expressions=" + std::to_string(traits.predicate_expression_count);
	result += ",control_expressions=" + std::to_string(traits.control_expression_count);
	result += ",reference_expressions=" + std::to_string(traits.reference_expression_count);
	result += ",expression_cost=" + std::to_string(traits.expression_cost);
	result += ",string_predicates=" + std::to_string(traits.string_predicate_expression_count);
	result += ",high_cost_string_predicates=" + std::to_string(traits.high_cost_string_predicate_expression_count);
	result += ",string_like_expressions=" + std::to_string(traits.string_like_expression_count);
	result += ",string_contains_expressions=" + std::to_string(traits.string_contains_expression_count);
	result += ",string_prefix_expressions=" + std::to_string(traits.string_prefix_expression_count);
	result += ",string_suffix_expressions=" + std::to_string(traits.string_suffix_expression_count);
	result += ",expression_missing=" + std::to_string(traits.expression_missing_count);
	result += ",operator_missing=" + std::to_string(traits.operator_missing_count);
	result += ">";
	return result;
}

static ExecutionRegionSourceKind InferExecutionRegionSourceKind(const ExecutionRegionNode &node) {
	if (node.source) {
		return node.source->kind;
	}
	if (node.operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN) {
		return ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN;
	}
	if (node.boundary == ExecutionRegionBoundaryKind::SCAN ||
	    node.boundary == ExecutionRegionBoundaryKind::SOURCE_CONTRACT) {
		return ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN;
	}
	if (node.kind == ExecutionRegionNodeKind::SOURCE) {
		return ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	}
	return ExecutionRegionSourceKind::NONE;
}

static void RecordExecutionRegionContractOwnership(ExecutionRegionContract &contract,
                                                   ExecutionRegionOwnershipKind ownership) {
	switch (ownership) {
	case ExecutionRegionOwnershipKind::GENERATED_IR:
		contract.generated_operator_count++;
		break;
	case ExecutionRegionOwnershipKind::SOURCE_BOUNDARY:
		contract.source_boundary_count++;
		break;
	default:
		break;
	}
}

static void RecordExecutionRegionMissingContract(ExecutionRegionContract &contract, const string &required_capability,
                                                 const string &blocker) {
	contract.missing_contract_count++;
	AddExecutionRegionUniqueString(contract.required_capabilities, required_capability);
	AddExecutionRegionUniqueString(contract.blockers, blocker);
}

static ExecutionRegionSourceExecutionKind
GetExecutionRegionCandidateSourceExecution(const ExecutionRegionCandidate &candidate, const ExecutionRegionNode &node) {
	if (candidate.source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		return candidate.source_execution;
	}
	return node.source ? node.source->execution : ExecutionRegionSourceExecutionKind::NONE;
}

static bool ExecutionRegionCandidateUsesScanFilters(const ExecutionRegionCandidate &candidate,
                                                    const ExecutionRegionNode &node) {
	if (candidate.uses_scan_filters) {
		return true;
	}
	auto source_execution = GetExecutionRegionCandidateSourceExecution(candidate, node);
	if (source_execution != ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT || !node.source ||
	    node.source->filters.empty() || !node.source->table_scan_contract.present) {
		return false;
	}
	return node.source->table_scan_contract.filter_pushdown;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionSourceProtocolContractOwnership(const ExecutionRegionSourceInfo &source,
                                                 ExecutionRegionContract &region_contract,
                                                 ExecutionRegionSourceExecutionKind execution) {
	auto &source_contract = source.source_contract;
	if (execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    source_contract.status == ExecutionRegionSourceContractStatus::READY) {
		AddExecutionRegionUniqueString(region_contract.required_capabilities, source_contract.required_capability);
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	if (execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		AddExecutionRegionUniqueString(region_contract.blockers, source_contract.blocker);
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (!source_contract.required_capability.empty() || !source_contract.blocker.empty()) {
		RecordExecutionRegionMissingContract(region_contract, source_contract.required_capability,
		                                     source_contract.blocker);
	}
	return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionRegionStateScanOwnership(const ExecutionRegionSourceInfo &source,
                                          ExecutionRegionContract &region_contract) {
	auto &state_contract = source.native_state_scan_contract;
	if (state_contract.status == ExecutionRegionStateContractStatus::NONE) {
		return ExecutionRegionOwnershipKind::NONE;
	}
	if (state_contract.status == ExecutionRegionStateContractStatus::READY) {
		AddExecutionRegionUniqueString(region_contract.required_capabilities, state_contract.required_capability);
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	RecordExecutionRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
	return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
}

static void RecordExecutionRegionGroupedStateContract(ExecutionRegionContract &region_contract,
                                                      const ExecutionRegionAggregateContract &contract);

static ExecutionRegionOwnershipKind
ClassifyExecutionCompiledContractOwnership(const ExecutionCompiledOperatorContract &compiled_contract,
                                           ExecutionRegionContract &region_contract, const string &blocker_reason) {
	if (!compiled_contract.Present()) {
		RecordExecutionRegionMissingContract(region_contract, "native-operator-contract", blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	bool saw_native = false;
	bool saw_generated = false;
	bool saw_source_boundary = false;
	bool saw_missing = false;
	for (auto &stage : compiled_contract.stages) {
		switch (stage.execution) {
		case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
			saw_native = true;
			AddExecutionRegionUniqueString(region_contract.required_capabilities, stage.required_capability);
			break;
		case ExecutionRegionStageExecutionKind::GENERATED_IR:
			saw_generated = true;
			break;
		case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
			saw_source_boundary = true;
			AddExecutionRegionUniqueString(region_contract.blockers,
			                               stage.blocker.empty() ? blocker_reason : stage.blocker);
			break;
		case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
			saw_missing = true;
			RecordExecutionRegionMissingContract(region_contract, stage.required_capability,
			                                     stage.blocker.empty() ? blocker_reason : stage.blocker);
			break;
		default:
			break;
		}
	}
	if (saw_missing) {
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	if (saw_source_boundary) {
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (saw_native) {
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	}
	if (saw_generated) {
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	}
	return ExecutionRegionOwnershipKind::NONE;
}

static ExecutionRegionOwnershipKind
ClassifyExecutionRegionSourceOwnership(const ExecutionRegionNode &node, ExecutionRegionContract &region_contract,
                                       ExecutionRegionSourceExecutionKind execution) {
	if (!node.source) {
		RecordExecutionRegionMissingContract(region_contract, "source-contract", node.blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	auto &source = *node.source;
	if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
		region_contract.state_scan_ownership = ClassifyExecutionRegionStateScanOwnership(source, region_contract);
		if (region_contract.state_scan_ownership != ExecutionRegionOwnershipKind::NONE) {
			return region_contract.state_scan_ownership;
		}
		return ClassifyExecutionSourceProtocolContractOwnership(source, region_contract, execution);
	}
	region_contract.state_scan_ownership = ExecutionRegionOwnershipKind::NONE;
	return ClassifyExecutionSourceProtocolContractOwnership(source, region_contract, execution);
}

static void RecordExecutionRegionGroupedStateContract(ExecutionRegionContract &region_contract,
                                                      const ExecutionRegionAggregateContract &contract) {
	if (contract.present) {
		region_contract.hash_aggregate_lookup_present = true;
		if (contract.kind == ExecutionRegionAggregateOperatorKind::PERFECT_HASH &&
		    contract.native_hash_lookup_contract.status == ExecutionRegionStateContractStatus::READY) {
			region_contract.hash_aggregate_lookup_mode = "generated-perfect-hash";
		} else if (contract.native_hash_lookup_contract.status == ExecutionRegionStateContractStatus::READY) {
			region_contract.hash_aggregate_lookup_mode = "native-contract";
		} else {
			region_contract.hash_aggregate_lookup_mode = "blocked";
		}
		region_contract.hash_aggregate_lookup_native_blocker = contract.native_hash_lookup_contract.blocker;
		region_contract.hash_aggregate_lookup_layout_blocker = contract.hash_lookup_layout_blocker;
		region_contract.hash_aggregate_lookup_row_compare_blocker = contract.hash_lookup_layout_row_compare_blocker;
		region_contract.hash_aggregate_lookup_backend_lowering_blocker =
		    contract.hash_lookup_layout_backend_lowering_blocker;
	}
	auto &state_contract = contract.native_grouped_state_contract;
	if (state_contract.status == ExecutionRegionStateContractStatus::NONE ||
	    state_contract.status == ExecutionRegionStateContractStatus::READY) {
		if (state_contract.status == ExecutionRegionStateContractStatus::READY) {
			AddExecutionRegionUniqueString(region_contract.required_capabilities, state_contract.required_capability);
		}
		return;
	}
	RecordExecutionRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
}

static ExecutionRegionOwnershipKind ClassifyExecutionRegionSinkOwnership(const ExecutionRegionNode &node,
                                                                         ExecutionRegionContract &region_contract) {
	if (!node.sink) {
		RecordExecutionRegionMissingContract(region_contract, "native-sink-contract", node.blocker_reason);
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	auto &sink = *node.sink;
	if (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		RecordExecutionRegionGroupedStateContract(region_contract, sink.aggregate_contract);
	}
	return ClassifyExecutionCompiledContractOwnership(node.compiled_contract, region_contract, node.blocker_reason);
}

static ExecutionRegionOwnershipKind CombineExecutionRegionTransformOwnership(ExecutionRegionOwnershipKind current,
                                                                             ExecutionRegionOwnershipKind next) {
	if (current == ExecutionRegionOwnershipKind::MISSING_CONTRACT ||
	    next == ExecutionRegionOwnershipKind::MISSING_CONTRACT) {
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	}
	if (current == ExecutionRegionOwnershipKind::GENERATED_IR || next == ExecutionRegionOwnershipKind::GENERATED_IR) {
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	}
	return current == ExecutionRegionOwnershipKind::NONE ? next : current;
}

static string DescribeExecutionRegionContract(const ExecutionRegionContract &contract) {
	string result = "contract<abi=" + string(ExecutionRegionABIToString(contract.abi));
	result += ",owns_source=" + ExecutionRegionBool(contract.OwnsSource());
	result += ",owns_transform=" + ExecutionRegionBool(contract.OwnsTransform());
	result += ",owns_sink=" + ExecutionRegionBool(contract.OwnsSink());
	result += ",owns_state_scan=" + ExecutionRegionBool(contract.OwnsStateScan());
	result += ",source=" + string(ExecutionRegionOwnershipKindToString(contract.source_ownership));
	result += ",state_scan=" + string(ExecutionRegionOwnershipKindToString(contract.state_scan_ownership));
	result += ",transform=" + string(ExecutionRegionOwnershipKindToString(contract.transform_ownership));
	result += ",sink=" + string(ExecutionRegionOwnershipKindToString(contract.sink_ownership));
	result += ",generated_ops=" + std::to_string(contract.generated_operator_count);
	result += ",source_boundaries=" + std::to_string(contract.source_boundary_count);
	result += ",missing_contracts=" + std::to_string(contract.missing_contract_count);
	result += ",required_capabilities=" + BuildExecutionRegionStringList(contract.required_capabilities);
	result += ",blockers=" + BuildExecutionRegionStringList(contract.blockers);
	result += ",hash_aggregate_lookup_present=" + ExecutionRegionBool(contract.hash_aggregate_lookup_present);
	result += ",hash_aggregate_lookup_mode=" + contract.hash_aggregate_lookup_mode;
	result += ",hash_aggregate_lookup_native_blocker=" + contract.hash_aggregate_lookup_native_blocker;
	result += ",hash_aggregate_lookup_layout_blocker=" + contract.hash_aggregate_lookup_layout_blocker;
	result += ",hash_aggregate_lookup_row_compare_blocker=" + contract.hash_aggregate_lookup_row_compare_blocker;
	result +=
	    ",hash_aggregate_lookup_backend_lowering_blocker=" + contract.hash_aggregate_lookup_backend_lowering_blocker;
	result += ">";
	return result;
}

static ExecutionRegionABI DetermineExecutionRegionContractABI(const ExecutionRegionContract &contract) {
	if (contract.OwnsSource() && contract.OwnsSink()) {
		return ExecutionRegionABI::FULL_PIPELINE;
	}
	return ExecutionRegionABI::NONE;
}

static ExecutionRegionContract BuildExecutionRegionContract(const ExecutionRegionIR &region_ir,
                                                            const ExecutionRegionCandidate &candidate,
                                                            ExecutionRegionIRMode mode) {
	ExecutionRegionContract contract;
	bool has_transform = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			contract.source_ownership = ClassifyExecutionRegionSourceOwnership(
			    node, contract, GetExecutionRegionCandidateSourceExecution(candidate, node));
			RecordExecutionRegionContractOwnership(contract, contract.source_ownership);
			break;
		case ExecutionRegionNodeKind::FILTER:
		case ExecutionRegionNodeKind::PROJECTION: {
			auto ownership = node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                             : ExecutionRegionOwnershipKind::MISSING_CONTRACT;
			if (ownership == ExecutionRegionOwnershipKind::MISSING_CONTRACT) {
				RecordExecutionRegionMissingContract(contract, "native-expression-ir", node.blocker_reason);
			}
			contract.transform_ownership =
			    CombineExecutionRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordExecutionRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		case ExecutionRegionNodeKind::SINK:
			contract.sink_ownership = ClassifyExecutionRegionSinkOwnership(node, contract);
			RecordExecutionRegionContractOwnership(contract, contract.sink_ownership);
			break;
		case ExecutionRegionNodeKind::OPERATOR: {
			auto ownership =
			    ClassifyExecutionCompiledContractOwnership(node.compiled_contract, contract, node.blocker_reason);
			contract.transform_ownership =
			    CombineExecutionRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordExecutionRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		default:
			contract.transform_ownership = CombineExecutionRegionTransformOwnership(
			    contract.transform_ownership, ExecutionRegionOwnershipKind::MISSING_CONTRACT);
			RecordExecutionRegionMissingContract(contract, "native-region-node", node.blocker_reason);
			RecordExecutionRegionContractOwnership(contract, ExecutionRegionOwnershipKind::MISSING_CONTRACT);
			has_transform = true;
			break;
		}
	}
	if (!has_transform) {
		contract.transform_ownership = ExecutionRegionOwnershipKind::NONE;
	}
	contract.abi = DetermineExecutionRegionContractABI(contract);
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		contract.ir = DescribeExecutionRegionContract(contract);
	}
	return contract;
}

static ExecutionRegionStagePlan BuildExecutionRegionStagePlan(const ExecutionRegionIR &region_ir,
                                                              const ExecutionRegionCandidate &candidate,
                                                              ExecutionRegionIRMode mode);

static void AccumulateExecutionRegionFilterTraits(const ExecutionExpressionTraits &expression_traits,
                                                  idx_t &comparison_filter_count,
                                                  idx_t &integer_comparison_filter_count,
                                                  idx_t &non_integer_comparison_filter_count,
                                                  idx_t &conjunction_filter_count) {
	if (expression_traits.has_comparison_binary) {
		comparison_filter_count++;
	}
	if (expression_traits.has_integer_comparison_operands) {
		integer_comparison_filter_count++;
	}
	if (expression_traits.has_non_integer_comparison_operands) {
		non_integer_comparison_filter_count++;
	}
	if (expression_traits.has_conjunction) {
		conjunction_filter_count++;
	}
}

static void AccumulateExecutionRegionExpressionTraits(const ExecutionExpressionTraits &expression_traits,
                                                      ExecutionRegionCandidateTraits &traits) {
	traits.expression_node_count += expression_traits.expression_node_count;
	traits.reference_expression_count += expression_traits.reference_expression_count;
	traits.predicate_expression_count += expression_traits.predicate_expression_count;
	traits.control_expression_count += expression_traits.control_expression_count;
	traits.expression_cost += expression_traits.expression_cost;
	traits.string_predicate_expression_count += expression_traits.string_predicate_count;
	traits.high_cost_string_predicate_expression_count += expression_traits.high_cost_string_predicate_count;
	traits.string_like_expression_count += expression_traits.string_like_count;
	traits.string_contains_expression_count += expression_traits.string_contains_count;
	traits.string_prefix_expression_count += expression_traits.string_prefix_count;
	traits.string_suffix_expression_count += expression_traits.string_suffix_count;
}

static bool ExecutionRegionStageIsOperatorSlot(const ExecutionRegionStage &stage) {
	return stage.kind == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::OPERATOR_BOUNDARY;
}

static bool ExecutionRegionStageRequiresMissingContract(ExecutionRegionStageExecutionKind execution) {
	return execution == ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
}

static void AccumulateExecutionRegionStageTraits(const ExecutionRegionStage &stage,
                                                 ExecutionRegionCandidateTraits &traits) {
	if ((stage.kind == ExecutionRegionStageKind::FILTER || stage.kind == ExecutionRegionStageKind::PROJECTION) &&
	    ExecutionRegionStageRequiresMissingContract(stage.execution)) {
		traits.expression_missing_count++;
	}
	if (ExecutionRegionStageIsOperatorSlot(stage)) {
		if (ExecutionRegionStageRequiresMissingContract(stage.execution)) {
			traits.operator_missing_count++;
		}
	}
}

static void AccumulateExecutionRegionStagePlanTraits(const ExecutionRegionStagePlan &stage_plan,
                                                     ExecutionRegionCandidateTraits &traits) {
	for (auto &stage : stage_plan.stages) {
		AccumulateExecutionRegionStageTraits(stage, traits);
	}
}

static void AccumulateExecutionRegionSourceTraits(const ExecutionRegionNode &node,
                                                  ExecutionRegionSourceExecutionKind execution,
                                                  ExecutionRegionCandidateTraits &traits,
                                                  bool &unknown_expression_traits) {
	traits.source_kind = InferExecutionRegionSourceKind(node);
	traits.source_execution = execution;
	if (!node.source) {
		return;
	}

	traits.estimated_source_cardinality = node.source->estimated_source_cardinality;
	traits.source_projected_column_count = node.source->projection_ids.size();
	traits.source_returned_column_count = node.source->returned_column_count;
	traits.source_filter_count = node.source->filters.size();
	if (node.source->table_scan_contract.present) {
		auto &table_scan_contract = node.source->table_scan_contract;
		traits.source_contract_input_column_count = table_scan_contract.source_contract_input_column_count;
		traits.source_contract_filter_prune_required = table_scan_contract.source_contract_filter_prune_required;
		traits.source_projection_pushdown = table_scan_contract.projection_pushdown;
		traits.source_filter_pushdown = table_scan_contract.filter_pushdown;
		traits.source_filter_prune = table_scan_contract.filter_prune;
	}
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root) {
			traits.source_filter_missing_count++;
			unknown_expression_traits = true;
			continue;
		}
		traits.source_filter_expression_count++;
		AccumulateExecutionRegionFilterTraits(filter.expression->traits, traits.source_comparison_filter_count,
		                                      traits.source_integer_comparison_filter_count,
		                                      traits.source_non_integer_comparison_filter_count,
		                                      traits.source_conjunction_filter_count);
	}
}

static ExecutionRegionCandidateTraits BuildExecutionRegionCandidateTraits(const ExecutionRegionIR &region_ir,
                                                                          const ExecutionRegionCandidate &candidate,
                                                                          const ExecutionRegionStagePlan &stage_plan,
                                                                          ExecutionRegionIRMode mode) {
	ExecutionRegionCandidateTraits traits;
	bool unknown_expression_traits = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			AccumulateExecutionRegionSourceTraits(node, GetExecutionRegionCandidateSourceExecution(candidate, node),
			                                      traits, unknown_expression_traits);
			break;
		case ExecutionRegionNodeKind::SINK:
			traits.sink_present = true;
			traits.sink_kind = node.sink ? node.sink->kind : ExecutionRegionSinkKind::NONE;
			if (node.sink) {
				AccumulateExecutionRegionAggregateFunctionKinds(
				    node.sink->aggregate_contract, traits.aggregate_count, traits.aggregate_count_function_count,
				    traits.aggregate_sum_function_count, traits.aggregate_other_function_count);
			}
			break;
		case ExecutionRegionNodeKind::FILTER:
			traits.filter_count++;
			if (node.filter && node.filter->root) {
				traits.core_expression_operator_count++;
				AccumulateExecutionRegionExpressionTraits(node.filter->traits, traits);
				AccumulateExecutionRegionFilterTraits(
				    node.filter->traits, traits.comparison_filter_count, traits.integer_comparison_filter_count,
				    traits.non_integer_comparison_filter_count, traits.conjunction_filter_count);
			} else {
				unknown_expression_traits = true;
			}
			break;
		case ExecutionRegionNodeKind::PROJECTION:
			traits.projection_count++;
			if (node.projections.empty()) {
				unknown_expression_traits = true;
				break;
			}
			traits.core_expression_operator_count++;
			for (auto &projection : node.projections) {
				if (!projection || !projection->root) {
					unknown_expression_traits = true;
					continue;
				}
				AccumulateExecutionRegionExpressionTraits(projection->traits, traits);
				if (projection->traits.root_is_reference) {
					traits.reference_projection_count++;
				}
				if (projection->traits.arithmetic_binary_count > 0) {
					traits.arithmetic_projection_count++;
				}
				if (projection->traits.integer_arithmetic_result_count > 0) {
					traits.integer_arithmetic_projection_count++;
				}
				if (projection->traits.non_integer_arithmetic_result_count > 0) {
					traits.non_integer_arithmetic_projection_count++;
				}
				if (projection->traits.expression_cost >= HIGH_COST_GENERATED_PROJECTION_EXPRESSION_COST) {
					traits.high_cost_projection_count++;
				}
			}
			break;
		case ExecutionRegionNodeKind::OPERATOR:
			traits.operator_count++;
			if (node.operator_info && node.operator_info->hash_join_contract.present) {
				AccumulateExecutionRegionHashJoinKind(
				    node.operator_info->hash_join_contract.join_type, traits.hash_join_operator_count,
				    traits.right_hash_join_operator_count, traits.inner_hash_join_operator_count);
			}
			break;
		default:
			traits.operator_count++;
			break;
		}
	}
	AccumulateExecutionRegionStagePlanTraits(stage_plan, traits);
	traits.expression_traits_known = !unknown_expression_traits;
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		traits.ir = DescribeExecutionRegionCandidateTraits(traits);
	}
	return traits;
}

static ExecutionRegionCandidateTraits BuildExecutionRegionSpanTraits(const ExecutionRegionIR &region_ir,
                                                                     ExecutionRegionCandidate span_candidate,
                                                                     ExecutionRegionIRMode mode) {
	span_candidate.contract = BuildExecutionRegionContract(region_ir, span_candidate, mode);
	span_candidate.stage_plan = BuildExecutionRegionStagePlan(region_ir, span_candidate, mode);
	return BuildExecutionRegionCandidateTraits(region_ir, span_candidate, span_candidate.stage_plan, mode);
}

static ExecutionRegionCandidateTraits BuildExecutionRegionContextTraits(const ExecutionRegionIR &region_ir,
                                                                        ExecutionRegionIRMode mode) {
	ExecutionRegionCandidate context_candidate;
	context_candidate.first_node = 0;
	context_candidate.node_count = region_ir.nodes.size();
	return BuildExecutionRegionSpanTraits(region_ir, std::move(context_candidate), mode);
}

static ExecutionRegionCandidateTraits BuildExecutionRegionUpstreamTraits(const ExecutionRegionIR &region_ir,
                                                                         const ExecutionRegionCandidate &candidate,
                                                                         ExecutionRegionIRMode mode) {
	if (candidate.first_node == 0 || region_ir.nodes.empty()) {
		return ExecutionRegionCandidateTraits();
	}
	ExecutionRegionCandidate upstream_candidate;
	upstream_candidate.first_node = 0;
	upstream_candidate.node_count = MinValue(candidate.first_node, NumericCast<idx_t>(region_ir.nodes.size()));
	return BuildExecutionRegionSpanTraits(region_ir, std::move(upstream_candidate), mode);
}

static ExecutionRegionCandidateTraits BuildExecutionRegionContinuationTraits(const ExecutionRegionIR &region_ir,
                                                                             const ExecutionRegionCandidate &candidate,
                                                                             ExecutionRegionIRMode mode) {
	ExecutionRegionCandidate continuation_candidate;
	continuation_candidate.first_node = MinValue(candidate.EndNode(), NumericCast<idx_t>(region_ir.nodes.size()));
	if (continuation_candidate.first_node >= region_ir.nodes.size()) {
		return ExecutionRegionCandidateTraits();
	}
	continuation_candidate.node_count = region_ir.nodes.size() - continuation_candidate.first_node;
	return BuildExecutionRegionSpanTraits(region_ir, std::move(continuation_candidate), mode);
}

static void AppendExecutionRegionCandidateShapeSegment(string &result, const string &segment) {
	if (segment.empty()) {
		return;
	}
	if (!result.empty()) {
		result += "-";
	}
	result += segment;
}

static void AppendExecutionRegionSourceShapeSegments(string &result, const ExecutionRegionNode &node) {
	if (!node.source) {
		return;
	}
	if (node.source->filters.empty() && node.source->projection_ids.empty()) {
		return;
	}
	switch (node.source->kind) {
	case ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN:
	case ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN:
	case ExecutionRegionSourceKind::GENERIC_SCAN:
		if (!node.source->filters.empty()) {
			AppendExecutionRegionCandidateShapeSegment(result, "scan-filter");
			if (!node.source->projection_ids.empty()) {
				AppendExecutionRegionCandidateShapeSegment(result, "scan-project");
			}
		}
		break;
	default:
		break;
	}
}

static string DescribeExecutionRegionCandidateShape(const ExecutionRegionIR &region_ir,
                                                    const ExecutionRegionCandidate &candidate) {
	string result;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == ExecutionRegionNodeKind::SOURCE) {
			AppendExecutionRegionSourceShapeSegments(result, node);
			continue;
		}
		AppendExecutionRegionCandidateShapeSegment(
		    result, StringUtil::Lower(string(ExecutionRegionNodeKindToString(node.kind))));
	}
	return result.empty() ? "boundary-only" : result;
}

static string GetExecutionRegionSignatureContext(const ExecutionRegionContract &contract) {
	if (ExecutionRegionABIIsFullPipeline(contract.abi)) {
		return "full-pipeline";
	}
	return "unknown";
}

static string GetExecutionRegionSinkSignatureFeature(const ExecutionRegionNode &node) {
	if (!node.sink) {
		return ExecutionRegionOperatorKindSignatureSegment(node.operator_kind) + "-sink";
	}
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
		return "result-collector-sink";
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return "nested-loop-join-build";
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case ExecutionRegionSinkKind::HASH_AGGREGATE_DISTINCT_SINK:
		return "hash-aggregate-distinct-sink";
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case ExecutionRegionSinkKind::SORT:
		return "sort";
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return "materialization";
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return "delim-join-sink";
	case ExecutionRegionSinkKind::NONE:
		return string();
	default:
		return ExecutionRegionOperatorKindSignatureSegment(node.operator_kind) + "-sink";
	}
}

static string GetExecutionRegionNodeSignatureFeature(const ExecutionRegionNode &node) {
	if (node.kind == ExecutionRegionNodeKind::SOURCE) {
		if (node.source && node.source->kind == ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN) {
			return "table-scan-source";
		}
		return ExecutionRegionOperatorKindSignatureSegment(node.operator_kind) + "-source";
	}
	if (node.kind == ExecutionRegionNodeKind::SINK) {
		return GetExecutionRegionSinkSignatureFeature(node);
	}
	if (node.boundary == ExecutionRegionBoundaryKind::OPERATOR_MISSING ||
	    node.boundary == ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY ||
	    node.boundary == ExecutionRegionBoundaryKind::OPERATOR_NATIVE) {
		return ExecutionRegionOperatorKindSignatureSegment(node.operator_kind) + "-operator";
	}
	if (node.boundary == ExecutionRegionBoundaryKind::EXPRESSION_MISSING) {
		return "expression-missing";
	}
	if (node.kind == ExecutionRegionNodeKind::FILTER || node.kind == ExecutionRegionNodeKind::PROJECTION) {
		return string();
	}
	return ExecutionRegionOperatorKindSignatureSegment(node.operator_kind);
}

static string BuildExecutionRegionFeatureShape(const ExecutionRegionIR &region_ir, idx_t first_node, idx_t node_count) {
	vector<string> features;
	const auto end_node = MinValue(first_node + node_count, NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = first_node; node_idx < end_node; node_idx++) {
		AddExecutionRegionFeature(features, GetExecutionRegionNodeSignatureFeature(region_ir.nodes[node_idx]));
	}
	return BuildExecutionRegionFeatureSetShape(std::move(features));
}

static vector<ExecutionRegionContractShapePart>
BuildExecutionRegionContractShapeParts(const ExecutionRegionIR &region_ir, idx_t first_node, idx_t node_count) {
	vector<ExecutionRegionContractShapePart> result;
	const auto end_node = MinValue(first_node + node_count, NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = first_node; node_idx < end_node; node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			if (node.source) {
				AppendExecutionRegionContractShapePart(result, node.kind,
				                                       DescribeExecutionSourceProtocolContractShape(*node.source));
			}
			break;
		case ExecutionRegionNodeKind::OPERATOR:
			if (node.operator_info) {
				AppendExecutionRegionContractShapePart(
				    result, node.kind, DescribeExecutionRegionOperatorContractShape(*node.operator_info));
			}
			break;
		case ExecutionRegionNodeKind::SINK:
			if (node.sink) {
				AppendExecutionRegionContractShapePart(result, node.kind,
				                                       DescribeExecutionRegionSinkContractShape(*node.sink));
			}
			break;
		default:
			break;
		}
	}
	return result;
}

static ExecutionRegionSignature BuildExecutionRegionSignature(const ExecutionRegionIR &region_ir,
                                                              const ExecutionRegionCandidate &candidate,
                                                              ExecutionRegionIRMode mode) {
	ExecutionRegionSignature signature;
	signature.context = GetExecutionRegionSignatureContext(candidate.contract);
	signature.shape = candidate.shape;
	signature.feature_shape = BuildExecutionRegionFeatureShape(region_ir, candidate.first_node, candidate.node_count);
	signature.context_feature_shape = BuildExecutionRegionFeatureShape(region_ir, 0, region_ir.nodes.size());
	signature.contract_shape_parts =
	    BuildExecutionRegionContractShapeParts(region_ir, candidate.first_node, candidate.node_count);
	signature.contract_shape = DescribeExecutionRegionContractShapeParts(signature.contract_shape_parts);
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		signature.ir = "signature<context=" + signature.context + ",shape=" + signature.shape +
		               ",features=" + signature.feature_shape + ",context_features=" + signature.context_feature_shape +
		               ",contract_shape=" + signature.contract_shape + ">";
	}
	return signature;
}

static ExecutionRegionStageExecutionKind
ExecutionRegionStageExecutionFromOwnership(ExecutionRegionOwnershipKind ownership) {
	switch (ownership) {
	case ExecutionRegionOwnershipKind::GENERATED_IR:
		return ExecutionRegionStageExecutionKind::GENERATED_IR;
	case ExecutionRegionOwnershipKind::NATIVE_CONTRACT:
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	case ExecutionRegionOwnershipKind::SOURCE_BOUNDARY:
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	case ExecutionRegionOwnershipKind::MISSING_CONTRACT:
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	case ExecutionRegionOwnershipKind::NONE:
	default:
		return ExecutionRegionStageExecutionKind::NONE;
	}
}

static ExecutionRegionOwnershipKind
ExecutionRegionOwnershipFromStageExecution(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::GENERATED_IR:
		return ExecutionRegionOwnershipKind::GENERATED_IR;
	case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
		return ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
	case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
		return ExecutionRegionOwnershipKind::SOURCE_BOUNDARY;
	case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
		return ExecutionRegionOwnershipKind::MISSING_CONTRACT;
	default:
		return ExecutionRegionOwnershipKind::NONE;
	}
}

static ExecutionRegionStageExecutionKind
ExecutionRegionSourceStageExecution(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                    ExecutionRegionSourceExecutionKind source_execution) {
	if (contract.source_ownership == ExecutionRegionOwnershipKind::NATIVE_CONTRACT) {
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	}
	if (node.source && node.source->source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED) {
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	}
	return ExecutionRegionStageExecutionFromOwnership(contract.source_ownership);
}

static bool ExecutionRegionProjectionHasExecutableWork(const ExecutionRegionNode &node) {
	for (auto &projection : node.projections) {
		if (!projection || !projection->root) {
			continue;
		}
		if (projection->root->kind != ExecutionExpressionIRKind::REFERENCE) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionStageHasExecutableWork(ExecutionRegionStageExecutionKind execution, bool executable_work) {
	return executable_work && execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
	       execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
}

static void AddExecutionRegionStage(ExecutionRegionStagePlan &plan, ExecutionRegionStageKind kind,
                                    ExecutionRegionStageExecutionKind execution, ExecutionRegionOwnershipKind ownership,
                                    idx_t node_index, const ExecutionRegionNode &node,
                                    idx_t filter_index = DConstants::INVALID_INDEX, string reason = string(),
                                    ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE,
                                    ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE,
                                    string required_capability = string(), bool executable_work = false) {
	ExecutionRegionStage stage;
	stage.kind = kind;
	stage.execution = execution;
	stage.ownership = ownership;
	stage.operation = operation;
	stage.drain = drain;
	stage.executable_work = executable_work;
	stage.node_index = node_index;
	stage.operator_index = node.operator_index;
	stage.filter_index = filter_index;
	stage.operator_name = node.operator_name;
	stage.required_capability = std::move(required_capability);
	stage.reason = std::move(reason);
	plan.has_executable_work =
	    plan.has_executable_work || ExecutionRegionStageHasExecutableWork(execution, executable_work);
	plan.stages.push_back(std::move(stage));
}

static string BuildExecutionRegionCompiledStageReason(const ExecutionCompiledStageContract &compiled_stage,
                                                      const ExecutionRegionNode &node) {
	string reason = compiled_stage.ir.empty() ? node.blocker_reason : compiled_stage.ir;
	if (!compiled_stage.blocker.empty() && !StringUtil::Contains(reason, compiled_stage.blocker)) {
		if (!reason.empty()) {
			reason += ";";
		}
		reason += "blocker=" + compiled_stage.blocker;
	}
	return reason;
}

static void AddExecutionRegionCompiledStage(ExecutionRegionStagePlan &plan,
                                            const ExecutionCompiledStageContract &compiled_stage, idx_t node_index,
                                            const ExecutionRegionNode &node,
                                            ExecutionRegionStageExecutionKind execution,
                                            ExecutionRegionOwnershipKind ownership) {
	AddExecutionRegionStage(plan, compiled_stage.stage, execution, ownership, node_index, node,
	                        DConstants::INVALID_INDEX, BuildExecutionRegionCompiledStageReason(compiled_stage, node),
	                        compiled_stage.operation, compiled_stage.drain, compiled_stage.required_capability,
	                        compiled_stage.executable_work);
}

static bool AddExecutionRegionCompiledStages(ExecutionRegionStagePlan &plan, const ExecutionRegionNode &node,
                                             idx_t node_index) {
	if (!node.compiled_contract.Present()) {
		return false;
	}
	for (auto &compiled_stage : node.compiled_contract.stages) {
		auto execution = compiled_stage.execution;
		auto ownership = ExecutionRegionOwnershipFromStageExecution(execution);
		AddExecutionRegionCompiledStage(plan, compiled_stage, node_index, node, execution, ownership);
	}
	return true;
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

static ExecutionRegionStagePlan BuildExecutionRegionStagePlan(const ExecutionRegionIR &region_ir,
                                                              const ExecutionRegionCandidate &candidate,
                                                              ExecutionRegionIRMode mode) {
	ExecutionRegionStagePlan plan;
	plan.shape = GetExecutionRegionSignatureContext(candidate.contract) + ":" + candidate.shape;
	const auto end_node = MinValue(candidate.EndNode(), NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = candidate.first_node; node_idx < end_node; node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE: {
			auto source_execution = GetExecutionRegionCandidateSourceExecution(candidate, node);
			bool added_source_stage = false;
			if (node.compiled_contract.Present()) {
				for (auto &compiled_stage : node.compiled_contract.stages) {
					if (compiled_stage.stage != ExecutionRegionStageKind::SOURCE) {
						continue;
					}
					auto execution = ExecutionRegionSourceStageExecution(node, candidate.contract, source_execution);
					auto ownership = ExecutionRegionOwnershipFromStageExecution(execution);
					AddExecutionRegionCompiledStage(plan, compiled_stage, node_idx, node, execution, ownership);
					added_source_stage = true;
				}
			}
			if (!added_source_stage) {
				auto execution = ExecutionRegionSourceStageExecution(node, candidate.contract, source_execution);
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE, execution,
				                        ExecutionRegionOwnershipFromStageExecution(execution), node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			if (node.source) {
				for (idx_t filter_idx = 0; filter_idx < node.source->filters.size(); filter_idx++) {
					auto &filter = node.source->filters[filter_idx];
					ExecutionRegionOwnershipKind filter_ownership;
					if (ExecutionRegionCandidateUsesScanFilters(candidate, node)) {
						filter_ownership = ExecutionRegionOwnershipKind::NATIVE_CONTRACT;
					} else {
						filter_ownership = ExecutionRegionOwnershipKind::MISSING_CONTRACT;
					}
					auto filter_execution =
					    source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
					        ? ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
					        : ExecutionRegionStageExecutionFromOwnership(filter_ownership);
					AddExecutionRegionStage(plan, ExecutionRegionStageKind::SOURCE_FILTER, filter_execution,
					                        filter_ownership, node_idx, node, filter_idx, filter.reason,
					                        ExecutionCompiledContractKind::FILTER_STAGE,
					                        ExecutionCompiledDrainKind::ZERO_OR_ONE_OUTPUT, string(),
					                        filter_execution == ExecutionRegionStageExecutionKind::GENERATED_IR);
				}
			}
			break;
		}
		case ExecutionRegionNodeKind::FILTER:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::FILTER,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                                    : ExecutionRegionOwnershipKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty());
			break;
		case ExecutionRegionNodeKind::PROJECTION:
			AddExecutionRegionStage(plan, ExecutionRegionStageKind::PROJECTION,
			                        node.blocker_reason.empty() ? ExecutionRegionStageExecutionKind::GENERATED_IR
			                                                    : ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
			                        node.blocker_reason.empty() ? ExecutionRegionOwnershipKind::GENERATED_IR
			                                                    : ExecutionRegionOwnershipKind::MISSING_CONTRACT,
			                        node_idx, node, DConstants::INVALID_INDEX, node.blocker_reason,
			                        ExecutionCompiledContractKind::NONE, ExecutionCompiledDrainKind::NONE, string(),
			                        node.blocker_reason.empty() && ExecutionRegionProjectionHasExecutableWork(node));
			break;
		case ExecutionRegionNodeKind::OPERATOR: {
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::OPERATOR_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
				                        ExecutionRegionOwnershipKind::MISSING_CONTRACT, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			break;
		}
		case ExecutionRegionNodeKind::SINK:
			if (!AddExecutionRegionCompiledStages(plan, node, node_idx)) {
				AddExecutionRegionStage(plan, ExecutionRegionStageKind::SINK_BOUNDARY,
				                        ExecutionRegionStageExecutionKind::MISSING_CONTRACT,
				                        ExecutionRegionOwnershipKind::MISSING_CONTRACT, node_idx, node,
				                        DConstants::INVALID_INDEX, node.blocker_reason);
			}
			break;
		default:
			break;
		}
	}
	if (ExecutionRegionShouldRenderDiagnostics(mode)) {
		plan.ir = DescribeExecutionRegionStagePlan(plan);
	}
	return plan;
}

static idx_t EstimateExecutionRegionCandidateCardinality(const ExecutionRegionIR &region_ir,
                                                         const ExecutionRegionCandidate &candidate) {
	idx_t result = 0;
	if (candidate.first_node > 0 && candidate.first_node - 1 < region_ir.nodes.size()) {
		result = MaxValue(result, region_ir.nodes[candidate.first_node - 1].estimated_cardinality);
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		result = MaxValue(result, region_ir.nodes[node_idx].estimated_cardinality);
	}
	return result;
}

static idx_t EstimateExecutionRegionCandidateSourceCardinality(const ExecutionRegionIR &region_ir,
                                                               const ExecutionRegionCandidate &candidate) {
	if (candidate.first_node >= region_ir.nodes.size()) {
		return 0;
	}
	auto &source = region_ir.nodes[candidate.first_node];
	if (!source.source) {
		return source.estimated_cardinality;
	}
	return MaxValue(source.source->estimated_source_cardinality, source.estimated_cardinality);
}

static string DescribeExecutionRegionCandidate(const ExecutionRegionCandidate &candidate) {
	string result = "candidate" + std::to_string(candidate.candidate_id);
	result += "<first_node=" + std::to_string(candidate.first_node);
	result += ",node_count=" + std::to_string(candidate.node_count);
	result += ",start_operator_index=" + std::to_string(candidate.start_operator_index);
	result += ",end_operator_index=" + std::to_string(candidate.end_operator_index);
	result += ",estimated_cardinality=" + std::to_string(candidate.estimated_cardinality);
	result += ",estimated_source_cardinality=" + std::to_string(candidate.estimated_source_cardinality);
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
	result += "," + candidate.signature.ir;
	if (!candidate.stage_plan.ir.empty()) {
		result += ",";
		result += candidate.stage_plan.ir;
	}
	result += ",pipeline_shape=" + candidate.pipeline_shape;
	result += "," + candidate.traits.ir;
	result += "," + candidate.contract.ir;
	if (!candidate.upstream_traits.ir.empty()) {
		result += ",upstream_" + candidate.upstream_traits.ir;
	}
	if (!candidate.context_traits.ir.empty()) {
		result += ",context_" + candidate.context_traits.ir;
	}
	if (!candidate.continuation_traits.ir.empty()) {
		result += ",continuation_" + candidate.continuation_traits.ir;
	}
	result += ">";
	return result;
}

static vector<LogicalType> GetExecutionRegionCandidateInputTypes(const ExecutionRegionIR &region_ir,
                                                                 const ExecutionRegionCandidate &candidate) {
	if (candidate.first_node < region_ir.nodes.size() &&
	    region_ir.nodes[candidate.first_node].kind == ExecutionRegionNodeKind::SOURCE) {
		return region_ir.nodes[candidate.first_node].output_types;
	}
	if (candidate.first_node > 0) {
		return region_ir.nodes[candidate.first_node - 1].output_types;
	}
	if (!region_ir.nodes.empty()) {
		return region_ir.nodes[candidate.first_node].output_types;
	}
	return vector<LogicalType>();
}

static vector<LogicalType> GetExecutionRegionCandidateOutputTypes(const ExecutionRegionIR &region_ir,
                                                                  const ExecutionRegionCandidate &candidate) {
	for (idx_t node_offset = candidate.node_count; node_offset > 0; node_offset--) {
		auto &node = region_ir.nodes[candidate.first_node + node_offset - 1];
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			continue;
		}
		return node.output_types;
	}
	return vector<LogicalType>();
}

static bool HasExecutionRegionCandidate(const ExecutionRegionIR &region_ir, idx_t first_node, idx_t node_count,
                                        idx_t start_operator_index, idx_t end_operator_index,
                                        ExecutionRegionSourceExecutionKind source_execution) {
	for (auto &candidate : region_ir.candidates) {
		if (candidate.first_node == first_node && candidate.node_count == node_count &&
		    candidate.start_operator_index == start_operator_index &&
		    candidate.end_operator_index == end_operator_index && candidate.source_execution == source_execution) {
			return true;
		}
	}
	return false;
}

static void AddExecutionRegionCandidateBlocker(ExecutionRegionIR &region_ir, string reason) {
	if (std::find(region_ir.candidate_blockers.begin(), region_ir.candidate_blockers.end(), reason) ==
	    region_ir.candidate_blockers.end()) {
		region_ir.candidate_blockers.push_back(std::move(reason));
	}
}

static string DescribeExecutionRegionCandidateSpan(idx_t first_node, idx_t node_count, idx_t start_operator_index,
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

static string AppendExecutionRegionCandidateDiagnostic(string reason, const string &diagnostic) {
	if (diagnostic.empty()) {
		return reason;
	}
	return std::move(reason) + ";" + diagnostic;
}

struct ExecutionRegionCandidateSummary {
	ExecutionRegionCandidate candidate;
	string blocker;

	bool Accepted() const {
		return blocker.empty();
	}
};

static bool ExecutionRegionCandidateCoversFullPipeline(const ExecutionRegionIR &region_ir,
                                                       const ExecutionRegionCandidate &candidate) {
	return candidate.first_node == 0 && candidate.node_count == region_ir.nodes.size();
}

static ExecutionRegionCandidateTraits
BuildExecutionRegionCandidateContextTraits(const ExecutionRegionIR &region_ir,
                                           const ExecutionRegionCandidate &candidate, ExecutionRegionIRMode mode) {
	if (ExecutionRegionCandidateCoversFullPipeline(region_ir, candidate)) {
		return candidate.traits;
	}
	return BuildExecutionRegionContextTraits(region_ir, mode);
}

static ExecutionRegionCandidateSummary
BuildExecutionRegionCandidateSummary(const ExecutionRegionIR &region_ir, idx_t candidate_id, idx_t first_node,
                                     idx_t node_count, idx_t start_operator_index, idx_t end_operator_index,
                                     ExecutionRegionSourceExecutionKind source_execution, ExecutionRegionIRMode mode) {
	ExecutionRegionCandidateSummary summary;
	auto &candidate = summary.candidate;
	candidate.candidate_id = candidate_id;
	candidate.first_node = first_node;
	candidate.node_count = node_count;
	candidate.start_operator_index = start_operator_index;
	candidate.end_operator_index = end_operator_index;
	candidate.source_execution = source_execution;
	candidate.estimated_cardinality = EstimateExecutionRegionCandidateCardinality(region_ir, candidate);
	candidate.estimated_source_cardinality = EstimateExecutionRegionCandidateSourceCardinality(region_ir, candidate);
	candidate.input_types = GetExecutionRegionCandidateInputTypes(region_ir, candidate);
	candidate.output_types = GetExecutionRegionCandidateOutputTypes(region_ir, candidate);
	candidate.shape = DescribeExecutionRegionCandidateShape(region_ir, candidate);
	candidate.pipeline_shape = DescribeExecutionRegionPipelineShape(region_ir, first_node, node_count);
	if (candidate.first_node < region_ir.nodes.size()) {
		candidate.uses_scan_filters = ExecutionRegionCandidateUsesScanFilters(candidate, region_ir.nodes[first_node]);
	}
	candidate.contract = BuildExecutionRegionContract(region_ir, candidate, mode);

	auto describe_span = [&]() {
		return DescribeExecutionRegionCandidateSpan(first_node, node_count, start_operator_index, end_operator_index,
		                                            source_execution);
	};
	if (!candidate.contract.OwnsSource()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:source-ownership-missing;" + describe_span(), candidate.contract.ir);
		return summary;
	}
	if (!candidate.contract.OwnsSink()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:sink-ownership-missing;" + describe_span(), candidate.contract.ir);
		return summary;
	}
	if (!ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:requires-full-pipeline-abi;" + describe_span(), candidate.contract.ir);
		return summary;
	}

	candidate.stage_plan = BuildExecutionRegionStagePlan(region_ir, candidate, mode);
	if (!candidate.stage_plan.HasExecutableWork()) {
		summary.blocker = AppendExecutionRegionCandidateDiagnostic(
		    "candidate-builder-blocked:no-executable-work;" + describe_span(), candidate.stage_plan.ir);
		return summary;
	}
	candidate.traits = BuildExecutionRegionCandidateTraits(region_ir, candidate, candidate.stage_plan, mode);
	candidate.signature = BuildExecutionRegionSignature(region_ir, candidate, mode);
	candidate.upstream_traits = BuildExecutionRegionUpstreamTraits(region_ir, candidate, mode);
	candidate.context_traits = BuildExecutionRegionCandidateContextTraits(region_ir, candidate, mode);
	candidate.continuation_traits = BuildExecutionRegionContinuationTraits(region_ir, candidate, mode);
	if (mode == ExecutionRegionIRMode::TRACE) {
		candidate.ir = DescribeExecutionRegionCandidate(candidate);
	}
	return summary;
}

static bool AddExecutionRegionCandidate(
    ExecutionRegionIR &region_ir, idx_t candidate_id, idx_t first_node, idx_t node_count, idx_t start_operator_index,
    idx_t end_operator_index,
    ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE,
    ExecutionRegionIRMode mode = ExecutionRegionIRMode::COMPACT) {
	D_ASSERT(node_count > 0);
	if (HasExecutionRegionCandidate(region_ir, first_node, node_count, start_operator_index, end_operator_index,
	                                source_execution)) {
		return false;
	}
	if (first_node >= region_ir.nodes.size() || region_ir.nodes[first_node].kind != ExecutionRegionNodeKind::SOURCE) {
		AddExecutionRegionCandidateBlocker(
		    region_ir, "candidate-builder-blocked:span-does-not-start-at-source;" +
		                   DescribeExecutionRegionCandidateSpan(first_node, node_count, start_operator_index,
		                                                        end_operator_index, source_execution));
		return false;
	}
	auto summary =
	    BuildExecutionRegionCandidateSummary(region_ir, candidate_id, first_node, node_count, start_operator_index,
	                                         end_operator_index, source_execution, mode);
	if (!summary.Accepted()) {
		AddExecutionRegionCandidateBlocker(region_ir, std::move(summary.blocker));
		return false;
	}
	region_ir.candidates.push_back(std::move(summary.candidate));
	return true;
}

static bool AddExecutionRegionCandidateAndIncrement(ExecutionRegionIR &region_ir, idx_t &candidate_id, idx_t first_node,
                                                    idx_t node_count, idx_t start_operator_index,
                                                    idx_t end_operator_index,
                                                    ExecutionRegionIRMode mode = ExecutionRegionIRMode::COMPACT) {
	if (AddExecutionRegionCandidate(region_ir, candidate_id, first_node, node_count, start_operator_index,
	                                end_operator_index, ExecutionRegionSourceExecutionKind::NONE, mode)) {
		candidate_id++;
		return true;
	}
	return false;
}

static void BuildExecutionRegionCandidates(ExecutionRegionIR &region_ir, idx_t operator_count,
                                           ExecutionRegionIRMode mode) {
	if (region_ir.nodes.empty()) {
		return;
	}
	idx_t candidate_id = 0;
	AddExecutionRegionCandidateAndIncrement(region_ir, candidate_id, 0, region_ir.nodes.size(), 0, operator_count,
	                                        mode);
}

static ExecutionExpressionIRMode ExecutionExpressionModeFromRegionMode(ExecutionRegionIRMode mode) {
	return mode == ExecutionRegionIRMode::TRACE ? ExecutionExpressionIRMode::TRACE : ExecutionExpressionIRMode::COMPACT;
}

static unique_ptr<ExecutionRegionIR> TryBuildExecutionRegion(const ExecutionRegionGraph &descriptor,
                                                             ExecutionRegionIRMode mode,
                                                             ExecutionExpressionAnalysisCache *expression_cache) {
	auto expression_mode = ExecutionExpressionModeFromRegionMode(mode);
	auto result = make_uniq<ExecutionRegionIR>();
	if (descriptor.HasSource()) {
		auto &source = descriptor.source;
		auto source_node = BuildExecutionRegionBoundaryNode("source", source, ExecutionRegionNodeKind::SOURCE,
		                                                    BuildExecutionRegionSourceBoundaryReason(source));
		source_node.compiled_contract = source.source_contract;
		if (source.HasSourceContract()) {
			source_node.source =
			    BuildExecutionRegionSourceInfo(source.source_payload, expression_mode, mode, expression_cache);
			if (source.UsesSourceContract()) {
				source_node.boundary = ExecutionRegionBoundaryKind::SOURCE_CONTRACT;
			}
		} else if (source.IsScanSource()) {
			source_node.source = BuildExecutionRegionGenericScanSourceInfo(source, source_node.blocker_reason, mode);
		} else {
			source_node.source = BuildExecutionRegionStatefulSourceInfo(source, source_node.blocker_reason, mode);
		}
		result->nodes.push_back(std::move(source_node));
	}
	ExecutionRegionDataflowState state;
	for (idx_t op_idx = 0; op_idx < descriptor.operators.size(); op_idx++) {
		result->nodes.push_back(BuildExecutionRegionOperatorNode("op" + std::to_string(op_idx),
		                                                         descriptor.operators[op_idx], state, expression_mode,
		                                                         mode, expression_cache));
	}
	if (descriptor.HasSink()) {
		auto &sink = descriptor.sink;
		auto sink_reason = BuildExecutionRegionSinkBoundaryReason(sink);
		auto sink_node =
		    BuildExecutionRegionBoundaryNode("sink", sink, ExecutionRegionNodeKind::SINK, std::move(sink_reason));
		sink_node.compiled_contract = sink.sink_contract;
		sink_node.sink = BuildExecutionRegionSinkInfo(sink, sink.sink_payload, sink.HasSinkContract(), mode);
		if (sink_node.sink) {
			sink_node.blocker_reason = sink_node.sink->reason;
			if (sink.HasNativeSink()) {
				sink_node.boundary = ExecutionRegionBoundaryKind::SINK_NATIVE;
			}
		}
		result->nodes.push_back(std::move(sink_node));
	}
	if (result->nodes.empty()) {
		return nullptr;
	}
	result->pipeline_shape = DescribeExecutionRegionPipelineShape(*result);
	BuildExecutionRegionCandidates(*result, descriptor.OperatorCount(), mode);
	if (mode == ExecutionRegionIRMode::TRACE) {
		result->ir = "duckdb.region typed-vector-ir";
		for (auto &candidate : result->candidates) {
			result->ir += ";";
			result->ir += candidate.ir;
		}
		for (auto &blocker : result->candidate_blockers) {
			result->ir += ";candidate-blocker:";
			result->ir += blocker;
		}
		for (auto &node : result->nodes) {
			result->ir += ";";
			result->ir += DescribeExecutionRegionNode(node);
		}
	}
	return result;
}

unique_ptr<ExecutionRegionIR> TryLowerExecutionRegion(const ExecutionRegionGraph &descriptor,
                                                      ExecutionRegionIRMode mode,
                                                      ExecutionExpressionAnalysisCache *expression_cache) {
	return TryBuildExecutionRegion(descriptor, mode, expression_cache);
}

} // namespace duckdb

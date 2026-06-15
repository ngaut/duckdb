#include "duckdb/execution/jit/lowering.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

static bool IsJitRegionScanSource(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		return true;
	default:
		return false;
	}
}

static string JitRegionBool(bool value) {
	return value ? "true" : "false";
}

static string BuildJitLogicalTypeList(const vector<LogicalType> &types) {
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

static string BuildJitIdxList(const vector<idx_t> &values) {
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

static string JitRegionHashJoinProbeOutputModeToString(JitRegionHashJoinProbeOutputMode mode) {
	switch (mode) {
	case JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
		return "matched_probe_and_build";
	case JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
		return "matched_probe_only";
	case JitRegionHashJoinProbeOutputMode::MARK_PROBE:
		return "mark_probe";
	case JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
		return "mark_build_only";
	default:
		return "none";
	}
}

static string BuildJitValueList(const vector<Value> &values) {
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

static string BuildJitStringList(const vector<string> &values) {
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

static void AddJitUniqueString(vector<string> &values, string value) {
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

static string BuildJitExpressionTypeList(const vector<ExpressionType> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += ExpressionTypeToString(values[value_idx]);
	}
	result += "]";
	return result;
}

static string DescribeJitRegionSourceFilter(const JitRegionSourceFilter &filter) {
	string result = "filter<index=" + std::to_string(filter.filter_index);
	result += ",scan_column=" + std::to_string(filter.scan_column_index);
	result += ",table_column=" + std::to_string(filter.table_column_index);
	result += ">(";
	if (filter.expression) {
		result += filter.expression->ir;
	} else {
		result += "fallback:" + filter.reason;
	}
	result += ")";
	return result;
}

static void AddJitRegionProtocolField(vector<JitRegionProtocolField> &fields, string name, string value) {
	JitRegionProtocolField field;
	field.name = std::move(name);
	field.value = std::move(value);
	fields.push_back(std::move(field));
}

static vector<JitRegionProtocolField> BuildJitRegionProtocolFields(const string &reason) {
	vector<JitRegionProtocolField> result;
	auto segments = StringUtil::Split(reason, ";");
	if (!segments.empty() && !segments[0].empty()) {
		AddJitRegionProtocolField(result, "marker", segments[0]);
	}
	for (idx_t segment_idx = 1; segment_idx < segments.size(); segment_idx++) {
		auto &segment = segments[segment_idx];
		auto equals = segment.find('=');
		if (equals == string::npos || equals == 0) {
			continue;
		}
		AddJitRegionProtocolField(result, segment.substr(0, equals), segment.substr(equals + 1));
	}
	return result;
}

static string DescribeJitRegionProtocolFields(const vector<JitRegionProtocolField> &fields) {
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

static string DescribeJitRegionTableScanProtocol(const JitRegionTableScanProtocol &protocol) {
	if (!protocol.present) {
		return string();
	}
	string result = "table_scan_protocol<function=" + protocol.function_name;
	result += ",output_columns=" + std::to_string(protocol.output_column_count);
	result += ",returned_columns=" + std::to_string(protocol.returned_column_count);
	result += ",column_ids=" + std::to_string(protocol.column_id_count);
	result += ",projected_columns=" + std::to_string(protocol.projected_column_count);
	result += ",column_id_bindings=" + BuildJitIdxList(protocol.column_ids);
	result += ",projection_ids=" + BuildJitIdxList(protocol.projection_ids);
	result += ",source_prefix_input_columns=" + std::to_string(protocol.source_prefix_input_column_count);
	result += ",source_prefix_input_types=" + BuildJitLogicalTypeList(protocol.source_prefix_input_types);
	result += ",source_prefix_output_projection_map=" + BuildJitIdxList(protocol.source_prefix_output_projection_map);
	result += ",source_prefix_filter_column_map=" + BuildJitIdxList(protocol.source_prefix_filter_column_map);
	result +=
	    ",source_prefix_requires_unfiltered_input=" + JitRegionBool(protocol.source_prefix_requires_unfiltered_input);
	result += ",source_prefix_filter_prune_required=" + JitRegionBool(protocol.source_prefix_filter_prune_required);
	result += ",source_prefix_filter_split_supported=" + JitRegionBool(protocol.source_prefix_filter_split_supported);
	result += ",projection_pushdown=" + JitRegionBool(protocol.projection_pushdown);
	result += ",filter_pushdown=" + JitRegionBool(protocol.filter_pushdown);
	result += ",filter_prune=" + JitRegionBool(protocol.filter_prune);
	result += ",dynamic_filters=" + JitRegionBool(protocol.dynamic_filters);
	result += ",in_out_function=" + JitRegionBool(protocol.in_out_function);
	result += ",filter_count=" + std::to_string(protocol.filter_count);
	result += ">";
	return result;
}

static string DescribeJitRegionNativeOperatorContract(const JitRegionNativeOperatorContract &contract,
                                                      const string &prefix);
static void AppendJitRegionContractIR(string &result, const string &ir);

static string DescribeJitRegionHashJoinProtocol(const JitRegionHashJoinProtocol &protocol) {
	if (!protocol.present) {
		return string();
	}
	string result = "hash_join_protocol<join_type=" + StringUtil::Lower(JoinTypeToString(protocol.join_type));
	result += ",condition_count=" + std::to_string(protocol.condition_count);
	result += ",equality_condition_count=" + std::to_string(protocol.equality_condition_count);
	result += ",non_equality_condition_count=" + std::to_string(protocol.non_equality_condition_count);
	result += ",null_equal_condition_count=" + std::to_string(protocol.null_equal_condition_count);
	result += ",condition_types=" + BuildJitLogicalTypeList(protocol.condition_types);
	result += ",comparison_ops=" + BuildJitExpressionTypeList(protocol.comparison_types);
	result += ",payload_columns=" + std::to_string(protocol.payload_column_count);
	result += ",payload_column_indices=" + BuildJitIdxList(protocol.payload_column_indices);
	result += ",payload_types=" + BuildJitLogicalTypeList(protocol.payload_types);
	result += ",lhs_output_columns=" + std::to_string(protocol.lhs_output_column_count);
	result += ",lhs_output_column_indices=" + BuildJitIdxList(protocol.lhs_output_column_indices);
	result += ",lhs_output_types=" + BuildJitLogicalTypeList(protocol.lhs_output_types);
	result += ",rhs_output_columns=" + std::to_string(protocol.rhs_output_column_count);
	result += ",rhs_output_types=" + BuildJitLogicalTypeList(protocol.rhs_output_types);
	result += ",lhs_probe_columns=" + std::to_string(protocol.lhs_probe_column_count);
	result += ",lhs_probe_column_indices=" + BuildJitIdxList(protocol.lhs_probe_column_indices);
	result += ",lhs_probe_types=" + BuildJitLogicalTypeList(protocol.lhs_probe_types);
	result += ",lhs_output_in_probe=" + std::to_string(protocol.lhs_output_in_probe_count);
	result += ",delim_types=" + std::to_string(protocol.delim_type_count);
	result += ",correlated_mark_counts_required=" + JitRegionBool(protocol.correlated_mark_counts_required);
	result += ",residual_predicate=" + JitRegionBool(protocol.residual_predicate);
	result += ",residual_info=" + JitRegionBool(protocol.residual_info);
	result += ",filter_pushdown=" + JitRegionBool(protocol.filter_pushdown);
	result += ",filter_pushdown_condition_count=" + std::to_string(protocol.filter_pushdown_condition_count);
	result += ",filter_pushdown_probe_count=" + std::to_string(protocol.filter_pushdown_probe_count);
	result += ",build_side_has_filter=" + JitRegionBool(protocol.build_side_has_filter);
	result += ",source_produces_rows=" + JitRegionBool(protocol.source_produces_rows);
	result += ",regular_hash_table_layout_ready=" + JitRegionBool(protocol.regular_hash_table_layout_ready);
	result += ",native_probe_shape_ready=" + JitRegionBool(protocol.native_probe_shape_ready);
	result += ",native_probe_shape_blocker=" + protocol.native_probe_shape_blocker;
	result += ",native_probe_output_mode=" +
	          JitRegionHashJoinProbeOutputModeToString(protocol.native_probe_output_mode);
	result += ",build_append_shape_ready=" + JitRegionBool(protocol.build_append_shape_ready);
	result += ",build_append_shape_blocker=" + protocol.build_append_shape_blocker;
	result += ",hash_join_layout_column_count=" + std::to_string(protocol.layout_column_count);
	result += ",hash_join_layout_offsets=" + BuildJitIdxList(protocol.layout_offsets);
	result += ",hash_join_tuple_size=" + std::to_string(protocol.tuple_size);
	result += ",hash_join_entry_size=" + std::to_string(protocol.entry_size);
	result += ",hash_join_pointer_offset=" + std::to_string(protocol.pointer_offset);
	result += ",hash_join_hash_column_index=" + std::to_string(protocol.hash_column_index);
	result += ",hash_join_found_match_column_present=" + JitRegionBool(protocol.found_match_column_present);
	result += ",hash_join_found_match_column_index=" + std::to_string(protocol.found_match_column_index);
	result += ",hash_join_native_protocol_blocker=" + protocol.native_protocol_blocker;
	AppendJitRegionContractIR(result, protocol.native_probe_contract.ir);
	AppendJitRegionContractIR(result, protocol.native_build_contract.ir);
	result += ">";
	return result;
}

static string DescribeJitRegionNativeGroupedStateContract(const JitRegionNativeGroupedStateContract &contract) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return string();
	}
	string result =
	    "native_grouped_state_contract_status=" + string(JitRegionStateContractStatusToString(contract.status));
	result += ",native_grouped_state_required_capability=" + contract.required_capability;
	result += ",native_grouped_state_protocol=" + contract.protocol_version;
	result += ",native_grouped_state_blocker=" + contract.blocker;
	return result;
}

static string DescribeJitRegionNativeStateScanContract(const JitRegionNativeStateScanContract &contract) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return string();
	}
	string result =
	    "native_state_scan_contract_status=" + string(JitRegionStateContractStatusToString(contract.status));
	result += ",native_state_scan_required_capability=" + contract.required_capability;
	result += ",native_state_scan_protocol=" + contract.protocol_version;
	result += ",native_state_scan_blocker=" + contract.blocker;
	return result;
}

static string DescribeJitRegionNativeOperatorContract(const JitRegionNativeOperatorContract &contract,
                                                      const string &prefix) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return string();
	}
	string result = prefix + "_contract_status=" + string(JitRegionStateContractStatusToString(contract.status));
	result += "," + prefix + "_required_capability=" + contract.required_capability;
	result += "," + prefix + "_protocol=" + contract.protocol_version;
	result += "," + prefix + "_blocker=" + contract.blocker;
	return result;
}

static void AppendJitRegionContractIR(string &result, const string &ir) {
	if (!ir.empty()) {
		result += "," + ir;
	}
}

static string DescribeJitRegionAggregateProtocol(const JitRegionAggregateProtocol &protocol) {
	if (!protocol.present) {
		return string();
	}
	string result = "aggregate_protocol<aggregate_operator_kind=";
	result += JitRegionAggregateOperatorKindToString(protocol.kind);
	result += ",group_count=" + std::to_string(protocol.group_count);
	result += ",group_types=" + BuildJitLogicalTypeList(protocol.group_types);
	result += ",aggregate_count=" + std::to_string(protocol.aggregate_count);
	result += ",aggregate_functions=" + BuildJitStringList(protocol.aggregate_functions);
	result += ",aggregate_return_types=" + BuildJitLogicalTypeList(protocol.aggregate_return_types);
	result += ",aggregate_child_counts=" + BuildJitIdxList(protocol.aggregate_child_counts);
	result += ",aggregate_types=" + BuildJitStringList(protocol.aggregate_types);
	result += ",aggregate_filter_count=" + std::to_string(protocol.aggregate_filter_count);
	result += ",aggregate_order_count=" + std::to_string(protocol.aggregate_order_count);
	result += ",payload_type_count=" + std::to_string(protocol.payload_type_count);
	result += ",payload_types=" + BuildJitLogicalTypeList(protocol.payload_types);
	result += ",grouping_set_count=" + std::to_string(protocol.grouping_set_count);
	result += ",grouping_function_count=" + std::to_string(protocol.grouping_function_count);
	result += ",radix_table_count=" + std::to_string(protocol.radix_table_count);
	result += ",distinct_aggregate_count=" + std::to_string(protocol.distinct_aggregate_count);
	result += ",distinct_table_count=" + std::to_string(protocol.distinct_table_count);
	result += ",distinct_child_count=" + std::to_string(protocol.distinct_child_count);
	result += ",input_group_type_count=" + std::to_string(protocol.input_group_type_count);
	result += ",input_group_types=" + BuildJitLogicalTypeList(protocol.input_group_types);
	result += ",non_distinct_filter_count=" + std::to_string(protocol.non_distinct_filter_count);
	result += ",distinct_filter_count=" + std::to_string(protocol.distinct_filter_count);
	result += ",perfect_required_bits_count=" + std::to_string(protocol.perfect_required_bits_count);
	result += ",perfect_required_bits_total=" + std::to_string(protocol.perfect_required_bits_total);
	result += ",perfect_required_bits=" + BuildJitIdxList(protocol.perfect_required_bits);
	result += ",perfect_group_minima_count=" + std::to_string(protocol.perfect_group_minima_count);
	result += ",perfect_group_minima=" + BuildJitValueList(protocol.perfect_group_minima);
	result += ",grouped_state_layout_ready=" + JitRegionBool(protocol.grouped_state_layout_ready);
	result += ",grouped_state_offsets=" + BuildJitIdxList(protocol.grouped_state_offsets);
	result += ",grouped_state_payload_sizes=" + BuildJitIdxList(protocol.grouped_state_payload_sizes);
	AppendJitRegionContractIR(result, protocol.native_grouped_state_contract.ir);
	AppendJitRegionContractIR(result, protocol.native_hash_lookup_contract.ir);
	result += ">";
	return result;
}

static string DescribeJitRegionAggregateInput(const JitRegionAggregateInput &aggregate) {
	string result = "aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "<function=" + aggregate.function_name;
	result += ",payload_index=" + std::to_string(aggregate.payload_index);
	result += ",child_count=" + std::to_string(aggregate.child_count);
	result += ",child_indices=" + BuildJitIdxList(aggregate.child_indices);
	result += ",child_types=" + BuildJitLogicalTypeList(aggregate.child_types);
	result += ",return_type=" + aggregate.return_type.ToString();
	result += ",distinct=" + JitRegionBool(aggregate.distinct);
	result += ",filter=" + JitRegionBool(aggregate.has_filter);
	result += ",order_bys=" + JitRegionBool(aggregate.has_order_bys);
	result += ",order_dependent=" + JitRegionBool(aggregate.order_dependent);
	result += ",state_update=" + JitRegionBool(aggregate.has_state_update);
	result += ",supported_payload_references=" + JitRegionBool(aggregate.supported_payload_references);
	result += ",native_update=" + string(JitAggregateUpdateKindToString(aggregate.native_update));
	result += ",state_type=" + aggregate.state_type.ToString();
	result += ",state_size=" + std::to_string(aggregate.state_size);
	result += ",state_optional=" + JitRegionBool(aggregate.state_is_optional);
	result += ",state_value_offset=" + std::to_string(aggregate.state_value_offset);
	result += ",state_is_set_offset=" + std::to_string(aggregate.state_is_set_offset);
	if (!aggregate.reason.empty()) {
		result += ",reason=" + aggregate.reason;
	}
	result += ">";
	return result;
}

static string DescribeJitRegionAggregateInputs(const vector<JitRegionAggregateInput> &aggregates) {
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

static string DescribeJitRegionHashJoinKeyInput(const JitRegionHashJoinKeyInput &key) {
	string result = "key";
	result += std::to_string(key.key_index);
	result += "<input_index=" + std::to_string(key.input_index);
	result += ",type=" + key.type.ToString();
	result += ",supported_reference=" + JitRegionBool(key.supported_reference);
	if (!key.reason.empty()) {
		result += ",reason=" + key.reason;
	}
	result += ">";
	return result;
}

static string DescribeJitRegionHashJoinKeyInputs(const vector<JitRegionHashJoinKeyInput> &keys) {
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

static string DescribeJitRegionGroupInput(const JitRegionGroupInput &group) {
	string result = "group";
	result += std::to_string(group.group_index);
	result += "<input_index=" + std::to_string(group.input_index);
	result += ",type=" + group.type.ToString();
	result += ",supported_reference=" + JitRegionBool(group.supported_reference);
	if (!group.reason.empty()) {
		result += ",reason=" + group.reason;
	}
	result += ">";
	return result;
}

static string DescribeJitRegionGroupInputs(const vector<JitRegionGroupInput> &groups) {
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

static string DescribeJitRegionNativeSourceContract(const JitRegionNativeSourceContract &contract) {
	if (contract.status == JitRegionNativeSourceStatus::NONE) {
		return string();
	}
	string result = "native_source_contract<status=" + string(JitRegionNativeSourceStatusToString(contract.status));
	result += ",required_capability=" + contract.required_capability;
	result += ",protocol=" + contract.protocol_version;
	result += ",blocker=" + contract.blocker;
	result += ">";
	return result;
}

JitRegionNativeSourceContract BuildJitRegionNativeSourceContract(JitRegionSourceKind kind,
                                                                 JitRegionSourceExecutionKind execution) {
	JitRegionNativeSourceContract result;
	if (kind == JitRegionSourceKind::NONE) {
		return result;
	}
	result.status = execution == JitRegionSourceExecutionKind::NATIVE_SOURCE ? JitRegionNativeSourceStatus::READY
	                                                                        : JitRegionNativeSourceStatus::BLOCKED;
	result.protocol_version = "v1";
	switch (kind) {
	case JitRegionSourceKind::DUCKDB_TABLE_SCAN:
		result.required_capability = "duckdb-table-scan-native-source";
		result.blocker = execution == JitRegionSourceExecutionKind::NATIVE_SOURCE
		                     ? "none"
		                     : execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
		                           ? "duckdb-table-scan-source-boundary"
		                           : "duckdb-table-scan-executor-fallback-boundary";
		break;
	case JitRegionSourceKind::TABLE_FUNCTION_SCAN:
		result.required_capability = "table-function-native-source";
		result.blocker = execution == JitRegionSourceExecutionKind::NATIVE_SOURCE
		                     ? "none"
		                     : execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
		                           ? "table-function-source-boundary"
		                           : "table-function-executor-fallback-boundary";
		break;
	case JitRegionSourceKind::GENERIC_SCAN:
		result.required_capability = "generic-scan-native-source";
		result.blocker = execution == JitRegionSourceExecutionKind::NATIVE_SOURCE
		                     ? "none"
		                     : execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
		                           ? "generic-scan-source-boundary"
		                           : "generic-scan-executor-fallback-boundary";
		break;
	case JitRegionSourceKind::STATEFUL_OPERATOR:
		result.required_capability = "stateful-operator-native-source";
		result.blocker =
		    execution == JitRegionSourceExecutionKind::NATIVE_SOURCE ? "none" : "stateful-source-protocol-boundary";
		break;
	default:
		break;
	}
	result.ir = DescribeJitRegionNativeSourceContract(result);
	return result;
}

static string JitRegionSourceBoundaryMarker(JitRegionSourceKind kind, JitRegionSourceExecutionKind execution) {
	switch (kind) {
	case JitRegionSourceKind::DUCKDB_TABLE_SCAN:
	case JitRegionSourceKind::TABLE_FUNCTION_SCAN:
	case JitRegionSourceKind::GENERIC_SCAN:
			if (execution == JitRegionSourceExecutionKind::NATIVE_SOURCE) {
				return "DuckDB native table scan source runtime";
			}
			if (execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
				return "DuckDB table scan source boundary;source-fusion-gap:requires-native-source;"
				       "source_execution=duckdb-source-boundary";
			}
		return "DuckDB table scan executor fallback boundary";
	default:
		return string();
	}
}

static vector<JitRegionProtocolField>
BuildJitRegionEffectiveSourceFields(const JitRegionSourceInfo &source, JitRegionSourceExecutionKind execution) {
	auto result = source.fields;
	auto marker = JitRegionSourceBoundaryMarker(source.kind, execution);
	if (marker.empty()) {
		return result;
	}
	for (auto &field : result) {
		if (field.name == "marker") {
			field.value = std::move(marker);
			return result;
		}
	}
	JitRegionProtocolField marker_field;
	marker_field.name = "marker";
	marker_field.value = std::move(marker);
	result.insert(result.begin(), std::move(marker_field));
	return result;
}

string DescribeJitRegionSourceInfo(const JitRegionSourceInfo &source, JitRegionSourceExecutionKind execution) {
	if (execution == JitRegionSourceExecutionKind::NONE) {
		execution = source.execution;
	}
	auto native_source_contract = execution == source.execution ? source.native_source_contract
	                                                           : BuildJitRegionNativeSourceContract(source.kind, execution);
	auto fields = BuildJitRegionEffectiveSourceFields(source, execution);
	string result = "source<kind=" + string(JitRegionSourceKindToString(source.kind));
	result += ",execution=" + string(JitRegionSourceExecutionKindToString(execution));
	if (!native_source_contract.ir.empty()) {
		result += "," + native_source_contract.ir;
	}
	if (!source.native_state_scan_contract.ir.empty()) {
		result += "," + source.native_state_scan_contract.ir;
	}
	result += ",function=" + source.function_name;
	result += ",fields=" + DescribeJitRegionProtocolFields(fields);
	result += ",output_columns=" + std::to_string(source.output_column_count);
	result += ",returned_columns=" + std::to_string(source.returned_column_count);
	result += ",column_ids=" + BuildJitIdxList(source.column_ids);
	result += ",projection_ids=" + BuildJitIdxList(source.projection_ids);
	result += ",projection_pushdown=" + JitRegionBool(source.projection_pushdown);
	result += ",filter_pushdown=" + JitRegionBool(source.filter_pushdown);
	result += ",filter_prune=" + JitRegionBool(source.filter_prune);
	result += ",dynamic_filters=" + JitRegionBool(source.dynamic_filters);
	result += ",in_out_function=" + JitRegionBool(source.in_out_function);
	result += ",filter_count=" + std::to_string(source.filters.size());
	if (!source.table_scan_protocol.ir.empty()) {
		result += "," + source.table_scan_protocol.ir;
	}
	if (!source.hash_join_protocol.ir.empty()) {
		result += "," + source.hash_join_protocol.ir;
	}
	if (!source.aggregate_protocol.ir.empty()) {
		result += "," + source.aggregate_protocol.ir;
	}
	if (!source.aggregates.empty()) {
		result += ",aggregates=" + DescribeJitRegionAggregateInputs(source.aggregates);
	}
	if (!source.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeJitRegionHashJoinKeyInputs(source.hash_join_keys);
	}
	if (!source.groups.empty()) {
		result += ",groups=" + DescribeJitRegionGroupInputs(source.groups);
	}
	result += ">";
	for (auto &filter : source.filters) {
		result += ";";
		result += DescribeJitRegionSourceFilter(filter);
	}
	return result;
}

static string DescribeJitRegionOperatorInfo(const JitRegionOperatorInfo &operator_info) {
	string result = "operator<kind=" + string(JitRegionOperatorKindToString(operator_info.kind));
	result += ",fields=" + DescribeJitRegionProtocolFields(operator_info.fields);
	if (!operator_info.hash_join_protocol.ir.empty()) {
		result += "," + operator_info.hash_join_protocol.ir;
	}
	if (!operator_info.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeJitRegionHashJoinKeyInputs(operator_info.hash_join_keys);
	}
	result += ">";
	return result;
}

static string DescribeJitRegionSinkInfo(const JitRegionSinkInfo &sink) {
	string result = "sink<kind=" + string(JitRegionSinkKindToString(sink.kind));
	result += ",fields=" + DescribeJitRegionProtocolFields(sink.fields);
	if (!sink.hash_join_protocol.ir.empty()) {
		result += "," + sink.hash_join_protocol.ir;
	}
	if (!sink.aggregate_protocol.ir.empty()) {
		result += "," + sink.aggregate_protocol.ir;
	}
	if (!sink.aggregates.empty()) {
		result += ",aggregates=" + DescribeJitRegionAggregateInputs(sink.aggregates);
	}
	if (!sink.hash_join_keys.empty()) {
		result += ",hash_join_keys=" + DescribeJitRegionHashJoinKeyInputs(sink.hash_join_keys);
	}
	if (!sink.groups.empty()) {
		result += ",groups=" + DescribeJitRegionGroupInputs(sink.groups);
	}
	result += ">";
	return result;
}

static void AddJitRegionSourceFilters(const JitOperatorSourceDescriptor &descriptor, JitRegionSourceInfo &source) {
	for (auto &entry : descriptor.filters) {
		JitRegionSourceFilter filter;
		filter.filter_index = entry.filter_index;
		filter.scan_column_index = entry.scan_column_index;
		filter.table_column_index = entry.table_column_index;
		if (!entry.expression) {
			filter.reason = entry.reason.empty() ? "source filter has no expression descriptor" : entry.reason;
			source.filters.push_back(std::move(filter));
			continue;
		}
		filter.expression = TryLowerJitExpression(*entry.expression, filter.filter_index);
		if (!filter.expression) {
			filter.reason = DescribeJitExpressionLoweringFailure(*entry.expression);
		}
		source.filters.push_back(std::move(filter));
	}
}

static unique_ptr<JitRegionSourceInfo> BuildJitRegionSourceInfo(const JitOperatorSourceDescriptor &descriptor) {
	auto result = make_uniq<JitRegionSourceInfo>();
	result->kind = descriptor.kind;
	result->execution = descriptor.execution;
	result->function_name = descriptor.function_name;
	result->fields = descriptor.fields;
	result->output_column_count = descriptor.output_column_count;
	result->returned_column_count = descriptor.returned_column_count;
	result->column_ids = descriptor.column_ids;
	result->projection_ids = descriptor.projection_ids;
	result->projection_pushdown = descriptor.projection_pushdown;
	result->filter_pushdown = descriptor.filter_pushdown;
	result->filter_prune = descriptor.filter_prune;
	result->dynamic_filters = descriptor.dynamic_filters;
	result->in_out_function = descriptor.in_out_function;
	result->table_scan_protocol = descriptor.table_scan_protocol;
	result->hash_join_protocol = descriptor.hash_join_protocol;
	result->aggregate_protocol = descriptor.aggregate_protocol;
	result->aggregates = descriptor.aggregates;
	result->hash_join_keys = descriptor.hash_join_keys;
	result->groups = descriptor.groups;
	result->native_source_contract = descriptor.native_source_contract;
	result->native_state_scan_contract = descriptor.native_state_scan_contract;
	result->reason = descriptor.reason;
	AddJitRegionSourceFilters(descriptor, *result);
	for (auto &aggregate : result->aggregates) {
		aggregate.ir = DescribeJitRegionAggregateInput(aggregate);
	}
	for (auto &key : result->hash_join_keys) {
		key.ir = DescribeJitRegionHashJoinKeyInput(key);
	}
	for (auto &group : result->groups) {
		group.ir = DescribeJitRegionGroupInput(group);
	}
	result->table_scan_protocol.ir = DescribeJitRegionTableScanProtocol(result->table_scan_protocol);
	result->hash_join_protocol.native_probe_contract.ir =
	    DescribeJitRegionNativeOperatorContract(result->hash_join_protocol.native_probe_contract,
	                                            "native_hash_join_probe");
	result->hash_join_protocol.native_build_contract.ir =
	    DescribeJitRegionNativeOperatorContract(result->hash_join_protocol.native_build_contract,
	                                            "native_hash_join_build");
	result->hash_join_protocol.ir = DescribeJitRegionHashJoinProtocol(result->hash_join_protocol);
	result->aggregate_protocol.native_grouped_state_contract.ir =
	    DescribeJitRegionNativeGroupedStateContract(result->aggregate_protocol.native_grouped_state_contract);
	result->aggregate_protocol.native_hash_lookup_contract.ir =
	    DescribeJitRegionNativeOperatorContract(result->aggregate_protocol.native_hash_lookup_contract,
	                                            "native_hash_aggregate_lookup");
	result->aggregate_protocol.ir = DescribeJitRegionAggregateProtocol(result->aggregate_protocol);
	result->native_source_contract.ir = DescribeJitRegionNativeSourceContract(result->native_source_contract);
	result->native_state_scan_contract.ir =
	    DescribeJitRegionNativeStateScanContract(result->native_state_scan_contract);
	result->ir = DescribeJitRegionSourceInfo(*result);
	return result;
}

static unique_ptr<JitRegionOperatorInfo> BuildJitRegionOperatorInfo(const JitRegionOperatorInfo &descriptor) {
	auto result = make_uniq<JitRegionOperatorInfo>(descriptor);
	for (auto &key : result->hash_join_keys) {
		key.ir = DescribeJitRegionHashJoinKeyInput(key);
	}
	result->hash_join_protocol.native_probe_contract.ir =
	    DescribeJitRegionNativeOperatorContract(result->hash_join_protocol.native_probe_contract,
	                                            "native_hash_join_probe");
	result->hash_join_protocol.native_build_contract.ir =
	    DescribeJitRegionNativeOperatorContract(result->hash_join_protocol.native_build_contract,
	                                            "native_hash_join_build");
	result->hash_join_protocol.ir = DescribeJitRegionHashJoinProtocol(result->hash_join_protocol);
	result->ir = DescribeJitRegionOperatorInfo(*result);
	return result;
}

static unique_ptr<JitRegionSourceInfo> BuildJitRegionGenericScanSourceInfo(const PhysicalOperator &op,
                                                                           const string &reason) {
	auto result = make_uniq<JitRegionSourceInfo>();
	result->kind = JitRegionSourceKind::GENERIC_SCAN;
	result->execution = JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result->function_name = StringUtil::Lower(PhysicalOperatorToString(op.type));
	result->fields = BuildJitRegionProtocolFields(reason);
	result->output_column_count = op.GetTypes().size();
	result->returned_column_count = op.GetTypes().size();
	result->native_source_contract.status = JitRegionNativeSourceStatus::BLOCKED;
	result->native_source_contract.required_capability = "generic-scan-native-source";
	result->native_source_contract.protocol_version = "v1";
	result->native_source_contract.blocker = "generic-scan-source-boundary";
	result->native_source_contract.ir = DescribeJitRegionNativeSourceContract(result->native_source_contract);
	result->reason = reason;
	result->ir = DescribeJitRegionSourceInfo(*result);
	return result;
}

static unique_ptr<JitRegionSourceInfo> BuildJitRegionStatefulSourceInfo(const PhysicalOperator &op,
                                                                        const string &reason) {
	auto result = make_uniq<JitRegionSourceInfo>();
	result->kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result->execution = JitRegionSourceExecutionKind::EXECUTOR_FALLBACK;
	result->function_name = StringUtil::Lower(PhysicalOperatorToString(op.type));
	result->fields = BuildJitRegionProtocolFields(reason);
	result->output_column_count = op.GetTypes().size();
	result->returned_column_count = op.GetTypes().size();
	result->native_source_contract.status = JitRegionNativeSourceStatus::BLOCKED;
	result->native_source_contract.required_capability = "stateful-operator-native-source";
	result->native_source_contract.protocol_version = "v1";
	result->native_source_contract.blocker = "stateful-source-protocol-boundary";
	result->native_source_contract.ir = DescribeJitRegionNativeSourceContract(result->native_source_contract);
	result->reason = reason;
	result->ir = DescribeJitRegionSourceInfo(*result);
	return result;
}

static string BuildJitGenericSinkProtocolReason(const PhysicalOperator &op) {
	auto operator_name = PhysicalOperatorToString(op.type);
	if (operator_name == "ORDER_BY" || operator_name == "TOP_N") {
		return "DuckDB sort sink protocol missing;operator=" + operator_name;
	}
	if (operator_name == "CTE" || operator_name == "RESULT_COLLECTOR" || operator_name == "EXPLAIN_ANALYZE") {
		return "DuckDB materialization sink protocol missing;operator=" + operator_name;
	}
	return "DuckDB sink operator boundary;operator=" + operator_name;
}

static JitRegionSinkKind GetJitGenericSinkKind(const PhysicalOperator &op) {
	auto operator_name = PhysicalOperatorToString(op.type);
	if (operator_name == "ORDER_BY" || operator_name == "TOP_N") {
		return JitRegionSinkKind::SORT;
	}
	if (operator_name == "CTE" || operator_name == "RESULT_COLLECTOR" || operator_name == "EXPLAIN_ANALYZE") {
		return JitRegionSinkKind::MATERIALIZATION;
	}
	return JitRegionSinkKind::OPERATOR;
}

static void FinalizeJitRegionSinkInfo(JitRegionSinkInfo &sink) {
	for (auto &aggregate : sink.aggregates) {
		aggregate.ir = DescribeJitRegionAggregateInput(aggregate);
	}
	for (auto &key : sink.hash_join_keys) {
		key.ir = DescribeJitRegionHashJoinKeyInput(key);
	}
	for (auto &group : sink.groups) {
		group.ir = DescribeJitRegionGroupInput(group);
	}
	sink.hash_join_protocol.native_probe_contract.ir =
	    DescribeJitRegionNativeOperatorContract(sink.hash_join_protocol.native_probe_contract,
	                                            "native_hash_join_probe");
	sink.hash_join_protocol.native_build_contract.ir =
	    DescribeJitRegionNativeOperatorContract(sink.hash_join_protocol.native_build_contract,
	                                            "native_hash_join_build");
	sink.hash_join_protocol.ir = DescribeJitRegionHashJoinProtocol(sink.hash_join_protocol);
	sink.aggregate_protocol.native_grouped_state_contract.ir =
	    DescribeJitRegionNativeGroupedStateContract(sink.aggregate_protocol.native_grouped_state_contract);
	sink.aggregate_protocol.native_hash_lookup_contract.ir =
	    DescribeJitRegionNativeOperatorContract(sink.aggregate_protocol.native_hash_lookup_contract,
	                                            "native_hash_aggregate_lookup");
	sink.aggregate_protocol.ir = DescribeJitRegionAggregateProtocol(sink.aggregate_protocol);
	sink.ir = DescribeJitRegionSinkInfo(sink);
}

static unique_ptr<JitRegionSinkInfo> BuildJitRegionSinkInfo(const PhysicalOperator &op,
                                                            const JitRegionSinkInfo &sink_payload,
                                                            bool has_sink_contract) {
	auto result = has_sink_contract ? make_uniq<JitRegionSinkInfo>(sink_payload) : make_uniq<JitRegionSinkInfo>();
	if (!has_sink_contract) {
		result->kind = GetJitGenericSinkKind(op);
		result->reason = BuildJitGenericSinkProtocolReason(op);
		result->fields = BuildJitRegionProtocolFields(result->reason);
	}
	FinalizeJitRegionSinkInfo(*result);
	return result;
}

static string BuildJitSourceBoundaryReason(const JitPipelineOperatorEntry &entry) {
	if (!entry.source_boundary_reason.empty()) {
		return entry.source_boundary_reason;
	}
	if (IsJitRegionScanSource(entry.type)) {
		return "DuckDB scan source boundary;operator=" + entry.operator_name;
	}
	return "DuckDB stateful source operator fallback boundary;operator=" + entry.operator_name;
}

static string BuildJitSinkBoundaryReason(const JitPipelineOperatorEntry &entry) {
	if (entry.HasSinkContract() && !entry.sink_payload.reason.empty()) {
		return entry.sink_payload.reason;
	}
	return BuildJitGenericSinkProtocolReason(entry.Physical());
}

static JitRegionIRNode BuildJitRegionFallbackNode(string role, const JitPipelineOperatorEntry &entry,
                                                  JitRegionIRNodeKind kind, string fallback_reason) {
	JitRegionIRNode node;
	node.role = std::move(role);
	node.operator_name = entry.operator_name;
	node.operator_index = entry.operator_index;
	node.kind = kind;
	node.output_types = entry.output_types;
	node.estimated_cardinality = entry.estimated_cardinality;
	node.input_format = JitRegionVectorFormatKind::EXECUTOR_BOUNDARY;
	node.output_format = JitRegionVectorFormatKind::EXECUTOR_BOUNDARY;
	node.vector_source = JitRegionVectorSourceKind::EXECUTOR_BOUNDARY;
	node.selection_source = JitRegionSelectionSourceKind::EXECUTOR_BOUNDARY;
	switch (kind) {
	case JitRegionIRNodeKind::SOURCE:
		node.boundary =
		    IsJitRegionScanSource(entry.type) ? JitRegionBoundaryKind::SCAN : JitRegionBoundaryKind::OPERATOR_FALLBACK;
		break;
	case JitRegionIRNodeKind::SINK:
		node.boundary = JitRegionBoundaryKind::SINK;
		break;
	default:
		node.boundary = JitRegionBoundaryKind::OPERATOR_FALLBACK;
		break;
	}
	node.fallback_reason = std::move(fallback_reason);
	return node;
}

struct JitRegionDataflowState {
	JitRegionVectorSourceKind vector_source = JitRegionVectorSourceKind::REGION_INPUT;
	JitRegionSelectionSourceKind selection_source = JitRegionSelectionSourceKind::INPUT_SELECTION;
};

static void SetJitRegionInputDataflow(JitRegionIRNode &node, const JitRegionDataflowState &state) {
	node.vector_source = state.vector_source;
	node.selection_source = state.selection_source;
}

static void SetJitRegionExecutorBoundaryDataflow(JitRegionDataflowState &state) {
	state.vector_source = JitRegionVectorSourceKind::EXECUTOR_BOUNDARY;
	state.selection_source = JitRegionSelectionSourceKind::EXECUTOR_BOUNDARY;
}

static JitRegionIRNode BuildJitRegionOperatorNode(string role, const JitPipelineOperatorEntry &entry,
                                                  JitRegionDataflowState &state) {
	auto &op = entry.Physical();
	switch (entry.type) {
	case PhysicalOperatorType::FILTER: {
		auto &filter = op.Cast<PhysicalFilter>();
		JitRegionIRNode node;
		node.role = std::move(role);
		node.operator_name = entry.operator_name;
		node.operator_index = entry.operator_index;
		node.kind = JitRegionIRNodeKind::FILTER;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = JitRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = JitRegionVectorFormatKind::SELECTION_VECTOR;
		SetJitRegionInputDataflow(node, state);
		node.filter = TryLowerJitExpression(*filter.expression);
		if (!node.filter) {
			node.boundary = JitRegionBoundaryKind::EXPRESSION_FALLBACK;
			node.fallback_reason = "core filter expression lowering unsupported;" +
			                       DescribeJitExpressionLoweringFailure(*filter.expression);
			SetJitRegionExecutorBoundaryDataflow(state);
			return node;
		}
		state.vector_source = JitRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = JitRegionSelectionSourceKind::FILTER_SELECTION;
		return node;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = op.Cast<PhysicalProjection>();
		JitRegionIRNode node;
		node.role = std::move(role);
		node.operator_name = entry.operator_name;
		node.operator_index = entry.operator_index;
		node.kind = JitRegionIRNodeKind::PROJECTION;
		node.output_types = entry.output_types;
		node.estimated_cardinality = entry.estimated_cardinality;
		node.input_format = JitRegionVectorFormatKind::UNIFIED_VECTOR;
		node.output_format = JitRegionVectorFormatKind::FLAT_VECTOR;
		SetJitRegionInputDataflow(node, state);
		for (idx_t expr_idx = 0; expr_idx < projection.select_list.size(); expr_idx++) {
			auto fragment = TryLowerJitExpression(*projection.select_list[expr_idx], expr_idx);
			if (!fragment) {
				node.projections.clear();
				node.boundary = JitRegionBoundaryKind::EXPRESSION_FALLBACK;
				node.fallback_reason =
				    "core projection expression lowering unsupported;expression_index=" + std::to_string(expr_idx) +
				    ";" + DescribeJitExpressionLoweringFailure(*projection.select_list[expr_idx]);
				SetJitRegionExecutorBoundaryDataflow(state);
				return node;
			}
			node.projections.push_back(std::move(fragment));
		}
		state.vector_source = JitRegionVectorSourceKind::OPERATOR_OUTPUT;
		state.selection_source = JitRegionSelectionSourceKind::NONE;
		return node;
	}
	default: {
		if (entry.HasOperatorContract()) {
			JitRegionIRNode node;
			node.role = std::move(role);
			node.operator_name = entry.operator_name;
			node.operator_index = entry.operator_index;
			node.compiled_contract = entry.operator_contract;
			node.kind = JitRegionIRNodeKind::OPERATOR;
			node.output_types = entry.output_types;
			node.estimated_cardinality = entry.estimated_cardinality;
			node.input_format = JitRegionVectorFormatKind::DATA_CHUNK;
			node.output_format = JitRegionVectorFormatKind::DATA_CHUNK;
			SetJitRegionInputDataflow(node, state);
			node.operator_info = BuildJitRegionOperatorInfo(entry.operator_payload);
			if (entry.native_operator) {
				node.boundary = JitRegionBoundaryKind::OPERATOR_NATIVE;
			} else {
				node.boundary = JitRegionBoundaryKind::OPERATOR_HELPER;
			}
			state.vector_source = JitRegionVectorSourceKind::OPERATOR_OUTPUT;
			state.selection_source = JitRegionSelectionSourceKind::NONE;
			return node;
		}
		auto node = BuildJitRegionFallbackNode(std::move(role), entry, JitRegionIRNodeKind::OPERATOR,
		                                       "DuckDB physical operator outside generated JIT region");
		SetJitRegionExecutorBoundaryDataflow(state);
		return node;
	}
	}
}

static void AddJitRegionInventoryFeature(vector<string> &features, string feature) {
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

static string BuildJitRegionInventoryFeatureShape(const JitRegionPipelineInventory &inventory) {
	vector<string> features;
	if (inventory.has_table_scan_source) {
		AddJitRegionInventoryFeature(features, "table-scan-source");
	} else if (!inventory.source_operator_name.empty()) {
		AddJitRegionInventoryFeature(features, StringUtil::Lower(inventory.source_operator_name) + "-source");
	}
	if (inventory.has_hash_join_operator) {
		AddJitRegionInventoryFeature(features, "hash-join-operator");
	}
	if (inventory.has_hash_join_sink) {
		AddJitRegionInventoryFeature(features, "hash-join-build");
	}
	if (inventory.has_hash_aggregate_sink) {
		AddJitRegionInventoryFeature(features, "hash-aggregate-update");
	}
	if (inventory.has_perfect_hash_aggregate_sink) {
		AddJitRegionInventoryFeature(features, "perfect-hash-aggregate-update");
	}
	if (inventory.has_ungrouped_aggregate_sink) {
		AddJitRegionInventoryFeature(features, "ungrouped-aggregate-update");
	}
	string result;
	for (idx_t feature_idx = 0; feature_idx < features.size(); feature_idx++) {
		if (feature_idx > 0) {
			result += "+";
		}
		result += features[feature_idx];
	}
	return result;
}

static string DescribeJitRegionInventoryPipelineShape(const JitRegionPipelineInventory &inventory) {
	string result = "pipeline";
	if (inventory.has_source) {
		result += ";source:source:" + inventory.source_operator_name + ":";
		result +=
		    inventory.source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE ? "source-native" : "operator-fallback";
	}
	for (idx_t op_idx = 0; op_idx < inventory.operator_names.size(); op_idx++) {
		auto &operator_name = inventory.operator_names[op_idx];
		result += ";op" + std::to_string(op_idx) + ":";
		if (operator_name == "FILTER") {
			result += "filter:FILTER:none";
		} else if (operator_name == "PROJECTION") {
			result += "projection:PROJECTION:none";
		} else {
			result += "operator:" + operator_name + ":operator-fallback";
		}
	}
	if (inventory.has_sink) {
		result += ";sink:sink:" + inventory.sink_operator_name + ":sink";
	}
	return result;
}

static bool IsJitRegionPipelineWrapperOnlySource(const string &operator_name) {
	return operator_name == "CREATE_TABLE_AS" || operator_name == "RESULT_COLLECTOR" ||
	       operator_name == "EXPLAIN_ANALYZE";
}

static bool IsJitRegionPipelineInventoryWorkloadRelevant(const JitRegionPipelineInventory &inventory) {
	if (inventory.has_source && inventory.source_operator_name == "HASH_JOIN" && !inventory.source_produces_rows) {
		return false;
	}
	if (inventory.has_scan_source || inventory.has_table_scan_source || inventory.has_hash_join_operator ||
	    inventory.has_hash_join_sink || inventory.has_hash_aggregate_sink ||
	    inventory.has_perfect_hash_aggregate_sink || inventory.has_ungrouped_aggregate_sink ||
	    inventory.has_filter_operator || inventory.has_projection_operator) {
		return true;
	}
	if (inventory.has_stateful_source && !IsJitRegionPipelineWrapperOnlySource(inventory.source_operator_name)) {
		return true;
	}
	return false;
}

static void AccumulateJitRegionInventorySource(JitRegionPipelineInventory &inventory,
                                               const JitPipelineOperatorEntry &source) {
	inventory.has_source = true;
	inventory.source_operator_name = source.operator_name;
	inventory.estimated_cardinality = MaxValue(inventory.estimated_cardinality, source.estimated_cardinality);
	if (source.HasSourceContract()) {
		auto &source_payload = source.source_payload;
		inventory.source_kind = source_payload.kind;
		inventory.source_execution = source_payload.execution;
		inventory.has_scan_source = IsJitRegionScanSource(source.type);
		inventory.has_table_scan_source = source_payload.kind == JitRegionSourceKind::DUCKDB_TABLE_SCAN ||
		                                  inventory.source_operator_name == "TABLE_SCAN";
		inventory.has_stateful_source = source_payload.kind == JitRegionSourceKind::STATEFUL_OPERATOR;
		if (source_payload.hash_join_protocol.present) {
			inventory.source_produces_rows = source_payload.hash_join_protocol.source_produces_rows;
		}
		inventory.source_filter_count = source_payload.filters.size();
		inventory.source_projected_column_count = source_payload.projection_ids.size();
		inventory.source_returned_column_count = source_payload.returned_column_count;
		return;
	}
	if (IsJitRegionScanSource(source.type)) {
		inventory.source_kind = JitRegionSourceKind::GENERIC_SCAN;
		inventory.source_execution = JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
		inventory.has_scan_source = true;
		return;
	}
	inventory.source_kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	inventory.source_execution = JitRegionSourceExecutionKind::NONE;
	inventory.has_stateful_source = true;
}

static void AccumulateJitRegionInventoryOperator(JitRegionPipelineInventory &inventory,
                                                 const JitPipelineOperatorEntry &op) {
	inventory.operator_count++;
	inventory.operator_names.push_back(op.operator_name);
	inventory.estimated_cardinality = MaxValue(inventory.estimated_cardinality, op.estimated_cardinality);
	switch (op.type) {
	case PhysicalOperatorType::FILTER:
		inventory.filter_operator_count++;
		inventory.has_filter_operator = true;
		break;
	case PhysicalOperatorType::PROJECTION:
		inventory.projection_operator_count++;
		inventory.has_projection_operator = true;
		break;
	case PhysicalOperatorType::HASH_JOIN:
		inventory.has_hash_join_operator = true;
		break;
	default:
		break;
	}
}

static void AccumulateJitRegionInventorySink(JitRegionPipelineInventory &inventory,
                                             const JitPipelineOperatorEntry &sink) {
	inventory.has_sink = true;
	inventory.sink_operator_name = sink.operator_name;
	inventory.estimated_cardinality = MaxValue(inventory.estimated_cardinality, sink.estimated_cardinality);
	if (sink.HasSinkContract()) {
		inventory.sink_kind = sink.sink_payload.kind;
	} else {
		inventory.sink_kind = JitRegionSinkKind::OPERATOR;
	}
	inventory.has_hash_join_sink = inventory.sink_kind == JitRegionSinkKind::HASH_JOIN_BUILD;
	inventory.has_hash_aggregate_sink = inventory.sink_kind == JitRegionSinkKind::HASH_AGGREGATE_UPDATE;
	inventory.has_perfect_hash_aggregate_sink = inventory.sink_kind == JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
	inventory.has_ungrouped_aggregate_sink = inventory.sink_kind == JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
}

unique_ptr<JitRegionPipelineInventory> TryInspectJitRegionPipeline(const JitPipelineDescriptor &descriptor,
                                                                   JitRegionPipelineInventoryMode mode) {
	auto result = make_uniq<JitRegionPipelineInventory>();
	if (descriptor.HasSource()) {
		AccumulateJitRegionInventorySource(*result, descriptor.source);
	}
	for (auto &op : descriptor.operators) {
		AccumulateJitRegionInventoryOperator(*result, op);
	}
	if (descriptor.HasSink()) {
		AccumulateJitRegionInventorySink(*result, descriptor.sink);
	}
	if (!result->has_source && result->operator_count == 0 && !result->has_sink) {
		return nullptr;
	}
	if (!IsJitRegionPipelineInventoryWorkloadRelevant(*result)) {
		return nullptr;
	}
	if (mode == JitRegionPipelineInventoryMode::DIAGNOSTIC) {
		result->feature_shape = BuildJitRegionInventoryFeatureShape(*result);
		result->pipeline_shape = DescribeJitRegionInventoryPipelineShape(*result);
		result->ir = "duckdb.region admission-inventory";
		result->ir += ";pipeline_shape=" + result->pipeline_shape;
		result->ir += ";features=" + result->feature_shape;
		result->ir += ";source_filters=" + std::to_string(result->source_filter_count);
		result->ir += ";source_projected_columns=" + std::to_string(result->source_projected_column_count);
		result->ir += ";source_produces_rows=" + string(result->source_produces_rows ? "true" : "false");
		result->ir += ";estimated_cardinality=" + std::to_string(result->estimated_cardinality);
	}
	return result;
}

static string DescribeJitTypeList(const vector<LogicalType> &types) {
	string result = "[";
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += JitTypeDescriptor(types[type_idx]);
	}
	result += "]";
	return result;
}

static string DescribeJitRegionIRNodeHeader(const JitRegionIRNode &node) {
	string result = node.role + ":" + string(JitRegionIRNodeKindToString(node.kind)) + ":" + node.operator_name;
	result += "<outputs=" + DescribeJitTypeList(node.output_types);
	result += ",input_format=" + string(JitRegionVectorFormatKindToString(node.input_format));
	result += ",output_format=" + string(JitRegionVectorFormatKindToString(node.output_format));
	result += ",vector_source=" + string(JitRegionVectorSourceKindToString(node.vector_source));
	result += ",selection_source=" + string(JitRegionSelectionSourceKindToString(node.selection_source));
	result += ",boundary=" + string(JitRegionBoundaryKindToString(node.boundary));
	result += ",estimated_cardinality=" + std::to_string(node.estimated_cardinality);
	if (node.compiled_contract.present) {
		result += ",compiled_contract=" + node.compiled_contract.ir;
	}
	result += ">";
	return result;
}

static string DescribeJitRegionIRNode(const JitRegionIRNode &node) {
	string result = DescribeJitRegionIRNodeHeader(node);
	switch (node.kind) {
	case JitRegionIRNodeKind::FILTER:
		result += "(";
		result += node.filter ? node.filter->ir : "fallback:" + node.fallback_reason;
		result += ")";
		break;
	case JitRegionIRNodeKind::PROJECTION:
		result += "(";
		for (idx_t expr_idx = 0; expr_idx < node.projections.size(); expr_idx++) {
			if (expr_idx > 0) {
				result += ",";
			}
			result += node.projections[expr_idx]->ir;
		}
		if (node.projections.empty() && !node.fallback_reason.empty()) {
			result += "fallback:" + node.fallback_reason;
		}
		result += ")";
		break;
	default:
		if (node.sink) {
			result += "(" + node.sink->ir;
			if (!node.fallback_reason.empty()) {
				result += ";fallback:" + node.fallback_reason;
			}
			result += ")";
			break;
		}
		if (node.source) {
			result += "(" + node.source->ir;
			if (!node.fallback_reason.empty()) {
				result += ";fallback:" + node.fallback_reason;
			}
			result += ")";
			break;
		}
		if (node.operator_info) {
			result += "(" + node.operator_info->ir;
			if (!node.fallback_reason.empty()) {
				result += ";fallback:" + node.fallback_reason;
			}
			result += ")";
			break;
		}
		if (!node.fallback_reason.empty()) {
			result += "(" + node.fallback_reason + ")";
		}
		break;
	}
	return result;
}

static string DescribeJitRegionPipelineShape(const JitRegionIR &region_ir) {
	string result = "pipeline";
	for (auto &node : region_ir.nodes) {
		result += ";";
		result += node.role;
		result += ":";
		result += StringUtil::Lower(string(JitRegionIRNodeKindToString(node.kind)));
		result += ":";
		result += node.operator_name;
		result += ":";
		result += StringUtil::Lower(string(JitRegionBoundaryKindToString(node.boundary)));
	}
	return result;
}

static string DescribeJitRegionPipelineShape(const JitRegionIR &region_ir, idx_t first_node, idx_t node_count) {
	string result = "pipeline";
	auto end_node = first_node + node_count;
	for (idx_t node_idx = first_node; node_idx < end_node && node_idx < region_ir.nodes.size(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		result += ";";
		result += node.role;
		result += ":";
		result += StringUtil::Lower(string(JitRegionIRNodeKindToString(node.kind)));
		result += ":";
		result += node.operator_name;
		result += ":";
		result += StringUtil::Lower(string(JitRegionBoundaryKindToString(node.boundary)));
	}
	return result;
}

static bool IsJitRegionArithmeticBinaryOp(JitExpressionBinaryOp op) {
	switch (op) {
	case JitExpressionBinaryOp::ADD:
	case JitExpressionBinaryOp::SUBTRACT:
	case JitExpressionBinaryOp::MULTIPLY:
	case JitExpressionBinaryOp::INTEGER_DIVIDE:
	case JitExpressionBinaryOp::MODULO:
		return true;
	default:
		return false;
	}
}

static bool IsJitRegionComparisonBinaryOp(JitExpressionBinaryOp op) {
	switch (op) {
	case JitExpressionBinaryOp::COMPARE_EQUAL:
	case JitExpressionBinaryOp::COMPARE_NOTEQUAL:
	case JitExpressionBinaryOp::COMPARE_LESSTHAN:
	case JitExpressionBinaryOp::COMPARE_GREATERTHAN:
	case JitExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case JitExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
	case JitExpressionBinaryOp::COMPARE_DISTINCT_FROM:
	case JitExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

static bool IsJitRegionIntegerLogicalType(const LogicalType &type) {
	return type.IsIntegral();
}

static bool JitExpressionContainsBinaryOp(const JitExpressionIR &expr, bool (*op_matches)(JitExpressionBinaryOp)) {
	if (expr.kind == JitExpressionIRKind::BINARY && op_matches(expr.binary_op)) {
		return true;
	}
	if (expr.left && JitExpressionContainsBinaryOp(*expr.left, op_matches)) {
		return true;
	}
	if (expr.right && JitExpressionContainsBinaryOp(*expr.right, op_matches)) {
		return true;
	}
	if (expr.else_node && JitExpressionContainsBinaryOp(*expr.else_node, op_matches)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsBinaryOp(*child, op_matches)) {
			return true;
		}
	}
	return false;
}

static bool JitExpressionContainsBinaryOpWithType(const JitExpressionIR &expr,
                                                  bool (*op_matches)(JitExpressionBinaryOp),
                                                  bool (*type_matches)(const LogicalType &)) {
	if (expr.kind == JitExpressionIRKind::BINARY && op_matches(expr.binary_op) && type_matches(expr.return_type)) {
		return true;
	}
	if (expr.left && JitExpressionContainsBinaryOpWithType(*expr.left, op_matches, type_matches)) {
		return true;
	}
	if (expr.right && JitExpressionContainsBinaryOpWithType(*expr.right, op_matches, type_matches)) {
		return true;
	}
	if (expr.else_node && JitExpressionContainsBinaryOpWithType(*expr.else_node, op_matches, type_matches)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsBinaryOpWithType(*child, op_matches, type_matches)) {
			return true;
		}
	}
	return false;
}

static bool JitExpressionContainsBinaryOpWithOtherType(const JitExpressionIR &expr,
                                                       bool (*op_matches)(JitExpressionBinaryOp),
                                                       bool (*type_matches)(const LogicalType &)) {
	if (expr.kind == JitExpressionIRKind::BINARY && op_matches(expr.binary_op) && !type_matches(expr.return_type)) {
		return true;
	}
	if (expr.left && JitExpressionContainsBinaryOpWithOtherType(*expr.left, op_matches, type_matches)) {
		return true;
	}
	if (expr.right && JitExpressionContainsBinaryOpWithOtherType(*expr.right, op_matches, type_matches)) {
		return true;
	}
	if (expr.else_node && JitExpressionContainsBinaryOpWithOtherType(*expr.else_node, op_matches, type_matches)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsBinaryOpWithOtherType(*child, op_matches, type_matches)) {
			return true;
		}
	}
	return false;
}

static bool JitExpressionBinaryOperandsMatchType(const JitExpressionIR &expr,
                                                 bool (*type_matches)(const LogicalType &)) {
	return expr.left && expr.right && type_matches(expr.left->return_type) && type_matches(expr.right->return_type);
}

static bool JitExpressionContainsBinaryOpWithOperandType(const JitExpressionIR &expr,
                                                         bool (*op_matches)(JitExpressionBinaryOp),
                                                         bool (*type_matches)(const LogicalType &)) {
	if (expr.kind == JitExpressionIRKind::BINARY && op_matches(expr.binary_op) &&
	    JitExpressionBinaryOperandsMatchType(expr, type_matches)) {
		return true;
	}
	if (expr.left && JitExpressionContainsBinaryOpWithOperandType(*expr.left, op_matches, type_matches)) {
		return true;
	}
	if (expr.right && JitExpressionContainsBinaryOpWithOperandType(*expr.right, op_matches, type_matches)) {
		return true;
	}
	if (expr.else_node && JitExpressionContainsBinaryOpWithOperandType(*expr.else_node, op_matches, type_matches)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsBinaryOpWithOperandType(*child, op_matches, type_matches)) {
			return true;
		}
	}
	return false;
}

static bool JitExpressionContainsBinaryOpWithOtherOperandType(const JitExpressionIR &expr,
                                                              bool (*op_matches)(JitExpressionBinaryOp),
                                                              bool (*type_matches)(const LogicalType &)) {
	if (expr.kind == JitExpressionIRKind::BINARY && op_matches(expr.binary_op) &&
	    !JitExpressionBinaryOperandsMatchType(expr, type_matches)) {
		return true;
	}
	if (expr.left && JitExpressionContainsBinaryOpWithOtherOperandType(*expr.left, op_matches, type_matches)) {
		return true;
	}
	if (expr.right && JitExpressionContainsBinaryOpWithOtherOperandType(*expr.right, op_matches, type_matches)) {
		return true;
	}
	if (expr.else_node &&
	    JitExpressionContainsBinaryOpWithOtherOperandType(*expr.else_node, op_matches, type_matches)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsBinaryOpWithOtherOperandType(*child, op_matches, type_matches)) {
			return true;
		}
	}
	return false;
}

static bool JitExpressionContainsKind(const JitExpressionIR &expr, JitExpressionIRKind kind) {
	if (expr.kind == kind) {
		return true;
	}
	if (expr.left && JitExpressionContainsKind(*expr.left, kind)) {
		return true;
	}
	if (expr.right && JitExpressionContainsKind(*expr.right, kind)) {
		return true;
	}
	if (expr.else_node && JitExpressionContainsKind(*expr.else_node, kind)) {
		return true;
	}
	for (auto &child : expr.children) {
		if (child && JitExpressionContainsKind(*child, kind)) {
			return true;
		}
	}
	return false;
}

static string DescribeJitRegionCandidateTraits(const JitRegionCandidateTraits &traits) {
	string result = "traits<source=" + JitRegionBool(traits.has_source);
	result += ",sink=" + JitRegionBool(traits.has_sink);
	result += ",source_kind=" + string(JitRegionSourceKindToString(traits.source_kind));
	result += ",source_execution=" + string(JitRegionSourceExecutionKindToString(traits.source_execution));
	result += ",sink_kind=" + string(JitRegionSinkKindToString(traits.sink_kind));
	result += ",table_scan_source=" + JitRegionBool(traits.has_table_scan_source);
	result += ",stateful_source=" + JitRegionBool(traits.has_stateful_source);
	result += ",expression_traits_known=" + JitRegionBool(traits.expression_traits_known);
	result += ",source_filters=" + std::to_string(traits.source_filter_count);
	result += ",source_filter_expressions=" + std::to_string(traits.source_filter_expression_count);
	result += ",source_filter_fallbacks=" + std::to_string(traits.source_filter_fallback_count);
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
	result += ",core_expression_operators=" + std::to_string(traits.core_expression_operator_count);
	result += ",arithmetic_projections=" + std::to_string(traits.arithmetic_projection_count);
	result += ",integer_arithmetic_projections=" + std::to_string(traits.integer_arithmetic_projection_count);
	result += ",non_integer_arithmetic_projections=" + std::to_string(traits.non_integer_arithmetic_projection_count);
	result += ",reference_projections=" + std::to_string(traits.reference_projection_count);
	result += ",comparison_filters=" + std::to_string(traits.comparison_filter_count);
	result += ",integer_comparison_filters=" + std::to_string(traits.integer_comparison_filter_count);
	result += ",non_integer_comparison_filters=" + std::to_string(traits.non_integer_comparison_filter_count);
	result += ",conjunction_filters=" + std::to_string(traits.conjunction_filter_count);
	result += ",expression_fallbacks=" + std::to_string(traits.expression_fallback_count);
	result += ",operator_fallbacks=" + std::to_string(traits.operator_fallback_count);
	result += ",operator_helpers=" + std::to_string(traits.operator_helper_count);
	result += ",resumable_operators=" + std::to_string(traits.resumable_operator_count);
	result += ",scan_boundaries=" + std::to_string(traits.scan_boundary_count);
	result += ",sink_boundaries=" + std::to_string(traits.sink_boundary_count);
	result += ">";
	return result;
}

static JitRegionSourceKind InferJitRegionSourceKind(const JitRegionIRNode &node) {
	if (node.source) {
		return node.source->kind;
	}
	if (node.operator_name == "TABLE_SCAN") {
		return JitRegionSourceKind::DUCKDB_TABLE_SCAN;
	}
	if (node.boundary == JitRegionBoundaryKind::SCAN || node.boundary == JitRegionBoundaryKind::SOURCE_NATIVE) {
		return JitRegionSourceKind::TABLE_FUNCTION_SCAN;
	}
	if (node.kind == JitRegionIRNodeKind::SOURCE) {
		return JitRegionSourceKind::STATEFUL_OPERATOR;
	}
	return JitRegionSourceKind::NONE;
}

static void RecordJitRegionContractOwnership(JitRegionContract &contract, JitRegionOwnershipKind ownership) {
	switch (ownership) {
	case JitRegionOwnershipKind::GENERATED_IR:
		contract.generated_operator_count++;
		break;
	case JitRegionOwnershipKind::SOURCE_BOUNDARY:
		contract.source_boundary_count++;
		break;
	case JitRegionOwnershipKind::EXECUTOR_BOUNDARY:
		contract.executor_boundary_count++;
		break;
	default:
		break;
	}
}

static void RecordJitRegionMissingContract(JitRegionContract &contract, const string &required_capability,
                                           const string &blocker) {
	contract.missing_protocol_count++;
	AddJitUniqueString(contract.required_capabilities, required_capability);
	AddJitUniqueString(contract.blockers, blocker);
}

static JitRegionSourceExecutionKind GetJitRegionCandidateSourceExecution(const JitRegionCandidate &candidate,
                                                                         const JitRegionIRNode &node) {
	if (candidate.source_execution != JitRegionSourceExecutionKind::NONE) {
		return candidate.source_execution;
	}
	return node.source ? node.source->execution : JitRegionSourceExecutionKind::NONE;
}

static JitRegionOwnershipKind ClassifyJitRegionNativeSourceOwnership(const JitRegionSourceInfo &source,
                                                                     JitRegionContract &region_contract,
                                                                     JitRegionSourceExecutionKind execution) {
	auto &source_contract = source.native_source_contract;
	if (execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
	    source_contract.status == JitRegionNativeSourceStatus::READY) {
		AddJitUniqueString(region_contract.required_capabilities, source_contract.required_capability);
		return JitRegionOwnershipKind::NATIVE_PROTOCOL;
	}
	if (execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		AddJitUniqueString(region_contract.blockers, source_contract.blocker);
		return JitRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (execution == JitRegionSourceExecutionKind::EXECUTOR_FALLBACK) {
		AddJitUniqueString(region_contract.blockers, source_contract.blocker);
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	}
	if (!source_contract.required_capability.empty() || !source_contract.blocker.empty()) {
		RecordJitRegionMissingContract(region_contract, source_contract.required_capability, source_contract.blocker);
	}
	return JitRegionOwnershipKind::MISSING_PROTOCOL;
}

static JitRegionOwnershipKind ClassifyJitRegionStateScanOwnership(const JitRegionSourceInfo &source,
                                                                  JitRegionContract &region_contract) {
	auto &state_contract = source.native_state_scan_contract;
	if (state_contract.status == JitRegionStateContractStatus::NONE) {
		return JitRegionOwnershipKind::NONE;
	}
	if (state_contract.status == JitRegionStateContractStatus::READY) {
		AddJitUniqueString(region_contract.required_capabilities, state_contract.required_capability);
		return JitRegionOwnershipKind::NATIVE_PROTOCOL;
	}
	RecordJitRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
	return JitRegionOwnershipKind::MISSING_PROTOCOL;
}

static void RecordJitRegionGroupedStateContract(JitRegionContract &region_contract,
                                                const JitRegionAggregateProtocol &protocol);

static JitRegionOwnershipKind ClassifyJitCompiledContractOwnership(const JitCompiledOperatorContract &compiled_contract,
                                                                   JitRegionContract &region_contract,
                                                                   const string &fallback_reason) {
	if (!compiled_contract.present) {
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	}
	bool saw_native = false;
	bool saw_generated = false;
	bool saw_source_boundary = false;
	bool saw_missing = false;
	for (auto &stage : compiled_contract.stages) {
		switch (stage.execution) {
		case JitRegionStageExecutionKind::NATIVE_PROTOCOL:
			saw_native = true;
			AddJitUniqueString(region_contract.required_capabilities, stage.required_capability);
			break;
		case JitRegionStageExecutionKind::GENERATED_IR:
		case JitRegionStageExecutionKind::PASS_THROUGH:
			saw_generated = true;
			break;
		case JitRegionStageExecutionKind::SOURCE_BOUNDARY:
			saw_source_boundary = true;
			AddJitUniqueString(region_contract.blockers, stage.blocker.empty() ? fallback_reason : stage.blocker);
			break;
		case JitRegionStageExecutionKind::MISSING_PROTOCOL:
			saw_missing = true;
			RecordJitRegionMissingContract(region_contract, stage.required_capability,
			                               stage.blocker.empty() ? fallback_reason : stage.blocker);
			break;
		case JitRegionStageExecutionKind::EXECUTOR_FALLBACK:
			AddJitUniqueString(region_contract.blockers, stage.blocker.empty() ? fallback_reason : stage.blocker);
			return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
		default:
			break;
		}
	}
	if (saw_missing) {
		return JitRegionOwnershipKind::MISSING_PROTOCOL;
	}
	if (saw_source_boundary) {
		return JitRegionOwnershipKind::SOURCE_BOUNDARY;
	}
	if (saw_native) {
		return JitRegionOwnershipKind::NATIVE_PROTOCOL;
	}
	if (saw_generated) {
		return JitRegionOwnershipKind::GENERATED_IR;
	}
	return JitRegionOwnershipKind::NONE;
}

static JitRegionOwnershipKind ClassifyJitRegionSourceOwnership(const JitRegionIRNode &node,
                                                               JitRegionContract &region_contract,
                                                               JitRegionSourceExecutionKind execution) {
	if (!node.source) {
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	}
	auto &source = *node.source;
	if (source.kind == JitRegionSourceKind::STATEFUL_OPERATOR) {
		region_contract.state_scan_ownership = ClassifyJitRegionStateScanOwnership(source, region_contract);
		RecordJitRegionGroupedStateContract(region_contract, source.aggregate_protocol);
		if (region_contract.state_scan_ownership != JitRegionOwnershipKind::NONE) {
			return region_contract.state_scan_ownership;
		}
		return ClassifyJitRegionNativeSourceOwnership(source, region_contract, execution);
	}
	region_contract.state_scan_ownership = JitRegionOwnershipKind::NONE;
	return ClassifyJitRegionNativeSourceOwnership(source, region_contract, execution);
}

static void RecordJitRegionGroupedStateContract(JitRegionContract &region_contract,
                                                const JitRegionAggregateProtocol &protocol) {
	auto &state_contract = protocol.native_grouped_state_contract;
	if (state_contract.status == JitRegionStateContractStatus::NONE ||
	    state_contract.status == JitRegionStateContractStatus::READY) {
		if (state_contract.status == JitRegionStateContractStatus::READY) {
			AddJitUniqueString(region_contract.required_capabilities, state_contract.required_capability);
		}
		return;
	}
	RecordJitRegionMissingContract(region_contract, state_contract.required_capability, state_contract.blocker);
}

static JitRegionOwnershipKind ClassifyJitRegionSinkOwnership(const JitRegionIRNode &node,
                                                             JitRegionContract &region_contract) {
	if (!node.sink) {
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	}
	auto &sink = *node.sink;
	if (sink.kind == JitRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink.kind == JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		RecordJitRegionGroupedStateContract(region_contract, sink.aggregate_protocol);
	}
	return ClassifyJitCompiledContractOwnership(node.compiled_contract, region_contract, node.fallback_reason);
}

static JitRegionOwnershipKind CombineJitRegionTransformOwnership(JitRegionOwnershipKind current,
                                                                 JitRegionOwnershipKind next) {
	if (current == JitRegionOwnershipKind::EXECUTOR_BOUNDARY || next == JitRegionOwnershipKind::EXECUTOR_BOUNDARY) {
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	}
	if (current == JitRegionOwnershipKind::MISSING_PROTOCOL || next == JitRegionOwnershipKind::MISSING_PROTOCOL) {
		return JitRegionOwnershipKind::MISSING_PROTOCOL;
	}
	if (current == JitRegionOwnershipKind::GENERATED_IR || next == JitRegionOwnershipKind::GENERATED_IR) {
		return JitRegionOwnershipKind::GENERATED_IR;
	}
	return current == JitRegionOwnershipKind::NONE ? next : current;
}

static string DescribeJitRegionContract(const JitRegionContract &contract) {
	string result = "contract<abi=" + string(JitRegionABIToString(contract.abi));
	result += ",first_node=" + std::to_string(contract.first_node);
	result += ",node_count=" + std::to_string(contract.node_count);
	result += ",start_operator_index=" + std::to_string(contract.start_operator_index);
	result += ",end_operator_index=" + std::to_string(contract.end_operator_index);
	result += ",owns_source=" + JitRegionBool(contract.owns_source);
	result += ",owns_transform=" + JitRegionBool(contract.owns_transform);
	result += ",owns_sink=" + JitRegionBool(contract.owns_sink);
	result += ",owns_state_scan=" + JitRegionBool(contract.owns_state_scan);
	result += ",source=" + string(JitRegionOwnershipKindToString(contract.source_ownership));
	result += ",state_scan=" + string(JitRegionOwnershipKindToString(contract.state_scan_ownership));
	result += ",transform=" + string(JitRegionOwnershipKindToString(contract.transform_ownership));
	result += ",sink=" + string(JitRegionOwnershipKindToString(contract.sink_ownership));
	result += ",executor_boundary_free=" + JitRegionBool(contract.executor_boundary_free);
	result += ",native_fusion_ready=" + JitRegionBool(contract.native_fusion_ready);
	result += ",generated_ops=" + std::to_string(contract.generated_operator_count);
	result += ",source_boundaries=" + std::to_string(contract.source_boundary_count);
	result += ",executor_boundaries=" + std::to_string(contract.executor_boundary_count);
	result += ",missing_protocols=" + std::to_string(contract.missing_protocol_count);
	result += ",required_capabilities=" + BuildJitStringList(contract.required_capabilities);
	result += ",blockers=" + BuildJitStringList(contract.blockers);
	result += ">";
	return result;
}

static JitRegionABI DetermineJitRegionContractABI(const JitRegionContract &contract) {
	if (contract.owns_source && contract.owns_sink) {
		return JitRegionABI::FULL_PIPELINE;
	}
	if (contract.owns_source) {
		return contract.owns_state_scan && !contract.owns_transform ? JitRegionABI::STATE_SCAN
		                                                            : JitRegionABI::SOURCE_PREFIX;
	}
	if (contract.owns_sink) {
		return JitRegionABI::SINK_SUFFIX;
	}
	if (contract.owns_transform) {
		return JitRegionABI::CHUNK_TRANSFORM;
	}
	return JitRegionABI::NONE;
}

static JitRegionContract BuildJitRegionContract(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	JitRegionContract contract;
	contract.first_node = candidate.first_node;
	contract.node_count = candidate.node_count;
	contract.start_operator_index = candidate.start_operator_index;
	contract.end_operator_index = candidate.end_operator_index;
	bool has_transform = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case JitRegionIRNodeKind::SOURCE:
			contract.owns_source = true;
			contract.source_ownership =
			    ClassifyJitRegionSourceOwnership(node, contract, GetJitRegionCandidateSourceExecution(candidate, node));
			contract.owns_state_scan = contract.state_scan_ownership != JitRegionOwnershipKind::NONE;
			RecordJitRegionContractOwnership(contract, contract.source_ownership);
			break;
		case JitRegionIRNodeKind::FILTER:
		case JitRegionIRNodeKind::PROJECTION: {
			auto ownership = node.fallback_reason.empty() ? JitRegionOwnershipKind::GENERATED_IR
			                                              : JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
			contract.transform_ownership = CombineJitRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordJitRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		case JitRegionIRNodeKind::SINK:
			contract.owns_sink = true;
			contract.sink_ownership = ClassifyJitRegionSinkOwnership(node, contract);
			RecordJitRegionContractOwnership(contract, contract.sink_ownership);
			break;
		case JitRegionIRNodeKind::OPERATOR: {
			auto ownership =
			    ClassifyJitCompiledContractOwnership(node.compiled_contract, contract, node.fallback_reason);
			contract.transform_ownership = CombineJitRegionTransformOwnership(contract.transform_ownership, ownership);
			RecordJitRegionContractOwnership(contract, ownership);
			has_transform = true;
			break;
		}
		default:
			contract.transform_ownership = CombineJitRegionTransformOwnership(
			    contract.transform_ownership, JitRegionOwnershipKind::EXECUTOR_BOUNDARY);
			RecordJitRegionContractOwnership(contract, JitRegionOwnershipKind::EXECUTOR_BOUNDARY);
			has_transform = true;
			break;
		}
	}
	if (!has_transform) {
		contract.transform_ownership = JitRegionOwnershipKind::NONE;
	}
	contract.owns_transform = has_transform;
	contract.executor_boundary_free = contract.executor_boundary_count == 0;
	contract.native_fusion_ready = contract.executor_boundary_count == 0 && contract.source_boundary_count == 0 &&
	                               contract.missing_protocol_count == 0;
	contract.abi = DetermineJitRegionContractABI(contract);
	contract.ir = DescribeJitRegionContract(contract);
	return contract;
}

static JitRegionStagePlan BuildJitRegionStagePlan(const JitRegionIR &region_ir,
                                                  const JitRegionCandidate &candidate);

static void AccumulateJitRegionFilterTraits(const JitExpressionIR &root, idx_t &comparison_filter_count,
                                            idx_t &integer_comparison_filter_count,
                                            idx_t &non_integer_comparison_filter_count,
                                            idx_t &conjunction_filter_count) {
	if (JitExpressionContainsBinaryOp(root, IsJitRegionComparisonBinaryOp)) {
		comparison_filter_count++;
	}
	if (JitExpressionContainsBinaryOpWithOperandType(root, IsJitRegionComparisonBinaryOp,
	                                                 IsJitRegionIntegerLogicalType)) {
		integer_comparison_filter_count++;
	}
	if (JitExpressionContainsBinaryOpWithOtherOperandType(root, IsJitRegionComparisonBinaryOp,
	                                                      IsJitRegionIntegerLogicalType)) {
		non_integer_comparison_filter_count++;
	}
	if (JitExpressionContainsKind(root, JitExpressionIRKind::CONJUNCTION)) {
		conjunction_filter_count++;
	}
}

static bool JitRegionStageIsOperatorRole(const JitRegionStage &stage) {
	return stage.kind == JitRegionStageKind::HASH_JOIN_PROBE ||
	       stage.kind == JitRegionStageKind::OPERATOR_BOUNDARY;
}

static bool JitRegionStageIsSinkRole(const JitRegionStage &stage) {
	return stage.kind == JitRegionStageKind::HASH_JOIN_BUILD ||
	       stage.kind == JitRegionStageKind::HASH_AGGREGATE_UPDATE ||
	       stage.kind == JitRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	       stage.kind == JitRegionStageKind::UNGROUPED_AGGREGATE_UPDATE ||
	       stage.kind == JitRegionStageKind::SINK_BOUNDARY;
}

static bool JitRegionStageRequiresExecutorBoundary(JitRegionStageExecutionKind execution) {
	return execution == JitRegionStageExecutionKind::EXECUTOR_FALLBACK;
}

static bool JitRegionStageRequiresMissingProtocol(JitRegionStageExecutionKind execution) {
	return execution == JitRegionStageExecutionKind::MISSING_PROTOCOL;
}

static void AccumulateJitRegionStageTraits(const JitRegionStage &stage, JitRegionCandidateTraits &traits) {
	if (stage.kind == JitRegionStageKind::SOURCE && stage.protocol == JitCompiledProtocolKind::SCAN_CURSOR &&
	    stage.execution != JitRegionStageExecutionKind::NATIVE_PROTOCOL) {
		traits.scan_boundary_count++;
	}
	if ((stage.kind == JitRegionStageKind::FILTER || stage.kind == JitRegionStageKind::PROJECTION) &&
	    JitRegionStageRequiresExecutorBoundary(stage.execution)) {
		traits.expression_fallback_count++;
	}
	if (JitRegionStageIsOperatorRole(stage)) {
		if (JitRegionStageRequiresExecutorBoundary(stage.execution)) {
			traits.operator_fallback_count++;
		} else if (JitRegionStageRequiresMissingProtocol(stage.execution)) {
			traits.operator_helper_count++;
		}
	}
	if (JitRegionStageIsSinkRole(stage) && stage.execution != JitRegionStageExecutionKind::NATIVE_PROTOCOL) {
		traits.sink_boundary_count++;
	}
}

static void AccumulateJitRegionStagePlanTraits(const JitRegionStagePlan &stage_plan,
                                               JitRegionCandidateTraits &traits) {
	for (auto &stage : stage_plan.stages) {
		AccumulateJitRegionStageTraits(stage, traits);
	}
}

static void AccumulateJitRegionSourceTraits(const JitRegionIRNode &node, JitRegionSourceExecutionKind execution,
                                            JitRegionCandidateTraits &traits, bool &unknown_expression_traits) {
	traits.has_source = true;
	traits.source_kind = InferJitRegionSourceKind(node);
	traits.source_execution = execution;
	traits.has_table_scan_source =
	    traits.source_kind == JitRegionSourceKind::DUCKDB_TABLE_SCAN || node.operator_name == "TABLE_SCAN";
	traits.has_stateful_source = traits.source_kind == JitRegionSourceKind::STATEFUL_OPERATOR;
	if (!node.source) {
		return;
	}

	traits.source_projected_column_count = node.source->projection_ids.size();
	traits.source_returned_column_count = node.source->returned_column_count;
	traits.source_filter_count = node.source->filters.size();
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root) {
			traits.source_filter_fallback_count++;
			unknown_expression_traits = true;
			continue;
		}
		traits.source_filter_expression_count++;
		traits.core_expression_operator_count++;
		AccumulateJitRegionFilterTraits(*filter.expression->root, traits.source_comparison_filter_count,
		                                traits.source_integer_comparison_filter_count,
		                                traits.source_non_integer_comparison_filter_count,
		                                traits.source_conjunction_filter_count);
	}
}

static JitRegionCandidateTraits BuildJitRegionCandidateTraits(const JitRegionIR &region_ir,
                                                              const JitRegionCandidate &candidate,
                                                              const JitRegionStagePlan &stage_plan) {
	JitRegionCandidateTraits traits;
	bool unknown_expression_traits = false;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case JitRegionIRNodeKind::SOURCE:
			AccumulateJitRegionSourceTraits(node, GetJitRegionCandidateSourceExecution(candidate, node), traits,
			                                unknown_expression_traits);
			break;
		case JitRegionIRNodeKind::SINK:
			traits.has_sink = true;
			traits.sink_kind = node.sink ? node.sink->kind : JitRegionSinkKind::NONE;
			break;
		case JitRegionIRNodeKind::FILTER:
			traits.filter_count++;
			if (node.filter && node.filter->root) {
				traits.core_expression_operator_count++;
				AccumulateJitRegionFilterTraits(
				    *node.filter->root, traits.comparison_filter_count, traits.integer_comparison_filter_count,
				    traits.non_integer_comparison_filter_count, traits.conjunction_filter_count);
			} else {
				unknown_expression_traits = true;
			}
			break;
		case JitRegionIRNodeKind::PROJECTION:
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
				if (projection->root->kind == JitExpressionIRKind::REFERENCE) {
					traits.reference_projection_count++;
				}
				if (JitExpressionContainsBinaryOp(*projection->root, IsJitRegionArithmeticBinaryOp)) {
					traits.arithmetic_projection_count++;
				}
				if (JitExpressionContainsBinaryOpWithType(*projection->root, IsJitRegionArithmeticBinaryOp,
				                                          IsJitRegionIntegerLogicalType)) {
					traits.integer_arithmetic_projection_count++;
				}
				if (JitExpressionContainsBinaryOpWithOtherType(*projection->root, IsJitRegionArithmeticBinaryOp,
				                                               IsJitRegionIntegerLogicalType)) {
					traits.non_integer_arithmetic_projection_count++;
				}
			}
			break;
		case JitRegionIRNodeKind::OPERATOR:
			traits.operator_count++;
			traits.resumable_operator_count++;
			break;
		default:
			traits.operator_count++;
			break;
		}
	}
	AccumulateJitRegionStagePlanTraits(stage_plan, traits);
	traits.expression_traits_known = !unknown_expression_traits;
	traits.ir = DescribeJitRegionCandidateTraits(traits);
	return traits;
}

static JitRegionCandidateTraits BuildJitRegionSpanTraits(const JitRegionIR &region_ir,
                                                         JitRegionCandidate span_candidate) {
	span_candidate.contract = BuildJitRegionContract(region_ir, span_candidate);
	span_candidate.stage_plan = BuildJitRegionStagePlan(region_ir, span_candidate);
	return BuildJitRegionCandidateTraits(region_ir, span_candidate, span_candidate.stage_plan);
}

static JitRegionCandidateTraits BuildJitRegionContextTraits(const JitRegionIR &region_ir) {
	JitRegionCandidate context_candidate;
	context_candidate.first_node = 0;
	context_candidate.node_count = region_ir.nodes.size();
	context_candidate.scope = JitRegionCandidateScope::FULL_PIPELINE;
	return BuildJitRegionSpanTraits(region_ir, std::move(context_candidate));
}

static JitRegionCandidateTraits BuildJitRegionUpstreamTraits(const JitRegionIR &region_ir,
                                                             const JitRegionCandidate &candidate) {
	if (candidate.first_node == 0 || region_ir.nodes.empty()) {
		return JitRegionCandidateTraits();
	}
	JitRegionCandidate upstream_candidate;
	upstream_candidate.first_node = 0;
	upstream_candidate.node_count = MinValue(candidate.first_node, NumericCast<idx_t>(region_ir.nodes.size()));
	upstream_candidate.scope = JitRegionCandidateScope::SOURCE_PIPELINE;
	return BuildJitRegionSpanTraits(region_ir, std::move(upstream_candidate));
}

static JitRegionCandidateTraits BuildJitRegionContinuationTraits(const JitRegionIR &region_ir,
                                                                 const JitRegionCandidate &candidate) {
	JitRegionCandidate continuation_candidate;
	continuation_candidate.first_node = MinValue(candidate.EndNode(), NumericCast<idx_t>(region_ir.nodes.size()));
	if (continuation_candidate.first_node >= region_ir.nodes.size()) {
		return JitRegionCandidateTraits();
	}
	continuation_candidate.node_count = region_ir.nodes.size() - continuation_candidate.first_node;
	continuation_candidate.scope = JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL;
	return BuildJitRegionSpanTraits(region_ir, std::move(continuation_candidate));
}

static void AppendJitRegionCandidateShapeSegment(string &result, const string &segment) {
	if (segment.empty()) {
		return;
	}
	if (!result.empty()) {
		result += "-";
	}
	result += segment;
}

static void AppendJitRegionSourceShapeSegments(string &result, const JitRegionIRNode &node) {
	if (!node.source) {
		return;
	}
	if (node.source->filters.empty() && node.source->projection_ids.empty()) {
		return;
	}
	switch (node.source->kind) {
	case JitRegionSourceKind::DUCKDB_TABLE_SCAN:
	case JitRegionSourceKind::TABLE_FUNCTION_SCAN:
	case JitRegionSourceKind::GENERIC_SCAN:
		if (!node.source->filters.empty()) {
			AppendJitRegionCandidateShapeSegment(result, "scan-filter");
			if (!node.source->projection_ids.empty()) {
				AppendJitRegionCandidateShapeSegment(result, "scan-project");
			}
		}
		break;
	default:
		break;
	}
}

static string DescribeJitRegionCandidateShape(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	string result;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == JitRegionIRNodeKind::SOURCE) {
			AppendJitRegionSourceShapeSegments(result, node);
			continue;
		}
		AppendJitRegionCandidateShapeSegment(result, StringUtil::Lower(string(JitRegionIRNodeKindToString(node.kind))));
	}
	return result.empty() ? "boundary-only" : result;
}

static string NormalizeJitRegionSignatureSegment(string input) {
	input = StringUtil::Lower(input);
	input = StringUtil::Replace(std::move(input), "_", "-");
	return input;
}

static string GetJitRegionSignatureContext(const JitRegionContract &contract) {
	if (JitRegionABIIsSourcePipeline(contract.abi)) {
		return "source-prefix";
	}
	if (JitRegionABIIsChunkTransform(contract.abi)) {
		return "post-source";
	}
	if (JitRegionABIIsSinkPipeline(contract.abi)) {
		return "sink";
	}
	if (JitRegionABIIsFullPipeline(contract.abi)) {
		return "full-pipeline";
	}
	return "unknown";
}

static string GetJitRegionSinkSignatureFeature(const JitRegionIRNode &node) {
	if (!node.sink) {
		return NormalizeJitRegionSignatureSegment(node.operator_name) + "-sink";
	}
	switch (node.sink->kind) {
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case JitRegionSinkKind::SORT:
		return "sort";
	case JitRegionSinkKind::MATERIALIZATION:
		return "materialization";
	case JitRegionSinkKind::NONE:
		return string();
	default:
		return NormalizeJitRegionSignatureSegment(node.operator_name) + "-sink";
	}
}

static string GetJitRegionNodeSignatureFeature(const JitRegionIRNode &node) {
	if (node.role == "source") {
		if (node.source && node.source->kind == JitRegionSourceKind::DUCKDB_TABLE_SCAN) {
			return "table-scan-source";
		}
		if (node.operator_name.empty()) {
			return string();
		}
		return NormalizeJitRegionSignatureSegment(node.operator_name) + "-source";
	}
	if (node.role == "sink") {
		return GetJitRegionSinkSignatureFeature(node);
	}
	if (node.boundary == JitRegionBoundaryKind::OPERATOR_FALLBACK ||
	    node.boundary == JitRegionBoundaryKind::OPERATOR_HELPER ||
	    node.boundary == JitRegionBoundaryKind::OPERATOR_NATIVE) {
		return NormalizeJitRegionSignatureSegment(node.operator_name) + "-operator";
	}
	if (node.boundary == JitRegionBoundaryKind::EXPRESSION_FALLBACK) {
		return "expression-fallback";
	}
	if (node.kind == JitRegionIRNodeKind::FILTER || node.kind == JitRegionIRNodeKind::PROJECTION) {
		return string();
	}
	if (node.operator_name.empty()) {
		return string();
	}
	return NormalizeJitRegionSignatureSegment(node.operator_name);
}

static void AddJitRegionSignatureFeature(vector<string> &features, string feature) {
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

static string BuildJitRegionFeatureShape(const JitRegionIR &region_ir, idx_t first_node, idx_t node_count) {
	vector<string> features;
	const auto end_node = MinValue(first_node + node_count, NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = first_node; node_idx < end_node; node_idx++) {
		AddJitRegionSignatureFeature(features, GetJitRegionNodeSignatureFeature(region_ir.nodes[node_idx]));
	}
	return StringUtil::Join(features, "+");
}

static JitRegionSignature BuildJitRegionSignature(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	JitRegionSignature signature;
	signature.context = GetJitRegionSignatureContext(candidate.contract);
	signature.shape = candidate.shape;
	signature.feature_shape = BuildJitRegionFeatureShape(region_ir, candidate.first_node, candidate.node_count);
	signature.context_feature_shape = BuildJitRegionFeatureShape(region_ir, 0, region_ir.nodes.size());
	signature.ir = "signature<context=" + signature.context + ",shape=" + signature.shape +
	               ",features=" + signature.feature_shape + ",context_features=" + signature.context_feature_shape +
	               ">";
	return signature;
}

static JitRegionStageExecutionKind
JitRegionStageExecutionFromOwnership(JitRegionOwnershipKind ownership) {
	switch (ownership) {
	case JitRegionOwnershipKind::GENERATED_IR:
		return JitRegionStageExecutionKind::GENERATED_IR;
	case JitRegionOwnershipKind::NATIVE_PROTOCOL:
		return JitRegionStageExecutionKind::NATIVE_PROTOCOL;
	case JitRegionOwnershipKind::SOURCE_BOUNDARY:
		return JitRegionStageExecutionKind::SOURCE_BOUNDARY;
	case JitRegionOwnershipKind::EXECUTOR_BOUNDARY:
		return JitRegionStageExecutionKind::EXECUTOR_FALLBACK;
	case JitRegionOwnershipKind::MISSING_PROTOCOL:
		return JitRegionStageExecutionKind::MISSING_PROTOCOL;
	case JitRegionOwnershipKind::NONE:
	default:
		return JitRegionStageExecutionKind::NONE;
	}
}

static JitRegionOwnershipKind JitRegionOwnershipFromStageExecution(JitRegionStageExecutionKind execution) {
	switch (execution) {
	case JitRegionStageExecutionKind::GENERATED_IR:
		return JitRegionOwnershipKind::GENERATED_IR;
	case JitRegionStageExecutionKind::NATIVE_PROTOCOL:
		return JitRegionOwnershipKind::NATIVE_PROTOCOL;
	case JitRegionStageExecutionKind::SOURCE_BOUNDARY:
		return JitRegionOwnershipKind::SOURCE_BOUNDARY;
	case JitRegionStageExecutionKind::EXECUTOR_FALLBACK:
		return JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
	case JitRegionStageExecutionKind::MISSING_PROTOCOL:
		return JitRegionOwnershipKind::MISSING_PROTOCOL;
	default:
		return JitRegionOwnershipKind::NONE;
	}
}

static JitRegionStageExecutionKind
JitRegionSourceStageExecution(const JitRegionIRNode &node, const JitRegionContract &contract,
                              JitRegionSourceExecutionKind source_execution) {
	if (contract.source_ownership == JitRegionOwnershipKind::NATIVE_PROTOCOL) {
		return JitRegionStageExecutionKind::NATIVE_PROTOCOL;
	}
	if (source_execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		return JitRegionStageExecutionKind::SOURCE_BOUNDARY;
	}
	if (source_execution == JitRegionSourceExecutionKind::EXECUTOR_FALLBACK) {
		return JitRegionStageExecutionKind::EXECUTOR_FALLBACK;
	}
	if (node.source && node.source->native_source_contract.status == JitRegionNativeSourceStatus::BLOCKED) {
		return JitRegionStageExecutionKind::MISSING_PROTOCOL;
	}
	return JitRegionStageExecutionFromOwnership(contract.source_ownership);
}

static void AddJitRegionStage(JitRegionStagePlan &plan, JitRegionStageKind kind,
                              JitRegionStageExecutionKind execution, JitRegionOwnershipKind ownership,
                              idx_t node_index, const JitRegionIRNode &node, idx_t filter_index = DConstants::INVALID_INDEX,
                              string reason = string(),
                              JitCompiledProtocolKind protocol = JitCompiledProtocolKind::NONE,
                              JitCompiledDrainKind drain = JitCompiledDrainKind::NONE,
                              string required_capability = string()) {
	JitRegionStage stage;
	stage.kind = kind;
	stage.execution = execution;
	stage.ownership = ownership;
	stage.protocol = protocol;
	stage.drain = drain;
	stage.node_index = node_index;
	stage.operator_index = node.operator_index;
	stage.filter_index = filter_index;
	stage.operator_name = node.operator_name;
	stage.required_capability = std::move(required_capability);
	stage.reason = std::move(reason);
	plan.stages.push_back(std::move(stage));
}

static string BuildJitRegionCompiledStageReason(const JitCompiledStageContract &compiled_stage,
                                                const JitRegionIRNode &node) {
	string reason = compiled_stage.ir.empty() ? node.fallback_reason : compiled_stage.ir;
	if (!compiled_stage.blocker.empty() && compiled_stage.blocker != "none" &&
	    !StringUtil::Contains(reason, compiled_stage.blocker)) {
		if (!reason.empty()) {
			reason += ";";
		}
		reason += "blocker=" + compiled_stage.blocker;
	}
	return reason;
}

static void AddJitRegionCompiledStage(JitRegionStagePlan &plan, const JitCompiledStageContract &compiled_stage,
                                      idx_t node_index, const JitRegionIRNode &node,
                                      JitRegionStageExecutionKind execution,
                                      JitRegionOwnershipKind ownership) {
	AddJitRegionStage(plan, compiled_stage.stage, execution, ownership, node_index, node, DConstants::INVALID_INDEX,
	                  BuildJitRegionCompiledStageReason(compiled_stage, node), compiled_stage.protocol,
	                  compiled_stage.drain, compiled_stage.required_capability);
}

static bool AddJitRegionCompiledStages(JitRegionStagePlan &plan, const JitRegionIRNode &node, idx_t node_index) {
	if (!node.compiled_contract.present) {
		return false;
	}
	for (auto &compiled_stage : node.compiled_contract.stages) {
		auto execution = compiled_stage.execution;
		auto ownership = JitRegionOwnershipFromStageExecution(execution);
		AddJitRegionCompiledStage(plan, compiled_stage, node_index, node, execution, ownership);
	}
	return true;
}

static string DescribeJitRegionStagePlan(const JitRegionStagePlan &plan) {
	string result = "duckdb.operator-stage-region<shape=" + plan.shape;
	result += ",stages=[";
	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto &stage = plan.stages[stage_idx];
		if (stage_idx > 0) {
			result += "|";
		}
		result += JitRegionStageKindToString(stage.kind);
		result += ":";
		result += JitRegionStageExecutionKindToString(stage.execution);
		result += ":";
		result += JitRegionOwnershipKindToString(stage.ownership);
		result += ":protocol=";
		result += JitCompiledProtocolKindToString(stage.protocol);
		result += ":drain=";
		result += JitCompiledDrainKindToString(stage.drain);
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

static JitRegionStagePlan BuildJitRegionStagePlan(const JitRegionIR &region_ir,
                                                  const JitRegionCandidate &candidate) {
	JitRegionStagePlan plan;
	plan.shape = GetJitRegionSignatureContext(candidate.contract) + ":" + candidate.shape;
	const auto end_node = MinValue(candidate.EndNode(), NumericCast<idx_t>(region_ir.nodes.size()));
	for (idx_t node_idx = candidate.first_node; node_idx < end_node; node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		switch (node.kind) {
		case JitRegionIRNodeKind::SOURCE: {
			auto source_execution = GetJitRegionCandidateSourceExecution(candidate, node);
			bool added_source_stage = false;
			if (node.compiled_contract.present) {
				for (auto &compiled_stage : node.compiled_contract.stages) {
					if (compiled_stage.stage != JitRegionStageKind::SOURCE) {
						continue;
					}
					auto execution = JitRegionSourceStageExecution(node, candidate.contract, source_execution);
					auto ownership = JitRegionOwnershipFromStageExecution(execution);
					AddJitRegionCompiledStage(plan, compiled_stage, node_idx, node, execution, ownership);
					added_source_stage = true;
				}
			}
			if (!added_source_stage) {
				auto execution = JitRegionSourceStageExecution(node, candidate.contract, source_execution);
				AddJitRegionStage(plan, JitRegionStageKind::SOURCE, execution,
				                  JitRegionOwnershipFromStageExecution(execution), node_idx, node,
				                  DConstants::INVALID_INDEX, node.fallback_reason);
			}
			if (node.source) {
				for (idx_t filter_idx = 0; filter_idx < node.source->filters.size(); filter_idx++) {
					auto &filter = node.source->filters[filter_idx];
					auto filter_ownership =
					    filter.expression ? JitRegionOwnershipKind::GENERATED_IR : JitRegionOwnershipKind::EXECUTOR_BOUNDARY;
					auto filter_execution =
					    source_execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY
					        ? JitRegionStageExecutionKind::SOURCE_BOUNDARY
					        : JitRegionStageExecutionFromOwnership(filter_ownership);
					AddJitRegionStage(plan, JitRegionStageKind::SOURCE_FILTER, filter_execution, filter_ownership,
					                  node_idx, node, filter_idx, filter.reason);
				}
			}
			break;
		}
		case JitRegionIRNodeKind::FILTER:
			AddJitRegionStage(plan, JitRegionStageKind::FILTER,
			                  node.fallback_reason.empty() ? JitRegionStageExecutionKind::GENERATED_IR
			                                               : JitRegionStageExecutionKind::EXECUTOR_FALLBACK,
			                  node.fallback_reason.empty() ? JitRegionOwnershipKind::GENERATED_IR
			                                               : JitRegionOwnershipKind::EXECUTOR_BOUNDARY,
			                  node_idx, node, DConstants::INVALID_INDEX, node.fallback_reason);
			break;
		case JitRegionIRNodeKind::PROJECTION:
			AddJitRegionStage(plan, JitRegionStageKind::PROJECTION,
			                  node.fallback_reason.empty() ? JitRegionStageExecutionKind::GENERATED_IR
			                                               : JitRegionStageExecutionKind::EXECUTOR_FALLBACK,
			                  node.fallback_reason.empty() ? JitRegionOwnershipKind::GENERATED_IR
			                                               : JitRegionOwnershipKind::EXECUTOR_BOUNDARY,
			                  node_idx, node, DConstants::INVALID_INDEX, node.fallback_reason);
			break;
		case JitRegionIRNodeKind::OPERATOR: {
			if (!AddJitRegionCompiledStages(plan, node, node_idx)) {
				AddJitRegionStage(plan, JitRegionStageKind::OPERATOR_BOUNDARY,
				                  JitRegionStageExecutionKind::EXECUTOR_FALLBACK,
				                  JitRegionOwnershipKind::EXECUTOR_BOUNDARY, node_idx, node,
				                  DConstants::INVALID_INDEX, node.fallback_reason);
			}
			break;
		}
		case JitRegionIRNodeKind::SINK:
			if (!AddJitRegionCompiledStages(plan, node, node_idx)) {
				AddJitRegionStage(plan, JitRegionStageKind::SINK_BOUNDARY,
				                  JitRegionStageExecutionKind::EXECUTOR_FALLBACK,
				                  JitRegionOwnershipKind::EXECUTOR_BOUNDARY, node_idx, node,
				                  DConstants::INVALID_INDEX, node.fallback_reason);
			}
			break;
		default:
			break;
		}
	}
	plan.ir = DescribeJitRegionStagePlan(plan);
	return plan;
}

static JitRegionCandidateScope DetermineJitRegionCandidateScope(const JitRegionIR &region_ir, idx_t first_node,
                                                                idx_t node_count) {
	if (node_count == 0 || first_node >= region_ir.nodes.size()) {
		return JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL;
	}
	auto end_node = MinValue(first_node + node_count, NumericCast<idx_t>(region_ir.nodes.size()));
	auto starts_at_source = region_ir.nodes[first_node].kind == JitRegionIRNodeKind::SOURCE;
	auto ends_at_sink = end_node > first_node && region_ir.nodes[end_node - 1].kind == JitRegionIRNodeKind::SINK;
	if (starts_at_source && ends_at_sink) {
		return JitRegionCandidateScope::FULL_PIPELINE;
	}
	if (ends_at_sink) {
		return JitRegionCandidateScope::SINK_PIPELINE;
	}
	if (starts_at_source) {
		return JitRegionCandidateScope::SOURCE_PIPELINE;
	}
	return JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL;
}

static idx_t EstimateJitRegionCandidateCardinality(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	idx_t result = 0;
	if (candidate.first_node > 0 && candidate.first_node - 1 < region_ir.nodes.size()) {
		result = MaxValue(result, region_ir.nodes[candidate.first_node - 1].estimated_cardinality);
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		result = MaxValue(result, region_ir.nodes[node_idx].estimated_cardinality);
	}
	return result;
}

static string DescribeJitRegionCandidate(const JitRegionCandidate &candidate) {
	string result = "candidate" + std::to_string(candidate.candidate_id);
	result += "<first_node=" + std::to_string(candidate.first_node);
	result += ",node_count=" + std::to_string(candidate.node_count);
	result += ",start_operator_index=" + std::to_string(candidate.start_operator_index);
	result += ",end_operator_index=" + std::to_string(candidate.end_operator_index);
	result += ",estimated_cardinality=" + std::to_string(candidate.estimated_cardinality);
	result += ",scope=" + string(JitRegionCandidateScopeToString(candidate.scope));
	if (candidate.source_execution != JitRegionSourceExecutionKind::NONE) {
		result += ",source_execution=";
		result += JitRegionSourceExecutionKindToString(candidate.source_execution);
	}
	result += ",inputs=" + DescribeJitTypeList(candidate.input_types);
	result += ",outputs=" + DescribeJitTypeList(candidate.output_types);
	result += ",shape=" + candidate.shape;
	result += "," + candidate.signature.ir;
	if (!candidate.stage_plan.ir.empty()) {
		result += ",";
		result += candidate.stage_plan.ir;
	}
	result += ",pipeline_shape=" + candidate.pipeline_shape;
	result += ",context_pipeline_shape=" + candidate.context_pipeline_shape;
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

static vector<LogicalType> GetJitRegionCandidateInputTypes(const JitRegionIR &region_ir,
                                                           const JitRegionCandidate &candidate) {
	if (candidate.first_node < region_ir.nodes.size() &&
	    region_ir.nodes[candidate.first_node].kind == JitRegionIRNodeKind::SOURCE) {
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

static vector<LogicalType> GetJitRegionCandidateOutputTypes(const JitRegionIR &region_ir,
                                                            const JitRegionCandidate &candidate) {
	for (idx_t node_offset = candidate.node_count; node_offset > 0; node_offset--) {
		auto &node = region_ir.nodes[candidate.first_node + node_offset - 1];
		if (node.kind == JitRegionIRNodeKind::SINK) {
			continue;
		}
		return node.output_types;
	}
	return vector<LogicalType>();
}

static bool HasJitRegionCandidate(const JitRegionIR &region_ir, idx_t first_node, idx_t node_count,
                                  idx_t start_operator_index, idx_t end_operator_index,
                                  JitRegionSourceExecutionKind source_execution) {
	for (auto &candidate : region_ir.candidates) {
		if (candidate.first_node == first_node && candidate.node_count == node_count &&
		    candidate.start_operator_index == start_operator_index &&
		    candidate.end_operator_index == end_operator_index && candidate.source_execution == source_execution) {
			return true;
		}
	}
	return false;
}

static bool JitRegionTraitsRequireOperatorResumeProtocol(const JitRegionCandidateTraits &traits) {
	return traits.resumable_operator_count > 0 || traits.operator_helper_count > 0 ||
	       traits.operator_fallback_count > 0;
}

static bool JitRegionCandidateRequiresMissingSplitProtocol(const JitRegionCandidate &candidate) {
	switch (candidate.scope) {
	case JitRegionCandidateScope::SOURCE_PIPELINE:
		return candidate.context_traits.operator_helper_count > 0;
	case JitRegionCandidateScope::POST_SOURCE_OPERATOR_INTERVAL:
		return JitRegionTraitsRequireOperatorResumeProtocol(candidate.upstream_traits) ||
		       JitRegionTraitsRequireOperatorResumeProtocol(candidate.continuation_traits) ||
		       candidate.continuation_traits.expression_fallback_count > 0;
	case JitRegionCandidateScope::SINK_PIPELINE:
		return JitRegionTraitsRequireOperatorResumeProtocol(candidate.upstream_traits) ||
		       candidate.upstream_traits.expression_fallback_count > 0;
	default:
		return false;
	}
}

static bool AddJitRegionCandidate(JitRegionIR &region_ir, idx_t candidate_id, idx_t first_node, idx_t node_count,
                                  idx_t start_operator_index, idx_t end_operator_index,
                                  JitRegionSourceExecutionKind source_execution = JitRegionSourceExecutionKind::NONE) {
	D_ASSERT(node_count > 0);
	if (HasJitRegionCandidate(region_ir, first_node, node_count, start_operator_index, end_operator_index,
	                          source_execution)) {
		return false;
	}
	JitRegionCandidate candidate;
	candidate.candidate_id = candidate_id;
	candidate.first_node = first_node;
	candidate.node_count = node_count;
	candidate.start_operator_index = start_operator_index;
	candidate.end_operator_index = end_operator_index;
	candidate.source_execution = source_execution;
	candidate.estimated_cardinality = EstimateJitRegionCandidateCardinality(region_ir, candidate);
	candidate.scope = DetermineJitRegionCandidateScope(region_ir, first_node, node_count);
	candidate.input_types = GetJitRegionCandidateInputTypes(region_ir, candidate);
	candidate.output_types = GetJitRegionCandidateOutputTypes(region_ir, candidate);
	candidate.shape = DescribeJitRegionCandidateShape(region_ir, candidate);
	candidate.pipeline_shape = DescribeJitRegionPipelineShape(region_ir, first_node, node_count);
	candidate.context_pipeline_shape = region_ir.pipeline_shape;
	candidate.contract = BuildJitRegionContract(region_ir, candidate);
	candidate.stage_plan = BuildJitRegionStagePlan(region_ir, candidate);
	candidate.traits = BuildJitRegionCandidateTraits(region_ir, candidate, candidate.stage_plan);
	candidate.signature = BuildJitRegionSignature(region_ir, candidate);
	candidate.upstream_traits = BuildJitRegionUpstreamTraits(region_ir, candidate);
	candidate.context_traits = BuildJitRegionContextTraits(region_ir);
	candidate.continuation_traits = BuildJitRegionContinuationTraits(region_ir, candidate);
	if (JitRegionCandidateRequiresMissingSplitProtocol(candidate)) {
		return false;
	}
	candidate.ir = DescribeJitRegionCandidate(candidate);
	region_ir.candidates.push_back(std::move(candidate));
	return true;
}

static void AddJitRegionCandidateAndIncrement(JitRegionIR &region_ir, idx_t &candidate_id, idx_t first_node,
                                               idx_t node_count, idx_t start_operator_index,
                                               idx_t end_operator_index) {
	if (AddJitRegionCandidate(region_ir, candidate_id, first_node, node_count, start_operator_index,
	                          end_operator_index)) {
		candidate_id++;
	}
}

static bool JitRegionSourceHasGeneratedPrefixWork(const JitRegionIRNode &node) {
	if (!node.source) {
		return false;
	}
	if (!node.source->projection_ids.empty()) {
		return true;
	}
	for (auto &filter : node.source->filters) {
		if (filter.expression) {
			return true;
		}
	}
	return false;
}

static bool JitRegionNodeCanStayInMaximalSourcePrefixSpan(const JitRegionIRNode &node) {
	switch (node.kind) {
	case JitRegionIRNodeKind::SOURCE:
		return node.boundary == JitRegionBoundaryKind::SOURCE_NATIVE;
	case JitRegionIRNodeKind::FILTER:
	case JitRegionIRNodeKind::PROJECTION:
		return node.fallback_reason.empty();
	default:
		return false;
	}
}

static idx_t FindJitRegionMaximalSourcePrefixEnd(const JitRegionIR &region_ir) {
	idx_t end_node = 0;
	while (end_node < region_ir.nodes.size()) {
		if (!JitRegionNodeCanStayInMaximalSourcePrefixSpan(region_ir.nodes[end_node])) {
			break;
		}
		end_node++;
	}
	return end_node;
}

static idx_t GetJitRegionPrefixEndOperatorIndex(const JitRegionIR &region_ir, idx_t prefix_end,
                                                idx_t operator_count) {
	const idx_t source_offset = region_ir.nodes[0].kind == JitRegionIRNodeKind::SOURCE ? 1 : 0;
	if (prefix_end <= source_offset) {
		return 0;
	}
	return MinValue(prefix_end - source_offset, operator_count);
}

static void AddJitRegionMaximalPrefixCandidate(JitRegionIR &region_ir, idx_t &candidate_id, idx_t operator_count) {
	const auto prefix_end = FindJitRegionMaximalSourcePrefixEnd(region_ir);
	if (prefix_end == 0 || prefix_end >= region_ir.nodes.size()) {
		return;
	}
	if (prefix_end == 1 && !JitRegionSourceHasGeneratedPrefixWork(region_ir.nodes[0])) {
		return;
	}
	const auto end_operator_index = GetJitRegionPrefixEndOperatorIndex(region_ir, prefix_end, operator_count);
	AddJitRegionCandidateAndIncrement(region_ir, candidate_id, 0, prefix_end, 0, end_operator_index);
}

static void BuildJitRegionCandidates(JitRegionIR &region_ir, idx_t operator_count) {
	if (region_ir.nodes.empty()) {
		return;
	}
	idx_t candidate_id = 0;
	AddJitRegionMaximalPrefixCandidate(region_ir, candidate_id, operator_count);
	AddJitRegionCandidateAndIncrement(region_ir, candidate_id, 0, region_ir.nodes.size(), 0, operator_count);
}

static unique_ptr<JitRegionIR> TryBuildJitRegion(const JitPipelineDescriptor &descriptor) {
	if (!TryInspectJitRegionPipeline(descriptor, JitRegionPipelineInventoryMode::ADMISSION)) {
		return nullptr;
	}
	auto result = make_uniq<JitRegionIR>();
	if (descriptor.HasSource()) {
		auto &source = descriptor.source;
		auto source_node = BuildJitRegionFallbackNode("source", source, JitRegionIRNodeKind::SOURCE,
		                                              BuildJitSourceBoundaryReason(source));
		source_node.compiled_contract = source.source_contract;
		if (source.HasSourceContract()) {
			source_node.source = BuildJitRegionSourceInfo(source.source_payload);
			if (source.native_source) {
				source_node.boundary = JitRegionBoundaryKind::SOURCE_NATIVE;
			}
		} else if (IsJitRegionScanSource(source.type)) {
			source_node.source = BuildJitRegionGenericScanSourceInfo(source.Physical(), source_node.fallback_reason);
		} else {
			source_node.source = BuildJitRegionStatefulSourceInfo(source.Physical(), source_node.fallback_reason);
		}
		result->nodes.push_back(std::move(source_node));
	}
	JitRegionDataflowState state;
	for (idx_t op_idx = 0; op_idx < descriptor.operators.size(); op_idx++) {
		result->nodes.push_back(BuildJitRegionOperatorNode("op" + std::to_string(op_idx),
		                                                   descriptor.operators[op_idx], state));
	}
	if (descriptor.HasSink()) {
		auto &sink = descriptor.sink;
		auto sink_reason = BuildJitSinkBoundaryReason(sink);
		auto sink_node = BuildJitRegionFallbackNode("sink", sink, JitRegionIRNodeKind::SINK, std::move(sink_reason));
		sink_node.compiled_contract = sink.sink_contract;
		sink_node.sink = BuildJitRegionSinkInfo(sink.Physical(), sink.sink_payload, sink.HasSinkContract());
		if (sink_node.sink) {
			sink_node.fallback_reason = sink_node.sink->reason;
			if (sink.native_sink) {
				sink_node.boundary = JitRegionBoundaryKind::SINK_NATIVE;
			}
		}
		result->nodes.push_back(std::move(sink_node));
	}
	if (result->nodes.empty()) {
		return nullptr;
	}
	result->pipeline_shape = DescribeJitRegionPipelineShape(*result);
	BuildJitRegionCandidates(*result, descriptor.OperatorCount());
	result->ir = "duckdb.region typed-vector-ir";
	for (auto &candidate : result->candidates) {
		result->ir += ";";
		result->ir += candidate.ir;
	}
	for (auto &node : result->nodes) {
		result->ir += ";";
		result->ir += DescribeJitRegionIRNode(node);
	}
	return result;
}

unique_ptr<JitRegionIR> TryLowerJitRegion(const JitPipelineDescriptor &descriptor) {
	return TryBuildJitRegion(descriptor);
}

} // namespace duckdb

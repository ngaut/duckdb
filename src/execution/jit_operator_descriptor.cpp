#include "duckdb/execution/jit/operator_descriptor.hpp"

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

static constexpr const char *HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER =
    "hash-join-source-does-not-produce-rows-for-join-type";

static string JitDescriptorBool(bool value) {
	return value ? "true" : "false";
}

static string BuildJitDescriptorLogicalTypeList(const vector<LogicalType> &types) {
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

static string BuildJitDescriptorIdxList(const vector<idx_t> &values) {
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

static vector<idx_t> BuildJitDescriptorColumnIndexList(const vector<ColumnIndex> &column_indexes) {
	vector<idx_t> result;
	result.reserve(column_indexes.size());
	for (auto &column_index : column_indexes) {
		result.push_back(column_index.HasPrimaryIndex() ? column_index.GetPrimaryIndex() : DConstants::INVALID_INDEX);
	}
	return result;
}

static LogicalType BuildJitDescriptorTableScanColumnType(const PhysicalTableScan &scan,
                                                         const ColumnIndex &column_index) {
	if (column_index.IsRowIdColumn() || column_index.IsRowNumberColumn()) {
		return LogicalType::ROW_TYPE;
	}
	if (column_index.HasType()) {
		return column_index.GetScanType();
	}
	auto column_id = column_index.GetPrimaryIndex();
	if (IsVirtualColumn(column_id)) {
		auto entry = scan.virtual_columns.find(column_id);
		if (entry == scan.virtual_columns.end()) {
			throw InternalException("Virtual column not found while building JIT table scan source layout");
		}
		return entry->second.type;
	}
	if (column_id >= scan.returned_types.size()) {
		throw InternalException("Column index %llu is outside returned type count %llu while building JIT table scan "
		                        "source layout",
		                        static_cast<unsigned long long>(column_id),
		                        static_cast<unsigned long long>(scan.returned_types.size()));
	}
	return scan.returned_types[column_id];
}

static vector<LogicalType> BuildJitDescriptorTableScanSourceInputTypes(const PhysicalTableScan &scan) {
	vector<LogicalType> result;
	result.reserve(scan.column_ids.size());
	for (auto &column_index : scan.column_ids) {
		result.push_back(BuildJitDescriptorTableScanColumnType(scan, column_index));
	}
	return result;
}

static vector<idx_t> BuildJitDescriptorTableScanOutputProjectionMap(const PhysicalTableScan &scan) {
	vector<idx_t> result;
	auto output_column_count = scan.GetTypes().size();
	result.reserve(output_column_count);
	if (!scan.projection_ids.empty()) {
		for (auto projection_id : scan.projection_ids) {
			result.push_back(projection_id);
		}
		return result;
	}
	for (idx_t column_idx = 0; column_idx < output_column_count; column_idx++) {
		result.push_back(column_idx);
	}
	return result;
}

static vector<idx_t> BuildJitDescriptorTableScanFilterColumnMap(const PhysicalTableScan &scan) {
	vector<idx_t> result;
	if (!scan.table_filters) {
		return result;
	}
	result.reserve(scan.table_filters->FilterCount());
	for (auto &entry : *scan.table_filters) {
		result.push_back(entry.GetIndex().GetIndex());
	}
	return result;
}

static JitRegionNativeGroupedStateContract
BuildJitDescriptorNativeGroupedStateContract(JitRegionAggregateOperatorKind kind) {
	JitRegionNativeGroupedStateContract result;
	switch (kind) {
	case JitRegionAggregateOperatorKind::HASH:
		result.status = JitRegionStateContractStatus::MISSING;
		result.required_capability = "hash-aggregate-native-grouped-state";
		result.protocol_version = "v1";
		result.blocker = "grouped-state-protocol-boundary";
		break;
	case JitRegionAggregateOperatorKind::PERFECT_HASH:
		result.status = JitRegionStateContractStatus::MISSING;
		result.required_capability = "perfect-hash-aggregate-native-grouped-state";
		result.protocol_version = "v1";
		result.blocker = "grouped-state-protocol-boundary";
		break;
	default:
		break;
	}
	return result;
}

static JitRegionNativeStateScanContract BuildJitDescriptorNativeStateScanContract(string required_capability,
                                                                                  string blocker) {
	JitRegionNativeStateScanContract result;
	result.status = JitRegionStateContractStatus::MISSING;
	result.required_capability = std::move(required_capability);
	result.protocol_version = "v1";
	result.blocker = std::move(blocker);
	return result;
}

static JitRegionNativeOperatorContract BuildJitDescriptorNativeOperatorContract(string required_capability,
                                                                                string blocker) {
	JitRegionNativeOperatorContract result;
	result.status = JitRegionStateContractStatus::MISSING;
	result.required_capability = std::move(required_capability);
	result.protocol_version = "v1";
	result.blocker = std::move(blocker);
	return result;
}

static void MarkJitDescriptorNativeOperatorContractReady(JitRegionNativeOperatorContract &contract) {
	contract.status = JitRegionStateContractStatus::READY;
	contract.blocker = "none";
}

static void MarkJitDescriptorNativeOperatorContractBlocked(JitRegionNativeOperatorContract &contract,
                                                           string blocker) {
	contract.status = JitRegionStateContractStatus::BLOCKED;
	contract.blocker = std::move(blocker);
}

static void MarkJitDescriptorNativeStateScanContractReady(JitRegionNativeStateScanContract &contract) {
	contract.status = JitRegionStateContractStatus::READY;
	contract.blocker = "none";
}

static void MarkJitDescriptorNativeStateScanContractBlocked(JitRegionNativeStateScanContract &contract,
                                                            string blocker) {
	contract.status = JitRegionStateContractStatus::BLOCKED;
	contract.blocker = std::move(blocker);
}

static void AppendJitDescriptorNativeStateScanReason(string &reason,
                                                     const JitRegionNativeStateScanContract &contract) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";native_state_scan_contract_status=";
	reason += JitRegionStateContractStatusToString(contract.status);
	reason += ";native_state_scan_required_capability=" + contract.required_capability;
	reason += ";native_state_scan_protocol=" + contract.protocol_version;
	reason += ";native_state_scan_blocker=" + contract.blocker;
}

static void AppendJitDescriptorNativeGroupedStateReason(string &reason,
                                                        const JitRegionNativeGroupedStateContract &contract) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";native_grouped_state_contract_status=";
	reason += JitRegionStateContractStatusToString(contract.status);
	reason += ";native_grouped_state_required_capability=" + contract.required_capability;
	reason += ";native_grouped_state_protocol=" + contract.protocol_version;
	reason += ";native_grouped_state_blocker=" + contract.blocker;
}

static void AppendJitDescriptorNativeOperatorReason(string &reason, const JitRegionNativeOperatorContract &contract,
                                                    const string &prefix) {
	if (contract.status == JitRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";" + prefix + "_contract_status=";
	reason += JitRegionStateContractStatusToString(contract.status);
	reason += ";" + prefix + "_required_capability=" + contract.required_capability;
	reason += ";" + prefix + "_protocol=" + contract.protocol_version;
	reason += ";" + prefix + "_blocker=" + contract.blocker;
}

static void AppendJitDescriptorGroupedStateLayoutReason(string &reason, const JitRegionAggregateProtocol &protocol) {
	if (!protocol.present || protocol.native_grouped_state_contract.status == JitRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";grouped_state_layout_ready=" + JitDescriptorBool(protocol.grouped_state_layout_ready);
	reason += ";grouped_state_offsets=" + BuildJitDescriptorIdxList(protocol.grouped_state_offsets);
	reason += ";grouped_state_payload_sizes=" + BuildJitDescriptorIdxList(protocol.grouped_state_payload_sizes);
}

static bool IsJitDescriptorNativeDuckTableScanSupported(const PhysicalTableScan &scan) {
	if (StringUtil::Lower(scan.function.name.GetIdentifierName()) != "seq_scan") {
		return false;
	}
	if (!scan.function.function || scan.function.in_out_function || !scan.bind_data) {
		return false;
	}
	auto &bind_data = scan.bind_data->Cast<TableScanBindData>();
	return !bind_data.is_index_scan;
}

static string BuildJitDescriptorAggregateFunctionList(const vector<unique_ptr<Expression>> &aggregates) {
	string result = "[";
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		if (aggregate_idx > 0) {
			result += "|";
		}
		auto &aggregate = *aggregates[aggregate_idx];
		if (aggregate.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result += "non_aggregate";
			continue;
		}
		auto &bound_aggregate = aggregate.Cast<BoundAggregateExpression>();
		result += StringUtil::Lower(bound_aggregate.Function().GetName().GetIdentifierName());
	}
	result += "]";
	return result;
}

static vector<string> BuildJitDescriptorAggregateFunctionVector(const vector<unique_ptr<Expression>> &aggregates) {
	vector<string> result;
	result.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		if (aggregate->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result.push_back("non_aggregate");
			continue;
		}
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		result.push_back(StringUtil::Lower(bound_aggregate.Function().GetName().GetIdentifierName()));
	}
	return result;
}

static string BuildJitDescriptorAggregateReturnTypeList(const vector<unique_ptr<Expression>> &aggregates) {
	vector<LogicalType> return_types;
	return_types.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		return_types.push_back(aggregate->GetReturnType());
	}
	return BuildJitDescriptorLogicalTypeList(return_types);
}

static vector<LogicalType>
BuildJitDescriptorAggregateReturnTypeVector(const vector<unique_ptr<Expression>> &aggregates) {
	vector<LogicalType> result;
	result.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		result.push_back(aggregate->GetReturnType());
	}
	return result;
}

static string BuildJitDescriptorAggregateChildCountList(const vector<unique_ptr<Expression>> &aggregates) {
	string result = "[";
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		if (aggregate_idx > 0) {
			result += "|";
		}
		auto &aggregate = *aggregates[aggregate_idx];
		if (aggregate.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result += "0";
			continue;
		}
		auto &bound_aggregate = aggregate.Cast<BoundAggregateExpression>();
		result += std::to_string(bound_aggregate.GetChildren().size());
	}
	result += "]";
	return result;
}

static vector<idx_t> BuildJitDescriptorAggregateChildCountVector(const vector<unique_ptr<Expression>> &aggregates) {
	vector<idx_t> result;
	result.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		if (aggregate->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result.push_back(0);
			continue;
		}
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		result.push_back(bound_aggregate.GetChildren().size());
	}
	return result;
}

static string BuildJitDescriptorAggregateTypeList(const vector<unique_ptr<Expression>> &aggregates) {
	string result = "[";
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		if (aggregate_idx > 0) {
			result += "|";
		}
		auto &aggregate = *aggregates[aggregate_idx];
		if (aggregate.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result += "UNKNOWN";
			continue;
		}
		auto &bound_aggregate = aggregate.Cast<BoundAggregateExpression>();
		result += EnumUtil::ToString(bound_aggregate.GetAggregateType());
	}
	result += "]";
	return result;
}

static vector<string> BuildJitDescriptorAggregateTypeVector(const vector<unique_ptr<Expression>> &aggregates) {
	vector<string> result;
	result.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		if (aggregate->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			result.push_back("UNKNOWN");
			continue;
		}
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		result.push_back(EnumUtil::ToString(bound_aggregate.GetAggregateType()));
	}
	return result;
}

static idx_t CountJitDescriptorAggregateOrderModifiers(const vector<unique_ptr<Expression>> &aggregates) {
	idx_t order_count = 0;
	for (auto &aggregate : aggregates) {
		if (aggregate->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		if (bound_aggregate.GetOrderBys()) {
			order_count++;
		}
	}
	return order_count;
}

static idx_t CountJitDescriptorDistinctAggregates(const vector<unique_ptr<Expression>> &aggregates) {
	idx_t distinct_count = 0;
	for (auto &aggregate : aggregates) {
		if (aggregate->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound_aggregate = aggregate->Cast<BoundAggregateExpression>();
		if (bound_aggregate.IsDistinct()) {
			distinct_count++;
		}
	}
	return distinct_count;
}

static idx_t SumJitDescriptorGroupingFunctionCount(const vector<vector<ProjectionIndex>> &grouping_functions) {
	idx_t result = 0;
	for (auto &grouping_function_set : grouping_functions) {
		result += grouping_function_set.size();
	}
	return result;
}

static idx_t SumJitDescriptorIdxVector(const vector<idx_t> &values) {
	idx_t result = 0;
	for (auto value : values) {
		result += value;
	}
	return result;
}

static void AddJitDescriptorProtocolField(vector<JitRegionProtocolField> &fields, string name, string value) {
	JitRegionProtocolField field;
	field.name = std::move(name);
	field.value = std::move(value);
	fields.push_back(std::move(field));
}

static vector<JitRegionProtocolField> BuildJitDescriptorProtocolFields(const string &reason) {
	vector<JitRegionProtocolField> result;
	auto segments = StringUtil::Split(reason, ";");
	if (!segments.empty() && !segments[0].empty()) {
		AddJitDescriptorProtocolField(result, "marker", segments[0]);
	}
	for (idx_t segment_idx = 1; segment_idx < segments.size(); segment_idx++) {
		auto &segment = segments[segment_idx];
		auto equals = segment.find('=');
		if (equals == string::npos || equals == 0) {
			continue;
		}
		AddJitDescriptorProtocolField(result, segment.substr(0, equals), segment.substr(equals + 1));
	}
	return result;
}

static JitRegionStageExecutionKind JitCompiledSourceExecution(JitRegionSourceExecutionKind execution) {
	switch (execution) {
	case JitRegionSourceExecutionKind::NATIVE_SOURCE:
		return JitRegionStageExecutionKind::NATIVE_PROTOCOL;
	case JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY:
		return JitRegionStageExecutionKind::SOURCE_BOUNDARY;
	case JitRegionSourceExecutionKind::EXECUTOR_FALLBACK:
		return JitRegionStageExecutionKind::EXECUTOR_FALLBACK;
	default:
		return JitRegionStageExecutionKind::MISSING_PROTOCOL;
	}
}

static JitCompiledStageContract BuildJitCompiledSourceStage(const JitOperatorSourceDescriptor &source) {
	JitCompiledStageContract stage;
	stage.stage = JitRegionStageKind::SOURCE;
	stage.protocol = source.native_state_scan_contract.status != JitRegionStateContractStatus::NONE
	                     ? JitCompiledProtocolKind::STATE_SCAN_CURSOR
	                     : JitCompiledProtocolKind::SCAN_CURSOR;
	stage.execution = JitCompiledSourceExecution(source.execution);
	stage.drain = source.native_state_scan_contract.status != JitRegionStateContractStatus::NONE
	                  ? JitCompiledDrainKind::STATE_DRAIN
	                  : JitCompiledDrainKind::ZERO_OR_ONE_OUTPUT;
	stage.required_capability = source.native_state_scan_contract.status != JitRegionStateContractStatus::NONE
	                                ? source.native_state_scan_contract.required_capability
	                                : source.native_source_contract.required_capability;
	stage.blocker = source.native_state_scan_contract.status != JitRegionStateContractStatus::NONE
	                    ? source.native_state_scan_contract.blocker
	                    : source.native_source_contract.blocker;
	stage.ir = source.reason;
	return stage;
}

static JitCompiledStageContract BuildJitCompiledOperatorStage(const JitRegionOperatorInfo &operator_info) {
	JitCompiledStageContract stage;
	stage.stage = operator_info.kind == JitRegionOperatorKind::HASH_JOIN_PROBE ? JitRegionStageKind::HASH_JOIN_PROBE
	                                                                           : JitRegionStageKind::OPERATOR_BOUNDARY;
	stage.protocol = operator_info.kind == JitRegionOperatorKind::HASH_JOIN_PROBE
	                     ? JitCompiledProtocolKind::HASH_JOIN_PROBE_CURSOR
	                     : JitCompiledProtocolKind::NONE;
	stage.execution = operator_info.hash_join_protocol.native_probe_contract.status == JitRegionStateContractStatus::READY
	                      ? JitRegionStageExecutionKind::NATIVE_PROTOCOL
	                      : JitRegionStageExecutionKind::MISSING_PROTOCOL;
	stage.drain = operator_info.kind == JitRegionOperatorKind::HASH_JOIN_PROBE
	                  ? JitCompiledDrainKind::ZERO_OR_MANY_OUTPUT
	                  : JitCompiledDrainKind::NONE;
	stage.required_capability = operator_info.hash_join_protocol.native_probe_contract.required_capability;
	stage.blocker = operator_info.hash_join_protocol.native_probe_contract.blocker;
	stage.ir = operator_info.reason;
	return stage;
}

static bool JitCompiledAllAggregatesHaveNativeUpdates(const vector<JitRegionAggregateInput> &aggregates) {
	if (aggregates.empty()) {
		return false;
	}
	for (auto &aggregate : aggregates) {
		if (aggregate.native_update == JitAggregateUpdateKind::NONE) {
			return false;
		}
	}
	return true;
}

static JitRegionStateContractStatus JitCompiledSinkStatus(const JitRegionSinkInfo &sink) {
	switch (sink.kind) {
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return sink.hash_join_protocol.native_build_contract.status;
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return sink.aggregate_protocol.native_hash_lookup_contract.status == JitRegionStateContractStatus::READY &&
		               sink.aggregate_protocol.native_grouped_state_contract.status == JitRegionStateContractStatus::READY &&
		               JitCompiledAllAggregatesHaveNativeUpdates(sink.aggregates)
		           ? JitRegionStateContractStatus::READY
		           : JitRegionStateContractStatus::MISSING;
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return JitCompiledAllAggregatesHaveNativeUpdates(sink.aggregates) ? JitRegionStateContractStatus::READY
		                                                                  : JitRegionStateContractStatus::MISSING;
	default:
		return JitRegionStateContractStatus::MISSING;
	}
}

static JitCompiledProtocolKind JitCompiledSinkProtocol(const JitRegionSinkInfo &sink) {
	switch (sink.kind) {
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return JitCompiledProtocolKind::HASH_JOIN_BUILD;
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return JitCompiledProtocolKind::AGGREGATE_UPDATE;
	case JitRegionSinkKind::SORT:
	case JitRegionSinkKind::MATERIALIZATION:
	case JitRegionSinkKind::OPERATOR:
		return JitCompiledProtocolKind::SINK_CURSOR;
	default:
		return JitCompiledProtocolKind::NONE;
	}
}

static JitRegionStageKind JitCompiledSinkStage(const JitRegionSinkInfo &sink) {
	switch (sink.kind) {
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return JitRegionStageKind::HASH_JOIN_BUILD;
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return JitRegionStageKind::HASH_AGGREGATE_UPDATE;
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return JitRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE;
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return JitRegionStageKind::UNGROUPED_AGGREGATE_UPDATE;
	default:
		return JitRegionStageKind::SINK_BOUNDARY;
	}
}

static JitCompiledStageContract BuildJitCompiledSinkStage(const JitRegionSinkInfo &sink) {
	JitCompiledStageContract stage;
	stage.stage = JitCompiledSinkStage(sink);
	stage.protocol = JitCompiledSinkProtocol(sink);
	stage.execution = JitCompiledSinkStatus(sink) == JitRegionStateContractStatus::READY
	                      ? JitRegionStageExecutionKind::NATIVE_PROTOCOL
	                      : JitRegionStageExecutionKind::MISSING_PROTOCOL;
	stage.drain = JitCompiledDrainKind::NONE;
	if (sink.kind == JitRegionSinkKind::HASH_JOIN_BUILD) {
		stage.required_capability = sink.hash_join_protocol.native_build_contract.required_capability;
		stage.blocker = sink.hash_join_protocol.native_build_contract.blocker;
	} else if (sink.kind == JitRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	           sink.kind == JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		if (sink.aggregate_protocol.native_hash_lookup_contract.status != JitRegionStateContractStatus::READY) {
			stage.required_capability = sink.aggregate_protocol.native_hash_lookup_contract.required_capability;
			stage.blocker = sink.aggregate_protocol.native_hash_lookup_contract.blocker;
		} else if (sink.aggregate_protocol.native_grouped_state_contract.status !=
		           JitRegionStateContractStatus::READY) {
			stage.required_capability = sink.aggregate_protocol.native_grouped_state_contract.required_capability;
			stage.blocker = sink.aggregate_protocol.native_grouped_state_contract.blocker;
		} else if (!JitCompiledAllAggregatesHaveNativeUpdates(sink.aggregates)) {
			stage.required_capability = "aggregate-native-update";
			stage.blocker = "aggregate-native-update-missing";
		} else {
			stage.required_capability = sink.aggregate_protocol.native_hash_lookup_contract.required_capability;
			stage.blocker = sink.aggregate_protocol.native_hash_lookup_contract.blocker;
		}
	} else if (sink.kind == JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		stage.required_capability = "ungrouped-aggregate-native-update";
		stage.blocker = JitCompiledAllAggregatesHaveNativeUpdates(sink.aggregates) ? "none"
		                                                                           : "aggregate-native-update-missing";
	} else {
		stage.required_capability = "sink-cursor";
		stage.blocker = "sink-cursor-protocol-missing";
	}
	stage.ir = sink.reason;
	return stage;
}

static string BuildJitCompiledContractIR(const JitCompiledOperatorContract &contract) {
	string result = "compiled_contract<stages=" + std::to_string(contract.stages.size());
	result += ",source=" + JitDescriptorBool(contract.has_source);
	result += ",operator=" + JitDescriptorBool(contract.has_operator);
	result += ",sink=" + JitDescriptorBool(contract.has_sink);
	result += ",state_scan=" + JitDescriptorBool(contract.has_state_scan);
	result += ",resumable_output=" + JitDescriptorBool(contract.has_resumable_output);
	result += ",executor_boundary_free=" + JitDescriptorBool(contract.executor_boundary_free);
	for (idx_t stage_idx = 0; stage_idx < contract.stages.size(); stage_idx++) {
		auto &stage = contract.stages[stage_idx];
		result += ",stage" + std::to_string(stage_idx) + "=<protocol=" +
		          string(JitCompiledProtocolKindToString(stage.protocol));
		result += ",execution=" + string(JitRegionStageExecutionKindToString(stage.execution));
		result += ",drain=" + string(JitCompiledDrainKindToString(stage.drain));
		if (!stage.required_capability.empty()) {
			result += ",capability=" + stage.required_capability;
		}
		if (!stage.blocker.empty()) {
			result += ",blocker=" + stage.blocker;
		}
		result += ">";
	}
	result += ">";
	return result;
}

JitOperatorDescriptor FinalizeJitOperatorDescriptor(JitOperatorDescriptor descriptor) {
	auto &contract = descriptor.compiled_contract;
	contract.present = descriptor.has_source || descriptor.has_operator || descriptor.has_sink;
	contract.has_source = descriptor.has_source;
	contract.has_operator = descriptor.has_operator;
	contract.has_sink = descriptor.has_sink;
	contract.stages.clear();

	if (descriptor.has_source) {
		auto stage = BuildJitCompiledSourceStage(descriptor.source);
		contract.has_state_scan = stage.protocol == JitCompiledProtocolKind::STATE_SCAN_CURSOR;
		contract.stages.push_back(std::move(stage));
	}
	if (descriptor.has_operator) {
		auto stage = BuildJitCompiledOperatorStage(descriptor.operator_info);
		contract.has_resumable_output =
		    contract.has_resumable_output || stage.drain == JitCompiledDrainKind::ZERO_OR_MANY_OUTPUT;
		contract.stages.push_back(std::move(stage));
	}
	if (descriptor.has_sink) {
		contract.stages.push_back(BuildJitCompiledSinkStage(descriptor.sink));
	}

	contract.executor_boundary_free = contract.present;
	for (auto &stage : contract.stages) {
		if (stage.execution != JitRegionStageExecutionKind::NATIVE_PROTOCOL &&
		    stage.execution != JitRegionStageExecutionKind::GENERATED_IR) {
			contract.executor_boundary_free = false;
			break;
		}
	}
	contract.ir = BuildJitCompiledContractIR(contract);
	return descriptor;
}

static string BuildJitDescriptorJoinComparisonList(const vector<JoinCondition> &conditions) {
	string result = "[";
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += "|";
		}
		auto &condition = conditions[condition_idx];
		result += condition.IsComparison() ? ExpressionTypeToString(condition.GetComparisonType()) : "ARBITRARY";
	}
	result += "]";
	return result;
}

static bool IsJitDescriptorHashJoinEqualityComparison(ExpressionType comparison_type) {
	return comparison_type == ExpressionType::COMPARE_EQUAL ||
	       comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static bool IsJitDescriptorHashJoinNullEqualComparison(ExpressionType comparison_type) {
	return comparison_type == ExpressionType::COMPARE_DISTINCT_FROM ||
	       comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static string JitDescriptorHashJoinProbeOutputModeToString(JitRegionHashJoinProbeOutputMode mode) {
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

static JitRegionHashJoinProbeOutputMode BuildJitDescriptorHashJoinProbeOutputMode(JoinType join_type) {
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
		return JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
	case JoinType::SEMI:
		return JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	case JoinType::MARK:
		return JitRegionHashJoinProbeOutputMode::MARK_PROBE;
	case JoinType::RIGHT_SEMI:
		return JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
	default:
		return JitRegionHashJoinProbeOutputMode::NONE;
	}
}

static bool IsJitDescriptorNativeHashJoinOwnedJoinType(JoinType join_type) {
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::MARK:
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

static bool IsJitDescriptorHashJoinBoundReference(const Expression &expression, idx_t &input_index) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	input_index = expression.Cast<BoundReferenceExpression>().Index();
	return true;
}

static string BuildJitDescriptorHashJoinKeyBindingBlocker(const PhysicalHashJoin &join, bool build_side) {
	for (idx_t key_idx = 0; key_idx < join.conditions.size(); key_idx++) {
		auto &condition = join.conditions[key_idx];
		idx_t input_index = 0;
		auto &expression = build_side ? condition.GetRHS() : condition.GetLHS();
		if (!IsJitDescriptorHashJoinBoundReference(expression, input_index)) {
			return string("hash-join-native-") + (build_side ? "build" : "probe") +
			       "-key-not-reference;key_index=" + std::to_string(key_idx);
		}
	}
	return "none";
}

static void AddJitDescriptorHashJoinRegularLayout(JitRegionHashJoinProtocol &result) {
	vector<LogicalType> layout_types(result.condition_types);
	layout_types.insert(layout_types.end(), result.payload_types.begin(), result.payload_types.end());
	result.found_match_column_present = PropagatesBuildSide(result.join_type);
	if (result.found_match_column_present) {
		result.found_match_column_index = layout_types.size();
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	result.hash_column_index = layout_types.size();
	layout_types.emplace_back(LogicalType::HASH);

	TupleDataLayout layout;
	layout.Initialize(std::move(layout_types), TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	auto &offsets = layout.GetOffsets();
	if (offsets.size() <= result.hash_column_index) {
		return;
	}
	result.layout_column_count = layout.ColumnCount();
	result.layout_offsets = offsets;
	result.tuple_size = offsets[result.condition_count + result.payload_column_count];
	result.pointer_offset = offsets.back();
	result.entry_size = layout.GetRowWidth();
	result.regular_hash_table_layout_ready = true;
}

static string BuildJitDescriptorHashJoinCommonNativeBlocker(const JitRegionHashJoinProtocol &protocol) {
	if (!IsJitDescriptorNativeHashJoinOwnedJoinType(protocol.join_type)) {
		return "hash-join-native-join-type;join_type=" + StringUtil::Lower(JoinTypeToString(protocol.join_type));
	}
	if (protocol.condition_count == 0) {
		return "hash-join-native-no-conditions";
	}
	if (!protocol.regular_hash_table_layout_ready) {
		return "hash-join-native-layout";
	}
	return "none";
}

static string BuildJitDescriptorHashJoinProbeNativeShapeBlocker(const JitRegionHashJoinProtocol &protocol,
                                                                const string &common_blocker) {
	if (common_blocker != "none") {
		return common_blocker;
	}
	if (protocol.equality_condition_count == 0) {
		return "hash-join-native-no-equality-keys";
	}
	if (protocol.equality_condition_count > protocol.condition_count) {
		return "hash-join-native-condition-shape";
	}
	if (protocol.residual_predicate || protocol.residual_info) {
		return "hash-join-native-residual-predicate";
	}
	if (protocol.non_equality_condition_count != 0 &&
	    protocol.native_probe_output_mode != JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    protocol.native_probe_output_mode != JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return "hash-join-native-non-equality-output-mode-protocol-missing";
	}
	return "none";
}

static string BuildJitDescriptorHashJoinProbeShapeBlocker(const JitRegionHashJoinProtocol &protocol,
                                                                     const string &probe_shape_blocker) {
	if (probe_shape_blocker != "none") {
		return probe_shape_blocker;
	}
	if (protocol.native_probe_output_mode == JitRegionHashJoinProbeOutputMode::NONE) {
		return "hash-join-native-probe-join-type;join_type=" +
		       StringUtil::Lower(JoinTypeToString(protocol.join_type));
	}
	return "none";
}

static string BuildJitDescriptorHashJoinBuildAppendShapeBlocker(const JitRegionHashJoinProtocol &protocol,
                                                                const string &common_blocker) {
	if (common_blocker != "none") {
		return common_blocker;
	}
	return "none";
}

static string BuildJitDescriptorHashJoinBuildNativeBlocker(const PhysicalHashJoin &join,
                                                           const JitRegionHashJoinProtocol &protocol,
                                                           const string &common_blocker) {
	if (common_blocker != "none") {
		return common_blocker;
	}
	auto key_blocker = BuildJitDescriptorHashJoinKeyBindingBlocker(join, true);
	if (key_blocker != "none") {
		return key_blocker;
	}
	return "none";
}

static string BuildJitDescriptorHashJoinProbeNativeBlocker(const PhysicalHashJoin &join,
                                                           const JitRegionHashJoinProtocol &protocol,
                                                           const string &probe_shape_blocker) {
	if (probe_shape_blocker != "none") {
		return probe_shape_blocker;
	}
	auto key_blocker = BuildJitDescriptorHashJoinKeyBindingBlocker(join, false);
	if (key_blocker != "none") {
		return key_blocker;
	}
	return "none";
}

static bool JitDescriptorHashJoinSourceProducesRows(JoinType join_type) {
	return PropagatesBuildSide(join_type);
}

static void MarkJitDescriptorHashJoinNativeContracts(const PhysicalHashJoin &join,
                                                     JitRegionHashJoinProtocol &protocol) {
	auto common_blocker = BuildJitDescriptorHashJoinCommonNativeBlocker(protocol);
	auto probe_shape_blocker = BuildJitDescriptorHashJoinProbeNativeShapeBlocker(protocol, common_blocker);
	protocol.native_protocol_blocker = common_blocker;
	protocol.native_probe_shape_blocker =
	    BuildJitDescriptorHashJoinProbeShapeBlocker(protocol, probe_shape_blocker);
	protocol.native_probe_shape_ready = protocol.native_probe_shape_blocker == "none";
	protocol.build_append_shape_blocker = BuildJitDescriptorHashJoinBuildAppendShapeBlocker(protocol, common_blocker);
	protocol.build_append_shape_ready = protocol.build_append_shape_blocker == "none";

	auto build_blocker = BuildJitDescriptorHashJoinBuildNativeBlocker(join, protocol, common_blocker);
	if (build_blocker == "none") {
		MarkJitDescriptorNativeOperatorContractReady(protocol.native_build_contract);
	} else {
		protocol.native_build_contract.blocker = build_blocker;
	}

	auto probe_blocker = BuildJitDescriptorHashJoinProbeNativeBlocker(join, protocol, probe_shape_blocker);
	if (probe_blocker == "none") {
		MarkJitDescriptorNativeOperatorContractReady(protocol.native_probe_contract);
	} else {
		protocol.native_probe_contract.blocker = probe_blocker;
	}
}

static JitRegionHashJoinProtocol BuildJitDescriptorHashJoinProtocol(const PhysicalHashJoin &join) {
	JitRegionHashJoinProtocol result;
	result.present = true;
	result.native_probe_contract =
	    BuildJitDescriptorNativeOperatorContract("hash-join-native-probe", "join-probe-protocol-boundary");
	result.native_build_contract =
	    BuildJitDescriptorNativeOperatorContract("hash-join-native-build", "join-build-protocol-boundary");
	result.join_type = join.join_type;
	result.condition_count = join.conditions.size();
	result.condition_types = join.condition_types;
	result.payload_column_count = join.payload_columns.col_idxs.size();
	result.payload_column_indices = join.payload_columns.col_idxs;
	result.payload_types = join.payload_columns.col_types;
	result.lhs_output_column_count = join.lhs_output_columns.col_idxs.size();
	result.lhs_output_column_indices = join.lhs_output_columns.col_idxs;
	result.lhs_output_types = join.lhs_output_columns.col_types;
	result.rhs_output_column_count = join.rhs_output_columns.col_idxs.size();
	result.rhs_output_types = join.rhs_output_columns.col_types;
	result.lhs_probe_column_count = join.lhs_probe_columns.col_idxs.size();
	result.lhs_probe_column_indices = join.lhs_probe_columns.col_idxs;
	result.lhs_probe_types = join.lhs_probe_columns.col_types;
	result.lhs_output_in_probe_count = join.lhs_output_in_probe.size();
	result.delim_type_count = join.delim_types.size();
	result.correlated_mark_counts_required =
	    result.join_type == JoinType::MARK && result.delim_type_count > 0 &&
	    result.delim_type_count + 1 == result.condition_count;
	result.residual_predicate = static_cast<bool>(join.predicate);
	result.residual_info = static_cast<bool>(join.residual_info);
	result.filter_pushdown = static_cast<bool>(join.filter_pushdown);
	result.source_produces_rows = JitDescriptorHashJoinSourceProducesRows(result.join_type);
	result.native_probe_output_mode = BuildJitDescriptorHashJoinProbeOutputMode(result.join_type);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			result.comparison_types.push_back(ExpressionType::INVALID);
			continue;
		}
		auto comparison_type = condition.GetComparisonType();
		result.comparison_types.push_back(comparison_type);
		if (IsJitDescriptorHashJoinEqualityComparison(comparison_type)) {
			result.equality_condition_count++;
		}
		if (IsJitDescriptorHashJoinNullEqualComparison(comparison_type)) {
			result.null_equal_condition_count++;
		}
	}
	result.non_equality_condition_count = result.condition_count - result.equality_condition_count;
	if (join.filter_pushdown) {
		result.filter_pushdown_condition_count = join.filter_pushdown->join_condition.size();
		result.filter_pushdown_probe_count = join.filter_pushdown->probe_info.size();
		result.build_side_has_filter = join.filter_pushdown->build_side_has_filter;
	}
	AddJitDescriptorHashJoinRegularLayout(result);
	MarkJitDescriptorHashJoinNativeContracts(join, result);
	return result;
}

static string BuildJitDescriptorHashJoinBoundaryReason(const PhysicalHashJoin &join, const string &marker) {
	auto protocol = BuildJitDescriptorHashJoinProtocol(join);
	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(join.type);
	result += ";join_type=" + StringUtil::Lower(JoinTypeToString(protocol.join_type));
	result += ";condition_count=" + std::to_string(protocol.condition_count);
	result += ";equality_condition_count=" + std::to_string(protocol.equality_condition_count);
	result += ";non_equality_condition_count=" + std::to_string(protocol.non_equality_condition_count);
	result += ";null_equal_condition_count=" + std::to_string(protocol.null_equal_condition_count);
	result += ";condition_types=" + BuildJitDescriptorLogicalTypeList(protocol.condition_types);
	result += ";comparison_ops=" + BuildJitDescriptorJoinComparisonList(join.conditions);
	result += ";payload_columns=" + std::to_string(protocol.payload_column_count);
	result += ";payload_column_indices=" + BuildJitDescriptorIdxList(protocol.payload_column_indices);
	result += ";payload_types=" + BuildJitDescriptorLogicalTypeList(protocol.payload_types);
	result += ";lhs_output_columns=" + std::to_string(protocol.lhs_output_column_count);
	result += ";lhs_output_column_indices=" + BuildJitDescriptorIdxList(protocol.lhs_output_column_indices);
	result += ";lhs_output_types=" + BuildJitDescriptorLogicalTypeList(protocol.lhs_output_types);
	result += ";rhs_output_columns=" + std::to_string(protocol.rhs_output_column_count);
	result += ";rhs_output_types=" + BuildJitDescriptorLogicalTypeList(protocol.rhs_output_types);
	result += ";lhs_probe_columns=" + std::to_string(protocol.lhs_probe_column_count);
	result += ";lhs_probe_column_indices=" + BuildJitDescriptorIdxList(protocol.lhs_probe_column_indices);
	result += ";lhs_probe_types=" + BuildJitDescriptorLogicalTypeList(protocol.lhs_probe_types);
	result += ";lhs_output_in_probe=" + std::to_string(protocol.lhs_output_in_probe_count);
	result += ";delim_types=" + std::to_string(protocol.delim_type_count);
	result += ";correlated_mark_counts_required=" + JitDescriptorBool(protocol.correlated_mark_counts_required);
	result += ";residual_predicate=" + JitDescriptorBool(protocol.residual_predicate);
	result += ";residual_info=" + JitDescriptorBool(protocol.residual_info);
	result += ";filter_pushdown=" + JitDescriptorBool(protocol.filter_pushdown);
	result += ";filter_pushdown_condition_count=" + std::to_string(protocol.filter_pushdown_condition_count);
	result += ";filter_pushdown_probe_count=" + std::to_string(protocol.filter_pushdown_probe_count);
	result += ";build_side_has_filter=" + JitDescriptorBool(protocol.build_side_has_filter);
	result += ";source_produces_rows=" + JitDescriptorBool(protocol.source_produces_rows);
	result += ";regular_hash_table_layout_ready=" + JitDescriptorBool(protocol.regular_hash_table_layout_ready);
	result += ";native_probe_shape_ready=" + JitDescriptorBool(protocol.native_probe_shape_ready);
	result += ";native_probe_shape_blocker=" + protocol.native_probe_shape_blocker;
	result += ";native_probe_output_mode=" +
	          JitDescriptorHashJoinProbeOutputModeToString(protocol.native_probe_output_mode);
	result += ";build_append_shape_ready=" + JitDescriptorBool(protocol.build_append_shape_ready);
	result += ";build_append_shape_blocker=" + protocol.build_append_shape_blocker;
	result += ";hash_join_layout_column_count=" + std::to_string(protocol.layout_column_count);
	result += ";hash_join_layout_offsets=" + BuildJitDescriptorIdxList(protocol.layout_offsets);
	result += ";hash_join_tuple_size=" + std::to_string(protocol.tuple_size);
	result += ";hash_join_entry_size=" + std::to_string(protocol.entry_size);
	result += ";hash_join_pointer_offset=" + std::to_string(protocol.pointer_offset);
	result += ";hash_join_hash_column_index=" + std::to_string(protocol.hash_column_index);
	result += ";hash_join_found_match_column_present=" + JitDescriptorBool(protocol.found_match_column_present);
	result += ";hash_join_found_match_column_index=" + std::to_string(protocol.found_match_column_index);
	result += ";hash_join_native_protocol_blocker=" + protocol.native_protocol_blocker;
	AppendJitDescriptorNativeOperatorReason(result, protocol.native_probe_contract, "native_hash_join_probe");
	AppendJitDescriptorNativeOperatorReason(result, protocol.native_build_contract, "native_hash_join_build");
	return result;
}

static JitOperatorDescriptor BuildJitDescriptorSortStateSource(PhysicalOperatorType type, idx_t output_column_count,
                                                               idx_t order_count, idx_t projection_count,
                                                               const string &function_name,
                                                               const string &native_capability,
                                                               const string &marker) {
	JitOperatorDescriptor result;
	auto state_scan_contract = BuildJitDescriptorNativeStateScanContract(native_capability, "none");
	MarkJitDescriptorNativeStateScanContractReady(state_scan_contract);
	result.source_boundary_reason = marker;
	result.source_boundary_reason += ";operator=" + PhysicalOperatorToString(type);
	result.source_boundary_reason += ";order_count=" + std::to_string(order_count);
	result.source_boundary_reason += ";projection_count=" + std::to_string(projection_count);
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = JitRegionSourceExecutionKind::NATIVE_SOURCE;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	AppendJitDescriptorNativeStateScanReason(result.source_boundary_reason, result.source.native_state_scan_contract);
	result.source.function_name = function_name;
	result.source.output_column_count = output_column_count;
	result.source.returned_column_count = output_column_count;
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	return result;
}

static JitRegionAggregateProtocol BuildJitDescriptorHashAggregateProtocol(const PhysicalHashAggregate &aggregate);
static JitRegionAggregateProtocol
BuildJitDescriptorPerfectHashAggregateProtocol(const PhysicalPerfectHashAggregate &aggregate);

static string BuildJitDescriptorHashAggregateBoundaryReason(const PhysicalHashAggregate &aggregate,
                                                            const string &marker) {
	auto &aggregate_data = aggregate.grouped_aggregate_data;
	idx_t distinct_aggregate_count = 0;
	idx_t distinct_table_count = 0;
	idx_t distinct_child_count = 0;
	if (aggregate.distinct_collection_info) {
		distinct_aggregate_count = aggregate.distinct_collection_info->Indices().size();
		distinct_table_count = aggregate.distinct_collection_info->table_count;
		distinct_child_count = aggregate.distinct_collection_info->total_child_count;
	}

	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(aggregate.type);
	result += ";aggregate_operator_kind=hash";
	result += ";group_count=" + std::to_string(aggregate_data.GroupCount());
	result += ";group_types=" + BuildJitDescriptorLogicalTypeList(aggregate_data.group_types);
	result += ";aggregate_count=" + std::to_string(aggregate_data.aggregates.size());
	result += ";aggregate_functions=" + BuildJitDescriptorAggregateFunctionList(aggregate_data.aggregates);
	result += ";aggregate_return_types=" + BuildJitDescriptorAggregateReturnTypeList(aggregate_data.aggregates);
	result += ";aggregate_child_counts=" + BuildJitDescriptorAggregateChildCountList(aggregate_data.aggregates);
	result += ";aggregate_types=" + BuildJitDescriptorAggregateTypeList(aggregate_data.aggregates);
	result += ";aggregate_filter_count=" + std::to_string(aggregate_data.filter_count);
	result += ";aggregate_order_count=" +
	          std::to_string(CountJitDescriptorAggregateOrderModifiers(aggregate_data.aggregates));
	result += ";payload_type_count=" + std::to_string(aggregate_data.payload_types.size());
	result += ";payload_types=" + BuildJitDescriptorLogicalTypeList(aggregate_data.payload_types);
	result += ";grouping_set_count=" + std::to_string(aggregate.grouping_sets.size());
	result += ";grouping_function_count=" +
	          std::to_string(SumJitDescriptorGroupingFunctionCount(aggregate_data.GetGroupingFunctions()));
	result += ";radix_table_count=" + std::to_string(aggregate.groupings.size());
	result += ";distinct_aggregate_count=" + std::to_string(distinct_aggregate_count);
	result += ";distinct_table_count=" + std::to_string(distinct_table_count);
	result += ";distinct_child_count=" + std::to_string(distinct_child_count);
	result += ";input_group_type_count=" + std::to_string(aggregate.input_group_types.size());
	result += ";input_group_types=" + BuildJitDescriptorLogicalTypeList(aggregate.input_group_types);
	result += ";non_distinct_filter_count=" + std::to_string(aggregate.non_distinct_filter.size());
	result += ";distinct_filter_count=" + std::to_string(aggregate.distinct_filter.size());
	auto protocol = BuildJitDescriptorHashAggregateProtocol(aggregate);
	AppendJitDescriptorNativeOperatorReason(result, protocol.native_hash_lookup_contract,
	                                        "native_hash_aggregate_lookup");
	return result;
}

static void AddJitDescriptorAggregateProtocolCommon(JitRegionAggregateProtocol &result,
                                                    const vector<unique_ptr<Expression>> &aggregates) {
	result.aggregate_count = aggregates.size();
	result.aggregate_functions = BuildJitDescriptorAggregateFunctionVector(aggregates);
	result.aggregate_return_types = BuildJitDescriptorAggregateReturnTypeVector(aggregates);
	result.aggregate_child_counts = BuildJitDescriptorAggregateChildCountVector(aggregates);
	result.aggregate_types = BuildJitDescriptorAggregateTypeVector(aggregates);
	result.aggregate_order_count = CountJitDescriptorAggregateOrderModifiers(aggregates);
	result.distinct_aggregate_count = CountJitDescriptorDistinctAggregates(aggregates);
}

static void AddJitDescriptorHashAggregateGroupedStateLayout(JitRegionAggregateProtocol &result,
                                                            const PhysicalHashAggregate &aggregate) {
	if (aggregate.groupings.empty()) {
		return;
	}
	auto &layout = aggregate.groupings[0].table_data.GetLayout();
	auto aggregate_offset_idx = layout.ColumnCount();
	auto &offsets = layout.GetOffsets();
	auto &layout_aggregates = layout.GetAggregates();
	if (offsets.size() < aggregate_offset_idx + result.aggregate_count ||
	    layout_aggregates.size() < result.aggregate_count) {
		return;
	}
	result.grouped_state_offsets.reserve(result.aggregate_count);
	result.grouped_state_payload_sizes.reserve(result.aggregate_count);
	for (idx_t aggregate_idx = 0; aggregate_idx < result.aggregate_count; aggregate_idx++) {
		result.grouped_state_offsets.push_back(offsets[aggregate_offset_idx + aggregate_idx]);
		result.grouped_state_payload_sizes.push_back(layout_aggregates[aggregate_idx].payload_size);
	}
	result.grouped_state_layout_ready = true;
}

static void MarkJitDescriptorHashAggregateGroupedStateContract(JitRegionAggregateProtocol &result) {
	if (!result.grouped_state_layout_ready || result.grouping_set_count != 1 || result.radix_table_count != 1 ||
	    result.distinct_aggregate_count != 0 || result.aggregate_filter_count != 0 || result.aggregate_order_count != 0 ||
	    result.non_distinct_filter_count != result.aggregate_count) {
		return;
	}
	result.native_grouped_state_contract.status = JitRegionStateContractStatus::READY;
	result.native_grouped_state_contract.blocker = "none";
}

static bool JitDescriptorHashAggregateHasDistinctState(const JitRegionAggregateProtocol &protocol) {
	return protocol.distinct_aggregate_count != 0 || protocol.distinct_table_count != 0 ||
	       protocol.distinct_child_count != 0 || protocol.distinct_filter_count != 0;
}

static void MarkJitDescriptorHashAggregateDistinctStateBoundary(JitRegionAggregateProtocol &protocol) {
	if (!JitDescriptorHashAggregateHasDistinctState(protocol)) {
		return;
	}
	if (protocol.native_grouped_state_contract.status == JitRegionStateContractStatus::MISSING) {
		protocol.native_grouped_state_contract.blocker = "hash-aggregate-distinct-grouped-state-protocol-boundary";
	}
	if (protocol.native_hash_lookup_contract.status == JitRegionStateContractStatus::MISSING) {
		protocol.native_hash_lookup_contract.blocker = "hash-aggregate-distinct-lookup-protocol-boundary";
	}
}

static void MarkJitDescriptorPerfectHashAggregateGroupedStateContract(JitRegionAggregateProtocol &result) {
	if (!result.grouped_state_layout_ready || result.distinct_aggregate_count != 0 || result.aggregate_filter_count != 0 ||
	    result.aggregate_order_count != 0) {
		return;
	}
	result.native_grouped_state_contract.status = JitRegionStateContractStatus::READY;
	result.native_grouped_state_contract.blocker = "none";
}

static string BuildJitDescriptorPerfectHashAggregateStateScanBlocker(const JitRegionAggregateProtocol &protocol) {
	if (!protocol.present || protocol.kind != JitRegionAggregateOperatorKind::PERFECT_HASH) {
		return "perfect-hash-aggregate-state-scan-kind";
	}
	if (protocol.group_count == 0) {
		return "perfect-hash-aggregate-state-scan-no-groups";
	}
	if (protocol.group_count != protocol.perfect_required_bits_count ||
	    protocol.group_count != protocol.perfect_group_minima_count) {
		return "perfect-hash-aggregate-state-scan-group-layout";
	}
	if (protocol.distinct_aggregate_count != 0 || protocol.distinct_table_count != 0 ||
	    protocol.distinct_child_count != 0) {
		return "perfect-hash-aggregate-state-scan-distinct-state";
	}
	return "none";
}

static void MarkJitDescriptorPerfectHashAggregateStateScanContract(
    JitRegionNativeStateScanContract &contract, const JitRegionAggregateProtocol &protocol) {
	auto blocker = BuildJitDescriptorPerfectHashAggregateStateScanBlocker(protocol);
	if (blocker == "none") {
		MarkJitDescriptorNativeStateScanContractReady(contract);
	} else {
		contract.blocker = blocker;
	}
}

static string BuildJitDescriptorUngroupedAggregateStateScanBlocker(const JitRegionAggregateProtocol &protocol) {
	if (!protocol.present || protocol.kind != JitRegionAggregateOperatorKind::UNGROUPED) {
		return "ungrouped-aggregate-state-scan-kind";
	}
	if (protocol.aggregate_count == 0) {
		return "ungrouped-aggregate-state-scan-no-aggregates";
	}
	return "none";
}

static void MarkJitDescriptorUngroupedAggregateStateScanContract(
    JitRegionNativeStateScanContract &contract, const JitRegionAggregateProtocol &protocol) {
	auto blocker = BuildJitDescriptorUngroupedAggregateStateScanBlocker(protocol);
	if (blocker == "none") {
		MarkJitDescriptorNativeStateScanContractReady(contract);
	} else {
		contract.blocker = blocker;
	}
}

static void AddJitDescriptorPerfectHashAggregateGroupedStateLayout(JitRegionAggregateProtocol &result,
                                                                   const PhysicalPerfectHashAggregate &aggregate) {
	if (aggregate.aggregate_objects.size() < result.aggregate_count) {
		return;
	}
	idx_t state_offset = 0;
	result.grouped_state_offsets.reserve(result.aggregate_count);
	result.grouped_state_payload_sizes.reserve(result.aggregate_count);
	for (idx_t aggregate_idx = 0; aggregate_idx < result.aggregate_count; aggregate_idx++) {
		auto payload_size = aggregate.aggregate_objects[aggregate_idx].payload_size;
		result.grouped_state_offsets.push_back(state_offset);
		result.grouped_state_payload_sizes.push_back(payload_size);
		state_offset += payload_size;
	}
	result.grouped_state_layout_ready = true;
}

static JitRegionAggregateProtocol BuildJitDescriptorHashAggregateProtocol(const PhysicalHashAggregate &aggregate) {
	JitRegionAggregateProtocol result;
	result.present = true;
	result.kind = JitRegionAggregateOperatorKind::HASH;
	result.native_grouped_state_contract = BuildJitDescriptorNativeGroupedStateContract(result.kind);
	result.native_hash_lookup_contract =
	    BuildJitDescriptorNativeOperatorContract("hash-aggregate-native-lookup", "hash-aggregate-lookup-boundary");
	auto &aggregate_data = aggregate.grouped_aggregate_data;
	AddJitDescriptorAggregateProtocolCommon(result, aggregate_data.aggregates);
	result.group_count = aggregate_data.GroupCount();
	result.group_types = aggregate_data.group_types;
	result.aggregate_filter_count = aggregate_data.filter_count;
	result.payload_type_count = aggregate_data.payload_types.size();
	result.payload_types = aggregate_data.payload_types;
	result.grouping_set_count = aggregate.grouping_sets.size();
	result.grouping_function_count = SumJitDescriptorGroupingFunctionCount(aggregate_data.GetGroupingFunctions());
	result.radix_table_count = aggregate.groupings.size();
	if (aggregate.distinct_collection_info) {
		result.distinct_aggregate_count = aggregate.distinct_collection_info->Indices().size();
		result.distinct_table_count = aggregate.distinct_collection_info->table_count;
		result.distinct_child_count = aggregate.distinct_collection_info->total_child_count;
	}
	result.input_group_type_count = aggregate.input_group_types.size();
	result.input_group_types = aggregate.input_group_types;
	result.non_distinct_filter_count = aggregate.non_distinct_filter.size();
	result.distinct_filter_count = aggregate.distinct_filter.size();
	AddJitDescriptorHashAggregateGroupedStateLayout(result, aggregate);
	MarkJitDescriptorHashAggregateGroupedStateContract(result);
	MarkJitDescriptorHashAggregateDistinctStateBoundary(result);
	if (result.native_grouped_state_contract.status == JitRegionStateContractStatus::READY) {
		MarkJitDescriptorNativeOperatorContractReady(result.native_hash_lookup_contract);
	}
	return result;
}

static string BuildJitDescriptorPerfectHashAggregateBoundaryReason(const PhysicalPerfectHashAggregate &aggregate,
                                                                   const string &marker) {
	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(aggregate.type);
	result += ";aggregate_operator_kind=perfect_hash";
	result += ";group_count=" + std::to_string(aggregate.groups.size());
	result += ";group_types=" + BuildJitDescriptorLogicalTypeList(aggregate.group_types);
	result += ";aggregate_count=" + std::to_string(aggregate.aggregates.size());
	result += ";aggregate_functions=" + BuildJitDescriptorAggregateFunctionList(aggregate.aggregates);
	result += ";aggregate_return_types=" + BuildJitDescriptorAggregateReturnTypeList(aggregate.aggregates);
	result += ";aggregate_child_counts=" + BuildJitDescriptorAggregateChildCountList(aggregate.aggregates);
	result += ";aggregate_types=" + BuildJitDescriptorAggregateTypeList(aggregate.aggregates);
	result += ";aggregate_filter_count=" + std::to_string(aggregate.filter_indexes.size());
	result += ";aggregate_order_count=" +
	          std::to_string(CountJitDescriptorAggregateOrderModifiers(aggregate.aggregates));
	result += ";payload_type_count=" + std::to_string(aggregate.payload_types.size());
	result += ";payload_types=" + BuildJitDescriptorLogicalTypeList(aggregate.payload_types);
	result += ";grouping_set_count=0";
	result += ";grouping_function_count=0";
	result += ";radix_table_count=0";
	result += ";distinct_aggregate_count=" + std::to_string(CountJitDescriptorDistinctAggregates(aggregate.aggregates));
	result += ";distinct_table_count=0";
	result += ";distinct_child_count=0";
	result += ";input_group_type_count=0";
	result += ";input_group_types=[]";
	result += ";non_distinct_filter_count=0";
	result += ";distinct_filter_count=0";
	result += ";perfect_required_bits_count=" + std::to_string(aggregate.required_bits.size());
	result += ";perfect_required_bits_total=" + std::to_string(SumJitDescriptorIdxVector(aggregate.required_bits));
	result += ";perfect_required_bits=" + BuildJitDescriptorIdxList(aggregate.required_bits);
	result += ";perfect_group_minima_count=" + std::to_string(aggregate.group_minima.size());
	auto protocol = BuildJitDescriptorPerfectHashAggregateProtocol(aggregate);
	AppendJitDescriptorNativeOperatorReason(result, protocol.native_hash_lookup_contract,
	                                        "native_hash_aggregate_lookup");
	return result;
}

static JitRegionAggregateProtocol
BuildJitDescriptorPerfectHashAggregateProtocol(const PhysicalPerfectHashAggregate &aggregate) {
	JitRegionAggregateProtocol result;
	result.present = true;
	result.kind = JitRegionAggregateOperatorKind::PERFECT_HASH;
	result.native_grouped_state_contract = BuildJitDescriptorNativeGroupedStateContract(result.kind);
	result.native_hash_lookup_contract = BuildJitDescriptorNativeOperatorContract(
	    "perfect-hash-aggregate-native-lookup", "perfect-hash-aggregate-lookup-boundary");
	AddJitDescriptorAggregateProtocolCommon(result, aggregate.aggregates);
	result.group_count = aggregate.groups.size();
	result.group_types = aggregate.group_types;
	result.aggregate_filter_count = aggregate.filter_indexes.size();
	result.payload_type_count = aggregate.payload_types.size();
	result.payload_types = aggregate.payload_types;
	result.perfect_required_bits_count = aggregate.required_bits.size();
	result.perfect_required_bits_total = SumJitDescriptorIdxVector(aggregate.required_bits);
	result.perfect_required_bits = aggregate.required_bits;
	result.perfect_group_minima_count = aggregate.group_minima.size();
	result.perfect_group_minima = aggregate.group_minima;
	AddJitDescriptorPerfectHashAggregateGroupedStateLayout(result, aggregate);
	MarkJitDescriptorPerfectHashAggregateGroupedStateContract(result);
	if (result.native_grouped_state_contract.status == JitRegionStateContractStatus::READY) {
		MarkJitDescriptorNativeOperatorContractReady(result.native_hash_lookup_contract);
	}
	return result;
}

static string BuildJitDescriptorUngroupedAggregateBoundaryReason(const PhysicalUngroupedAggregate &aggregate,
                                                                 const string &marker) {
	idx_t distinct_aggregate_count = 0;
	idx_t distinct_table_count = 0;
	idx_t distinct_child_count = 0;
	if (aggregate.distinct_collection_info) {
		distinct_aggregate_count = aggregate.distinct_collection_info->Indices().size();
		distinct_table_count = aggregate.distinct_collection_info->table_count;
		distinct_child_count = aggregate.distinct_collection_info->total_child_count;
	}

	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(aggregate.type);
	result += ";aggregate_operator_kind=ungrouped";
	result += ";group_count=0";
	result += ";group_types=[]";
	result += ";aggregate_count=" + std::to_string(aggregate.aggregates.size());
	result += ";aggregate_functions=" + BuildJitDescriptorAggregateFunctionList(aggregate.aggregates);
	result += ";aggregate_return_types=" + BuildJitDescriptorAggregateReturnTypeList(aggregate.aggregates);
	result += ";aggregate_child_counts=" + BuildJitDescriptorAggregateChildCountList(aggregate.aggregates);
	result += ";aggregate_types=" + BuildJitDescriptorAggregateTypeList(aggregate.aggregates);
	result += ";aggregate_filter_count=0";
	result += ";aggregate_order_count=" + std::to_string(CountJitDescriptorAggregateOrderModifiers(aggregate.aggregates));
	result += ";payload_type_count=0";
	result += ";payload_types=[]";
	result += ";grouping_set_count=0";
	result += ";grouping_function_count=0";
	result += ";radix_table_count=0";
	result += ";distinct_aggregate_count=" + std::to_string(distinct_aggregate_count);
	result += ";distinct_table_count=" + std::to_string(distinct_table_count);
	result += ";distinct_child_count=" + std::to_string(distinct_child_count);
	result += ";input_group_type_count=0";
	result += ";input_group_types=[]";
	result += ";non_distinct_filter_count=0";
	result += ";distinct_filter_count=0";
	return result;
}

static JitRegionAggregateProtocol
BuildJitDescriptorUngroupedAggregateProtocol(const PhysicalUngroupedAggregate &aggregate) {
	JitRegionAggregateProtocol result;
	result.present = true;
	result.kind = JitRegionAggregateOperatorKind::UNGROUPED;
	AddJitDescriptorAggregateProtocolCommon(result, aggregate.aggregates);
	if (aggregate.distinct_collection_info) {
		result.distinct_aggregate_count = aggregate.distinct_collection_info->Indices().size();
		result.distinct_table_count = aggregate.distinct_collection_info->table_count;
		result.distinct_child_count = aggregate.distinct_collection_info->total_child_count;
	}
	return result;
}

static JitRegionTableScanProtocol BuildJitDescriptorTableScanProtocol(const PhysicalTableScan &scan) {
	JitRegionTableScanProtocol result;
	result.present = true;
	result.function_name = StringUtil::Lower(scan.function.name.GetIdentifierName());
	result.output_column_count = scan.GetTypes().size();
	result.returned_column_count = scan.returned_types.size();
	result.column_id_count = scan.column_ids.size();
	result.projected_column_count =
	    scan.function.projection_pushdown ? (scan.function.filter_prune ? scan.projection_ids.size()
	                                                                    : scan.column_ids.size())
	                                      : scan.GetTypes().size();
	result.column_ids = BuildJitDescriptorColumnIndexList(scan.column_ids);
	result.projection_ids = scan.projection_ids;
	result.source_prefix_input_column_count = scan.column_ids.size();
	result.source_prefix_input_types = BuildJitDescriptorTableScanSourceInputTypes(scan);
	result.source_prefix_output_projection_map = BuildJitDescriptorTableScanOutputProjectionMap(scan);
	result.source_prefix_filter_column_map = BuildJitDescriptorTableScanFilterColumnMap(scan);
	result.source_prefix_requires_unfiltered_input = !result.source_prefix_filter_column_map.empty();
	result.source_prefix_filter_prune_required =
	    result.source_prefix_requires_unfiltered_input && scan.function.filter_prune && !scan.projection_ids.empty();
	result.source_prefix_filter_takeover_supported =
	    result.source_prefix_requires_unfiltered_input && scan.function.filter_pushdown;
	result.projection_pushdown = scan.function.projection_pushdown;
	result.filter_pushdown = scan.function.filter_pushdown;
	result.filter_prune = scan.function.filter_prune;
	result.dynamic_filters = scan.dynamic_filters && scan.dynamic_filters->HasFilters();
	result.in_out_function = static_cast<bool>(scan.function.in_out_function);
	result.filter_count = scan.table_filters ? scan.table_filters->FilterCount() : 0;
	return result;
}

static string BuildJitDescriptorTableScanSourceBoundaryReason(const PhysicalTableScan &scan,
                                                              JitRegionSourceExecutionKind execution) {
	auto protocol = BuildJitDescriptorTableScanProtocol(scan);
	string result = execution == JitRegionSourceExecutionKind::NATIVE_SOURCE
	                    ? "DuckDB native table scan source protocol"
	                    : "DuckDB table scan source boundary";
	if (execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		result += ";source-fusion-gap:requires-native-source;source_execution=duckdb-source-boundary";
	}
	result += ";function=" + protocol.function_name;
	result += ";output_columns=" + std::to_string(protocol.output_column_count);
	result += ";returned_columns=" + std::to_string(protocol.returned_column_count);
	result += ";column_ids=" + std::to_string(protocol.column_id_count);
	result += ";projection_pushdown=" + JitDescriptorBool(protocol.projection_pushdown);
	result += ";projected_columns=" + std::to_string(protocol.projected_column_count);
	result += ";source_prefix_input_columns=" + std::to_string(protocol.source_prefix_input_column_count);
	result += ";source_prefix_input_types=" + BuildJitDescriptorLogicalTypeList(protocol.source_prefix_input_types);
	result += ";source_prefix_output_projection_map=" + BuildJitDescriptorIdxList(protocol.source_prefix_output_projection_map);
	result += ";source_prefix_filter_column_map=" + BuildJitDescriptorIdxList(protocol.source_prefix_filter_column_map);
	result += ";source_prefix_requires_unfiltered_input=" +
	          JitDescriptorBool(protocol.source_prefix_requires_unfiltered_input);
	result += ";source_prefix_filter_prune_required=" +
	          JitDescriptorBool(protocol.source_prefix_filter_prune_required);
	result += ";source_prefix_filter_takeover_supported=" +
	          JitDescriptorBool(protocol.source_prefix_filter_takeover_supported);
	result += ";filter_pushdown=" + JitDescriptorBool(protocol.filter_pushdown);
	result += ";filter_prune=" + JitDescriptorBool(protocol.filter_prune);
	result += ";filter_count=" + std::to_string(protocol.filter_count);
	result += ";dynamic_filters=" + JitDescriptorBool(protocol.dynamic_filters);
	result += ";in_out_function=" + JitDescriptorBool(protocol.in_out_function);
	return result;
}

static bool TryReadJitDescriptorAggregateReferenceInput(const BoundAggregateExpression &aggregate,
                                                        JitRegionAggregateInput &result) {
	auto &children = aggregate.GetChildren();
	if (children.empty()) {
		result.payload_index = 0;
		result.supported_payload_references = true;
		return true;
	}

	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		result.child_types.push_back(child.GetReturnType());
		if (child.GetExpressionClass() != ExpressionClass::BOUND_REF) {
			result.reason = "aggregate child is not a payload reference;child_index=" + std::to_string(child_idx);
			return false;
		}
		auto &reference = child.Cast<BoundReferenceExpression>();
		auto reference_index = reference.Index();
		result.child_indices.push_back(reference_index);
		if (child_idx == 0) {
			result.payload_index = reference_index;
		} else if (reference_index != result.payload_index + child_idx) {
			result.reason = "aggregate payload references are not contiguous;child_index=" + std::to_string(child_idx);
			return false;
		}
	}
	result.supported_payload_references = true;
	return true;
}

static void AppendJitDescriptorAggregateReason(JitRegionAggregateInput &aggregate, string reason) {
	if (reason.empty()) {
		return;
	}
	if (!aggregate.reason.empty()) {
		aggregate.reason += ";";
	}
	aggregate.reason += std::move(reason);
}

static JitAggregateUpdateKind GetJitDescriptorAggregateUpdateKind(const JitRegionAggregateInput &aggregate) {
	if (aggregate.function_name == "count_star" && aggregate.child_count == 0 &&
	    aggregate.state_type == LogicalType::BIGINT && !aggregate.state_is_optional) {
		return JitAggregateUpdateKind::COUNT_STAR;
	}
	if (aggregate.function_name == "count" && aggregate.child_count == 1 &&
	    aggregate.state_type == LogicalType::BIGINT && !aggregate.state_is_optional) {
		return JitAggregateUpdateKind::COUNT;
	}
	if ((aggregate.function_name == "sum" || aggregate.function_name == "sum_no_overflow") &&
	    aggregate.child_count == 1 && aggregate.state_is_optional) {
		return JitAggregateUpdateKind::SUM;
	}
	return JitAggregateUpdateKind::NONE;
}

static void AddJitDescriptorAggregateNativeUpdate(const BoundAggregateFunction &function,
                                                  JitRegionAggregateInput &result) {
	if (!function.HasGetStateTypeCallback()) {
		AppendJitDescriptorAggregateReason(result, "aggregate function exports no native state layout");
		return;
	}
	auto state_layout = function.GetStateType();
	result.state_type = state_layout.type;
	result.state_size = state_layout.total_state_size;
	result.state_is_optional = state_layout.field.is_optional;
	result.state_value_offset = state_layout.field.field_offset;
	if (result.state_is_optional) {
		result.state_is_set_offset =
		    result.state_value_offset + AggregateStateField::GetPhysicalSize(result.state_type);
	}
	result.native_update = GetJitDescriptorAggregateUpdateKind(result);
	if (result.native_update == JitAggregateUpdateKind::NONE) {
		AppendJitDescriptorAggregateReason(result, "aggregate native update kind is unsupported");
	}
}

static JitRegionAggregateInput BuildJitDescriptorAggregateInput(idx_t aggregate_idx,
                                                                const unique_ptr<Expression> &aggregate_expression) {
	JitRegionAggregateInput result;
	result.aggregate_index = aggregate_idx;
	result.return_type = aggregate_expression->GetReturnType();
	if (aggregate_expression->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		result.function_name = "non_aggregate";
		result.reason = "aggregate expression is not bound aggregate";
		return result;
	}

	auto &aggregate = aggregate_expression->Cast<BoundAggregateExpression>();
	auto &function = aggregate.Function();
	result.function_name = StringUtil::Lower(function.GetName().GetIdentifierName());
	result.child_count = aggregate.GetChildren().size();
	result.distinct = aggregate.IsDistinct();
	result.has_filter = static_cast<bool>(aggregate.GetFilter());
	result.has_order_bys = static_cast<bool>(aggregate.GetOrderBys());
	result.order_dependent = function.GetOrderDependent() == AggregateOrderDependent::ORDER_DEPENDENT;
	result.has_state_update = function.HasStateUpdateCallback() || function.HasStateClusterUpdateCallback();

	if (result.distinct) {
		result.reason = "distinct aggregate update requires DuckDB distinct sink protocol";
	} else if (result.has_filter) {
		result.reason = "aggregate filter requires per-aggregate filtered payload protocol";
	} else if (result.has_order_bys) {
		result.reason = "ordered aggregate requires DuckDB sorted aggregate protocol";
	} else if (!result.has_state_update) {
		result.reason = "aggregate function has no state update callback";
	} else {
		if (TryReadJitDescriptorAggregateReferenceInput(aggregate, result)) {
			AddJitDescriptorAggregateNativeUpdate(function, result);
		}
	}
	return result;
}

static vector<JitRegionAggregateInput>
BuildJitDescriptorUngroupedAggregateInputs(const PhysicalUngroupedAggregate &op) {
	vector<JitRegionAggregateInput> result;
	result.reserve(op.aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < op.aggregates.size(); aggregate_idx++) {
		result.push_back(BuildJitDescriptorAggregateInput(aggregate_idx, op.aggregates[aggregate_idx]));
	}
	return result;
}

static vector<JitRegionAggregateInput> BuildJitDescriptorHashAggregateInputs(const PhysicalHashAggregate &op) {
	vector<JitRegionAggregateInput> result;
	auto &aggregates = op.grouped_aggregate_data.aggregates;
	result.reserve(aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		result.push_back(BuildJitDescriptorAggregateInput(aggregate_idx, aggregates[aggregate_idx]));
	}
	return result;
}

static vector<JitRegionAggregateInput> BuildJitDescriptorPerfectHashAggregateInputs(
    const PhysicalPerfectHashAggregate &op) {
	vector<JitRegionAggregateInput> result;
	result.reserve(op.aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < op.aggregates.size(); aggregate_idx++) {
		result.push_back(BuildJitDescriptorAggregateInput(aggregate_idx, op.aggregates[aggregate_idx]));
	}
	return result;
}

static JitRegionHashJoinKeyInput BuildJitDescriptorHashJoinBuildKeyInput(const PhysicalHashJoin &op, idx_t key_idx) {
	JitRegionHashJoinKeyInput result;
	result.key_index = key_idx;
	result.type = op.condition_types[key_idx];
	auto &key_expression = op.conditions[key_idx].GetRHS();
	if (key_expression.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		result.reason = "hash join build key is not a payload reference";
		return result;
	}
	auto &bound_ref = key_expression.Cast<BoundReferenceExpression>();
	result.input_index = bound_ref.Index();
	result.supported_reference = true;
	return result;
}

static JitRegionHashJoinKeyInput BuildJitDescriptorHashJoinProbeKeyInput(const PhysicalHashJoin &op, idx_t key_idx) {
	JitRegionHashJoinKeyInput result;
	result.key_index = key_idx;
	result.type = op.condition_types[key_idx];
	auto &key_expression = op.conditions[key_idx].GetLHS();
	if (key_expression.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		result.reason = "hash join probe key is not a payload reference";
		return result;
	}
	auto &bound_ref = key_expression.Cast<BoundReferenceExpression>();
	result.input_index = bound_ref.Index();
	result.supported_reference = true;
	return result;
}

static vector<JitRegionHashJoinKeyInput> BuildJitDescriptorHashJoinBuildKeyInputs(const PhysicalHashJoin &op) {
	vector<JitRegionHashJoinKeyInput> result;
	result.reserve(op.conditions.size());
	for (idx_t key_idx = 0; key_idx < op.conditions.size(); key_idx++) {
		result.push_back(BuildJitDescriptorHashJoinBuildKeyInput(op, key_idx));
	}
	return result;
}

static vector<JitRegionHashJoinKeyInput> BuildJitDescriptorHashJoinProbeKeyInputs(const PhysicalHashJoin &op) {
	vector<JitRegionHashJoinKeyInput> result;
	result.reserve(op.conditions.size());
	for (idx_t key_idx = 0; key_idx < op.conditions.size(); key_idx++) {
		result.push_back(BuildJitDescriptorHashJoinProbeKeyInput(op, key_idx));
	}
	return result;
}

static JitRegionGroupInput BuildJitDescriptorGroupInput(const vector<unique_ptr<Expression>> &groups,
                                                        const vector<LogicalType> &group_types, const string &operator_name,
                                                        idx_t group_idx) {
	JitRegionGroupInput result;
	result.group_index = group_idx;
	result.type = group_types[group_idx];
	auto &group_expression = *groups[group_idx];
	if (group_expression.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		result.reason = operator_name + " group is not a payload reference";
		return result;
	}
	auto &bound_ref = group_expression.Cast<BoundReferenceExpression>();
	result.input_index = bound_ref.Index();
	result.supported_reference = true;
	return result;
}

static vector<JitRegionGroupInput> BuildJitDescriptorGroupInputs(const PhysicalHashAggregate &op) {
	vector<JitRegionGroupInput> result;
	auto group_count = op.grouped_aggregate_data.GroupCount();
	result.reserve(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		result.push_back(BuildJitDescriptorGroupInput(op.grouped_aggregate_data.groups,
		                                              op.grouped_aggregate_data.group_types, "hash aggregate",
		                                              group_idx));
	}
	return result;
}

static vector<JitRegionGroupInput> BuildJitDescriptorGroupInputs(const PhysicalPerfectHashAggregate &op) {
	vector<JitRegionGroupInput> result;
	result.reserve(op.groups.size());
	for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
		result.push_back(BuildJitDescriptorGroupInput(op.groups, op.group_types, "perfect hash aggregate", group_idx));
	}
	return result;
}

static void AddJitDescriptorTableScanSourceFilters(const PhysicalTableScan &scan,
                                                   JitOperatorSourceDescriptor &source) {
	if (!scan.table_filters) {
		return;
	}
	idx_t filter_index = 0;
	for (auto &entry : *scan.table_filters) {
		JitOperatorSourceFilterDescriptor filter;
		filter.filter_index = filter_index++;
		filter.scan_column_index = entry.GetIndex().GetIndex();
		filter.table_column_index = DConstants::INVALID_INDEX;
		if (filter.scan_column_index < scan.column_ids.size()) {
			auto &column_index = scan.column_ids[filter.scan_column_index];
			if (column_index.HasPrimaryIndex()) {
				filter.table_column_index = column_index.GetPrimaryIndex();
			}
		}
		auto &table_filter = entry.Filter();
		if (table_filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
			filter.reason = "table filter is not an expression filter";
			source.filters.push_back(std::move(filter));
			continue;
		}
		auto &expression_filter = table_filter.Cast<ExpressionFilter>();
		filter.expression = optional_ptr<const Expression>(*expression_filter.expr);
		source.filters.push_back(std::move(filter));
	}
}

JitOperatorDescriptor PhysicalTableScan::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	result.has_source = true;
	auto function_name = StringUtil::Lower(function.name.GetIdentifierName());
	result.source.kind =
	    function_name == "seq_scan" ? JitRegionSourceKind::DUCKDB_TABLE_SCAN : JitRegionSourceKind::TABLE_FUNCTION_SCAN;
	result.source.execution = IsJitDescriptorNativeDuckTableScanSupported(*this)
	                              ? JitRegionSourceExecutionKind::NATIVE_SOURCE
	                              : JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	result.source_boundary_reason = BuildJitDescriptorTableScanSourceBoundaryReason(*this, result.source.execution);
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	result.source.function_name = function_name;
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = returned_types.size();
	result.source.column_ids = BuildJitDescriptorColumnIndexList(column_ids);
	result.source.projection_ids = projection_ids;
	result.source.projection_pushdown = function.projection_pushdown;
	result.source.filter_pushdown = function.filter_pushdown;
	result.source.filter_prune = function.filter_prune;
	result.source.dynamic_filters = dynamic_filters && dynamic_filters->HasFilters();
	result.source.in_out_function = static_cast<bool>(function.in_out_function);
	result.source.table_scan_protocol = BuildJitDescriptorTableScanProtocol(*this);
	AddJitDescriptorTableScanSourceFilters(*this, result.source);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalColumnDataScan::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	if (type != PhysicalOperatorType::CTE_SCAN && type != PhysicalOperatorType::COLUMN_DATA_SCAN) {
		return FinalizeJitOperatorDescriptor(std::move(result));
	}
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = JitRegionSourceExecutionKind::NATIVE_SOURCE;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.function_name = StringUtil::Lower(PhysicalOperatorToString(type));
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	result.source_boundary_reason = "DuckDB column data native source protocol";
	result.source_boundary_reason += ";operator=" + PhysicalOperatorToString(type);
	result.source_boundary_reason += ";function=" + result.source.function_name;
	result.source_boundary_reason += ";output_columns=" + std::to_string(result.source.output_column_count);
	result.source_boundary_reason += ";returned_columns=" + std::to_string(result.source.returned_column_count);
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalHashJoin::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	auto protocol = BuildJitDescriptorHashJoinProtocol(*this);
	auto state_scan_contract =
	    BuildJitDescriptorNativeStateScanContract("hash-join-native-state-scan", "none");
	if (protocol.source_produces_rows) {
		MarkJitDescriptorNativeStateScanContractReady(state_scan_contract);
	} else {
		MarkJitDescriptorNativeStateScanContractBlocked(state_scan_contract, HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER);
	}
	result.source_boundary_reason =
	    BuildJitDescriptorHashJoinBoundaryReason(
	        *this, protocol.source_produces_rows ? "DuckDB hash join native state scan protocol"
	                                             : "DuckDB hash join state scan source does not produce rows");
	result.has_operator = true;
	result.operator_info.kind = JitRegionOperatorKind::HASH_JOIN_PROBE;
	result.operator_info.reason =
	    BuildJitDescriptorHashJoinBoundaryReason(*this, "DuckDB hash join probe operator protocol boundary");
	result.operator_info.fields = BuildJitDescriptorProtocolFields(result.operator_info.reason);
	result.operator_info.hash_join_protocol = protocol;
	result.operator_info.hash_join_keys = BuildJitDescriptorHashJoinProbeKeyInputs(*this);
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = protocol.source_produces_rows ? JitRegionSourceExecutionKind::NATIVE_SOURCE
	                                                        : JitRegionSourceExecutionKind::EXECUTOR_FALLBACK;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	if (!protocol.source_produces_rows) {
		result.source.native_source_contract.blocker = HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER;
	}
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	AppendJitDescriptorNativeStateScanReason(result.source_boundary_reason, result.source.native_state_scan_contract);
	result.source.function_name = "hash_join_probe";
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	result.source.hash_join_protocol = protocol;
	result.source.hash_join_keys = BuildJitDescriptorHashJoinProbeKeyInputs(*this);
	result.has_sink = true;
	result.sink.kind = JitRegionSinkKind::HASH_JOIN_BUILD;
	result.sink.reason = BuildJitDescriptorHashJoinBoundaryReason(*this, "DuckDB hash join build sink protocol");
	result.sink.fields = BuildJitDescriptorProtocolFields(result.sink.reason);
	result.sink.hash_join_protocol = std::move(protocol);
	result.sink.hash_join_keys = BuildJitDescriptorHashJoinBuildKeyInputs(*this);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalOrder::GetJitOperatorDescriptor() const {
	auto result = BuildJitDescriptorSortStateSource(type, GetTypes().size(), orders.size(), projections.size(),
	                                                "order_by_scan", "order-by-native-state-scan",
	                                                "DuckDB order by native state scan protocol");
	result.source_boundary_reason += ";is_index_sort=" + JitDescriptorBool(is_index_sort);
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalTopN::GetJitOperatorDescriptor() const {
	auto result = BuildJitDescriptorSortStateSource(type, GetTypes().size(), orders.size(), GetTypes().size(),
	                                                "top_n_scan", "top-n-native-state-scan",
	                                                "DuckDB top-n native state scan protocol");
	result.source_boundary_reason += ";limit=" + std::to_string(limit);
	result.source_boundary_reason += ";offset=" + std::to_string(offset);
	result.source_boundary_reason += ";dynamic_filter=" + JitDescriptorBool(static_cast<bool>(dynamic_filter));
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalHashAggregate::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	auto protocol = BuildJitDescriptorHashAggregateProtocol(*this);
	auto state_scan_contract =
	    BuildJitDescriptorNativeStateScanContract("hash-aggregate-native-state-scan", "none");
	MarkJitDescriptorNativeStateScanContractReady(state_scan_contract);
	result.source_boundary_reason =
	    BuildJitDescriptorHashAggregateBoundaryReason(*this, "DuckDB hash aggregate native state scan protocol");
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = JitRegionSourceExecutionKind::NATIVE_SOURCE;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	AppendJitDescriptorNativeStateScanReason(result.source_boundary_reason, result.source.native_state_scan_contract);
	AppendJitDescriptorNativeGroupedStateReason(result.source_boundary_reason,
	                                           protocol.native_grouped_state_contract);
	AppendJitDescriptorGroupedStateLayoutReason(result.source_boundary_reason, protocol);
	result.source.function_name = "hash_aggregate_scan";
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	result.source.aggregate_protocol = protocol;
	result.source.aggregates = BuildJitDescriptorHashAggregateInputs(*this);
	result.source.groups = BuildJitDescriptorGroupInputs(*this);
	result.has_sink = true;
	result.sink.kind = JitRegionSinkKind::HASH_AGGREGATE_UPDATE;
	result.sink.reason =
	    BuildJitDescriptorHashAggregateBoundaryReason(*this, "DuckDB hash aggregate sink update protocol");
	AppendJitDescriptorNativeGroupedStateReason(result.sink.reason, protocol.native_grouped_state_contract);
	AppendJitDescriptorGroupedStateLayoutReason(result.sink.reason, protocol);
	result.sink.fields = BuildJitDescriptorProtocolFields(result.sink.reason);
	result.sink.aggregate_protocol = std::move(protocol);
	result.sink.aggregates = BuildJitDescriptorHashAggregateInputs(*this);
	result.sink.groups = BuildJitDescriptorGroupInputs(*this);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalPerfectHashAggregate::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	auto protocol = BuildJitDescriptorPerfectHashAggregateProtocol(*this);
	auto state_scan_contract = BuildJitDescriptorNativeStateScanContract(
	    "perfect-hash-aggregate-native-state-scan", "aggregate-state-scan-protocol-boundary");
	MarkJitDescriptorPerfectHashAggregateStateScanContract(state_scan_contract, protocol);
	result.source_boundary_reason = BuildJitDescriptorPerfectHashAggregateBoundaryReason(
	    *this, state_scan_contract.status == JitRegionStateContractStatus::READY
	               ? "DuckDB perfect hash aggregate native state scan protocol"
	               : "DuckDB aggregate source state protocol missing");
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = state_scan_contract.status == JitRegionStateContractStatus::READY
	                              ? JitRegionSourceExecutionKind::NATIVE_SOURCE
	                              : JitRegionSourceExecutionKind::EXECUTOR_FALLBACK;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	AppendJitDescriptorNativeStateScanReason(result.source_boundary_reason, result.source.native_state_scan_contract);
	AppendJitDescriptorNativeGroupedStateReason(result.source_boundary_reason,
	                                           protocol.native_grouped_state_contract);
	AppendJitDescriptorGroupedStateLayoutReason(result.source_boundary_reason, protocol);
	result.source.function_name = "perfect_hash_aggregate_scan";
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	result.source.aggregate_protocol = protocol;
	result.source.aggregates = BuildJitDescriptorPerfectHashAggregateInputs(*this);
	result.source.groups = BuildJitDescriptorGroupInputs(*this);
	result.has_sink = true;
	result.sink.kind = JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
	result.sink.reason =
	    BuildJitDescriptorPerfectHashAggregateBoundaryReason(*this, "DuckDB perfect hash aggregate sink update protocol");
	AppendJitDescriptorNativeGroupedStateReason(result.sink.reason, protocol.native_grouped_state_contract);
	AppendJitDescriptorGroupedStateLayoutReason(result.sink.reason, protocol);
	result.sink.fields = BuildJitDescriptorProtocolFields(result.sink.reason);
	result.sink.aggregate_protocol = std::move(protocol);
	result.sink.aggregates = BuildJitDescriptorPerfectHashAggregateInputs(*this);
	result.sink.groups = BuildJitDescriptorGroupInputs(*this);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

JitOperatorDescriptor PhysicalUngroupedAggregate::GetJitOperatorDescriptor() const {
	JitOperatorDescriptor result;
	auto protocol = BuildJitDescriptorUngroupedAggregateProtocol(*this);
	auto state_scan_contract = BuildJitDescriptorNativeStateScanContract(
	    "ungrouped-aggregate-native-state-scan", "aggregate-state-scan-protocol-boundary");
	MarkJitDescriptorUngroupedAggregateStateScanContract(state_scan_contract, protocol);
	result.source_boundary_reason = BuildJitDescriptorUngroupedAggregateBoundaryReason(
	    *this, state_scan_contract.status == JitRegionStateContractStatus::READY
	               ? "DuckDB ungrouped aggregate native state scan protocol"
	               : "DuckDB aggregate source state protocol missing");
	result.has_source = true;
	result.source.kind = JitRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = state_scan_contract.status == JitRegionStateContractStatus::READY
	                              ? JitRegionSourceExecutionKind::NATIVE_SOURCE
	                              : JitRegionSourceExecutionKind::EXECUTOR_FALLBACK;
	result.source.native_source_contract =
	    BuildJitRegionNativeSourceContract(result.source.kind, result.source.execution);
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	AppendJitDescriptorNativeStateScanReason(result.source_boundary_reason, result.source.native_state_scan_contract);
	result.source.function_name = "ungrouped_aggregate_scan";
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	result.source.reason = result.source_boundary_reason;
	result.source.fields = BuildJitDescriptorProtocolFields(result.source.reason);
	result.source.aggregate_protocol = protocol;
	result.source.aggregates = BuildJitDescriptorUngroupedAggregateInputs(*this);
	result.has_sink = true;
	result.sink.kind = JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	result.sink.reason =
	    BuildJitDescriptorUngroupedAggregateBoundaryReason(*this, "DuckDB ungrouped aggregate payload update protocol");
	result.sink.fields = BuildJitDescriptorProtocolFields(result.sink.reason);
	result.sink.aggregate_protocol = std::move(protocol);
	result.sink.aggregates = BuildJitDescriptorUngroupedAggregateInputs(*this);
	return FinalizeJitOperatorDescriptor(std::move(result));
}

} // namespace duckdb

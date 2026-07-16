#include "duckdb/execution/execution_contract.hpp"

#include "duckdb/common/column_index.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include "execution_region_duckdb_type_adapter.hpp"

namespace duckdb {

static constexpr const char *HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER =
    "hash-join-source-does-not-produce-rows-for-join-type";

static string ExecutionContractBool(bool value) {
	return value ? "true" : "false";
}

static string BuildExecutionContractLogicalTypeList(const vector<LogicalType> &types) {
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

static idx_t ExecutionContractSignedIntegerWidth(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return 8;
	case PhysicalType::INT16:
		return 16;
	case PhysicalType::INT32:
		return 32;
	case PhysicalType::INT64:
		return 64;
	default:
		return 0;
	}
}

static bool ExecutionContractIsWideningSignedIntegerCast(const LogicalType &source_type,
                                                         const LogicalType &target_type) {
	const auto source_width = ExecutionContractSignedIntegerWidth(source_type.InternalType());
	const auto target_width = ExecutionContractSignedIntegerWidth(target_type.InternalType());
	return source_width != 0 && target_width != 0 && source_width < target_width;
}

static LogicalType ExecutionContractHashJoinConditionDomainType(const Expression &expression) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_CAST) {
		return expression.GetReturnType();
	}
	auto &cast = expression.Cast<BoundCastExpression>();
	if (cast.IsTryCast() || cast.Child().GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return expression.GetReturnType();
	}
	if (!ExecutionContractIsWideningSignedIntegerCast(cast.Child().GetReturnType(), expression.GetReturnType())) {
		return expression.GetReturnType();
	}
	return cast.Child().GetReturnType();
}

static string BuildExecutionContractIdxList(const vector<idx_t> &values) {
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

static string BuildExecutionContractBoolList(const vector<bool> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += ExecutionContractBool(values[value_idx]);
	}
	result += "]";
	return result;
}

static vector<idx_t> BuildExecutionContractColumnIndexList(const vector<ColumnIndex> &column_indexes) {
	vector<idx_t> result;
	result.reserve(column_indexes.size());
	for (auto &column_index : column_indexes) {
		result.push_back(column_index.HasPrimaryIndex() ? column_index.GetPrimaryIndex() : DConstants::INVALID_INDEX);
	}
	return result;
}

static LogicalType BuildExecutionContractTableScanColumnType(const PhysicalTableScan &scan,
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
			throw InternalException("Virtual column not found while building execution table scan source layout");
		}
		return entry->second.type;
	}
	if (column_id >= scan.returned_types.size()) {
		throw InternalException(
		    "Column index %llu is outside returned type count %llu while building execution table scan "
		    "source layout",
		    static_cast<unsigned long long>(column_id), static_cast<unsigned long long>(scan.returned_types.size()));
	}
	return scan.returned_types[column_id];
}

LogicalType GetExecutionRegionTableScanSourceInputType(const PhysicalTableScan &scan, idx_t source_input_idx) {
	if (source_input_idx >= scan.column_ids.size()) {
		throw InternalException("Source input index %llu is outside table scan column count %llu",
		                        static_cast<unsigned long long>(source_input_idx),
		                        static_cast<unsigned long long>(scan.column_ids.size()));
	}
	return BuildExecutionContractTableScanColumnType(scan, scan.column_ids[source_input_idx]);
}

static vector<LogicalType> BuildExecutionContractTableScanSourceInputTypes(const PhysicalTableScan &scan) {
	vector<LogicalType> result;
	result.reserve(scan.column_ids.size());
	for (idx_t source_input_idx = 0; source_input_idx < scan.column_ids.size(); source_input_idx++) {
		result.push_back(GetExecutionRegionTableScanSourceInputType(scan, source_input_idx));
	}
	return result;
}

static vector<bool> BuildExecutionContractTableScanSourceInputNotNull(const PhysicalTableScan &scan) {
	vector<bool> result;
	result.assign(scan.column_ids.size(), false);
	if (StringUtil::Lower(scan.function.name.GetIdentifierName()) != "seq_scan" || !scan.bind_data) {
		return result;
	}
	auto &bind_data = scan.bind_data->Cast<TableScanBindData>();
	for (auto &constraint : bind_data.table.GetConstraints()) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &not_null = constraint->Cast<NotNullConstraint>();
		for (idx_t column_idx = 0; column_idx < scan.column_ids.size(); column_idx++) {
			auto &column_id = scan.column_ids[column_idx];
			if (column_id.HasPrimaryIndex() && column_id.GetPrimaryIndex() == not_null.index.index) {
				result[column_idx] = true;
			}
		}
	}
	return result;
}

static vector<idx_t> BuildExecutionContractTableScanOutputProjectionMap(const PhysicalTableScan &scan) {
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

static ExecutionRegionNativeGroupedStateContract
BuildExecutionContractNativeGroupedStateContract(ExecutionRegionAggregateOperatorKind kind) {
	ExecutionRegionNativeGroupedStateContract result;
	switch (kind) {
	case ExecutionRegionAggregateOperatorKind::HASH:
		result.status = ExecutionRegionStateContractStatus::MISSING;
		result.required_capability = "hash-aggregate-native-grouped-state";
		result.contract_version = "v1";
		result.blocker = "grouped-state-contract-boundary";
		break;
	case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
		result.status = ExecutionRegionStateContractStatus::MISSING;
		result.required_capability = "perfect-hash-aggregate-native-grouped-state";
		result.contract_version = "v1";
		result.blocker = "grouped-state-contract-boundary";
		break;
	default:
		break;
	}
	return result;
}

static ExecutionRegionNativeStateScanContract BuildExecutionContractNativeStateScanContract(string required_capability,
                                                                                            string blocker) {
	ExecutionRegionNativeStateScanContract result;
	result.status = ExecutionRegionStateContractStatus::MISSING;
	result.required_capability = std::move(required_capability);
	result.contract_version = "v1";
	result.blocker = std::move(blocker);
	return result;
}

static ExecutionRegionNativeOperatorContract BuildExecutionContractNativeOperatorContract(string required_capability,
                                                                                          string blocker) {
	ExecutionRegionNativeOperatorContract result;
	result.status = ExecutionRegionStateContractStatus::MISSING;
	result.required_capability = std::move(required_capability);
	result.contract_version = "v1";
	result.blocker = std::move(blocker);
	return result;
}

static void MarkExecutionContractNativeOperatorContractReady(ExecutionRegionNativeOperatorContract &contract) {
	contract.status = ExecutionRegionStateContractStatus::READY;
	contract.blocker.clear();
}

static void MarkExecutionContractNativeOperatorContractBlocked(ExecutionRegionNativeOperatorContract &contract,
                                                               string blocker) {
	contract.status = ExecutionRegionStateContractStatus::BLOCKED;
	contract.blocker = std::move(blocker);
}

static void MarkExecutionContractNativeStateScanContractReady(ExecutionRegionNativeStateScanContract &contract) {
	contract.status = ExecutionRegionStateContractStatus::READY;
	contract.blocker.clear();
}

static void MarkExecutionContractNativeStateScanContractBlocked(ExecutionRegionNativeStateScanContract &contract,
                                                                string blocker) {
	contract.status = ExecutionRegionStateContractStatus::BLOCKED;
	contract.blocker = std::move(blocker);
}

static void AppendExecutionContractNativeStateScanReason(string &reason,
                                                         const ExecutionRegionNativeStateScanContract &contract) {
	if (reason.empty() || contract.status == ExecutionRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";native_state_scan_contract_status=";
	reason += ExecutionRegionStateContractStatusToString(contract.status);
	reason += ";native_state_scan_required_capability=" + contract.required_capability;
	reason += ";native_state_scan_contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		reason += ";native_state_scan_blocker=" + contract.blocker;
	}
}

static void AppendExecutionContractNativeGroupedStateReason(string &reason,
                                                            const ExecutionRegionNativeGroupedStateContract &contract) {
	if (reason.empty() || contract.status == ExecutionRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";native_grouped_state_contract_status=";
	reason += ExecutionRegionStateContractStatusToString(contract.status);
	reason += ";native_grouped_state_required_capability=" + contract.required_capability;
	reason += ";native_grouped_state_contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		reason += ";native_grouped_state_blocker=" + contract.blocker;
	}
}

static void AppendExecutionContractNativeOperatorReason(string &reason,
                                                        const ExecutionRegionNativeOperatorContract &contract,
                                                        const string &prefix) {
	if (reason.empty() || contract.status == ExecutionRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";" + prefix + "_contract_status=";
	reason += ExecutionRegionStateContractStatusToString(contract.status);
	reason += ";" + prefix + "_required_capability=" + contract.required_capability;
	reason += ";" + prefix + "_contract_version=" + contract.contract_version;
	if (!contract.blocker.empty()) {
		reason += ";" + prefix + "_blocker=" + contract.blocker;
	}
}

static void AppendExecutionContractGroupedStateLayoutReason(string &reason,
                                                            const ExecutionRegionAggregateContract &contract) {
	if (reason.empty() || !contract.present ||
	    contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::NONE) {
		return;
	}
	reason += ";grouped_state_layout_ready=" + ExecutionContractBool(contract.grouped_state_layout_ready);
	reason += ";grouped_state_offsets=" + BuildExecutionContractIdxList(contract.grouped_state_offsets);
	reason += ";grouped_state_payload_sizes=" + BuildExecutionContractIdxList(contract.grouped_state_payload_sizes);
}

ExecutionSourceContractCapability GetExecutionSourceContractCapability(const PhysicalTableScan &scan) {
	ExecutionSourceContractCapability result;
	auto function_name = StringUtil::Lower(scan.function.name.GetIdentifierName());
	result.kind = function_name == "seq_scan" ? ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN
	                                          : ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN;
	if (!scan.function.function && !scan.function.in_out_function) {
		return result;
	}
	if (result.kind == ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN) {
		if (!scan.bind_data) {
			return result;
		}
		auto &bind_data = scan.bind_data->Cast<TableScanBindData>();
		if (bind_data.is_index_scan) {
			return result;
		}
		result.uses_storage_scan = true;
		result.supports_source_contract_input_layout = true;
	} else {
		// The generic callback path can expose the source-contract layout only when the function does not
		// independently project its output columns.
		if (scan.function.projection_pushdown) {
			return result;
		}
		result.supports_source_contract_input_layout = true;
	}
	result.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	return result;
}

idx_t GetExecutionRegionTableScanSourceCardinality(const PhysicalTableScan &scan) {
	auto capability = GetExecutionSourceContractCapability(scan);
	if (!capability.uses_storage_scan) {
		return scan.estimated_cardinality;
	}
	auto &bind_data = scan.bind_data->Cast<TableScanBindData>();
	return bind_data.table.GetStorage().GetTotalRows();
}

static string BuildExecutionContractAggregateFunctionList(const vector<unique_ptr<Expression>> &aggregates) {
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

static vector<string> BuildExecutionContractAggregateFunctionVector(const vector<unique_ptr<Expression>> &aggregates) {
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

static string BuildExecutionContractAggregateReturnTypeList(const vector<unique_ptr<Expression>> &aggregates) {
	vector<LogicalType> return_types;
	return_types.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		return_types.push_back(aggregate->GetReturnType());
	}
	return BuildExecutionContractLogicalTypeList(return_types);
}

static vector<LogicalType>
BuildExecutionContractAggregateReturnTypeVector(const vector<unique_ptr<Expression>> &aggregates) {
	vector<LogicalType> result;
	result.reserve(aggregates.size());
	for (auto &aggregate : aggregates) {
		result.push_back(aggregate->GetReturnType());
	}
	return result;
}

static string BuildExecutionContractAggregateChildCountList(const vector<unique_ptr<Expression>> &aggregates) {
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

static vector<idx_t> BuildExecutionContractAggregateChildCountVector(const vector<unique_ptr<Expression>> &aggregates) {
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

static string BuildExecutionContractAggregateTypeList(const vector<unique_ptr<Expression>> &aggregates) {
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

static vector<string> BuildExecutionContractAggregateTypeVector(const vector<unique_ptr<Expression>> &aggregates) {
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

static idx_t CountExecutionContractAggregateOrderModifiers(const vector<unique_ptr<Expression>> &aggregates) {
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

static idx_t CountExecutionContractDistinctAggregates(const vector<unique_ptr<Expression>> &aggregates) {
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

static idx_t SumExecutionContractGroupingFunctionCount(const vector<vector<ProjectionIndex>> &grouping_functions) {
	idx_t result = 0;
	for (auto &grouping_function_set : grouping_functions) {
		result += grouping_function_set.size();
	}
	return result;
}

static idx_t SumExecutionContractIdxVector(const vector<idx_t> &values) {
	idx_t result = 0;
	for (auto value : values) {
		result += value;
	}
	return result;
}

static void AddExecutionContractField(vector<ExecutionRegionContractField> &fields, string name, string value) {
	ExecutionRegionContractField field;
	field.name = std::move(name);
	field.value = std::move(value);
	fields.push_back(std::move(field));
}

vector<ExecutionRegionContractField> BuildExecutionContractFields(const string &reason) {
	vector<ExecutionRegionContractField> result;
	auto segments = StringUtil::Split(reason, ";");
	if (!segments.empty() && !segments[0].empty()) {
		AddExecutionContractField(result, "marker", segments[0]);
	}
	for (idx_t segment_idx = 1; segment_idx < segments.size(); segment_idx++) {
		auto &segment = segments[segment_idx];
		auto equals = segment.find('=');
		if (equals == string::npos || equals == 0) {
			continue;
		}
		AddExecutionContractField(result, segment.substr(0, equals), segment.substr(equals + 1));
	}
	return result;
}

static ExecutionRegionStageExecutionKind
ExecutionCompiledSourceExecution(ExecutionRegionSourceExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT:
		return ExecutionRegionStageExecutionKind::NATIVE_CONTRACT;
	case ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY:
		return ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
	default:
		return ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	}
}

static ExecutionCompiledStageContract BuildExecutionCompiledSourceStage(const ExecutionSourceContract &source) {
	ExecutionCompiledStageContract stage;
	stage.stage = ExecutionRegionStageKind::SOURCE;
	stage.operation = source.native_state_scan_contract.status != ExecutionRegionStateContractStatus::NONE
	                      ? ExecutionCompiledContractKind::STATE_SCAN_CURSOR
	                      : ExecutionCompiledContractKind::SCAN_CURSOR;
	stage.execution = ExecutionCompiledSourceExecution(source.execution);
	stage.drain = source.native_state_scan_contract.status != ExecutionRegionStateContractStatus::NONE
	                  ? ExecutionCompiledDrainKind::STATE_DRAIN
	                  : ExecutionCompiledDrainKind::ZERO_OR_ONE_OUTPUT;
	stage.required_capability = source.native_state_scan_contract.status != ExecutionRegionStateContractStatus::NONE
	                                ? source.native_state_scan_contract.required_capability
	                                : source.source_contract.required_capability;
	stage.blocker = source.native_state_scan_contract.status != ExecutionRegionStateContractStatus::NONE
	                    ? source.native_state_scan_contract.blocker
	                    : source.source_contract.blocker;
	return stage;
}

static bool ExecutionHashJoinProbeIsExecutableWork(const ExecutionRegionHashJoinContract &contract) {
	return contract.native_probe_contract.status == ExecutionRegionStateContractStatus::READY &&
	       contract.equality_condition_count > 0 && contract.equality_condition_count <= contract.condition_count &&
	       contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::NONE;
}

static ExecutionCompiledStageContract
BuildExecutionCompiledOperatorStage(const ExecutionRegionOperatorInfo &operator_info) {
	ExecutionCompiledStageContract stage;
	switch (operator_info.kind) {
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE:
		stage.stage = ExecutionRegionStageKind::HASH_JOIN_PROBE;
		stage.operation = ExecutionCompiledContractKind::HASH_JOIN_PROBE_CURSOR;
		stage.execution =
		    operator_info.hash_join_contract.native_probe_contract.status == ExecutionRegionStateContractStatus::READY
		        ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
		        : ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
		stage.drain = ExecutionCompiledDrainKind::ZERO_OR_MANY_OUTPUT;
		stage.executable_work = ExecutionHashJoinProbeIsExecutableWork(operator_info.hash_join_contract);
		stage.required_capability = operator_info.hash_join_contract.native_probe_contract.required_capability;
		stage.blocker = operator_info.hash_join_contract.native_probe_contract.blocker;
		break;
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE:
		stage.stage = ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE;
		stage.operation = ExecutionCompiledContractKind::NESTED_LOOP_JOIN_PROBE_CURSOR;
		stage.execution = operator_info.nested_loop_join_contract.native_probe_contract.status ==
		                          ExecutionRegionStateContractStatus::READY
		                      ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
		                      : ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
		stage.drain = ExecutionCompiledDrainKind::ZERO_OR_MANY_OUTPUT;
		stage.executable_work = false;
		stage.required_capability = operator_info.nested_loop_join_contract.native_probe_contract.required_capability;
		stage.blocker = operator_info.nested_loop_join_contract.native_probe_contract.blocker;
		break;
	default:
		stage.stage = ExecutionRegionStageKind::OPERATOR_BOUNDARY;
		stage.operation = ExecutionCompiledContractKind::NONE;
		stage.execution = ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
		stage.drain = ExecutionCompiledDrainKind::NONE;
		break;
	}
	return stage;
}

static bool ExecutionCompiledSinkIsExecutableWork(const ExecutionRegionSinkInfo &sink) {
	switch (sink.kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return true;
	default:
		return false;
	}
}

static ExecutionRegionStateContractStatus ExecutionCompiledSinkStatus(const ExecutionRegionSinkInfo &sink) {
	switch (sink.kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return sink.hash_join_contract.native_build_contract.status;
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return sink.nested_loop_join_contract.native_build_contract.status;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return sink.aggregate_contract.native_state_update_contract.status;
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return sink.aggregate_contract.native_state_update_contract.status;
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
	case ExecutionRegionSinkKind::SORT:
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return sink.native_sink_contract.status;
	default:
		return ExecutionRegionStateContractStatus::MISSING;
	}
}

static ExecutionCompiledContractKind ExecutionCompiledSinkOperation(const ExecutionRegionSinkInfo &sink) {
	switch (sink.kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return ExecutionCompiledContractKind::HASH_JOIN_BUILD;
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return ExecutionCompiledContractKind::NESTED_LOOP_JOIN_BUILD;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return ExecutionCompiledContractKind::AGGREGATE_UPDATE;
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return ExecutionCompiledContractKind::SINK_CURSOR;
	case ExecutionRegionSinkKind::SORT:
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return ExecutionCompiledContractKind::SINK_CURSOR;
	default:
		return ExecutionCompiledContractKind::NONE;
	}
}

static ExecutionRegionStageKind ExecutionCompiledSinkStage(const ExecutionRegionSinkInfo &sink) {
	switch (sink.kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return ExecutionRegionStageKind::HASH_JOIN_BUILD;
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE;
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE;
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE;
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return ExecutionRegionStageKind::APPEND_SINK;
	case ExecutionRegionSinkKind::SORT:
		return ExecutionRegionStageKind::SORT_SINK;
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return ExecutionRegionStageKind::DELIM_JOIN_SINK;
	default:
		return ExecutionRegionStageKind::SINK_BOUNDARY;
	}
}

static ExecutionCompiledStageContract BuildExecutionCompiledSinkStage(const ExecutionRegionSinkInfo &sink) {
	ExecutionCompiledStageContract stage;
	stage.stage = ExecutionCompiledSinkStage(sink);
	stage.operation = ExecutionCompiledSinkOperation(sink);
	stage.execution = ExecutionCompiledSinkStatus(sink) == ExecutionRegionStateContractStatus::READY
	                      ? ExecutionRegionStageExecutionKind::NATIVE_CONTRACT
	                      : ExecutionRegionStageExecutionKind::MISSING_CONTRACT;
	stage.drain = ExecutionCompiledDrainKind::NONE;
	stage.executable_work = ExecutionCompiledSinkIsExecutableWork(sink);
	if (sink.kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		stage.required_capability = sink.hash_join_contract.native_build_contract.required_capability;
		stage.blocker = sink.hash_join_contract.native_build_contract.blocker;
	} else if (sink.kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD) {
		stage.required_capability = sink.nested_loop_join_contract.native_build_contract.required_capability;
		stage.blocker = sink.nested_loop_join_contract.native_build_contract.blocker;
	} else if (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	           sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		stage.required_capability = sink.aggregate_contract.native_state_update_contract.required_capability;
		stage.blocker = sink.aggregate_contract.native_state_update_contract.blocker;
	} else if (sink.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		stage.required_capability = sink.aggregate_contract.native_state_update_contract.required_capability;
		stage.blocker = sink.aggregate_contract.native_state_update_contract.blocker;
	} else if (sink.kind == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
	           sink.kind == ExecutionRegionSinkKind::MATERIALIZATION ||
	           sink.kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
		stage.required_capability = sink.native_sink_contract.required_capability;
		stage.blocker = sink.native_sink_contract.blocker;
	} else if (sink.native_sink_contract.status != ExecutionRegionStateContractStatus::NONE) {
		stage.required_capability = sink.native_sink_contract.required_capability;
		stage.blocker = sink.native_sink_contract.blocker;
	} else {
		stage.required_capability = "sink-cursor";
		stage.blocker = "sink-cursor-contract-missing";
	}
	return stage;
}

ExecutionContract FinalizeExecutionContract(ExecutionContract descriptor) {
	auto &contract = descriptor.compiled_contract;
	contract.stages.clear();

	if (descriptor.HasSource()) {
		contract.stages.push_back(BuildExecutionCompiledSourceStage(descriptor.source));
	}
	if (descriptor.HasOperator()) {
		contract.stages.push_back(BuildExecutionCompiledOperatorStage(descriptor.operator_info));
	}
	if (descriptor.HasSink()) {
		contract.stages.push_back(BuildExecutionCompiledSinkStage(descriptor.sink));
	}

	return descriptor;
}

static string BuildExecutionContractJoinComparisonList(const vector<ExecutionRegionComparisonType> &comparison_types) {
	string result = "[";
	for (idx_t condition_idx = 0; condition_idx < comparison_types.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += "|";
		}
		auto comparison_type = comparison_types[condition_idx];
		result += comparison_type == ExecutionRegionComparisonType::INVALID
		              ? "arbitrary"
		              : string(ExecutionRegionComparisonTypeToString(comparison_type));
	}
	result += "]";
	return result;
}

static bool IsExecutionContractHashJoinEqualityComparison(ExecutionRegionComparisonType comparison_type) {
	return comparison_type == ExecutionRegionComparisonType::EQUAL ||
	       comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
}

static bool IsExecutionContractHashJoinNullEqualComparison(ExecutionRegionComparisonType comparison_type) {
	return comparison_type == ExecutionRegionComparisonType::DISTINCT_FROM ||
	       comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
}

static string ExecutionContractHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode) {
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

static ExecutionHashJoinProbeOutputMode BuildExecutionContractHashJoinProbeOutputMode(ExecutionRegionJoinType join_type,
                                                                                      idx_t rhs_output_column_count) {
	switch (join_type) {
	case ExecutionRegionJoinType::INNER:
		return rhs_output_column_count == 0 ? ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY
		                                    : ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
	case ExecutionRegionJoinType::RIGHT:
		return ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
	case ExecutionRegionJoinType::LEFT:
		return ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD;
	case ExecutionRegionJoinType::SEMI:
		return ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	case ExecutionRegionJoinType::MARK:
		return ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	case ExecutionRegionJoinType::RIGHT_SEMI:
		return ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
	default:
		return ExecutionHashJoinProbeOutputMode::NONE;
	}
}

static const char *ExecutionContractHashJoinResidualSourceKindToString(ExecutionHashJoinResidualSourceKind kind) {
	switch (kind) {
	case ExecutionHashJoinResidualSourceKind::PROBE:
		return "probe";
	case ExecutionHashJoinResidualSourceKind::BUILD:
		return "build";
	default:
		return "unknown";
	}
}

static bool IsExecutionContractNativeHashJoinOwnedJoinType(ExecutionRegionJoinType join_type) {
	switch (join_type) {
	case ExecutionRegionJoinType::INNER:
	case ExecutionRegionJoinType::LEFT:
	case ExecutionRegionJoinType::RIGHT:
	case ExecutionRegionJoinType::OUTER:
	case ExecutionRegionJoinType::SEMI:
	case ExecutionRegionJoinType::ANTI:
	case ExecutionRegionJoinType::MARK:
	case ExecutionRegionJoinType::RIGHT_SEMI:
	case ExecutionRegionJoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

static bool IsExecutionContractHashJoinBoundReference(const Expression &expression, idx_t &input_index) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	input_index = expression.Cast<BoundReferenceExpression>().Index();
	return true;
}

static string BuildExecutionContractHashJoinKeyBindingBlocker(const PhysicalHashJoin &join, bool build_side) {
	for (idx_t key_idx = 0; key_idx < join.conditions.size(); key_idx++) {
		auto &condition = join.conditions[key_idx];
		idx_t input_index = 0;
		auto &expression = build_side ? condition.GetRHS() : condition.GetLHS();
		if (!IsExecutionContractHashJoinBoundReference(expression, input_index)) {
			return string("hash-join-native-") + (build_side ? "build" : "probe") +
			       "-key-not-reference;key_index=" + std::to_string(key_idx);
		}
	}
	return string();
}

static void AddExecutionContractHashJoinRegularLayout(ExecutionRegionHashJoinContract &result) {
	vector<LogicalType> layout_types(result.condition_types);
	layout_types.insert(layout_types.end(), result.payload_types.begin(), result.payload_types.end());
	result.found_match_column_present = ExecutionRegionJoinTypePropagatesBuildSide(result.join_type);
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

static LogicalType ExecutionContractHashJoinLayoutType(const ExecutionRegionHashJoinContract &contract,
                                                       idx_t layout_col) {
	if (layout_col < contract.condition_types.size()) {
		return contract.condition_types[layout_col];
	}
	auto payload_idx = layout_col - contract.condition_types.size();
	if (payload_idx < contract.payload_types.size()) {
		return contract.payload_types[payload_idx];
	}
	return LogicalType::INVALID;
}

static bool RemapExecutionContractExpressionReferences(ExecutionExpressionIR &node,
                                                       const unordered_map<idx_t, idx_t> &source_map, string &blocker) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		auto entry = source_map.find(node.ref_index);
		if (entry == source_map.end()) {
			blocker = "hash-join-native-residual-reference-not-bound;ref_index=" + std::to_string(node.ref_index);
			return false;
		}
		node.ref_index = entry->second;
		return true;
	}
	if (node.left && !RemapExecutionContractExpressionReferences(*node.left, source_map, blocker)) {
		return false;
	}
	if (node.right && !RemapExecutionContractExpressionReferences(*node.right, source_map, blocker)) {
		return false;
	}
	if (node.else_node && !RemapExecutionContractExpressionReferences(*node.else_node, source_map, blocker)) {
		return false;
	}
	for (auto &child : node.children) {
		if (!RemapExecutionContractExpressionReferences(*child, source_map, blocker)) {
			return false;
		}
	}
	return true;
}

static void AddExecutionContractHashJoinResidualSource(ExecutionRegionHashJoinContract &contract,
                                                       unordered_map<idx_t, idx_t> &source_map,
                                                       ExecutionHashJoinResidualSourceKind kind, idx_t original_index,
                                                       idx_t input_index, const LogicalType &type, bool not_null) {
	if (source_map.find(original_index) != source_map.end()) {
		return;
	}
	ExecutionHashJoinResidualSource source;
	source.kind = kind;
	source.source_index = contract.residual_sources.size();
	source.input_index = input_index;
	source.type = type;
	source.not_null = not_null;
	source_map[original_index] = source.source_index;
	contract.residual_sources.push_back(std::move(source));
}

static vector<pair<idx_t, idx_t>> SortExecutionContractResidualMap(const unordered_map<idx_t, idx_t> &input) {
	vector<pair<idx_t, idx_t>> result;
	result.reserve(input.size());
	for (auto &entry : input) {
		result.emplace_back(entry.first, entry.second);
	}
	std::sort(result.begin(), result.end());
	return result;
}

static void AddExecutionContractHashJoinResidualExpression(const PhysicalHashJoin &join,
                                                           ExecutionRegionHashJoinContract &contract) {
	if (!join.predicate && !join.residual_info) {
		return;
	}
	if (!join.predicate || !join.residual_info) {
		contract.residual_expression_blocker = "hash-join-native-residual-predicate-metadata-missing";
		return;
	}
	auto residual = TryLowerExecutionExpression(*join.predicate, 0, ExecutionExpressionIRMode::COMPACT);
	if (!residual || !residual->root) {
		contract.residual_expression_blocker = DescribeExecutionExpressionLoweringFailure(*join.predicate);
		return;
	}

	unordered_map<idx_t, idx_t> source_map;
	for (auto &entry : SortExecutionContractResidualMap(join.residual_info->probe_input_to_probe_map)) {
		auto original_index = entry.first;
		auto probe_index = entry.second;
		if (probe_index >= join.residual_info->probe_types.size() ||
		    probe_index >= join.lhs_probe_columns.col_idxs.size()) {
			contract.residual_expression_blocker = "hash-join-native-residual-probe-source-out-of-range";
			return;
		}
		auto not_null_entry = join.residual_info->probe_input_not_null_map.find(original_index);
		auto not_null = not_null_entry != join.residual_info->probe_input_not_null_map.end() && not_null_entry->second;
		AddExecutionContractHashJoinResidualSource(contract, source_map, ExecutionHashJoinResidualSourceKind::PROBE,
		                                           original_index, join.lhs_probe_columns.col_idxs[probe_index],
		                                           join.residual_info->probe_types[probe_index], not_null);
	}
	for (auto &entry : SortExecutionContractResidualMap(join.residual_info->build_input_to_layout_map)) {
		auto original_index = entry.first;
		auto layout_index = entry.second;
		auto type = ExecutionContractHashJoinLayoutType(contract, layout_index);
		if (type.id() == LogicalTypeId::INVALID) {
			contract.residual_expression_blocker = "hash-join-native-residual-build-source-out-of-range";
			return;
		}
		auto not_null_entry = join.residual_info->build_input_not_null_map.find(original_index);
		auto not_null = not_null_entry != join.residual_info->build_input_not_null_map.end() && not_null_entry->second;
		AddExecutionContractHashJoinResidualSource(contract, source_map, ExecutionHashJoinResidualSourceKind::BUILD,
		                                           original_index, layout_index, type, not_null);
	}

	string blocker;
	if (!RemapExecutionContractExpressionReferences(*residual->root, source_map, blocker)) {
		contract.residual_expression_blocker = blocker;
		return;
	}
	contract.residual_expression = std::move(*residual);
	contract.residual_expression_ready = true;
	contract.residual_expression_blocker.clear();
}

static string BuildExecutionContractHashJoinCommonNativeBlocker(const ExecutionRegionHashJoinContract &contract) {
	if (!IsExecutionContractNativeHashJoinOwnedJoinType(contract.join_type)) {
		return "hash-join-native-join-type;join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	}
	if (contract.condition_count == 0) {
		return "hash-join-native-no-conditions";
	}
	if (contract.rhs_condition_types.size() != contract.condition_count) {
		return "hash-join-native-rhs-condition-type-shape";
	}
	if (!contract.regular_hash_table_layout_ready) {
		return "hash-join-native-layout";
	}
	return string();
}

static string BuildExecutionContractHashJoinProbeNativeShapeBlocker(const ExecutionRegionHashJoinContract &contract,
                                                                    const string &common_blocker) {
	if (!common_blocker.empty()) {
		return common_blocker;
	}
	if (contract.equality_condition_count == 0) {
		return "hash-join-native-no-equality-keys";
	}
	if (contract.equality_condition_count > contract.condition_count) {
		return "hash-join-native-condition-shape";
	}
	if ((contract.residual_predicate || contract.residual_info) && !contract.residual_expression_ready) {
		if (contract.residual_expression_blocker.empty()) {
			return "hash-join-native-residual-predicate";
		}
		return contract.residual_expression_blocker;
	}
	if (contract.non_equality_condition_count != 0 &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return "hash-join-native-non-equality-output-mode-contract-missing";
	}
	return string();
}

static string BuildExecutionContractHashJoinProbeShapeBlocker(const ExecutionRegionHashJoinContract &contract,
                                                              const string &probe_shape_blocker) {
	if (!probe_shape_blocker.empty()) {
		return probe_shape_blocker;
	}
	if (contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::NONE) {
		return "hash-join-native-probe-join-type;join_type=" +
		       string(ExecutionRegionJoinTypeToString(contract.join_type));
	}
	return string();
}

static string BuildExecutionContractPerfectHashJoinProbeShapeBlocker(const ExecutionRegionHashJoinContract &contract) {
	if (contract.join_type != ExecutionRegionJoinType::INNER) {
		return "perfect-hash-join-native-join-type;join_type=" +
		       string(ExecutionRegionJoinTypeToString(contract.join_type));
	}
	if (contract.condition_count != 1 || contract.equality_condition_count != 1 ||
	    contract.non_equality_condition_count != 0) {
		return "perfect-hash-join-native-condition-shape";
	}
	if (contract.comparison_types.empty() || contract.comparison_types[0] != ExecutionRegionComparisonType::EQUAL) {
		return "perfect-hash-join-native-comparison";
	}
	if (contract.condition_types.empty()) {
		return "perfect-hash-join-native-key-type";
	}
	switch (contract.condition_types[0].InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
		break;
	default:
		return "perfect-hash-join-native-key-type";
	}
	if (contract.residual_predicate || contract.residual_info) {
		return "perfect-hash-probe-native-residual-predicate";
	}
	if (contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    contract.native_probe_output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		return "perfect-hash-join-native-output-mode";
	}
	return string();
}

static string BuildExecutionContractHashJoinBuildNativeBlocker(const PhysicalHashJoin &join,
                                                               const string &common_blocker) {
	if (!common_blocker.empty()) {
		return common_blocker;
	}
	auto key_blocker = BuildExecutionContractHashJoinKeyBindingBlocker(join, true);
	if (!key_blocker.empty()) {
		return key_blocker;
	}
	return string();
}

static string BuildExecutionContractHashJoinProbeNativeBlocker(const PhysicalHashJoin &join,
                                                               const string &probe_shape_blocker) {
	if (!probe_shape_blocker.empty()) {
		return probe_shape_blocker;
	}
	auto key_blocker = BuildExecutionContractHashJoinKeyBindingBlocker(join, false);
	if (!key_blocker.empty()) {
		return key_blocker;
	}
	return string();
}

static bool ExecutionContractHashJoinSourceProducesRows(ExecutionRegionJoinType join_type) {
	return ExecutionRegionJoinTypePropagatesBuildSide(join_type);
}

static void MarkExecutionContractHashJoinNativeContracts(const PhysicalHashJoin &join,
                                                         ExecutionRegionHashJoinContract &contract) {
	auto common_blocker = BuildExecutionContractHashJoinCommonNativeBlocker(contract);
	auto probe_shape_blocker = BuildExecutionContractHashJoinProbeNativeShapeBlocker(contract, common_blocker);
	contract.native_contract_blocker = common_blocker;
	contract.native_probe_shape_blocker =
	    BuildExecutionContractHashJoinProbeShapeBlocker(contract, probe_shape_blocker);
	contract.native_probe_shape_ready = contract.native_probe_shape_blocker.empty();
	contract.perfect_hash_probe_shape_blocker = BuildExecutionContractPerfectHashJoinProbeShapeBlocker(contract);
	contract.perfect_hash_probe_shape_ready = contract.perfect_hash_probe_shape_blocker.empty();
	contract.build_sink_shape_blocker = common_blocker;
	contract.build_sink_shape_ready = contract.build_sink_shape_blocker.empty();

	auto build_blocker = BuildExecutionContractHashJoinBuildNativeBlocker(join, common_blocker);
	if (build_blocker.empty()) {
		MarkExecutionContractNativeOperatorContractReady(contract.native_build_contract);
	} else {
		contract.native_build_contract.blocker = build_blocker;
	}

	auto probe_blocker = BuildExecutionContractHashJoinProbeNativeBlocker(join, probe_shape_blocker);
	if (probe_blocker.empty()) {
		MarkExecutionContractNativeOperatorContractReady(contract.native_probe_contract);
	} else {
		contract.native_probe_contract.blocker = probe_blocker;
	}
}

static ExecutionRegionHashJoinContract BuildExecutionContractHashJoinContract(const PhysicalHashJoin &join) {
	ExecutionRegionHashJoinContract result;
	result.present = true;
	result.native_probe_contract =
	    BuildExecutionContractNativeOperatorContract("hash-join-native-probe", "join-probe-contract-boundary");
	result.native_build_contract =
	    BuildExecutionContractNativeOperatorContract("hash-join-native-build", "join-build-contract-boundary");
	result.join_type = ExecutionRegionJoinTypeFromDuckDB(join.join_type);
	result.condition_count = join.conditions.size();
	result.condition_types = join.condition_types;
	result.rhs_condition_types.reserve(join.conditions.size());
	for (auto &condition : join.conditions) {
		result.rhs_condition_types.push_back(ExecutionContractHashJoinConditionDomainType(condition.GetRHS()));
	}
	result.payload_column_count = join.payload_columns.col_idxs.size();
	result.payload_column_indices = join.payload_columns.col_idxs;
	result.payload_types = join.payload_columns.col_types;
	result.lhs_output_column_count = join.lhs_output_columns.col_idxs.size();
	result.lhs_output_column_indices = join.lhs_output_columns.col_idxs;
	result.lhs_output_types = join.lhs_output_columns.col_types;
	result.rhs_output_column_count = join.rhs_output_columns.col_idxs.size();
	result.rhs_output_column_indices = join.rhs_output_columns.col_idxs;
	result.rhs_output_types = join.rhs_output_columns.col_types;
	result.lhs_probe_column_count = join.lhs_probe_columns.col_idxs.size();
	result.lhs_probe_column_indices = join.lhs_probe_columns.col_idxs;
	result.lhs_probe_types = join.lhs_probe_columns.col_types;
	result.lhs_output_in_probe_count = join.lhs_output_in_probe.size();
	result.delim_type_count = join.delim_types.size();
	result.correlated_mark_counts_required = result.join_type == ExecutionRegionJoinType::MARK &&
	                                         result.delim_type_count > 0 &&
	                                         result.delim_type_count + 1 == result.condition_count;
	result.residual_predicate = static_cast<bool>(join.predicate);
	result.residual_info = static_cast<bool>(join.residual_info);
	result.filter_pushdown = static_cast<bool>(join.filter_pushdown);
	result.source_produces_rows = ExecutionContractHashJoinSourceProducesRows(result.join_type);
	result.native_probe_output_mode =
	    BuildExecutionContractHashJoinProbeOutputMode(result.join_type, result.rhs_output_column_count);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison()) {
			result.comparison_types.push_back(ExecutionRegionComparisonType::INVALID);
			continue;
		}
		auto comparison_type = ExecutionRegionComparisonTypeFromDuckDB(condition.GetComparisonType());
		result.comparison_types.push_back(comparison_type);
		if (IsExecutionContractHashJoinEqualityComparison(comparison_type)) {
			result.equality_condition_count++;
		}
		if (IsExecutionContractHashJoinNullEqualComparison(comparison_type)) {
			result.null_equal_condition_count++;
		}
	}
	result.non_equality_condition_count = result.condition_count - result.equality_condition_count;
	if (join.filter_pushdown) {
		result.filter_pushdown_condition_count = join.filter_pushdown->join_condition.size();
		result.filter_pushdown_probe_count = join.filter_pushdown->probe_info.size();
		result.build_side_has_filter = join.filter_pushdown->build_side_has_filter;
	}
	AddExecutionContractHashJoinRegularLayout(result);
	AddExecutionContractHashJoinResidualExpression(join, result);
	MarkExecutionContractHashJoinNativeContracts(join, result);
	return result;
}

static string BuildExecutionContractHashJoinBoundaryReason(const PhysicalHashJoin &join,
                                                           const ExecutionRegionHashJoinContract &contract,
                                                           const string &marker) {
	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(join.type);
	result += ";join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	result += ";condition_count=" + std::to_string(contract.condition_count);
	result += ";equality_condition_count=" + std::to_string(contract.equality_condition_count);
	result += ";non_equality_condition_count=" + std::to_string(contract.non_equality_condition_count);
	result += ";null_equal_condition_count=" + std::to_string(contract.null_equal_condition_count);
	result += ";condition_types=" + BuildExecutionContractLogicalTypeList(contract.condition_types);
	result += ";rhs_condition_types=" + BuildExecutionContractLogicalTypeList(contract.rhs_condition_types);
	result += ";comparison_ops=" + BuildExecutionContractJoinComparisonList(contract.comparison_types);
	result += ";payload_columns=" + std::to_string(contract.payload_column_count);
	result += ";payload_column_indices=" + BuildExecutionContractIdxList(contract.payload_column_indices);
	result += ";payload_types=" + BuildExecutionContractLogicalTypeList(contract.payload_types);
	result += ";lhs_output_columns=" + std::to_string(contract.lhs_output_column_count);
	result += ";lhs_output_column_indices=" + BuildExecutionContractIdxList(contract.lhs_output_column_indices);
	result += ";lhs_output_types=" + BuildExecutionContractLogicalTypeList(contract.lhs_output_types);
	result += ";rhs_output_columns=" + std::to_string(contract.rhs_output_column_count);
	result += ";rhs_output_column_indices=" + BuildExecutionContractIdxList(contract.rhs_output_column_indices);
	result += ";rhs_output_types=" + BuildExecutionContractLogicalTypeList(contract.rhs_output_types);
	result += ";lhs_probe_columns=" + std::to_string(contract.lhs_probe_column_count);
	result += ";lhs_probe_column_indices=" + BuildExecutionContractIdxList(contract.lhs_probe_column_indices);
	result += ";lhs_probe_types=" + BuildExecutionContractLogicalTypeList(contract.lhs_probe_types);
	result += ";lhs_output_in_probe=" + std::to_string(contract.lhs_output_in_probe_count);
	result += ";delim_types=" + std::to_string(contract.delim_type_count);
	result += ";correlated_mark_counts_required=" + ExecutionContractBool(contract.correlated_mark_counts_required);
	result += ";residual_predicate=" + ExecutionContractBool(contract.residual_predicate);
	result += ";residual_info=" + ExecutionContractBool(contract.residual_info);
	result += ";residual_expression_ready=" + ExecutionContractBool(contract.residual_expression_ready);
	result += ";residual_expression_blocker=" + contract.residual_expression_blocker;
	result += ";residual_source_count=" + std::to_string(contract.residual_sources.size());
	for (auto &source : contract.residual_sources) {
		result += ";residual_source" + std::to_string(source.source_index) + "=";
		result += ExecutionContractHashJoinResidualSourceKindToString(source.kind);
		result += ":input=" + std::to_string(source.input_index);
		result += ":type=" + source.type.ToString();
		result += ":not_null=" + ExecutionContractBool(source.not_null);
	}
	result += ";filter_pushdown=" + ExecutionContractBool(contract.filter_pushdown);
	result += ";filter_pushdown_condition_count=" + std::to_string(contract.filter_pushdown_condition_count);
	result += ";filter_pushdown_probe_count=" + std::to_string(contract.filter_pushdown_probe_count);
	result += ";build_side_has_filter=" + ExecutionContractBool(contract.build_side_has_filter);
	result += ";source_produces_rows=" + ExecutionContractBool(contract.source_produces_rows);
	result += ";regular_hash_table_layout_ready=" + ExecutionContractBool(contract.regular_hash_table_layout_ready);
	result += ";perfect_hash_probe_shape_ready=" + ExecutionContractBool(contract.perfect_hash_probe_shape_ready);
	result += ";perfect_hash_probe_shape_blocker=" + contract.perfect_hash_probe_shape_blocker;
	result += ";native_probe_shape_ready=" + ExecutionContractBool(contract.native_probe_shape_ready);
	result += ";native_probe_shape_blocker=" + contract.native_probe_shape_blocker;
	result += ";native_probe_output_mode=" +
	          ExecutionContractHashJoinProbeOutputModeToString(contract.native_probe_output_mode);
	result += ";build_sink_shape_ready=" + ExecutionContractBool(contract.build_sink_shape_ready);
	result += ";build_sink_shape_blocker=" + contract.build_sink_shape_blocker;
	result += ";hash_join_layout_column_count=" + std::to_string(contract.layout_column_count);
	result += ";hash_join_layout_offsets=" + BuildExecutionContractIdxList(contract.layout_offsets);
	result += ";hash_join_tuple_size=" + std::to_string(contract.tuple_size);
	result += ";hash_join_entry_size=" + std::to_string(contract.entry_size);
	result += ";hash_join_pointer_offset=" + std::to_string(contract.pointer_offset);
	result += ";hash_join_hash_column_index=" + std::to_string(contract.hash_column_index);
	result += ";hash_join_found_match_column_present=" + ExecutionContractBool(contract.found_match_column_present);
	result += ";hash_join_found_match_column_index=" + std::to_string(contract.found_match_column_index);
	result += ";hash_join_native_contract_blocker=" + contract.native_contract_blocker;
	AppendExecutionContractNativeOperatorReason(result, contract.native_probe_contract, "native_hash_join_probe");
	AppendExecutionContractNativeOperatorReason(result, contract.native_build_contract, "native_hash_join_build");
	return result;
}

static bool IsExecutionContractNestedLoopSimpleJoin(ExecutionRegionJoinType join_type) {
	return join_type == ExecutionRegionJoinType::SEMI || join_type == ExecutionRegionJoinType::ANTI ||
	       join_type == ExecutionRegionJoinType::MARK;
}

static bool IsExecutionContractNestedLoopComplexJoin(ExecutionRegionJoinType join_type) {
	return join_type == ExecutionRegionJoinType::INNER || join_type == ExecutionRegionJoinType::LEFT ||
	       join_type == ExecutionRegionJoinType::RIGHT || join_type == ExecutionRegionJoinType::OUTER;
}

static ExecutionRegionNestedLoopJoinConditionInput
BuildExecutionContractNestedLoopJoinConditionInput(const JoinCondition &condition, idx_t condition_idx,
                                                   string &condition_blocker) {
	ExecutionRegionNestedLoopJoinConditionInput result;
	result.condition_index = condition_idx;
	result.type = condition.GetLHS().GetReturnType();
	if (!condition.IsComparison()) {
		result.comparison_type = ExecutionRegionComparisonType::INVALID;
		result.lhs_expression_blocker = "nested-loop-native-condition-not-comparison";
		result.rhs_expression_blocker = "nested-loop-native-condition-not-comparison";
		if (condition_blocker.empty()) {
			condition_blocker =
			    "nested-loop-native-condition-not-comparison;condition_index=" + std::to_string(condition_idx);
		}
		return result;
	}

	result.comparison_type = ExecutionRegionComparisonTypeFromDuckDB(condition.GetComparisonType());
	auto lhs_expression =
	    TryLowerExecutionExpression(condition.GetLHS(), condition_idx, ExecutionExpressionIRMode::COMPACT);
	if (lhs_expression && lhs_expression->root) {
		result.lhs_expression_ready = true;
		result.lhs_expression = std::move(*lhs_expression);
	} else {
		result.lhs_expression_blocker = DescribeExecutionExpressionLoweringFailure(condition.GetLHS());
		if (condition_blocker.empty()) {
			condition_blocker = "nested-loop-native-lhs-expression;" + result.lhs_expression_blocker +
			                    ";condition_index=" + std::to_string(condition_idx);
		}
	}

	auto rhs_expression =
	    TryLowerExecutionExpression(condition.GetRHS(), condition_idx, ExecutionExpressionIRMode::COMPACT);
	if (rhs_expression && rhs_expression->root) {
		result.rhs_expression_ready = true;
		result.rhs_expression = std::move(*rhs_expression);
	} else {
		result.rhs_expression_blocker = DescribeExecutionExpressionLoweringFailure(condition.GetRHS());
		if (condition_blocker.empty()) {
			condition_blocker = "nested-loop-native-rhs-expression;" + result.rhs_expression_blocker +
			                    ";condition_index=" + std::to_string(condition_idx);
		}
	}
	return result;
}

static string BuildExecutionContractNestedLoopJoinNativeBlocker(const ExecutionRegionNestedLoopJoinContract &contract) {
	if (!contract.simple_join && !contract.complex_join) {
		return "nested-loop-native-join-type;join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	}
	if (contract.condition_count == 0) {
		return "nested-loop-native-no-conditions";
	}
	if (!contract.conditions_ready) {
		return contract.condition_blocker.empty() ? "nested-loop-native-condition-lowering"
		                                          : contract.condition_blocker;
	}
	if (contract.residual_predicate) {
		return "nested-loop-native-residual-predicate";
	}
	if (contract.filter_pushdown) {
		return "nested-loop-native-filter-pushdown";
	}
	return string();
}

static void MarkExecutionContractNestedLoopJoinNativeContracts(ExecutionRegionNestedLoopJoinContract &contract) {
	auto blocker = BuildExecutionContractNestedLoopJoinNativeBlocker(contract);
	contract.native_probe_shape_blocker = blocker;
	contract.build_sink_shape_blocker = blocker;
	contract.native_probe_shape_ready = blocker.empty();
	contract.build_sink_shape_ready = blocker.empty();
	if (blocker.empty()) {
		MarkExecutionContractNativeOperatorContractReady(contract.native_probe_contract);
		MarkExecutionContractNativeOperatorContractReady(contract.native_build_contract);
		return;
	}
	contract.native_probe_contract.blocker = blocker;
	contract.native_build_contract.blocker = blocker;
}

static ExecutionRegionNestedLoopJoinContract
BuildExecutionContractNestedLoopJoinContract(const PhysicalNestedLoopJoin &join) {
	ExecutionRegionNestedLoopJoinContract result;
	result.present = true;
	result.native_probe_contract =
	    BuildExecutionContractNativeOperatorContract("nested-loop-join-native-probe", "nested-loop-probe-boundary");
	result.native_build_contract =
	    BuildExecutionContractNativeOperatorContract("nested-loop-join-native-build", "nested-loop-build-boundary");
	result.join_type = ExecutionRegionJoinTypeFromDuckDB(join.join_type);
	result.condition_count = join.conditions.size();
	result.lhs_input_types = join.children[0].get().GetTypes();
	result.rhs_input_types = join.children[1].get().GetTypes();
	result.output_types = join.GetTypes();
	result.simple_join = IsExecutionContractNestedLoopSimpleJoin(result.join_type);
	result.complex_join = IsExecutionContractNestedLoopComplexJoin(result.join_type);
	result.source_produces_rows = ExecutionRegionJoinTypePropagatesBuildSide(result.join_type);
	result.residual_predicate = static_cast<bool>(join.predicate);
	result.filter_pushdown = static_cast<bool>(join.filter_pushdown);

	string condition_blocker;
	result.conditions.reserve(join.conditions.size());
	for (idx_t condition_idx = 0; condition_idx < join.conditions.size(); condition_idx++) {
		auto &condition = join.conditions[condition_idx];
		result.condition_types.push_back(condition.GetLHS().GetReturnType());
		auto comparison_type = condition.IsComparison()
		                           ? ExecutionRegionComparisonTypeFromDuckDB(condition.GetComparisonType())
		                           : ExecutionRegionComparisonType::INVALID;
		result.comparison_types.push_back(comparison_type);
		if (comparison_type != ExecutionRegionComparisonType::INVALID) {
			result.comparison_condition_count++;
		}
		result.conditions.push_back(
		    BuildExecutionContractNestedLoopJoinConditionInput(condition, condition_idx, condition_blocker));
	}
	result.conditions_ready = condition_blocker.empty() && result.comparison_condition_count == result.condition_count;
	result.condition_blocker = std::move(condition_blocker);
	MarkExecutionContractNestedLoopJoinNativeContracts(result);
	return result;
}

static string BuildExecutionContractNestedLoopJoinBoundaryReason(const PhysicalNestedLoopJoin &join,
                                                                 const ExecutionRegionNestedLoopJoinContract &contract,
                                                                 const string &marker) {
	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(join.type);
	result += ";join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type));
	result += ";condition_count=" + std::to_string(contract.condition_count);
	result += ";comparison_condition_count=" + std::to_string(contract.comparison_condition_count);
	result += ";condition_types=" + BuildExecutionContractLogicalTypeList(contract.condition_types);
	result += ";comparison_ops=" + BuildExecutionContractJoinComparisonList(contract.comparison_types);
	result += ";lhs_input_types=" + BuildExecutionContractLogicalTypeList(contract.lhs_input_types);
	result += ";rhs_input_types=" + BuildExecutionContractLogicalTypeList(contract.rhs_input_types);
	result += ";output_types=" + BuildExecutionContractLogicalTypeList(contract.output_types);
	result += ";simple_join=" + ExecutionContractBool(contract.simple_join);
	result += ";complex_join=" + ExecutionContractBool(contract.complex_join);
	result += ";source_produces_rows=" + ExecutionContractBool(contract.source_produces_rows);
	result += ";residual_predicate=" + ExecutionContractBool(contract.residual_predicate);
	result += ";filter_pushdown=" + ExecutionContractBool(contract.filter_pushdown);
	result += ";conditions_ready=" + ExecutionContractBool(contract.conditions_ready);
	result += ";condition_blocker=" + contract.condition_blocker;
	result += ";native_probe_shape_ready=" + ExecutionContractBool(contract.native_probe_shape_ready);
	result += ";native_probe_shape_blocker=" + contract.native_probe_shape_blocker;
	result += ";build_sink_shape_ready=" + ExecutionContractBool(contract.build_sink_shape_ready);
	result += ";build_sink_shape_blocker=" + contract.build_sink_shape_blocker;
	AppendExecutionContractNativeOperatorReason(result, contract.native_probe_contract,
	                                            "native_nested_loop_join_probe");
	AppendExecutionContractNativeOperatorReason(result, contract.native_build_contract,
	                                            "native_nested_loop_join_build");
	return result;
}

static ExecutionRegionOperatorKind ExecutionContractOrderOperatorKind(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::ORDER_BY:
		return ExecutionRegionOperatorKind::ORDER_BY;
	case PhysicalOperatorType::TOP_N:
		return ExecutionRegionOperatorKind::TOP_N;
	default:
		return ExecutionRegionOperatorKind::GENERIC;
	}
}

static ExecutionRegionOrderKeyInput BuildExecutionContractOrderKeyInput(const BoundOrderByNode &order, idx_t key_index,
                                                                        string &first_blocker) {
	ExecutionRegionOrderKeyInput result;
	result.key_index = key_index;
	result.type = order.expression->GetReturnType();
	result.physical_type = result.type.InternalType();
	result.order_type = order.type;
	result.null_order = order.null_order;
	result.reason = "order-key";
	result.reason += ";key_index=" + std::to_string(key_index);
	result.reason += ";type=" + result.type.ToString();
	result.reason += ";physical_type=" + TypeIdToString(result.physical_type);
	result.reason += ";order_type=" + EnumUtil::ToString(result.order_type);
	result.reason += ";null_order=" + EnumUtil::ToString(result.null_order);
	auto expression = TryLowerExecutionExpression(*order.expression, key_index, ExecutionExpressionIRMode::COMPACT);
	if (!expression || !expression->root) {
		result.expression_blocker =
		    "order-native-key-expression;" + DescribeExecutionExpressionLoweringFailure(*order.expression);
		if (first_blocker.empty()) {
			first_blocker = result.expression_blocker;
		}
		result.reason += ";expression_ready=false;expression_blocker=" + result.expression_blocker;
		return result;
	}
	result.expression = std::move(*expression);
	result.expression_ready = true;
	result.expression_blocker.clear();
	result.reason += ";expression_ready=true";
	return result;
}

static ExecutionRegionOrderContract BuildExecutionContractOrderContract(PhysicalOperatorType type,
                                                                        const vector<BoundOrderByNode> &orders,
                                                                        const vector<LogicalType> &payload_types,
                                                                        const vector<idx_t> &projections,
                                                                        bool is_index_sort, bool has_limit, idx_t limit,
                                                                        idx_t offset, bool dynamic_filter) {
	ExecutionRegionOrderContract result;
	result.present = true;
	result.kind = ExecutionContractOrderOperatorKind(type);
	result.order_count = orders.size();
	result.payload_type_count = payload_types.size();
	result.payload_types = payload_types;
	result.projection_count = projections.size();
	result.projection_ids = projections;
	result.has_limit = has_limit;
	result.limit = has_limit ? limit : 0;
	result.offset = has_limit ? offset : 0;
	result.dynamic_filter = dynamic_filter;
	result.is_index_sort = is_index_sort;
	string first_blocker;
	result.order_keys.reserve(orders.size());
	for (idx_t key_idx = 0; key_idx < orders.size(); key_idx++) {
		result.order_keys.push_back(BuildExecutionContractOrderKeyInput(orders[key_idx], key_idx, first_blocker));
	}
	result.all_order_keys_ready = first_blocker.empty();
	result.order_key_blocker = result.all_order_keys_ready ? string() : first_blocker;
	return result;
}

static string BuildExecutionContractOrderSinkBlocker(const ExecutionRegionOrderContract &contract) {
	if (!contract.all_order_keys_ready) {
		return contract.order_key_blocker;
	}
	return string();
}

static ExecutionContract BuildExecutionContractSortStateContracts(
    PhysicalOperatorType type, const vector<BoundOrderByNode> &orders, const vector<LogicalType> &payload_types,
    const vector<idx_t> &projections, idx_t output_column_count, const string &function_name,
    const string &native_scan_capability, const string &source_marker, bool is_index_sort, bool has_limit, idx_t limit,
    idx_t offset, bool dynamic_filter) {
	ExecutionContract result;
	auto order_contract = BuildExecutionContractOrderContract(type, orders, payload_types, projections, is_index_sort,
	                                                          has_limit, limit, offset, dynamic_filter);
	auto state_scan_contract = BuildExecutionContractNativeStateScanContract(native_scan_capability, string());
	MarkExecutionContractNativeStateScanContractReady(state_scan_contract);
	result.source_boundary_reason = source_marker;
	result.source_boundary_reason += ";operator=" + PhysicalOperatorToString(type);
	result.source_boundary_reason += ";order_count=" + std::to_string(order_contract.order_count);
	result.source_boundary_reason += ";projection_count=" + std::to_string(order_contract.projection_count);
	result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	result.source.source_contract = BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
	result.source.native_state_scan_contract = std::move(state_scan_contract);
	result.source.order_contract = order_contract;
	AppendExecutionContractNativeStateScanReason(result.source_boundary_reason,
	                                             result.source.native_state_scan_contract);
	result.source.function_name = function_name;
	result.source.output_column_count = output_column_count;
	result.source.returned_column_count = output_column_count;
	result.source.reason = result.source_boundary_reason;

	result.sink.kind = ExecutionRegionSinkKind::SORT;
	result.sink.reason = "DuckDB ordered sink contract";
	result.sink.reason += ";operator=" + PhysicalOperatorToString(type);
	result.sink.reason += ";order_count=" + std::to_string(order_contract.order_count);
	result.sink.reason += ";projection_count=" + std::to_string(order_contract.projection_count);
	result.sink.reason += ";output_columns=" + std::to_string(output_column_count);
	result.sink.reason += ";order_keys_ready=" + ExecutionContractBool(order_contract.all_order_keys_ready);
	result.sink.reason += ";order_key_blocker=" + order_contract.order_key_blocker;
	result.sink.native_sink_contract = BuildExecutionContractNativeOperatorContract(
	    "order-native-sink-update", BuildExecutionContractOrderSinkBlocker(order_contract));
	if (order_contract.all_order_keys_ready) {
		MarkExecutionContractNativeOperatorContractReady(result.sink.native_sink_contract);
	} else {
		MarkExecutionContractNativeOperatorContractBlocked(result.sink.native_sink_contract,
		                                                   order_contract.order_key_blocker);
	}
	result.sink.order_contract = std::move(order_contract);
	AppendExecutionContractNativeOperatorReason(result.sink.reason, result.sink.native_sink_contract, "sink");
	return result;
}

static void ApplyExecutionContractFinalizedSourceCardinality(ExecutionContract &contract, optional_idx cardinality) {
	contract.source.finalized_source_cardinality_required = true;
	if (!cardinality.IsValid()) {
		contract.source_boundary_reason += ";finalized_source_cardinality=not_ready";
		contract.source.reason = contract.source_boundary_reason;
		return;
	}
	const auto count = cardinality.GetIndex();
	contract.source.estimated_source_cardinality = count;
	contract.source.estimated_source_cardinality_exact = true;
	contract.source_boundary_reason += ";finalized_source_cardinality=" + std::to_string(count);
	contract.source.reason = contract.source_boundary_reason;
}

static ExecutionRegionAggregateContract
BuildExecutionContractHashAggregateContract(const PhysicalHashAggregate &aggregate);
static ExecutionRegionAggregateContract
BuildExecutionContractPerfectHashAggregateContract(const PhysicalPerfectHashAggregate &aggregate);

static string BuildExecutionContractHashAggregateBoundaryReason(const PhysicalHashAggregate &aggregate,
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
	result += ";group_types=" + BuildExecutionContractLogicalTypeList(aggregate_data.group_types);
	result += ";aggregate_count=" + std::to_string(aggregate_data.aggregates.size());
	result += ";aggregate_functions=" + BuildExecutionContractAggregateFunctionList(aggregate_data.aggregates);
	result += ";aggregate_return_types=" + BuildExecutionContractAggregateReturnTypeList(aggregate_data.aggregates);
	result += ";aggregate_child_counts=" + BuildExecutionContractAggregateChildCountList(aggregate_data.aggregates);
	result += ";aggregate_types=" + BuildExecutionContractAggregateTypeList(aggregate_data.aggregates);
	result += ";aggregate_filter_count=" + std::to_string(aggregate_data.filter_count);
	result += ";aggregate_order_count=" +
	          std::to_string(CountExecutionContractAggregateOrderModifiers(aggregate_data.aggregates));
	result += ";payload_type_count=" + std::to_string(aggregate_data.payload_types.size());
	result += ";payload_types=" + BuildExecutionContractLogicalTypeList(aggregate_data.payload_types);
	result += ";grouping_set_count=" + std::to_string(aggregate.grouping_sets.size());
	result += ";grouping_function_count=" +
	          std::to_string(SumExecutionContractGroupingFunctionCount(aggregate_data.GetGroupingFunctions()));
	result += ";radix_table_count=" + std::to_string(aggregate.groupings.size());
	result += ";distinct_aggregate_count=" + std::to_string(distinct_aggregate_count);
	result += ";distinct_table_count=" + std::to_string(distinct_table_count);
	result += ";distinct_child_count=" + std::to_string(distinct_child_count);
	result += ";input_group_type_count=" + std::to_string(aggregate.input_group_types.size());
	result += ";input_group_types=" + BuildExecutionContractLogicalTypeList(aggregate.input_group_types);
	result += ";non_distinct_filter_count=" + std::to_string(aggregate.non_distinct_filter.size());
	result += ";distinct_filter_count=" + std::to_string(aggregate.distinct_filter.size());
	return result;
}

static void AddExecutionContractAggregateContractCommon(ExecutionRegionAggregateContract &result,
                                                        const vector<unique_ptr<Expression>> &aggregates) {
	result.aggregate_count = aggregates.size();
	result.aggregate_functions = BuildExecutionContractAggregateFunctionVector(aggregates);
	result.aggregate_return_types = BuildExecutionContractAggregateReturnTypeVector(aggregates);
	result.aggregate_child_counts = BuildExecutionContractAggregateChildCountVector(aggregates);
	result.aggregate_types = BuildExecutionContractAggregateTypeVector(aggregates);
	result.aggregate_order_count = CountExecutionContractAggregateOrderModifiers(aggregates);
	result.distinct_aggregate_count = CountExecutionContractDistinctAggregates(aggregates);
}

static void AddExecutionContractHashAggregateGroupedStateLayout(ExecutionRegionAggregateContract &result,
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

static void MarkExecutionContractHashAggregateGroupedStateContract(ExecutionRegionAggregateContract &result) {
	if (!result.grouped_state_layout_ready || result.grouping_set_count != 1 || result.radix_table_count != 1 ||
	    result.distinct_aggregate_count != 0 || result.aggregate_filter_count != 0 ||
	    result.aggregate_order_count != 0 || result.non_distinct_filter_count != result.aggregate_count) {
		return;
	}
	result.native_grouped_state_contract.status = ExecutionRegionStateContractStatus::READY;
	result.native_grouped_state_contract.blocker.clear();
}

static bool ExecutionContractHashAggregateHasDistinctState(const ExecutionRegionAggregateContract &contract) {
	return contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	       contract.distinct_child_count != 0 || contract.distinct_filter_count != 0;
}

static void MarkExecutionContractHashAggregateDistinctStateBoundary(ExecutionRegionAggregateContract &contract) {
	if (!ExecutionContractHashAggregateHasDistinctState(contract)) {
		return;
	}
	if (contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::MISSING) {
		contract.native_grouped_state_contract.blocker = "hash-aggregate-distinct-grouped-state-contract-boundary";
	}
}

static void MarkExecutionContractPerfectHashAggregateGroupedStateContract(ExecutionRegionAggregateContract &result) {
	if (!result.grouped_state_layout_ready || result.distinct_aggregate_count != 0 ||
	    result.aggregate_filter_count != 0 || result.aggregate_order_count != 0) {
		return;
	}
	result.native_grouped_state_contract.status = ExecutionRegionStateContractStatus::READY;
	result.native_grouped_state_contract.blocker.clear();
}

static string
BuildExecutionContractPerfectHashAggregateStateScanBlocker(const ExecutionRegionAggregateContract &contract) {
	if (!contract.present || contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH) {
		return "perfect-hash-aggregate-state-scan-kind";
	}
	if (contract.group_count == 0) {
		return "perfect-hash-aggregate-state-scan-no-groups";
	}
	if (contract.group_count != contract.perfect_required_bits_count ||
	    contract.group_count != contract.perfect_group_minima_count) {
		return "perfect-hash-aggregate-state-scan-group-layout";
	}
	if (contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	    contract.distinct_child_count != 0) {
		return "perfect-hash-aggregate-state-scan-distinct-state";
	}
	return string();
}

static void
MarkExecutionContractPerfectHashAggregateStateScanContract(ExecutionRegionNativeStateScanContract &state_scan_contract,
                                                           const ExecutionRegionAggregateContract &aggregate_contract) {
	auto blocker = BuildExecutionContractPerfectHashAggregateStateScanBlocker(aggregate_contract);
	if (blocker.empty()) {
		MarkExecutionContractNativeStateScanContractReady(state_scan_contract);
	} else {
		state_scan_contract.blocker = blocker;
	}
}

static string
BuildExecutionContractUngroupedAggregateStateScanBlocker(const ExecutionRegionAggregateContract &contract) {
	if (!contract.present || contract.kind != ExecutionRegionAggregateOperatorKind::UNGROUPED) {
		return "ungrouped-aggregate-state-scan-kind";
	}
	if (contract.aggregate_count == 0) {
		return "ungrouped-aggregate-state-scan-no-aggregates";
	}
	return string();
}

static void
MarkExecutionContractUngroupedAggregateStateScanContract(ExecutionRegionNativeStateScanContract &state_scan_contract,
                                                         const ExecutionRegionAggregateContract &aggregate_contract) {
	auto blocker = BuildExecutionContractUngroupedAggregateStateScanBlocker(aggregate_contract);
	if (blocker.empty()) {
		MarkExecutionContractNativeStateScanContractReady(state_scan_contract);
	} else {
		state_scan_contract.blocker = blocker;
	}
}

static void MarkExecutionContractAggregateStateUpdateContract(ExecutionRegionAggregateContract &contract,
                                                              const vector<ExecutionRegionAggregateInput> &aggregates,
                                                              const vector<ExecutionRegionGroupInput> &groups) {
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, aggregates, groups);
	if (blocker.empty()) {
		MarkExecutionContractNativeOperatorContractReady(contract.native_state_update_contract);
	} else {
		MarkExecutionContractNativeOperatorContractBlocked(contract.native_state_update_contract, std::move(blocker));
	}
}

static void AddExecutionContractPerfectHashAggregateGroupedStateLayout(ExecutionRegionAggregateContract &result,
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

static ExecutionRegionAggregateContract
BuildExecutionContractHashAggregateContract(const PhysicalHashAggregate &aggregate) {
	ExecutionRegionAggregateContract result;
	result.present = true;
	result.kind = ExecutionRegionAggregateOperatorKind::HASH;
	result.native_grouped_state_contract = BuildExecutionContractNativeGroupedStateContract(result.kind);
	result.native_state_update_contract = BuildExecutionContractNativeOperatorContract(
	    "hash-aggregate-native-state-update", "hash-aggregate-native-state-update-boundary");
	auto &aggregate_data = aggregate.grouped_aggregate_data;
	AddExecutionContractAggregateContractCommon(result, aggregate_data.aggregates);
	result.group_count = aggregate_data.GroupCount();
	result.group_types = aggregate_data.group_types;
	result.aggregate_filter_count = aggregate_data.filter_count;
	result.payload_type_count = aggregate_data.payload_types.size();
	result.payload_types = aggregate_data.payload_types;
	result.grouping_set_count = aggregate.grouping_sets.size();
	result.grouping_function_count = SumExecutionContractGroupingFunctionCount(aggregate_data.GetGroupingFunctions());
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
	AddExecutionContractHashAggregateGroupedStateLayout(result, aggregate);
	MarkExecutionContractHashAggregateGroupedStateContract(result);
	MarkExecutionContractHashAggregateDistinctStateBoundary(result);
	return result;
}

static string BuildExecutionContractPerfectHashAggregateBoundaryReason(const PhysicalPerfectHashAggregate &aggregate,
                                                                       const string &marker) {
	string result = marker;
	result += ";operator=" + PhysicalOperatorToString(aggregate.type);
	result += ";aggregate_operator_kind=perfect_hash";
	result += ";group_count=" + std::to_string(aggregate.groups.size());
	result += ";group_types=" + BuildExecutionContractLogicalTypeList(aggregate.group_types);
	result += ";aggregate_count=" + std::to_string(aggregate.aggregates.size());
	result += ";aggregate_functions=" + BuildExecutionContractAggregateFunctionList(aggregate.aggregates);
	result += ";aggregate_return_types=" + BuildExecutionContractAggregateReturnTypeList(aggregate.aggregates);
	result += ";aggregate_child_counts=" + BuildExecutionContractAggregateChildCountList(aggregate.aggregates);
	result += ";aggregate_types=" + BuildExecutionContractAggregateTypeList(aggregate.aggregates);
	result += ";aggregate_filter_count=" + std::to_string(aggregate.filter_indexes.size());
	result +=
	    ";aggregate_order_count=" + std::to_string(CountExecutionContractAggregateOrderModifiers(aggregate.aggregates));
	result += ";payload_type_count=" + std::to_string(aggregate.payload_types.size());
	result += ";payload_types=" + BuildExecutionContractLogicalTypeList(aggregate.payload_types);
	result += ";grouping_set_count=0";
	result += ";grouping_function_count=0";
	result += ";radix_table_count=0";
	result +=
	    ";distinct_aggregate_count=" + std::to_string(CountExecutionContractDistinctAggregates(aggregate.aggregates));
	result += ";distinct_table_count=0";
	result += ";distinct_child_count=0";
	result += ";input_group_type_count=0";
	result += ";input_group_types=[]";
	result += ";non_distinct_filter_count=0";
	result += ";distinct_filter_count=0";
	result += ";perfect_required_bits_count=" + std::to_string(aggregate.required_bits.size());
	result += ";perfect_required_bits_total=" + std::to_string(SumExecutionContractIdxVector(aggregate.required_bits));
	result += ";perfect_required_bits=" + BuildExecutionContractIdxList(aggregate.required_bits);
	result += ";perfect_group_minima_count=" + std::to_string(aggregate.group_minima.size());
	return result;
}

static ExecutionRegionAggregateContract
BuildExecutionContractPerfectHashAggregateContract(const PhysicalPerfectHashAggregate &aggregate) {
	ExecutionRegionAggregateContract result;
	result.present = true;
	result.kind = ExecutionRegionAggregateOperatorKind::PERFECT_HASH;
	result.native_grouped_state_contract = BuildExecutionContractNativeGroupedStateContract(result.kind);
	result.native_state_update_contract = BuildExecutionContractNativeOperatorContract(
	    "perfect-hash-aggregate-native-state-update", "perfect-hash-aggregate-native-state-update-boundary");
	AddExecutionContractAggregateContractCommon(result, aggregate.aggregates);
	result.group_count = aggregate.groups.size();
	result.group_types = aggregate.group_types;
	result.aggregate_filter_count = aggregate.filter_indexes.size();
	result.payload_type_count = aggregate.payload_types.size();
	result.payload_types = aggregate.payload_types;
	result.perfect_required_bits_count = aggregate.required_bits.size();
	result.perfect_required_bits_total = SumExecutionContractIdxVector(aggregate.required_bits);
	result.perfect_required_bits = aggregate.required_bits;
	result.perfect_group_minima_count = aggregate.group_minima.size();
	result.perfect_group_minima = aggregate.group_minima;
	AddExecutionContractPerfectHashAggregateGroupedStateLayout(result, aggregate);
	MarkExecutionContractPerfectHashAggregateGroupedStateContract(result);
	return result;
}

static string BuildExecutionContractUngroupedAggregateBoundaryReason(const PhysicalUngroupedAggregate &aggregate,
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
	result += ";aggregate_functions=" + BuildExecutionContractAggregateFunctionList(aggregate.aggregates);
	result += ";aggregate_return_types=" + BuildExecutionContractAggregateReturnTypeList(aggregate.aggregates);
	result += ";aggregate_child_counts=" + BuildExecutionContractAggregateChildCountList(aggregate.aggregates);
	result += ";aggregate_types=" + BuildExecutionContractAggregateTypeList(aggregate.aggregates);
	result += ";aggregate_filter_count=0";
	result +=
	    ";aggregate_order_count=" + std::to_string(CountExecutionContractAggregateOrderModifiers(aggregate.aggregates));
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

static ExecutionRegionAggregateContract
BuildExecutionContractUngroupedAggregateContract(const PhysicalUngroupedAggregate &aggregate) {
	ExecutionRegionAggregateContract result;
	result.present = true;
	result.kind = ExecutionRegionAggregateOperatorKind::UNGROUPED;
	result.native_state_update_contract = BuildExecutionContractNativeOperatorContract(
	    "ungrouped-aggregate-native-state-update", "ungrouped-aggregate-native-state-update-boundary");
	AddExecutionContractAggregateContractCommon(result, aggregate.aggregates);
	if (aggregate.distinct_collection_info) {
		result.distinct_aggregate_count = aggregate.distinct_collection_info->Indices().size();
		result.distinct_table_count = aggregate.distinct_collection_info->table_count;
		result.distinct_child_count = aggregate.distinct_collection_info->total_child_count;
	}
	return result;
}

static ExecutionRegionTableScanContract
BuildExecutionContractTableScanContract(const PhysicalTableScan &scan,
                                        const ExecutionSourceContractCapability &capability) {
	ExecutionRegionTableScanContract result;
	result.present = true;
	result.function_name = StringUtil::Lower(scan.function.name.GetIdentifierName());
	result.estimated_source_cardinality = GetExecutionRegionTableScanSourceCardinality(scan);
	result.output_column_count = scan.GetTypes().size();
	result.returned_column_count = scan.returned_types.size();
	result.column_id_count = scan.column_ids.size();
	result.projected_column_count =
	    scan.function.projection_pushdown
	        ? (scan.function.filter_prune ? scan.projection_ids.size() : scan.column_ids.size())
	        : scan.GetTypes().size();
	result.column_ids = BuildExecutionContractColumnIndexList(scan.column_ids);
	result.projection_ids = scan.projection_ids;
	result.source_contract_input_column_count = scan.column_ids.size();
	result.source_contract_input_types = BuildExecutionContractTableScanSourceInputTypes(scan);
	result.source_contract_input_not_null = BuildExecutionContractTableScanSourceInputNotNull(scan);
	result.source_contract_output_projection_map = BuildExecutionContractTableScanOutputProjectionMap(scan);
	result.source_contract_filter_prune_required =
	    scan.table_filters && scan.function.filter_prune && !scan.projection_ids.empty();
	result.projection_pushdown = scan.function.projection_pushdown;
	result.filter_pushdown = scan.function.filter_pushdown;
	result.filter_prune = scan.function.filter_prune;
	result.dynamic_filters = scan.dynamic_filters && scan.dynamic_filters->HasFilters();
	result.in_out_function = static_cast<bool>(scan.function.in_out_function);
	result.filter_count = scan.table_filters ? scan.table_filters->FilterCount() : 0;
	return result;
}

static string BuildExecutionContractTableScanSourceBoundaryReason(const ExecutionRegionTableScanContract &contract,
                                                                  ExecutionRegionSourceExecutionKind execution) {
	auto source_name = contract.function_name == "seq_scan" ? "table scan" : "table-function";
	string result = execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
	                    ? "DuckDB " + string(source_name) + " source contract"
	                    : "DuckDB " + string(source_name) + " source boundary";
	if (execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY) {
		result += ";source-contract-blocker:requires-source-contract;source_execution=duckdb-source-boundary";
	}
	result += ";function=" + contract.function_name;
	result += ";estimated_source_cardinality=" + std::to_string(contract.estimated_source_cardinality);
	result += ";output_columns=" + std::to_string(contract.output_column_count);
	result += ";returned_columns=" + std::to_string(contract.returned_column_count);
	result += ";column_ids=" + std::to_string(contract.column_id_count);
	result += ";projection_pushdown=" + ExecutionContractBool(contract.projection_pushdown);
	result += ";projected_columns=" + std::to_string(contract.projected_column_count);
	result += ";source_contract_input_columns=" + std::to_string(contract.source_contract_input_column_count);
	result +=
	    ";source_contract_input_types=" + BuildExecutionContractLogicalTypeList(contract.source_contract_input_types);
	result +=
	    ";source_contract_input_not_null=" + BuildExecutionContractBoolList(contract.source_contract_input_not_null);
	result += ";source_contract_output_projection_map=" +
	          BuildExecutionContractIdxList(contract.source_contract_output_projection_map);
	result += ";source_contract_filter_prune_required=" +
	          ExecutionContractBool(contract.source_contract_filter_prune_required);
	result += ";filter_pushdown=" + ExecutionContractBool(contract.filter_pushdown);
	result += ";filter_prune=" + ExecutionContractBool(contract.filter_prune);
	result += ";filter_count=" + std::to_string(contract.filter_count);
	result += ";dynamic_filters=" + ExecutionContractBool(contract.dynamic_filters);
	result += ";in_out_function=" + ExecutionContractBool(contract.in_out_function);
	return result;
}

static bool TryReadExecutionContractAggregateInput(const BoundAggregateExpression &aggregate,
                                                   ExecutionRegionAggregateInput &result) {
	auto &children = aggregate.GetChildren();
	if (children.empty()) {
		result.payload_index = 0;
		result.supported_payload_references = true;
		result.payload_expressions_ready = true;
		return true;
	}

	bool contiguous_references = true;
	idx_t payload_index = 0;
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = *children[child_idx];
		result.child_types.push_back(child.GetReturnType());
		auto child_expression = TryLowerExecutionExpression(child, child_idx, ExecutionExpressionIRMode::COMPACT);
		if (!child_expression) {
			result.payload_expression_blocker =
			    "aggregate child expression lowering unsupported;child_index=" + std::to_string(child_idx) + ";" +
			    DescribeExecutionExpressionLoweringFailure(child);
			result.reason = result.payload_expression_blocker;
			return false;
		}
		result.child_expressions.push_back(std::move(*child_expression));

		if (child.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &reference = child.Cast<BoundReferenceExpression>();
			auto reference_index = reference.Index();
			result.child_indices.push_back(reference_index);
			if (child_idx == 0) {
				payload_index = reference_index;
			} else if (reference_index != payload_index + child_idx) {
				contiguous_references = false;
			}
		} else {
			contiguous_references = false;
		}
	}
	result.payload_expressions_ready = true;
	result.supported_payload_references = contiguous_references;
	if (contiguous_references) {
		result.payload_index = payload_index;
	}
	return true;
}

static void AppendExecutionContractAggregateReason(ExecutionRegionAggregateInput &aggregate, string reason) {
	if (reason.empty()) {
		return;
	}
	if (!aggregate.reason.empty()) {
		aggregate.reason += ";";
	}
	aggregate.reason += std::move(reason);
}

static ExecutionRegionAggregateInput
BuildExecutionContractAggregateInput(idx_t aggregate_idx, const unique_ptr<Expression> &aggregate_expression,
                                     bool allow_distinct_update = false) {
	ExecutionRegionAggregateInput result;
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
	if (function.HasPrimitiveUpdateABI()) {
		auto &abi = function.GetPrimitiveUpdateABI();
		result.primitive_update_ready = abi.IsReady();
		result.primitive_update_kind = abi.kind;
		result.primitive_update_input_type = abi.input_type;
		result.primitive_update_state_size = abi.state_size;
		result.primitive_update_state_value_offset = abi.state_value_offset;
		result.primitive_update_state_is_set_offset = abi.state_is_set_offset;
		if (!result.primitive_update_ready) {
			result.primitive_update_blocker = "aggregate primitive update ABI is not ready";
		}
	} else {
		result.primitive_update_blocker = "aggregate function has no primitive update ABI";
	}

	if (result.distinct && !allow_distinct_update) {
		result.reason = "distinct aggregate update is handled by DuckDB distinct aggregate finalization";
	} else if (result.has_filter) {
		result.reason = "aggregate filter requires per-aggregate filtered payload contract";
	} else if (result.has_order_bys) {
		result.reason = "ordered aggregate requires DuckDB sorted aggregate contract";
	} else if (!result.has_state_update) {
		result.reason = "aggregate function has no state update callback";
	} else {
		TryReadExecutionContractAggregateInput(aggregate, result);
	}
	return result;
}

static vector<ExecutionRegionAggregateInput>
BuildExecutionContractUngroupedAggregateInputs(const PhysicalUngroupedAggregate &op) {
	vector<ExecutionRegionAggregateInput> result;
	result.reserve(op.aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < op.aggregates.size(); aggregate_idx++) {
		result.push_back(BuildExecutionContractAggregateInput(aggregate_idx, op.aggregates[aggregate_idx]));
	}
	return result;
}

static vector<ExecutionRegionAggregateInput>
BuildExecutionContractHashAggregateInputs(const PhysicalHashAggregate &op) {
	vector<ExecutionRegionAggregateInput> result;
	auto &aggregates = op.grouped_aggregate_data.aggregates;
	result.reserve(aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		result.push_back(BuildExecutionContractAggregateInput(aggregate_idx, aggregates[aggregate_idx], true));
	}
	return result;
}

static vector<ExecutionRegionAggregateInput>
BuildExecutionContractPerfectHashAggregateInputs(const PhysicalPerfectHashAggregate &op) {
	vector<ExecutionRegionAggregateInput> result;
	result.reserve(op.aggregates.size());
	for (idx_t aggregate_idx = 0; aggregate_idx < op.aggregates.size(); aggregate_idx++) {
		result.push_back(BuildExecutionContractAggregateInput(aggregate_idx, op.aggregates[aggregate_idx]));
	}
	return result;
}

static ExecutionRegionHashJoinKeyInput BuildExecutionContractHashJoinBuildKeyInput(const PhysicalHashJoin &op,
                                                                                   idx_t key_idx) {
	ExecutionRegionHashJoinKeyInput result;
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

static ExecutionRegionHashJoinKeyInput BuildExecutionContractHashJoinProbeKeyInput(const PhysicalHashJoin &op,
                                                                                   idx_t key_idx) {
	ExecutionRegionHashJoinKeyInput result;
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

static vector<ExecutionRegionHashJoinKeyInput>
BuildExecutionContractHashJoinBuildKeyInputs(const PhysicalHashJoin &op) {
	vector<ExecutionRegionHashJoinKeyInput> result;
	result.reserve(op.conditions.size());
	for (idx_t key_idx = 0; key_idx < op.conditions.size(); key_idx++) {
		result.push_back(BuildExecutionContractHashJoinBuildKeyInput(op, key_idx));
	}
	return result;
}

static vector<ExecutionRegionHashJoinKeyInput>
BuildExecutionContractHashJoinProbeKeyInputs(const PhysicalHashJoin &op) {
	vector<ExecutionRegionHashJoinKeyInput> result;
	result.reserve(op.conditions.size());
	for (idx_t key_idx = 0; key_idx < op.conditions.size(); key_idx++) {
		result.push_back(BuildExecutionContractHashJoinProbeKeyInput(op, key_idx));
	}
	return result;
}

static ExecutionRegionGroupInput BuildExecutionContractGroupInput(const vector<unique_ptr<Expression>> &groups,
                                                                  const vector<LogicalType> &group_types,
                                                                  const string &operator_name, idx_t group_idx) {
	ExecutionRegionGroupInput result;
	result.group_index = group_idx;
	result.type = group_types[group_idx];
	auto &group_expression = *groups[group_idx];
	auto lowered_expression =
	    TryLowerExecutionExpression(group_expression, group_idx, ExecutionExpressionIRMode::COMPACT);
	if (lowered_expression) {
		result.expression = std::move(*lowered_expression);
		result.expression_ready = true;
	} else {
		result.expression_blocker = operator_name + " group expression lowering unsupported;" +
		                            DescribeExecutionExpressionLoweringFailure(group_expression);
		result.reason = result.expression_blocker;
	}
	if (group_expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &bound_ref = group_expression.Cast<BoundReferenceExpression>();
		result.input_index = bound_ref.Index();
		result.supported_reference = true;
	}
	if (!result.supported_reference && result.expression_ready) {
		result.reason.clear();
	}
	return result;
}

static vector<ExecutionRegionGroupInput> BuildExecutionContractGroupInputs(const PhysicalHashAggregate &op) {
	vector<ExecutionRegionGroupInput> result;
	auto group_count = op.grouped_aggregate_data.GroupCount();
	result.reserve(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		result.push_back(BuildExecutionContractGroupInput(
		    op.grouped_aggregate_data.groups, op.grouped_aggregate_data.group_types, "hash aggregate", group_idx));
	}
	return result;
}

static vector<ExecutionRegionGroupInput> BuildExecutionContractGroupInputs(const PhysicalPerfectHashAggregate &op) {
	vector<ExecutionRegionGroupInput> result;
	result.reserve(op.groups.size());
	for (idx_t group_idx = 0; group_idx < op.groups.size(); group_idx++) {
		result.push_back(
		    BuildExecutionContractGroupInput(op.groups, op.group_types, "perfect hash aggregate", group_idx));
	}
	return result;
}

static void AddExecutionContractTableScanSourceFilters(const PhysicalTableScan &scan, ExecutionSourceContract &source) {
	if (!scan.table_filters) {
		return;
	}
	idx_t filter_index = 0;
	for (auto &entry : *scan.table_filters) {
		ExecutionSourceFilterContract filter;
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

ExecutionContract PhysicalTableScan::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                          bool render_diagnostics) const {
	ExecutionContract result;
	auto capability = GetExecutionSourceContractCapability(*this);
	auto table_scan_contract = BuildExecutionContractTableScanContract(*this, capability);
	result.source.kind = capability.kind;
	result.source.execution = capability.execution;
	if (result.source.execution == ExecutionRegionSourceExecutionKind::NONE) {
		result.source.execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
	}
	if (render_diagnostics) {
		result.source_boundary_reason =
		    BuildExecutionContractTableScanSourceBoundaryReason(table_scan_contract, result.source.execution);
	}
	result.source.source_contract = BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
	result.source.reason = result.source_boundary_reason;
	result.source.function_name = table_scan_contract.function_name;
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = returned_types.size();
	result.source.column_ids = BuildExecutionContractColumnIndexList(column_ids);
	result.source.projection_ids = projection_ids;
	result.source.projection_pushdown = function.projection_pushdown;
	result.source.filter_pushdown = function.filter_pushdown;
	result.source.filter_prune = function.filter_prune;
	result.source.dynamic_filters = dynamic_filters && dynamic_filters->HasFilters();
	result.source.in_out_function = static_cast<bool>(function.in_out_function);
	result.source.table_scan_contract = std::move(table_scan_contract);
	result.source.estimated_source_cardinality = result.source.table_scan_contract.estimated_source_cardinality;
	AddExecutionContractTableScanSourceFilters(*this, result.source);
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalColumnDataScan::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                               bool render_diagnostics) const {
	ExecutionContract result;
	if (type != PhysicalOperatorType::CTE_SCAN && type != PhysicalOperatorType::COLUMN_DATA_SCAN) {
		return FinalizeExecutionContract(std::move(result));
	}
	result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	result.source.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	result.source.source_contract = BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
	result.source.function_name = StringUtil::Lower(PhysicalOperatorToString(type));
	result.source.output_column_count = GetTypes().size();
	result.source.returned_column_count = GetTypes().size();
	if (render_diagnostics) {
		result.source_boundary_reason = "DuckDB column data source contract";
		result.source_boundary_reason += ";operator=" + PhysicalOperatorToString(type);
		result.source_boundary_reason += ";function=" + result.source.function_name;
		result.source_boundary_reason += ";output_columns=" + std::to_string(result.source.output_column_count);
		result.source_boundary_reason += ";returned_columns=" + std::to_string(result.source.returned_column_count);
	}
	if (collection) {
		result.source.estimated_source_cardinality = collection->Count();
		result.source.estimated_source_cardinality_exact = true;
		if (render_diagnostics) {
			result.source_boundary_reason +=
			    ";column_data_count=" + std::to_string(result.source.estimated_source_cardinality);
		}
	}
	result.source.reason = result.source_boundary_reason;
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalHashJoin::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                         bool render_diagnostics) const {
	ExecutionContract result;
	auto contract = BuildExecutionContractHashJoinContract(*this);
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE: {
		auto state_scan_contract =
		    BuildExecutionContractNativeStateScanContract("hash-join-native-state-scan", string());
		if (contract.source_produces_rows) {
			MarkExecutionContractNativeStateScanContractReady(state_scan_contract);
		} else {
			MarkExecutionContractNativeStateScanContractBlocked(state_scan_contract,
			                                                    HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER);
		}
		if (render_diagnostics) {
			result.source_boundary_reason = BuildExecutionContractHashJoinBoundaryReason(
			    *this, contract,
			    contract.source_produces_rows ? "DuckDB hash join native state scan contract"
			                                  : "DuckDB hash join state scan source does not produce rows");
		}
		result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		result.source.execution = contract.source_produces_rows
		                              ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		                              : ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
		result.source.source_contract =
		    BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
		if (!contract.source_produces_rows) {
			result.source.source_contract.blocker = HASH_JOIN_SOURCE_NON_PRODUCING_BLOCKER;
		}
		result.source.native_state_scan_contract = std::move(state_scan_contract);
		AppendExecutionContractNativeStateScanReason(result.source_boundary_reason,
		                                             result.source.native_state_scan_contract);
		result.source.function_name = "hash_join_probe";
		result.source.output_column_count = GetTypes().size();
		result.source.returned_column_count = GetTypes().size();
		result.source.reason = result.source_boundary_reason;
		result.source.hash_join_contract = std::move(contract);
		result.source.hash_join_keys = BuildExecutionContractHashJoinProbeKeyInputs(*this);
		break;
	}
	case ExecutionRegionOperatorSlot::OPERATOR:
		result.operator_info.kind = ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE;
		if (render_diagnostics) {
			result.operator_info.reason = BuildExecutionContractHashJoinBoundaryReason(
			    *this, contract, "DuckDB hash join probe operator contract boundary");
		}
		result.operator_info.hash_join_contract = std::move(contract);
		result.operator_info.hash_join_keys = BuildExecutionContractHashJoinProbeKeyInputs(*this);
		break;
	case ExecutionRegionOperatorSlot::SINK:
		result.sink.kind = ExecutionRegionSinkKind::HASH_JOIN_BUILD;
		if (render_diagnostics) {
			result.sink.reason =
			    BuildExecutionContractHashJoinBoundaryReason(*this, contract, "DuckDB hash join build sink contract");
		}
		result.sink.hash_join_contract = std::move(contract);
		result.sink.hash_join_keys = BuildExecutionContractHashJoinBuildKeyInputs(*this);
		break;
	default:
		break;
	}
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalNestedLoopJoin::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                               bool render_diagnostics) const {
	ExecutionContract result;
	auto contract = BuildExecutionContractNestedLoopJoinContract(*this);
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		if (render_diagnostics) {
			result.source_boundary_reason = BuildExecutionContractNestedLoopJoinBoundaryReason(
			    *this, contract,
			    contract.source_produces_rows ? "DuckDB nested loop join state scan boundary"
			                                  : "DuckDB nested loop join source does not produce rows");
		}
		result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		result.source.execution = ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
		result.source.source_contract =
		    BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
		result.source.source_contract.blocker = contract.source_produces_rows
		                                            ? "nested-loop-join-native-state-scan-contract-missing"
		                                            : "nested-loop-join-source-does-not-produce-rows-for-join-type";
		result.source.function_name = "nested_loop_join_scan";
		result.source.output_column_count = GetTypes().size();
		result.source.returned_column_count = GetTypes().size();
		result.source.reason = result.source_boundary_reason;
		result.source.nested_loop_join_contract = std::move(contract);
		break;
	case ExecutionRegionOperatorSlot::OPERATOR:
		result.operator_info.kind = ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE;
		if (render_diagnostics) {
			result.operator_info.reason = BuildExecutionContractNestedLoopJoinBoundaryReason(
			    *this, contract, "DuckDB nested loop join probe operator contract");
		}
		result.operator_info.nested_loop_join_contract = std::move(contract);
		break;
	case ExecutionRegionOperatorSlot::SINK:
		result.sink.kind = ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD;
		if (render_diagnostics) {
			result.sink.reason = BuildExecutionContractNestedLoopJoinBoundaryReason(
			    *this, contract, "DuckDB nested loop join build sink contract");
		}
		result.sink.nested_loop_join_contract = std::move(contract);
		break;
	default:
		break;
	}
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalOrder::GetExecutionContract(ExecutionRegionOperatorSlot slot, bool render_diagnostics) const {
	auto result = BuildExecutionContractSortStateContracts(
	    type, orders, children[0].get().types, projections, GetTypes().size(), "order_by_scan",
	    "order-by-native-state-scan", "DuckDB order by native state scan contract", is_index_sort, false, 0, 0, false);
	result.source_boundary_reason += ";is_index_sort=" + ExecutionContractBool(is_index_sort);
	ApplyExecutionContractFinalizedSourceCardinality(result, FinalizedSourceCardinality());
	result.source.reason = result.source_boundary_reason;
	result.sink.reason += ";is_index_sort=" + ExecutionContractBool(is_index_sort);
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalTopN::GetExecutionContract(ExecutionRegionOperatorSlot slot, bool render_diagnostics) const {
	vector<idx_t> projections;
	auto result = BuildExecutionContractSortStateContracts(
	    type, orders, GetTypes(), projections, GetTypes().size(), "top_n_scan", "top-n-native-state-scan",
	    "DuckDB top-n native state scan contract", false, true, limit, offset, static_cast<bool>(dynamic_filter));
	result.source_boundary_reason += ";limit=" + std::to_string(limit);
	result.source_boundary_reason += ";offset=" + std::to_string(offset);
	result.source_boundary_reason += ";dynamic_filter=" + ExecutionContractBool(static_cast<bool>(dynamic_filter));
	ApplyExecutionContractFinalizedSourceCardinality(result, FinalizedSourceCardinality());
	result.source.reason = result.source_boundary_reason;
	result.sink.reason += ";limit=" + std::to_string(limit);
	result.sink.reason += ";offset=" + std::to_string(offset);
	result.sink.reason += ";dynamic_filter=" + ExecutionContractBool(static_cast<bool>(dynamic_filter));
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalHashAggregate::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                              bool render_diagnostics) const {
	ExecutionContract result;
	auto contract = BuildExecutionContractHashAggregateContract(*this);
	auto sink_aggregates = BuildExecutionContractHashAggregateInputs(*this);
	auto sink_groups = BuildExecutionContractGroupInputs(*this);
	MarkExecutionContractAggregateStateUpdateContract(contract, sink_aggregates, sink_groups);
	if (slot == ExecutionRegionOperatorSlot::SOURCE) {
		auto state_scan_contract =
		    BuildExecutionContractNativeStateScanContract("hash-aggregate-native-state-scan", string());
		MarkExecutionContractNativeStateScanContractReady(state_scan_contract);
		if (render_diagnostics) {
			result.source_boundary_reason = BuildExecutionContractHashAggregateBoundaryReason(
			    *this, "DuckDB hash aggregate native state scan contract");
		}
		result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		result.source.execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
		result.source.source_contract =
		    BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
		result.source.native_state_scan_contract = std::move(state_scan_contract);
		AppendExecutionContractNativeStateScanReason(result.source_boundary_reason,
		                                             result.source.native_state_scan_contract);
		AppendExecutionContractGroupedStateLayoutReason(result.source_boundary_reason, contract);
		result.source.function_name = "hash_aggregate_scan";
		result.source.output_column_count = GetTypes().size();
		result.source.returned_column_count = GetTypes().size();
		result.source.reason = result.source_boundary_reason;
		result.source.aggregate_contract = std::move(contract);
		result.source.aggregates = std::move(sink_aggregates);
		result.source.groups = std::move(sink_groups);
		ApplyExecutionContractFinalizedSourceCardinality(result, FinalizedSourceCardinality());
	} else if (slot == ExecutionRegionOperatorSlot::SINK) {
		result.sink.kind = ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
		if (render_diagnostics) {
			result.sink.reason =
			    BuildExecutionContractHashAggregateBoundaryReason(*this, "DuckDB hash aggregate sink update contract");
		}
		AppendExecutionContractNativeOperatorReason(result.sink.reason, contract.native_state_update_contract,
		                                            "aggregate_state_update");
		result.sink.aggregate_contract = std::move(contract);
		result.sink.aggregates = std::move(sink_aggregates);
		result.sink.groups = std::move(sink_groups);
	}
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalPerfectHashAggregate::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                                     bool render_diagnostics) const {
	ExecutionContract result;
	auto contract = BuildExecutionContractPerfectHashAggregateContract(*this);
	auto sink_aggregates = BuildExecutionContractPerfectHashAggregateInputs(*this);
	auto sink_groups = BuildExecutionContractGroupInputs(*this);
	MarkExecutionContractAggregateStateUpdateContract(contract, sink_aggregates, sink_groups);
	if (slot == ExecutionRegionOperatorSlot::SOURCE) {
		auto state_scan_contract = BuildExecutionContractNativeStateScanContract(
		    "perfect-hash-aggregate-native-state-scan", "aggregate-state-scan-contract-boundary");
		MarkExecutionContractPerfectHashAggregateStateScanContract(state_scan_contract, contract);
		if (render_diagnostics) {
			result.source_boundary_reason = BuildExecutionContractPerfectHashAggregateBoundaryReason(
			    *this, state_scan_contract.status == ExecutionRegionStateContractStatus::READY
			               ? "DuckDB perfect hash aggregate native state scan contract"
			               : "DuckDB aggregate source state contract missing");
		}
		result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		result.source.execution = state_scan_contract.status == ExecutionRegionStateContractStatus::READY
		                              ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		                              : ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
		result.source.source_contract =
		    BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
		result.source.native_state_scan_contract = std::move(state_scan_contract);
		AppendExecutionContractNativeStateScanReason(result.source_boundary_reason,
		                                             result.source.native_state_scan_contract);
		AppendExecutionContractGroupedStateLayoutReason(result.source_boundary_reason, contract);
		result.source.function_name = "perfect_hash_aggregate_scan";
		result.source.output_column_count = GetTypes().size();
		result.source.returned_column_count = GetTypes().size();
		result.source.reason = result.source_boundary_reason;
		result.source.aggregate_contract = std::move(contract);
		result.source.aggregates = std::move(sink_aggregates);
		result.source.groups = std::move(sink_groups);
		ApplyExecutionContractFinalizedSourceCardinality(result, FinalizedSourceCardinality());
	} else if (slot == ExecutionRegionOperatorSlot::SINK) {
		result.sink.kind = ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
		if (render_diagnostics) {
			result.sink.reason = BuildExecutionContractPerfectHashAggregateBoundaryReason(
			    *this, "DuckDB perfect hash aggregate sink update contract");
		}
		AppendExecutionContractNativeOperatorReason(result.sink.reason, contract.native_state_update_contract,
		                                            "aggregate_state_update");
		result.sink.aggregate_contract = std::move(contract);
		result.sink.aggregates = std::move(sink_aggregates);
		result.sink.groups = std::move(sink_groups);
	}
	return FinalizeExecutionContract(std::move(result));
}

ExecutionContract PhysicalUngroupedAggregate::GetExecutionContract(ExecutionRegionOperatorSlot slot,
                                                                   bool render_diagnostics) const {
	ExecutionContract result;
	auto contract = BuildExecutionContractUngroupedAggregateContract(*this);
	auto sink_aggregates = BuildExecutionContractUngroupedAggregateInputs(*this);
	vector<ExecutionRegionGroupInput> sink_groups;
	MarkExecutionContractAggregateStateUpdateContract(contract, sink_aggregates, sink_groups);
	if (slot == ExecutionRegionOperatorSlot::SOURCE) {
		auto state_scan_contract = BuildExecutionContractNativeStateScanContract(
		    "ungrouped-aggregate-native-state-scan", "aggregate-state-scan-contract-boundary");
		MarkExecutionContractUngroupedAggregateStateScanContract(state_scan_contract, contract);
		if (render_diagnostics) {
			result.source_boundary_reason = BuildExecutionContractUngroupedAggregateBoundaryReason(
			    *this, state_scan_contract.status == ExecutionRegionStateContractStatus::READY
			               ? "DuckDB ungrouped aggregate native state scan contract"
			               : "DuckDB aggregate source state contract missing");
		}
		result.source.kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		result.source.execution = state_scan_contract.status == ExecutionRegionStateContractStatus::READY
		                              ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		                              : ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY;
		result.source.source_contract =
		    BuildExecutionSourceProtocolContract(result.source.kind, result.source.execution);
		result.source.native_state_scan_contract = std::move(state_scan_contract);
		AppendExecutionContractNativeStateScanReason(result.source_boundary_reason,
		                                             result.source.native_state_scan_contract);
		result.source.function_name = "ungrouped_aggregate_scan";
		result.source.output_column_count = GetTypes().size();
		result.source.returned_column_count = GetTypes().size();
		result.source.reason = result.source_boundary_reason;
		result.source.aggregate_contract = std::move(contract);
		result.source.aggregates = std::move(sink_aggregates);
	} else if (slot == ExecutionRegionOperatorSlot::SINK) {
		result.sink.kind = ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
		if (render_diagnostics) {
			result.sink.reason = BuildExecutionContractUngroupedAggregateBoundaryReason(
			    *this, "DuckDB ungrouped aggregate payload update contract");
		}
		AppendExecutionContractNativeOperatorReason(result.sink.reason, contract.native_state_update_contract,
		                                            "aggregate_state_update");
		result.sink.aggregate_contract = std::move(contract);
		result.sink.aggregates = std::move(sink_aggregates);
		result.sink.groups = std::move(sink_groups);
	}
	return FinalizeExecutionContract(std::move(result));
}

} // namespace duckdb

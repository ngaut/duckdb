#include "execution_region_signature.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

static void AddExecutionRegionSignatureFeature(vector<string> &features, string feature) {
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

static string BuildExecutionRegionSignatureFeatureSetShape(vector<string> features) {
	std::sort(features.begin(), features.end());
	features.erase(std::unique(features.begin(), features.end()), features.end());
	return StringUtil::Join(features, "+");
}

static string ExecutionRegionSignatureBool(bool value) {
	return value ? "true" : "false";
}

static string BuildExecutionRegionSignatureStringList(const vector<string> &values) {
	string result = "[" + std::to_string(values.size()) + ":";
	for (auto &value : values) {
		result += std::to_string(value.size()) + ":" + value + ";";
	}
	result += "]";
	return result;
}

static string ExecutionHashJoinProbeOutputModeSignatureString(ExecutionHashJoinProbeOutputMode mode) {
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
	result += ",dynamic=" + ExecutionRegionSignatureBool(dynamic_filters);
	result += ",inout=" + ExecutionRegionSignatureBool(in_out_function);
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
	result += ",probe=" + ExecutionHashJoinProbeOutputModeSignatureString(contract.native_probe_output_mode);
	result += ",build_filter=" + ExecutionRegionSignatureBool(contract.build_side_has_filter);
	result += ",filter_conditions=" + std::to_string(contract.filter_pushdown_condition_count);
	result += ",delim=" + std::to_string(contract.delim_type_count);
	result += ",residual=" + ExecutionRegionSignatureBool(contract.residual_predicate || contract.residual_info);
	result += ",residual_ready=" + ExecutionRegionSignatureBool(contract.residual_expression_ready);
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
	result += ",simple=" + ExecutionRegionSignatureBool(contract.simple_join);
	result += ",complex=" + ExecutionRegionSignatureBool(contract.complex_join);
	result += ",source=" + ExecutionRegionSignatureBool(contract.source_produces_rows);
	result += ",conditions_ready=" + ExecutionRegionSignatureBool(contract.conditions_ready);
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
	result += ",functions=" + BuildExecutionRegionSignatureStringList(contract.aggregate_functions);
	result += ",payloads=" + std::to_string(contract.payload_type_count);
	result += ",distinct=" + std::to_string(contract.distinct_aggregate_count);
	result += ",filters=" + std::to_string(contract.aggregate_filter_count);
	result += ",orders=" + std::to_string(contract.aggregate_order_count);
	result += ",state_layout=" + ExecutionRegionSignatureBool(contract.grouped_state_layout_ready);
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
	result += ",limit=" + ExecutionRegionSignatureBool(contract.has_limit);
	if (contract.has_limit) {
		result += ":" + std::to_string(contract.limit);
		result += ":offset=" + std::to_string(contract.offset);
	}
	result += ",dynamic_filter=" + ExecutionRegionSignatureBool(contract.dynamic_filter);
	result += ",index_sort=" + ExecutionRegionSignatureBool(contract.is_index_sort);
	result += ",keys_ready=" + ExecutionRegionSignatureBool(contract.all_order_keys_ready);
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

string DescribeExecutionRegionPipelineShape(const ExecutionRegionIR &region_ir) {
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

string DescribeExecutionRegionCandidateShape(const ExecutionRegionIR &region_ir) {
	string result;
	for (auto &node : region_ir.nodes) {
		if (node.kind == ExecutionRegionNodeKind::SOURCE) {
			AppendExecutionRegionSourceShapeSegments(result, node);
			continue;
		}
		AppendExecutionRegionCandidateShapeSegment(
		    result, StringUtil::Lower(string(ExecutionRegionNodeKindToString(node.kind))));
	}
	return result.empty() ? "boundary-only" : result;
}

static string GetExecutionRegionSignatureContext(ExecutionRegionABI abi) {
	if (ExecutionRegionABIIsFullPipeline(abi)) {
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

static string BuildExecutionRegionFeatureShape(const ExecutionRegionIR &region_ir) {
	vector<string> features;
	for (auto &node : region_ir.nodes) {
		AddExecutionRegionSignatureFeature(features, GetExecutionRegionNodeSignatureFeature(node));
	}
	return BuildExecutionRegionSignatureFeatureSetShape(std::move(features));
}

static string BuildExecutionRegionContractShape(const ExecutionRegionIR &region_ir) {
	string result;
	for (auto &node : region_ir.nodes) {
		switch (node.kind) {
		case ExecutionRegionNodeKind::SOURCE:
			if (node.source) {
				AppendExecutionRegionContractShapeEntry(result,
				                                        DescribeExecutionSourceProtocolContractShape(*node.source));
			}
			break;
		case ExecutionRegionNodeKind::OPERATOR:
			if (node.operator_info) {
				AppendExecutionRegionContractShapeEntry(
				    result, DescribeExecutionRegionOperatorContractShape(*node.operator_info));
			}
			break;
		case ExecutionRegionNodeKind::SINK:
			if (node.sink) {
				AppendExecutionRegionContractShapeEntry(result, DescribeExecutionRegionSinkContractShape(*node.sink));
			}
			break;
		default:
			break;
		}
	}
	return result;
}

ExecutionRegionSignature BuildExecutionRegionSignature(const ExecutionRegionIR &region_ir,
                                                       const ExecutionRegionCandidate &candidate) {
	ExecutionRegionSignature signature;
	signature.context = GetExecutionRegionSignatureContext(candidate.abi);
	signature.shape = candidate.shape;
	signature.feature_shape = BuildExecutionRegionFeatureShape(region_ir);
	signature.context_feature_shape = signature.feature_shape;
	signature.contract_shape = BuildExecutionRegionContractShape(region_ir);
	return signature;
}

} // namespace duckdb

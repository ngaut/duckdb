//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_description.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"
#include "sljit_region_plan_internal.hpp"

namespace duckdb {

const char *SljitNativeRegionOpKindName(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::FILTER:
		return "filter";
	case SljitNativeRegionOpKind::PROJECTION:
		return "projection";
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return "nested-loop-join-probe";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return "nested-loop-join-build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order-sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append-sink";
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return "delim-join-sink";
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return "aggregate-update";
	default:
		return "unknown";
	}
}

static string BuildSljitNativeRegionShape(const SljitNativeRegionPlan &region) {
	string result;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		if (op_idx > 0) {
			result += "-";
		}
		result += SljitNativeRegionOpKindName(region.ops[op_idx].kind);
	}
	return result.empty() ? "empty" : result;
}

static string DescribeSljitLogicalTypes(const vector<LogicalType> &types) {
	string result;
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += types[type_idx].ToString();
	}
	return result;
}

static string DescribeSljitLogicalTypesSemantic(const vector<LogicalType> &types) {
	string result;
	for (auto &type : types) {
		auto value = type.ToString();
		result += std::to_string(value.size()) + ":" + value + ";";
	}
	return result;
}

static const char *SljitHashJoinKeyKindToString(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return "int8";
	case SljitNativeHashJoinKeyKind::INT16:
		return "int16";
	case SljitNativeHashJoinKeyKind::INT32:
		return "int32";
	case SljitNativeHashJoinKeyKind::INT64:
		return "int64";
	case SljitNativeHashJoinKeyKind::INT128:
		return "int128";
	case SljitNativeHashJoinKeyKind::UINT8:
		return "uint8";
	case SljitNativeHashJoinKeyKind::UINT16:
		return "uint16";
	case SljitNativeHashJoinKeyKind::UINT32:
		return "uint32";
	case SljitNativeHashJoinKeyKind::UINT64:
		return "uint64";
	case SljitNativeHashJoinKeyKind::UINT128:
		return "uint128";
	default:
		return "unknown";
	}
}

const char *SljitHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode) {
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

const char *SljitHashJoinComparisonToString(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return "equal";
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return "notequal";
	case ExecutionRegionComparisonType::LESS_THAN:
		return "lessthan";
	case ExecutionRegionComparisonType::GREATER_THAN:
		return "greaterthan";
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return "lessthanorequalto";
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return "greaterthanorequalto";
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return "not_distinct_from";
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return "distinct_from";
	default:
		return "unsupported";
	}
}

static string DescribeSljitHashJoinProbeKey(idx_t key_idx, const SljitNativeHashJoinProbeKeyPlan &key) {
	string result = key.equality_key ? "key" : "predicate";
	result += std::to_string(key_idx);
	result += "<input_index=" + std::to_string(key.key_input_index);
	result += ",kind=" + string(SljitHashJoinKeyKindToString(key.key_kind));
	result += ",layout_offset=" + std::to_string(key.key_layout_offset);
	result += ",comparison=" + string(SljitHashJoinComparisonToString(key.comparison_type));
	result += key.null_equal ? ",null_equal=true" : ",null_equal=false";
	result += key.source_known_not_null ? ",source_not_null=true>" : ",source_not_null=false>";
	return result;
}

string DescribeSljitHashJoinProbeKeys(const vector<SljitNativeHashJoinProbeKeyPlan> &keys, const char *separator) {
	string result;
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if (key_idx > 0) {
			result += separator;
		}
		result += DescribeSljitHashJoinProbeKey(key_idx, keys[key_idx]);
	}
	return result;
}

void AppendSljitHashJoinProbeMarkOffsets(string &result, const char *name, const SljitNativeHashJoinProbePlan &probe) {
	result += ",";
	result += name;
	result += "=true";
	result += ",found_match_offset=" + std::to_string(probe.found_match_offset);
	result += ",pointer_offset=" + std::to_string(probe.pointer_offset);
}

const char *SljitNestedLoopJoinValueKindToString(SljitNativeNestedLoopJoinValueKind kind) {
	switch (kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return "int32";
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return "int64";
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return "int128";
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return "double";
	default:
		return "unknown";
	}
}

static string
DescribeNativeNestedLoopJoinProbeConditions(const vector<SljitNativeNestedLoopJoinProbeConditionPlan> &conditions,
                                            bool semantic) {
	string result;
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += ",";
		}
		auto &condition = conditions[condition_idx];
		result += "condition" + std::to_string(condition_idx);
		result += "<kind=" + string(SljitNestedLoopJoinValueKindToString(condition.value_kind));
		result += ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type));
		result += ",lhs=" +
		          (semantic ? DescribeNativeRegionExpressionSemantic(condition.lhs_condition)
		                    : DescribeNativeRegionExpression(condition.lhs_condition)) +
		          ">";
	}
	return result;
}

static string DescribeNativeRegionInternal(const SljitNativeRegionPlan &region, const string &mode, bool semantic) {
	string result = "sljit.region " + mode;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		result += ";op" + std::to_string(op_idx) + "=";
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			result += "filter(" +
			          (semantic ? DescribeNativeRegionExpressionSemantic(op.filter)
			                    : DescribeNativeRegionExpression(op.filter)) +
			          ")";
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			result += "projection(" +
			          (semantic ? DescribeNativeRegionExpressionListSemantic(op.projections)
			                    : DescribeNativeRegionExpressionList(op.projections)) +
			          ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			result +=
			    "hash_join_probe(hash_keys=" + std::to_string(op.hash_join_probe.equality_key_count) + ",conditions=";
			result += DescribeSljitHashJoinProbeKeys(op.hash_join_probe.keys, ",");
			if (op.hash_join_probe.mark_build_match) {
				AppendSljitHashJoinProbeMarkOffsets(result, "mark_build_match", op.hash_join_probe);
			}
			if (op.hash_join_probe.mark_build_match_after_residual) {
				AppendSljitHashJoinProbeMarkOffsets(result, "mark_build_match_after_residual", op.hash_join_probe);
			}
			if (op.hash_join_probe.perfect_hash_probe) {
				result += ",perfect_hash_probe_shape=native";
			}
			result += op.hash_join_probe.exact_source_filter_binding != DConstants::INVALID_INDEX
			              ? ",exact_source_filter_proof=true"
			              : ",exact_source_filter_proof=false";
			result += ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(op.hash_join_probe.output_mode));
			if (op.hash_join_probe.residual_predicate) {
				result += ",residual_predicate=true";
				result += ",residual=" +
				          (semantic ? DescribeNativeRegionExpressionSemantic(op.hash_join_probe.residual_filter)
				                    : DescribeNativeRegionExpression(op.hash_join_probe.residual_filter));
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			result += "hash_join_build(keys=";
			result += std::to_string(op.hash_join_build.sink_info.hash_join_keys.size());
			result += ";payload_columns=" + std::to_string(op.hash_join_build.input_types.size());
			result += ";execution=primitive-protocol-build";
			result += ")";
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			result += "nested_loop_join_probe(conditions=" +
			          DescribeNativeNestedLoopJoinProbeConditions(op.nested_loop_join_probe.conditions, semantic);
			result += ")";
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			result += "nested_loop_join_build(conditions=" +
			          (semantic ? DescribeNativeRegionExpressionListSemantic(op.nested_loop_join_build.rhs_conditions)
			                    : DescribeNativeRegionExpressionList(op.nested_loop_join_build.rhs_conditions));
			result += ";payload_columns=" + std::to_string(op.nested_loop_join_build.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::APPEND_SINK:
			result += "append_sink(columns=";
			result += std::to_string(op.append_sink.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			result += "delim_join_sink(columns=";
			result += std::to_string(op.delim_join_sink.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			result += "aggregate_update(kind=";
			result += string(
			    ExecutionRegionAggregateOperatorKindToString(op.aggregate_update.sink_info.aggregate_contract.kind));
			result += ";columns=" + std::to_string(op.aggregate_update.input_types.size());
			result += ";input_types=[" +
			          (semantic ? DescribeSljitLogicalTypesSemantic(op.aggregate_update.input_types)
			                    : DescribeSljitLogicalTypes(op.aggregate_update.input_types)) +
			          "]";
			result += ";groups=" + std::to_string(op.aggregate_update.sink_info.groups.size());
			result += ";aggregates=" + std::to_string(op.aggregate_update.sink_info.aggregates.size());
			if (!semantic && !op.aggregate_update.ir.empty()) {
				result += ";diagnostics=" + op.aggregate_update.ir;
			}
			if (op.aggregate_update.UsesPrimitivePayloads()) {
				result += ";payload_update=generated-primitive";
				result += op.aggregate_update.PayloadsWereComposedThroughProjection()
				              ? ";payload_binding=projection-composed"
				              : ";payload_binding=direct";
				result += ";primitive_payloads=" +
				          (semantic ? DescribeNativeRegionExpressionListSemantic(op.aggregate_update.payloads)
				                    : DescribeNativeRegionExpressionList(op.aggregate_update.payloads));
				if (op.aggregate_update.use_perfect_hash_group_lookup) {
					result += ";grouped_state_lookup=generated-perfect-hash";
					if (!op.aggregate_update.group_expressions.empty()) {
						result +=
						    ";group_expressions=" +
						    (semantic
						         ? DescribeNativeRegionExpressionListSemantic(op.aggregate_update.group_expressions)
						         : DescribeNativeRegionExpressionList(op.aggregate_update.group_expressions));
					}
				} else if (op.aggregate_update.use_grouped_state_addresses) {
					result += ";grouped_state_lookup=native-state-address";
				}
			} else {
				result += ";payload_update=native-aggregate-contract";
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			result +=
			    "ordered_sink(keys=" + (semantic ? DescribeNativeRegionExpressionListSemantic(op.order_sink.order_keys)
			                                     : DescribeNativeRegionExpressionList(op.order_sink.order_keys));
			result += ";payload_columns=" + std::to_string(op.order_sink.input_types.size());
			result += ";operator_kind=" +
			          string(ExecutionRegionOperatorKindToString(op.order_sink.sink_info.order_contract.kind));
			result += ")";
			break;
		default:
			result += "unknown";
			break;
		}
		if (semantic) {
			result += "<operator_index=" + std::to_string(op.operator_index);
			result += ",input_types=[" + DescribeSljitLogicalTypesSemantic(op.input_types) + "]";
			result += ",output_types=[" + DescribeSljitLogicalTypesSemantic(op.output_types) + "]>";
		}
	}
	return result;
}

string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode) {
	return DescribeNativeRegionInternal(region, mode, false);
}

static void AppendSljitArtifactKeyField(string &result, const string &name, const string &value) {
	result += name;
	result += "=" + std::to_string(value.size()) + ":";
	result += value;
	result += ";";
}

string BuildSljitRegionArtifactSemanticKey(const ExecutionRegionCandidate &candidate,
                                           const SljitNativeRegionPlan &region) {
	string result = "sljit-artifact-v3";
	AppendSljitArtifactKeyField(result, "context", candidate.signature.context);
	AppendSljitArtifactKeyField(result, "shape", candidate.signature.shape);
	AppendSljitArtifactKeyField(result, "features", candidate.signature.feature_shape);
	AppendSljitArtifactKeyField(result, "context_features", candidate.signature.context_feature_shape);
	AppendSljitArtifactKeyField(result, "contract", candidate.signature.contract_shape);
	result += ";scan_filter_mode=" + std::to_string(static_cast<uint8_t>(region.scan_filter_mode));
	AppendSljitArtifactKeyField(result, "source_types", DescribeSljitLogicalTypesSemantic(region.source_output_types));
	result += "source_not_null_count=" + std::to_string(region.source_not_null.size()) + ";";
	for (idx_t source_idx = 0; source_idx < region.source_not_null.size(); source_idx++) {
		result += "source_not_null" + std::to_string(source_idx) + "=";
		result += region.source_not_null[source_idx] ? "1;" : "0;";
	}
	result += "scan_filter_count=" + std::to_string(region.scan_filters.size()) + ";";
	for (idx_t filter_idx = 0; filter_idx < region.scan_filters.size(); filter_idx++) {
		auto &scan_filter = region.scan_filters[filter_idx];
		auto field_prefix = "scan_filter" + std::to_string(filter_idx);
		result += field_prefix + "_index=" + std::to_string(scan_filter.filter_index) + ";";
		AppendSljitArtifactKeyField(result, field_prefix + "_type", scan_filter.input_type.ToString());
		result += field_prefix + (scan_filter.input_not_null ? "_not_null=1;" : "_not_null=0;");
		AppendSljitArtifactKeyField(result, field_prefix + "_expression",
		                            DescribeNativeRegionExpressionSemantic(scan_filter.filter));
	}
	AppendSljitArtifactKeyField(result, "region", DescribeNativeRegionInternal(region, "semantic", true));
	return result;
}

static void AppendSljitArtifactBindingCounts(string &result, const string &name, const vector<idx_t> &values) {
	result += name + "_count=" + std::to_string(values.size()) + ";";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		result += name + std::to_string(value_idx) + "=" + std::to_string(values[value_idx]) + ";";
	}
}

static void AppendSljitArtifactBindingValues(string &result, const string &name, const vector<Value> &values) {
	result += name + "_count=" + std::to_string(values.size()) + ";";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		auto field_name = name + std::to_string(value_idx);
		AppendSljitArtifactKeyField(result, field_name + "_type", values[value_idx].type().ToString());
		AppendSljitArtifactKeyField(result, field_name + "_value", values[value_idx].ToSQLString());
	}
}

string BuildSljitRegionArtifactBindingKey(const SljitNativeRegionPlan &region) {
	string result = "sljit-binding-v2;";
	AppendSljitArtifactBindingCounts(result, "distinct", region.source_distinct_counts);
	AppendSljitArtifactBindingCounts(result, "reserve", region.source_distinct_reserve_counts);
	AppendSljitArtifactBindingValues(result, "min", region.source_min_values);
	AppendSljitArtifactBindingValues(result, "max", region.source_max_values);
	return result;
}

string DescribeNativeRegionShape(const SljitNativeRegionPlan &region) {
	return BuildSljitNativeRegionShape(region);
}

} // namespace duckdb

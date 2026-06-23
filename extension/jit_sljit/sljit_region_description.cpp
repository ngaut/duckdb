//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_description.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"
#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"

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
	result += key.null_equal ? ",null_equal=true>" : ",null_equal=false>";
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

static string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr) {
	string result;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		result = "native:reference";
		switch (expr.reference_origin) {
		case SljitNativeReferenceOrigin::REGION_INPUT:
			result += ":region-input";
			break;
		case SljitNativeReferenceOrigin::PROJECTION_PASS_THROUGH:
			result += ":projection-pass";
			break;
		case SljitNativeReferenceOrigin::PROJECTION_TEMP:
			result += ":projection-temp";
			break;
		case SljitNativeReferenceOrigin::SOURCE_OUTPUT:
			result += ":source-output";
			break;
		default:
			result += ":unknown";
			break;
		}
		break;
	case SljitNativeRegionExpressionKind::CONSTANT:
		result = "native:constant<" + expr.return_type.ToString() + ">(" + expr.constant_value.ToString() + ")";
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		result = NativeIntegerBinaryReason(expr.integer_kind, expr.binary_op);
		if (!expr.check_arithmetic_overflow) {
			result += ":no-overflow";
		}
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		result = NativeIntegerBinaryReferenceReason(expr.integer_kind, expr.binary_op);
		if (!expr.check_arithmetic_overflow) {
			result += ":no-overflow";
		}
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		result = NativeDoubleBinaryReason(expr.double_binary_op);
		break;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		result = NativeDoubleBinaryReferenceReason(expr.double_binary_op);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		result = NativeIntegerCompareReason(expr.integer_kind);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		result = NativeIntegerCompareReferenceReason(expr.integer_kind);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		result = NativeIntegerCastReason(expr.cast_source_width, expr.cast_target_width, expr.try_cast);
		break;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		result = "native:signed-to-unsigned-cast:" + NativeSignedIntegerTypeName(expr.cast_source_width) + "->" +
		         NativeUnsignedIntegerTypeName(expr.unsigned_cast_target_width) +
		         (expr.try_cast ? ":try" : ":throwing");
		break;
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		result = "native:decimal64-to-double:scale=" + std::to_string(expr.double_constant);
		break;
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		result = "native:decimal128-scale-up:factor=" + std::to_string(expr.constant);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		result = NativeIntegerCoalesceReason(expr.signed_integer_width);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		result = NativeIntegerInListReason(expr.integer_kind, expr.not_in);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		result = NativeIntegerBetweenReason(expr.integer_kind, expr.not_between);
		break;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		result = "native:constant-or-null";
		break;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		result = "native:string-compress:" + std::to_string(expr.string_compress_target_size);
		break;
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		result = "native:string-decompress:" + std::to_string(expr.string_decompress_source_size);
		break;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		result = "native:integral-compress";
		break;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		result = "native:integral-decompress";
		break;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		result = "native:date-year";
		break;
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		result = "native:error-guarded-reference:" + std::to_string(expr.guarded_value_size);
		break;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		result = NativeNullCheckReason(expr.null_check_op);
		break;
	case SljitNativeRegionExpressionKind::PREDICATE:
		result = "native:boolean-predicate";
		break;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		result = "native:expression-tree:sources=" + std::to_string(expr.expression_tree_source_indices.size());
		break;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		result = "native:typed-expression-tree:sources=" + std::to_string(expr.expression_tree_source_indices.size());
		break;
	default:
		result = "native:unknown";
		break;
	}
	if (!expr.ir.empty()) {
		result += "[" + expr.ir + "]";
	}
	return result;
}

static string DescribeNativeRegionExpressionList(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	string result;
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (expr_idx > 0) {
			result += ",";
		}
		result += DescribeNativeRegionExpression(expressions[expr_idx]);
	}
	return result;
}

static string
DescribeNativeNestedLoopJoinProbeConditions(const vector<SljitNativeNestedLoopJoinProbeConditionPlan> &conditions) {
	string result;
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += ",";
		}
		auto &condition = conditions[condition_idx];
		result += "condition" + std::to_string(condition_idx);
		result += "<kind=" + string(SljitNestedLoopJoinValueKindToString(condition.value_kind));
		result += ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type));
		result += ",lhs=" + DescribeNativeRegionExpression(condition.lhs_condition) + ">";
	}
	return result;
}

string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode) {
	string result = "sljit.region " + mode;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		result += ";op" + std::to_string(op_idx) + "=";
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			result += "filter(" + DescribeNativeRegionExpression(op.filter) + ")";
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			result += "projection(" + DescribeNativeRegionExpressionList(op.projections) + ")";
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
			result += ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(op.hash_join_probe.output_mode));
			if (op.hash_join_probe.residual_predicate) {
				result += ",residual_predicate=true";
				result += ",residual=" + DescribeNativeRegionExpression(op.hash_join_probe.residual_filter);
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
			          DescribeNativeNestedLoopJoinProbeConditions(op.nested_loop_join_probe.conditions);
			result += ")";
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			result += "nested_loop_join_build(conditions=" +
			          DescribeNativeRegionExpressionList(op.nested_loop_join_build.rhs_conditions);
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
			result += ";groups=" + std::to_string(op.aggregate_update.sink_info.groups.size());
			result += ";aggregates=" + std::to_string(op.aggregate_update.sink_info.aggregates.size());
			if (op.aggregate_update.use_primitive_payloads) {
				result += ";payload_update=generated-primitive";
				result += ";primitive_payloads=" + DescribeNativeRegionExpressionList(op.aggregate_update.payloads);
				if (op.aggregate_update.use_perfect_hash_group_lookup) {
					result += ";grouped_state_lookup=generated-perfect-hash";
				}
			} else {
				result += ";execution=vectorized-operator-boundary";
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			result += "ordered_sink(keys=" + DescribeNativeRegionExpressionList(op.order_sink.order_keys);
			result += ";payload_columns=" + std::to_string(op.order_sink.input_types.size());
			result += ";operator_kind=" +
			          string(ExecutionRegionOperatorKindToString(op.order_sink.sink_info.order_contract.kind));
			result += ")";
			break;
		default:
			result += "unknown";
			break;
		}
	}
	return result;
}

string DescribeNativeRegionShape(const SljitNativeRegionPlan &region) {
	return BuildSljitNativeRegionShape(region);
}

} // namespace duckdb

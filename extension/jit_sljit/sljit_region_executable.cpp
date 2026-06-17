#include "sljit_region_executable.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_region_codegen.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static bool BuildExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                            SljitExecutableRegionExpression &expr, string &error) {
	expr.plan = CopySljitNativeRegionExpression(plan);
	auto &semantic = expr.plan;
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return true;
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (require_boolean) {
			error = "SLJIT constant projection cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		expr.overflow_message = NativeIntegerBinaryOverflowMessage(semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryConstant(
		    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.function, error,
		    semantic.check_result_range, semantic.result_min, semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeIntegerBinaryOverflowMessage(semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op, expr.function,
		                                                    error, semantic.check_result_range, semantic.result_min,
		                                                    semantic.result_max);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		expr.code = BuildSljitNativeDoubleBinaryConstant(semantic.double_binary_op, semantic.constant_on_left,
		                                                 expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		expr.code = BuildSljitNativeDoubleBinaryReferences(semantic.double_binary_op, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerCompareConstant(semantic.integer_kind, semantic.compare_op,
			                                                   semantic.constant_on_left, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerSelectConstant(
		    semantic.integer_kind, semantic.compare_op, semantic.constant_on_left, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op,
			                                                     expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op,
		                                                           expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		expr.overflow_message =
		    NativeIntegerCastOverflowMessage(semantic.cast_source_width, semantic.cast_target_width);
		expr.code = BuildSljitNativeIntegerCast(semantic.cast_source_width, semantic.cast_target_width,
		                                        semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		expr.overflow_message = NativeSignedToUnsignedIntegerCastOverflowMessage(semantic.cast_source_width,
		                                                                         semantic.unsigned_cast_target_width);
		expr.code = BuildSljitNativeSignedToUnsignedIntegerCast(
		    semantic.cast_source_width, semantic.unsigned_cast_target_width, semantic.try_cast, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		if (require_boolean) {
			error = "SLJIT decimal64-to-double cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		if (require_boolean) {
			error = "SLJIT decimal128 scale-up cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		expr.code = BuildSljitNativeIntegerCoalesce(semantic.signed_integer_width, semantic.coalesce_rhs_kind,
		                                            semantic.coalesce_constant_is_null, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(),
			                                          semantic.list_has_null, semantic.not_in, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code =
		    BuildSljitNativeIntegerInListSelect(semantic.integer_kind, semantic.constants.size(),
		                                        semantic.list_has_null, semantic.not_in, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (!require_boolean) {
			expr.code = BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
			                                           semantic.lower_inclusive, semantic.upper_inclusive,
			                                           semantic.not_between, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeIntegerBetweenSelect(semantic.integer_kind, semantic.lower, semantic.upper,
		                                                        semantic.lower_inclusive, semantic.upper_inclusive,
		                                                        semantic.not_between, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		if (require_boolean) {
			error = "SLJIT constant_or_null cannot lower as a predicate";
			return false;
		}
		expr.predicate_code = BuildSljitNativeConstantOrNull(semantic.constant_or_null.guard_source_indices,
		                                                     expr.predicate_function, error);
		return expr.predicate_code != nullptr;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		if (require_boolean) {
			error = "SLJIT string compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeStringCompress(semantic.string_compress_target_size, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT string decompression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeStringDecompress(semantic.string_decompress_source_size, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (require_boolean) {
			error = "SLJIT integral compression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralCompress(semantic.cast_source_width, semantic.unsigned_cast_target_width,
		                                             expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT integral decompression cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeIntegralDecompress(semantic.unsigned_source_width, semantic.cast_target_width,
		                                               expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		if (require_boolean) {
			error = "SLJIT date-year cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeDateYear(expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		if (require_boolean) {
			error = "SLJIT error-guarded reference cannot lower as a predicate";
			return false;
		}
		expr.code = BuildSljitNativeErrorGuardedReference(semantic.guarded_value_size, semantic.guard_compare_op,
		                                                  semantic.guard_constant_on_left, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (!require_boolean) {
			expr.code = BuildSljitNativeNullCheck(semantic.null_check_op, expr.function, error);
			if (!expr.code) {
				return false;
			}
		}
		expr.select_code = BuildSljitNativeNullCheckSelect(semantic.null_check_op, expr.select_function, error);
		return expr.select_code != nullptr;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (!require_boolean) {
			expr.predicate_code = BuildSljitNativePredicate(*semantic.predicate, true, expr.predicate_function, error);
			if (!expr.predicate_code) {
				return false;
			}
		}
		expr.predicate_select_code =
		    BuildSljitNativePredicate(*semantic.predicate, false, expr.predicate_select_function, error);
		return expr.predicate_select_code != nullptr;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		if (require_boolean) {
			error = "SLJIT expression tree cannot lower as a predicate";
			return false;
		}
		if (!semantic.expression_tree) {
			error = "SLJIT expression tree plan is missing its typed expression IR";
			return false;
		}
		expr.overflow_message = "Overflow in arithmetic expression";
		expr.code = BuildSljitNativeExpressionTree(*semantic.expression_tree, expr.function, error);
		return expr.code != nullptr;
	default:
		throw InternalException("Unknown SLJIT native region expression kind");
	}
}

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.output_types = op.output_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return BuildExecutableRegionExpression(op.filter, true, executable.filter, error);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.hash_join_probe.plan = CopySljitNativeHashJoinProbePlan(op.hash_join_probe);
		if (op.hash_join_probe.residual_predicate &&
		    !BuildExecutableRegionExpression(op.hash_join_probe.residual_filter, true,
		                                     executable.hash_join_probe.residual_filter, error)) {
			return false;
		}
		executable.hash_join_probe.code = BuildSljitHashJoinProbe(
		    op.hash_join_probe.keys, op.hash_join_probe.equality_key_count, op.hash_join_probe.mark_build_match,
		    op.hash_join_probe.found_match_offset, op.hash_join_probe.pointer_offset, op.hash_join_probe.output_mode,
		    executable.hash_join_probe.function, error);
		if (!executable.hash_join_probe.code || !executable.hash_join_probe.function) {
			return false;
		}
		if (op.hash_join_probe.perfect_hash_probe) {
			executable.hash_join_probe.perfect_code =
			    BuildSljitPerfectHashJoinProbe(op.hash_join_probe.keys[0], op.hash_join_probe.output_mode,
			                                   executable.hash_join_probe.perfect_function, error);
			return executable.hash_join_probe.perfect_code != nullptr &&
			       executable.hash_join_probe.perfect_function != nullptr;
		}
		return true;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		executable.nested_loop_join_probe.plan.operator_index = op.nested_loop_join_probe.operator_index;
		executable.nested_loop_join_probe.plan.input_types = op.nested_loop_join_probe.input_types;
		executable.nested_loop_join_probe.plan.condition_types = op.nested_loop_join_probe.condition_types;
		executable.nested_loop_join_probe.plan.join_type = op.nested_loop_join_probe.join_type;
		executable.nested_loop_join_probe.plan.operator_info = op.nested_loop_join_probe.operator_info;
		executable.nested_loop_join_probe.plan.ir = op.nested_loop_join_probe.ir;
		executable.nested_loop_join_probe.plan.conditions.reserve(op.nested_loop_join_probe.conditions.size());
		executable.nested_loop_join_probe.lhs_conditions.reserve(op.nested_loop_join_probe.conditions.size());
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
			condition_plan.type = condition.type;
			condition_plan.comparison_type = condition.comparison_type;
			condition_plan.value_kind = condition.value_kind;
			condition_plan.ir = condition.ir;
			condition_plan.lhs_condition = CopySljitNativeRegionExpression(condition.lhs_condition);
			executable.nested_loop_join_probe.plan.conditions.push_back(std::move(condition_plan));

			SljitExecutableRegionExpression executable_condition;
			if (!BuildExecutableRegionExpression(condition.lhs_condition, false, executable_condition, error)) {
				return false;
			}
			executable.nested_loop_join_probe.lhs_conditions.push_back(std::move(executable_condition));
		}
		executable.nested_loop_join_probe.code = BuildSljitNestedLoopJoinProbe(
		    executable.nested_loop_join_probe.plan, executable.nested_loop_join_probe.function, error);
		return executable.nested_loop_join_probe.code != nullptr &&
		       executable.nested_loop_join_probe.function != nullptr;
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.hash_join_build.plan = op.hash_join_build;
		return true;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		executable.nested_loop_join_build.plan.sink_info = op.nested_loop_join_build.sink_info;
		executable.nested_loop_join_build.plan.input_types = op.nested_loop_join_build.input_types;
		executable.nested_loop_join_build.plan.condition_types = op.nested_loop_join_build.condition_types;
		executable.nested_loop_join_build.plan.ir = op.nested_loop_join_build.ir;
		executable.nested_loop_join_build.rhs_conditions.reserve(op.nested_loop_join_build.rhs_conditions.size());
		for (auto &condition : op.nested_loop_join_build.rhs_conditions) {
			SljitExecutableRegionExpression executable_condition;
			if (!BuildExecutableRegionExpression(condition, false, executable_condition, error)) {
				return false;
			}
			executable.nested_loop_join_build.rhs_conditions.push_back(std::move(executable_condition));
		}
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		executable.order_sink.plan.sink_info = op.order_sink.sink_info;
		executable.order_sink.plan.input_types = op.order_sink.input_types;
		executable.order_sink.plan.key_types = op.order_sink.key_types;
		executable.order_sink.plan.ir = op.order_sink.ir;
		executable.order_sink.order_keys.reserve(op.order_sink.order_keys.size());
		for (auto &order_key : op.order_sink.order_keys) {
			SljitExecutableRegionExpression executable_order_key;
			if (!BuildExecutableRegionExpression(order_key, false, executable_order_key, error)) {
				return false;
			}
			executable.order_sink.order_keys.push_back(std::move(executable_order_key));
		}
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK:
		executable.append_sink.plan = op.append_sink;
		return true;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		if (op.append_sink.sink_info.kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			error = "SLJIT delimiter join sink executable is missing delimiter sink info";
			return false;
		}
		executable.delim_join_sink.plan.sink_info = op.append_sink.sink_info;
		executable.delim_join_sink.plan.input_types = op.append_sink.input_types;
		executable.delim_join_sink.plan.ir = op.append_sink.ir;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		executable.aggregate_update.plan.sink_info = op.aggregate_update.sink_info;
		executable.aggregate_update.plan.input_types = op.aggregate_update.input_types;
		executable.aggregate_update.plan.group_input_indices = op.aggregate_update.group_input_indices;
		executable.aggregate_update.plan.state_value_offsets = op.aggregate_update.state_value_offsets;
		executable.aggregate_update.plan.state_is_set_offsets = op.aggregate_update.state_is_set_offsets;
		executable.aggregate_update.plan.use_primitive_payloads = op.aggregate_update.use_primitive_payloads;
		executable.aggregate_update.plan.use_grouped_state_addresses = op.aggregate_update.use_grouped_state_addresses;
		executable.aggregate_update.plan.ir = op.aggregate_update.ir;
		executable.aggregate_update.plan.payloads.reserve(op.aggregate_update.payloads.size());
		executable.aggregate_update.payloads.reserve(op.aggregate_update.payloads.size());
		executable.aggregate_update.payload_update_code.reserve(op.aggregate_update.payloads.size());
		executable.aggregate_update.payload_update_functions.reserve(op.aggregate_update.payloads.size());
		for (auto &payload : op.aggregate_update.payloads) {
			executable.aggregate_update.plan.payloads.push_back(CopySljitNativeRegionExpression(payload));
			SljitExecutableRegionExpression executable_payload;
			executable_payload.plan = CopySljitNativeRegionExpression(payload);
			executable.aggregate_update.payloads.push_back(std::move(executable_payload));
			SljitNativeAggregateUpdateFunction function = nullptr;
			unique_ptr<ExecutionRegionCodeHandle> code;
			switch (payload.kind) {
			case SljitNativeRegionExpressionKind::REFERENCE:
				if (op.aggregate_update.use_grouped_state_addresses) {
					auto payload_idx = executable.aggregate_update.payload_update_code.size();
					if (payload_idx >= op.aggregate_update.state_value_offsets.size() ||
					    payload_idx >= op.aggregate_update.state_is_set_offsets.size()) {
						error = "SLJIT grouped aggregate update is missing primitive state offsets";
						return false;
					}
					code = BuildSljitNativeGroupedSumInt64Reference(
					    payload.integer_kind, op.aggregate_update.state_value_offsets[payload_idx],
					    op.aggregate_update.state_is_set_offsets[payload_idx], function, error);
				} else {
					code = BuildSljitNativeUngroupedSumInt64Reference(payload.integer_kind, function, error);
				}
				break;
			case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
				if (op.aggregate_update.use_grouped_state_addresses) {
					error = "SLJIT grouped aggregate update only supports reference primitive payloads";
					return false;
				}
				code = BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
				    payload.integer_kind, payload.binary_op, payload.constant_on_left, function, error,
				    payload.check_result_range, payload.result_min, payload.result_max);
				break;
			case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
				if (op.aggregate_update.use_grouped_state_addresses) {
					error = "SLJIT grouped aggregate update only supports reference primitive payloads";
					return false;
				}
				code = BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
				    payload.integer_kind, payload.binary_op, function, error, payload.check_result_range,
				    payload.result_min, payload.result_max);
				break;
			case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
				if (op.aggregate_update.use_grouped_state_addresses) {
					error = "SLJIT grouped aggregate update only supports reference primitive payloads";
					return false;
				}
				if (payload.expression_tree) {
					code = BuildSljitNativeUngroupedSumInt64ExpressionTree(*payload.expression_tree, function, error);
				}
				break;
			default:
				break;
			}
			if (!code || !function) {
				if (error.empty()) {
					error = "SLJIT aggregate update payload has no native primitive reducer";
				}
				return false;
			}
			executable.aggregate_update.payload_update_code.push_back(std::move(code));
			executable.aggregate_update.payload_update_functions.push_back(function);
		}
		return true;
	case SljitNativeRegionOpKind::PROJECTION:
		executable.use_vectorized_projection = op.use_vectorized_projection;
		if (executable.use_vectorized_projection) {
			return true;
		}
		executable.projections.reserve(op.projections.size());
		for (auto &projection : op.projections) {
			SljitExecutableRegionExpression executable_projection;
			if (!BuildExecutableRegionExpression(projection, false, executable_projection, error)) {
				return false;
			}
			executable.projections.push_back(std::move(executable_projection));
		}
		return true;
	default:
		throw InternalException("Unknown SLJIT native region operator kind");
	}
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error) {
	executable.ops.reserve(region.ops.size());
	for (auto &op : region.ops) {
		SljitExecutableRegionOp executable_op;
		if (!BuildExecutableRegionOp(op, executable_op, error)) {
			return false;
		}
		executable.ops.push_back(std::move(executable_op));
	}
	return true;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_copy.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"

namespace duckdb {

unique_ptr<SljitNativePredicate> SljitNativePredicate::Copy() const {
	auto result = make_uniq<SljitNativePredicate>();
	result->kind = kind;
	result->return_type = return_type;
	result->constant_value = constant_value;
	result->constant_is_null = constant_is_null;
	result->conjunction_op = conjunction_op;
	result->source_index = source_index;
	result->right_source_index = right_source_index;
	result->constant = constant;
	result->int128_constant_lower = int128_constant_lower;
	result->int128_constant_upper = int128_constant_upper;
	result->constant_on_left = constant_on_left;
	result->integer_kind = integer_kind;
	result->double_source_kind = double_source_kind;
	result->double_right_source_kind = double_right_source_kind;
	result->double_constant = double_constant;
	result->double_source_scale = double_source_scale;
	result->double_right_source_scale = double_right_source_scale;
	result->compare_op = compare_op;
	result->null_check_op = null_check_op;
	result->constants = constants;
	result->list_has_null = list_has_null;
	result->not_in = not_in;
	result->lower = lower;
	result->upper = upper;
	result->lower_inclusive = lower_inclusive;
	result->upper_inclusive = upper_inclusive;
	result->not_between = not_between;
	result->string_constant = string_constant;
	result->string_constants = string_constants;
	result->substring_length = substring_length;
	result->guard_has_null_constant = guard_has_null_constant;
	result->guard_source_indices = guard_source_indices;
	result->source_indices = source_indices;
	result->source_not_null = source_not_null;
	result->child = child ? child->Copy() : nullptr;
	result->children.reserve(children.size());
	for (auto &child_entry : children) {
		result->children.push_back(child_entry ? child_entry->Copy() : nullptr);
	}
	return result;
}

SljitNativeRegionExpressionPlan SljitNativeRegionExpressionPlan::Copy(bool copy_auxiliary_expression_tree,
                                                                      bool copy_ir) const {
	SljitNativeRegionExpressionPlan result;
	result.kind = kind;
	result.integer_kind = integer_kind;
	result.return_type = return_type;
	result.constant_value = constant_value;
	result.source_index = source_index;
	result.right_source_index = right_source_index;
	result.constant = constant;
	result.double_constant = double_constant;
	result.result_min = result_min;
	result.result_max = result_max;
	result.constant_on_left = constant_on_left;
	result.check_arithmetic_overflow = check_arithmetic_overflow;
	result.check_result_range = check_result_range;
	result.binary_op = binary_op;
	result.double_binary_op = double_binary_op;
	result.double_source_kind = double_source_kind;
	result.double_right_source_kind = double_right_source_kind;
	result.double_source_scale = double_source_scale;
	result.double_right_source_scale = double_right_source_scale;
	result.compare_op = compare_op;
	result.cast_source_width = cast_source_width;
	result.cast_target_width = cast_target_width;
	result.unsigned_source_width = unsigned_source_width;
	result.unsigned_cast_target_width = unsigned_cast_target_width;
	result.query_location = query_location;
	result.string_compress_target_size = string_compress_target_size;
	result.string_decompress_source_size = string_decompress_source_size;
	result.guard_source_index = guard_source_index;
	result.guard_compare_op = guard_compare_op;
	result.guard_constant = guard_constant;
	result.guard_constant_on_left = guard_constant_on_left;
	result.guarded_value_size = guarded_value_size;
	result.error_message = error_message;
	result.try_cast = try_cast;
	result.signed_integer_width = signed_integer_width;
	result.coalesce_rhs_kind = coalesce_rhs_kind;
	result.coalesce_constant_is_null = coalesce_constant_is_null;
	result.null_check_op = null_check_op;
	result.constants = constants;
	result.lower = lower;
	result.upper = upper;
	result.list_has_null = list_has_null;
	result.not_in = not_in;
	result.not_between = not_between;
	result.lower_inclusive = lower_inclusive;
	result.upper_inclusive = upper_inclusive;
	result.predicate = predicate ? predicate->Copy() : nullptr;
	const bool copy_expression_tree =
	    expression_tree && (copy_auxiliary_expression_tree ||
	                        kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
	                        kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE);
	if (copy_expression_tree) {
		result.expression_tree = expression_tree->Copy();
		result.expression_tree_source_indices = expression_tree_source_indices;
	}
	result.constant_or_null = constant_or_null;
	result.references_region_input = references_region_input;
	result.emit_flat_nullable_fast_path = emit_flat_nullable_fast_path;
	if (copy_ir) {
		result.ir = ir;
	}
	return result;
}

static vector<SljitNativeRegionExpressionPlan>
CopySljitNativeRegionExpressions(const vector<SljitNativeRegionExpressionPlan> &input) {
	vector<SljitNativeRegionExpressionPlan> result;
	result.reserve(input.size());
	for (auto &expr : input) {
		result.push_back(expr.Copy());
	}
	return result;
}

SljitNativeHashJoinProbePlan SljitNativeHashJoinProbePlan::Copy(bool copy_ir) const {
	SljitNativeHashJoinProbePlan result;
	result.operator_index = operator_index;
	result.keys = keys;
	result.equality_key_count = equality_key_count;
	result.mark_build_match = mark_build_match;
	result.mark_build_match_after_residual = mark_build_match_after_residual;
	result.residual_predicate = residual_predicate;
	result.perfect_hash_probe = perfect_hash_probe;
	result.found_match_offset = found_match_offset;
	result.pointer_offset = pointer_offset;
	result.output_mode = output_mode;
	result.input_types = input_types;
	result.residual_source_types = residual_source_types;
	result.residual_source_not_null = residual_source_not_null;
	result.residual_filter = residual_filter.Copy(false, copy_ir);
	result.operator_info = operator_info;
	if (copy_ir) {
		result.ir = ir;
	}
	return result;
}

static SljitNativeNestedLoopJoinProbeConditionPlan
CopySljitNativeNestedLoopJoinProbeConditionPlan(const SljitNativeNestedLoopJoinProbeConditionPlan &input) {
	SljitNativeNestedLoopJoinProbeConditionPlan result;
	result.lhs_condition = input.lhs_condition.Copy();
	result.type = input.type;
	result.comparison_type = input.comparison_type;
	result.value_kind = input.value_kind;
	result.ir = input.ir;
	return result;
}

static SljitNativeNestedLoopJoinProbePlan
CopySljitNativeNestedLoopJoinProbePlan(const SljitNativeNestedLoopJoinProbePlan &input) {
	SljitNativeNestedLoopJoinProbePlan result;
	result.operator_index = input.operator_index;
	result.input_types = input.input_types;
	result.condition_types = input.condition_types;
	result.join_type = input.join_type;
	result.operator_info = input.operator_info;
	result.ir = input.ir;
	result.conditions.reserve(input.conditions.size());
	for (auto &condition : input.conditions) {
		result.conditions.push_back(CopySljitNativeNestedLoopJoinProbeConditionPlan(condition));
	}
	return result;
}

static SljitNativeRegionOpPlan CopySljitNativeRegionOp(const SljitNativeRegionOpPlan &input) {
	SljitNativeRegionOpPlan result;
	result.kind = input.kind;
	result.operator_index = input.operator_index;
	result.output_types = input.output_types;
	result.filter = input.filter.Copy();
	result.hash_join_probe = input.hash_join_probe.Copy();
	result.hash_join_build = input.hash_join_build;
	result.nested_loop_join_probe = CopySljitNativeNestedLoopJoinProbePlan(input.nested_loop_join_probe);
	result.nested_loop_join_build.sink_info = input.nested_loop_join_build.sink_info;
	result.nested_loop_join_build.input_types = input.nested_loop_join_build.input_types;
	result.nested_loop_join_build.condition_types = input.nested_loop_join_build.condition_types;
	result.nested_loop_join_build.ir = input.nested_loop_join_build.ir;
	result.nested_loop_join_build.rhs_conditions =
	    CopySljitNativeRegionExpressions(input.nested_loop_join_build.rhs_conditions);
	result.append_sink = input.append_sink;
	result.delim_join_sink = input.delim_join_sink;
	result.aggregate_update.sink_info = input.aggregate_update.sink_info;
	result.aggregate_update.input_types = input.aggregate_update.input_types;
	result.aggregate_update.use_primitive_payloads = input.aggregate_update.use_primitive_payloads;
	result.aggregate_update.use_grouped_state_addresses = input.aggregate_update.use_grouped_state_addresses;
	result.aggregate_update.use_perfect_hash_group_lookup = input.aggregate_update.use_perfect_hash_group_lookup;
	result.aggregate_update.ir = input.aggregate_update.ir;
	result.aggregate_update.payloads = CopySljitNativeRegionExpressions(input.aggregate_update.payloads);
	result.aggregate_update.group_expressions = CopySljitNativeRegionExpressions(input.aggregate_update.group_expressions);
	result.order_sink.sink_info = input.order_sink.sink_info;
	result.order_sink.input_types = input.order_sink.input_types;
	result.order_sink.key_types = input.order_sink.key_types;
	result.order_sink.ir = input.order_sink.ir;
	result.order_sink.order_keys = CopySljitNativeRegionExpressions(input.order_sink.order_keys);
	result.projections = CopySljitNativeRegionExpressions(input.projections);
	return result;
}

unique_ptr<SljitNativeRegionPlan> SljitNativeRegionPlan::Copy() const {
	auto result = make_uniq<SljitNativeRegionPlan>();
	result->source_execution = source_execution;
	result->source_distinct_counts = source_distinct_counts;
	result->source_min_values = source_min_values;
	result->source_max_values = source_max_values;
	result->source_not_null = source_not_null;
	result->ops.reserve(ops.size());
	for (auto &op : ops) {
		result->ops.push_back(CopySljitNativeRegionOp(op));
	}
	return result;
}

} // namespace duckdb

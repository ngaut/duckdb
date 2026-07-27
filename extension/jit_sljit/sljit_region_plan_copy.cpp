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
	result.right_cast_source_width = right_cast_source_width;
	result.cast_target_width = cast_target_width;
	result.unsigned_source_width = unsigned_source_width;
	result.unsigned_cast_target_width = unsigned_cast_target_width;
	result.query_location = query_location;
	result.string_compress_target_size = string_compress_target_size;
	result.string_decompress_source_size = string_decompress_source_size;
	result.string_substring_length = string_substring_length;
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
	    expression_tree &&
	    (copy_auxiliary_expression_tree || kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
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

SljitNativeHashJoinProbePlan SljitNativeHashJoinProbePlan::Copy(bool copy_ir) const {
	SljitNativeHashJoinProbePlan result;
	result.operator_index = operator_index;
	result.keys = keys;
	result.equality_key_count = equality_key_count;
	result.mark_build_match = mark_build_match;
	result.mark_build_match_after_residual = mark_build_match_after_residual;
	result.residual_predicate = residual_predicate;
	result.perfect_hash_probe = perfect_hash_probe;
	result.exact_source_filter_binding = exact_source_filter_binding;
	result.found_match_offset = found_match_offset;
	result.pointer_offset = pointer_offset;
	result.output_mode = output_mode;
	result.input_types = input_types;
	result.residual_source_types = residual_source_types;
	result.residual_source_not_null = residual_source_not_null;
	result.residual_filter = residual_filter.Copy(false, copy_ir);
	result.operator_info = operator_info;
	return result;
}

} // namespace duckdb

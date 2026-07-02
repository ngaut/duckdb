//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_predicate_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"

namespace duckdb {

bool TryReadNativeRegionPredicateExpression(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr) {
	SljitNativeIntegerKind in_list_kind;
	idx_t in_list_source_index;
	vector<int64_t> in_list_constants;
	bool list_has_null;
	bool not_in;
	if (TryReadNativeIntegerInList(root, in_list_kind, in_list_source_index, in_list_constants, list_has_null,
	                               not_in)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_IN_LIST;
		expr.integer_kind = in_list_kind;
		expr.return_type = root.return_type;
		expr.source_index = in_list_source_index;
		expr.constants = std::move(in_list_constants);
		expr.list_has_null = list_has_null;
		expr.not_in = not_in;
		return true;
	}

	SljitNativeIntegerKind between_kind;
	idx_t between_source_index;
	int64_t lower;
	int64_t upper;
	bool lower_inclusive;
	bool upper_inclusive;
	bool not_between;
	if (TryReadNativeIntegerBetween(root, between_kind, between_source_index, lower, upper, lower_inclusive,
	                                upper_inclusive, not_between)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BETWEEN;
		expr.integer_kind = between_kind;
		expr.return_type = root.return_type;
		expr.source_index = between_source_index;
		expr.lower = lower;
		expr.upper = upper;
		expr.lower_inclusive = lower_inclusive;
		expr.upper_inclusive = upper_inclusive;
		expr.not_between = not_between;
		return true;
	}

	SljitNativeNullCheckOp null_check_op;
	idx_t null_check_source_index;
	if (TryReadNativeNullCheck(root, null_check_op, null_check_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::NULL_CHECK;
		expr.return_type = root.return_type;
		expr.source_index = null_check_source_index;
		expr.null_check_op = null_check_op;
		return true;
	}

	SljitNativeIntegerCompareOp compare_op;
	SljitNativeIntegerKind integer_kind;
	idx_t source_index;
	idx_t right_source_index;
	int64_t constant;
	bool constant_on_left;
	if (TryReadNativeIntegerCompareReferences(root, compare_op, integer_kind, source_index, right_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES;
		expr.integer_kind = integer_kind;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.compare_op = compare_op;
		return true;
	}
	if (TryReadNativeIntegerCompareConstant(root, compare_op, integer_kind, source_index, constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT;
		expr.integer_kind = integer_kind;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = constant;
		expr.constant_on_left = constant_on_left;
		expr.compare_op = compare_op;
		return true;
	}

	if (ShouldTryNativePredicateRoot(root)) {
		unique_ptr<SljitNativePredicate> predicate;
		if (TryBuildNativePredicate(root, predicate)) {
			expr.kind = SljitNativeRegionExpressionKind::PREDICATE;
			expr.return_type = root.return_type;
			expr.predicate = std::move(predicate);
			return true;
		}
	}
	return false;
}

} // namespace duckdb

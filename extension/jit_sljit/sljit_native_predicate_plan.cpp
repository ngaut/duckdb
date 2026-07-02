//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_predicate_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "sljit_native_double_plan.hpp"
#include "sljit_native_integer_plan.hpp"
#include "sljit_native_string_plan.hpp"

namespace duckdb {

static bool TryBuildNativePredicateInternal(const ExecutionExpressionIR &root,
                                            unique_ptr<SljitNativePredicate> &predicate) {
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}

	if (TryBuildNativeBasePredicate(root, predicate)) {
		return true;
	}

	auto result = make_uniq<SljitNativePredicate>();
	result->return_type = root.return_type;

	if (TryReadNativeIntegerCompareNullConstant(root)) {
		result->kind = SljitNativePredicateKind::CONSTANT;
		result->constant_is_null = true;
		predicate = std::move(result);
		return true;
	}

	if (TryBuildNativeConstantOrNullPredicate(root, predicate)) {
		return true;
	}

	idx_t substring_in_list_source_index;
	idx_t substring_in_list_length;
	vector<string> substring_in_list_constants;
	if (TryReadNativeStringSubstringInListConstant(root, substring_in_list_source_index, substring_in_list_length,
	                                               substring_in_list_constants)) {
		result->kind = SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT;
		result->source_index = substring_in_list_source_index;
		result->substring_length = substring_in_list_length;
		result->string_constants = std::move(substring_in_list_constants);
		predicate = std::move(result);
		return true;
	}

	idx_t early_string_in_list_source_index;
	vector<string> early_string_in_list_constants;
	bool early_string_in_list_has_null;
	bool early_string_not_in;
	if (TryReadNativeStringInListConstant(root, early_string_in_list_source_index, early_string_in_list_constants,
	                                      early_string_in_list_has_null, early_string_not_in)) {
		result->kind = SljitNativePredicateKind::STRING_IN_LIST_CONSTANT;
		result->source_index = early_string_in_list_source_index;
		result->string_constants = std::move(early_string_in_list_constants);
		result->list_has_null = early_string_in_list_has_null;
		result->not_in = early_string_not_in;
		predicate = std::move(result);
		return true;
	}

	{
		SljitNativeIntegerKind between_kind;
		idx_t between_source_index;
		int64_t lower;
		int64_t upper;
		bool lower_inclusive;
		bool upper_inclusive;
		bool not_between;
		if (TryReadNativeIntegerBetween(root, between_kind, between_source_index, lower, upper, lower_inclusive,
		                                upper_inclusive, not_between)) {
			result->kind = SljitNativePredicateKind::INTEGER_BETWEEN;
			result->integer_kind = between_kind;
			result->source_index = between_source_index;
			result->lower = lower;
			result->upper = upper;
			result->lower_inclusive = lower_inclusive;
			result->upper_inclusive = upper_inclusive;
			result->not_between = not_between;
			predicate = std::move(result);
			return true;
		}
	}

	if (TryBuildNativeConjunctionPredicate(root, predicate)) {
		return true;
	}

	if (TryBuildNativeNullCheckPredicate(root, predicate)) {
		return true;
	}

	idx_t source_index;
	SljitNativeIntegerKind kind;
	SljitNativeIntegerCompareOp compare_op;
	idx_t right_source_index;
	SljitNativeDoubleSourceKind double_source_kind;
	SljitNativeDoubleSourceKind double_right_source_kind;
	double double_source_scale;
	double double_right_source_scale;
	double double_constant;
	uint64_t int128_lower;
	int64_t int128_upper;
	if (TryReadNativeIntegerCompareReferences(root, compare_op, kind, source_index, right_source_index)) {
		result->kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		result->integer_kind = kind;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeInt128CompareReferences(root, compare_op, source_index, right_source_index)) {
		result->kind = SljitNativePredicateKind::INT128_COMPARE_REFERENCES;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeDoubleCompareReferences(root, compare_op, double_source_kind, source_index, double_source_scale,
	                                         double_right_source_kind, right_source_index, double_right_source_scale)) {
		result->kind = SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->right_source_index = right_source_index;
		result->double_source_kind = double_source_kind;
		result->double_right_source_kind = double_right_source_kind;
		result->double_source_scale = double_source_scale;
		result->double_right_source_scale = double_right_source_scale;
		predicate = std::move(result);
		return true;
	}

	int64_t constant;
	bool constant_on_left;
	if (TryReadNativeIntegerCompareConstant(root, compare_op, kind, source_index, constant, constant_on_left)) {
		result->kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		result->integer_kind = kind;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->constant = constant;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeDoubleCompareConstant(root, compare_op, double_source_kind, source_index, double_source_scale,
	                                       double_constant, constant_on_left)) {
		result->kind = SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->double_source_kind = double_source_kind;
		result->double_source_scale = double_source_scale;
		result->double_constant = double_constant;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}
	if (TryReadNativeInt128CompareConstant(root, compare_op, source_index, int128_lower, int128_upper,
	                                       constant_on_left)) {
		result->kind = SljitNativePredicateKind::INT128_COMPARE_CONSTANT;
		result->compare_op = compare_op;
		result->source_index = source_index;
		result->int128_constant_lower = int128_lower;
		result->int128_constant_upper = int128_upper;
		result->constant_on_left = constant_on_left;
		predicate = std::move(result);
		return true;
	}

	vector<int64_t> constants;
	bool list_has_null;
	bool not_in;
	if (TryReadNativeIntegerInList(root, kind, source_index, constants, list_has_null, not_in)) {
		result->kind = SljitNativePredicateKind::INTEGER_IN_LIST;
		result->integer_kind = kind;
		result->source_index = source_index;
		result->constants = std::move(constants);
		result->list_has_null = list_has_null;
		result->not_in = not_in;
		predicate = std::move(result);
		return true;
	}

	string string_constant;
	if (TryReadNativeStringEqualConstant(root, source_index, string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_EQUAL_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	string prefix;
	if (TryReadNativeStringPrefixConstant(root, source_index, prefix)) {
		result->kind = SljitNativePredicateKind::STRING_PREFIX_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(prefix);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_SUFFIX, source_index,
	                                     string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_SUFFIX_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_CONTAINS, source_index,
	                                     string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_CONTAINS_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	if (TryReadNativeStringMatchConstant(root, ExecutionExpressionIntrinsicKind::STRING_LIKE, source_index,
	                                     string_constant) &&
	    SljitNativeLikePatternIsPercentOnly(string_constant)) {
		result->kind = SljitNativePredicateKind::STRING_LIKE_CONSTANT;
		result->source_index = source_index;
		result->string_constant = std::move(string_constant);
		predicate = std::move(result);
		return true;
	}

	return false;
}

bool TryBuildNativePredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate) {
	if (!TryBuildNativePredicateInternal(root, predicate)) {
		return false;
	}
	FinalizeSljitNativePredicateSourceIndices(*predicate);
	return true;
}

} // namespace duckdb

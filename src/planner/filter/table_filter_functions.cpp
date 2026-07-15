//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_functions.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/filter/table_filter_function_helpers.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

#include <algorithm>

namespace duckdb {

// LCOV_EXCL_START
unique_ptr<FunctionData> TableFilterFunctions::Bind(BindScalarFunctionInput &input) {
	throw BinderException("Table filter functions are for internal use only!");
}

bool TableFilterFunctions::IsTableFilterFunction(const Identifier &name) {
	static const char *const TABLE_FILTER_FUNCTIONS[] = {
	    BloomFilterScalarFun::NAME,     DynamicFilterScalarFun::NAME, OptionalFilterScalarFun::NAME,
	    PerfectHashJoinScalarFun::NAME, PrefixRangeScalarFun::NAME,   SelectivityOptionalFilterScalarFun::NAME};
	for (auto function_name : TABLE_FILTER_FUNCTIONS) {
		if (name == function_name) {
			return true;
		}
	}
	return false;
}
// LCOV_EXCL_STOP

void GetThresholdAndVectorsToCheck(SelectivityOptionalFilterType type, float &selectivity_threshold,
                                   idx_t &n_vectors_to_check) {
	static constexpr float MIN_MAX_THRESHOLD = 0.9f;
	static constexpr float BF_THRESHOLD = 0.5f;
	static constexpr float PHJ_THRESHOLD = 0.3f;
	static constexpr float PRF_THRESHOLD = 0.5f;

	static constexpr idx_t MIN_MAX_CHECK_N = 6;
	static constexpr idx_t BF_CHECK_N = 6;
	static constexpr idx_t PHJ_CHECK_N = 6;
	static constexpr idx_t PRF_CHECK_N = 6;

	switch (type) {
	case SelectivityOptionalFilterType::MIN_MAX:
		selectivity_threshold = MIN_MAX_THRESHOLD;
		n_vectors_to_check = MIN_MAX_CHECK_N;
		return;
	case SelectivityOptionalFilterType::BF:
		selectivity_threshold = BF_THRESHOLD;
		n_vectors_to_check = BF_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PHJ:
		selectivity_threshold = PHJ_THRESHOLD;
		n_vectors_to_check = PHJ_CHECK_N;
		return;
	case SelectivityOptionalFilterType::PRF:
		selectivity_threshold = PRF_THRESHOLD;
		n_vectors_to_check = PRF_CHECK_N;
		return;
	default:
		throw NotImplementedException("GetThresholdAndVectorsToCheck");
	}
}

bool IsSignedNumericRangePhysicalType(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static bool IsIntegralLogicalType(LogicalTypeId type_id) {
	switch (type_id) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool TryGetFoldedConstantValue(const Expression &expr, Value &value) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		value = expr.Cast<BoundConstantExpression>().GetValue();
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		if (cast_expr.IsTryCast()) {
			return false;
		}
		Value child_value;
		if (!TryGetFoldedConstantValue(cast_expr.Child(), child_value)) {
			return false;
		}
		string error_message;
		return child_value.DefaultTryCastAs(cast_expr.TargetType(), value, &error_message, false);
	}
	return false;
}

static bool TryGetSignedNumericConstant(const Expression &expr, const LogicalType &target_type, int64_t &result) {
	if (!IsSignedNumericRangePhysicalType(target_type.InternalType())) {
		return false;
	}
	Value value;
	if (!TryGetFoldedConstantValue(expr, value) || value.IsNull()) {
		return false;
	}

	const auto source_id = value.type().id();
	if (target_type.id() == LogicalTypeId::DECIMAL) {
		if (source_id == LogicalTypeId::DECIMAL) {
			if (DecimalType::GetScale(value.type()) != DecimalType::GetScale(target_type)) {
				return false;
			}
		} else if (!IsIntegralLogicalType(source_id)) {
			return false;
		}
	} else if (target_type.id() == LogicalTypeId::DATE) {
		if (source_id != LogicalTypeId::DATE) {
			return false;
		}
	} else if (target_type.id() != source_id) {
		return false;
	}

	Value cast_value;
	if (value.type() == target_type) {
		cast_value = value;
	} else {
		string error_message;
		if (!value.DefaultTryCastAs(target_type, cast_value, &error_message, false)) {
			return false;
		}
	}
	switch (target_type.InternalType()) {
	case PhysicalType::INT8:
		result = cast_value.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		result = cast_value.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		result = cast_value.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		result = cast_value.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static ExpressionType FlipComparison(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return comparison_type;
	}
}

static void MarkRangeEmpty(SignedNumericRangeFilterData &range) {
	range.empty = true;
	range.has_lower = true;
	range.has_upper = true;
	range.lower = 1;
	range.upper = 0;
}

static bool IntersectRange(SignedNumericRangeFilterData &result, const SignedNumericRangeFilterData &other) {
	if (result.empty || other.empty) {
		MarkRangeEmpty(result);
		return true;
	}
	if (other.has_lower && (!result.has_lower || other.lower > result.lower)) {
		result.has_lower = true;
		result.lower = other.lower;
	}
	if (other.has_upper && (!result.has_upper || other.upper < result.upper)) {
		result.has_upper = true;
		result.upper = other.upper;
	}
	if (result.has_lower && result.has_upper && result.lower > result.upper) {
		MarkRangeEmpty(result);
	}
	return true;
}

static bool TryBuildSignedRangeInterval(const Expression &expr, const LogicalType &target_type,
                                        SignedNumericRangeFilterData &range);

static bool TryBuildSignedComparisonInterval(const Expression &expr, const LogicalType &target_type,
                                             SignedNumericRangeFilterData &range) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	const auto expr_type = expr.GetExpressionType();
	switch (expr_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		break;
	default:
		return false;
	}

	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);
	optional_ptr<const Expression> constant_expr;
	auto comparison_type = expr_type;
	if (left.GetExpressionType() == ExpressionType::BOUND_REF) {
		auto &bound_ref = left.Cast<BoundReferenceExpression>();
		if (bound_ref.Index() != 0) {
			return false;
		}
		constant_expr = &right;
	} else if (right.GetExpressionType() == ExpressionType::BOUND_REF) {
		auto &bound_ref = right.Cast<BoundReferenceExpression>();
		if (bound_ref.Index() != 0) {
			return false;
		}
		constant_expr = &left;
		comparison_type = FlipComparison(comparison_type);
	} else {
		return false;
	}

	int64_t constant;
	if (!TryGetSignedNumericConstant(*constant_expr, target_type, constant)) {
		return false;
	}

	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		range.has_lower = true;
		range.has_upper = true;
		range.lower = constant;
		range.upper = constant;
		return true;
	case ExpressionType::COMPARE_GREATERTHAN:
		if (constant == NumericLimits<int64_t>::Maximum()) {
			MarkRangeEmpty(range);
		} else {
			range.has_lower = true;
			range.lower = constant + 1;
		}
		return true;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		range.has_lower = true;
		range.lower = constant;
		return true;
	case ExpressionType::COMPARE_LESSTHAN:
		if (constant == NumericLimits<int64_t>::Minimum()) {
			MarkRangeEmpty(range);
		} else {
			range.has_upper = true;
			range.upper = constant - 1;
		}
		return true;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		range.has_upper = true;
		range.upper = constant;
		return true;
	default:
		return false;
	}
}

static bool TryBuildSignedRangeInterval(const Expression &expr, const LogicalType &target_type,
                                        SignedNumericRangeFilterData &range) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			SignedNumericRangeFilterData child_range;
			if (!TryBuildSignedRangeInterval(*child, target_type, child_range)) {
				return false;
			}
			IntersectRange(range, child_range);
		}
		return true;
	}
	return TryBuildSignedComparisonInterval(expr, target_type, range);
}

static bool TryCollectSignedRangeIntervals(const Expression &expr, const LogicalType &target_type,
                                           vector<SignedNumericRangeFilterData> &ranges) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conjunction.GetChildren()) {
			if (!TryCollectSignedRangeIntervals(*child, target_type, ranges)) {
				return false;
			}
		}
		return true;
	}
	SignedNumericRangeFilterData range;
	if (!TryBuildSignedRangeInterval(expr, target_type, range)) {
		return false;
	}
	ranges.push_back(range);
	return true;
}

static bool RangeLowerBefore(const SignedNumericRangeFilterData &left, const SignedNumericRangeFilterData &right) {
	if (!left.has_lower) {
		return right.has_lower;
	}
	if (!right.has_lower) {
		return false;
	}
	return left.lower < right.lower;
}

static bool RangesCanMerge(const SignedNumericRangeFilterData &left, const SignedNumericRangeFilterData &right) {
	if (!left.has_upper || !right.has_lower) {
		return true;
	}
	return left.upper == NumericLimits<int64_t>::Maximum() || right.lower <= left.upper + 1;
}

static void MergeRange(SignedNumericRangeFilterData &result, const SignedNumericRangeFilterData &other) {
	if (!result.has_upper || !other.has_upper) {
		result.has_upper = false;
		return;
	}
	if (other.upper > result.upper) {
		result.upper = other.upper;
	}
}

static bool TryMergeSignedRanges(vector<SignedNumericRangeFilterData> &ranges,
                                 SignedNumericRangeFilterData &merged_range) {
	ranges.erase(std::remove_if(ranges.begin(), ranges.end(),
	                            [](const SignedNumericRangeFilterData &range) { return range.empty; }),
	             ranges.end());
	if (ranges.empty()) {
		MarkRangeEmpty(merged_range);
		return true;
	}
	std::sort(ranges.begin(), ranges.end(), RangeLowerBefore);
	merged_range = ranges[0];
	for (idx_t i = 1; i < ranges.size(); i++) {
		if (!RangesCanMerge(merged_range, ranges[i])) {
			return false;
		}
		MergeRange(merged_range, ranges[i]);
	}
	return true;
}

bool TryGetSignedNumericRange(const Expression &expr, const LogicalType &target_type,
                              SignedNumericRangeFilterData &range) {
	if (!IsSignedNumericRangePhysicalType(target_type.InternalType())) {
		return false;
	}
	vector<SignedNumericRangeFilterData> ranges;
	SignedNumericRangeFilterData merged_range;
	if (!TryCollectSignedRangeIntervals(expr, target_type, ranges) || !TryMergeSignedRanges(ranges, merged_range)) {
		return false;
	}
	range = merged_range;
	return true;
}

bool TryGetSignedNumericRange(const ExpressionFilter &filter, ExpressionFilterState &state,
                              const LogicalType &target_type, SignedNumericRangeFilterData &range) {
	const auto physical_type = target_type.InternalType();
	if (!IsSignedNumericRangePhysicalType(physical_type)) {
		return false;
	}
	if (!state.fast_signed_numeric_range_filter_initialized) {
		state.fast_signed_numeric_range_filter_initialized = true;
		state.fast_signed_numeric_range_type = physical_type;

		SignedNumericRangeFilterData merged_range;
		state.fast_signed_numeric_range_filter_supported =
		    TryGetSignedNumericRange(*filter.expr, target_type, merged_range);
		if (state.fast_signed_numeric_range_filter_supported) {
			state.fast_signed_numeric_range_filter_always_false = merged_range.empty;
			state.fast_signed_numeric_range_has_lower = merged_range.has_lower;
			state.fast_signed_numeric_range_has_upper = merged_range.has_upper;
			state.fast_signed_numeric_range_lower = merged_range.lower;
			state.fast_signed_numeric_range_upper = merged_range.upper;
		}
	}
	if (!state.fast_signed_numeric_range_filter_supported || state.fast_signed_numeric_range_type != physical_type) {
		return false;
	}
	range.empty = state.fast_signed_numeric_range_filter_always_false;
	range.has_lower = state.fast_signed_numeric_range_has_lower;
	range.has_upper = state.fast_signed_numeric_range_has_upper;
	range.lower = state.fast_signed_numeric_range_lower;
	range.upper = state.fast_signed_numeric_range_upper;
	return true;
}

static unique_ptr<Expression> CreateSingleArgumentFunctionExpression(const ScalarFunction &function,
                                                                     const LogicalType &target_type,
                                                                     unique_ptr<FunctionData> bind_data) {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(target_type, storage_t(0)));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                          std::move(bind_data));
}

unique_ptr<Expression> CreateOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                      const LogicalType &target_type) {
	auto function = OptionalFilterScalarFun::GetFunction(target_type);
	auto bind_data = make_uniq<OptionalFilterFunctionData>(std::move(child_expr));
	return CreateSingleArgumentFunctionExpression(function, target_type, std::move(bind_data));
}

unique_ptr<Expression> CreateSelectivityOptionalFilterExpression(unique_ptr<Expression> child_expr,
                                                                 const LogicalType &target_type,
                                                                 float selectivity_threshold,
                                                                 idx_t n_vectors_to_check) {
	auto function = SelectivityOptionalFilterScalarFun::GetFunction(target_type);
	auto bind_data = make_uniq<SelectivityOptionalFilterFunctionData>(std::move(child_expr), selectivity_threshold,
	                                                                  n_vectors_to_check);
	return CreateSingleArgumentFunctionExpression(function, target_type, std::move(bind_data));
}

unique_ptr<Expression> CreateDynamicFilterExpression(shared_ptr<DynamicFilterData> filter_data,
                                                     const LogicalType &target_type) {
	auto function = DynamicFilterScalarFun::GetFunction(target_type);
	auto bind_data = make_uniq<DynamicFilterFunctionData>(std::move(filter_data));
	return CreateSingleArgumentFunctionExpression(function, target_type, std::move(bind_data));
}

void TableFilterFunctionSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                  const BoundScalarFunction &function) {
	// Runtime state cannot be serialized - write nothing
}

unique_ptr<FunctionData> TableFilterFunctionDeserialize(Deserializer &deserializer, BoundScalarFunction &function) {
	auto key_type = function.GetArguments().empty() ? LogicalType::ANY : function.GetArguments()[0];
	if (function.GetName() == BloomFilterScalarFun::NAME) {
		return make_uniq<BloomFilterFunctionData>(nullptr, false, string(), key_type);
	}
	if (function.GetName() == PerfectHashJoinScalarFun::NAME) {
		return make_uniq<PerfectHashJoinFunctionData>(nullptr, string());
	}
	if (function.GetName() == PrefixRangeScalarFun::NAME) {
		return make_uniq<PrefixRangeFunctionData>(nullptr, string(), key_type);
	}
	if (function.GetName() == DynamicFilterScalarFun::NAME) {
		return make_uniq<DynamicFilterFunctionData>(nullptr);
	}
	throw InternalException("Unsupported table filter function \"%s\" during deserialization", function.GetName());
}

} // namespace duckdb

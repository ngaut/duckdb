//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_expression_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_expression_codegen.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_native_double_source_helpers.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static string NativeRegionIntegerBinaryOverflowMessage(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	if (kind == SljitNativeIntegerKind::DATE) {
		return "Date out of range";
	}
	if (kind != SljitNativeIntegerKind::DECIMAL64) {
		return NativeIntegerBinaryOverflowMessage(op);
	}
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "Overflow in addition of DECIMAL";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "Overflow in subtract of DECIMAL";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "Overflow in multiplication of DECIMAL";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

bool SljitCompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
                                                    string &error) {
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
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryConstant(
		    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.function, error,
		    semantic.check_arithmetic_overflow, semantic.check_result_range, semantic.result_min, semantic.result_max);
		if (!expr.code) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			expr.flat_code = BuildSljitNativeFlatIntegerBinaryConstant(
			    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.flat_function, error);
			if (!expr.flat_code) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		expr.code = BuildSljitNativeIntegerBinaryReferences(
		    semantic.integer_kind, semantic.binary_op, expr.function, error, semantic.check_arithmetic_overflow,
		    semantic.check_result_range, semantic.result_min, semantic.result_max);
		if (!expr.code) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			expr.flat_code = BuildSljitNativeFlatIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op,
			                                                             expr.flat_function, error);
			if (!expr.flat_code) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		expr.code =
		    BuildSljitNativeDoubleBinaryConstant(semantic.double_binary_op, semantic.double_source_kind,
		                                         semantic.constant_on_left, single_precision, expr.function, error);
		if (!expr.code) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind)) {
			expr.flat_code = BuildSljitNativeFlatDoubleBinaryConstant(
			    semantic.double_binary_op, semantic.double_source_kind, semantic.constant_on_left, single_precision,
			    expr.flat_function, error);
			if (!expr.flat_code) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		expr.code = BuildSljitNativeDoubleBinaryReferences(semantic.double_binary_op, semantic.double_source_kind,
		                                                   semantic.double_right_source_kind, single_precision,
		                                                   expr.function, error);
		if (!expr.code) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind) &&
		    IsDirectNativeFloatingSource(semantic.double_right_source_kind)) {
			expr.flat_code = BuildSljitNativeFlatDoubleBinaryReferences(
			    semantic.double_binary_op, semantic.double_source_kind, semantic.double_right_source_kind,
			    single_precision, expr.flat_function, error);
			if (!expr.flat_code) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerSelectConstant(
			    semantic.integer_kind, semantic.compare_op, semantic.constant_on_left, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerCompareConstant(semantic.integer_kind, semantic.compare_op,
		                                                   semantic.constant_on_left, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op,
			                                                           expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code =
		    BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op, expr.function, error);
		return expr.code != nullptr;
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
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerInListSelect(semantic.integer_kind, semantic.constants.size(),
			                                                       semantic.list_has_null, semantic.not_in,
			                                                       expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(),
		                                          semantic.list_has_null, semantic.not_in, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (require_boolean) {
			expr.select_code = BuildSljitNativeIntegerBetweenSelect(
			    semantic.integer_kind, semantic.lower, semantic.upper, semantic.lower_inclusive,
			    semantic.upper_inclusive, semantic.not_between, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
		                                           semantic.lower_inclusive, semantic.upper_inclusive,
		                                           semantic.not_between, expr.function, error);
		return expr.code != nullptr;
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
		if (require_boolean) {
			expr.select_code = BuildSljitNativeNullCheckSelect(semantic.null_check_op, expr.select_function, error);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeNullCheck(semantic.null_check_op, expr.function, error);
		return expr.code != nullptr;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (require_boolean) {
			expr.predicate_select_code =
			    BuildSljitNativePredicate(*semantic.predicate, false, expr.predicate_select_function, error);
			return expr.predicate_select_code != nullptr;
		}
		expr.predicate_code = BuildSljitNativePredicate(*semantic.predicate, true, expr.predicate_function, error);
		return expr.predicate_code != nullptr;
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
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		if (require_boolean && semantic.return_type.id() != LogicalTypeId::BOOLEAN) {
			error = "SLJIT typed expression tree predicate must return BOOLEAN";
			return false;
		}
		if (!semantic.expression_tree) {
			error = "SLJIT typed expression tree plan is missing its typed expression IR";
			return false;
		}
		expr.overflow_message = "Overflow in arithmetic expression";
		if (require_boolean) {
			expr.select_code = BuildSljitNativeTypedExpressionTreeSelect(
			    *semantic.expression_tree, expr.select_function, error, semantic.emit_flat_nullable_fast_path);
			return expr.select_code != nullptr;
		}
		expr.code = BuildSljitNativeTypedExpressionTree(*semantic.expression_tree, semantic.integer_kind, expr.function,
		                                                error, semantic.emit_flat_nullable_fast_path);
		return expr.code != nullptr;
	default:
		throw InternalException("Unknown SLJIT native region expression kind");
	}
}

bool SljitPrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan, bool require_boolean,
                                                      SljitExecutableRegionExpression &expr, string &error,
                                                      const vector<bool> *input_not_null) {
	SljitPrepareExecutableRegionExpression(plan, expr, input_not_null);
	return SljitCompilePreparedExecutableRegionExpression(expr, require_boolean, error);
}

} // namespace duckdb

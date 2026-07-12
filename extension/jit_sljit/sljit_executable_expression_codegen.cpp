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
		expr.vector.Code() = BuildSljitNativeIntegerBinaryConstant(
		    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.vector.Function(), error,
		    semantic.check_arithmetic_overflow, semantic.check_result_range, semantic.result_min, semantic.result_max);
		if (!expr.vector.Code()) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			expr.flat.Code() = BuildSljitNativeFlatIntegerBinaryConstant(
			    semantic.integer_kind, semantic.binary_op, semantic.constant_on_left, expr.flat.Function(), error);
			if (!expr.flat.Code()) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		expr.overflow_message = NativeRegionIntegerBinaryOverflowMessage(semantic.integer_kind, semantic.binary_op);
		expr.vector.Code() = BuildSljitNativeIntegerBinaryReferences(
		    semantic.integer_kind, semantic.binary_op, expr.vector.Function(), error,
		    semantic.check_arithmetic_overflow, semantic.check_result_range, semantic.result_min, semantic.result_max);
		if (!expr.vector.Code()) {
			return false;
		}
		if (!semantic.check_arithmetic_overflow && !semantic.check_result_range &&
		    (semantic.integer_kind == SljitNativeIntegerKind::INT32 ||
		     semantic.integer_kind == SljitNativeIntegerKind::INT64)) {
			expr.flat.Code() = BuildSljitNativeFlatIntegerBinaryReferences(semantic.integer_kind, semantic.binary_op,
			                                                               expr.flat.Function(), error);
			if (!expr.flat.Code()) {
				return false;
			}
		}
		return true;
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		expr.vector.Code() = BuildSljitNativeDoubleBinaryConstant(
		    semantic.double_binary_op, semantic.double_source_kind, semantic.constant_on_left, single_precision,
		    expr.vector.Function(), error);
		if (!expr.vector.Code()) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind)) {
			expr.flat.Code() = BuildSljitNativeFlatDoubleBinaryConstant(
			    semantic.double_binary_op, semantic.double_source_kind, semantic.constant_on_left, single_precision,
			    expr.flat.Function(), error);
			if (!expr.flat.Code()) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES: {
		auto single_precision = semantic.return_type.InternalType() == PhysicalType::FLOAT;
		expr.vector.Code() = BuildSljitNativeDoubleBinaryReferences(
		    semantic.double_binary_op, semantic.double_source_kind, semantic.double_right_source_kind, single_precision,
		    expr.vector.Function(), error);
		if (!expr.vector.Code()) {
			return false;
		}
		if (IsDirectNativeFloatingSource(semantic.double_source_kind) &&
		    IsDirectNativeFloatingSource(semantic.double_right_source_kind)) {
			expr.flat.Code() = BuildSljitNativeFlatDoubleBinaryReferences(
			    semantic.double_binary_op, semantic.double_source_kind, semantic.double_right_source_kind,
			    single_precision, expr.flat.Function(), error);
			if (!expr.flat.Code()) {
				return false;
			}
		}
		return true;
	}
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		if (require_boolean) {
			expr.select.Code() = BuildSljitNativeIntegerSelectConstant(
			    semantic.integer_kind, semantic.compare_op, semantic.constant_on_left, expr.select.Function(), error);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() = BuildSljitNativeIntegerCompareConstant(
		    semantic.integer_kind, semantic.compare_op, semantic.constant_on_left, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (require_boolean) {
			expr.select.Code() = BuildSljitNativeIntegerSelectReferences(semantic.integer_kind, semantic.compare_op,
			                                                             expr.select.Function(), error);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() = BuildSljitNativeIntegerCompareReferences(semantic.integer_kind, semantic.compare_op,
		                                                              expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		expr.overflow_message =
		    NativeIntegerCastOverflowMessage(semantic.cast_source_width, semantic.cast_target_width);
		expr.vector.Code() = BuildSljitNativeIntegerCast(semantic.cast_source_width, semantic.cast_target_width,
		                                                 semantic.try_cast, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		expr.overflow_message = NativeSignedToUnsignedIntegerCastOverflowMessage(semantic.cast_source_width,
		                                                                         semantic.unsigned_cast_target_width);
		expr.vector.Code() =
		    BuildSljitNativeSignedToUnsignedIntegerCast(semantic.cast_source_width, semantic.unsigned_cast_target_width,
		                                                semantic.try_cast, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
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
	case SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY:
		if (require_boolean) {
			error = "SLJIT decimal128 widening multiply cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() = BuildSljitNativeDecimal128WideningMultiply(
		    semantic.cast_source_width, semantic.right_cast_source_width, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		expr.vector.Code() =
		    BuildSljitNativeIntegerCoalesce(semantic.signed_integer_width, semantic.coalesce_rhs_kind,
		                                    semantic.coalesce_constant_is_null, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		if (require_boolean) {
			expr.select.Code() = BuildSljitNativeIntegerInListSelect(semantic.integer_kind, semantic.constants.size(),
			                                                         semantic.list_has_null, semantic.not_in,
			                                                         expr.select.Function(), error);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() =
		    BuildSljitNativeIntegerInList(semantic.integer_kind, semantic.constants.size(), semantic.list_has_null,
		                                  semantic.not_in, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		if (require_boolean) {
			expr.select.Code() = BuildSljitNativeIntegerBetweenSelect(
			    semantic.integer_kind, semantic.lower, semantic.upper, semantic.lower_inclusive,
			    semantic.upper_inclusive, semantic.not_between, expr.select.Function(), error);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() = BuildSljitNativeIntegerBetween(semantic.integer_kind, semantic.lower, semantic.upper,
		                                                    semantic.lower_inclusive, semantic.upper_inclusive,
		                                                    semantic.not_between, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		if (require_boolean) {
			error = "SLJIT constant_or_null cannot lower as a predicate";
			return false;
		}
		expr.predicate.Code() = BuildSljitNativeConstantOrNull(semantic.constant_or_null.guard_source_indices,
		                                                       expr.predicate.Function(), error);
		return expr.predicate.Code() != nullptr;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		if (require_boolean) {
			error = "SLJIT string compression cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() =
		    BuildSljitNativeStringCompress(semantic.string_compress_target_size, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT string decompression cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() =
		    BuildSljitNativeStringDecompress(semantic.string_decompress_source_size, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
		if (require_boolean) {
			error = "SLJIT string substring cannot lower as a predicate";
			return false;
		}
		return true;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (require_boolean) {
			error = "SLJIT integral compression cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() = BuildSljitNativeIntegralCompress(
		    semantic.cast_source_width, semantic.unsigned_cast_target_width, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		if (require_boolean) {
			error = "SLJIT integral decompression cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() = BuildSljitNativeIntegralDecompress(
		    semantic.unsigned_source_width, semantic.cast_target_width, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::DATE_YEAR:
		if (require_boolean) {
			error = "SLJIT date-year cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() = BuildSljitNativeDateYear(expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		if (require_boolean) {
			error = "SLJIT error-guarded reference cannot lower as a predicate";
			return false;
		}
		expr.vector.Code() =
		    BuildSljitNativeErrorGuardedReference(semantic.guarded_value_size, semantic.guard_compare_op,
		                                          semantic.guard_constant_on_left, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (require_boolean) {
			expr.select.Code() = BuildSljitNativeNullCheckSelect(semantic.null_check_op, expr.select.Function(), error);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() = BuildSljitNativeNullCheck(semantic.null_check_op, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (require_boolean) {
			expr.predicate_select.Code() =
			    BuildSljitNativePredicate(*semantic.predicate, false, expr.predicate_select.Function(), error);
			return expr.predicate_select.Code() != nullptr;
		}
		expr.predicate.Code() = BuildSljitNativePredicate(*semantic.predicate, true, expr.predicate.Function(), error);
		return expr.predicate.Code() != nullptr;
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
		expr.vector.Code() = BuildSljitNativeExpressionTree(*semantic.expression_tree, expr.vector.Function(), error);
		return expr.vector.Code() != nullptr;
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
			expr.select.Code() = BuildSljitNativeTypedExpressionTreeSelect(
			    *semantic.expression_tree, expr.select.Function(), error, semantic.emit_flat_nullable_fast_path);
			return expr.select.Code() != nullptr;
		}
		expr.vector.Code() =
		    BuildSljitNativeTypedExpressionTree(*semantic.expression_tree, semantic.integer_kind,
		                                        expr.vector.Function(), error, semantic.emit_flat_nullable_fast_path);
		return expr.vector.Code() != nullptr;
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

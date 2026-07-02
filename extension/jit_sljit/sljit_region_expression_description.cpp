//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_description.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr) {
	string result;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		result = "native:reference";
		result += expr.references_region_input ? ":region-input" : ":projection-local";
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

string DescribeNativeRegionExpressionList(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	string result;
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (expr_idx > 0) {
			result += ",";
		}
		result += DescribeNativeRegionExpression(expressions[expr_idx]);
	}
	return result;
}

} // namespace duckdb

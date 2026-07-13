//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

namespace duckdb {

static constexpr int64_t SLJIT_DATE_MIN_DAYS = -2147483646;
static constexpr int64_t SLJIT_DATE_MAX_DAYS = 2147483646;

bool TryReadNativeRegionExpression(const ExecutionExpressionIR &root, bool require_boolean,
                                   SljitNativeRegionExpressionPlan &expr) {
	if (!require_boolean && root.kind == ExecutionExpressionIRKind::CONSTANT) {
		expr.kind = SljitNativeRegionExpressionKind::CONSTANT;
		expr.return_type = root.return_type;
		expr.constant_value = root.constant;
		return true;
	}

	if (!require_boolean && root.kind == ExecutionExpressionIRKind::REFERENCE) {
		expr.kind = SljitNativeRegionExpressionKind::REFERENCE;
		expr.return_type = root.return_type;
		expr.source_index = root.ref_index;
		return true;
	}

	if (!require_boolean && TryReadNativeScalarIntrinsicRegionExpression(root, expr)) {
		return true;
	}

	// Any boolean tree accepted by the SIMD gate uses the typed generated
	// selector. The backend then chooses packed evaluation or a target-specific
	// generated scalar loop from the same plan. String, IN, BETWEEN, and
	// unsupported arithmetic predicates fail the gate and retain their
	// specialized handlers below.
	if (require_boolean && TryPlanSljitTypedExpressionTreeSimd(root).supported &&
	    TryBuildSljitNativeTypedExpressionTreePlan(root, expr)) {
		return true;
	}

	if (TryReadNativeRegionPredicateExpression(root, expr)) {
		if (expr.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			AttachSljitNativeExpressionTree(root, expr);
		}
		return true;
	}

	SljitNativeConstantOrNull constant_or_null;
	if (!require_boolean && TryReadNativeConstantOrNull(root, constant_or_null)) {
		expr.kind = SljitNativeRegionExpressionKind::CONSTANT_OR_NULL;
		expr.return_type = root.return_type;
		expr.constant_or_null = std::move(constant_or_null);
		return true;
	}

	if (require_boolean) {
		if (TryBuildSljitNativeTypedExpressionTreePlan(root, expr)) {
			return true;
		}
		return false;
	}

	SljitNativeDoubleBinaryOp double_binary_op;
	SljitNativeDoubleSourceKind double_source_kind;
	SljitNativeDoubleSourceKind double_right_source_kind;
	double double_source_scale;
	double double_right_source_scale;
	double double_constant;
	idx_t source_index;
	idx_t right_source_index;
	bool constant_on_left;
	if (TryReadNativeDoubleBinaryReferences(root, double_binary_op, double_source_kind, source_index,
	                                        double_source_scale, double_right_source_kind, right_source_index,
	                                        double_right_source_scale)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES;
		expr.double_binary_op = double_binary_op;
		expr.double_source_kind = double_source_kind;
		expr.double_right_source_kind = double_right_source_kind;
		expr.double_source_scale = double_source_scale;
		expr.double_right_source_scale = double_right_source_scale;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		return true;
	}
	if (TryReadNativeDoubleBinaryConstant(root, double_binary_op, double_source_kind, source_index, double_source_scale,
	                                      double_constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT;
		expr.double_binary_op = double_binary_op;
		expr.double_source_kind = double_source_kind;
		expr.double_source_scale = double_source_scale;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.double_constant = double_constant;
		expr.constant_on_left = constant_on_left;
		return true;
	}
	SljitNativeSignedIntegerWidth cast_source_width;
	SljitNativeSignedIntegerWidth cast_target_width;
	SljitNativeUnsignedIntegerWidth unsigned_cast_target_width;
	bool try_cast;
	if (TryReadNativeSignedToUnsignedIntegerCast(root, cast_source_width, unsigned_cast_target_width, source_index,
	                                             try_cast)) {
		expr.kind = SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = cast_source_width;
		expr.unsigned_cast_target_width = unsigned_cast_target_width;
		expr.query_location = root.query_location;
		expr.try_cast = try_cast;
		return true;
	}
	if (TryReadNativeIntegerCast(root, cast_source_width, cast_target_width, source_index, try_cast)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = cast_source_width;
		expr.cast_target_width = cast_target_width;
		expr.query_location = root.query_location;
		expr.try_cast = try_cast;
		return true;
	}

	SljitNativeSignedIntegerWidth coalesce_width;
	SljitNativeCoalesceRhsKind coalesce_rhs_kind;
	idx_t coalesce_right_source_index;
	int64_t coalesce_constant;
	bool coalesce_constant_is_null;
	if (TryReadNativeIntegerCoalesce(root, coalesce_width, source_index, coalesce_rhs_kind, coalesce_right_source_index,
	                                 coalesce_constant, coalesce_constant_is_null)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_COALESCE;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = coalesce_right_source_index;
		expr.signed_integer_width = coalesce_width;
		expr.coalesce_rhs_kind = coalesce_rhs_kind;
		expr.constant = coalesce_constant;
		expr.coalesce_constant_is_null = coalesce_constant_is_null;
		return true;
	}

	SljitNativeIntegerBinaryOp binary_op;
	SljitNativeIntegerKind integer_kind;
	int64_t constant;
	int64_t result_min;
	int64_t result_max;
	if (TryReadNativeDateBinaryReferences(root, binary_op, source_index, right_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		expr.integer_kind = SljitNativeIntegerKind::DATE;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = true;
		expr.check_result_range = true;
		expr.result_min = SLJIT_DATE_MIN_DAYS;
		expr.result_max = SLJIT_DATE_MAX_DAYS;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryReadNativeDateBinaryConstant(root, binary_op, source_index, constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT;
		expr.integer_kind = SljitNativeIntegerKind::DATE;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = constant;
		expr.constant_on_left = constant_on_left;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = true;
		expr.check_result_range = true;
		expr.result_min = SLJIT_DATE_MIN_DAYS;
		expr.result_max = SLJIT_DATE_MAX_DAYS;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryReadNativeDecimal64BinaryReferences(root, binary_op, source_index, right_source_index, result_min,
	                                           result_max)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		expr.integer_kind = SljitNativeIntegerKind::DECIMAL64;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = root.arithmetic_overflow_check;
		expr.check_result_range = true;
		expr.result_min = result_min;
		expr.result_max = result_max;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryReadNativeDecimal64BinaryConstant(root, binary_op, source_index, constant, constant_on_left, result_min,
	                                         result_max)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT;
		expr.integer_kind = SljitNativeIntegerKind::DECIMAL64;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = constant;
		expr.constant_on_left = constant_on_left;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = root.arithmetic_overflow_check;
		expr.check_result_range = true;
		expr.result_min = result_min;
		expr.result_max = result_max;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryReadNativeIntegerBinaryReferences(root, binary_op, integer_kind, source_index, right_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		expr.integer_kind = integer_kind;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = root.arithmetic_overflow_check;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryReadNativeIntegerBinaryConstant(root, binary_op, integer_kind, source_index, constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT;
		expr.integer_kind = integer_kind;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = constant;
		expr.constant_on_left = constant_on_left;
		expr.binary_op = binary_op;
		expr.check_arithmetic_overflow = root.arithmetic_overflow_check;
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryBuildSljitNativeAnyExpressionTreePlan(root, expr)) {
		return true;
	}
	return false;
}

bool TryLowerNativeRegionExpression(const ExecutionExpressionFragment &fragment, bool require_boolean,
                                    SljitNativeRegionExpressionPlan &expr, string &error, bool render_diagnostics) {
	if (!fragment.root) {
		error = "sljit-expression-lowering-missing-root";
		return false;
	}
	if (!TryReadNativeRegionExpression(*fragment.root, require_boolean, expr)) {
		error = "sljit-expression-lowering-unsupported";
		error += ";root_kind=" + string(ExecutionExpressionIRKindToString(fragment.root->kind));
		error += ";logical_type=" + fragment.root->return_type.ToString();
		error += require_boolean ? ";required=boolean" : ";required=value";
		if (render_diagnostics && !fragment.ir.empty()) {
			error += ";ir=" + fragment.ir;
		}
		return false;
	}
	if (!expr.expression_tree && require_boolean) {
		SljitNativeRegionExpressionPlan expression_tree;
		if (TryBuildSljitNativeTypedExpressionTreePlan(*fragment.root, expression_tree)) {
			expr.expression_tree = std::move(expression_tree.expression_tree);
			expr.expression_tree_source_indices = std::move(expression_tree.expression_tree_source_indices);
		}
	}
	if (!require_boolean && !expr.expression_tree) {
		SljitNativeRegionExpressionPlan expression_tree;
		if (TryBuildSljitNativeAnyExpressionTreePlan(*fragment.root, expression_tree)) {
			expr.expression_tree = std::move(expression_tree.expression_tree);
			expr.expression_tree_source_indices = std::move(expression_tree.expression_tree_source_indices);
		}
	}
	if (render_diagnostics) {
		expr.ir = fragment.ir;
	}
	return true;
}

} // namespace duckdb

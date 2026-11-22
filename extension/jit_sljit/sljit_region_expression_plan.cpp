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
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

static constexpr int64_t SLJIT_DATE_MIN_DAYS = -2147483646;
static constexpr int64_t SLJIT_DATE_MAX_DAYS = 2147483646;

static bool TryReadNativeRegionExpressionKind(const ExecutionExpressionIR &root, bool require_boolean,
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

	if (TryReadNativeRegionPredicateExpression(root, require_boolean, expr)) {
		if (expr.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			AttachSljitNativeExpressionTree(root, expr);
		}
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
	uint32_t double_source_decimal_scale;
	uint32_t double_right_source_decimal_scale;
	double double_constant;
	idx_t source_index;
	idx_t right_source_index;
	bool constant_on_left;
	if (TryReadNativeDoubleBinaryReferences(root, double_binary_op, double_source_kind, source_index,
	                                        double_source_decimal_scale, double_right_source_kind, right_source_index,
	                                        double_right_source_decimal_scale)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES;
		expr.double_binary_op = double_binary_op;
		expr.double_source_kind = double_source_kind;
		expr.double_right_source_kind = double_right_source_kind;
		expr.double_source_decimal_scale = double_source_decimal_scale;
		expr.double_right_source_decimal_scale = double_right_source_decimal_scale;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		return true;
	}
	if (TryReadNativeDoubleBinaryConstant(root, double_binary_op, double_source_kind, source_index,
	                                      double_source_decimal_scale, double_constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT;
		expr.double_binary_op = double_binary_op;
		expr.double_source_kind = double_source_kind;
		expr.double_source_decimal_scale = double_source_decimal_scale;
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
		expr.cast_chain = ReadNativeIntegerCastChain(root);
		expr.try_cast = try_cast;
		return true;
	}
	if (TryReadNativeIntegerCast(root, cast_source_width, cast_target_width, source_index, try_cast)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = cast_source_width;
		expr.cast_target_width = cast_target_width;
		expr.cast_chain = ReadNativeIntegerCastChain(root);
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
	return TryBuildSljitNativeTypedExpressionTreePlan(root, expr);
}

//! Every plan carries the typed expression tree of its expression when the trees evaluate it, so the kernels that
//! evaluate trees take any plan
bool TryReadNativeRegionExpression(const ExecutionExpressionIR &root, bool require_boolean,
                                   SljitNativeRegionExpressionPlan &expr) {
	if (!TryReadNativeRegionExpressionKind(root, require_boolean, expr)) {
		return false;
	}
	if (!expr.expression_tree) {
		SljitNativeRegionExpressionPlan expression_tree;
		if (TryBuildSljitNativeTypedExpressionTreePlan(root, expression_tree)) {
			expr.expression_tree = std::move(expression_tree.expression_tree);
			expr.expression_tree_source_indices = std::move(expression_tree.expression_tree_source_indices);
		}
	}
	return true;
}

static bool SljitExpressionIRCannotThrow(const ExecutionExpressionIR &node) {
	if (node.exception_behavior != ExecutionExpressionExceptionKind::NONE &&
	    node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR) {
		return false;
	}
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child && !SljitExpressionIRCannotThrow(*child)) {
			return false;
		}
	}
	for (auto &child : node.children) {
		if (!SljitExpressionIRCannotThrow(*child)) {
			return false;
		}
	}
	return true;
}

//! The type a narrow signed value compares as: INTEGER for TINYINT/SMALLINT, the same-scale DECIMAL64 for a DECIMAL
//! stored in 16 or 32 bits (whose raw integer the generated loops sign-extend), or nothing for any other type
static bool TryGetSljitWidenedComparisonType(const ExecutionExpressionIR &node, LogicalType &widened) {
	if (node.return_type.id() == LogicalTypeId::DECIMAL) {
		if (node.physical_type != PhysicalType::INT16 && node.physical_type != PhysicalType::INT32) {
			return false;
		}
		widened = LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT64, DecimalType::GetScale(node.return_type));
		return true;
	}
	if (!node.return_type.IsIntegral() ||
	    (node.physical_type != PhysicalType::INT8 && node.physical_type != PhysicalType::INT16)) {
		return false;
	}
	widened = LogicalType::INTEGER;
	return true;
}

//! A narrow signed column or constant, or a column widened to another narrow type of the same scale, with the type
//! it compares as
static bool TryGetSljitNarrowSignedValue(const ExecutionExpressionIR &node, LogicalType &widened) {
	if (!TryGetSljitWidenedComparisonType(node, widened)) {
		return false;
	}
	if (node.kind == ExecutionExpressionIRKind::CAST) {
		LogicalType source_widened;
		return !node.try_cast && node.left && node.left->kind == ExecutionExpressionIRKind::REFERENCE &&
		       TryGetSljitWidenedComparisonType(*node.left, source_widened) && source_widened == widened &&
		       GetTypeIdSize(node.left->physical_type) <= GetTypeIdSize(node.physical_type);
	}
	return node.kind == ExecutionExpressionIRKind::REFERENCE || node.kind == ExecutionExpressionIRKind::CONSTANT;
}

//! A narrow signed value as the type it compares as: a constant converts exactly, a column takes a widening cast,
//! and a widening cast widens to that type instead
static void SljitWidenNarrowSignedValue(unique_ptr<ExecutionExpressionIR> &node, const LogicalType &widened) {
	if (node->kind == ExecutionExpressionIRKind::CONSTANT) {
		node->constant = node->constant.DefaultCastAs(widened);
		node->return_type = widened;
		node->physical_type = widened.InternalType();
		return;
	}
	if (node->kind != ExecutionExpressionIRKind::CAST) {
		auto cast = make_uniq<ExecutionExpressionIR>();
		cast->kind = ExecutionExpressionIRKind::CAST;
		cast->validity = ExecutionExpressionValidityKind::CHILD;
		cast->source = ExecutionExpressionSourceKind::DERIVED;
		cast->left = std::move(node);
		node = std::move(cast);
	}
	node->return_type = widened;
	node->physical_type = widened.InternalType();
	node->exception_behavior = ExecutionExpressionExceptionKind::NONE;
}

//! Compares TINYINT/SMALLINT values as INTEGERs and DECIMALs stored in 16 or 32 bits as DECIMAL64s: widening never
//! changes a comparison, and the generated loops read a narrow column with one sign-extending load. Returns whether
//! any comparison changed
static bool SljitWidenNarrowSignedComparisons(ExecutionExpressionIR &node) {
	LogicalType left_widened;
	LogicalType right_widened;
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op) &&
	    node.left && node.right && TryGetSljitNarrowSignedValue(*node.left, left_widened) &&
	    TryGetSljitNarrowSignedValue(*node.right, right_widened) && left_widened == right_widened) {
		SljitWidenNarrowSignedValue(node.left, left_widened);
		SljitWidenNarrowSignedValue(node.right, right_widened);
		return true;
	}
	bool changed = false;
	for (auto child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			changed = SljitWidenNarrowSignedComparisons(*child) || changed;
		}
	}
	for (auto &child : node.children) {
		changed = SljitWidenNarrowSignedComparisons(*child) || changed;
	}
	return changed;
}

bool TryLowerNativeRegionExpression(const ExecutionExpressionFragment &fragment, bool require_boolean,
                                    SljitNativeRegionExpressionPlan &expr, string &error, bool render_diagnostics) {
	if (!fragment.root) {
		error = "sljit-expression-lowering-missing-root";
		return false;
	}
	auto widened = fragment.root->Copy();
	const auto &root = SljitWidenNarrowSignedComparisons(*widened) ? *widened : *fragment.root;
	if (!TryReadNativeRegionExpression(root, require_boolean, expr)) {
		error = "sljit-expression-lowering-unsupported";
		error += ";root_kind=" + string(ExecutionExpressionIRKindToString(root.kind));
		error += ";logical_type=" + root.return_type.ToString();
		error += require_boolean ? ";required=boolean" : ";required=value";
		if (render_diagnostics && !fragment.ir.empty()) {
			error += ";ir=" + fragment.ir;
		}
		return false;
	}
	expr.cannot_throw = SljitExpressionIRCannotThrow(root);
	if (render_diagnostics) {
		expr.ir = fragment.ir;
	}
	return true;
}

} // namespace duckdb

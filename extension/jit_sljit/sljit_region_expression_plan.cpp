//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/value.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

static constexpr int64_t SLJIT_DATE_MIN_DAYS = -2147483646;
static constexpr int64_t SLJIT_DATE_MAX_DAYS = 2147483646;

static bool IsSljitNativeTreeDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitNativeTreeDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

static bool SljitNativeTreeNodeSupported(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return IsSljitNativeTreeDecimal64Node(node);
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return !node.constant.IsNull() && IsSljitNativeTreeDecimal64Node(node);
	}
	int64_t result_min;
	int64_t result_max;
	if (node.kind != ExecutionExpressionIRKind::BINARY || !node.left || !node.right ||
	    !SljitExpressionTreeBinaryOpSupported(node.binary_op) || !IsSljitNativeTreeDecimal64Node(node) ||
	    !IsSljitNativeTreeDecimal64Node(*node.left) || !IsSljitNativeTreeDecimal64Node(*node.right) ||
	    !SljitNativeTreeDecimal64BinaryHasRawSemantics(node) ||
	    !TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		return false;
	}
	return SljitNativeTreeNodeSupported(*node.left) && SljitNativeTreeNodeSupported(*node.right);
}

static void RemapSljitNativeTreeReferences(ExecutionExpressionIR &node, vector<idx_t> &source_indices) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		auto source_idx = node.ref_index;
		for (idx_t map_idx = 0; map_idx < source_indices.size(); map_idx++) {
			if (source_indices[map_idx] == source_idx) {
				node.ref_index = map_idx;
				return;
			}
		}
		node.ref_index = source_indices.size();
		source_indices.push_back(source_idx);
		return;
	}
	if (node.left) {
		RemapSljitNativeTreeReferences(*node.left, source_indices);
	}
	if (node.right) {
		RemapSljitNativeTreeReferences(*node.right, source_indices);
	}
	if (node.else_node) {
		RemapSljitNativeTreeReferences(*node.else_node, source_indices);
	}
	for (auto &child : node.children) {
		RemapSljitNativeTreeReferences(*child, source_indices);
	}
}

static void AttachSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeRegionExpressionPlan &expr) {
	expr.expression_tree = root.Copy();
	expr.expression_tree_source_indices.clear();
	RemapSljitNativeTreeReferences(*expr.expression_tree, expr.expression_tree_source_indices);
}

static bool TryBuildSljitNativeExpressionTreePlan(const ExecutionExpressionIR &root,
                                                  SljitNativeRegionExpressionPlan &expr) {
	if (!SljitNativeTreeNodeSupported(root)) {
		return false;
	}
	expr.kind = SljitNativeRegionExpressionKind::EXPRESSION_TREE;
	expr.return_type = root.return_type;
	AttachSljitNativeExpressionTree(root, expr);
	return true;
}

static bool TryBuildSljitNativeTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                       SljitNativeRegionExpressionPlan &expr) {
	SljitNativeIntegerKind result_kind;
	if (!SljitTypedExpressionTreeIsSupported(root) || !TryGetSljitTypedExpressionTreeResultKind(root, result_kind)) {
		return false;
	}
	expr.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
	expr.integer_kind = result_kind;
	expr.return_type = root.return_type;
	AttachSljitNativeExpressionTree(root, expr);
	return true;
}

static bool TryBuildSljitNativeAnyExpressionTreePlan(const ExecutionExpressionIR &root,
                                                     SljitNativeRegionExpressionPlan &expr) {
	if (TryBuildSljitNativeExpressionTreePlan(root, expr)) {
		return true;
	}
	return TryBuildSljitNativeTypedExpressionTreePlan(root, expr);
}

static bool IsComposableNativeAddConstant(const SljitNativeRegionExpressionPlan &expr) {
	return expr.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT &&
	       expr.binary_op == SljitNativeIntegerBinaryOp::ADD && !expr.constant_on_left;
}

static bool SljitSignedIntegerWidthMatchesType(SljitNativeSignedIntegerWidth width, const LogicalType &type) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return type.InternalType() == PhysicalType::INT8;
	case SljitNativeSignedIntegerWidth::INT16:
		return type.InternalType() == PhysicalType::INT16;
	case SljitNativeSignedIntegerWidth::INT32:
		return type.InternalType() == PhysicalType::INT32;
	case SljitNativeSignedIntegerWidth::INT64:
		return type.InternalType() == PhysicalType::INT64;
	default:
		return false;
	}
}

static bool SljitUnsignedIntegerWidthMatchesType(SljitNativeUnsignedIntegerWidth width, const LogicalType &type) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return type.InternalType() == PhysicalType::UINT8;
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return type.InternalType() == PhysicalType::UINT16;
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return type.InternalType() == PhysicalType::UINT32;
	default:
		return false;
	}
}

static SljitNativeRegionExpressionPlan
SljitProjectionReferenceThroughSource(const SljitNativeRegionExpressionPlan &expr,
                                      const SljitNativeRegionExpressionPlan &source, bool render_diagnostics,
                                      const char *reason) {
	SljitNativeRegionExpressionPlan result;
	result.kind = SljitNativeRegionExpressionKind::REFERENCE;
	result.return_type = expr.return_type;
	result.source_index = source.source_index;
	result.reference_origin = source.reference_origin;
	if (render_diagnostics) {
		result.ir = string(reason) + "(" + source.ir + "," + expr.ir + ")";
	}
	return result;
}

static bool TryComposeNativeRoundTripProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                const SljitNativeRegionExpressionPlan &expr,
                                                SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	if (expr.source_index >= input_projection.size()) {
		return false;
	}
	auto &source = input_projection[expr.source_index];
	if (expr.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS &&
	    source.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
	    expr.string_compress_target_size == source.string_decompress_source_size &&
	    expr.return_type.InternalType() != PhysicalType::INVALID &&
	    GetTypeIdSize(expr.return_type.InternalType()) == expr.string_compress_target_size) {
		result = SljitProjectionReferenceThroughSource(expr, source, render_diagnostics,
		                                               "compose-string-compress-decompress");
		return true;
	}
	if (expr.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS &&
	    source.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS &&
	    expr.cast_source_width == source.cast_target_width &&
	    expr.unsigned_cast_target_width == source.unsigned_source_width && expr.constant == source.constant &&
	    SljitUnsignedIntegerWidthMatchesType(expr.unsigned_cast_target_width, expr.return_type)) {
		result = SljitProjectionReferenceThroughSource(expr, source, render_diagnostics,
		                                               "compose-integral-compress-decompress");
		return true;
	}
	if (expr.kind == SljitNativeRegionExpressionKind::INTEGER_CAST &&
	    source.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && !expr.try_cast && !source.try_cast &&
	    expr.cast_source_width == source.cast_target_width && expr.cast_target_width == source.cast_source_width &&
	    SljitSignedIntegerWidthMatchesType(expr.cast_target_width, expr.return_type)) {
		result =
		    SljitProjectionReferenceThroughSource(expr, source, render_diagnostics, "compose-integer-cast-roundtrip");
		return true;
	}
	return false;
}

static bool TryMapNativeProjectionSourceIndex(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                              idx_t &source_index) {
	if (source_index >= input_projection.size()) {
		return false;
	}
	auto &source = input_projection[source_index];
	if (source.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		return false;
	}
	source_index = source.source_index;
	return true;
}

static bool TryMapNativeExpressionTreeSourceIndicesThroughProjection(
    const vector<SljitNativeRegionExpressionPlan> &input_projection, SljitNativeRegionExpressionPlan &expr) {
	if (!expr.expression_tree) {
		return true;
	}
	for (auto &source_index : expr.expression_tree_source_indices) {
		if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
			return false;
		}
	}
	return true;
}

static bool
TryMapNativePredicateSourcesThroughProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                              SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return true;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		return TryMapNativeProjectionSourceIndex(input_projection, predicate.source_index);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		return TryMapNativeProjectionSourceIndex(input_projection, predicate.source_index) &&
		       TryMapNativeProjectionSourceIndex(input_projection, predicate.right_source_index);
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
				return false;
			}
		}
		return predicate.child && TryMapNativePredicateSourcesThroughProjection(input_projection, *predicate.child);
	case SljitNativePredicateKind::NOT:
		return predicate.child && TryMapNativePredicateSourcesThroughProjection(input_projection, *predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (!child || !TryMapNativePredicateSourcesThroughProjection(input_projection, *child)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeReference(idx_t source_index, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = source_index;
	return result;
}

static unique_ptr<ExecutionExpressionIR> MakeSljitTreeConstant(const Value &constant, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::CONSTANT;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = constant.IsNull() ? ExecutionExpressionValidityKind::CONSTANT_NULL
	                                     : ExecutionExpressionValidityKind::CONSTANT_VALID;
	result->source = ExecutionExpressionSourceKind::CONSTANT;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->constant = constant;
	return result;
}

static void ExpandSljitExpressionTreeSources(ExecutionExpressionIR &node, const vector<idx_t> &source_indices) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		D_ASSERT(node.ref_index < source_indices.size());
		node.ref_index = source_indices[node.ref_index];
		return;
	}
	if (node.left) {
		ExpandSljitExpressionTreeSources(*node.left, source_indices);
	}
	if (node.right) {
		ExpandSljitExpressionTreeSources(*node.right, source_indices);
	}
	if (node.else_node) {
		ExpandSljitExpressionTreeSources(*node.else_node, source_indices);
	}
	for (auto &child : node.children) {
		ExpandSljitExpressionTreeSources(*child, source_indices);
	}
}

static unique_ptr<ExecutionExpressionIR>
CopySljitExpressionPlanAsInputTree(const SljitNativeRegionExpressionPlan &expr) {
	if (expr.expression_tree) {
		auto result = expr.expression_tree->Copy();
		ExpandSljitExpressionTreeSources(*result, expr.expression_tree_source_indices);
		return result;
	}
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return MakeSljitTreeReference(expr.source_index, expr.return_type);
	case SljitNativeRegionExpressionKind::CONSTANT:
		return MakeSljitTreeConstant(expr.constant_value, expr.return_type);
	default:
		return nullptr;
	}
}

static bool
RewriteSljitExpressionTreeReferencesThroughProjection(unique_ptr<ExecutionExpressionIR> &node,
                                                      const vector<SljitNativeRegionExpressionPlan> &input_projection) {
	if (!node) {
		return false;
	}
	if (node->kind == ExecutionExpressionIRKind::REFERENCE) {
		if (node->ref_index >= input_projection.size()) {
			return false;
		}
		node = CopySljitExpressionPlanAsInputTree(input_projection[node->ref_index]);
		return node != nullptr;
	}
	if (node->left && !RewriteSljitExpressionTreeReferencesThroughProjection(node->left, input_projection)) {
		return false;
	}
	if (node->right && !RewriteSljitExpressionTreeReferencesThroughProjection(node->right, input_projection)) {
		return false;
	}
	if (node->else_node && !RewriteSljitExpressionTreeReferencesThroughProjection(node->else_node, input_projection)) {
		return false;
	}
	for (auto &child : node->children) {
		if (!RewriteSljitExpressionTreeReferencesThroughProjection(child, input_projection)) {
			return false;
		}
	}
	return true;
}

static bool
TryComposeNativeExpressionTreeThroughProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                const SljitNativeRegionExpressionPlan &expr,
                                                SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	auto tree = CopySljitExpressionPlanAsInputTree(expr);
	if (!tree || !RewriteSljitExpressionTreeReferencesThroughProjection(tree, input_projection)) {
		return false;
	}
	if (!TryBuildSljitNativeAnyExpressionTreePlan(*tree, result)) {
		return false;
	}
	if (render_diagnostics) {
		result.ir = "compose-expression-tree(" + expr.ir + ")";
	}
	return true;
}

bool TryMapNativeProjectionExpressionSources(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                             SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (expr.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
			       TryMapNativeProjectionSourceIndex(input_projection, expr.guard_source_index) &&
			       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
			for (auto &source_index : expr.constant_or_null.guard_source_indices) {
				if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
					return false;
				}
			}
			return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
		       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
		       TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index) &&
		       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		if (!TryMapNativeProjectionSourceIndex(input_projection, expr.source_index)) {
			return false;
		}
		if (expr.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			return TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index) &&
			       TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
		}
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (!expr.predicate || !TryMapNativePredicateSourcesThroughProjection(input_projection, *expr.predicate)) {
			return false;
		}
		FinalizeSljitNativePredicateSourceIndices(*expr.predicate);
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return TryMapNativeExpressionTreeSourceIndicesThroughProjection(input_projection, expr);
	default:
		return false;
	}
}

static bool TryComposeNativeProjectionExpression(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                 const SljitNativeRegionExpressionPlan &expr,
                                                 SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	if (expr.expression_tree &&
	    TryComposeNativeExpressionTreeThroughProjection(input_projection, expr, result, render_diagnostics)) {
		return true;
	}
	if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT) {
		result = expr.Copy();
		if (render_diagnostics) {
			result.ir = "compose-constant(" + expr.ir + ")";
		}
		return true;
	}
	if (expr.source_index >= input_projection.size()) {
		return false;
	}
	auto &source = input_projection[expr.source_index];
	if (expr.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (source.return_type != expr.return_type) {
			return false;
		}
		result = source.Copy();
		if (render_diagnostics) {
			result.ir = "compose-reference(" + source.ir + ")";
		}
		return true;
	}
	if (TryComposeNativeRoundTripProjection(input_projection, expr, result, render_diagnostics)) {
		return true;
	}
	if (!IsComposableNativeAddConstant(expr)) {
		return false;
	}
	if (source.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (source.return_type != expr.return_type) {
			return false;
		}
		result = expr.Copy();
		result.source_index = source.source_index;
		if (render_diagnostics) {
			result.ir = "compose-add-reference(" + source.ir + "," + expr.ir + ")";
		}
		return true;
	}
	if (!IsComposableNativeAddConstant(source) || source.integer_kind != expr.integer_kind ||
	    source.return_type != expr.return_type) {
		return false;
	}
	int64_t constant;
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(source.constant, expr.constant, constant)) {
		return false;
	}
	result = source.Copy();
	result.constant = constant;
	if (render_diagnostics) {
		result.ir = "compose-add-constant(" + source.ir + "," + expr.ir + ")";
	}
	return true;
}

static bool
TryComposeNativeProjectionThroughReferenceProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                     const SljitNativeRegionExpressionPlan &expr,
                                                     SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	result = expr.Copy();
	if (!TryMapNativeProjectionExpressionSources(input_projection, result)) {
		return false;
	}
	if (render_diagnostics) {
		result.ir = "compose-reference-projection(" + expr.ir + ")";
	}
	return true;
}

static bool TryComposeNativeProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                       const SljitNativeRegionExpressionPlan &expr,
                                       SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	return TryComposeNativeProjectionExpression(input_projection, expr, result, render_diagnostics) ||
	       TryComposeNativeProjectionThroughReferenceProjection(input_projection, expr, result, render_diagnostics);
}

static bool TryFuseAdjacentNativeProjection(SljitNativeRegionOpPlan &left, const SljitNativeRegionOpPlan &right,
                                            bool render_diagnostics) {
	if (left.kind != SljitNativeRegionOpKind::PROJECTION || right.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	vector<SljitNativeRegionExpressionPlan> projections;
	projections.reserve(right.projections.size());
	for (auto &projection : right.projections) {
		SljitNativeRegionExpressionPlan composed;
		if (!TryComposeNativeProjection(left.projections, projection, composed, render_diagnostics)) {
			return false;
		}
		projections.push_back(std::move(composed));
	}
	left.projections = std::move(projections);
	left.output_types = right.output_types;
	return true;
}

void FuseAdjacentNativeProjections(SljitNativeRegionPlan &region, bool render_diagnostics) {
	if (region.ops.size() < 2) {
		return;
	}
	idx_t op_idx = 0;
	while (op_idx + 1 < region.ops.size()) {
		if (TryFuseAdjacentNativeProjection(region.ops[op_idx], region.ops[op_idx + 1], render_diagnostics)) {
			region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx + 1));
			continue;
		}
		op_idx++;
	}
}

static bool TryGetSljitPrimitiveAggregatePayloadKind(const LogicalType &payload_type,
                                                     SljitNativeIntegerKind &integer_kind) {
	switch (payload_type.InternalType()) {
	case PhysicalType::INT32:
		integer_kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		integer_kind = payload_type.id() == LogicalTypeId::DECIMAL ? SljitNativeIntegerKind::DECIMAL64
		                                                           : SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool SljitPrimitiveAggregatePayloadSupported(SljitNativeRegionExpressionPlan &payload,
                                                    const ExecutionRegionAggregateInput &aggregate,
                                                    bool grouped_state = false) {
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty() && aggregate.primitive_update_ready;
	}
	if (aggregate.child_types.size() != 1 ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	if (!aggregate.primitive_update_ready ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType()) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
		if (grouped_state || aggregate.child_types[0].InternalType() != PhysicalType::DOUBLE) {
			return false;
		}
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			payload.double_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
			return true;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			return true;
		default:
			return false;
		}
	}
	SljitNativeIntegerKind aggregate_payload_kind;
	if (!TryGetSljitPrimitiveAggregatePayloadKind(aggregate.child_types[0], aggregate_payload_kind)) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		if (grouped_state && payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload.integer_kind = aggregate_payload_kind;
			return true;
		}
		if (!payload.expression_tree) {
			return false;
		}
		SljitNativeIntegerKind typed_tree_kind;
		if (SljitTypedExpressionTreeIsSupported(*payload.expression_tree) &&
		    TryGetSljitTypedExpressionTreeResultKind(*payload.expression_tree, typed_tree_kind) &&
		    typed_tree_kind == aggregate_payload_kind) {
			payload.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
			payload.integer_kind = typed_tree_kind;
			return true;
		}
		if (aggregate_payload_kind == SljitNativeIntegerKind::DECIMAL64) {
			payload.kind = SljitNativeRegionExpressionKind::EXPRESSION_TREE;
			payload.integer_kind = aggregate_payload_kind;
			return true;
		}
		return false;
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
		return false;
	}
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		payload.integer_kind = aggregate_payload_kind;
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return payload.integer_kind == aggregate_payload_kind;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!payload.expression_tree || !SljitTypedExpressionTreeIsSupported(*payload.expression_tree)) {
			return false;
		}
		SljitNativeIntegerKind typed_tree_kind;
		return TryGetSljitTypedExpressionTreeResultKind(*payload.expression_tree, typed_tree_kind) &&
		       typed_tree_kind == aggregate_payload_kind;
	}
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		return aggregate_payload_kind == SljitNativeIntegerKind::DECIMAL64 && payload.expression_tree != nullptr;
	default:
		return false;
	}
}

static bool TryBuildSljitPrimitiveReferencePayload(const vector<LogicalType> &input_types,
                                                   const ExecutionRegionAggregateInput &aggregate,
                                                   SljitNativeRegionExpressionPlan &payload, bool grouped_state,
                                                   bool render_diagnostics);
static bool SljitPerfectHashGroupLookupSupported(
    const ExecutionRegionSinkInfo &sink, const vector<SljitNativeRegionExpressionPlan> &payloads,
    optional_ptr<const vector<SljitNativeRegionExpressionPlan>> group_expressions = nullptr);

static bool SljitAggregateUpdateUsesGeneratedPerfectHashLookup(const ExecutionRegionSinkInfo &sink) {
	return sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	       sink.aggregate_contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::READY;
}

static bool SljitPrimitiveAggregatePayloadsContainNonReference(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                               const ExecutionRegionSinkInfo &sink) {
	if (payloads.size() != sink.aggregates.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (sink.aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (payloads[payload_idx].kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return true;
		}
	}
	return false;
}

static bool
SljitAggregateUpdateAlreadyHasFusedProjectionPayloads(const SljitNativeAggregateUpdatePlan &aggregate_update) {
	return aggregate_update.use_primitive_payloads &&
	       SljitPrimitiveAggregatePayloadsContainNonReference(aggregate_update.payloads, aggregate_update.sink_info);
}

static bool TryGetSljitExpressionTreePassthroughReference(const ExecutionExpressionIR &node, idx_t &ref_index) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		ref_index = node.ref_index;
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CAST && !node.try_cast && node.left &&
	    node.return_type == node.left->return_type) {
		return TryGetSljitExpressionTreePassthroughReference(*node.left, ref_index);
	}
	return false;
}

static bool TrySimplifySljitExpressionTreeReferencePayload(SljitNativeRegionExpressionPlan &payload) {
	if (!payload.expression_tree) {
		return false;
	}
	idx_t ref_index;
	if (!TryGetSljitExpressionTreePassthroughReference(*payload.expression_tree, ref_index)) {
		return false;
	}
	auto source_index = ref_index < payload.expression_tree_source_indices.size()
	                        ? payload.expression_tree_source_indices[ref_index]
	                        : ref_index;
	payload.kind = SljitNativeRegionExpressionKind::REFERENCE;
	payload.source_index = source_index;
	payload.return_type = payload.expression_tree->return_type;
	payload.expression_tree.reset();
	payload.expression_tree_source_indices.clear();
	return true;
}

static bool TryNormalizePerfectHashAggregatePayloads(vector<SljitNativeRegionExpressionPlan> &payloads,
                                                     const ExecutionRegionSinkInfo &sink, bool render_diagnostics) {
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink) ||
	    !SljitPrimitiveAggregatePayloadsContainNonReference(payloads, sink)) {
		return true;
	}

	vector<SljitNativeRegionExpressionPlan> normalized_payloads;
	normalized_payloads.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}

		if (TrySimplifySljitExpressionTreeReferencePayload(payloads[payload_idx])) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}
		if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}

		auto tree = CopySljitExpressionPlanAsInputTree(payloads[payload_idx]);
		if (!tree) {
			return false;
		}
		SljitNativeRegionExpressionPlan typed_payload;
		if (!TryBuildSljitNativeTypedExpressionTreePlan(*tree, typed_payload)) {
			return false;
		}
		const bool simplified_reference_payload = TrySimplifySljitExpressionTreeReferencePayload(typed_payload);
		if (!SljitPrimitiveAggregatePayloadSupported(typed_payload, aggregate, true)) {
			return false;
		}
		if (render_diagnostics) {
			typed_payload.ir =
			    (simplified_reference_payload ? "perfect-hash-reference-payload(" : "perfect-hash-typed-payload(") +
			    payloads[payload_idx].ir + ")";
		}
		normalized_payloads.push_back(std::move(typed_payload));
	}
	payloads = std::move(normalized_payloads);
	return true;
}

static bool TryNormalizeGroupedTypedAggregatePayloads(vector<SljitNativeRegionExpressionPlan> &payloads,
                                                      const ExecutionRegionSinkInfo &sink, bool render_diagnostics) {
	if (!SljitPrimitiveAggregatePayloadsContainNonReference(payloads, sink)) {
		return true;
	}

	vector<SljitNativeRegionExpressionPlan> normalized_payloads;
	normalized_payloads.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}
		if (TrySimplifySljitExpressionTreeReferencePayload(payloads[payload_idx])) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}
		if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}

		auto tree = CopySljitExpressionPlanAsInputTree(payloads[payload_idx]);
		if (!tree) {
			return false;
		}
		SljitNativeRegionExpressionPlan typed_payload;
		if (!TryBuildSljitNativeTypedExpressionTreePlan(*tree, typed_payload)) {
			return false;
		}
		const bool simplified_reference_payload = TrySimplifySljitExpressionTreeReferencePayload(typed_payload);
		if (!SljitPrimitiveAggregatePayloadSupported(typed_payload, aggregate, true)) {
			return false;
		}
		if (render_diagnostics) {
			typed_payload.ir =
			    (simplified_reference_payload ? "grouped-reference-payload(" : "grouped-typed-payload(") +
			    payloads[payload_idx].ir + ")";
		}
		normalized_payloads.push_back(std::move(typed_payload));
	}
	payloads = std::move(normalized_payloads);
	return true;
}

static SljitNativeRegionExpressionPlan SljitProjectionSourceReference(idx_t source_index, const LogicalType &type,
                                                                      bool render_diagnostics) {
	SljitNativeRegionExpressionPlan reference;
	reference.kind = SljitNativeRegionExpressionKind::REFERENCE;
	reference.source_index = source_index;
	reference.return_type = type;
	reference.reference_origin = SljitNativeReferenceOrigin::REGION_INPUT;
	if (render_diagnostics) {
		reference.ir = "primitive-payload-source-reference";
	}
	return reference;
}

static idx_t AddSljitProjectionSourceReference(const vector<LogicalType> &input_types,
                                               vector<SljitNativeRegionExpressionPlan> &projections,
                                               vector<LogicalType> &projection_types, vector<idx_t> &source_map,
                                               idx_t source_index, bool render_diagnostics) {
	if (source_index >= input_types.size()) {
		return DConstants::INVALID_INDEX;
	}
	if (source_map[source_index] != DConstants::INVALID_INDEX) {
		return source_map[source_index];
	}
	const auto projection_index = projections.size();
	projections.push_back(SljitProjectionSourceReference(source_index, input_types[source_index], render_diagnostics));
	projection_types.push_back(input_types[source_index]);
	source_map[source_index] = projection_index;
	return projection_index;
}

static bool RewriteSljitDirectPayloadSourceThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, idx_t &source_index, bool render_diagnostics) {
	auto projection_index = AddSljitProjectionSourceReference(input_types, projections, projection_types, source_map,
	                                                          source_index, render_diagnostics);
	if (projection_index == DConstants::INVALID_INDEX) {
		return false;
	}
	source_index = projection_index;
	return true;
}

static bool RewriteSljitDirectExpressionTreeSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, SljitNativeRegionExpressionPlan &expr,
    bool render_diagnostics) {
	for (auto &source_index : expr.expression_tree_source_indices) {
		if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                             source_map, source_index, render_diagnostics)) {
			return false;
		}
	}
	return true;
}

static bool RewriteSljitDirectPredicateSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &projections,
    vector<LogicalType> &projection_types, vector<idx_t> &source_map, SljitNativePredicate &predicate,
    bool render_diagnostics) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return true;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		return RewriteSljitDirectPayloadSourceThroughPartialProjection(
		    input_types, projections, projection_types, source_map, predicate.source_index, render_diagnostics);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		return RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                               source_map, predicate.source_index,
		                                                               render_diagnostics) &&
		       RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
		                                                               source_map, predicate.right_source_index,
		                                                               render_diagnostics);
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, source_index, render_diagnostics)) {
				return false;
			}
		}
		return predicate.child &&
		       RewriteSljitDirectPredicateSourcesThroughPartialProjection(
		           input_types, projections, projection_types, source_map, *predicate.child, render_diagnostics);
	case SljitNativePredicateKind::NOT:
		return predicate.child &&
		       RewriteSljitDirectPredicateSourcesThroughPartialProjection(
		           input_types, projections, projection_types, source_map, *predicate.child, render_diagnostics);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (!child || !RewriteSljitDirectPredicateSourcesThroughPartialProjection(
			                  input_types, projections, projection_types, source_map, *child, render_diagnostics)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool RewriteSljitDirectPayloadSourcesThroughPartialProjection(
    const vector<LogicalType> &input_types, vector<SljitNativeRegionExpressionPlan> &payloads,
    vector<SljitNativeRegionExpressionPlan> &projections, vector<LogicalType> &projection_types,
    bool render_diagnostics) {
	vector<idx_t> source_map(input_types.size(), DConstants::INVALID_INDEX);
	for (auto &payload : payloads) {
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			break;
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics) ||
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.guard_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			for (auto &source_index : payload.constant_or_null.guard_source_indices) {
				if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
				        input_types, projections, projection_types, source_map, source_index, render_diagnostics)) {
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics) ||
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.right_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!RewriteSljitDirectPayloadSourceThroughPartialProjection(
			        input_types, projections, projection_types, source_map, payload.source_index, render_diagnostics)) {
				return false;
			}
			if (payload.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !RewriteSljitDirectPayloadSourceThroughPartialProjection(input_types, projections, projection_types,
			                                                             source_map, payload.right_source_index,
			                                                             render_diagnostics)) {
				return false;
			}
			break;
		case SljitNativeRegionExpressionKind::PREDICATE:
			if (!payload.predicate ||
			    !RewriteSljitDirectPredicateSourcesThroughPartialProjection(
			        input_types, projections, projection_types, source_map, *payload.predicate, render_diagnostics)) {
				return false;
			}
			FinalizeSljitNativePredicateSourceIndices(*payload.predicate);
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			break;
		default:
			return false;
		}
		if (!RewriteSljitDirectExpressionTreeSourcesThroughPartialProjection(input_types, projections, projection_types,
		                                                                     source_map, payload, render_diagnostics)) {
			return false;
		}
	}
	return true;
}

static bool SljitPrimitiveAggregatePayloadCanEraseProjection(const SljitNativeRegionExpressionPlan &payload) {
	if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		return true;
	}
	return payload.reference_origin == SljitNativeReferenceOrigin::REGION_INPUT ||
	       payload.reference_origin == SljitNativeReferenceOrigin::SOURCE_OUTPUT;
}

static bool SljitPerfectHashGroupExpressionCanEraseProjection(const vector<LogicalType> &input_types,
                                                              const SljitNativeRegionExpressionPlan &expr,
                                                              const ExecutionRegionGroupInput &group) {
	if (expr.return_type.InternalType() != group.type.InternalType() || expr.source_index >= input_types.size()) {
		return false;
	}
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return expr.reference_origin == SljitNativeReferenceOrigin::REGION_INPUT ||
		       expr.reference_origin == SljitNativeReferenceOrigin::SOURCE_OUTPUT;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		return input_types[expr.source_index].id() == LogicalTypeId::VARCHAR &&
		       group.type.InternalType() == PhysicalType::UINT8 && expr.string_compress_target_size == sizeof(uint8_t);
	default:
		return false;
	}
}

static bool TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(const vector<LogicalType> &input_types,
                                                                SljitNativeRegionOpPlan &projection,
                                                                SljitNativeRegionOpPlan &aggregate_update,
                                                                bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || sink.aggregates.empty()) {
		return false;
	}
	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, false, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (!SljitPrimitiveAggregatePayloadCanEraseProjection(payload)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}

	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "payload_update=generated-primitive";
	}
	aggregate_update.output_types = projection.output_types;
	return true;
}

static bool TryFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                  SljitNativeRegionOpPlan &projection,
                                                                  SljitNativeRegionOpPlan &aggregate_update,
                                                                  bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	if (aggregate_update.aggregate_update.use_primitive_payloads) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink) || sink.aggregates.empty() || sink.groups.empty()) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> group_expressions;
	group_expressions.reserve(sink.groups.size());
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.projections.size()) {
			return false;
		}
		auto group_expression = projection.projections[group.input_index].Copy();
		if (!SljitPerfectHashGroupExpressionCanEraseProjection(input_types, group_expression, group)) {
			return false;
		}
		if (render_diagnostics && !group_expression.ir.empty()) {
			group_expression.ir = "perfect-hash-group(" + group_expression.ir + ")";
		}
		group_expressions.push_back(std::move(group_expression));
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, true, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true)) {
				return false;
			}
		} else if (payload.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
		           !aggregate.primitive_update_ready) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics) ||
	    !SljitPerfectHashGroupLookupSupported(sink, payloads, group_expressions)) {
		return false;
	}

	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.group_expressions = std::move(group_expressions);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	aggregate_update.aggregate_update.use_grouped_state_addresses = true;
	aggregate_update.aggregate_update.use_perfect_hash_group_lookup = true;
	aggregate_update.output_types = input_types;
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "primitive_payload_projection_composed=true";
		aggregate_update.aggregate_update.ir += ";perfect_hash_group_projection_composed=true";
		aggregate_update.aggregate_update.ir += ";grouped_state_lookup=generated-perfect-hash";
	}
	return true;
}

static bool TryPartiallyFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                           SljitNativeRegionOpPlan &projection,
                                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                                           bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (SljitAggregateUpdateAlreadyHasFusedProjectionPayloads(aggregate_update.aggregate_update)) {
		return false;
	}
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink) || sink.aggregates.empty() || sink.groups.empty()) {
		return false;
	}

	idx_t group_projection_count = 0;
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.projections.size()) {
			return false;
		}
		group_projection_count = MaxValue<idx_t>(group_projection_count, group.input_index + 1);
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, true, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	vector<SljitNativeRegionExpressionPlan> rewritten_projections;
	vector<LogicalType> rewritten_types;
	rewritten_projections.reserve(group_projection_count + input_types.size());
	rewritten_types.reserve(group_projection_count + input_types.size());
	for (idx_t projection_idx = 0; projection_idx < group_projection_count; projection_idx++) {
		rewritten_projections.push_back(projection.projections[projection_idx].Copy());
		rewritten_types.push_back(projection.output_types[projection_idx]);
	}
	if (!RewriteSljitDirectPayloadSourcesThroughPartialProjection(input_types, payloads, rewritten_projections,
	                                                              rewritten_types, render_diagnostics)) {
		return false;
	}
	if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics) ||
	    !SljitPerfectHashGroupLookupSupported(sink, payloads)) {
		return false;
	}
	const bool primitive_payload_transition = !aggregate_update.aggregate_update.use_primitive_payloads;
	const bool fused_payload_projection = SljitPrimitiveAggregatePayloadsContainNonReference(payloads, sink);
	const bool shrank_projection = rewritten_projections.size() < projection.projections.size();
	if (!primitive_payload_transition && !fused_payload_projection && !shrank_projection) {
		return false;
	}

	projection.projections = std::move(rewritten_projections);
	projection.output_types = std::move(rewritten_types);
	aggregate_update.aggregate_update.input_types = projection.output_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	aggregate_update.aggregate_update.use_grouped_state_addresses = true;
	aggregate_update.aggregate_update.use_perfect_hash_group_lookup = true;
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "primitive_payload_projection_partially_composed=true";
		aggregate_update.aggregate_update.ir += ";grouped_state_lookup=generated-perfect-hash";
	}
	return true;
}

static bool TryPartiallyFuseNativeProjectionIntoRegularHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                           SljitNativeRegionOpPlan &projection,
                                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                                           bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (SljitAggregateUpdateAlreadyHasFusedProjectionPayloads(aggregate_update.aggregate_update)) {
		return false;
	}
	if (sink.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink.aggregates.empty() || sink.groups.empty() ||
	    sink.aggregate_contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY ||
	    !sink.aggregate_contract.grouped_state_layout_ready) {
		return false;
	}

	idx_t group_projection_count = 0;
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.projections.size()) {
			return false;
		}
		group_projection_count = MaxValue<idx_t>(group_projection_count, group.input_index + 1);
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, true, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true)) {
				return false;
			}
		} else if (aggregate.child_types.size() != 1 ||
		           payload.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
		           !aggregate.primitive_update_ready) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}

	vector<SljitNativeRegionExpressionPlan> rewritten_projections;
	vector<LogicalType> rewritten_types;
	rewritten_projections.reserve(group_projection_count + input_types.size());
	rewritten_types.reserve(group_projection_count + input_types.size());
	for (idx_t projection_idx = 0; projection_idx < group_projection_count; projection_idx++) {
		rewritten_projections.push_back(projection.projections[projection_idx].Copy());
		rewritten_types.push_back(projection.output_types[projection_idx]);
	}
	if (!RewriteSljitDirectPayloadSourcesThroughPartialProjection(input_types, payloads, rewritten_projections,
	                                                              rewritten_types, render_diagnostics)) {
		return false;
	}
	if (!TryNormalizeGroupedTypedAggregatePayloads(payloads, sink, render_diagnostics)) {
		return false;
	}

	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			has_typed_payload = true;
			continue;
		}
		if (payloads[payload_idx].kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return false;
		}
	}
	if (!has_typed_payload) {
		return false;
	}

	projection.projections = std::move(rewritten_projections);
	projection.output_types = std::move(rewritten_types);
	aggregate_update.aggregate_update.input_types = projection.output_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	aggregate_update.aggregate_update.use_grouped_state_addresses = true;
	aggregate_update.aggregate_update.use_perfect_hash_group_lookup = false;
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "primitive_payload_projection_partially_composed=true";
		aggregate_update.aggregate_update.ir += ";grouped_state_lookup=native-state-address";
	}
	return true;
}

static bool TryComposePrimitiveAggregatePayloadsThroughProjection(const vector<LogicalType> &input_types,
                                                                  const SljitNativeRegionOpPlan &projection,
                                                                  SljitNativeRegionOpPlan &aggregate_update,
                                                                  bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !aggregate_update.aggregate_update.use_primitive_payloads) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (sink.aggregates.empty() || sink.aggregates.size() != aggregate_update.aggregate_update.payloads.size()) {
		return false;
	}

	if (sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	    aggregate_update.aggregate_update.use_perfect_hash_group_lookup) {
		auto mark_blocker = [&](const string &blocker) {
			if (!render_diagnostics) {
				return;
			}
			if (!aggregate_update.aggregate_update.ir.empty()) {
				aggregate_update.aggregate_update.ir += ";";
			}
			aggregate_update.aggregate_update.ir += "perfect_hash_payload_projection_compose_blocker=" + blocker;
		};
		if (sink.groups.empty() || aggregate_update.aggregate_update.group_expressions.size() != sink.groups.size()) {
			mark_blocker("group-expression-count");
			return false;
		}

		vector<SljitNativeRegionExpressionPlan> group_expressions;
		group_expressions.reserve(aggregate_update.aggregate_update.group_expressions.size());
		for (idx_t group_idx = 0; group_idx < aggregate_update.aggregate_update.group_expressions.size(); group_idx++) {
			auto &group = sink.groups[group_idx];
			auto &group_expression = aggregate_update.aggregate_update.group_expressions[group_idx];
			SljitNativeRegionExpressionPlan composed;
			if (!TryComposeNativeProjection(projection.projections, group_expression, composed, render_diagnostics)) {
				mark_blocker("group-expression-compose");
				return false;
			}
			if (!SljitPerfectHashGroupExpressionCanEraseProjection(input_types, composed, group)) {
				mark_blocker("group-expression-erase");
				return false;
			}
			group_expressions.push_back(std::move(composed));
		}

		vector<SljitNativeRegionExpressionPlan> payloads;
		payloads.reserve(aggregate_update.aggregate_update.payloads.size());
		for (idx_t payload_idx = 0; payload_idx < aggregate_update.aggregate_update.payloads.size(); payload_idx++) {
			auto &aggregate = sink.aggregates[payload_idx];
			auto &payload = aggregate_update.aggregate_update.payloads[payload_idx];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				payloads.push_back(payload.Copy());
				continue;
			}
			SljitNativeRegionExpressionPlan composed;
			if (!TryComposeNativeProjection(projection.projections, payload, composed, render_diagnostics)) {
				mark_blocker("payload-compose");
				return false;
			}
			if (!SljitPrimitiveAggregatePayloadSupported(composed, aggregate, true)) {
				mark_blocker("payload-supported");
				return false;
			}
			payloads.push_back(std::move(composed));
		}
		if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics)) {
			mark_blocker("payload-normalize");
			return false;
		}
		if (!SljitPerfectHashGroupLookupSupported(sink, payloads, group_expressions)) {
			mark_blocker("lookup-supported");
			return false;
		}

		aggregate_update.output_types = input_types;
		aggregate_update.aggregate_update.input_types = input_types;
		aggregate_update.aggregate_update.payloads = std::move(payloads);
		aggregate_update.aggregate_update.group_expressions = std::move(group_expressions);
		if (render_diagnostics) {
			if (!aggregate_update.aggregate_update.ir.empty()) {
				aggregate_update.aggregate_update.ir += ";";
			}
			aggregate_update.aggregate_update.ir += "perfect_hash_payload_projection_composed=true";
		}
		return true;
	}

	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(aggregate_update.aggregate_update.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.aggregate_update.payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		auto &payload = aggregate_update.aggregate_update.payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			payloads.push_back(payload.Copy());
			continue;
		}
		SljitNativeRegionExpressionPlan composed;
		if (!TryComposeNativeProjection(projection.projections, payload, composed, render_diagnostics)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadCanEraseProjection(composed)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadSupported(composed, aggregate)) {
			return false;
		}
		payloads.push_back(std::move(composed));
	}

	aggregate_update.output_types = input_types;
	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "primitive_payload_projection_composed=true";
	}
	return true;
}

static bool TryBuildSljitPrimitiveReferencePayload(const vector<LogicalType> &input_types,
                                                   const ExecutionRegionAggregateInput &aggregate,
                                                   SljitNativeRegionExpressionPlan &payload, bool grouped_state,
                                                   bool render_diagnostics) {
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		if (aggregate.child_count != 0) {
			return false;
		}
		payload.kind = SljitNativeRegionExpressionKind::CONSTANT;
		payload.return_type = LogicalType::BIGINT;
		if (render_diagnostics) {
			payload.ir = "primitive-count-star";
		}
		return SljitPrimitiveAggregatePayloadSupported(payload, aggregate, grouped_state);
	}
	if (aggregate.child_count != 1 || aggregate.payload_index >= input_types.size()) {
		return false;
	}
	payload.kind = SljitNativeRegionExpressionKind::REFERENCE;
	payload.source_index = aggregate.payload_index;
	payload.return_type = input_types[aggregate.payload_index];
	if (render_diagnostics) {
		payload.ir = "primitive-reference";
	}
	return SljitPrimitiveAggregatePayloadSupported(payload, aggregate, grouped_state);
}

static bool
SljitPerfectHashGroupLookupSupported(const ExecutionRegionSinkInfo &sink,
                                     const vector<SljitNativeRegionExpressionPlan> &payloads,
                                     optional_ptr<const vector<SljitNativeRegionExpressionPlan>> group_expressions) {
	auto &contract = sink.aggregate_contract;
	if (sink.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	    contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH || !contract.grouped_state_layout_ready ||
	    payloads.empty() || contract.perfect_required_bits.size() != sink.groups.size() ||
	    contract.perfect_group_minima.size() != sink.groups.size() ||
	    (group_expressions && group_expressions->size() != sink.groups.size())) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < sink.groups.size(); group_idx++) {
		auto &group = sink.groups[group_idx];
		if (!group.supported_reference) {
			return false;
		}
		switch (group.type.InternalType()) {
		case PhysicalType::INT8:
		case PhysicalType::UINT8:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
			break;
		default:
			return false;
		}
		if (group_expressions) {
			auto &group_expression = (*group_expressions)[group_idx];
			if (group_expression.return_type.InternalType() != group.type.InternalType()) {
				return false;
			}
			switch (group_expression.kind) {
			case SljitNativeRegionExpressionKind::REFERENCE:
				break;
			case SljitNativeRegionExpressionKind::STRING_COMPRESS:
				if (group.type.InternalType() != PhysicalType::UINT8 ||
				    group_expression.string_compress_target_size != sizeof(uint8_t)) {
					return false;
				}
				break;
			default:
				return false;
			}
		}
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		auto &payload = payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return false;
			}
			continue;
		}
		if (!AggregatePrimitiveUpdateRequiresPayload(aggregate.primitive_update_kind) ||
		    (payload.kind != SljitNativeRegionExpressionKind::REFERENCE &&
		     payload.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE)) {
			return false;
		}
		if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
		    aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			return false;
		}
	}
	return true;
}

static bool TryUsePrimitiveReferenceAggregateUpdate(const vector<LogicalType> &input_types,
                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                    bool render_diagnostics) {
	if (aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    aggregate_update.aggregate_update.use_primitive_payloads) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (sink.aggregates.empty()) {
		return false;
	}
	const bool grouped_state =
	    (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	     sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) &&
	    sink.aggregate_contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::READY;
	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE && !grouped_state) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		SljitNativeRegionExpressionPlan payload;
		if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, grouped_state,
		                                            render_diagnostics)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	const bool perfect_hash_group_lookup = grouped_state && SljitPerfectHashGroupLookupSupported(sink, payloads);

	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	aggregate_update.aggregate_update.use_grouped_state_addresses = grouped_state;
	aggregate_update.aggregate_update.use_perfect_hash_group_lookup = perfect_hash_group_lookup;
	if (render_diagnostics) {
		if (!aggregate_update.aggregate_update.ir.empty()) {
			aggregate_update.aggregate_update.ir += ";";
		}
		aggregate_update.aggregate_update.ir += "payload_update=generated-primitive";
		if (aggregate_update.aggregate_update.use_perfect_hash_group_lookup) {
			aggregate_update.aggregate_update.ir += ";grouped_state_lookup=generated-perfect-hash";
		} else if (aggregate_update.aggregate_update.use_grouped_state_addresses) {
			aggregate_update.aggregate_update.ir += ";grouped_state_lookup=native-state-address";
		}
	}
	return true;
}

void FusePrimitiveAggregateUpdates(SljitNativeRegionPlan &region, const vector<LogicalType> &region_input_types,
                                   bool render_diagnostics) {
	bool changed;
	do {
		changed = false;
		auto input_types = region_input_types;
		idx_t op_idx = 0;
		while (op_idx + 1 < region.ops.size()) {
			auto &op = region.ops[op_idx];
			auto &next = region.ops[op_idx + 1];
			if (TryComposePrimitiveAggregatePayloadsThroughProjection(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryFuseNativeProjectionIntoPerfectHashAggregateUpdate(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(input_types, op, next, render_diagnostics)) {
				region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
				changed = true;
				op_idx = 0;
				input_types = region_input_types;
				continue;
			}
			if (TryPartiallyFuseNativeProjectionIntoPerfectHashAggregateUpdate(input_types, op, next,
			                                                                   render_diagnostics)) {
				changed = true;
				input_types = op.output_types;
				op_idx++;
				continue;
			}
			if (TryPartiallyFuseNativeProjectionIntoRegularHashAggregateUpdate(input_types, op, next,
			                                                                   render_diagnostics)) {
				changed = true;
				input_types = op.output_types;
				op_idx++;
				continue;
			}
			input_types = op.output_types;
			op_idx++;
		}
		input_types = region_input_types;
		for (auto &op : region.ops) {
			if (TryUsePrimitiveReferenceAggregateUpdate(input_types, op, render_diagnostics)) {
				changed = true;
			}
			input_types = op.output_types;
		}
	} while (changed);
}

static bool SljitGuardedReferenceCanCopyRawValue(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::UINT128:
	case PhysicalType::INT128:
		return true;
	default:
		return false;
	}
}

static bool TryReadNativeErrorGuardedReference(const ExecutionExpressionIR &root,
                                               SljitNativeRegionExpressionPlan &expr) {
	if (root.kind != ExecutionExpressionIRKind::CASE || root.children.size() != 2 || !root.else_node) {
		return false;
	}
	auto &when = *root.children[0];
	auto &then_node = *root.children[1];
	auto &else_node = *root.else_node;
	if (then_node.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    then_node.intrinsic != ExecutionExpressionIntrinsicKind::ERROR || then_node.children.size() != 1 ||
	    !then_node.children[0]) {
		return false;
	}
	auto &message_node = *then_node.children[0];
	if (message_node.kind != ExecutionExpressionIRKind::CONSTANT ||
	    message_node.return_type.id() != LogicalTypeId::VARCHAR || message_node.constant.IsNull()) {
		return false;
	}
	if (else_node.kind != ExecutionExpressionIRKind::REFERENCE || else_node.return_type != root.return_type) {
		return false;
	}
	if (!SljitGuardedReferenceCanCopyRawValue(root.physical_type)) {
		return false;
	}
	auto value_size = GetTypeIdSize(root.physical_type);
	if (value_size == 0) {
		return false;
	}

	SljitNativeIntegerCompareOp guard_compare_op;
	SljitNativeIntegerKind guard_kind;
	idx_t guard_source_index;
	int64_t guard_constant;
	bool guard_constant_on_left;
	if (!TryReadNativeIntegerCompareConstant(when, guard_compare_op, guard_kind, guard_source_index, guard_constant,
	                                         guard_constant_on_left) ||
	    guard_kind != SljitNativeIntegerKind::INT64) {
		return false;
	}

	expr.kind = SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE;
	expr.return_type = root.return_type;
	expr.source_index = else_node.ref_index;
	expr.guard_source_index = guard_source_index;
	expr.guard_compare_op = guard_compare_op;
	expr.guard_constant = guard_constant;
	expr.guard_constant_on_left = guard_constant_on_left;
	expr.guarded_value_size = value_size;
	expr.error_message = StringValue::Get(message_node.constant);
	return true;
}

static bool TryReadNativeDecimal128ScaleUp(const ExecutionExpressionIR &root, idx_t &source_index,
                                           int64_t &scale_factor) {
	if (root.kind != ExecutionExpressionIRKind::CAST || root.try_cast || !root.left ||
	    root.return_type.id() != LogicalTypeId::DECIMAL || root.return_type.InternalType() != PhysicalType::INT128 ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.left->return_type.id() != LogicalTypeId::DECIMAL ||
	    root.left->return_type.InternalType() != PhysicalType::INT128) {
		return false;
	}
	auto source_scale = DecimalType::GetScale(root.left->return_type);
	auto target_scale = DecimalType::GetScale(root.return_type);
	if (target_scale <= source_scale || target_scale - source_scale > 18) {
		return false;
	}
	int64_t factor = 1;
	for (idx_t scale_idx = source_scale; scale_idx < target_scale; scale_idx++) {
		factor *= 10;
	}
	source_index = root.left->ref_index;
	scale_factor = factor;
	return true;
}

static bool TryReadNativeDecimal64ToDouble(const ExecutionExpressionIR &root, idx_t &source_index,
                                           double &scale_factor) {
	if (root.kind != ExecutionExpressionIRKind::CAST || root.try_cast || !root.left ||
	    root.return_type.id() != LogicalTypeId::DOUBLE || root.return_type.InternalType() != PhysicalType::DOUBLE ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.left->return_type.id() != LogicalTypeId::DECIMAL ||
	    root.left->return_type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto source_scale = DecimalType::GetScale(root.left->return_type);
	double factor = 1;
	for (idx_t scale_idx = 0; scale_idx < source_scale; scale_idx++) {
		factor *= 10;
	}
	source_index = root.left->ref_index;
	scale_factor = factor;
	return true;
}

static bool TryReadNativeRegionExpression(const ExecutionExpressionIR &root, bool require_boolean,
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

	if (!require_boolean && TryReadNativeErrorGuardedReference(root, expr)) {
		return true;
	}

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

	SljitNativeConstantOrNull constant_or_null;
	if (!require_boolean && TryReadNativeConstantOrNull(root, constant_or_null)) {
		expr.kind = SljitNativeRegionExpressionKind::CONSTANT_OR_NULL;
		expr.return_type = root.return_type;
		expr.constant_or_null = std::move(constant_or_null);
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
	if (require_boolean) {
		if (TryBuildSljitNativeTypedExpressionTreePlan(root, expr)) {
			return true;
		}
		return false;
	}

	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::STRING_COMPRESS && root.children.size() == 1 &&
	    root.children[0] && root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::VARCHAR && GetTypeIdSize(root.physical_type) > 0) {
		expr.kind = SljitNativeRegionExpressionKind::STRING_COMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		expr.string_compress_target_size = GetTypeIdSize(root.physical_type);
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::STRING_DECOMPRESS && root.children.size() == 1 &&
	    root.children[0] && root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.return_type.id() == LogicalTypeId::VARCHAR && GetTypeIdSize(root.children[0]->physical_type) > 0) {
		expr.kind = SljitNativeRegionExpressionKind::STRING_DECOMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		expr.string_decompress_source_size = GetTypeIdSize(root.children[0]->physical_type);
		return true;
	}
	SljitNativeSignedIntegerWidth integral_compress_source_width;
	SljitNativeUnsignedIntegerWidth integral_compress_target_width;
	int64_t integral_compress_minimum;
	if (TryReadNativeIntegralCompress(root, integral_compress_source_width, integral_compress_target_width,
	                                  source_index, integral_compress_minimum)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = integral_compress_source_width;
		expr.unsigned_cast_target_width = integral_compress_target_width;
		expr.constant = integral_compress_minimum;
		return true;
	}
	SljitNativeUnsignedIntegerWidth integral_decompress_source_width;
	SljitNativeSignedIntegerWidth integral_decompress_target_width;
	int64_t integral_decompress_minimum;
	if (TryReadNativeIntegralDecompress(root, integral_decompress_source_width, integral_decompress_target_width,
	                                    source_index, integral_decompress_minimum)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.unsigned_source_width = integral_decompress_source_width;
		expr.cast_target_width = integral_decompress_target_width;
		expr.constant = integral_decompress_minimum;
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::DATE_YEAR &&
	    root.return_type.id() == LogicalTypeId::BIGINT && root.children.size() == 1 && root.children[0] &&
	    root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::DATE) {
		expr.kind = SljitNativeRegionExpressionKind::DATE_YEAR;
		expr.integer_kind = SljitNativeIntegerKind::INT64;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		return true;
	}

	SljitNativeDoubleBinaryOp double_binary_op;
	SljitNativeDoubleSourceKind double_source_kind;
	SljitNativeDoubleSourceKind double_right_source_kind;
	double double_source_scale;
	double double_right_source_scale;
	double double_constant;
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
	if (TryReadNativeDecimal64ToDouble(root, source_index, double_constant)) {
		expr.kind = SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.double_constant = double_constant;
		return true;
	}

	SljitNativeSignedIntegerWidth cast_source_width;
	SljitNativeSignedIntegerWidth cast_target_width;
	SljitNativeUnsignedIntegerWidth unsigned_cast_target_width;
	bool try_cast;
	if (TryReadNativeDecimal128ScaleUp(root, source_index, constant)) {
		expr.kind = SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = constant;
		return true;
	}
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

bool TryLowerNativeRegionExpressionTreeThroughProjection(
    const ExecutionExpressionFragment &fragment, const vector<SljitNativeRegionExpressionPlan> &input_projection,
    unique_ptr<ExecutionExpressionIR> &tree, string &error, bool render_diagnostics) {
	SljitNativeRegionExpressionPlan expr;
	if (!TryLowerNativeRegionExpression(fragment, true, expr, error, render_diagnostics)) {
		return false;
	}
	if (!TryMapNativeProjectionExpressionSources(input_projection, expr)) {
		error = "sljit-expression-tree-projection-map-failed";
		return false;
	}
	tree = CopySljitExpressionPlanAsInputTree(expr);
	if (!tree) {
		error = "sljit-expression-tree-projection-copy-failed";
		return false;
	}
	return true;
}

static unique_ptr<ExecutionExpressionIR> MakeSljitReferenceExpression(idx_t source_index, const LogicalType &type) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = source_index;
	return result;
}

static SljitNativeRegionExpressionPlan
MakeSljitNativeReference(idx_t source_index, const LogicalType &type, string ir,
                         SljitNativeReferenceOrigin origin = SljitNativeReferenceOrigin::REGION_INPUT) {
	SljitNativeRegionExpressionPlan result;
	result.kind = SljitNativeRegionExpressionKind::REFERENCE;
	result.reference_origin = origin;
	result.return_type = type;
	result.source_index = source_index;
	result.ir = std::move(ir);
	return result;
}

static string SljitTempExpressionIr(const ExecutionExpressionIR &node, idx_t temp_index) {
	return "ssa.temp#" + std::to_string(temp_index) + ":" + ExecutionExpressionIRKindToString(node.kind) + "<" +
	       node.return_type.ToString() + ">";
}

static void AppendSljitNativeTempProjection(SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan expression) {
	auto temp_type = expression.return_type;

	SljitNativeRegionOpPlan temp_op;
	temp_op.kind = SljitNativeRegionOpKind::PROJECTION;
	temp_op.output_types.reserve(graph.current_types.size() + 1);
	temp_op.projections.reserve(graph.current_types.size() + 1);
	for (idx_t col_idx = 0; col_idx < graph.current_types.size(); col_idx++) {
		auto ir = graph.render_diagnostics ? "ssa.pass#" + std::to_string(col_idx) : string();
		temp_op.output_types.push_back(graph.current_types[col_idx]);
		temp_op.projections.push_back(MakeSljitNativeReference(col_idx, graph.current_types[col_idx], std::move(ir),
		                                                       SljitNativeReferenceOrigin::PROJECTION_PASS_THROUGH));
	}
	temp_op.output_types.push_back(temp_type);
	temp_op.projections.push_back(std::move(expression));
	graph.current_types.push_back(std::move(temp_type));
	graph.native_ops.push_back(std::move(temp_op));
}

bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root, SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan &expression);

static bool TryBuildSljitProjectionGraphOperand(const ExecutionExpressionIR &node, SljitProjectionGraphLowering &graph,
                                                unique_ptr<ExecutionExpressionIR> &operand) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE || node.kind == ExecutionExpressionIRKind::CONSTANT) {
		operand = node.Copy();
		return true;
	}

	SljitNativeRegionExpressionPlan temp_expression;
	if (!TryBuildSljitProjectionGraphExpression(node, graph, temp_expression)) {
		return false;
	}
	auto temp_index = graph.current_types.size();
	auto temp_type = temp_expression.return_type;
	if (graph.render_diagnostics && temp_expression.ir.empty()) {
		temp_expression.ir = SljitTempExpressionIr(node, temp_index);
	}
	AppendSljitNativeTempProjection(graph, std::move(temp_expression));
	operand = MakeSljitReferenceExpression(temp_index, temp_type);
	return true;
}

static bool RewriteSljitProjectionGraphOperands(ExecutionExpressionIR &rewritten, SljitProjectionGraphLowering &graph) {
	if (rewritten.left && !TryBuildSljitProjectionGraphOperand(*rewritten.left, graph, rewritten.left)) {
		return false;
	}
	if (rewritten.right && !TryBuildSljitProjectionGraphOperand(*rewritten.right, graph, rewritten.right)) {
		return false;
	}
	if (rewritten.else_node && !TryBuildSljitProjectionGraphOperand(*rewritten.else_node, graph, rewritten.else_node)) {
		return false;
	}
	for (auto &child : rewritten.children) {
		if (!TryBuildSljitProjectionGraphOperand(*child, graph, child)) {
			return false;
		}
	}
	return true;
}

bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root, SljitProjectionGraphLowering &graph,
                                            SljitNativeRegionExpressionPlan &expression) {
	if (TryReadNativeRegionExpression(root, false, expression)) {
		if (expression.kind == SljitNativeRegionExpressionKind::REFERENCE &&
		    expression.source_index >= graph.input_type_count) {
			expression.reference_origin = SljitNativeReferenceOrigin::PROJECTION_TEMP;
		}
		return true;
	}

	auto rewritten = root.Copy();
	if (!RewriteSljitProjectionGraphOperands(*rewritten, graph)) {
		return false;
	}
	if (!TryReadNativeRegionExpression(*rewritten, false, expression)) {
		return false;
	}
	if (expression.kind == SljitNativeRegionExpressionKind::REFERENCE &&
	    expression.source_index >= graph.input_type_count) {
		expression.reference_origin = SljitNativeReferenceOrigin::PROJECTION_TEMP;
	}
	return true;
}

} // namespace duckdb

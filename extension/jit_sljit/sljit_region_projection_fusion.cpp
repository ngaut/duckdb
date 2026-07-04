//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_projection_fusion.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

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
	result.references_region_input = source.references_region_input;
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
	if (!TryReadNativeRegionExpression(*tree, false, result)) {
		return false;
	}
	if (render_diagnostics) {
		result.ir = "compose-expression-tree(" + expr.ir + ")";
	}
	return true;
}

static bool TryComposeNativeProjectionExpression(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                 const SljitNativeRegionExpressionPlan &expr,
                                                 SljitNativeRegionExpressionPlan &result, bool render_diagnostics) {
	if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT) {
		result = expr.Copy();
		if (render_diagnostics) {
			result.ir = "compose-constant(" + expr.ir + ")";
		}
		return true;
	}
	if (expr.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (expr.source_index >= input_projection.size()) {
			return false;
		}
		auto &source = input_projection[expr.source_index];
		if (source.return_type != expr.return_type) {
			return false;
		}
		result = source.Copy();
		if (render_diagnostics) {
			result.ir = "compose-reference(" + source.ir + ")";
		}
		return true;
	}
	if ((expr.expression_tree || expr.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
	     expr.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
	     expr.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
	     expr.kind == SljitNativeRegionExpressionKind::DATE_YEAR) &&
	    TryComposeNativeExpressionTreeThroughProjection(input_projection, expr, result, render_diagnostics)) {
		return true;
	}
	if (expr.source_index >= input_projection.size()) {
		return false;
	}
	auto &source = input_projection[expr.source_index];
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

bool TryComposeNativeProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                const SljitNativeRegionExpressionPlan &expr, SljitNativeRegionExpressionPlan &result,
                                bool render_diagnostics) {
	return TryComposeNativeProjectionExpression(input_projection, expr, result, render_diagnostics) ||
	       TryComposeNativeProjectionThroughReferenceProjection(input_projection, expr, result, render_diagnostics);
}

static bool TryFuseAdjacentNativeProjection(SljitNativeRegionOpPlan &left, const SljitNativeRegionOpPlan &right,
                                            bool render_diagnostics) {
	if (left.kind != SljitNativeRegionOpKind::PROJECTION || right.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	if (!right.input_types.empty() && right.input_types != left.output_types) {
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

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"

#include <limits>

namespace duckdb {

static constexpr const char *SLJIT_NATIVE_CONTRACT_UNSUPPORTED =
    "region IR node is unsupported by SLJIT native contract lowering";

static bool TryGetSljitHashJoinKeyKind(const LogicalType &type, SljitNativeHashJoinKeyKind &kind);
static const char *SljitHashJoinKeyKindToString(SljitNativeHashJoinKeyKind kind);

static unique_ptr<SljitNativePredicate> CopySljitNativePredicate(const unique_ptr<SljitNativePredicate> &input) {
	if (!input) {
		return nullptr;
	}
	auto result = make_uniq<SljitNativePredicate>();
	result->kind = input->kind;
	result->return_type = input->return_type;
	result->constant_value = input->constant_value;
	result->constant_is_null = input->constant_is_null;
	result->conjunction_op = input->conjunction_op;
	result->source_index = input->source_index;
	result->right_source_index = input->right_source_index;
	result->constant = input->constant;
	result->int128_constant_lower = input->int128_constant_lower;
	result->int128_constant_upper = input->int128_constant_upper;
	result->constant_on_left = input->constant_on_left;
	result->integer_kind = input->integer_kind;
	result->double_source_kind = input->double_source_kind;
	result->double_right_source_kind = input->double_right_source_kind;
	result->double_constant = input->double_constant;
	result->double_source_scale = input->double_source_scale;
	result->double_right_source_scale = input->double_right_source_scale;
	result->compare_op = input->compare_op;
	result->null_check_op = input->null_check_op;
	result->constants = input->constants;
	result->list_has_null = input->list_has_null;
	result->not_in = input->not_in;
	result->lower = input->lower;
	result->upper = input->upper;
	result->lower_inclusive = input->lower_inclusive;
	result->upper_inclusive = input->upper_inclusive;
	result->not_between = input->not_between;
	result->string_constant = input->string_constant;
	result->string_constants = input->string_constants;
	result->substring_length = input->substring_length;
	result->guard_has_null_constant = input->guard_has_null_constant;
	result->guard_source_indices = input->guard_source_indices;
	result->source_indices = input->source_indices;
	result->child = CopySljitNativePredicate(input->child);
	result->children.reserve(input->children.size());
	for (auto &child : input->children) {
		result->children.push_back(CopySljitNativePredicate(child));
	}
	return result;
}

static bool ShouldCopySljitNativeExpressionTree(const SljitNativeRegionExpressionPlan &input,
                                                bool copy_auxiliary_expression_tree) {
	if (!input.expression_tree) {
		return false;
	}
	if (input.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
	    input.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		return true;
	}
	return copy_auxiliary_expression_tree;
}

SljitNativeRegionExpressionPlan CopySljitNativeRegionExpression(const SljitNativeRegionExpressionPlan &input,
                                                                bool copy_auxiliary_expression_tree) {
	SljitNativeRegionExpressionPlan result;
	result.kind = input.kind;
	result.reference_origin = input.reference_origin;
	result.integer_kind = input.integer_kind;
	result.return_type = input.return_type;
	result.constant_value = input.constant_value;
	result.source_index = input.source_index;
	result.right_source_index = input.right_source_index;
	result.constant = input.constant;
	result.double_constant = input.double_constant;
	result.result_min = input.result_min;
	result.result_max = input.result_max;
	result.constant_on_left = input.constant_on_left;
	result.check_result_range = input.check_result_range;
	result.binary_op = input.binary_op;
	result.double_binary_op = input.double_binary_op;
	result.double_source_kind = input.double_source_kind;
	result.double_right_source_kind = input.double_right_source_kind;
	result.double_source_scale = input.double_source_scale;
	result.double_right_source_scale = input.double_right_source_scale;
	result.compare_op = input.compare_op;
	result.cast_source_width = input.cast_source_width;
	result.cast_target_width = input.cast_target_width;
	result.unsigned_source_width = input.unsigned_source_width;
	result.unsigned_cast_target_width = input.unsigned_cast_target_width;
	result.query_location = input.query_location;
	result.string_compress_target_size = input.string_compress_target_size;
	result.string_decompress_source_size = input.string_decompress_source_size;
	result.guard_source_index = input.guard_source_index;
	result.guard_compare_op = input.guard_compare_op;
	result.guard_constant = input.guard_constant;
	result.guard_constant_on_left = input.guard_constant_on_left;
	result.guarded_value_size = input.guarded_value_size;
	result.error_message = input.error_message;
	result.try_cast = input.try_cast;
	result.signed_integer_width = input.signed_integer_width;
	result.coalesce_rhs_kind = input.coalesce_rhs_kind;
	result.coalesce_constant_is_null = input.coalesce_constant_is_null;
	result.null_check_op = input.null_check_op;
	result.constants = input.constants;
	result.lower = input.lower;
	result.upper = input.upper;
	result.list_has_null = input.list_has_null;
	result.not_in = input.not_in;
	result.not_between = input.not_between;
	result.lower_inclusive = input.lower_inclusive;
	result.upper_inclusive = input.upper_inclusive;
	result.predicate = CopySljitNativePredicate(input.predicate);
	if (ShouldCopySljitNativeExpressionTree(input, copy_auxiliary_expression_tree)) {
		result.expression_tree = input.expression_tree->Copy();
		result.expression_tree_source_indices = input.expression_tree_source_indices;
	}
	result.constant_or_null = input.constant_or_null;
	result.ir = input.ir;
	return result;
}

static vector<SljitNativeRegionExpressionPlan>
CopySljitNativeRegionExpressions(const vector<SljitNativeRegionExpressionPlan> &input) {
	vector<SljitNativeRegionExpressionPlan> result;
	result.reserve(input.size());
	for (auto &expr : input) {
		result.push_back(CopySljitNativeRegionExpression(expr));
	}
	return result;
}

SljitNativeHashJoinProbePlan CopySljitNativeHashJoinProbePlan(const SljitNativeHashJoinProbePlan &input) {
	SljitNativeHashJoinProbePlan result;
	result.operator_index = input.operator_index;
	result.keys = input.keys;
	result.equality_key_count = input.equality_key_count;
	result.mark_build_match = input.mark_build_match;
	result.mark_build_match_after_residual = input.mark_build_match_after_residual;
	result.residual_predicate = input.residual_predicate;
	result.perfect_hash_probe = input.perfect_hash_probe;
	result.found_match_offset = input.found_match_offset;
	result.pointer_offset = input.pointer_offset;
	result.output_mode = input.output_mode;
	result.input_types = input.input_types;
	result.residual_source_types = input.residual_source_types;
	result.residual_filter = CopySljitNativeRegionExpression(input.residual_filter, false);
	result.operator_info = input.operator_info;
	result.ir = input.ir;
	return result;
}

static SljitNativeNestedLoopJoinProbeConditionPlan
CopySljitNativeNestedLoopJoinProbeConditionPlan(const SljitNativeNestedLoopJoinProbeConditionPlan &input) {
	SljitNativeNestedLoopJoinProbeConditionPlan result;
	result.lhs_condition = CopySljitNativeRegionExpression(input.lhs_condition);
	result.type = input.type;
	result.comparison_type = input.comparison_type;
	result.value_kind = input.value_kind;
	result.ir = input.ir;
	return result;
}

static SljitNativeNestedLoopJoinProbePlan
CopySljitNativeNestedLoopJoinProbePlan(const SljitNativeNestedLoopJoinProbePlan &input) {
	SljitNativeNestedLoopJoinProbePlan result;
	result.operator_index = input.operator_index;
	result.input_types = input.input_types;
	result.condition_types = input.condition_types;
	result.join_type = input.join_type;
	result.operator_info = input.operator_info;
	result.ir = input.ir;
	result.conditions.reserve(input.conditions.size());
	for (auto &condition : input.conditions) {
		result.conditions.push_back(CopySljitNativeNestedLoopJoinProbeConditionPlan(condition));
	}
	return result;
}

static SljitNativeRegionOpPlan CopySljitNativeRegionOp(const SljitNativeRegionOpPlan &input) {
	SljitNativeRegionOpPlan result;
	result.kind = input.kind;
	result.operator_index = input.operator_index;
	result.output_types = input.output_types;
	result.filter = CopySljitNativeRegionExpression(input.filter);
	result.hash_join_probe = CopySljitNativeHashJoinProbePlan(input.hash_join_probe);
	result.hash_join_build = input.hash_join_build;
	result.nested_loop_join_probe = CopySljitNativeNestedLoopJoinProbePlan(input.nested_loop_join_probe);
	result.nested_loop_join_build.sink_info = input.nested_loop_join_build.sink_info;
	result.nested_loop_join_build.input_types = input.nested_loop_join_build.input_types;
	result.nested_loop_join_build.condition_types = input.nested_loop_join_build.condition_types;
	result.nested_loop_join_build.ir = input.nested_loop_join_build.ir;
	result.nested_loop_join_build.rhs_conditions =
	    CopySljitNativeRegionExpressions(input.nested_loop_join_build.rhs_conditions);
	result.append_sink = input.append_sink;
	result.delim_join_sink = input.delim_join_sink;
	result.aggregate_update.sink_info = input.aggregate_update.sink_info;
	result.aggregate_update.input_types = input.aggregate_update.input_types;
	result.aggregate_update.use_primitive_payloads = input.aggregate_update.use_primitive_payloads;
	result.aggregate_update.use_grouped_state_addresses = input.aggregate_update.use_grouped_state_addresses;
	result.aggregate_update.use_perfect_hash_group_lookup = input.aggregate_update.use_perfect_hash_group_lookup;
	result.aggregate_update.ir = input.aggregate_update.ir;
	result.aggregate_update.payloads = CopySljitNativeRegionExpressions(input.aggregate_update.payloads);
	result.order_sink.sink_info = input.order_sink.sink_info;
	result.order_sink.input_types = input.order_sink.input_types;
	result.order_sink.key_types = input.order_sink.key_types;
	result.order_sink.ir = input.order_sink.ir;
	result.order_sink.order_keys = CopySljitNativeRegionExpressions(input.order_sink.order_keys);
	result.projections = CopySljitNativeRegionExpressions(input.projections);
	return result;
}

unique_ptr<SljitNativeRegionPlan> CopySljitNativeRegion(const SljitNativeRegionPlan &input) {
	auto result = make_uniq<SljitNativeRegionPlan>();
	result->source_execution = input.source_execution;
	result->summary = input.summary;
	result->ops.reserve(input.ops.size());
	for (auto &op : input.ops) {
		result->ops.push_back(CopySljitNativeRegionOp(op));
	}
	return result;
}

static bool IsIdentityProjection(const SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.size() != input_types.size() ||
	    op.output_types.size() != input_types.size()) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < input_types.size(); col_idx++) {
		auto &projection = op.projections[col_idx];
		if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE || projection.source_index != col_idx ||
		    projection.return_type != input_types[col_idx] || op.output_types[col_idx] != input_types[col_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitNativeRegionExpressionGeneratesCode(const SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::CONSTANT:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		return false;
	default:
		return true;
	}
}

static bool SljitNativeRegionExpressionsGenerateCode(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	for (auto &expr : expressions) {
		if (SljitNativeRegionExpressionGeneratesCode(expr)) {
			return true;
		}
	}
	return false;
}

static bool IsSljitNativeTreeDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool IsSljitNativeTypedTreeInt64Node(const ExecutionExpressionIR &node) {
	return node.return_type.IsIntegral() && node.physical_type == PhysicalType::INT64;
}

static bool IsSljitNativeTypedTreeInt32Node(const ExecutionExpressionIR &node) {
	return node.return_type.IsIntegral() && node.physical_type == PhysicalType::INT32;
}

static bool IsSljitNativeTypedTreeBoolNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::BOOLEAN && node.physical_type == PhysicalType::BOOL;
}

static bool IsSljitNativeTypedTreeValueNode(const ExecutionExpressionIR &node) {
	return IsSljitNativeTypedTreeInt64Node(node) || IsSljitNativeTypedTreeInt32Node(node) ||
	       IsSljitNativeTypedTreeBoolNode(node);
}

static bool IsSljitNativeTypedTreeIntegerNode(const ExecutionExpressionIR &node) {
	return IsSljitNativeTypedTreeInt64Node(node) || IsSljitNativeTypedTreeInt32Node(node);
}

static bool IsSljitNativeTypedTreeSameIntegerKind(const ExecutionExpressionIR &left,
                                                  const ExecutionExpressionIR &right) {
	return (IsSljitNativeTypedTreeInt64Node(left) && IsSljitNativeTypedTreeInt64Node(right)) ||
	       (IsSljitNativeTypedTreeInt32Node(left) && IsSljitNativeTypedTreeInt32Node(right));
}

static bool SljitNativeTypedTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (IsSljitNativeTypedTreeInt64Node(left) && IsSljitNativeTypedTreeInt64Node(right)) ||
	       (IsSljitNativeTypedTreeInt32Node(left) && IsSljitNativeTypedTreeInt32Node(right)) ||
	       (IsSljitNativeTypedTreeBoolNode(left) && IsSljitNativeTypedTreeBoolNode(right));
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

static bool SljitNativeTreeBinaryOpSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return true;
	default:
		return false;
	}
}

static bool SljitNativeTypedTreeComparisonSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return true;
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
	if (node.kind != ExecutionExpressionIRKind::BINARY || !node.left || !node.right ||
	    !SljitNativeTreeBinaryOpSupported(node.binary_op) || !IsSljitNativeTreeDecimal64Node(node) ||
	    !IsSljitNativeTreeDecimal64Node(*node.left) || !IsSljitNativeTreeDecimal64Node(*node.right) ||
	    !SljitNativeTreeDecimal64BinaryHasRawSemantics(node)) {
		return false;
	}
	return SljitNativeTreeNodeSupported(*node.left) && SljitNativeTreeNodeSupported(*node.right);
}

static bool SljitNativeTypedTreeNodeSupported(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		return IsSljitNativeTypedTreeValueNode(node);
	case ExecutionExpressionIRKind::CONSTANT:
		return IsSljitNativeTypedTreeValueNode(node);
	case ExecutionExpressionIRKind::CAST:
		return !node.try_cast && node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR &&
		       node.exception_behavior != ExecutionExpressionExceptionKind::ERROR &&
		       IsSljitNativeTypedTreeInt64Node(node) && node.left && IsSljitNativeTypedTreeIntegerNode(*node.left) &&
		       SljitNativeTypedTreeNodeSupported(*node.left);
	case ExecutionExpressionIRKind::UNARY:
		if (!node.left) {
			return false;
		}
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::NOT:
			return IsSljitNativeTypedTreeBoolNode(node) && IsSljitNativeTypedTreeBoolNode(*node.left) &&
			       SljitNativeTypedTreeNodeSupported(*node.left);
		case ExecutionExpressionUnaryOp::IS_NULL:
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			return IsSljitNativeTypedTreeBoolNode(node) && IsSljitNativeTypedTreeValueNode(*node.left) &&
			       SljitNativeTypedTreeNodeSupported(*node.left);
		default:
			return false;
		}
	case ExecutionExpressionIRKind::BINARY:
		if (!node.left || !node.right) {
			return false;
		}
		if (SljitNativeTypedTreeComparisonSupported(node.binary_op)) {
			return IsSljitNativeTypedTreeBoolNode(node) &&
			       IsSljitNativeTypedTreeSameIntegerKind(*node.left, *node.right) &&
			       SljitNativeTypedTreeNodeSupported(*node.left) && SljitNativeTypedTreeNodeSupported(*node.right);
		}
		return SljitNativeTreeBinaryOpSupported(node.binary_op) && IsSljitNativeTypedTreeIntegerNode(node) &&
		       IsSljitNativeTypedTreeSameIntegerKind(node, *node.left) &&
		       IsSljitNativeTypedTreeSameIntegerKind(node, *node.right) &&
		       SljitNativeTypedTreeNodeSupported(*node.left) && SljitNativeTypedTreeNodeSupported(*node.right);
	case ExecutionExpressionIRKind::CONJUNCTION:
		if (!IsSljitNativeTypedTreeBoolNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !IsSljitNativeTypedTreeBoolNode(*child) || !SljitNativeTypedTreeNodeSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::COALESCE:
		if (!IsSljitNativeTypedTreeValueNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !SljitNativeTypedTreeSameValueKind(node, *child) ||
			    !SljitNativeTypedTreeNodeSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::CASE:
		if (!IsSljitNativeTypedTreeValueNode(node) || !node.else_node || node.children.empty() ||
		    node.children.size() % 2 != 0 || !SljitNativeTypedTreeSameValueKind(node, *node.else_node) ||
		    !SljitNativeTypedTreeNodeSupported(*node.else_node)) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
			auto &condition = node.children[child_idx];
			auto &value = node.children[child_idx + 1];
			if (!condition || !value || !IsSljitNativeTypedTreeBoolNode(*condition) ||
			    !SljitNativeTypedTreeSameValueKind(node, *value) || !SljitNativeTypedTreeNodeSupported(*condition) ||
			    !SljitNativeTypedTreeNodeSupported(*value)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
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

static bool TryGetSljitTypedTreeResultKind(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind) {
	if (IsSljitNativeTypedTreeInt64Node(root)) {
		kind = SljitNativeIntegerKind::INT64;
		return true;
	}
	if (IsSljitNativeTypedTreeBoolNode(root)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	return false;
}

static bool TryBuildSljitNativeTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                       SljitNativeRegionExpressionPlan &expr) {
	SljitNativeIntegerKind result_kind;
	if (!SljitNativeTypedTreeNodeSupported(root) || !TryGetSljitTypedTreeResultKind(root, result_kind)) {
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

static bool SljitNativeRegionOpGeneratesCode(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.filter);
	case SljitNativeRegionOpKind::PROJECTION:
		return SljitNativeRegionExpressionsGenerateCode(op.projections);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return !op.hash_join_probe.keys.empty() && op.hash_join_probe.equality_key_count > 0 &&
		       op.hash_join_probe.equality_key_count <= op.hash_join_probe.keys.size() &&
		       op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::NONE;
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return SljitNativeRegionExpressionsGenerateCode(op.nested_loop_join_build.rhs_conditions);
	case SljitNativeRegionOpKind::ORDER_SINK:
		return SljitNativeRegionExpressionsGenerateCode(op.order_sink.order_keys);
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return false;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return op.aggregate_update.use_primitive_payloads;
	default:
		return false;
	}
}

static bool SljitNativeRegionOpGeneratesMachineCode(const SljitNativeRegionOpPlan &op) {
	if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
		return op.nested_loop_join_probe.join_type == ExecutionRegionJoinType::INNER &&
		       op.nested_loop_join_probe.conditions.size() == 1;
	}
	return SljitNativeRegionOpGeneratesCode(op);
}

static string SljitNativeRegionCodegenFusionBlocker() {
	return "operator-contract-blocker:native-operator-executable-body-missing";
}

static const char *SljitNativeRegionOpKindName(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::FILTER:
		return "filter";
	case SljitNativeRegionOpKind::PROJECTION:
		return "projection";
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return "nested-loop-join-probe";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return "nested-loop-join-build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order-sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append-sink";
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return "delim-join-sink";
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return "aggregate-update";
	default:
		return "unknown";
	}
}

static string BuildSljitNativeRegionShape(const SljitNativeRegionPlan &region) {
	string result;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		if (op_idx > 0) {
			result += "-";
		}
		result += SljitNativeRegionOpKindName(region.ops[op_idx].kind);
	}
	return result.empty() ? "empty" : result;
}

static bool SljitNativeRegionOpIsNativeSink(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return op.hash_join_build.sink_info.kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
		       op.hash_join_build.sink_info.hash_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return op.nested_loop_join_build.sink_info.kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD &&
		       op.nested_loop_join_build.sink_info.nested_loop_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::APPEND_SINK:
		return (op.append_sink.sink_info.kind == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
		        op.append_sink.sink_info.kind == ExecutionRegionSinkKind::MATERIALIZATION) &&
		       op.append_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::ORDER_SINK:
		return op.order_sink.sink_info.kind == ExecutionRegionSinkKind::SORT &&
		       op.order_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return op.delim_join_sink.sink_info.kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK &&
		       op.delim_join_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
		auto &sink = op.aggregate_update.sink_info;
		return (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) &&
		       sink.aggregate_contract.native_state_update_contract.status == ExecutionRegionStateContractStatus::READY;
	}
	default:
		return false;
	}
}

static string SljitNativeRegionOpBoundaryBlocker(const SljitNativeRegionOpPlan &op) {
	if (op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::PROJECTION) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		if (op.aggregate_update.use_primitive_payloads) {
			return string();
		}
		return "operator-contract-blocker:aggregate-update-generated-payload-missing";
	}
	if (SljitNativeRegionOpIsNativeSink(op)) {
		return string();
	}
	return "operator-contract-blocker:whole-vectorized-operator-boundary;stage=" +
	       string(SljitNativeRegionOpKindName(op.kind));
}

static SljitNativeRegionSummary BuildSljitNativeRegionSummary(const SljitNativeRegionPlan &region) {
	SljitNativeRegionSummary summary;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		const auto generates_machine_code = SljitNativeRegionOpGeneratesMachineCode(op);
		summary.generates_machine_code = summary.generates_machine_code || generates_machine_code;
		if (!summary.has_whole_operator_boundary_stage) {
			auto op_blocker = SljitNativeRegionOpBoundaryBlocker(op);
			if (!op_blocker.empty()) {
				summary.has_whole_operator_boundary_stage = true;
				summary.whole_operator_boundary_blocker = std::move(op_blocker);
			}
		}
	}
	return summary;
}

static void FinalizeSljitNativeRegionPlan(SljitNativeRegionPlan &region) {
	region.summary = BuildSljitNativeRegionSummary(region);
}

static bool SljitNativeRegionHasExecutableBody(const SljitNativeRegionPlan &region) {
	return !region.summary.has_whole_operator_boundary_stage && region.summary.generates_machine_code;
}

static bool SljitNativeRegionHasExecutableBodyGap(const SljitNativeRegionPlan &region, string &blocker) {
	if (region.summary.has_whole_operator_boundary_stage) {
		blocker = region.summary.whole_operator_boundary_blocker;
		return true;
	}
	if (!SljitNativeRegionHasExecutableBody(region)) {
		blocker = "SLJIT native region emits no generated machine code";
		return true;
	}
	return false;
}

static bool SljitRegionIsFullyFused(const SljitNativeRegionPlan &region, const ExecutionRegionContract &contract) {
	if (contract.source_boundary_count > 0 || contract.missing_contract_count > 0) {
		return false;
	}
	return SljitNativeRegionHasExecutableBody(region);
}

static bool IsComposableNativeAddConstant(const SljitNativeRegionExpressionPlan &expr) {
	return expr.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT &&
	       expr.binary_op == SljitNativeIntegerBinaryOp::ADD && !expr.constant_on_left;
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

static bool TryMapNativeProjectionExpressionSources(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                    SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return true;
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
			       TryMapNativeProjectionSourceIndex(input_projection, expr.guard_source_index);
		}
		if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
			for (auto &source_index : expr.constant_or_null.guard_source_indices) {
				if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
					return false;
				}
			}
			return true;
		}
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index);
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		return TryMapNativeProjectionSourceIndex(input_projection, expr.source_index) &&
		       TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index);
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		if (!TryMapNativeProjectionSourceIndex(input_projection, expr.source_index)) {
			return false;
		}
		if (expr.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			return TryMapNativeProjectionSourceIndex(input_projection, expr.right_source_index);
		}
		return true;
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (!expr.predicate || !TryMapNativePredicateSourcesThroughProjection(input_projection, *expr.predicate)) {
			return false;
		}
		FinalizeSljitNativePredicateSourceIndices(*expr.predicate);
		return true;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		for (auto &source_index : expr.expression_tree_source_indices) {
			if (!TryMapNativeProjectionSourceIndex(input_projection, source_index)) {
				return false;
			}
		}
		return true;
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
		result = CopySljitNativeRegionExpression(expr);
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
		result = CopySljitNativeRegionExpression(source);
		if (render_diagnostics) {
			result.ir = "compose-reference(" + source.ir + ")";
		}
		return true;
	}
	if (!IsComposableNativeAddConstant(expr)) {
		return false;
	}
	if (source.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (source.return_type != expr.return_type) {
			return false;
		}
		result = CopySljitNativeRegionExpression(expr);
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
	result = CopySljitNativeRegionExpression(source);
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
	result = CopySljitNativeRegionExpression(expr);
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

static void FuseAdjacentNativeProjections(SljitNativeRegionPlan &region, bool render_diagnostics) {
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
		if (aggregate_payload_kind == SljitNativeIntegerKind::INT64 &&
		    TryGetSljitTypedTreeResultKind(*payload.expression_tree, typed_tree_kind) &&
		    typed_tree_kind == SljitNativeIntegerKind::INT64) {
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
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return aggregate_payload_kind == SljitNativeIntegerKind::INT64 && payload.expression_tree != nullptr;
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

static bool SljitPrimitiveAggregatePayloadCanEraseProjection(const SljitNativeRegionExpressionPlan &payload) {
	if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		return true;
	}
	return payload.reference_origin == SljitNativeReferenceOrigin::REGION_INPUT ||
	       payload.reference_origin == SljitNativeReferenceOrigin::SOURCE_OUTPUT;
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
		auto payload = CopySljitNativeRegionExpression(projection.projections[aggregate.payload_index]);
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
	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || sink.aggregates.empty() ||
	    sink.aggregates.size() != aggregate_update.aggregate_update.payloads.size()) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(aggregate_update.aggregate_update.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.aggregate_update.payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		auto &payload = aggregate_update.aggregate_update.payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			payloads.push_back(CopySljitNativeRegionExpression(payload));
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

static bool SljitPerfectHashGroupLookupSupported(const ExecutionRegionSinkInfo &sink,
                                                 const vector<SljitNativeRegionExpressionPlan> &payloads) {
	auto &contract = sink.aggregate_contract;
	if (sink.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	    contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH || !contract.grouped_state_layout_ready ||
	    payloads.empty() || contract.perfect_required_bits.size() != sink.groups.size() ||
	    contract.perfect_group_minima.size() != sink.groups.size()) {
		return false;
	}
	for (auto &group : sink.groups) {
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
		    payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
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
	if (grouped_state && !perfect_hash_group_lookup) {
		return false;
	}

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
		}
	}
	return true;
}

static void FusePrimitiveAggregateUpdates(SljitNativeRegionPlan &region, const vector<LogicalType> &region_input_types,
                                          bool render_diagnostics) {
	if (region.ops.size() < 2) {
		auto input_types = region_input_types;
		for (auto &op : region.ops) {
			TryUsePrimitiveReferenceAggregateUpdate(input_types, op, render_diagnostics);
			input_types = op.output_types;
		}
		return;
	}
	auto input_types = region_input_types;
	idx_t op_idx = 0;
	while (op_idx + 1 < region.ops.size()) {
		auto &op = region.ops[op_idx];
		auto &next = region.ops[op_idx + 1];
		if (TryComposePrimitiveAggregatePayloadsThroughProjection(input_types, op, next, render_diagnostics)) {
			region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
			op_idx = 0;
			input_types = region_input_types;
			continue;
		}
		if (TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(input_types, op, next, render_diagnostics)) {
			region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx));
			op_idx = 0;
			input_types = region_input_types;
			continue;
		}
		input_types = op.output_types;
		op_idx++;
	}
	input_types = region_input_types;
	for (auto &op : region.ops) {
		TryUsePrimitiveReferenceAggregateUpdate(input_types, op, render_diagnostics);
		input_types = op.output_types;
	}
}

static string SljitRegionCandidateContext(const ExecutionRegionContract &contract) {
	if (ExecutionRegionABIIsFullPipeline(contract.abi)) {
		return "full-pipeline";
	}
	return "unknown";
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
	if (TryReadNativeDecimal64BinaryReferences(root, binary_op, source_index, right_source_index, result_min,
	                                           result_max)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		expr.integer_kind = SljitNativeIntegerKind::DECIMAL64;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.binary_op = binary_op;
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
		AttachSljitNativeExpressionTree(root, expr);
		return true;
	}
	if (TryBuildSljitNativeAnyExpressionTreePlan(root, expr)) {
		return true;
	}
	return false;
}

static bool TryLowerNativeRegionExpression(const ExecutionExpressionFragment &fragment, bool require_boolean,
                                           SljitNativeRegionExpressionPlan &expr, string &error,
                                           bool render_diagnostics) {
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

struct SljitProjectionGraphLowering {
	SljitProjectionGraphLowering(const vector<LogicalType> &input_types_p, bool render_diagnostics_p)
	    : input_type_count(input_types_p.size()), render_diagnostics(render_diagnostics_p),
	      current_types(input_types_p) {
	}

	idx_t input_type_count;
	bool render_diagnostics;
	vector<LogicalType> current_types;
	vector<SljitNativeRegionOpPlan> native_ops;
};

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

static bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root,
                                                   SljitProjectionGraphLowering &graph,
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

static bool TryBuildSljitProjectionGraphExpression(const ExecutionExpressionIR &root,
                                                   SljitProjectionGraphLowering &graph,
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

struct SljitRegionNodePlan {
	ExecutionRegionLoweringKind kind = ExecutionRegionLoweringKind::BOUNDARY;
	string reason;
	vector<SljitNativeRegionOpPlan> native_ops;
	bool uses_scan_filters = false;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool requires_source_contract = false;
};

static bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan) {
	return !node_plan.native_ops.empty();
}

static const SljitNativeRegionOpPlan &SljitRegionNodeFirstNativeOp(const SljitRegionNodePlan &node_plan) {
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops[0];
}

static SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(SljitRegionNodePlan &node_plan) {
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops.back();
}

static bool SljitRegionNodeHasSingleNativeOp(const SljitRegionNodePlan &node_plan) {
	return node_plan.native_ops.size() == 1;
}

static void AppendSljitRegionNodeNativeOps(SljitNativeRegionPlan &region, SljitRegionNodePlan &node_plan) {
	for (auto &op : node_plan.native_ops) {
		region.ops.push_back(std::move(op));
	}
}

static SljitRegionNodePlan SljitNativeNode(SljitNativeRegionOpPlan &&native_op, string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_ops.push_back(std::move(native_op));
	return result;
}

static SljitRegionNodePlan SljitNativeNode(vector<SljitNativeRegionOpPlan> native_ops, string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_ops = std::move(native_ops);
	return result;
}

static SljitRegionNodePlan SljitRegionBoundaryNode(string reason) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::BOUNDARY;
	result.reason = std::move(reason);
	return result;
}

static SljitRegionNodePlan SljitNodeBlockerBoundary(const ExecutionRegionNode &node, const char *fallback) {
	return SljitRegionBoundaryNode(node.blocker_reason.empty() ? string(fallback) : node.blocker_reason);
}

static string SljitBlockerOrReason(const string &blocker, const char *reason) {
	return blocker.empty() ? string(reason) : blocker;
}

static void AppendSljitReasonPart(string &reason, const string &part, bool render_diagnostics) {
	if (render_diagnostics && !part.empty()) {
		reason += ";";
		reason += part;
	}
}

static SljitRegionNodePlan SljitBlockedContractBoundary(const string &blocker, const char *reason) {
	return SljitRegionBoundaryNode(SljitBlockerOrReason(blocker, reason));
}

static SljitRegionNodePlan SljitUnsupportedExpressionBoundaryNode(const string &error) {
	string reason = SLJIT_NATIVE_CONTRACT_UNSUPPORTED;
	if (!error.empty()) {
		reason += ";";
		reason += error;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

static bool SljitSourceContractIsBlocked(const ExecutionRegionNode &node) {
	if (!node.source) {
		return false;
	}
	auto &source_contract = node.source->source_contract;
	return source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED &&
	       !source_contract.required_capability.empty() && !source_contract.contract_version.empty() &&
	       !source_contract.blocker.empty();
}

static SljitRegionNodePlan PlanSljitFilterNode(const ExecutionRegionNode &node, string &error,
                                               bool render_diagnostics) {
	if (!node.blocker_reason.empty() || !node.filter) {
		return SljitNodeBlockerBoundary(node, "filter expression unsupported by SLJIT IR lowering");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::FILTER;
	native_op.output_types = node.output_types;
	if (!TryLowerNativeRegionExpression(*node.filter, true, native_op.filter, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}

	return SljitNativeNode(std::move(native_op), "generated typed predicate filter");
}

static bool TryPlanDirectSljitProjection(const ExecutionRegionNode &node, SljitNativeRegionOpPlan &native_op,
                                         string &error, bool render_diagnostics) {
	native_op = SljitNativeRegionOpPlan();
	native_op.kind = SljitNativeRegionOpKind::PROJECTION;
	native_op.output_types = node.output_types;
	for (auto &expression : node.projections) {
		SljitNativeRegionExpressionPlan native_expression;
		if (!TryLowerNativeRegionExpression(*expression, false, native_expression, error, render_diagnostics)) {
			return false;
		}
		native_op.projections.push_back(std::move(native_expression));
	}
	return true;
}

static bool TryPlanExpandedSljitProjection(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                           vector<SljitNativeRegionOpPlan> &native_ops, string &error,
                                           bool render_diagnostics) {
	if (input_types.empty() && !node.output_types.empty()) {
		error = "projection expression graph lowering requires input types";
		return false;
	}

	SljitProjectionGraphLowering graph(input_types, render_diagnostics);
	vector<SljitNativeRegionExpressionPlan> final_projections;
	final_projections.reserve(node.projections.size());
	for (auto &fragment : node.projections) {
		if (!fragment->root) {
			error = "projection expression graph lowering requires rooted JIT IR";
			return false;
		}
		SljitNativeRegionExpressionPlan projection;
		if (!TryBuildSljitProjectionGraphExpression(*fragment->root, graph, projection)) {
			return false;
		}
		if (render_diagnostics) {
			projection.ir = fragment->ir;
		}
		final_projections.push_back(std::move(projection));
	}

	SljitNativeRegionOpPlan final_op;
	final_op.kind = SljitNativeRegionOpKind::PROJECTION;
	final_op.output_types = node.output_types;
	final_op.projections = std::move(final_projections);
	graph.native_ops.push_back(std::move(final_op));
	native_ops = std::move(graph.native_ops);
	return true;
}

static SljitRegionNodePlan PlanSljitProjectionNode(const ExecutionRegionNode &node,
                                                   const vector<LogicalType> &input_types, string &error,
                                                   bool render_diagnostics) {
	if (!node.blocker_reason.empty() || node.projections.empty()) {
		return SljitNodeBlockerBoundary(node, "projection has no lowered JIT IR expressions");
	}

	SljitNativeRegionOpPlan native_op;
	if (TryPlanDirectSljitProjection(node, native_op, error, render_diagnostics)) {
		const auto reason =
		    SljitNativeRegionOpGeneratesCode(native_op) ? "generated typed projection" : "reference projection remap";
		return SljitNativeNode(std::move(native_op), reason);
	}

	vector<SljitNativeRegionOpPlan> native_ops;
	if (!TryPlanExpandedSljitProjection(node, input_types, native_ops, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}
	return SljitNativeNode(std::move(native_ops), "generated typed projection expression graph");
}

static string SljitSourceIR(const ExecutionRegionNode &node, bool render_diagnostics,
                            ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE) {
	if (!render_diagnostics) {
		return string();
	}
	if (!node.source) {
		return string();
	}
	return DescribeExecutionRegionSourceInfo(*node.source, execution);
}

static void
AppendSljitSourceIR(string &reason, const ExecutionRegionNode &node, bool render_diagnostics,
                    ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE) {
	auto source_ir = SljitSourceIR(node, render_diagnostics, execution);
	if (!source_ir.empty()) {
		reason += ";";
		reason += source_ir;
	}
}

static SljitRegionNodePlan SljitNativeSourceNode(string reason, const ExecutionRegionNode &node,
                                                 ExecutionRegionSourceExecutionKind execution, bool render_diagnostics,
                                                 bool uses_scan_filters = false) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.source_execution = execution;
	result.uses_scan_filters = uses_scan_filters;
	result.reason = std::move(reason);
	AppendSljitSourceIR(result.reason, node, render_diagnostics, execution);
	return result;
}

static string SljitSourceBoundaryReason(const ExecutionRegionNode &node, bool render_diagnostics) {
	string result =
	    node.blocker_reason.empty() ? "source node is outside SLJIT native region lowering" : node.blocker_reason;
	AppendSljitSourceIR(result, node, render_diagnostics);
	return result;
}

static void AppendSljitSourceFilterFacts(string &reason, const ExecutionRegionNode &node,
                                         const ExecutionRegionTableScanContract &contract, bool include_input_columns) {
	D_ASSERT(node.source);
	reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
	if (include_input_columns) {
		reason += ";source_contract_input_columns=" + std::to_string(contract.source_contract_input_column_count);
	}
	reason += ";source_contract_filter_prune_required=" +
	          string(contract.source_contract_filter_prune_required ? "true" : "false");
}

static SljitRegionNodePlan SljitSourceBoundaryRequiresContract(const ExecutionRegionNode &node,
                                                               const ExecutionRegionTableScanContract &contract,
                                                               bool include_strategy, bool render_diagnostics) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::BOUNDARY;
	result.requires_source_contract = true;
	result.reason = "DuckDB source boundary;source-contract-blocker:requires-source-contract;"
	                "source_execution=duckdb-source-boundary";
	if (include_strategy) {
		result.reason += ";source-strategy=duckdb-source-boundary";
	}
	AppendSljitSourceFilterFacts(result.reason, node, contract, true);
	AppendSljitSourceIR(result.reason, node, render_diagnostics,
	                    ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	return result;
}

static SljitRegionNodePlan PlanSljitSourceContractNode(const ExecutionRegionNode &node,
                                                       const ExecutionRegionContract &contract,
                                                       bool render_diagnostics) {
	D_ASSERT(node.source);
	auto &table_scan_contract = node.source->table_scan_contract;
	if (!table_scan_contract.present) {
		return SljitRegionBoundaryNode("source contract requires typed table scan contract IR");
	}
	if (!ExecutionRegionABIOwnsSource(contract.abi)) {
		return SljitRegionBoundaryNode("source contract requires source ownership in the region contract");
	}

	if (node.source->filters.empty()) {
		return SljitNativeSourceNode("table scan source contract", node,
		                             ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT, render_diagnostics);
	}

	if (table_scan_contract.filter_pushdown) {
		string reason = "vectorized table scan filters;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=true";
		return SljitNativeSourceNode(std::move(reason), node, ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT,
		                             render_diagnostics, true);
	}

	return SljitRegionBoundaryNode("table scan source filters require DuckDB scan filter pushdown");
}

static SljitRegionNodePlan PlanSljitNativeStateScanSourceNode(const ExecutionRegionNode &node,
                                                              const ExecutionRegionContract &contract,
                                                              bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->native_state_scan_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitRegionBoundaryNode("native state scan source requires a ready state-scan contract");
	}
	if (!ExecutionRegionABIOwnsSource(contract.abi)) {
		return SljitRegionBoundaryNode("native state scan source requires source ownership in the region contract");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionBoundaryNode("native state scan source does not own source-pushed filters");
	}

	string reason = "state scan source contract";
	if (render_diagnostics) {
		reason += ";native-state-scan-contract-status=";
		reason += ExecutionRegionStateContractStatusToString(node.source->native_state_scan_contract.status);
		reason += ";native-state-scan-capability=" + node.source->native_state_scan_contract.required_capability;
		reason += ";native-state-scan-contract-version=" + node.source->native_state_scan_contract.contract_version;
	}
	return SljitNativeSourceNode(std::move(reason), node, ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT,
	                             render_diagnostics);
}

static SljitRegionNodePlan PlanSljitNativeStatefulSourceNode(const ExecutionRegionNode &node,
                                                             const ExecutionRegionContract &contract,
                                                             bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->source_contract.status != ExecutionRegionSourceContractStatus::READY) {
		return SljitRegionBoundaryNode("stateful source requires a ready source contract");
	}
	if (!ExecutionRegionABIOwnsSource(contract.abi)) {
		return SljitRegionBoundaryNode("native stateful source requires source ownership in the region contract");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionBoundaryNode("native stateful source does not own source-pushed filters");
	}

	string reason = "stateful source contract";
	if (render_diagnostics) {
		reason += ";source-contract-status=";
		reason += ExecutionRegionSourceContractStatusToString(node.source->source_contract.status);
		reason += ";source-contract-capability=" + node.source->source_contract.required_capability;
		reason += ";source-contract-version=" + node.source->source_contract.contract_version;
	}
	return SljitNativeSourceNode(std::move(reason), node, ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT,
	                             render_diagnostics);
}

static SljitRegionNodePlan PlanSljitSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                               ExecutionRegionSourceExecutionKind source_execution,
                                               bool render_diagnostics) {
	if (!node.source) {
		return SljitRegionBoundaryNode("source boundary requires typed source IR");
	}
	auto &source_contract = node.source->source_contract;
	if (source_contract.status == ExecutionRegionSourceContractStatus::NONE ||
	    source_contract.required_capability.empty() || source_contract.contract_version.empty() ||
	    (source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED && source_contract.blocker.empty())) {
		return SljitRegionBoundaryNode("source boundary requires source contract IR");
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY &&
	    !node.source->filters.empty()) {
		if (!node.source->table_scan_contract.present) {
			return SljitRegionBoundaryNode("source-pushed filters require typed table scan contract IR");
		}
		auto &table_scan_contract = node.source->table_scan_contract;
		return SljitSourceBoundaryRequiresContract(node, table_scan_contract, true, render_diagnostics);
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    source_contract.status == ExecutionRegionSourceContractStatus::READY) {
		if (node.source->kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			if (node.source->native_state_scan_contract.status == ExecutionRegionStateContractStatus::READY) {
				return PlanSljitNativeStateScanSourceNode(node, contract, render_diagnostics);
			}
			return PlanSljitNativeStatefulSourceNode(node, contract, render_diagnostics);
		}
		return PlanSljitSourceContractNode(node, contract, render_diagnostics);
	}
	if (node.operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN && !node.source->table_scan_contract.present) {
		return SljitRegionBoundaryNode("table scan source boundary requires typed table scan contract IR");
	}
	if (!node.source->filters.empty()) {
		if (!node.source->table_scan_contract.present) {
			return SljitRegionBoundaryNode("source-pushed filters require typed table scan contract IR");
		}
		auto &table_scan_contract = node.source->table_scan_contract;
		auto reason = "source-pushed filters require DuckDB scan source contract ownership;source_execution=" +
		              string(ExecutionRegionSourceExecutionKindToString(node.source->execution));
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, true);
		if (!ExecutionRegionABIOwnsSource(contract.abi)) {
			reason += ";source_contract_ownership_contract=source_required";
			AppendSljitSourceIR(reason, node, render_diagnostics, source_execution);
			return SljitRegionBoundaryNode(std::move(reason));
		}
		return SljitSourceBoundaryRequiresContract(node, table_scan_contract, false, render_diagnostics);
	}
	auto boundary_reason = "DuckDB source boundary;" + node.blocker_reason;
	AppendSljitSourceIR(boundary_reason, node, render_diagnostics,
	                    ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	auto result = SljitRegionBoundaryNode(std::move(boundary_reason));
	result.requires_source_contract = SljitSourceContractIsBlocked(node);
	return result;
}

static string DescribeSljitAggregateSinkInput(const ExecutionRegionAggregateInput &aggregate) {
	string result = "aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_children=";
	result += std::to_string(aggregate.child_count);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_index=";
	result += std::to_string(aggregate.payload_index);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_references=";
	result += aggregate.supported_payload_references ? "ready" : "missing";
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_primitive_update=";
	result += aggregate.primitive_update_ready ? "ready" : "missing";
	if (!aggregate.primitive_update_blocker.empty()) {
		result += ";aggregate";
		result += std::to_string(aggregate.aggregate_index);
		result += "_primitive_update_blocker=";
		result += aggregate.primitive_update_blocker;
	}
	return result;
}

static string ValidateSljitAggregateUpdateSink(const ExecutionRegionSinkInfo &sink) {
	auto &contract = sink.aggregate_contract;
	if (contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockerOrReason(contract.native_state_update_contract.blocker,
		                            "aggregate native state-update contract is not ready");
	}
	if (sink.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    sink.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	    sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return "aggregate update sink kind mismatch";
	}
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, sink.aggregates, sink.groups);
	if (!blocker.empty()) {
		return blocker;
	}
	return string();
}

static SljitRegionNodePlan PlanSljitAggregateUpdateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("aggregate sink is missing native sink IR");
	}
	auto blocker = ValidateSljitAggregateUpdateSink(*node.sink);
	if (!blocker.empty()) {
		return SljitRegionBoundaryNode(std::move(blocker));
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::AGGREGATE_UPDATE;
	native_op.aggregate_update.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.aggregate_update.ir = node.sink->ir;
	}

	auto &contract = node.sink->aggregate_contract;
	string reason = "native aggregate update sink contract";
	if (render_diagnostics) {
		reason += ";requires=aggregate_update_runtime_binding";
		if (!contract.native_state_update_contract.required_capability.empty()) {
			reason += ";requires=" + contract.native_state_update_contract.required_capability;
		}
		reason += ";sink_kind=" + string(ExecutionRegionSinkKindToString(node.sink->kind));
		reason += ";aggregate_operator_kind=" + string(ExecutionRegionAggregateOperatorKindToString(contract.kind));
		reason += ";aggregate_count=" + std::to_string(contract.aggregate_count);
		reason += ";group_count=" + std::to_string(contract.group_count);
		reason += ";payload_type_count=" + std::to_string(contract.payload_type_count);
		for (auto &aggregate : node.sink->aggregates) {
			reason += ";aggregate" + std::to_string(aggregate.aggregate_index) + "_function=" + aggregate.function_name;
			reason += ";";
			reason += DescribeSljitAggregateSinkInput(aggregate);
		}
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitHashAggregateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash aggregate sink is missing native sink IR");
	}
	auto &lookup_contract = node.sink->aggregate_contract.native_hash_lookup_contract;
	if (lookup_contract.status != ExecutionRegionStateContractStatus::READY) {
		auto blocker = SljitBlockerOrReason(lookup_contract.blocker, "hash-aggregate-native-lookup-contract-missing");
		string reason = "hash aggregate update requires generated hash lookup ownership";
		if (render_diagnostics) {
			reason += ";native_hash_aggregate_lookup_blocker=" + blocker;
		}
		AppendSljitReasonPart(reason, node.sink->aggregate_contract.hash_lookup_layout_ir, render_diagnostics);
		AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
		return SljitRegionBoundaryNode(std::move(reason));
	}
	return PlanSljitAggregateUpdateSinkNode(node, render_diagnostics);
}

static SljitRegionNodePlan PlanSljitHashAggregateDistinctSinkNode(const ExecutionRegionNode &node,
                                                                  bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink is missing native sink IR");
	}
	auto &distinct_contract = node.sink->aggregate_contract.native_distinct_state_update_contract;
	if (distinct_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitRegionBoundaryNode("hash aggregate distinct state-update contract missing;blocker=" +
		                               distinct_contract.blocker);
	}
	if (node.sink->groups.empty()) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink has no group bindings");
	}
	for (auto &group : node.sink->groups) {
		if (!group.supported_reference) {
			auto reason = group.reason.empty() ? "hash aggregate distinct group binding unsupported" : group.reason;
			return SljitRegionBoundaryNode("hash aggregate distinct sink group unsupported;group_index=" +
			                               std::to_string(group.group_index) + ";" + reason);
		}
	}
	if (node.sink->aggregates.empty()) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink has no aggregate payload bindings");
	}
	for (auto &aggregate : node.sink->aggregates) {
		if (!aggregate.distinct) {
			return SljitRegionBoundaryNode("hash aggregate distinct sink received non-distinct aggregate");
		}
	}

	string reason = "hash aggregate distinct state-update native lowering requires distinct aggregate contract";
	if (render_diagnostics) {
		reason += ";aggregate-state-update=distinct-contract-boundary";
		reason += ";aggregate_count=" + std::to_string(node.sink->aggregate_contract.aggregate_count);
		reason += ";group_count=" + std::to_string(node.sink->aggregate_contract.group_count);
	}
	AppendSljitReasonPart(reason, node.sink->reason, render_diagnostics);
	return SljitRegionBoundaryNode(std::move(reason));
}

static bool TryGetSljitHashJoinKeyKind(const LogicalType &type, SljitNativeHashJoinKeyKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		kind = SljitNativeHashJoinKeyKind::UINT8;
		return true;
	case PhysicalType::INT8:
		kind = SljitNativeHashJoinKeyKind::INT8;
		return true;
	case PhysicalType::UINT16:
		kind = SljitNativeHashJoinKeyKind::UINT16;
		return true;
	case PhysicalType::INT16:
		kind = SljitNativeHashJoinKeyKind::INT16;
		return true;
	case PhysicalType::UINT32:
		kind = SljitNativeHashJoinKeyKind::UINT32;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeHashJoinKeyKind::INT32;
		return true;
	case PhysicalType::UINT64:
		kind = SljitNativeHashJoinKeyKind::UINT64;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeHashJoinKeyKind::INT64;
		return true;
	case PhysicalType::UINT128:
		kind = SljitNativeHashJoinKeyKind::UINT128;
		return true;
	case PhysicalType::INT128:
		kind = SljitNativeHashJoinKeyKind::INT128;
		return true;
	default:
		return false;
	}
}

static const char *SljitHashJoinKeyKindToString(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return "int8";
	case SljitNativeHashJoinKeyKind::INT16:
		return "int16";
	case SljitNativeHashJoinKeyKind::INT32:
		return "int32";
	case SljitNativeHashJoinKeyKind::INT64:
		return "int64";
	case SljitNativeHashJoinKeyKind::INT128:
		return "int128";
	case SljitNativeHashJoinKeyKind::UINT8:
		return "uint8";
	case SljitNativeHashJoinKeyKind::UINT16:
		return "uint16";
	case SljitNativeHashJoinKeyKind::UINT32:
		return "uint32";
	case SljitNativeHashJoinKeyKind::UINT64:
		return "uint64";
	case SljitNativeHashJoinKeyKind::UINT128:
		return "uint128";
	default:
		return "unknown";
	}
}

static const char *SljitHashJoinProbeOutputModeToString(ExecutionHashJoinProbeOutputMode mode) {
	switch (mode) {
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
		return "matched_probe_and_build";
	case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD:
		return "left_probe_and_build";
	case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
		return "matched_probe_only";
	case ExecutionHashJoinProbeOutputMode::MARK_PROBE:
		return "mark_probe";
	case ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
		return "mark_build_only";
	default:
		return "none";
	}
}

static const char *SljitHashJoinComparisonToString(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return "equal";
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return "notequal";
	case ExecutionRegionComparisonType::LESS_THAN:
		return "lessthan";
	case ExecutionRegionComparisonType::GREATER_THAN:
		return "greaterthan";
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return "lessthanorequalto";
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return "greaterthanorequalto";
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return "not_distinct_from";
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return "distinct_from";
	default:
		return "unsupported";
	}
}

static bool SljitHashJoinEqualityComparisonSupported(ExecutionRegionComparisonType comparison_type) {
	return comparison_type == ExecutionRegionComparisonType::EQUAL ||
	       comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
}

static bool SljitHashJoinMatchPredicateSupported(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::NOT_EQUAL:
	case ExecutionRegionComparisonType::LESS_THAN:
	case ExecutionRegionComparisonType::GREATER_THAN:
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return true;
	default:
		return false;
	}
}

static string DescribeSljitHashJoinProbeKey(idx_t key_idx, const SljitNativeHashJoinProbeKeyPlan &key) {
	string result = key.equality_key ? "key" : "predicate";
	result += std::to_string(key_idx);
	result += "<input_index=" + std::to_string(key.key_input_index);
	result += ",kind=" + string(SljitHashJoinKeyKindToString(key.key_kind));
	result += ",layout_offset=" + std::to_string(key.key_layout_offset);
	result += ",comparison=" + string(SljitHashJoinComparisonToString(key.comparison_type));
	result += key.null_equal ? ",null_equal=true>" : ",null_equal=false>";
	return result;
}

static string DescribeSljitHashJoinProbeKeys(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                             const char *separator) {
	string result;
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if (key_idx > 0) {
			result += separator;
		}
		result += DescribeSljitHashJoinProbeKey(key_idx, keys[key_idx]);
	}
	return result;
}

static void AppendSljitHashJoinProbeMarkOffsets(string &result, const char *name,
                                                const SljitNativeHashJoinProbePlan &probe) {
	result += ",";
	result += name;
	result += "=true";
	result += ",found_match_offset=" + std::to_string(probe.found_match_offset);
	result += ",pointer_offset=" + std::to_string(probe.pointer_offset);
}

static bool TryGetSljitNestedLoopJoinValueKind(const LogicalType &type, SljitNativeNestedLoopJoinValueKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT32:
		kind = SljitNativeNestedLoopJoinValueKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeNestedLoopJoinValueKind::INT64;
		return true;
	case PhysicalType::INT128:
		kind = SljitNativeNestedLoopJoinValueKind::INT128;
		return true;
	case PhysicalType::DOUBLE:
		kind = SljitNativeNestedLoopJoinValueKind::DOUBLE;
		return true;
	default:
		return false;
	}
}

static const char *SljitNestedLoopJoinValueKindToString(SljitNativeNestedLoopJoinValueKind kind) {
	switch (kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return "int32";
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return "int64";
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return "int128";
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return "double";
	default:
		return "unknown";
	}
}

static SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                              const vector<LogicalType> &input_types,
                                                              bool render_diagnostics) {
	if (!node.operator_info) {
		return SljitRegionBoundaryNode("hash join probe operator is missing typed operator IR");
	}
	auto &contract = node.operator_info->hash_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires hash join contract IR");
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_probe_contract.blocker,
		                                    "hash join probe native contract is not ready");
	}
	if (!contract.native_probe_shape_ready) {
		return SljitBlockedContractBoundary(contract.native_probe_shape_blocker,
		                                    "hash join probe native shape is not ready");
	}
	if (node.operator_info->hash_join_keys.size() != contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering key count does not match contract");
	}
	if (contract.layout_offsets.size() < contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires hash table key layout offsets");
	}
	if (contract.equality_condition_count == 0 || contract.equality_condition_count > contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires an equality-key prefix");
	}

	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	keys.reserve(node.operator_info->hash_join_keys.size());
	for (idx_t key_idx = 0; key_idx < node.operator_info->hash_join_keys.size(); key_idx++) {
		auto &key = node.operator_info->hash_join_keys[key_idx];
		auto comparison_type = contract.comparison_types[key_idx];
		auto equality_key = key_idx < contract.equality_condition_count;
		if (equality_key && !SljitHashJoinEqualityComparisonSupported(comparison_type)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported equality comparison " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!equality_key && !SljitHashJoinMatchPredicateSupported(comparison_type)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported match predicate " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!key.supported_reference) {
			return SljitRegionBoundaryNode(
			    key.reason.empty() ? "hash join probe native lowering requires supported reference keys" : key.reason);
		}
		if (key.input_index >= input_types.size()) {
			return SljitRegionBoundaryNode("hash join probe native lowering key input index is outside operator input");
		}
		SljitNativeHashJoinKeyKind key_kind;
		if (!TryGetSljitHashJoinKeyKind(key.type, key_kind)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported key type " +
			                               key.type.ToString());
		}
		if (!equality_key &&
		    (key_kind == SljitNativeHashJoinKeyKind::INT128 || key_kind == SljitNativeHashJoinKeyKind::UINT128)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported 128-bit match predicate " +
			                               key.type.ToString());
		}
		SljitNativeHashJoinProbeKeyPlan key_plan;
		key_plan.key_input_index = key.input_index;
		key_plan.key_layout_offset = contract.layout_offsets[key_idx];
		key_plan.key_type = key.type;
		key_plan.key_kind = key_kind;
		key_plan.comparison_type = comparison_type;
		key_plan.equality_key = equality_key;
		key_plan.null_equal = equality_key && comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
		keys.push_back(std::move(key_plan));
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	native_op.operator_index = node.operator_index;
	native_op.output_types = node.output_types;
	native_op.hash_join_probe.operator_index = node.operator_index;
	native_op.hash_join_probe.input_types = input_types;
	native_op.hash_join_probe.keys = std::move(keys);
	native_op.hash_join_probe.equality_key_count = contract.equality_condition_count;
	native_op.hash_join_probe.found_match_offset = contract.tuple_size;
	native_op.hash_join_probe.pointer_offset = contract.pointer_offset;
	native_op.hash_join_probe.output_mode = contract.native_probe_output_mode;
	native_op.hash_join_probe.operator_info = *node.operator_info;
	if (contract.perfect_hash_probe_shape_ready) {
		if (native_op.hash_join_probe.keys.size() != 1 || native_op.hash_join_probe.equality_key_count != 1) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires one equality key");
		}
		auto perfect_key_kind = native_op.hash_join_probe.keys[0].key_kind;
		if (perfect_key_kind == SljitNativeHashJoinKeyKind::INT128 ||
		    perfect_key_kind == SljitNativeHashJoinKeyKind::UINT128) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering does not support 128-bit keys");
		}
		if (native_op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		    native_op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires inner output mode");
		}
		native_op.hash_join_probe.perfect_hash_probe = true;
	}
	if (contract.residual_predicate || contract.residual_info) {
		if (contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
			return SljitRegionBoundaryNode(
			    "hash join probe native lowering has unsupported residual mark-build-only contract");
		}
		if (!contract.residual_expression_ready || !contract.residual_expression.root) {
			return SljitBlockedContractBoundary(
			    contract.residual_expression_blocker,
			    "hash join probe native lowering requires lowered residual predicate IR");
		}
		SljitNativeRegionExpressionPlan residual_filter;
		if (!TryReadNativeRegionExpression(*contract.residual_expression.root, true, residual_filter)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported residual predicate");
		}
		native_op.hash_join_probe.residual_predicate = true;
		native_op.hash_join_probe.residual_filter = std::move(residual_filter);
		native_op.hash_join_probe.residual_source_types.resize(contract.residual_sources.size());
		for (auto &source : contract.residual_sources) {
			if (source.source_index >= native_op.hash_join_probe.residual_source_types.size()) {
				return SljitRegionBoundaryNode("hash join probe native lowering residual source index is out of range");
			}
			if (source.kind == ExecutionHashJoinResidualSourceKind::PROBE && source.input_index >= input_types.size()) {
				return SljitRegionBoundaryNode(
				    "hash join probe native lowering residual probe source is outside operator input");
			}
			native_op.hash_join_probe.residual_source_types[source.source_index] = source.type;
		}
	}
	auto mark_build_match = ExecutionRegionJoinTypePropagatesBuildSide(contract.join_type);
	native_op.hash_join_probe.mark_build_match = mark_build_match && !native_op.hash_join_probe.residual_predicate;
	native_op.hash_join_probe.mark_build_match_after_residual =
	    mark_build_match && native_op.hash_join_probe.residual_predicate;
	if (render_diagnostics) {
		native_op.hash_join_probe.ir =
		    "hash_join_probe_native<hash_keys=" + std::to_string(native_op.hash_join_probe.equality_key_count) +
		    ",conditions=";
		native_op.hash_join_probe.ir += DescribeSljitHashJoinProbeKeys(native_op.hash_join_probe.keys, "|");
		native_op.hash_join_probe.ir += ",probe_shape=native";
		if (native_op.hash_join_probe.perfect_hash_probe) {
			native_op.hash_join_probe.ir += ",perfect_hash_probe_shape=native";
		} else {
			native_op.hash_join_probe.ir +=
			    ",perfect_hash_probe_shape=" + (contract.perfect_hash_probe_shape_blocker.empty()
			                                        ? string("not_applicable")
			                                        : contract.perfect_hash_probe_shape_blocker);
		}
		native_op.hash_join_probe.ir +=
		    ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(native_op.hash_join_probe.output_mode));
		if (native_op.hash_join_probe.mark_build_match) {
			AppendSljitHashJoinProbeMarkOffsets(native_op.hash_join_probe.ir, "mark_build_match",
			                                    native_op.hash_join_probe);
		}
		if (native_op.hash_join_probe.mark_build_match_after_residual) {
			AppendSljitHashJoinProbeMarkOffsets(native_op.hash_join_probe.ir, "mark_build_match_after_residual",
			                                    native_op.hash_join_probe);
		}
		if (native_op.hash_join_probe.residual_predicate) {
			native_op.hash_join_probe.ir += ",residual_predicate=true";
			native_op.hash_join_probe.ir +=
			    ",residual_sources=" + std::to_string(native_op.hash_join_probe.residual_source_types.size());
			native_op.hash_join_probe.ir += ",residual_ir=(" + native_op.hash_join_probe.residual_filter.ir + ")";
		}
		native_op.hash_join_probe.ir += ">";
	}

	string reason = "generated native hash join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_hash_join_table_layout;"
		          "native-hash-join-probe-executable=ready;native_probe_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                                    const vector<LogicalType> &input_types,
                                                                    bool render_diagnostics) {
	if (!node.operator_info) {
		return SljitRegionBoundaryNode("nested loop join probe operator is missing typed operator IR");
	}
	auto &contract = node.operator_info->nested_loop_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering requires nested loop join contract IR");
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_probe_contract.blocker,
		                                    "nested loop join probe native contract is not ready");
	}
	if (!contract.native_probe_shape_ready) {
		return SljitBlockedContractBoundary(contract.native_probe_shape_blocker,
		                                    "nested loop join probe native shape is not ready");
	}
	if (contract.join_type != ExecutionRegionJoinType::INNER) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering supports inner joins only;join_type=" +
		                               string(ExecutionRegionJoinTypeToString(contract.join_type)));
	}
	if (!contract.complex_join || contract.simple_join) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering requires complex join contract");
	}
	if (!contract.conditions_ready) {
		return SljitBlockedContractBoundary(
		    contract.condition_blocker, "nested loop join probe native lowering requires ready condition expressions");
	}
	if (contract.conditions.size() != 1 || contract.condition_types.size() != 1 ||
	    contract.comparison_types.size() != 1) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering currently requires one condition");
	}
	if (input_types.size() != contract.lhs_input_types.size()) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering input width does not match contract");
	}

	auto &condition = contract.conditions[0];
	if (!condition.lhs_expression_ready || !condition.lhs_expression.root) {
		return SljitBlockedContractBoundary(condition.lhs_expression_blocker,
		                                    "nested loop join probe native lowering requires lowered LHS condition IR");
	}
	if (condition.comparison_type != contract.comparison_types[0]) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering condition comparison mismatch");
	}
	SljitNativeNestedLoopJoinValueKind value_kind;
	if (!TryGetSljitNestedLoopJoinValueKind(condition.type, value_kind)) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering has unsupported condition type " +
		                               condition.type.ToString());
	}
	SljitNativeRegionExpressionPlan lhs_condition;
	string error;
	if (!TryLowerNativeRegionExpression(condition.lhs_expression, false, lhs_condition, error, render_diagnostics)) {
		string reason = "nested loop join probe native lowering has unsupported LHS condition expression";
		if (!error.empty()) {
			reason += ";" + error;
		}
		return SljitRegionBoundaryNode(std::move(reason));
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE;
	native_op.operator_index = node.operator_index;
	native_op.output_types = node.output_types;
	native_op.nested_loop_join_probe.operator_index = node.operator_index;
	native_op.nested_loop_join_probe.input_types = input_types;
	native_op.nested_loop_join_probe.condition_types = contract.condition_types;
	native_op.nested_loop_join_probe.join_type = contract.join_type;
	native_op.nested_loop_join_probe.operator_info = *node.operator_info;

	SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
	condition_plan.lhs_condition = std::move(lhs_condition);
	condition_plan.type = condition.type;
	condition_plan.comparison_type = condition.comparison_type;
	condition_plan.value_kind = value_kind;
	if (render_diagnostics) {
		condition_plan.ir = "condition0<kind=" + string(SljitNestedLoopJoinValueKindToString(value_kind)) +
		                    ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type)) +
		                    ",lhs=(" + condition_plan.lhs_condition.ir + ")>";
	}
	native_op.nested_loop_join_probe.conditions.push_back(std::move(condition_plan));
	if (render_diagnostics) {
		native_op.nested_loop_join_probe.ir =
		    "nested_loop_join_probe_native<join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type)) +
		    ",conditions=1,value_kind=" + string(SljitNestedLoopJoinValueKindToString(value_kind)) +
		    ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type)) + ">";
	}

	string reason = "generated native nested loop join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_nested_loop_join_probe_cursor;"
		          "requires=primitive_compare_stub;native_probe_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitHashJoinSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash join sink is missing native sink IR");
	}
	auto &contract = node.sink->hash_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("hash join build native lowering requires hash join contract IR");
	}
	if (contract.native_build_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_build_contract.blocker,
		                                    "hash join build native contract is not ready");
	}
	if (!contract.build_sink_shape_ready) {
		return SljitBlockedContractBoundary(contract.build_sink_shape_blocker,
		                                    "hash join build native sink shape is not ready");
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::HASH_JOIN_BUILD;
	native_op.hash_join_build.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.hash_join_build.ir = "hash_join_build_native<execution=primitive-protocol-build,payload_columns=" +
		                               std::to_string(contract.payload_column_count) +
		                               ",keys=" + std::to_string(contract.condition_count) + ">";
	}

	string reason = "native hash join build contract";
	if (render_diagnostics) {
		reason += ";requires=hash_join_build_runtime_binding;requires=hash_join_build_prepare;"
		          "requires=hash_join_build_hash;requires=hash_join_build_append";
		reason += ";native_hash_join_build_contract_status=";
		reason += ExecutionRegionStateContractStatusToString(contract.native_build_contract.status);
		if (!contract.native_build_contract.blocker.empty()) {
			reason += ";native_hash_join_build_blocker=" + contract.native_build_contract.blocker;
		}
		reason += ";build_sink_shape_ready=true";
	}
	AppendSljitReasonPart(reason, native_op.hash_join_build.ir, render_diagnostics);
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitNestedLoopJoinSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("nested loop join sink is missing native sink IR");
	}
	auto &contract = node.sink->nested_loop_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("nested loop join build native lowering requires nested loop join contract IR");
	}
	if (contract.native_build_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_build_contract.blocker,
		                                    "nested loop join build native contract is not ready");
	}
	if (!contract.build_sink_shape_ready) {
		return SljitBlockedContractBoundary(contract.build_sink_shape_blocker,
		                                    "nested loop join build native sink shape is not ready");
	}
	if (contract.join_type != ExecutionRegionJoinType::INNER) {
		return SljitRegionBoundaryNode("nested loop join build native lowering supports inner joins only;join_type=" +
		                               string(ExecutionRegionJoinTypeToString(contract.join_type)));
	}
	if (!contract.complex_join || contract.simple_join) {
		return SljitRegionBoundaryNode("nested loop join build native lowering requires complex join contract");
	}
	if (contract.filter_pushdown) {
		return SljitRegionBoundaryNode("nested loop join build native lowering does not support filter pushdown state");
	}
	if (!contract.conditions_ready) {
		return SljitBlockedContractBoundary(
		    contract.condition_blocker, "nested loop join build native lowering requires ready condition expressions");
	}
	if (contract.conditions.size() != contract.condition_types.size()) {
		return SljitRegionBoundaryNode("nested loop join build native lowering condition count mismatch");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD;
	native_op.nested_loop_join_build.sink_info = *node.sink;
	native_op.nested_loop_join_build.condition_types = contract.condition_types;
	if (render_diagnostics) {
		native_op.nested_loop_join_build.ir = node.sink->ir;
	}
	native_op.nested_loop_join_build.rhs_conditions.reserve(contract.conditions.size());
	for (auto &condition : contract.conditions) {
		if (!condition.rhs_expression_ready || !condition.rhs_expression.root) {
			return SljitBlockedContractBoundary(
			    condition.rhs_expression_blocker,
			    "nested loop join build native lowering requires lowered RHS condition IR");
		}
		SljitNativeRegionExpressionPlan rhs_condition;
		string error;
		if (!TryLowerNativeRegionExpression(condition.rhs_expression, false, rhs_condition, error,
		                                    render_diagnostics)) {
			string reason = "nested loop join build native lowering has unsupported RHS condition expression";
			if (!error.empty()) {
				reason += ";" + error;
			}
			return SljitRegionBoundaryNode(std::move(reason));
		}
		native_op.nested_loop_join_build.rhs_conditions.push_back(std::move(rhs_condition));
	}

	string reason = "native nested loop join build sink contract";
	if (render_diagnostics) {
		reason += ";requires=native_sink_runtime_binding;requires=native_nested_loop_join_build_sink;"
		          "build_sink_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static void AppendSljitRequiredCapability(string &reason, const string &capability) {
	if (!capability.empty()) {
		reason += ";requires=" + capability;
	}
}

static string BuildSljitNativeSinkReason(const char *label, const char *runtime_binding,
                                         const ExecutionRegionSinkInfo &sink,
                                         const ExecutionRegionNativeOperatorContract &contract,
                                         bool render_diagnostics) {
	string reason = string(label);
	if (render_diagnostics) {
		reason += ";requires=";
		reason += runtime_binding;
		AppendSljitRequiredCapability(reason, contract.required_capability);
		reason += ";sink_kind=" + string(ExecutionRegionSinkKindToString(sink.kind));
	}
	return reason;
}

static void AppendSljitSinkIR(string &reason, const ExecutionRegionSinkInfo &sink, bool render_diagnostics) {
	AppendSljitReasonPart(reason, sink.ir, render_diagnostics);
}

static SljitRegionNodePlan PlanSljitSimpleNativeSinkNode(const ExecutionRegionNode &node,
                                                         SljitNativeRegionOpKind native_kind,
                                                         const char *contract_label, const char *runtime_binding,
                                                         const char *blocked_reason, bool render_diagnostics) {
	auto &contract = node.sink->native_sink_contract;
	if (contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.blocker, blocked_reason);
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = native_kind;
	switch (native_kind) {
	case SljitNativeRegionOpKind::APPEND_SINK:
		native_op.append_sink.sink_info = *node.sink;
		if (render_diagnostics) {
			native_op.append_sink.ir = node.sink->ir;
		}
		break;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		native_op.delim_join_sink.sink_info = *node.sink;
		if (render_diagnostics) {
			native_op.delim_join_sink.ir = node.sink->ir;
		}
		break;
	default:
		throw InternalException("Unsupported simple SLJIT native sink kind");
	}

	auto reason = BuildSljitNativeSinkReason(contract_label, runtime_binding, *node.sink, contract, render_diagnostics);
	AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitOrderSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	auto &contract = node.sink->native_sink_contract;
	auto &order_contract = node.sink->order_contract;
	if (contract.status != ExecutionRegionStateContractStatus::READY) {
		auto reason = SljitBlockerOrReason(contract.blocker, "ordered-sink-contract-not-ready");
		if (order_contract.present) {
			reason += ";ordered-sink-contract-present";
			reason += order_contract.all_order_keys_ready ? ";order_keys_ready=true" : ";order_keys_ready=false";
			if (!order_contract.order_key_blocker.empty()) {
				reason += ";order_key_blocker=" + order_contract.order_key_blocker;
			}
		}
		return SljitRegionBoundaryNode(reason);
	}
	if (!order_contract.present) {
		return SljitRegionBoundaryNode("ordered-sink-contract-missing");
	}
	if (!order_contract.all_order_keys_ready) {
		return SljitBlockedContractBoundary(order_contract.order_key_blocker, "ordered-sink-order-keys-not-ready");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::ORDER_SINK;
	native_op.order_sink.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.order_sink.ir = node.sink->ir;
	}
	native_op.order_sink.order_keys.reserve(order_contract.order_keys.size());
	native_op.order_sink.key_types.reserve(order_contract.order_keys.size());
	for (auto &key : order_contract.order_keys) {
		SljitNativeRegionExpressionPlan key_plan;
		string error;
		if (!TryLowerNativeRegionExpression(key.expression, false, key_plan, error, render_diagnostics)) {
			string reason = "ordered-sink-order-key-native-lowering-unsupported";
			if (!error.empty()) {
				reason += ";" + error;
			}
			if (!key.reason.empty()) {
				reason += ";order_key_blocker=" + key.reason;
			}
			return SljitRegionBoundaryNode(std::move(reason));
		}
		native_op.order_sink.key_types.push_back(key.type);
		native_op.order_sink.order_keys.push_back(std::move(key_plan));
	}

	auto reason = BuildSljitNativeSinkReason("ordered sink contract", "ordered_sink_runtime_binding", *node.sink,
	                                         contract, render_diagnostics);
	if (render_diagnostics) {
		reason += ";operator_kind=" + string(ExecutionRegionOperatorKindToString(order_contract.kind));
		reason += ";order_keys=" + std::to_string(native_op.order_sink.order_keys.size());
	}
	AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static void SetSljitNativeSinkInputTypes(SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
	op.output_types = input_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		op.hash_join_build.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		op.nested_loop_join_build.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::ORDER_SINK:
		op.order_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::APPEND_SINK:
		op.append_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		op.delim_join_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		op.aggregate_update.input_types = input_types;
		break;
	default:
		break;
	}
}

static SljitRegionNodePlan SljitFullPipelineSinkBoundary(const ExecutionRegionNode &node,
                                                         const SljitRegionNodePlan &native_sink, string reason,
                                                         const char *lowering_reason_key, bool render_diagnostics) {
	if (render_diagnostics && !native_sink.reason.empty()) {
		reason += ";";
		reason += lowering_reason_key;
		reason += "=";
		reason += native_sink.reason;
	}
	if (node.sink) {
		AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	}
	if (!node.blocker_reason.empty()) {
		reason += ";boundary=" + node.blocker_reason;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

static SljitRegionNodePlan PlanSljitSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitNodeBlockerBoundary(node, "sink node is missing native sink IR");
	}
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return PlanSljitHashJoinSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return PlanSljitNestedLoopJoinSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return PlanSljitHashAggregateSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::HASH_AGGREGATE_DISTINCT_SINK:
		return PlanSljitHashAggregateDistinctSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return PlanSljitAggregateUpdateSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return PlanSljitSimpleNativeSinkNode(node, SljitNativeRegionOpKind::APPEND_SINK, "append sink contract",
		                                     "append_sink_runtime_binding", "append sink contract is not ready",
		                                     render_diagnostics);
	case ExecutionRegionSinkKind::SORT:
		return PlanSljitOrderSinkNode(node, render_diagnostics);
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return PlanSljitSimpleNativeSinkNode(node, SljitNativeRegionOpKind::DELIM_JOIN_SINK,
		                                     "delimiter join sink contract", "delim_join_sink_runtime_binding",
		                                     "delim join sink contract is not ready", render_diagnostics);
	default:
		return SljitNodeBlockerBoundary(node, "sink kind is outside SLJIT native sink lowering");
	}
}

static SljitRegionNodePlan PlanSljitFullPipelineSinkNode(const ExecutionRegionNode &node,
                                                         const vector<LogicalType> &input_types,
                                                         bool render_diagnostics) {
	if (!node.sink) {
		return SljitNodeBlockerBoundary(node, "full pipeline sink node is missing native sink IR");
	}

	auto native_sink = PlanSljitSinkNode(node, render_diagnostics);
	if (native_sink.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(native_sink)) {
		auto &native_op = SljitRegionNodeLastNativeOp(native_sink);
		SetSljitNativeSinkInputTypes(native_op, input_types);
		if (render_diagnostics) {
			native_sink.reason += ";full-pipeline-native-sink";
		}
		return native_sink;
	}
	if (node.sink->kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		return SljitFullPipelineSinkBoundary(
		    node, native_sink, "full pipeline hash join build sink requires native hash build primitive lowering",
		    "hash-join-build-contract-boundary", render_diagnostics);
	}
	if (node.sink->kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD) {
		return SljitFullPipelineSinkBoundary(
		    node, native_sink,
		    "full pipeline nested loop join build sink requires native nested loop join build lowering",
		    "nested-loop-join-build-contract-boundary", render_diagnostics);
	}
	return SljitFullPipelineSinkBoundary(node, native_sink, "full pipeline sink requires native sink contract",
	                                     "native-sink-lowering", render_diagnostics);
}

static SljitRegionNodePlan PlanSljitRegionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                               string &error, bool render_diagnostics) {
	switch (node.kind) {
	case ExecutionRegionNodeKind::FILTER:
		return PlanSljitFilterNode(node, error, render_diagnostics);
	case ExecutionRegionNodeKind::PROJECTION:
		return PlanSljitProjectionNode(node, input_types, error, render_diagnostics);
	case ExecutionRegionNodeKind::SOURCE:
		return SljitRegionBoundaryNode(SljitSourceBoundaryReason(node, render_diagnostics));
	case ExecutionRegionNodeKind::SINK: {
		auto sink = PlanSljitSinkNode(node, render_diagnostics);
		if (sink.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(sink)) {
			auto &native_op = SljitRegionNodeLastNativeOp(sink);
			SetSljitNativeSinkInputTypes(native_op, input_types);
		}
		return sink;
	}
	case ExecutionRegionNodeKind::OPERATOR:
		if (node.operator_info && node.operator_info->kind == ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
			return PlanSljitHashJoinProbeOperatorNode(node, input_types, render_diagnostics);
		}
		if (node.operator_info &&
		    node.operator_info->kind == ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE) {
			return PlanSljitNestedLoopJoinProbeOperatorNode(node, input_types, render_diagnostics);
		}
		return SljitNodeBlockerBoundary(node, "operator contract boundary has no SLJIT native contract");
	default:
		return SljitNodeBlockerBoundary(node, "region IR node is outside SLJIT native region lowering");
	}
}

static bool SljitRejectsSinkRegionContext(const ExecutionRegionNode &node, const ExecutionRegionCandidate &candidate) {
	return node.kind == ExecutionRegionNodeKind::SINK && candidate.context_traits.operator_missing_count > 0;
}

static bool SljitCanExecuteSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract) {
	if (!ExecutionRegionABIOwnsSource(contract.abi) || !node.source) {
		return false;
	}
	if (node.source->source_contract.status == ExecutionRegionSourceContractStatus::READY ||
	    node.source->native_state_scan_contract.status == ExecutionRegionStateContractStatus::READY) {
		return true;
	}
	return node.boundary == ExecutionRegionBoundaryKind::SCAN;
}

static void AddSljitLoweredNode(ExecutionRegionLoweringPlan &lowering_plan, const ExecutionRegionNode &node,
                                const SljitRegionNodePlan &node_plan) {
	lowering_plan.AddNode(node.label, node.operator_name, node.operator_kind, node_plan.kind, node_plan.reason);
}

static bool SljitNodePlanIsBoundary(const SljitRegionNodePlan &node_plan) {
	return node_plan.kind == ExecutionRegionLoweringKind::BOUNDARY;
}

static bool SljitNativeContractReady(bool present, const ExecutionRegionNativeOperatorContract &native_contract) {
	return present && native_contract.status == ExecutionRegionStateContractStatus::READY;
}

static void AddSljitContractBlocker(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                    bool contract_ready, const char *ready_error, const char *missing_error,
                                    const char *ready_blocker, const char *missing_blocker,
                                    const string &ready_reason = string()) {
	backend_error = contract_ready ? ready_error : missing_error;
	if (!contract_ready) {
		lowering_plan.AddFusionBlocker(missing_blocker);
		return;
	}
	string blocker = ready_blocker;
	if (!ready_reason.empty()) {
		blocker += ";";
		blocker += ready_reason;
	}
	lowering_plan.AddFusionBlocker(std::move(blocker));
}

static void AddSljitFullPipelineSinkBlockers(ExecutionRegionLoweringPlan &lowering_plan,
                                             string &backend_error, const ExecutionRegionNode &node,
                                             const SljitRegionNodePlan &node_plan,
                                             const ExecutionRegionContract &contract) {
	if (!SljitNodePlanIsBoundary(node_plan) || !ExecutionRegionABIIsFullPipeline(contract.abi) || !node.sink) {
		return;
	}
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(node.sink->hash_join_contract.present,
		                                                     node.sink->hash_join_contract.native_build_contract);
		AddSljitContractBlocker(
		    lowering_plan, backend_error, build_contract_ready,
		    "SLJIT full-pipeline hash join build sink rejected by native hash join build lowering",
		    "SLJIT full-pipeline hash join build sink requires a native hash join build contract",
		    "sink-contract-blocker:hash-join-build-native-lowering",
		    "sink-contract-blocker:hash-join-build-contract-missing");
		break;
	}
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(
		    node.sink->nested_loop_join_contract.present, node.sink->nested_loop_join_contract.native_build_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, build_contract_ready,
		                        "SLJIT full-pipeline nested loop join build sink rejected by native lowering",
		                        "SLJIT full-pipeline nested loop join build sink requires a native build contract",
		                        "sink-contract-blocker:nested-loop-join-build-native-lowering-missing",
		                        "sink-contract-blocker:nested-loop-join-build-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

static void AddSljitOperatorContractBlockers(ExecutionRegionLoweringPlan &lowering_plan,
                                             string &backend_error, const ExecutionRegionNode &node,
                                             const SljitRegionNodePlan &node_plan) {
	if (!SljitNodePlanIsBoundary(node_plan) || !node.operator_info) {
		return;
	}
	switch (node.operator_info->kind) {
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->hash_join_contract.present,
		                             node.operator_info->hash_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT hash join probe rejected by native hash join lowering",
		                        "SLJIT hash join probe requires a native hash join probe contract",
		                        "operator-contract-blocker:hash-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:hash-join-probe-contract-missing", node_plan.reason);
		break;
	}
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->nested_loop_join_contract.present,
		                             node.operator_info->nested_loop_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT nested loop join probe rejected by native lowering",
		                        "SLJIT nested loop join probe requires a native probe contract",
		                        "operator-contract-blocker:nested-loop-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:nested-loop-join-probe-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

struct SljitRegionLoweringCursor {
	SljitRegionLoweringCursor(vector<LogicalType> input_types, SljitNativeRegionPlan &native_region)
	    : current_types(std::move(input_types)), native_region(native_region) {
	}

	const vector<LogicalType> &InputTypes() const {
		return current_types;
	}

	bool CanFuse() const {
		return can_fuse;
	}

	ExecutionRegionSourceExecutionKind SelectedSourceExecution() const {
		return selected_source_execution;
	}

	bool UsesScanFilters() const {
		return uses_scan_filters;
	}

	void BreakAtBoundary(const vector<LogicalType> &boundary_output_types) {
		can_fuse = false;
		current_types = boundary_output_types;
	}

	void AcceptSource(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		auto source_output_types = SljitRegionNodeHasNativeOps(node_plan)
		                               ? SljitRegionNodeLastNativeOp(node_plan).output_types
		                               : node.output_types;
		if (node_plan.source_execution != ExecutionRegionSourceExecutionKind::NONE) {
			selected_source_execution = node_plan.source_execution;
		}
		uses_scan_filters = uses_scan_filters || node_plan.uses_scan_filters;
		AppendIfFusing(node_plan);
		current_types = std::move(source_output_types);
	}

	void AcceptSink(SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		AppendIfFusing(node_plan);
	}

	void AcceptOperator(const ExecutionRegionNode &node, SljitRegionNodePlan &node_plan) {
		D_ASSERT(node_plan.kind == ExecutionRegionLoweringKind::NATIVE);
		D_ASSERT(SljitRegionNodeHasNativeOps(node_plan));
		if (current_types.empty()) {
			current_types = node.output_types;
		}
		if (can_fuse && SljitRegionNodeHasSingleNativeOp(node_plan) &&
		    IsIdentityProjection(SljitRegionNodeFirstNativeOp(node_plan), current_types)) {
			return;
		}
		auto output_types = SljitRegionNodeLastNativeOp(node_plan).output_types;
		AppendIfFusing(node_plan);
		current_types = std::move(output_types);
	}

private:
	void AppendIfFusing(SljitRegionNodePlan &node_plan) {
		if (can_fuse) {
			AppendSljitRegionNodeNativeOps(native_region, node_plan);
		}
	}

	vector<LogicalType> current_types;
	SljitNativeRegionPlan &native_region;
	bool can_fuse = true;
	ExecutionRegionSourceExecutionKind selected_source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;
};

ExecutionRegionLoweringPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate, bool render_diagnostics) {
	ExecutionRegionLoweringPlan lowering_plan;
	string backend_error;
	lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::UNSUPPORTED);
	if (candidate.stage_plan.HasStages()) {
		lowering_plan.SetOperatorStageIR(candidate.stage_plan.ir);
	}
	SljitNativeRegionPlan native_region;
	SljitRegionLoweringCursor cursor(candidate.input_types, native_region);
	if (candidate.EndNode() > region_ir.nodes.size()) {
		backend_error = "SLJIT region candidate references nodes outside the region IR";
		return lowering_plan;
	}
	auto &contract = candidate.contract;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == ExecutionRegionNodeKind::SOURCE) {
			auto executable_source = SljitCanExecuteSourceNode(node, contract);
			auto source_execution =
			    candidate.source_execution != ExecutionRegionSourceExecutionKind::NONE
			        ? candidate.source_execution
			        : (node.source ? node.source->execution : ExecutionRegionSourceExecutionKind::NONE);
			auto node_plan =
			    executable_source ? PlanSljitSourceNode(node, contract, source_execution, render_diagnostics)
			                      : PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
			const bool source_requires_native = executable_source &&
			                                    node_plan.kind == ExecutionRegionLoweringKind::BOUNDARY &&
			                                    node_plan.requires_source_contract;
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			if (source_requires_native) {
				lowering_plan.AddFusionBlocker(
				    "source-contract-blocker:requires-source-contract;source_execution=duckdb-source-boundary");
			}
			if (executable_source && node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY) {
				auto selected_source_execution = node_plan.source_execution != ExecutionRegionSourceExecutionKind::NONE
				                                     ? node_plan.source_execution
				                                     : node.source->execution;
				lowering_plan.SetSelectedSourceExecution(selected_source_execution);
			}
			if (executable_source && node_plan.kind == ExecutionRegionLoweringKind::NATIVE) {
				cursor.AcceptSource(node, node_plan);
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		if (node.kind == ExecutionRegionNodeKind::SINK) {
			auto node_plan =
			    ExecutionRegionABIIsFullPipeline(contract.abi)
			        ? PlanSljitFullPipelineSinkNode(node, cursor.InputTypes(), render_diagnostics)
			    : SljitRejectsSinkRegionContext(node, candidate)
			        ? SljitRegionBoundaryNode("sink region requires upstream operators with native contracts")
			        : PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
			AddSljitLoweredNode(lowering_plan, node, node_plan);
			AddSljitFullPipelineSinkBlockers(lowering_plan, backend_error, node, node_plan, contract);
			if (node_plan.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(node_plan)) {
				cursor.AcceptSink(node_plan);
			} else {
				cursor.BreakAtBoundary(node.output_types);
			}
			continue;
		}
		SljitRegionNodePlan node_plan;
		if (node.kind == ExecutionRegionNodeKind::OPERATOR &&
		    node.boundary == ExecutionRegionBoundaryKind::OPERATOR_NATIVE &&
		    !ExecutionRegionABIIsFullPipeline(contract.abi)) {
			node_plan = SljitRegionBoundaryNode("native operator contract requires full-pipeline region ABI");
		} else if (node.kind == ExecutionRegionNodeKind::OPERATOR &&
		           node.boundary == ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY &&
		           !ExecutionRegionABIIsFullPipeline(contract.abi)) {
			node_plan =
			    SljitRegionBoundaryNode("native operator contract boundary requires full-pipeline region ownership");
		} else {
			node_plan = PlanSljitRegionNode(node, cursor.InputTypes(), backend_error, render_diagnostics);
		}
		AddSljitLoweredNode(lowering_plan, node, node_plan);
		AddSljitOperatorContractBlockers(lowering_plan, backend_error, node, node_plan);
		if (node_plan.kind != ExecutionRegionLoweringKind::NATIVE || !SljitRegionNodeHasNativeOps(node_plan)) {
			cursor.BreakAtBoundary(node.output_types);
			continue;
		}
		cursor.AcceptOperator(node, node_plan);
	}
	if (cursor.CanFuse() && !native_region.ops.empty()) {
		native_region.source_execution = cursor.SelectedSourceExecution();
		FuseAdjacentNativeProjections(native_region, render_diagnostics);
		FusePrimitiveAggregateUpdates(native_region, candidate.input_types, render_diagnostics);
		FinalizeSljitNativeRegionPlan(native_region);
		lowering_plan.SetUsesScanFilters(cursor.UsesScanFilters());
		string codegen_blocker;
		if (SljitNativeRegionHasExecutableBodyGap(native_region, codegen_blocker)) {
			backend_error = codegen_blocker;
			auto fusion_blocker = SljitNativeRegionCodegenFusionBlocker() + ";" + codegen_blocker;
			if (!candidate.contract.ir.empty()) {
				fusion_blocker += ";" + candidate.contract.ir;
			}
			lowering_plan.AddFusionBlocker(std::move(fusion_blocker));
			return lowering_plan;
		}
		if (SljitRegionIsFullyFused(native_region, contract)) {
			lowering_plan.SetFullyFused(true);
			auto backend_plan = make_shared_ptr<SljitRegionBackendPlan>();
			backend_plan->error = std::move(backend_error);
			backend_plan->native_region = make_uniq<SljitNativeRegionPlan>(std::move(native_region));
			lowering_plan.backend_plan = std::move(backend_plan);
			lowering_plan.SetCompiledExecutionMode(ExecutionRegionExecutionMode::NATIVE);
		}
		if (!lowering_plan.IsFullyFused()) {
			if (contract.source_boundary_count > 0) {
				lowering_plan.AddFusionBlocker("candidate-contract-blocker:source-boundary;" + contract.ir);
			}
			if (contract.missing_contract_count > 0) {
				lowering_plan.AddFusionBlocker("candidate-contract-blocker:missing-contract;" + contract.ir);
			}
		}
	}
	return lowering_plan;
}

static string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr) {
	string result;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		result = "native:reference";
		switch (expr.reference_origin) {
		case SljitNativeReferenceOrigin::REGION_INPUT:
			result += ":region-input";
			break;
		case SljitNativeReferenceOrigin::PROJECTION_PASS_THROUGH:
			result += ":projection-pass";
			break;
		case SljitNativeReferenceOrigin::PROJECTION_TEMP:
			result += ":projection-temp";
			break;
		case SljitNativeReferenceOrigin::SOURCE_OUTPUT:
			result += ":source-output";
			break;
		default:
			result += ":unknown";
			break;
		}
		break;
	case SljitNativeRegionExpressionKind::CONSTANT:
		result = "native:constant<" + expr.return_type.ToString() + ">(" + expr.constant_value.ToString() + ")";
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		result = NativeIntegerBinaryReason(expr.integer_kind, expr.binary_op);
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		result = NativeIntegerBinaryReferenceReason(expr.integer_kind, expr.binary_op);
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

static string DescribeNativeRegionExpressionList(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	string result;
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (expr_idx > 0) {
			result += ",";
		}
		result += DescribeNativeRegionExpression(expressions[expr_idx]);
	}
	return result;
}

static string
DescribeNativeNestedLoopJoinProbeConditions(const vector<SljitNativeNestedLoopJoinProbeConditionPlan> &conditions) {
	string result;
	for (idx_t condition_idx = 0; condition_idx < conditions.size(); condition_idx++) {
		if (condition_idx > 0) {
			result += ",";
		}
		auto &condition = conditions[condition_idx];
		result += "condition" + std::to_string(condition_idx);
		result += "<kind=" + string(SljitNestedLoopJoinValueKindToString(condition.value_kind));
		result += ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type));
		result += ",lhs=" + DescribeNativeRegionExpression(condition.lhs_condition) + ">";
	}
	return result;
}

string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode) {
	string result = "sljit.region " + mode;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		result += ";op" + std::to_string(op_idx) + "=";
		switch (op.kind) {
		case SljitNativeRegionOpKind::FILTER:
			result += "filter(" + DescribeNativeRegionExpression(op.filter) + ")";
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			result += "projection(" + DescribeNativeRegionExpressionList(op.projections) + ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			result +=
			    "hash_join_probe(hash_keys=" + std::to_string(op.hash_join_probe.equality_key_count) + ",conditions=";
			result += DescribeSljitHashJoinProbeKeys(op.hash_join_probe.keys, ",");
			if (op.hash_join_probe.mark_build_match) {
				AppendSljitHashJoinProbeMarkOffsets(result, "mark_build_match", op.hash_join_probe);
			}
			if (op.hash_join_probe.mark_build_match_after_residual) {
				AppendSljitHashJoinProbeMarkOffsets(result, "mark_build_match_after_residual", op.hash_join_probe);
			}
			if (op.hash_join_probe.perfect_hash_probe) {
				result += ",perfect_hash_probe_shape=native";
			}
			result += ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(op.hash_join_probe.output_mode));
			if (op.hash_join_probe.residual_predicate) {
				result += ",residual_predicate=true";
				result += ",residual=" + DescribeNativeRegionExpression(op.hash_join_probe.residual_filter);
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			result += "hash_join_build(keys=";
			result += std::to_string(op.hash_join_build.sink_info.hash_join_keys.size());
			result += ";payload_columns=" + std::to_string(op.hash_join_build.input_types.size());
			result += ";execution=primitive-protocol-build";
			result += ")";
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			result += "nested_loop_join_probe(conditions=" +
			          DescribeNativeNestedLoopJoinProbeConditions(op.nested_loop_join_probe.conditions);
			result += ")";
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			result += "nested_loop_join_build(conditions=" +
			          DescribeNativeRegionExpressionList(op.nested_loop_join_build.rhs_conditions);
			result += ";payload_columns=" + std::to_string(op.nested_loop_join_build.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::APPEND_SINK:
			result += "append_sink(columns=";
			result += std::to_string(op.append_sink.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			result += "delim_join_sink(columns=";
			result += std::to_string(op.delim_join_sink.input_types.size());
			result += ")";
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			result += "aggregate_update(kind=";
			result += string(
			    ExecutionRegionAggregateOperatorKindToString(op.aggregate_update.sink_info.aggregate_contract.kind));
			result += ";columns=" + std::to_string(op.aggregate_update.input_types.size());
			result += ";groups=" + std::to_string(op.aggregate_update.sink_info.groups.size());
			result += ";aggregates=" + std::to_string(op.aggregate_update.sink_info.aggregates.size());
			if (op.aggregate_update.use_primitive_payloads) {
				result += ";payload_update=generated-primitive";
				result += ";primitive_payloads=" + DescribeNativeRegionExpressionList(op.aggregate_update.payloads);
				if (op.aggregate_update.use_perfect_hash_group_lookup) {
					result += ";grouped_state_lookup=generated-perfect-hash";
				}
			} else {
				result += ";execution=vectorized-operator-boundary";
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			result += "ordered_sink(keys=" + DescribeNativeRegionExpressionList(op.order_sink.order_keys);
			result += ";payload_columns=" + std::to_string(op.order_sink.input_types.size());
			result += ";operator_kind=" +
			          string(ExecutionRegionOperatorKindToString(op.order_sink.sink_info.order_contract.kind));
			result += ")";
			break;
		default:
			result += "unknown";
			break;
		}
	}
	return result;
}

string DescribeNativeRegionShape(const SljitNativeRegionPlan &region) {
	return BuildSljitNativeRegionShape(region);
}

} // namespace duckdb

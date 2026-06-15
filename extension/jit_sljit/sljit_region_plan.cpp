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

#include "sljit_native_plan.hpp"
#include "sljit_native_util.hpp"

namespace duckdb {

static constexpr const char *SLJIT_NATIVE_LOWERING_NOT_IMPLEMENTED =
    "sljit native lowering is not implemented for this region IR node";

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
	result->constant_on_left = input->constant_on_left;
	result->integer_kind = input->integer_kind;
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
	result->child = CopySljitNativePredicate(input->child);
	result->children.reserve(input->children.size());
	for (auto &child : input->children) {
		result->children.push_back(CopySljitNativePredicate(child));
	}
	return result;
}

SljitNativeRegionExpressionPlan
CopySljitNativeRegionExpression(const SljitNativeRegionExpressionPlan &input) {
	SljitNativeRegionExpressionPlan result;
	result.kind = input.kind;
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
	result.compare_op = input.compare_op;
	result.cast_source_width = input.cast_source_width;
	result.cast_target_width = input.cast_target_width;
	result.unsigned_source_width = input.unsigned_source_width;
	result.unsigned_cast_target_width = input.unsigned_cast_target_width;
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
	result.constant_or_null = input.constant_or_null;
	result.ir = input.ir;
	return result;
}

static SljitNativeRegionOpPlan CopySljitNativeRegionOp(const SljitNativeRegionOpPlan &input) {
	SljitNativeRegionOpPlan result;
	result.kind = input.kind;
	result.operator_index = input.operator_index;
	result.output_types = input.output_types;
	result.filter = CopySljitNativeRegionExpression(input.filter);
	result.hash_join_probe = input.hash_join_probe;
	result.hash_join_build = input.hash_join_build;
	result.projections.reserve(input.projections.size());
	for (auto &projection : input.projections) {
		result.projections.push_back(CopySljitNativeRegionExpression(projection));
	}
	result.aggregate_payloads = input.aggregate_payloads;
	result.native_ungrouped_aggregate_updates = input.native_ungrouped_aggregate_updates;
	result.native_grouped_aggregate_updates = input.native_grouped_aggregate_updates;
	result.grouped_aggregate_payloads = input.grouped_aggregate_payloads;
	result.grouped_aggregate_groups = input.grouped_aggregate_groups;
	result.perfect_hash_required_bits = input.perfect_hash_required_bits;
	result.perfect_hash_group_minima = input.perfect_hash_group_minima;
	return result;
}

unique_ptr<SljitNativeRegionPlan> CopySljitNativeRegion(const SljitNativeRegionPlan &input) {
	auto result = make_uniq<SljitNativeRegionPlan>();
	result->elided_identity_projections = input.elided_identity_projections;
	result->fused_projection_chains = input.fused_projection_chains;
	result->fused_arithmetic_projection_chains = input.fused_arithmetic_projection_chains;
	result->runtime_combined_filter_projections = input.runtime_combined_filter_projections;
	result->source_filter_count = input.source_filter_count;
	result->source_filter_execution = input.source_filter_execution;
	result->native_source = input.native_source;
	result->ops.reserve(input.ops.size());
	for (auto &op : input.ops) {
		result->ops.push_back(CopySljitNativeRegionOp(op));
	}
	return result;
}

static bool IsNativeIdentityProjection(const SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
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
	return expr.kind != SljitNativeRegionExpressionKind::REFERENCE;
}

static bool SljitNativeRegionOpGeneratesCode(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.filter);
	case SljitNativeRegionOpKind::PROJECTION:
		for (auto &projection : op.projections) {
			if (SljitNativeRegionExpressionGeneratesCode(projection)) {
				return true;
			}
		}
		return false;
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return true;
	case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
	case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return !op.native_grouped_aggregate_updates.empty();
	case SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE:
		return !op.native_ungrouped_aggregate_updates.empty();
	default:
		return false;
	}
}

static bool SljitNativeRegionGeneratesCode(const SljitNativeRegionPlan &region) {
	for (auto &op : region.ops) {
		if (SljitNativeRegionOpGeneratesCode(op)) {
			return true;
		}
	}
	return false;
}

static string SljitNativeRegionOpCodegenBlocker(const SljitNativeRegionOpPlan &op) {
	return string();
}

static bool SljitNativeRegionHasCodegenGap(const SljitNativeRegionPlan &region, string &blocker) {
	for (auto &op : region.ops) {
		auto op_blocker = SljitNativeRegionOpCodegenBlocker(op);
		if (!op_blocker.empty()) {
			blocker = std::move(op_blocker);
			return true;
		}
	}
	return false;
}

static string SljitNativeRegionCodegenFusionBlocker(const SljitNativeRegionPlan &region) {
	for (auto &op : region.ops) {
	}
	return "operator-fusion-gap:native-operator-codegen-missing";
}

static bool SljitNativeRegionOpIsTransform(const SljitNativeRegionOpPlan &op) {
	return op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::PROJECTION ||
	       op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE;
}

static bool SljitNativeRegionOpIsNativeUngroupedSink(const SljitNativeRegionOpPlan &op) {
	return op.kind == SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE &&
	       !op.native_ungrouped_aggregate_updates.empty();
}

static bool SljitNativeRegionOpIsNativeGroupedSink(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
	case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return !op.native_grouped_aggregate_updates.empty();
	default:
		return false;
	}
}

static bool SljitNativeRegionOpIsNativeHashJoinBuildSink(const SljitNativeRegionOpPlan &op) {
	return op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD &&
	       op.hash_join_build.sink_info.kind == JitRegionSinkKind::HASH_JOIN_BUILD;
}

static bool SljitNativeRegionOpIsNativeSink(const SljitNativeRegionOpPlan &op) {
	return SljitNativeRegionOpIsNativeUngroupedSink(op) || SljitNativeRegionOpIsNativeGroupedSink(op) ||
	       SljitNativeRegionOpIsNativeHashJoinBuildSink(op);
}

static bool SljitNativeRegionOpIsNativeProtocolSinkStage(const SljitNativeRegionOpPlan &op) {
	return SljitNativeRegionOpIsNativeSink(op) && !SljitNativeRegionOpGeneratesCode(op);
}

static bool SljitNativeRegionHasGenericExecutableLoop(const SljitNativeRegionPlan &region,
                                                      const JitRegionContract &contract) {
	if (region.ops.empty()) {
		return false;
	}
	const bool owns_sink = JitRegionABIOwnsSink(contract.abi);
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		if (SljitNativeRegionOpIsTransform(op)) {
			continue;
		}
		if (SljitNativeRegionOpIsNativeSink(op)) {
			return owns_sink && op_idx + 1 == region.ops.size();
		}
		return false;
	}
	return !owns_sink;
}

static JitRegionExecutionForm ClassifySljitRegionExecutionForm(const SljitNativeRegionPlan &region,
                                                               const JitRegionContract &contract,
                                                               const JitRegionStagePlan &core_stage_plan) {
	if (!contract.native_fusion_ready) {
		return JitRegionExecutionForm::NONE;
	}
	auto stage_region = BuildSljitOperatorStageRegionPlan(region, contract, core_stage_plan);
	if (stage_region.HasExecutableBody()) {
		return JitRegionExecutionForm::FUSED;
	}
	return JitRegionExecutionForm::NONE;
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
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		return TryMapNativeProjectionSourceIndex(input_projection, predicate.source_index);
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
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
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
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
		return expr.predicate && TryMapNativePredicateSourcesThroughProjection(input_projection, *expr.predicate);
	default:
		return false;
	}
}

static bool TryComposeNativeProjectionExpression(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                                 const SljitNativeRegionExpressionPlan &expr,
                                                 SljitNativeRegionExpressionPlan &result,
                                                 bool &composed_arithmetic) {
	if (expr.kind == SljitNativeRegionExpressionKind::CONSTANT) {
		result = CopySljitNativeRegionExpression(expr);
		result.ir = "compose-constant(" + expr.ir + ")";
		composed_arithmetic = false;
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
		result.ir = "compose-reference(" + source.ir + ")";
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
		result.ir = "compose-add-reference(" + source.ir + "," + expr.ir + ")";
		composed_arithmetic = true;
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
	result.ir = "compose-add-constant(" + source.ir + "," + expr.ir + ")";
	composed_arithmetic = true;
	return true;
}

static bool TryComposeNativeProjectionThroughReferenceProjection(
    const vector<SljitNativeRegionExpressionPlan> &input_projection, const SljitNativeRegionExpressionPlan &expr,
    SljitNativeRegionExpressionPlan &result, bool &composed_arithmetic) {
	result = CopySljitNativeRegionExpression(expr);
	if (!TryMapNativeProjectionExpressionSources(input_projection, result)) {
		return false;
	}
	result.ir = "compose-reference-projection(" + expr.ir + ")";
	composed_arithmetic = SljitNativeRegionExpressionGeneratesCode(result);
	return true;
}

static bool TryFuseAdjacentNativeProjection(SljitNativeRegionOpPlan &left, const SljitNativeRegionOpPlan &right,
                                            bool &fused_arithmetic) {
	if (left.kind != SljitNativeRegionOpKind::PROJECTION || right.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}
	vector<SljitNativeRegionExpressionPlan> projections;
	projections.reserve(right.projections.size());
	for (auto &projection : right.projections) {
		SljitNativeRegionExpressionPlan composed;
		bool composed_arithmetic = false;
		if (!TryComposeNativeProjectionExpression(left.projections, projection, composed, composed_arithmetic) &&
		    !TryComposeNativeProjectionThroughReferenceProjection(left.projections, projection, composed,
		                                                         composed_arithmetic)) {
			return false;
		}
		fused_arithmetic = fused_arithmetic || composed_arithmetic;
		projections.push_back(std::move(composed));
	}
	left.projections = std::move(projections);
	left.output_types = right.output_types;
	return true;
}

static void FuseAdjacentNativeProjections(SljitNativeRegionPlan &region) {
	if (region.ops.size() < 2) {
		return;
	}
	idx_t op_idx = 0;
	while (op_idx + 1 < region.ops.size()) {
		bool fused_arithmetic = false;
		if (TryFuseAdjacentNativeProjection(region.ops[op_idx], region.ops[op_idx + 1], fused_arithmetic)) {
			region.ops.erase(region.ops.begin() + NumericCast<int64_t>(op_idx + 1));
			region.fused_projection_chains++;
			if (fused_arithmetic) {
				region.fused_arithmetic_projection_chains++;
			}
			continue;
		}
		op_idx++;
	}
}

static void MarkRuntimeCombinedFilterProjections(SljitNativeRegionPlan &region) {
	region.runtime_combined_filter_projections = 0;
	if (region.ops.size() < 2) {
		return;
	}
	for (idx_t op_idx = 0; op_idx + 1 < region.ops.size(); op_idx++) {
		if (region.ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
		    region.ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION) {
			region.runtime_combined_filter_projections++;
		}
	}
}

static string SljitRegionCandidateContext(const JitRegionContract &contract) {
	if (JitRegionABIIsSourcePipeline(contract.abi)) {
		return "source-prefix";
	}
	if (JitRegionABIIsChunkTransform(contract.abi)) {
		return "post-source";
	}
	if (JitRegionABIIsSinkPipeline(contract.abi)) {
		return "sink";
	}
	if (JitRegionABIIsFullPipeline(contract.abi)) {
		return "full-pipeline";
	}
	return "unknown";
}

string BuildSljitRegionCandidateShapeKey(const JitRegionCandidate &candidate) {
	string result = "sljit:" + candidate.signature.context + ":" + candidate.signature.shape;
	if (!candidate.signature.feature_shape.empty()) {
		result += ":";
		result += candidate.signature.feature_shape;
	}
	return result;
}

string BuildSljitRegionCandidateContextShapeKey(const JitRegionCandidate &candidate, const string &shape_key) {
	if (candidate.signature.context_feature_shape.empty()) {
		return shape_key;
	}
	return shape_key + ":context:" + candidate.signature.context_feature_shape;
}

static string BuildSljitRegionShapeKey(const SljitNativeRegionPlan &region, const JitRegionContract &contract) {
	const auto context = SljitRegionCandidateContext(contract);
	auto shape = DescribeNativeRegionShape(region);
	if (region.fused_arithmetic_projection_chains > 0 && shape == "projection") {
		return "sljit:" + context + ":projection-chain";
	}
	return "sljit:" + context + ":" + shape;
}

static bool TryReadNativeRegionExpression(const JitExpressionIR &root, bool require_boolean,
                                            SljitNativeRegionExpressionPlan &expr) {
	if (!require_boolean && root.kind == JitExpressionIRKind::CONSTANT) {
		expr.kind = SljitNativeRegionExpressionKind::CONSTANT;
		expr.return_type = root.return_type;
		expr.constant_value = root.constant;
		return true;
	}

	if (!require_boolean && root.kind == JitExpressionIRKind::REFERENCE) {
		expr.kind = SljitNativeRegionExpressionKind::REFERENCE;
		expr.return_type = root.return_type;
		expr.source_index = root.ref_index;
		return true;
	}

	SljitNativeIntegerKind in_list_kind;
	idx_t in_list_source_index;
	vector<int64_t> in_list_constants;
	bool list_has_null;
	bool not_in;
	if (TryReadNativeIntegerInList(root, in_list_kind, in_list_source_index, in_list_constants, list_has_null, not_in)) {
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
		return false;
	}

	if (root.kind == JitExpressionIRKind::INTRINSIC &&
	    root.intrinsic == JitExpressionIntrinsicKind::STRING_COMPRESS &&
	    root.return_type.id() == LogicalTypeId::UTINYINT && root.children.size() == 1 && root.children[0] &&
	    root.children[0]->kind == JitExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::VARCHAR) {
		expr.kind = SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
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
	if (root.kind == JitExpressionIRKind::INTRINSIC && root.intrinsic == JitExpressionIntrinsicKind::DATE_YEAR &&
	    root.return_type.id() == LogicalTypeId::BIGINT && root.children.size() == 1 && root.children[0] &&
	    root.children[0]->kind == JitExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::DATE) {
		expr.kind = SljitNativeRegionExpressionKind::DATE_YEAR;
		expr.integer_kind = SljitNativeIntegerKind::INT64;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		return true;
	}

	SljitNativeDoubleBinaryOp double_binary_op;
	double double_constant;
	if (TryReadNativeDoubleBinaryReferences(root, double_binary_op, source_index, right_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES;
		expr.double_binary_op = double_binary_op;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		return true;
	}
	if (TryReadNativeDoubleBinaryConstant(root, double_binary_op, source_index, double_constant, constant_on_left)) {
		expr.kind = SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT;
		expr.double_binary_op = double_binary_op;
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
		expr.try_cast = try_cast;
		return true;
	}
	if (TryReadNativeIntegerCast(root, cast_source_width, cast_target_width, source_index, try_cast)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = cast_source_width;
		expr.cast_target_width = cast_target_width;
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
		return true;
	}
	if (TryReadNativeIntegerBinaryReferences(root, binary_op, integer_kind, source_index, right_source_index)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		expr.integer_kind = integer_kind;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.right_source_index = right_source_index;
		expr.binary_op = binary_op;
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
		return true;
	}
	return false;
}

static bool TryLowerNativeRegionExpression(const JitExpressionFragment &fragment, bool require_boolean,
                                             SljitNativeRegionExpressionPlan &expr, string &error) {
	if (!fragment.root || !TryReadNativeRegionExpression(*fragment.root, require_boolean, expr)) {
		return false;
	}
	expr.ir = fragment.ir;
	return true;
}

static unique_ptr<JitExpressionIR> MakeSljitReferenceExpression(idx_t source_index, const LogicalType &type) {
	auto result = make_uniq<JitExpressionIR>();
	result->kind = JitExpressionIRKind::REFERENCE;
	result->return_type = type;
	result->physical_type = type.InternalType();
	result->validity = JitExpressionValidityKind::SOURCE;
	result->source = JitExpressionSourceKind::VECTOR;
	result->exception_behavior = JitExpressionExceptionKind::NONE;
	result->ref_index = source_index;
	return result;
}

static SljitNativeRegionExpressionPlan MakeSljitNativeReference(idx_t source_index, const LogicalType &type,
                                                                string ir) {
	SljitNativeRegionExpressionPlan result;
	result.kind = SljitNativeRegionExpressionKind::REFERENCE;
	result.return_type = type;
	result.source_index = source_index;
	result.ir = std::move(ir);
	return result;
}

struct SljitProjectionGraphLowering {
	explicit SljitProjectionGraphLowering(const vector<LogicalType> &input_types_p) : current_types(input_types_p) {
	}

	vector<LogicalType> current_types;
	vector<SljitNativeRegionOpPlan> native_ops;
};

static string SljitTempExpressionIr(const JitExpressionIR &node, idx_t temp_index) {
	return "ssa.temp#" + std::to_string(temp_index) + ":" + JitExpressionIRKindToString(node.kind) + "<" +
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
		temp_op.output_types.push_back(graph.current_types[col_idx]);
		temp_op.projections.push_back(MakeSljitNativeReference(col_idx, graph.current_types[col_idx],
		                                                       "ssa.pass#" + std::to_string(col_idx)));
	}
	temp_op.output_types.push_back(temp_type);
	temp_op.projections.push_back(std::move(expression));
	graph.current_types.push_back(std::move(temp_type));
	graph.native_ops.push_back(std::move(temp_op));
}

static bool TryBuildSljitProjectionGraphExpression(const JitExpressionIR &root,
                                                   SljitProjectionGraphLowering &graph,
                                                   SljitNativeRegionExpressionPlan &expression);

static bool TryBuildSljitProjectionGraphOperand(const JitExpressionIR &node,
                                                SljitProjectionGraphLowering &graph,
                                                unique_ptr<JitExpressionIR> &operand) {
	if (node.kind == JitExpressionIRKind::REFERENCE || node.kind == JitExpressionIRKind::CONSTANT) {
		operand = node.Copy();
		return true;
	}

	SljitNativeRegionExpressionPlan temp_expression;
	if (!TryBuildSljitProjectionGraphExpression(node, graph, temp_expression)) {
		return false;
	}
	auto temp_index = graph.current_types.size();
	auto temp_type = temp_expression.return_type;
	if (temp_expression.ir.empty()) {
		temp_expression.ir = SljitTempExpressionIr(node, temp_index);
	}
	AppendSljitNativeTempProjection(graph, std::move(temp_expression));
	operand = MakeSljitReferenceExpression(temp_index, temp_type);
	return true;
}

static bool RewriteSljitProjectionGraphOperands(JitExpressionIR &rewritten,
                                                SljitProjectionGraphLowering &graph) {
	if (rewritten.left &&
	    !TryBuildSljitProjectionGraphOperand(*rewritten.left, graph, rewritten.left)) {
		return false;
	}
	if (rewritten.right &&
	    !TryBuildSljitProjectionGraphOperand(*rewritten.right, graph, rewritten.right)) {
		return false;
	}
	if (rewritten.else_node &&
	    !TryBuildSljitProjectionGraphOperand(*rewritten.else_node, graph, rewritten.else_node)) {
		return false;
	}
	for (auto &child : rewritten.children) {
		if (!TryBuildSljitProjectionGraphOperand(*child, graph, child)) {
			return false;
		}
	}
	return true;
}

static bool TryBuildSljitProjectionGraphExpression(const JitExpressionIR &root,
                                                   SljitProjectionGraphLowering &graph,
                                                   SljitNativeRegionExpressionPlan &expression) {
	if (TryReadNativeRegionExpression(root, false, expression)) {
		return true;
	}

	auto rewritten = root.Copy();
	if (!RewriteSljitProjectionGraphOperands(*rewritten, graph)) {
		return false;
	}
	return TryReadNativeRegionExpression(*rewritten, false, expression);
}

struct SljitRegionNodePlan {
	JitLoweringKind kind = JitLoweringKind::FALLBACK;
	string reason;
	unique_ptr<SljitNativeRegionOpPlan> native_op;
	vector<SljitNativeRegionOpPlan> native_ops;
	bool owns_source_filters = false;
	bool requires_native_source = false;
	idx_t source_filter_count = 0;
	SljitSourceFilterExecutionKind source_filter_execution = SljitSourceFilterExecutionKind::NONE;
};

static bool SljitRegionNodeHasNativeOps(const SljitRegionNodePlan &node_plan) {
	return node_plan.native_op || !node_plan.native_ops.empty();
}

static const SljitNativeRegionOpPlan &SljitRegionNodeFirstNativeOp(const SljitRegionNodePlan &node_plan) {
	if (node_plan.native_op) {
		return *node_plan.native_op;
	}
	D_ASSERT(!node_plan.native_ops.empty());
	return node_plan.native_ops[0];
}

static const SljitNativeRegionOpPlan &SljitRegionNodeLastNativeOp(const SljitRegionNodePlan &node_plan) {
	if (!node_plan.native_ops.empty()) {
		return node_plan.native_ops.back();
	}
	D_ASSERT(node_plan.native_op);
	return *node_plan.native_op;
}

static bool SljitRegionNodeHasSingleNativeOp(const SljitRegionNodePlan &node_plan) {
	return node_plan.native_op ? node_plan.native_ops.empty() : node_plan.native_ops.size() == 1;
}

static void AppendSljitRegionNodeNativeOps(SljitNativeRegionPlan &region, SljitRegionNodePlan &node_plan) {
	if (node_plan.native_op) {
		region.ops.push_back(std::move(*node_plan.native_op));
	}
	for (auto &op : node_plan.native_ops) {
		region.ops.push_back(std::move(op));
	}
}

static SljitRegionNodePlan SljitRegionFallbackNode(string reason) {
	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::FALLBACK;
	result.reason = std::move(reason);
	return result;
}

static SljitRegionNodePlan PlanSljitFilterNode(const JitRegionIRNode &node, string &error) {
	if (!node.fallback_reason.empty()) {
		return SljitRegionFallbackNode(node.fallback_reason);
	}
	if (!node.filter) {
		return SljitRegionFallbackNode("filter expression unsupported by SLJIT IR lowering");
	}

	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::FILTER;
	native_op->output_types = node.output_types;
	if (!TryLowerNativeRegionExpression(*node.filter, true, native_op->filter, error)) {
		return SljitRegionFallbackNode(SLJIT_NATIVE_LOWERING_NOT_IMPLEMENTED);
	}

	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::NATIVE;
	result.reason = "generated typed predicate filter";
	result.native_op = std::move(native_op);
	return result;
}

static bool TryPlanDirectSljitProjection(const JitRegionIRNode &node, unique_ptr<SljitNativeRegionOpPlan> &native_op,
                                         bool &generates_code, string &error) {
	native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::PROJECTION;
	native_op->output_types = node.output_types;
	generates_code = false;
	for (auto &expression : node.projections) {
		SljitNativeRegionExpressionPlan native_expression;
		if (!TryLowerNativeRegionExpression(*expression, false, native_expression, error)) {
			return false;
		}
		generates_code = generates_code || SljitNativeRegionExpressionGeneratesCode(native_expression);
		native_op->projections.push_back(std::move(native_expression));
	}
	return true;
}

static bool TryPlanExpandedSljitProjection(const JitRegionIRNode &node, const vector<LogicalType> &input_types,
                                           vector<SljitNativeRegionOpPlan> &native_ops, string &error) {
	if (input_types.empty() && !node.output_types.empty()) {
		error = "projection expression graph lowering requires input types";
		return false;
	}

	SljitProjectionGraphLowering graph(input_types);
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
		projection.ir = fragment->ir;
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

static SljitRegionNodePlan PlanSljitProjectionNode(const JitRegionIRNode &node,
                                                   const vector<LogicalType> &input_types, string &error) {
	if (!node.fallback_reason.empty()) {
		return SljitRegionFallbackNode(node.fallback_reason);
	}
	if (node.projections.empty()) {
		return SljitRegionFallbackNode("projection has no lowered JIT IR expressions");
	}

	unique_ptr<SljitNativeRegionOpPlan> native_op;
	bool generates_code = false;
	if (TryPlanDirectSljitProjection(node, native_op, generates_code, error)) {
		SljitRegionNodePlan result;
		result.kind = generates_code ? JitLoweringKind::NATIVE : JitLoweringKind::PASS_THROUGH;
		result.reason = generates_code ? "generated typed projection" : "typed reference projection pass-through";
		result.native_op = std::move(native_op);
		return result;
	}

	vector<SljitNativeRegionOpPlan> native_ops;
	if (!TryPlanExpandedSljitProjection(node, input_types, native_ops, error)) {
		return SljitRegionFallbackNode(SLJIT_NATIVE_LOWERING_NOT_IMPLEMENTED);
	}
	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::NATIVE;
	result.reason = "generated typed projection expression graph";
	result.native_ops = std::move(native_ops);
	return result;
}

static string SljitSourceIR(const JitRegionIRNode &node,
                            JitRegionSourceExecutionKind execution = JitRegionSourceExecutionKind::NONE) {
	if (!node.source) {
		return string();
	}
	return DescribeJitRegionSourceInfo(*node.source, execution);
}

static void AppendSljitSourceIR(string &reason, const JitRegionIRNode &node,
                                JitRegionSourceExecutionKind execution = JitRegionSourceExecutionKind::NONE) {
	auto source_ir = SljitSourceIR(node, execution);
	if (!source_ir.empty()) {
		reason += ";";
		reason += source_ir;
	}
}

static string SljitSourceFallbackReason(const JitRegionIRNode &node) {
	string result = node.fallback_reason.empty() ? "source node is outside SLJIT native region lowering"
	                                             : node.fallback_reason;
	AppendSljitSourceIR(result, node);
	return result;
}

static bool RemapSljitSourcePredicate(SljitNativePredicate &predicate, idx_t source_index) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return true;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		if (predicate.source_index != 0) {
			return false;
		}
		predicate.source_index = source_index;
		return true;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
		if (predicate.source_index != 0 || predicate.right_source_index != 0) {
			return false;
		}
		predicate.source_index = source_index;
		predicate.right_source_index = source_index;
		return true;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &guard_source_index : predicate.guard_source_indices) {
			if (guard_source_index != 0) {
				return false;
			}
			guard_source_index = source_index;
		}
		return predicate.child && RemapSljitSourcePredicate(*predicate.child, source_index);
	case SljitNativePredicateKind::NOT:
		return predicate.child && RemapSljitSourcePredicate(*predicate.child, source_index);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (!child || !RemapSljitSourcePredicate(*child, source_index)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool RemapSljitSourceFilterExpression(SljitNativeRegionExpressionPlan &expr, idx_t source_index) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return true;
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		if (expr.source_index != 0) {
			return false;
		}
		expr.source_index = source_index;
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		if (expr.source_index != 0 || expr.right_source_index != 0) {
			return false;
		}
		expr.source_index = source_index;
		expr.right_source_index = source_index;
		return true;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		for (auto &guard_source_index : expr.constant_or_null.guard_source_indices) {
			if (guard_source_index != 0) {
				return false;
			}
			guard_source_index = source_index;
		}
		return true;
	case SljitNativeRegionExpressionKind::PREDICATE:
		return expr.predicate && RemapSljitSourcePredicate(*expr.predicate, source_index);
	default:
		return false;
	}
}

static unique_ptr<SljitNativePredicate>
BuildSljitPredicateFromFilterExpression(SljitNativeRegionExpressionPlan &&expr) {
	if (expr.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		return std::move(expr.predicate);
	}

	auto predicate = make_uniq<SljitNativePredicate>();
	predicate->return_type = expr.return_type;
	predicate->source_index = expr.source_index;
	predicate->right_source_index = expr.right_source_index;
	predicate->constant = expr.constant;
	predicate->constant_on_left = expr.constant_on_left;
	predicate->integer_kind = expr.integer_kind;
	predicate->compare_op = expr.compare_op;
	predicate->constants = std::move(expr.constants);
	predicate->list_has_null = expr.list_has_null;
	predicate->not_in = expr.not_in;
	predicate->lower = expr.lower;
	predicate->upper = expr.upper;
	predicate->lower_inclusive = expr.lower_inclusive;
	predicate->upper_inclusive = expr.upper_inclusive;
	predicate->not_between = expr.not_between;
	predicate->null_check_op = expr.null_check_op;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		predicate->kind = SljitNativePredicateKind::REFERENCE;
		return predicate;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		predicate->kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		return predicate;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		predicate->kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		return predicate;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		predicate->kind = SljitNativePredicateKind::INTEGER_IN_LIST;
		return predicate;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		predicate->kind = SljitNativePredicateKind::INTEGER_BETWEEN;
		return predicate;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		predicate->kind = SljitNativePredicateKind::NULL_CHECK;
		return predicate;
	default:
		return nullptr;
	}
}

static bool TryPlanSljitGeneratedSourceFilters(const JitRegionSourceInfo &source,
                                               SljitNativeRegionExpressionPlan &filter, string &error) {
	auto conjunction = make_uniq<SljitNativePredicate>();
	conjunction->kind = SljitNativePredicateKind::CONJUNCTION;
	conjunction->return_type = LogicalType::BOOLEAN;
	conjunction->conjunction_op = JitExpressionConjunctionOp::AND;
	conjunction->children.reserve(source.filters.size());
	string ir;
	for (auto &source_filter : source.filters) {
		if (!source_filter.expression || !source_filter.expression->root) {
			error = source_filter.reason.empty() ? "source filter has no lowered JIT IR" : source_filter.reason;
			return false;
		}
		SljitNativeRegionExpressionPlan filter_expr;
		if (!TryLowerNativeRegionExpression(*source_filter.expression, true, filter_expr, error)) {
			return false;
		}
		if (!RemapSljitSourceFilterExpression(filter_expr, source_filter.scan_column_index)) {
			error = "source filter references must be local to one scan column before source-region remapping";
			return false;
		}
		auto predicate = BuildSljitPredicateFromFilterExpression(std::move(filter_expr));
		if (!predicate) {
			error = "source filter is not representable as a generated typed predicate";
			return false;
		}
		if (!ir.empty()) {
			ir += ";";
		}
		ir += "source-filter#" + std::to_string(source_filter.filter_index) + ":scan-column#" +
		      std::to_string(source_filter.scan_column_index) + "(" + source_filter.expression->ir + ")";
		conjunction->children.push_back(std::move(predicate));
	}
	filter.kind = SljitNativeRegionExpressionKind::PREDICATE;
	filter.return_type = LogicalType::BOOLEAN;
	filter.predicate = std::move(conjunction);
	filter.ir = std::move(ir);
	return true;
}

static SljitNativeRegionOpPlan BuildSljitSourceOutputProjection(const JitRegionIRNode &node,
                                                                const JitRegionTableScanProtocol &protocol) {
	SljitNativeRegionOpPlan projection;
	projection.kind = SljitNativeRegionOpKind::PROJECTION;
	projection.output_types = node.output_types;
	auto projection_map = protocol.source_prefix_output_projection_map;
	if (projection_map.empty()) {
		projection_map.reserve(node.output_types.size());
		for (idx_t output_idx = 0; output_idx < node.output_types.size(); output_idx++) {
			projection_map.push_back(output_idx);
		}
	}
	if (projection_map.size() != node.output_types.size()) {
		throw InternalException("SLJIT source output projection map does not match source output types");
	}
	projection.projections.reserve(projection_map.size());
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto source_index = projection_map[output_idx];
		if (source_index >= protocol.source_prefix_input_types.size()) {
			throw InternalException("SLJIT source output projection references a source-prefix column outside the "
			                        "typed table scan protocol");
		}
		projection.projections.push_back(
		    MakeSljitNativeReference(source_index, node.output_types[output_idx],
		                             "source-output-reference#" + std::to_string(source_index)));
	}
	return projection;
}

static SljitRegionNodePlan PlanSljitNativeSourceNode(const JitRegionIRNode &node,
                                                     const JitRegionContract &contract) {
	D_ASSERT(node.source);
	auto &protocol = node.source->table_scan_protocol;
	if (!protocol.present) {
		return SljitRegionFallbackNode("native source requires typed table scan protocol IR");
	}
	if (!JitRegionABIOwnsSource(contract.abi)) {
		return SljitRegionFallbackNode("native source requires source ownership in the region contract");
	}

	SljitRegionNodePlan result;
	if (node.source->filters.empty()) {
		result.kind = JitLoweringKind::PASS_THROUGH;
		result.requires_native_source = true;
		result.reason = "DuckDB native table scan source runtime";
		AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE);
		return result;
	}

	if (protocol.source_prefix_filter_split_supported) {
		SljitNativeRegionOpPlan filter_op;
		filter_op.kind = SljitNativeRegionOpKind::FILTER;
		filter_op.output_types = protocol.source_prefix_input_types;
		if (TryPlanSljitGeneratedSourceFilters(*node.source, filter_op.filter, result.reason)) {
			result.kind = JitLoweringKind::NATIVE;
			result.requires_native_source = true;
			result.owns_source_filters = true;
			result.source_filter_count = node.source->filters.size();
			result.source_filter_execution = SljitSourceFilterExecutionKind::GENERATED_REGION;
			result.reason = "generated source-prefix table scan filters";
			result.reason += ";source-strategy=prepared-unfiltered-native-source";
			result.reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
			result.reason += ";source_prefix_filter_prune_required=" +
			                 string(protocol.source_prefix_filter_prune_required ? "true" : "false");
			result.reason += ";source_prefix_filter_split_supported=true";
			AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE);
			result.native_ops.push_back(std::move(filter_op));
			result.native_ops.push_back(BuildSljitSourceOutputProjection(node, protocol));
			return result;
		}
		result.reason = "source-prefix filters are not representable as generated typed predicates;" + result.reason;
	}

	result.kind = JitLoweringKind::PASS_THROUGH;
	result.requires_native_source = true;
	result.source_filter_count = node.source->filters.size();
	result.source_filter_execution = SljitSourceFilterExecutionKind::DUCKDB_SCAN;
	result.reason = "DuckDB native table scan source runtime;source-filters-owned-by-duckdb-scan";
	result.reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
	result.reason += ";source_prefix_filter_prune_required=" +
	                 string(protocol.source_prefix_filter_prune_required ? "true" : "false");
	result.reason += ";source_prefix_filter_split_supported=" +
	                 string(protocol.source_prefix_filter_split_supported ? "true" : "false");
	AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE);
	return result;
}

static SljitRegionNodePlan PlanSljitNativeStateScanSourceNode(const JitRegionIRNode &node,
                                                              const JitRegionContract &contract) {
	D_ASSERT(node.source);
	if (node.source->native_state_scan_contract.status != JitRegionStateContractStatus::READY) {
		return SljitRegionFallbackNode("native state scan source requires a ready state-scan contract");
	}
	if (!JitRegionABIOwnsSource(contract.abi)) {
		return SljitRegionFallbackNode("native state scan source requires source ownership in the region contract");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionFallbackNode("native state scan source does not own source-pushed filters");
	}

	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::PASS_THROUGH;
	result.requires_native_source = true;
	result.reason = "DuckDB native state scan source runtime";
	result.reason += ";native-state-scan-contract=";
	result.reason += JitRegionStateContractStatusToString(node.source->native_state_scan_contract.status);
	result.reason += ";native-state-scan-capability=" + node.source->native_state_scan_contract.required_capability;
	result.reason += ";native-state-scan-protocol=" + node.source->native_state_scan_contract.protocol_version;
	AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE);
	return result;
}

static SljitRegionNodePlan PlanSljitNativeStatefulSourceNode(const JitRegionIRNode &node,
                                                             const JitRegionContract &contract) {
	D_ASSERT(node.source);
	if (node.source->native_source_contract.status != JitRegionNativeSourceStatus::READY) {
		return SljitRegionFallbackNode("native stateful source requires a ready native-source contract");
	}
	if (!JitRegionABIOwnsSource(contract.abi)) {
		return SljitRegionFallbackNode("native stateful source requires source ownership in the region contract");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionFallbackNode("native stateful source does not own source-pushed filters");
	}

	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::PASS_THROUGH;
	result.requires_native_source = true;
	result.reason = "DuckDB native stateful source runtime";
	result.reason += ";native-source-contract=";
	result.reason += JitRegionNativeSourceStatusToString(node.source->native_source_contract.status);
	result.reason += ";native-source-capability=" + node.source->native_source_contract.required_capability;
	result.reason += ";native-source-protocol=" + node.source->native_source_contract.protocol_version;
	AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::NATIVE_SOURCE);
	return result;
}

static SljitRegionNodePlan PlanSljitSourceNode(const JitRegionIRNode &node, const JitRegionContract &contract,
                                               JitRegionSourceExecutionKind source_execution) {
	if (!node.source) {
		return SljitRegionFallbackNode("source boundary requires typed source IR");
	}
	auto &native_contract = node.source->native_source_contract;
	if (native_contract.status == JitRegionNativeSourceStatus::NONE || native_contract.required_capability.empty() ||
	    native_contract.protocol_version.empty() || native_contract.blocker.empty()) {
		return SljitRegionFallbackNode("source boundary requires native-source contract IR");
	}
	if (source_execution == JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY && !node.source->filters.empty()) {
		if (!node.source->table_scan_protocol.present) {
			return SljitRegionFallbackNode("source-pushed filters require typed table scan protocol IR");
		}
		auto &protocol = node.source->table_scan_protocol;
		SljitRegionNodePlan result;
		result.kind = JitLoweringKind::FALLBACK;
		result.reason = "DuckDB source boundary;source-fusion-gap:requires-native-source;"
		                "source_execution=duckdb-source-boundary";
		result.reason += ";source-strategy=duckdb-source-boundary";
		result.reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
		result.reason += ";source_prefix_input_columns=" + std::to_string(protocol.source_prefix_input_column_count);
		result.reason += ";source_prefix_filter_prune_required=" +
		                 string(protocol.source_prefix_filter_prune_required ? "true" : "false");
		result.reason += ";source_prefix_filter_split_supported=" +
		                 string(protocol.source_prefix_filter_split_supported ? "true" : "false");
		AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
		return result;
	}
	if (source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
	    native_contract.status == JitRegionNativeSourceStatus::READY) {
		if (node.source->kind == JitRegionSourceKind::STATEFUL_OPERATOR) {
			if (node.source->native_state_scan_contract.status == JitRegionStateContractStatus::READY) {
				return PlanSljitNativeStateScanSourceNode(node, contract);
			}
			return PlanSljitNativeStatefulSourceNode(node, contract);
		}
		return PlanSljitNativeSourceNode(node, contract);
	}
	if (node.operator_name == "TABLE_SCAN" &&
	    !node.source->table_scan_protocol.present) {
		return SljitRegionFallbackNode("table scan source boundary requires typed table scan protocol IR");
	}
	if (!node.source->filters.empty()) {
		if (!node.source->table_scan_protocol.present) {
			return SljitRegionFallbackNode("source-pushed filters require typed table scan protocol IR");
		}
		auto &protocol = node.source->table_scan_protocol;
		auto reason = "source-pushed filters require source-prefix filter split;source_execution=" +
		              string(JitRegionSourceExecutionKindToString(node.source->execution)) +
		              ";source_filter_count=" + std::to_string(node.source->filters.size()) +
		              ";source_prefix_input_columns=" + std::to_string(protocol.source_prefix_input_column_count) +
		              ";source_prefix_requires_unfiltered_input=" +
		              string(protocol.source_prefix_requires_unfiltered_input ? "true" : "false") +
		              ";source_prefix_filter_prune_required=" +
		              string(protocol.source_prefix_filter_prune_required ? "true" : "false") +
		              ";source_prefix_filter_split_supported=" +
		              string(protocol.source_prefix_filter_split_supported ? "true" : "false");
		if (!JitRegionABIOwnsSource(contract.abi)) {
			reason += ";source_prefix_ownership_contract=source_required";
			AppendSljitSourceIR(reason, node, source_execution);
			return SljitRegionFallbackNode(std::move(reason));
		}
		SljitRegionNodePlan result;
		result.kind = JitLoweringKind::FALLBACK;
		result.reason = "DuckDB source boundary;source-fusion-gap:requires-native-source;"
		                "source_execution=duckdb-source-boundary";
		result.reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
		result.reason += ";source_prefix_input_columns=" + std::to_string(protocol.source_prefix_input_column_count);
		result.reason += ";source_prefix_filter_split_supported=" +
		                 string(protocol.source_prefix_filter_split_supported ? "true" : "false");
		AppendSljitSourceIR(result.reason, node, JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
		return result;
	}
	auto boundary_reason = "DuckDB source boundary;" + node.fallback_reason;
	AppendSljitSourceIR(boundary_reason, node, JitRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	return SljitRegionFallbackNode(std::move(boundary_reason));
}

static string DescribeSljitAggregateNativeUpdateContract(const JitRegionAggregateInput &aggregate) {
	string result = "aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_native_update=";
	result += JitAggregateUpdateKindToString(aggregate.native_update);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_state_type=";
	result += aggregate.state_type.ToString();
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_state_size=";
	result += std::to_string(aggregate.state_size);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_state_optional=";
	result += aggregate.state_is_optional ? "true" : "false";
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_state_value_offset=";
	result += std::to_string(aggregate.state_value_offset);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_state_is_set_offset=";
	result += std::to_string(aggregate.state_is_set_offset);
	return result;
}

static string DescribeSljitIdxList(const vector<idx_t> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += std::to_string(values[value_idx]);
	}
	result += "]";
	return result;
}

static string DescribeSljitGroupedStateLayoutContract(const JitRegionAggregateProtocol &protocol) {
	string result = "native-grouped-state-layout-contract=";
	result += protocol.grouped_state_layout_ready ? "ready" : "missing";
	result += ";native-grouped-state-layout-offsets=" + DescribeSljitIdxList(protocol.grouped_state_offsets);
	result += ";native-grouped-state-layout-payload-sizes=" +
	          DescribeSljitIdxList(protocol.grouped_state_payload_sizes);
	return result;
}

static bool SljitNativeSumStateSupported(const JitRegionAggregateInput &aggregate, string &reason) {
	if (!aggregate.state_is_optional) {
		reason = "SLJIT native sum update requires optional state";
		return false;
	}
	if (aggregate.state_type != LogicalType::BIGINT && aggregate.state_type != LogicalType::HUGEINT) {
		reason = "SLJIT native sum update supports optional BIGINT or HUGEINT state";
		return false;
	}
	if (aggregate.child_count != 1 || aggregate.child_types.size() != 1 ||
	    aggregate.child_types[0].InternalType() != PhysicalType::INT64) {
		reason = "SLJIT native sum update requires one INT64 payload child";
		return false;
	}
	return true;
}

static bool TryPlanSljitNativeUngroupedAggregateUpdate(const JitRegionAggregateInput &aggregate,
                                                       SljitNativeUngroupedAggregateUpdatePlan &update,
                                                       string &reason) {
	if (aggregate.native_update != JitAggregateUpdateKind::COUNT_STAR &&
	    aggregate.native_update != JitAggregateUpdateKind::COUNT &&
	    aggregate.native_update != JitAggregateUpdateKind::SUM) {
		reason = "SLJIT native aggregate update supports count-star/count/sum only";
		return false;
	}
	if (aggregate.native_update == JitAggregateUpdateKind::SUM) {
		if (!SljitNativeSumStateSupported(aggregate, reason)) {
			return false;
		}
	} else if (aggregate.state_type != LogicalType::BIGINT || aggregate.state_is_optional) {
		reason = "SLJIT native count update requires non-optional BIGINT state";
		return false;
	}
	if (aggregate.native_update == JitAggregateUpdateKind::COUNT && aggregate.child_count != 1) {
		reason = "SLJIT native count update requires one payload child";
		return false;
	}
	update.aggregate_index = aggregate.aggregate_index;
	update.payload_index = aggregate.payload_index;
	update.update_kind = aggregate.native_update;
	if (!aggregate.child_types.empty()) {
		update.payload_type = aggregate.child_types[0];
	}
	update.state_type = aggregate.state_type;
	update.state_value_offset = aggregate.state_value_offset;
	update.state_is_set_offset = aggregate.state_is_set_offset;
	update.ir = "native-ungrouped-aggregate-update<aggregate=" + std::to_string(aggregate.aggregate_index) +
	            ",kind=" + JitAggregateUpdateKindToString(aggregate.native_update) +
	            ",payload=" + std::to_string(aggregate.payload_index);
	if (!aggregate.child_types.empty()) {
		update.ir += ",payload_type=" + update.payload_type.ToString();
	}
	update.ir += ",state_type=" + aggregate.state_type.ToString() +
	            ",state_value_offset=" + std::to_string(aggregate.state_value_offset) +
	            ",state_is_set_offset=" + std::to_string(aggregate.state_is_set_offset) + ">";
	return true;
}

static bool TryPlanSljitNativeGroupedAggregateUpdate(const JitRegionAggregateInput &aggregate,
                                                     const JitRegionAggregateProtocol &protocol,
                                                     SljitNativeGroupedAggregateUpdatePlan &update, string &reason) {
	if (!protocol.grouped_state_layout_ready) {
		reason = "SLJIT native grouped aggregate update requires grouped state layout";
		return false;
	}
	if (protocol.grouped_state_offsets.size() <= aggregate.aggregate_index || protocol.grouped_state_offsets.empty()) {
		reason = "SLJIT native grouped aggregate update requires grouped state offset for aggregate";
		return false;
	}
	if (aggregate.native_update != JitAggregateUpdateKind::COUNT_STAR &&
	    aggregate.native_update != JitAggregateUpdateKind::COUNT &&
	    aggregate.native_update != JitAggregateUpdateKind::SUM) {
		reason = "SLJIT native grouped aggregate update supports count-star/count/sum only";
		return false;
	}
	if (aggregate.native_update == JitAggregateUpdateKind::SUM) {
		if (!SljitNativeSumStateSupported(aggregate, reason)) {
			return false;
		}
	} else if (aggregate.state_type != LogicalType::BIGINT || aggregate.state_is_optional) {
		reason = "SLJIT native grouped count update requires non-optional BIGINT state";
		return false;
	}
	if (aggregate.native_update == JitAggregateUpdateKind::COUNT && aggregate.child_count != 1) {
		reason = "SLJIT native grouped count update requires one payload child";
		return false;
	}
	auto base_offset = protocol.grouped_state_offsets[0];
	auto state_offset = protocol.grouped_state_offsets[aggregate.aggregate_index];
	if (state_offset < base_offset) {
		reason = "SLJIT native grouped aggregate state offset is before base aggregate offset";
		return false;
	}
	update.aggregate_index = aggregate.aggregate_index;
	update.payload_index = aggregate.payload_index;
	update.aggregate_state_offset = state_offset - base_offset;
	update.update_kind = aggregate.native_update;
	if (!aggregate.child_types.empty()) {
		update.payload_type = aggregate.child_types[0];
	}
	update.state_type = aggregate.state_type;
	update.state_value_offset = aggregate.state_value_offset;
	update.state_is_set_offset = aggregate.state_is_set_offset;
	update.ir = "native-grouped-aggregate-update<aggregate=" + std::to_string(aggregate.aggregate_index) +
	            ",kind=" + JitAggregateUpdateKindToString(aggregate.native_update) +
	            ",payload=" + std::to_string(aggregate.payload_index) +
	            ",aggregate_state_offset=" + std::to_string(update.aggregate_state_offset);
	if (!aggregate.child_types.empty()) {
		update.ir += ",payload_type=" + update.payload_type.ToString();
	}
	update.ir += ",state_type=" + aggregate.state_type.ToString() +
	            ",state_value_offset=" + std::to_string(aggregate.state_value_offset) +
	            ",state_is_set_offset=" + std::to_string(aggregate.state_is_set_offset) + ">";
	return true;
}

static SljitRegionNodePlan PlanSljitUngroupedAggregateSinkNode(const JitRegionIRNode &node) {
	if (!node.sink) {
		return SljitRegionFallbackNode("ungrouped aggregate sink is missing native sink IR");
	}
	if (node.sink->aggregates.empty()) {
		return SljitRegionFallbackNode("ungrouped aggregate sink has no aggregate payload bindings");
	}

	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE;
	bool native_update_contract_ready = true;
	bool native_update_executable = true;
	string native_update_contract;
	string native_update_blocker;
	for (auto &aggregate : node.sink->aggregates) {
		if (!aggregate.supported_payload_references) {
			auto reason = aggregate.reason.empty() ? "aggregate payload binding unsupported" : aggregate.reason;
			return SljitRegionFallbackNode("ungrouped aggregate sink payload unsupported;aggregate_index=" +
			                               std::to_string(aggregate.aggregate_index) + ";" + reason);
		}
		if (!native_update_contract.empty()) {
			native_update_contract += ";";
		}
		native_update_contract += DescribeSljitAggregateNativeUpdateContract(aggregate);
		native_update_contract_ready =
		    native_update_contract_ready && aggregate.native_update != JitAggregateUpdateKind::NONE;
		SljitNativeUngroupedAggregateUpdatePlan update;
		string update_blocker;
		if (TryPlanSljitNativeUngroupedAggregateUpdate(aggregate, update, update_blocker)) {
			native_op->native_ungrouped_aggregate_updates.push_back(std::move(update));
		} else {
			native_update_executable = false;
			if (!native_update_blocker.empty()) {
				native_update_blocker += ";";
			}
			native_update_blocker += "aggregate";
			native_update_blocker += std::to_string(aggregate.aggregate_index);
			native_update_blocker += "_native_update_blocker=";
			native_update_blocker += update_blocker;
		}
		JitUngroupedAggregatePayloadBinding binding;
		binding.aggregate_index = aggregate.aggregate_index;
		binding.payload_index = aggregate.payload_index;
		native_op->aggregate_payloads.push_back(binding);
	}

	SljitRegionNodePlan result;
	native_update_executable = native_update_executable && native_update_contract_ready &&
	                           native_op->native_ungrouped_aggregate_updates.size() == node.sink->aggregates.size();
	if (!native_update_executable) {
		native_op->native_ungrouped_aggregate_updates.clear();
	}
	result.kind = native_update_executable ? JitLoweringKind::NATIVE : JitLoweringKind::FALLBACK;
	result.reason = native_update_executable ? "generated native ungrouped aggregate state update"
	                                         : "ungrouped aggregate native state update protocol missing";
	result.reason += native_update_contract_ready ? ";native-aggregate-update-contract=ready"
	                                              : ";native-aggregate-update-contract=missing";
	result.reason += native_update_executable ? ";native-aggregate-update-executable=ready"
	                                          : ";native-aggregate-update-executable=missing";
	if (!native_update_blocker.empty()) {
		result.reason += ";";
		result.reason += native_update_blocker;
	}
	if (!native_update_contract.empty()) {
		result.reason += ";";
		result.reason += native_update_contract;
	}
	if (!node.sink->reason.empty()) {
		result.reason += ";" + node.sink->reason;
	}
	if (native_update_executable) {
		result.native_op = std::move(native_op);
	}
	return result;
}

static SljitRegionNodePlan PlanSljitHashAggregateSinkNode(const JitRegionIRNode &node) {
	if (!node.sink) {
		return SljitRegionFallbackNode("hash aggregate sink is missing native sink IR");
	}
	if (node.sink->groups.empty()) {
		return SljitRegionFallbackNode("hash aggregate sink has no group bindings");
	}
	for (auto &group : node.sink->groups) {
		if (!group.supported_reference) {
			auto reason = group.reason.empty() ? "hash aggregate group binding unsupported" : group.reason;
			return SljitRegionFallbackNode("hash aggregate sink group unsupported;group_index=" +
			                               std::to_string(group.group_index) + ";" + reason);
		}
	}
	if (node.sink->aggregates.empty()) {
		return SljitRegionFallbackNode("hash aggregate sink has no aggregate payload bindings");
	}

	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE;
	for (auto &group : node.sink->groups) {
		JitGroupedAggregateGroupBinding binding;
		binding.group_index = group.group_index;
		binding.input_index = group.input_index;
		native_op->grouped_aggregate_groups.push_back(binding);
	}

	bool native_update_contract_ready = true;
	bool native_update_executable = true;
	string native_update_contract;
	string native_update_blocker;
	for (auto &aggregate : node.sink->aggregates) {
		if (!aggregate.supported_payload_references) {
			auto reason = aggregate.reason.empty() ? "aggregate payload binding unsupported" : aggregate.reason;
			return SljitRegionFallbackNode("hash aggregate sink payload unsupported;aggregate_index=" +
			                               std::to_string(aggregate.aggregate_index) + ";" + reason);
		}
		if (!native_update_contract.empty()) {
			native_update_contract += ";";
		}
		native_update_contract += DescribeSljitAggregateNativeUpdateContract(aggregate);
		native_update_contract_ready =
		    native_update_contract_ready && aggregate.native_update != JitAggregateUpdateKind::NONE;
		JitGroupedAggregatePayloadBinding binding;
		binding.aggregate_index = aggregate.aggregate_index;
		binding.payload_index = aggregate.payload_index;
		native_op->grouped_aggregate_payloads.push_back(binding);
		SljitNativeGroupedAggregateUpdatePlan update;
		string update_blocker;
		if (TryPlanSljitNativeGroupedAggregateUpdate(aggregate, node.sink->aggregate_protocol, update,
		                                             update_blocker)) {
			native_op->native_grouped_aggregate_updates.push_back(std::move(update));
		} else {
			native_update_executable = false;
			if (!native_update_blocker.empty()) {
				native_update_blocker += ";";
			}
			native_update_blocker += "aggregate";
			native_update_blocker += std::to_string(aggregate.aggregate_index);
			native_update_blocker += "_native_grouped_update_blocker=";
			native_update_blocker += update_blocker;
		}
	}

	SljitRegionNodePlan result;
	auto grouped_state_contract = node.sink->aggregate_protocol.native_grouped_state_contract;
	if (grouped_state_contract.status == JitRegionStateContractStatus::NONE) {
		grouped_state_contract.status = JitRegionStateContractStatus::MISSING;
	}
	auto native_lookup_contract = node.sink->aggregate_protocol.native_hash_lookup_contract;
	if (native_lookup_contract.status == JitRegionStateContractStatus::NONE) {
		native_lookup_contract.status = JitRegionStateContractStatus::MISSING;
	}
	bool native_state_update_executable =
	    native_update_executable && native_update_contract_ready &&
	    grouped_state_contract.status == JitRegionStateContractStatus::READY &&
	    native_op->native_grouped_aggregate_updates.size() == node.sink->aggregates.size();
	bool native_lookup_executable = native_lookup_contract.status == JitRegionStateContractStatus::READY;
	bool state_update_has_lookup_path = native_state_update_executable && native_lookup_executable;
	if (!state_update_has_lookup_path) {
		native_op->native_grouped_aggregate_updates.clear();
	}
	result.kind = state_update_has_lookup_path ? JitLoweringKind::NATIVE : JitLoweringKind::FALLBACK;
	result.reason = state_update_has_lookup_path ? "generated native hash aggregate lookup and state update"
	                                             : "hash aggregate native lookup and state update protocol missing";
	result.reason += native_update_contract_ready ? ";native-aggregate-function-contract=ready"
	                                              : ";native-aggregate-function-contract=missing";
	result.reason += state_update_has_lookup_path ? ";native-grouped-aggregate-update-executable=ready"
	                                             : ";native-grouped-aggregate-update-executable=missing";
	result.reason += ";native-grouped-state-contract=" +
	                 string(JitRegionStateContractStatusToString(grouped_state_contract.status));
	result.reason += ";native-hash-aggregate-lookup-contract=" +
	                 string(JitRegionStateContractStatusToString(native_lookup_contract.status));
	result.reason += ";" + DescribeSljitGroupedStateLayoutContract(node.sink->aggregate_protocol);
	if (!grouped_state_contract.required_capability.empty()) {
		result.reason += ";native-grouped-state-required-capability=" + grouped_state_contract.required_capability;
	}
	if (!grouped_state_contract.protocol_version.empty()) {
		result.reason += ";native-grouped-state-protocol=" + grouped_state_contract.protocol_version;
	}
	if (!grouped_state_contract.blocker.empty()) {
		result.reason += ";native-grouped-state-blocker=" + grouped_state_contract.blocker;
	}
	if (grouped_state_contract.status != JitRegionStateContractStatus::READY) {
		result.reason += ";requires-native-grouped-state-abi=true";
	}
	if (!native_lookup_contract.required_capability.empty()) {
		result.reason += ";native-hash-aggregate-lookup-required-capability=" +
		                 native_lookup_contract.required_capability;
	}
	if (!native_lookup_contract.protocol_version.empty()) {
		result.reason += ";native-hash-aggregate-lookup-protocol=" + native_lookup_contract.protocol_version;
	}
	if (!native_lookup_contract.blocker.empty()) {
		result.reason += ";native-hash-aggregate-lookup-blocker=" + native_lookup_contract.blocker;
	}
	if (!native_update_blocker.empty()) {
		result.reason += ";";
		result.reason += native_update_blocker;
	}
	if (!native_update_contract.empty()) {
		result.reason += ";";
		result.reason += native_update_contract;
	}
	if (!node.sink->reason.empty()) {
		result.reason += ";" + node.sink->reason;
	}
	if (state_update_has_lookup_path) {
		result.native_op = std::move(native_op);
	}
	return result;
}

static SljitRegionNodePlan PlanSljitPerfectHashAggregateSinkNode(const JitRegionIRNode &node) {
	if (!node.sink) {
		return SljitRegionFallbackNode("perfect hash aggregate sink is missing native sink IR");
	}
	if (node.sink->groups.empty()) {
		return SljitRegionFallbackNode("perfect hash aggregate sink has no group bindings");
	}
	for (auto &group : node.sink->groups) {
		if (!group.supported_reference) {
			auto reason = group.reason.empty() ? "perfect hash aggregate group binding unsupported" : group.reason;
			return SljitRegionFallbackNode("perfect hash aggregate sink group unsupported;group_index=" +
			                               std::to_string(group.group_index) + ";" + reason);
		}
	}
	if (node.sink->aggregates.empty()) {
		return SljitRegionFallbackNode("perfect hash aggregate sink has no aggregate payload bindings");
	}

	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE;
	native_op->perfect_hash_required_bits = node.sink->aggregate_protocol.perfect_required_bits;
	native_op->perfect_hash_group_minima = node.sink->aggregate_protocol.perfect_group_minima;
	for (auto &group : node.sink->groups) {
		JitGroupedAggregateGroupBinding binding;
		binding.group_index = group.group_index;
		binding.input_index = group.input_index;
		native_op->grouped_aggregate_groups.push_back(binding);
	}

	bool native_update_contract_ready = true;
	bool native_update_executable = true;
	string native_update_contract;
	string native_update_blocker;
	for (auto &aggregate : node.sink->aggregates) {
		if (!aggregate.supported_payload_references) {
			auto reason = aggregate.reason.empty() ? "aggregate payload binding unsupported" : aggregate.reason;
			return SljitRegionFallbackNode("perfect hash aggregate sink payload unsupported;aggregate_index=" +
			                               std::to_string(aggregate.aggregate_index) + ";" + reason);
		}
		if (!native_update_contract.empty()) {
			native_update_contract += ";";
		}
		native_update_contract += DescribeSljitAggregateNativeUpdateContract(aggregate);
		native_update_contract_ready =
		    native_update_contract_ready && aggregate.native_update != JitAggregateUpdateKind::NONE;
		JitGroupedAggregatePayloadBinding binding;
		binding.aggregate_index = aggregate.aggregate_index;
		binding.payload_index = aggregate.payload_index;
		native_op->grouped_aggregate_payloads.push_back(binding);
		SljitNativeGroupedAggregateUpdatePlan update;
		string update_blocker;
		if (TryPlanSljitNativeGroupedAggregateUpdate(aggregate, node.sink->aggregate_protocol, update,
		                                             update_blocker)) {
			native_op->native_grouped_aggregate_updates.push_back(std::move(update));
		} else {
			native_update_executable = false;
			if (!native_update_blocker.empty()) {
				native_update_blocker += ";";
			}
			native_update_blocker += "aggregate";
			native_update_blocker += std::to_string(aggregate.aggregate_index);
			native_update_blocker += "_native_grouped_update_blocker=";
			native_update_blocker += update_blocker;
		}
	}

	SljitRegionNodePlan result;
	auto grouped_state_contract = node.sink->aggregate_protocol.native_grouped_state_contract;
	if (grouped_state_contract.status == JitRegionStateContractStatus::NONE) {
		grouped_state_contract.status = JitRegionStateContractStatus::MISSING;
	}
	native_update_executable =
	    native_update_executable && native_update_contract_ready &&
	    grouped_state_contract.status == JitRegionStateContractStatus::READY &&
	    native_op->native_grouped_aggregate_updates.size() == node.sink->aggregates.size();
	if (!native_update_executable) {
		native_op->native_grouped_aggregate_updates.clear();
	}
	result.kind = native_update_executable ? JitLoweringKind::NATIVE : JitLoweringKind::FALLBACK;
	result.reason = native_update_executable ? "generated native perfect hash aggregate state update"
	                                         : "perfect hash aggregate native state update protocol missing";
	result.reason += native_update_contract_ready ? ";native-aggregate-function-contract=ready"
	                                              : ";native-aggregate-function-contract=missing";
	result.reason += native_update_executable ? ";native-grouped-aggregate-update-executable=ready"
	                                          : ";native-grouped-aggregate-update-executable=missing";
	result.reason += ";native-grouped-state-contract=" +
	                 string(JitRegionStateContractStatusToString(grouped_state_contract.status));
	result.reason += ";" + DescribeSljitGroupedStateLayoutContract(node.sink->aggregate_protocol);
	if (!grouped_state_contract.required_capability.empty()) {
		result.reason += ";native-grouped-state-required-capability=" + grouped_state_contract.required_capability;
	}
	if (!grouped_state_contract.protocol_version.empty()) {
		result.reason += ";native-grouped-state-protocol=" + grouped_state_contract.protocol_version;
	}
	if (!grouped_state_contract.blocker.empty()) {
		result.reason += ";native-grouped-state-blocker=" + grouped_state_contract.blocker;
	}
	if (grouped_state_contract.status != JitRegionStateContractStatus::READY) {
		result.reason += ";requires-native-grouped-state-abi=true";
	}
	if (!native_update_blocker.empty()) {
		result.reason += ";";
		result.reason += native_update_blocker;
	}
	if (!native_update_contract.empty()) {
		result.reason += ";";
		result.reason += native_update_contract;
	}
	if (!node.sink->reason.empty()) {
		result.reason += ";" + node.sink->reason;
	}
	if (native_update_executable) {
		result.native_op = std::move(native_op);
	}
	return result;
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
	case SljitNativeHashJoinKeyKind::UINT8:
		return "uint8";
	case SljitNativeHashJoinKeyKind::UINT16:
		return "uint16";
	case SljitNativeHashJoinKeyKind::UINT32:
		return "uint32";
	case SljitNativeHashJoinKeyKind::UINT64:
		return "uint64";
	default:
		return "unknown";
	}
}

static const char *SljitHashJoinProbeOutputModeToString(JitRegionHashJoinProbeOutputMode mode) {
	switch (mode) {
	case JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
		return "matched_probe_and_build";
	case JitRegionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
		return "matched_probe_only";
	case JitRegionHashJoinProbeOutputMode::MARK_PROBE:
		return "mark_probe";
	case JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
		return "mark_build_only";
	default:
		return "none";
	}
}

static const char *SljitHashJoinComparisonToString(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return "equal";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "notequal";
	case ExpressionType::COMPARE_LESSTHAN:
		return "lessthan";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "greaterthan";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "lessthanorequalto";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "greaterthanorequalto";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "not_distinct_from";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "distinct_from";
	default:
		return "unsupported";
	}
}

static bool SljitHashJoinEqualityComparisonSupported(ExpressionType comparison_type) {
	return comparison_type == ExpressionType::COMPARE_EQUAL ||
	       comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static bool SljitHashJoinMatchPredicateSupported(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

static SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const JitRegionIRNode &node,
                                                              const vector<LogicalType> &input_types) {
	if (!node.operator_info) {
		return SljitRegionFallbackNode("hash join probe operator is missing typed operator IR");
	}
	auto &protocol = node.operator_info->hash_join_protocol;
	if (!protocol.present) {
		return SljitRegionFallbackNode("hash join probe native lowering requires hash join protocol IR");
	}
	if (protocol.native_probe_contract.status != JitRegionStateContractStatus::READY) {
		auto reason = protocol.native_probe_contract.blocker.empty()
		                  ? "hash join probe native protocol contract is not ready"
		                  : protocol.native_probe_contract.blocker;
		return SljitRegionFallbackNode(reason);
	}
	if (!protocol.native_probe_shape_ready) {
		auto reason = protocol.native_probe_shape_blocker.empty()
		                  ? "hash join probe native shape is not ready"
		                  : protocol.native_probe_shape_blocker;
		return SljitRegionFallbackNode(reason);
	}
	if (node.operator_info->hash_join_keys.size() != protocol.condition_count) {
		return SljitRegionFallbackNode("hash join probe native lowering key count does not match protocol");
	}
	if (protocol.layout_offsets.size() < protocol.condition_count) {
		return SljitRegionFallbackNode("hash join probe native lowering requires hash table key layout offsets");
	}
	if (protocol.equality_condition_count == 0 || protocol.equality_condition_count > protocol.condition_count) {
		return SljitRegionFallbackNode("hash join probe native lowering requires an equality-key prefix");
	}

	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	keys.reserve(node.operator_info->hash_join_keys.size());
	for (idx_t key_idx = 0; key_idx < node.operator_info->hash_join_keys.size(); key_idx++) {
		auto &key = node.operator_info->hash_join_keys[key_idx];
		auto comparison_type = protocol.comparison_types[key_idx];
		auto equality_key = key_idx < protocol.equality_condition_count;
		if (equality_key && !SljitHashJoinEqualityComparisonSupported(comparison_type)) {
			return SljitRegionFallbackNode("hash join probe native lowering has unsupported equality comparison " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!equality_key && !SljitHashJoinMatchPredicateSupported(comparison_type)) {
			return SljitRegionFallbackNode("hash join probe native lowering has unsupported match predicate " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!key.supported_reference) {
			return SljitRegionFallbackNode(key.reason.empty()
			                                   ? "hash join probe native lowering requires supported reference keys"
			                                   : key.reason);
		}
		if (key.input_index >= input_types.size()) {
			return SljitRegionFallbackNode("hash join probe native lowering key input index is outside operator input");
		}
		SljitNativeHashJoinKeyKind key_kind;
		if (!TryGetSljitHashJoinKeyKind(key.type, key_kind)) {
			return SljitRegionFallbackNode("hash join probe native lowering has unsupported key type " +
			                               key.type.ToString());
		}
		SljitNativeHashJoinProbeKeyPlan key_plan;
		key_plan.key_input_index = key.input_index;
		key_plan.key_layout_offset = protocol.layout_offsets[key_idx];
		key_plan.key_type = key.type;
		key_plan.key_kind = key_kind;
		key_plan.comparison_type = comparison_type;
		key_plan.equality_key = equality_key;
		key_plan.null_equal = equality_key && comparison_type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		keys.push_back(std::move(key_plan));
	}

	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	native_op->operator_index = node.operator_index;
	native_op->output_types = node.output_types;
	native_op->hash_join_probe.operator_index = node.operator_index;
	native_op->hash_join_probe.input_types = input_types;
	native_op->hash_join_probe.keys = std::move(keys);
	native_op->hash_join_probe.equality_key_count = protocol.equality_condition_count;
	native_op->hash_join_probe.mark_build_match = PropagatesBuildSide(protocol.join_type);
	native_op->hash_join_probe.found_match_offset = protocol.tuple_size;
	native_op->hash_join_probe.pointer_offset = protocol.pointer_offset;
	native_op->hash_join_probe.output_mode = protocol.native_probe_output_mode;
	native_op->hash_join_probe.operator_info = *node.operator_info;
	native_op->hash_join_probe.ir = "hash_join_probe_native<hash_keys=" +
	                                std::to_string(native_op->hash_join_probe.equality_key_count) + ",conditions=";
	for (idx_t key_idx = 0; key_idx < native_op->hash_join_probe.keys.size(); key_idx++) {
		if (key_idx > 0) {
			native_op->hash_join_probe.ir += "|";
		}
		auto &key = native_op->hash_join_probe.keys[key_idx];
		native_op->hash_join_probe.ir += key.equality_key ? "key" : "predicate";
		native_op->hash_join_probe.ir += std::to_string(key_idx);
		native_op->hash_join_probe.ir += "<input_index=" + std::to_string(key.key_input_index);
		native_op->hash_join_probe.ir += ",kind=" + string(SljitHashJoinKeyKindToString(key.key_kind));
		native_op->hash_join_probe.ir += ",layout_offset=" + std::to_string(key.key_layout_offset);
		native_op->hash_join_probe.ir += ",comparison=" + string(SljitHashJoinComparisonToString(key.comparison_type));
		native_op->hash_join_probe.ir += key.null_equal ? ",null_equal=true>" : ",null_equal=false>";
	}
	native_op->hash_join_probe.ir += ",probe_shape=native";
	native_op->hash_join_probe.ir += ",output_mode=" +
	                                  string(SljitHashJoinProbeOutputModeToString(
	                                      native_op->hash_join_probe.output_mode));
	if (native_op->hash_join_probe.mark_build_match) {
		native_op->hash_join_probe.ir += ",mark_build_match=true";
		native_op->hash_join_probe.ir += ",found_match_offset=" +
		                                  std::to_string(native_op->hash_join_probe.found_match_offset);
		native_op->hash_join_probe.ir += ",pointer_offset=" +
		                                  std::to_string(native_op->hash_join_probe.pointer_offset);
	}
	native_op->hash_join_probe.ir += ">";

	string reason =
	    "generated native hash join probe;requires=native_operator_runtime_binding;"
	    "requires=native_hash_join_table_layout;"
	    "native-hash-join-probe-executable=ready;native_probe_shape_ready=true";
	if (!node.operator_info->ir.empty()) {
		reason += ";" + node.operator_info->ir;
	}
	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_op = std::move(native_op);
	return result;
}

static SljitRegionNodePlan PlanSljitHashJoinSinkNode(const JitRegionIRNode &node) {
	if (!node.sink) {
		return SljitRegionFallbackNode("hash join sink is missing native sink IR");
	}
	auto &protocol = node.sink->hash_join_protocol;
	if (!protocol.present) {
		return SljitRegionFallbackNode("hash join build native lowering requires hash join protocol IR");
	}
	if (protocol.native_build_contract.status != JitRegionStateContractStatus::READY) {
		auto reason = protocol.native_build_contract.blocker.empty()
		                  ? "hash join build native protocol contract is not ready"
		                  : protocol.native_build_contract.blocker;
		return SljitRegionFallbackNode(reason);
	}
	if (!protocol.build_append_shape_ready) {
		auto reason = protocol.build_append_shape_blocker.empty()
		                  ? "hash join build native append shape is not ready"
		                  : protocol.build_append_shape_blocker;
		return SljitRegionFallbackNode(reason);
	}
	auto native_op = make_uniq<SljitNativeRegionOpPlan>();
	native_op->kind = SljitNativeRegionOpKind::HASH_JOIN_BUILD;
	native_op->hash_join_build.sink_info = *node.sink;
	native_op->hash_join_build.ir = node.sink->ir;

	string reason =
	    "generated native hash join build append protocol;requires=native_sink_runtime_binding;"
	    "requires=native_hash_join_build_append;build_append_shape_ready=true";
	if (!node.sink->ir.empty()) {
		reason += ";" + node.sink->ir;
	}
	SljitRegionNodePlan result;
	result.kind = JitLoweringKind::NATIVE;
	result.reason = std::move(reason);
	result.native_op = std::move(native_op);
	return result;
}

static SljitRegionNodePlan PlanSljitSinkNode(const JitRegionIRNode &node) {
	if (!node.sink) {
		if (!node.fallback_reason.empty()) {
			return SljitRegionFallbackNode(node.fallback_reason);
		}
		return SljitRegionFallbackNode("sink node is missing native sink IR");
	}
	switch (node.sink->kind) {
	case JitRegionSinkKind::HASH_JOIN_BUILD:
		return PlanSljitHashJoinSinkNode(node);
	case JitRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return PlanSljitHashAggregateSinkNode(node);
	case JitRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return PlanSljitPerfectHashAggregateSinkNode(node);
	case JitRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return PlanSljitUngroupedAggregateSinkNode(node);
	default:
		if (!node.fallback_reason.empty()) {
			return SljitRegionFallbackNode(node.fallback_reason);
		}
			return SljitRegionFallbackNode("sink kind is outside SLJIT native sink lowering");
	}
}

static SljitRegionNodePlan PlanSljitFullPipelineSinkNode(const JitRegionIRNode &node,
                                                         const vector<LogicalType> &input_types) {
	if (!node.sink) {
		if (!node.fallback_reason.empty()) {
			return SljitRegionFallbackNode(node.fallback_reason);
		}
		return SljitRegionFallbackNode("full pipeline sink node is missing native sink IR");
	}

	auto native_sink = PlanSljitSinkNode(node);
	if (native_sink.kind == JitLoweringKind::NATIVE && native_sink.native_op) {
		native_sink.native_op->output_types = input_types;
		if (native_sink.native_op->kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
			native_sink.native_op->hash_join_build.input_types = input_types;
		}
		native_sink.reason += SljitNativeRegionOpIsNativeProtocolSinkStage(*native_sink.native_op)
		                         ? ";full-pipeline-native-protocol-stage"
		                         : ";full-pipeline-native-sink-update";
		return native_sink;
	}
	if (node.sink->kind == JitRegionSinkKind::HASH_JOIN_BUILD) {
		auto reason = string("full pipeline hash join build sink requires native hash join build protocol lowering");
		if (!native_sink.reason.empty()) {
			reason += ";hash-join-build-protocol-fallback=" + native_sink.reason;
		}
		if (!node.sink->ir.empty()) {
			reason += ";" + node.sink->ir;
		}
		if (!node.fallback_reason.empty()) {
			reason += ";boundary=" + node.fallback_reason;
		}
		return SljitRegionFallbackNode(std::move(reason));
	}

	string reason = "full pipeline sink requires native sink or operator update protocol";
	if (!native_sink.reason.empty()) {
		reason += ";native-sink-fallback=" + native_sink.reason;
	}
	if (!node.sink->ir.empty()) {
		reason += ";" + node.sink->ir;
	}
	if (!node.fallback_reason.empty()) {
		reason += ";boundary=" + node.fallback_reason;
	}
	return SljitRegionFallbackNode(std::move(reason));
}

static SljitRegionNodePlan PlanSljitRegionNode(const JitRegionIRNode &node, const vector<LogicalType> &input_types,
                                               string &error) {
	switch (node.kind) {
	case JitRegionIRNodeKind::FILTER:
		return PlanSljitFilterNode(node, error);
	case JitRegionIRNodeKind::PROJECTION:
		return PlanSljitProjectionNode(node, input_types, error);
	case JitRegionIRNodeKind::SOURCE:
		return SljitRegionFallbackNode(SljitSourceFallbackReason(node));
		case JitRegionIRNodeKind::SINK:
		{
			auto sink = PlanSljitSinkNode(node);
			if (sink.kind == JitLoweringKind::NATIVE && sink.native_op) {
				sink.native_op->output_types = input_types;
			}
			return sink;
	}
	case JitRegionIRNodeKind::OPERATOR:
		if (node.operator_info && node.operator_info->kind == JitRegionOperatorKind::HASH_JOIN_PROBE) {
			return PlanSljitHashJoinProbeOperatorNode(node, input_types);
		}
		if (!node.fallback_reason.empty()) {
			return SljitRegionFallbackNode(node.fallback_reason);
		}
			return SljitRegionFallbackNode("operator helper kind has no SLJIT native protocol");
	default:
		if (!node.fallback_reason.empty()) {
			return SljitRegionFallbackNode(node.fallback_reason);
		}
			return SljitRegionFallbackNode("region IR node is outside SLJIT native region lowering");
	}
}

static bool SljitCandidateHasOpaqueStatefulUpstream(const JitRegionCandidate &candidate) {
	return candidate.context_traits.operator_fallback_count > 0;
}

static bool SljitRejectsSinkRegionContext(const JitRegionIRNode &node, const JitRegionCandidate &candidate) {
	return node.kind == JitRegionIRNodeKind::SINK && SljitCandidateHasOpaqueStatefulUpstream(candidate);
}

static bool SljitRejectsSourcePrefixResumeContext(const JitRegionCandidate &candidate) {
	return JitRegionABIIsSourcePipeline(candidate.contract.abi) && candidate.context_traits.operator_helper_count > 0;
}

static bool SljitRejectsPostSourceContinuationContext(const JitRegionCandidate &candidate) {
	if (!JitRegionABIIsChunkTransform(candidate.contract.abi)) {
		return false;
	}
	return candidate.continuation_traits.operator_helper_count > 0 ||
	       candidate.continuation_traits.operator_fallback_count > 0 ||
	       candidate.continuation_traits.expression_fallback_count > 0;
}

static bool SljitTraitsRequireUpstreamResumeProtocol(const JitRegionCandidateTraits &traits) {
	return traits.resumable_operator_count > 0 || traits.operator_helper_count > 0 ||
	       traits.operator_fallback_count > 0;
}

static bool SljitRejectsPostSourceUpstreamResumeContext(const JitRegionCandidate &candidate) {
	return JitRegionABIIsChunkTransform(candidate.contract.abi) &&
	       SljitTraitsRequireUpstreamResumeProtocol(candidate.upstream_traits);
}

static bool SljitRejectsSinkPipelineUpstreamResumeContext(const JitRegionCandidate &candidate) {
	if (!JitRegionABIIsSinkPipeline(candidate.contract.abi)) {
		return false;
	}
	return SljitTraitsRequireUpstreamResumeProtocol(candidate.upstream_traits) ||
	       candidate.upstream_traits.expression_fallback_count > 0;
}

static bool SljitCanExecuteSourceNode(const JitRegionIRNode &node, const JitRegionContract &contract) {
	if (!JitRegionABIOwnsSource(contract.abi) || !node.source) {
		return false;
	}
	if (node.source->native_source_contract.status == JitRegionNativeSourceStatus::READY ||
	    node.source->native_state_scan_contract.status == JitRegionStateContractStatus::READY) {
		return true;
	}
	return node.boundary == JitRegionBoundaryKind::SCAN;
}

static string SljitCandidateBoundaryIR(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	string result;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode() && node_idx < region_ir.nodes.size();
	     node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.source) {
			auto source_execution = node.source->execution;
			if (node_idx == candidate.first_node &&
			    candidate.source_execution != JitRegionSourceExecutionKind::NONE) {
				source_execution = candidate.source_execution;
			}
			auto source_ir = SljitSourceIR(node, source_execution);
			if (source_ir.empty()) {
				continue;
			}
			if (!result.empty()) {
				result += ";";
			}
			result += source_ir;
		}
		if (node.sink && !node.sink->ir.empty()) {
			if (!result.empty()) {
				result += ";";
			}
			result += node.sink->ir;
		}
	}
	return result;
}

static string SljitAttachCandidateBoundaryIR(string reason, const JitRegionIR &region_ir,
                                             const JitRegionCandidate &candidate) {
	auto boundary_ir = SljitCandidateBoundaryIR(region_ir, candidate);
	if (!boundary_ir.empty()) {
		reason += ";";
		reason += boundary_ir;
	}
	return reason;
}

SljitRegionPlan BuildSljitRegionPlan(const JitRegionIR &region_ir, const JitRegionCandidate &candidate) {
	SljitRegionPlan plan;
	plan.backend_plan = make_shared_ptr<SljitRegionBackendPlan>();
	plan.lowering_plan.SetCompiledExecutionMode(JitExecutionMode::UNSUPPORTED);
	if (candidate.stage_plan.HasStages()) {
		plan.lowering_plan.SetOperatorStageIR(candidate.stage_plan.ir);
	}
	auto native_region = make_uniq<SljitNativeRegionPlan>();
	vector<LogicalType> current_types = candidate.input_types;
	bool native_region_possible = true;
	bool requires_native_source = false;
	bool owns_source_filters = false;
	if (candidate.EndNode() > region_ir.nodes.size()) {
		plan.backend_plan->error = "SLJIT region candidate references nodes outside the region IR";
		plan.lowering_plan.backend_plan = plan.backend_plan;
		return plan;
	}
	if (SljitRejectsSourcePrefixResumeContext(candidate)) {
		plan.backend_plan->error =
		    "SLJIT source-prefix regions require a downstream helper-free resume protocol";
		auto blocker = candidate.source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE ||
		                       candidate.contract.native_fusion_ready
		                   ? "operator-fusion-gap:downstream-operator-helper-resume-protocol-missing;"
		                   : "source-fusion-gap:downstream-operator-helper-resume-protocol-missing;";
		plan.lowering_plan.AddFusionBlocker(SljitAttachCandidateBoundaryIR(
		    string(blocker) + candidate.contract.ir, region_ir, candidate));
		plan.lowering_plan.backend_plan = plan.backend_plan;
		return plan;
	}
	if (SljitRejectsPostSourceUpstreamResumeContext(candidate)) {
		plan.backend_plan->error =
		    "SLJIT post-source split regions require an upstream operator resume protocol";
		plan.lowering_plan.AddFusionBlocker(SljitAttachCandidateBoundaryIR(
		    "operator-fusion-gap:upstream-operator-resume-protocol-missing;" + candidate.contract.ir +
		        ";upstream_" + candidate.upstream_traits.ir,
		    region_ir, candidate));
		plan.lowering_plan.backend_plan = plan.backend_plan;
		return plan;
	}
	if (SljitRejectsPostSourceContinuationContext(candidate)) {
		plan.backend_plan->error =
		    "SLJIT post-source split regions require a downstream helper-free continuation protocol";
		plan.lowering_plan.AddFusionBlocker(SljitAttachCandidateBoundaryIR(
		    "operator-fusion-gap:downstream-helper-continuation-protocol-missing;" + candidate.contract.ir, region_ir,
		    candidate));
		plan.lowering_plan.backend_plan = plan.backend_plan;
		return plan;
	}
	if (SljitRejectsSinkPipelineUpstreamResumeContext(candidate)) {
		plan.backend_plan->error =
		    "SLJIT sink-pipeline split regions require an upstream operator resume protocol";
		plan.lowering_plan.AddFusionBlocker(SljitAttachCandidateBoundaryIR(
		    "sink-fusion-gap:upstream-operator-resume-protocol-missing;" + candidate.contract.ir + ";upstream_" +
		        candidate.upstream_traits.ir,
		    region_ir, candidate));
		plan.lowering_plan.backend_plan = plan.backend_plan;
		return plan;
	}
	auto &contract = candidate.contract;
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (node.kind == JitRegionIRNodeKind::SOURCE) {
			auto executable_source = SljitCanExecuteSourceNode(node, contract);
			auto source_execution =
			    candidate.source_execution != JitRegionSourceExecutionKind::NONE
			        ? candidate.source_execution
			        : (node.source ? node.source->execution : JitRegionSourceExecutionKind::NONE);
			auto node_plan = executable_source ? PlanSljitSourceNode(node, contract, source_execution)
			                                   : PlanSljitRegionNode(node, current_types, plan.backend_plan->error);
			const bool source_requires_native =
			    executable_source && node_plan.kind == JitLoweringKind::FALLBACK &&
			    StringUtil::Contains(node_plan.reason, "source-fusion-gap:requires-native-source");
			plan.lowering_plan.AddNode(node.role, node.operator_name, node_plan.kind, node_plan.reason);
			if (source_requires_native) {
				plan.lowering_plan.AddFusionBlocker(
				    "source-fusion-gap:requires-native-source;source_execution=duckdb-source-boundary");
			}
			if (executable_source && node_plan.kind != JitLoweringKind::FALLBACK) {
				auto selected_source_execution = node_plan.requires_native_source
				                                     ? JitRegionSourceExecutionKind::NATIVE_SOURCE
				                                     : node.source->execution;
				plan.lowering_plan.SetSelectedSourceExecution(selected_source_execution);
			}
			if (executable_source &&
			    (node_plan.kind == JitLoweringKind::NATIVE || node_plan.kind == JitLoweringKind::PASS_THROUGH)) {
				auto source_output_types = SljitRegionNodeHasNativeOps(node_plan)
				                               ? SljitRegionNodeLastNativeOp(node_plan).output_types
				                               : node.output_types;
				requires_native_source = requires_native_source || node_plan.requires_native_source;
				owns_source_filters = owns_source_filters || node_plan.owns_source_filters;
				if (native_region_possible) {
					native_region->source_filter_count += node_plan.source_filter_count;
					if (node_plan.source_filter_execution != SljitSourceFilterExecutionKind::NONE) {
						native_region->source_filter_execution = node_plan.source_filter_execution;
					}
					AppendSljitRegionNodeNativeOps(*native_region, node_plan);
				}
				current_types = std::move(source_output_types);
			} else {
				native_region_possible = false;
				current_types = node.output_types;
			}
			continue;
		}
		if (node.kind == JitRegionIRNodeKind::SINK) {
			auto node_plan = JitRegionABIIsFullPipeline(contract.abi)
			                     ? PlanSljitFullPipelineSinkNode(node, current_types)
			                     : SljitRejectsSinkRegionContext(node, candidate)
			                           ? SljitRegionFallbackNode("sink region requires stateful-fallback-free upstream "
			                                                     "region context")
			                           : PlanSljitRegionNode(node, current_types, plan.backend_plan->error);
			plan.lowering_plan.AddNode(node.role, node.operator_name, node_plan.kind, std::move(node_plan.reason));
			if (node_plan.kind == JitLoweringKind::FALLBACK && JitRegionABIIsFullPipeline(contract.abi) &&
			    node.sink && node.sink->kind == JitRegionSinkKind::HASH_JOIN_BUILD) {
				auto build_contract_ready =
				    node.sink->hash_join_protocol.present &&
				    node.sink->hash_join_protocol.native_build_contract.status == JitRegionStateContractStatus::READY;
				plan.backend_plan->error =
				    build_contract_ready
				        ? "SLJIT full-pipeline hash join build sink rejected by native hash join build lowering"
				        : "SLJIT full-pipeline hash join build sink requires a native hash join build protocol";
				plan.lowering_plan.AddFusionBlocker(build_contract_ready
				                                         ? "sink-fusion-gap:hash-join-build-native-lowering"
				                                         : "sink-fusion-gap:hash-join-build-protocol-missing");
			}
			if (node_plan.kind == JitLoweringKind::NATIVE && node_plan.native_op) {
				if (native_region_possible) {
					AppendSljitRegionNodeNativeOps(*native_region, node_plan);
				}
			} else {
				native_region_possible = false;
			}
			continue;
			}
			SljitRegionNodePlan node_plan;
			if (node.kind == JitRegionIRNodeKind::OPERATOR &&
			    node.boundary == JitRegionBoundaryKind::OPERATOR_NATIVE &&
			    !JitRegionABIIsFullPipeline(contract.abi)) {
				node_plan = SljitRegionFallbackNode("native operator protocol requires full-pipeline region ABI");
			} else if (node.kind == JitRegionIRNodeKind::OPERATOR &&
			           node.boundary == JitRegionBoundaryKind::OPERATOR_HELPER &&
			           !JitRegionABIIsFullPipeline(contract.abi)) {
			node_plan = SljitRegionFallbackNode("native operator protocol boundary requires full-pipeline region ownership");
		} else {
			node_plan = PlanSljitRegionNode(node, current_types, plan.backend_plan->error);
		}
		plan.lowering_plan.AddNode(node.role, node.operator_name, node_plan.kind, node_plan.reason);
		if (node_plan.kind == JitLoweringKind::FALLBACK && node.operator_info &&
		    node.operator_info->kind == JitRegionOperatorKind::HASH_JOIN_PROBE) {
				auto probe_contract_ready = node.operator_info->hash_join_protocol.present &&
				                            node.operator_info->hash_join_protocol.native_probe_contract.status ==
				                                JitRegionStateContractStatus::READY;
				plan.backend_plan->error =
				    !probe_contract_ready
				        ? "SLJIT hash join probe requires a native hash join probe protocol"
				        : "SLJIT hash join probe rejected by native hash join lowering";
				plan.lowering_plan.AddFusionBlocker(
				    !probe_contract_ready
				        ? "operator-fusion-gap:hash-join-probe-protocol-missing"
				        : "operator-fusion-gap:hash-join-probe-native-lowering-missing;" + node_plan.reason);
			}
		if ((node_plan.kind != JitLoweringKind::NATIVE && node_plan.kind != JitLoweringKind::PASS_THROUGH) ||
		    !SljitRegionNodeHasNativeOps(node_plan)) {
			native_region_possible = false;
			continue;
		}
		if (current_types.empty()) {
			current_types = node.output_types;
		}
		if (native_region_possible && SljitRegionNodeHasSingleNativeOp(node_plan) &&
		    IsNativeIdentityProjection(SljitRegionNodeFirstNativeOp(node_plan), current_types)) {
			native_region->elided_identity_projections++;
			continue;
		}
		current_types = SljitRegionNodeLastNativeOp(node_plan).output_types;
		if (native_region_possible) {
			AppendSljitRegionNodeNativeOps(*native_region, node_plan);
		}
	}
	if (native_region_possible && !native_region->ops.empty()) {
		native_region->native_source = requires_native_source;
		FuseAdjacentNativeProjections(*native_region);
		MarkRuntimeCombinedFilterProjections(*native_region);
		auto stage_plan = BuildSljitOperatorStageRegionPlan(*native_region, contract, candidate.stage_plan);
		if (stage_plan.IsValid()) {
			auto stage_ir = candidate.stage_plan.ir;
			if (!stage_ir.empty()) {
				stage_ir += ";";
			}
			stage_ir += stage_plan.stage_ir;
			plan.lowering_plan.SetOperatorStageIR(std::move(stage_ir));
		}
		string codegen_blocker;
		if (SljitNativeRegionHasCodegenGap(*native_region, codegen_blocker)) {
			plan.backend_plan->error = codegen_blocker;
			plan.lowering_plan.AddFusionBlocker(SljitNativeRegionCodegenFusionBlocker(*native_region) + ";" +
			                                    codegen_blocker + ";" + candidate.contract.ir);
			plan.lowering_plan.backend_plan = plan.backend_plan;
			return plan;
		}
		auto execution_form = ClassifySljitRegionExecutionForm(*native_region, contract, candidate.stage_plan);
		if (execution_form != JitRegionExecutionForm::NONE) {
			plan.lowering_plan.SetOwnsSourceFilters(owns_source_filters);
			plan.lowering_plan.SetRegionExecutionForm(execution_form);
			plan.backend_plan->native_region = std::move(native_region);
			plan.lowering_plan.SetCompiledExecutionMode(JitExecutionMode::NATIVE);
			plan.lowering_plan.shape_key = BuildSljitRegionShapeKey(*plan.backend_plan->native_region, contract);
			if (JitRegionABIIsSourcePipeline(contract.abi) && candidate.traits.has_table_scan_source &&
			    candidate.traits.source_execution == JitRegionSourceExecutionKind::NATIVE_SOURCE &&
			    candidate.traits.source_filter_count > 0 &&
			    plan.lowering_plan.shape_key == SLJIT_SOURCE_PREFIX_FILTER_PROJECTION_SHAPE) {
				plan.lowering_plan.shape_key =
				    BuildSljitRegionCandidateContextShapeKey(candidate, plan.lowering_plan.shape_key);
			}
		}
		if (plan.lowering_plan.ExpectedRegionExecutionForm() != JitRegionExecutionForm::FUSED) {
			if (!contract.executor_boundary_free) {
				plan.lowering_plan.AddFusionBlocker("candidate-fusion-gap:executor-boundary;" + contract.ir);
			}
			if (contract.source_boundary_count > 0) {
				plan.lowering_plan.AddFusionBlocker("candidate-fusion-gap:source-boundary;" + contract.ir);
			}
			if (contract.missing_protocol_count > 0) {
				plan.lowering_plan.AddFusionBlocker("candidate-fusion-gap:missing-protocol;" + contract.ir);
			}
		}
		if (plan.lowering_plan.shape_key.empty()) {
			plan.lowering_plan.shape_key = BuildSljitRegionCandidateShapeKey(candidate);
		}
	}
	plan.lowering_plan.backend_plan = plan.backend_plan;
	return plan;
}

static string DescribeNativeRegionExpression(const SljitNativeRegionExpressionPlan &expr) {
	string result;
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		result = "native:reference";
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
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8:
		result = "native:string-compress-uint8";
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
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		result = NativeNullCheckReason(expr.null_check_op);
		break;
	case SljitNativeRegionExpressionKind::PREDICATE:
		result = "native:boolean-predicate";
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
			result += "projection(";
			for (idx_t proj_idx = 0; proj_idx < op.projections.size(); proj_idx++) {
				if (proj_idx > 0) {
					result += ",";
				}
				result += DescribeNativeRegionExpression(op.projections[proj_idx]);
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			result += "hash_join_probe(hash_keys=" +
			          std::to_string(op.hash_join_probe.equality_key_count) + ",conditions=";
			for (idx_t key_idx = 0; key_idx < op.hash_join_probe.keys.size(); key_idx++) {
				if (key_idx > 0) {
					result += ",";
				}
				auto &key = op.hash_join_probe.keys[key_idx];
				result += key.equality_key ? "key" : "predicate";
				result += std::to_string(key_idx);
				result += "<input_index=" + std::to_string(key.key_input_index);
				result += ",kind=" + string(SljitHashJoinKeyKindToString(key.key_kind));
				result += ",layout_offset=" + std::to_string(key.key_layout_offset);
				result += ",comparison=" + string(SljitHashJoinComparisonToString(key.comparison_type));
				result += key.null_equal ? ",null_equal=true>" : ",null_equal=false>";
			}
			if (op.hash_join_probe.mark_build_match) {
				result += ",mark_build_match=true";
				result += ",found_match_offset=" + std::to_string(op.hash_join_probe.found_match_offset);
				result += ",pointer_offset=" + std::to_string(op.hash_join_probe.pointer_offset);
			}
			result += ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(op.hash_join_probe.output_mode));
			result += ")";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD: {
			result += "hash_join_build(keys=";
			for (idx_t key_idx = 0; key_idx < op.hash_join_build.sink_info.hash_join_keys.size(); key_idx++) {
				if (key_idx > 0) {
					result += ",";
				}
				auto &key = op.hash_join_build.sink_info.hash_join_keys[key_idx];
				result += "key" + std::to_string(key.key_index);
				result += "<input_index=" + std::to_string(key.input_index) + ">";
			}
			result += ";payload_columns=";
			auto &payload_columns = op.hash_join_build.sink_info.hash_join_protocol.payload_column_indices;
			for (idx_t payload_idx = 0; payload_idx < payload_columns.size(); payload_idx++) {
				if (payload_idx > 0) {
					result += ",";
				}
				result += std::to_string(payload_columns[payload_idx]);
			}
			result += ")";
			break;
		}
		case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
			result += "hash_aggregate_update(groups=";
			for (idx_t group_idx = 0; group_idx < op.grouped_aggregate_groups.size(); group_idx++) {
				if (group_idx > 0) {
					result += ",";
				}
				auto &binding = op.grouped_aggregate_groups[group_idx];
				result += "group" + std::to_string(binding.group_index);
				result += "<input_index=" + std::to_string(binding.input_index) + ">";
			}
			result += ";aggregates=";
			for (idx_t binding_idx = 0; binding_idx < op.grouped_aggregate_payloads.size(); binding_idx++) {
				if (binding_idx > 0) {
					result += ",";
				}
				auto &binding = op.grouped_aggregate_payloads[binding_idx];
				result += "aggregate" + std::to_string(binding.aggregate_index);
				result += "<payload_index=" + std::to_string(binding.payload_index) + ">";
			}
			result += ")";
			break;
		case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
			result += "perfect_hash_aggregate_update(groups=";
			for (idx_t group_idx = 0; group_idx < op.grouped_aggregate_groups.size(); group_idx++) {
				if (group_idx > 0) {
					result += ",";
				}
				auto &binding = op.grouped_aggregate_groups[group_idx];
				result += "group" + std::to_string(binding.group_index);
				result += "<input_index=" + std::to_string(binding.input_index) + ">";
			}
			result += ";aggregates=";
			for (idx_t binding_idx = 0; binding_idx < op.grouped_aggregate_payloads.size(); binding_idx++) {
				if (binding_idx > 0) {
					result += ",";
				}
				auto &binding = op.grouped_aggregate_payloads[binding_idx];
				result += "aggregate" + std::to_string(binding.aggregate_index);
				result += "<payload_index=" + std::to_string(binding.payload_index) + ">";
			}
			result += ")";
			break;
			case SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE:
				result += "ungrouped_aggregate_update(";
				for (idx_t binding_idx = 0; binding_idx < op.aggregate_payloads.size(); binding_idx++) {
					if (binding_idx > 0) {
						result += ",";
					}
					auto &binding = op.aggregate_payloads[binding_idx];
					result += "aggregate" + std::to_string(binding.aggregate_index);
					result += "<payload_index=" + std::to_string(binding.payload_index) + ">";
				}
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
	string result;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		if (op_idx > 0) {
			result += "-";
		}
		switch (region.ops[op_idx].kind) {
		case SljitNativeRegionOpKind::FILTER:
			result += "filter";
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			result += "projection";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			result += "hash-join-probe";
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			result += "hash-join-build";
			break;
		case SljitNativeRegionOpKind::HASH_AGGREGATE_UPDATE:
			result += "hash-aggregate-update";
			break;
		case SljitNativeRegionOpKind::PERFECT_HASH_AGGREGATE_UPDATE:
			result += "perfect-hash-aggregate-update";
			break;
		case SljitNativeRegionOpKind::UNGROUPED_AGGREGATE_UPDATE:
			result += "ungrouped-aggregate-update";
			break;
		default:
			result += "unknown";
			break;
		}
	}
	return result.empty() ? "empty" : result;
}

static void AppendCoreOperatorStages(SljitOperatorStageRegionPlan &result,
                                     const JitRegionStagePlan &core_stage_plan) {
	result.stages = core_stage_plan.stages;
}

static string DescribeOperatorStageRegion(const SljitOperatorStageRegionPlan &plan) {
	string result = "operator-stage-region<";
	result += "shape=" + plan.shape_key;
	result += ",kernel=" + string(plan.generic_runtime_loop ? "generic-runtime-loop" : "missing");
	if (!plan.kernel_blocker.empty()) {
		result += ",kernel_blocker=" + plan.kernel_blocker;
	}
	result += ",source=" + string(plan.native_source ? "native" : plan.owns_source ? "executor-boundary" : "none");
	result += ",stages=[";
	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto &stage = plan.stages[stage_idx];
		if (stage_idx > 0) {
			result += "|";
		}
		result += JitRegionStageKindToString(stage.kind);
		result += ":";
		result += JitRegionStageExecutionKindToString(stage.execution);
		result += ":";
		result += JitRegionOwnershipKindToString(stage.ownership);
		result += ":protocol=";
		result += JitCompiledProtocolKindToString(stage.protocol);
		result += ":drain=";
		result += JitCompiledDrainKindToString(stage.drain);
		if (stage.operator_index != DConstants::INVALID_INDEX) {
			result += "#" + std::to_string(stage.operator_index);
		}
		if (stage.filter_index != DConstants::INVALID_INDEX) {
			result += "#" + std::to_string(stage.filter_index);
		}
	}
	result += "]>";
	return result;
}

SljitOperatorStageRegionPlan BuildSljitOperatorStageRegionPlan(const SljitNativeRegionPlan &region,
                                                               const JitRegionContract &contract,
                                                               const JitRegionStagePlan &core_stage_plan) {
	SljitOperatorStageRegionPlan result;
	result.native_source = region.native_source;
	result.owns_source = JitRegionABIOwnsSource(contract.abi);
	result.owns_transform = contract.owns_transform;
	result.owns_sink = JitRegionABIOwnsSink(contract.abi);
	const auto context = SljitRegionCandidateContext(contract);
	if (core_stage_plan.HasStages() || !region.ops.empty() || result.owns_source || region.source_filter_count > 0 ||
	    result.owns_sink) {
		result.stage_plan_valid = true;
		result.shape_key = "sljit:" + context + ":operator-stage:" + DescribeNativeRegionShape(region);
		result.kernel_blocker = SljitNativeRegionCodegenFusionBlocker(region);
	}
	if (SljitNativeRegionHasGenericExecutableLoop(region, contract)) {
		result.generic_runtime_loop = true;
		result.execution_reason = "execution:native-sljit-region-" + DescribeNativeRegionShape(region);
		result.kernel_blocker.clear();
	}
	if (!result.IsValid()) {
		return result;
	}
	AppendCoreOperatorStages(result, core_stage_plan);
	result.stage_ir = DescribeOperatorStageRegion(result);
	return result;
}

} // namespace duckdb

#include "sljit_region_executable.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_region_codegen.hpp"
#include "sljit_native_util.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static idx_t AddSljitExecutableInputSource(vector<idx_t> &input_sources, idx_t input_source_index,
                                           vector<bool> *local_source_not_null = nullptr,
                                           const vector<bool> *input_not_null = nullptr) {
	for (idx_t source_idx = 0; source_idx < input_sources.size(); source_idx++) {
		if (input_sources[source_idx] == input_source_index) {
			return source_idx;
		}
	}
	input_sources.push_back(input_source_index);
	if (local_source_not_null) {
		local_source_not_null->push_back(input_not_null && input_source_index < input_not_null->size()
		                                     ? (*input_not_null)[input_source_index]
		                                     : false);
	}
	return input_sources.size() - 1;
}

static void RemapSljitPredicateToExecutableInputs(SljitNativePredicate &predicate, vector<idx_t> &input_sources,
                                                  vector<bool> &local_source_not_null,
                                                  const vector<bool> *input_not_null) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return;
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
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index,
		                                                       &local_source_not_null, input_not_null);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index,
		                                                       &local_source_not_null, input_not_null);
		predicate.right_source_index = AddSljitExecutableInputSource(input_sources, predicate.right_source_index,
		                                                             &local_source_not_null, input_not_null);
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			source_index =
			    AddSljitExecutableInputSource(input_sources, source_index, &local_source_not_null, input_not_null);
		}
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources, local_source_not_null,
			                                      input_not_null);
		}
		return;
	case SljitNativePredicateKind::NOT:
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources, local_source_not_null,
			                                      input_not_null);
		}
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child) {
				RemapSljitPredicateToExecutableInputs(*child, input_sources, local_source_not_null, input_not_null);
			}
		}
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

static void SetDenseSljitPredicateSourceIndices(SljitNativePredicate &predicate, idx_t source_count) {
	predicate.source_indices.clear();
	predicate.source_indices.reserve(source_count);
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		predicate.source_indices.push_back(source_idx);
	}
}

static void RemapSljitConstantOrNullToExecutableInputs(SljitNativeConstantOrNull &constant_or_null,
                                                       vector<idx_t> &input_sources) {
	for (auto &source_index : constant_or_null.guard_source_indices) {
		source_index = AddSljitExecutableInputSource(input_sources, source_index);
	}
}

static void PrepareExecutableRegionExpressionInputs(SljitExecutableRegionExpression &expr,
                                                    const vector<bool> *input_not_null = nullptr) {
	auto &semantic = expr.plan;
	expr.input_source_indices.clear();
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (semantic.predicate) {
			vector<bool> local_source_not_null;
			RemapSljitPredicateToExecutableInputs(*semantic.predicate, expr.input_source_indices, local_source_not_null,
			                                      input_not_null);
			SetDenseSljitPredicateSourceIndices(*semantic.predicate, expr.input_source_indices.size());
			semantic.predicate->source_not_null = std::move(local_source_not_null);
		}
		return;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		RemapSljitConstantOrNullToExecutableInputs(semantic.constant_or_null, expr.input_source_indices);
		return;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		expr.input_source_indices = semantic.expression_tree_source_indices;
		return;
	default:
		return;
	}
}

static idx_t AddSljitCombinedInputSource(idx_t source_index, vector<idx_t> &combined_sources,
                                         vector<bool> *combined_source_not_null = nullptr,
                                         const vector<bool> *input_not_null = nullptr,
                                         vector<Value> *combined_source_min_values = nullptr,
                                         vector<Value> *combined_source_max_values = nullptr,
                                         const vector<Value> *input_min_values = nullptr,
                                         const vector<Value> *input_max_values = nullptr);

static void RemapSljitExpressionTreeToCombinedInputs(
    ExecutionExpressionIR &node, const vector<idx_t> &local_sources, vector<idx_t> &combined_sources,
    vector<bool> *combined_source_not_null = nullptr, const vector<bool> *input_not_null = nullptr,
    vector<Value> *combined_source_min_values = nullptr, vector<Value> *combined_source_max_values = nullptr,
    const vector<Value> *input_min_values = nullptr, const vector<Value> *input_max_values = nullptr) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (node.ref_index >= local_sources.size()) {
			throw InternalException("SLJIT expression-tree reference source is out of range");
		}
		node.ref_index = AddSljitCombinedInputSource(
		    local_sources[node.ref_index], combined_sources, combined_source_not_null, input_not_null,
		    combined_source_min_values, combined_source_max_values, input_min_values, input_max_values);
		return;
	}
	if (node.left) {
		RemapSljitExpressionTreeToCombinedInputs(*node.left, local_sources, combined_sources, combined_source_not_null,
		                                         input_not_null, combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
	}
	if (node.right) {
		RemapSljitExpressionTreeToCombinedInputs(*node.right, local_sources, combined_sources, combined_source_not_null,
		                                         input_not_null, combined_source_min_values, combined_source_max_values,
		                                         input_min_values, input_max_values);
	}
	if (node.else_node) {
		RemapSljitExpressionTreeToCombinedInputs(*node.else_node, local_sources, combined_sources,
		                                         combined_source_not_null, input_not_null, combined_source_min_values,
		                                         combined_source_max_values, input_min_values, input_max_values);
	}
	for (auto &child : node.children) {
		if (child) {
			RemapSljitExpressionTreeToCombinedInputs(*child, local_sources, combined_sources, combined_source_not_null,
			                                         input_not_null, combined_source_min_values,
			                                         combined_source_max_values, input_min_values, input_max_values);
		}
	}
}

static idx_t AddSljitCombinedInputSource(idx_t source_index, vector<idx_t> &combined_sources,
                                         vector<bool> *combined_source_not_null, const vector<bool> *input_not_null,
                                         vector<Value> *combined_source_min_values,
                                         vector<Value> *combined_source_max_values,
                                         const vector<Value> *input_min_values, const vector<Value> *input_max_values) {
	for (idx_t combined_idx = 0; combined_idx < combined_sources.size(); combined_idx++) {
		if (combined_sources[combined_idx] == source_index) {
			return combined_idx;
		}
	}
	auto combined_idx = combined_sources.size();
	combined_sources.push_back(source_index);
	if (combined_source_not_null) {
		combined_source_not_null->push_back(
		    input_not_null && source_index < input_not_null->size() ? (*input_not_null)[source_index] : false);
	}
	if (combined_source_min_values) {
		combined_source_min_values->push_back(
		    input_min_values && source_index < input_min_values->size() ? (*input_min_values)[source_index] : Value());
	}
	if (combined_source_max_values) {
		combined_source_max_values->push_back(
		    input_max_values && source_index < input_max_values->size() ? (*input_max_values)[source_index] : Value());
	}
	return combined_idx;
}

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

static bool IsDirectNativeFloatingSource(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::FLOAT || kind == SljitNativeDoubleSourceKind::DOUBLE;
}

static bool SljitProjectionReturnIsSinglePrecision(const SljitNativeRegionExpressionPlan &plan,
                                                   bool &single_precision) {
	switch (plan.return_type.InternalType()) {
	case PhysicalType::FLOAT:
		single_precision = true;
		return true;
	case PhysicalType::DOUBLE:
		single_precision = false;
		return true;
	default:
		return false;
	}
}

static bool TryAddSljitDirectProjectionSource(SljitDirectProjectionPlan &direct_plan, idx_t input_index,
                                              idx_t projection_index, bool right_source) {
	for (auto &source : direct_plan.sources) {
		if (source.input_index == input_index) {
			return true;
		}
	}
	if (direct_plan.sources.size() >= 2) {
		return false;
	}
	SljitDirectProjectionSourceRef source;
	source.input_index = input_index;
	source.projection_index = projection_index;
	source.right_source = right_source;
	direct_plan.sources.push_back(source);
	return true;
}

static bool TryPlanSljitDirectProjectionSources(const vector<SljitExecutableRegionExpression> &projections,
                                                SljitDirectProjectionPlan &direct_plan) {
	direct_plan.sources.clear();
	for (auto projection_idx : direct_plan.projection_indices) {
		if (projection_idx >= projections.size()) {
			return false;
		}
		auto &plan = projections[projection_idx].plan;
		if (!TryAddSljitDirectProjectionSource(direct_plan, plan.source_index, projection_idx, false)) {
			return false;
		}
		if ((plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		     plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) &&
		    !TryAddSljitDirectProjectionSource(direct_plan, plan.right_source_index, projection_idx, true)) {
			return false;
		}
	}
	return !direct_plan.sources.empty();
}

static vector<SljitNativeRegionExpressionPlan>
BuildSljitProjectionPlans(const vector<SljitExecutableRegionExpression> &projections) {
	vector<SljitNativeRegionExpressionPlan> projection_plans;
	projection_plans.reserve(projections.size());
	for (auto &projection : projections) {
		projection_plans.push_back(projection.plan.Copy(false, false));
	}
	return projection_plans;
}

static bool SljitCanUseFlatFusedFloatingProjection(const SljitExecutableRegionExpression &expr, bool single_precision) {
	if (!expr.flat_function) {
		return false;
	}
	auto &plan = expr.plan;
	bool plan_single_precision;
	if (!SljitProjectionReturnIsSinglePrecision(plan, plan_single_precision) ||
	    plan_single_precision != single_precision) {
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		return IsDirectNativeFloatingSource(plan.double_source_kind);
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		return IsDirectNativeFloatingSource(plan.double_source_kind) &&
		       IsDirectNativeFloatingSource(plan.double_right_source_kind);
	default:
		return false;
	}
}

static bool TryPlanFlatFusedFloatingProjection(SljitExecutableRegionOp &op, SljitDirectProjectionPlan &direct_plan) {
	if (op.projections.size() < 2) {
		return false;
	}
	vector<idx_t> float_indices;
	vector<idx_t> double_indices;
	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &projection = op.projections[projection_idx];
		if (SljitCanUseFlatFusedFloatingProjection(projection, true)) {
			float_indices.push_back(projection_idx);
		} else if (SljitCanUseFlatFusedFloatingProjection(projection, false)) {
			double_indices.push_back(projection_idx);
		}
	}
	auto single_precision = float_indices.size() >= double_indices.size();
	auto &projection_indices = single_precision ? float_indices : double_indices;
	if (projection_indices.size() < 2) {
		return false;
	}
	direct_plan = SljitDirectProjectionPlan();
	direct_plan.kind = single_precision ? SljitDirectProjectionKind::FLOAT : SljitDirectProjectionKind::DOUBLE;
	direct_plan.stats_mode = SljitDirectProjectionStatsMode::GENERATED_FLOATING_MIN_MAX;
	direct_plan.projection_indices = projection_indices;
	direct_plan.covers_all_projections = projection_indices.size() == op.projections.size();
	TryPlanSljitDirectProjectionSources(op.projections, direct_plan);
	return true;
}

static bool TryBuildFlatFusedFloatingProjection(SljitExecutableRegionOp &op, string &error) {
	SljitDirectProjectionPlan direct_plan;
	if (!TryPlanFlatFusedFloatingProjection(op, direct_plan)) {
		return true;
	}

	auto projection_plans = BuildSljitProjectionPlans(op.projections);
	SljitNativeVectorFunction function = nullptr;
	string fused_error;
	auto code =
	    BuildSljitNativeFlatDoubleProjection(projection_plans, direct_plan.projection_indices, function, fused_error);
	if (!code || !function) {
		if (fused_error.empty() || fused_error.rfind("SLJIT flat floating projection", 0) == 0) {
			return true;
		}
		error = fused_error;
		return false;
	}
	op.flat_fused_floating_projection_plan = std::move(direct_plan);
	op.flat_fused_floating_projection_code = std::move(code);
	op.flat_fused_floating_projection_function = function;
	return true;
}

static bool SljitCanUseFlatFusedFixedProjection(const SljitExecutableRegionExpression &expr,
                                                SljitNativeIntegerKind integer_kind) {
	auto &plan = expr.plan;
	if (plan.integer_kind != integer_kind) {
		return false;
	}
	switch (integer_kind) {
	case SljitNativeIntegerKind::INT32:
		if (!expr.flat_function || plan.check_arithmetic_overflow || plan.check_result_range) {
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT32) {
			return false;
		}
		break;
	case SljitNativeIntegerKind::INT64:
		if (!expr.flat_function || plan.check_arithmetic_overflow || plan.check_result_range) {
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT64) {
			return false;
		}
		break;
	case SljitNativeIntegerKind::DECIMAL64:
		if (plan.return_type.id() != LogicalTypeId::DECIMAL || plan.return_type.InternalType() != PhysicalType::INT64) {
			return false;
		}
		break;
	case SljitNativeIntegerKind::DATE:
		if (plan.return_type.id() != LogicalTypeId::DATE || plan.return_type.InternalType() != PhysicalType::INT32) {
			return false;
		}
		break;
	default:
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		return false;
	}
}

static SljitDirectProjectionKind SljitDirectProjectionKindFromIntegerKind(SljitNativeIntegerKind integer_kind) {
	switch (integer_kind) {
	case SljitNativeIntegerKind::INT32:
		return SljitDirectProjectionKind::INT32;
	case SljitNativeIntegerKind::INT64:
		return SljitDirectProjectionKind::INT64;
	case SljitNativeIntegerKind::DECIMAL64:
		return SljitDirectProjectionKind::DECIMAL64;
	case SljitNativeIntegerKind::DATE:
		return SljitDirectProjectionKind::DATE;
	default:
		throw InternalException("Unsupported SLJIT fixed fused projection integer kind");
	}
}

static bool TryPlanFlatFusedFixedProjection(SljitExecutableRegionOp &op, SljitNativeIntegerKind integer_kind,
                                            SljitDirectProjectionPlan &direct_plan) {
	if (op.projections.size() < 2) {
		return false;
	}
	vector<idx_t> projection_indices;
	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &projection = op.projections[projection_idx];
		if (SljitCanUseFlatFusedFixedProjection(projection, integer_kind)) {
			projection_indices.push_back(projection_idx);
		}
	}
	if (projection_indices.empty()) {
		return false;
	}
	direct_plan = SljitDirectProjectionPlan();
	direct_plan.kind = SljitDirectProjectionKindFromIntegerKind(integer_kind);
	direct_plan.stats_mode = SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS;
	direct_plan.projection_indices = projection_indices;
	direct_plan.covers_all_projections = projection_indices.size() == op.projections.size();
	if (!TryPlanSljitDirectProjectionSources(op.projections, direct_plan)) {
		return false;
	}
	return true;
}

static bool TryBuildFlatFusedFixedProjection(SljitExecutableRegionOp &op, string &error) {
	static constexpr SljitNativeIntegerKind FIXED_FUSED_KINDS[] = {
	    SljitNativeIntegerKind::INT32, SljitNativeIntegerKind::INT64, SljitNativeIntegerKind::DECIMAL64,
	    SljitNativeIntegerKind::DATE};

	auto projection_plans = BuildSljitProjectionPlans(op.projections);
	for (auto integer_kind : FIXED_FUSED_KINDS) {
		SljitDirectProjectionPlan direct_plan;
		if (!TryPlanFlatFusedFixedProjection(op, integer_kind, direct_plan)) {
			continue;
		}

		SljitNativeVectorFunction function = nullptr;
		string fused_error;
		auto code = BuildSljitNativeFlatIntegerProjection(projection_plans, direct_plan.projection_indices, function,
		                                                  fused_error);
		if (!code || !function) {
			if (fused_error.empty() || fused_error.rfind("SLJIT flat integer projection", 0) == 0) {
				continue;
			}
			error = fused_error;
			return false;
		}
		op.flat_fused_fixed_projection_plans.push_back(std::move(direct_plan));
		op.flat_fused_fixed_projection_codes.push_back(std::move(code));
		op.flat_fused_fixed_projection_functions.push_back(function);
	}
	return true;
}

static void PrepareExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                              SljitExecutableRegionExpression &expr,
                                              const vector<bool> *input_not_null = nullptr,
                                              bool copy_auxiliary_expression_tree = false) {
	expr.plan = plan.Copy(copy_auxiliary_expression_tree, false);
	PrepareExecutableRegionExpressionInputs(expr, input_not_null);
}

static bool CompilePreparedExecutableRegionExpression(SljitExecutableRegionExpression &expr, bool require_boolean,
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

static bool PrepareAndCompileExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                        bool require_boolean, SljitExecutableRegionExpression &expr,
                                                        string &error, const vector<bool> *input_not_null = nullptr) {
	PrepareExecutableRegionExpression(plan, expr, input_not_null);
	return CompilePreparedExecutableRegionExpression(expr, require_boolean, error);
}

static unique_ptr<ExecutionExpressionIR>
SljitPayloadReferenceExpressionTree(const SljitNativeRegionExpressionPlan &plan) {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = ExecutionExpressionIRKind::REFERENCE;
	result->return_type = plan.return_type;
	result->physical_type = plan.return_type.InternalType();
	result->validity = ExecutionExpressionValidityKind::SOURCE;
	result->source = ExecutionExpressionSourceKind::VECTOR;
	result->exception_behavior = ExecutionExpressionExceptionKind::NONE;
	result->ref_index = 0;
	return result;
}

static bool NormalizeSljitFilteredAggregatePayloadExpression(SljitExecutableRegionExpression &payload,
                                                             vector<idx_t> &local_sources) {
	if (payload.plan.expression_tree) {
		local_sources = payload.input_source_indices.empty() ? payload.plan.expression_tree_source_indices
		                                                     : payload.input_source_indices;
		return true;
	}
	if (payload.plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
	    payload.plan.source_index == DConstants::INVALID_INDEX) {
		return false;
	}
	payload.plan.expression_tree = SljitPayloadReferenceExpressionTree(payload.plan);
	local_sources.clear();
	local_sources.push_back(payload.plan.source_index);
	payload.plan.expression_tree_source_indices = local_sources;
	payload.input_source_indices = local_sources;
	return true;
}

static bool TryBuildFilteredAggregateUpdate(SljitExecutableRegionOp &filter_op, SljitExecutableRegionOp &aggregate_op,
                                            string &error, const vector<bool> &input_not_null,
                                            const vector<Value> &input_min_values,
                                            const vector<Value> &input_max_values) {
	if (filter_op.kind != SljitNativeRegionOpKind::FILTER ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return true;
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	if (aggregate_update.filtered_update.IsExecutable() || !aggregate_update.plan.use_primitive_payloads ||
	    aggregate_update.payloads.empty() ||
	    aggregate_update.payloads.size() != aggregate_update.plan.sink_info.aggregates.size()) {
		return true;
	}
	if (!filter_op.filter.plan.expression_tree) {
		return true;
	}

	if (aggregate_update.plan.use_perfect_hash_group_lookup &&
	    aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		SljitExecutableFilteredAggregateUpdate filtered_update;
		filtered_update.filter.plan = filter_op.filter.plan.Copy(true, false);
		filtered_update.payloads.reserve(aggregate_update.payloads.size());
		for (auto &payload : aggregate_update.payloads) {
			SljitExecutableRegionExpression filtered_payload;
			filtered_payload.plan = payload.plan.Copy(true, false);
			filtered_update.payloads.push_back(std::move(filtered_payload));
		}
		if (!filtered_update.filter.plan.expression_tree) {
			return true;
		}

		vector<idx_t> combined_sources;
		vector<bool> combined_source_not_null;
		vector<Value> combined_source_min_values;
		vector<Value> combined_source_max_values;
		auto &filter_sources = filter_op.filter.input_source_indices.empty()
		                           ? filter_op.filter.plan.expression_tree_source_indices
		                           : filter_op.filter.input_source_indices;
		RemapSljitExpressionTreeToCombinedInputs(*filtered_update.filter.plan.expression_tree, filter_sources,
		                                         combined_sources, &combined_source_not_null, &input_not_null,
		                                         &combined_source_min_values, &combined_source_max_values,
		                                         &input_min_values, &input_max_values);

		bool has_typed_payload = false;
		for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
			auto &aggregate = aggregate_update.plan.sink_info.aggregates[payload_idx];
			auto &payload = filtered_update.payloads[payload_idx];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			if (payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				payload.plan.source_index = AddSljitCombinedInputSource(
				    payload.plan.source_index, combined_sources, &combined_source_not_null, &input_not_null,
				    &combined_source_min_values, &combined_source_max_values, &input_min_values, &input_max_values);
				continue;
			}
			if (payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
			    !payload.plan.expression_tree) {
				return true;
			}
			has_typed_payload = true;
			vector<idx_t> local_sources = payload.plan.expression_tree_source_indices;
			if (local_sources.empty()) {
				return true;
			}
			RemapSljitExpressionTreeToCombinedInputs(*payload.plan.expression_tree, local_sources, combined_sources,
			                                         &combined_source_not_null, &input_not_null,
			                                         &combined_source_min_values, &combined_source_max_values,
			                                         &input_min_values, &input_max_values);
		}
		if (!has_typed_payload) {
			return true;
		}

		filtered_update.input_source_indices = combined_sources;
		filtered_update.filter.input_source_indices = combined_sources;
		filtered_update.filter.plan.expression_tree_source_indices = combined_sources;
		vector<SljitNativeRegionExpressionPlan> codegen_payloads;
		codegen_payloads.reserve(filtered_update.payloads.size());
		for (auto &payload : filtered_update.payloads) {
			payload.input_source_indices = combined_sources;
			payload.plan.expression_tree_source_indices = combined_sources;
			codegen_payloads.push_back(payload.plan.Copy(true, false));
		}

		SljitNativeAggregateUpdateFunction function = nullptr;
		string filtered_error;
		auto code = BuildSljitNativeFilteredPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
		    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
		    aggregate_update.plan.sink_info.groups, aggregate_update.plan.group_expressions,
		    aggregate_update.plan.sink_info.aggregate_contract, combined_source_not_null, combined_source_min_values,
		    combined_source_max_values, function, filtered_error);
		if (code && function) {
			filtered_update.code = std::move(code);
			filtered_update.function = function;
			filtered_update.owns_perfect_hash_group_lookup = true;
			aggregate_update.filtered_update = std::move(filtered_update);
			return true;
		}
		if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
			error = filtered_error;
			return false;
		}
	}

	if (aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return true;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateKindCanGenerate(primitive_kind)) {
			return true;
		}
	}

	SljitExecutableFilteredAggregateUpdate filtered_update;
	filtered_update.filter.plan = filter_op.filter.plan.Copy(true, false);
	filtered_update.payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		SljitExecutableRegionExpression filtered_payload;
		filtered_payload.plan = payload.plan.Copy(true, false);
		filtered_update.payloads.push_back(std::move(filtered_payload));
	}
	if (!filtered_update.filter.plan.expression_tree) {
		return true;
	}

	vector<idx_t> combined_sources;
	auto &filter_sources = filter_op.filter.input_source_indices.empty()
	                           ? filter_op.filter.plan.expression_tree_source_indices
	                           : filter_op.filter.input_source_indices;
	RemapSljitExpressionTreeToCombinedInputs(*filtered_update.filter.plan.expression_tree, filter_sources,
	                                         combined_sources);
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.payloads.size(); payload_idx++) {
		auto primitive_kind = aggregate_update.plan.sink_info.aggregates[payload_idx].primitive_update_kind;
		if (!SljitFilteredAggregateUsesPayloadExpression(primitive_kind)) {
			continue;
		}
		vector<idx_t> payload_sources;
		if (!NormalizeSljitFilteredAggregatePayloadExpression(filtered_update.payloads[payload_idx], payload_sources)) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*filtered_update.payloads[payload_idx].plan.expression_tree,
		                                         payload_sources, combined_sources);
	}
	filtered_update.input_source_indices = combined_sources;
	filtered_update.filter.input_source_indices = combined_sources;
	filtered_update.filter.plan.expression_tree_source_indices = combined_sources;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	codegen_payloads.reserve(filtered_update.payloads.size());
	for (auto &payload : filtered_update.payloads) {
		payload.input_source_indices = combined_sources;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	SljitNativeAggregateUpdateFunction function = nullptr;
	string filtered_error;
	auto code = BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
	    *filtered_update.filter.plan.expression_tree, codegen_payloads, aggregate_update.plan.sink_info.aggregates,
	    function, filtered_error);

	if (code && function) {
		filtered_update.code = std::move(code);
		filtered_update.function = function;
		aggregate_update.filtered_update = std::move(filtered_update);
		return true;
	}
	if (!filtered_error.empty() && filtered_error.rfind("unsupported", 0) != 0) {
		error = filtered_error;
		return false;
	}
	return true;
}

static bool TryBuildUngroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                                 SljitExecutableAggregateUpdate &executable,
                                                                 string &error) {
	if (!op.use_primitive_payloads || op.use_grouped_state_addresses || op.payloads.size() < 2 ||
	    op.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	vector<SljitExecutableRegionExpression> fused_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	vector<idx_t> combined_sources;
	fused_payloads.reserve(op.payloads.size());
	codegen_payloads.reserve(op.payloads.size());
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &aggregate = op.sink_info.aggregates[payload_idx];
		SljitExecutableRegionExpression fused_payload;
		fused_payload.plan = op.payloads[payload_idx].Copy(true, false);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			fused_payload.plan.source_index =
			    AddSljitCombinedInputSource(fused_payload.plan.source_index, combined_sources);
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !fused_payload.plan.expression_tree) {
			return true;
		}
		has_typed_payload = true;
		vector<idx_t> local_sources = fused_payload.plan.expression_tree_source_indices;
		if (local_sources.empty()) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*fused_payload.plan.expression_tree, local_sources, combined_sources);
		fused_payloads.push_back(std::move(fused_payload));
	}
	if (!has_typed_payload) {
		return true;
	}

	for (auto &payload : fused_payloads) {
		payload.input_source_indices = combined_sources;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeUngroupedFusedTypedExpressionAggregateUpdate(
	    codegen_payloads, op.sink_info.aggregates, fused_function, fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(fused_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const SljitNativeAggregateUpdatePlan &op, SljitExecutableAggregateUpdate &executable, string &error,
    const vector<bool> &input_not_null, const vector<Value> &input_min_values, const vector<Value> &input_max_values) {
	if (!op.use_primitive_payloads || !op.use_perfect_hash_group_lookup || op.payloads.empty() ||
	    op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	vector<SljitExecutableRegionExpression> fused_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	vector<idx_t> combined_sources;
	vector<bool> combined_source_not_null;
	vector<Value> combined_source_min_values;
	vector<Value> combined_source_max_values;
	fused_payloads.reserve(op.payloads.size());
	codegen_payloads.reserve(op.payloads.size());
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &aggregate = op.sink_info.aggregates[payload_idx];
		SljitExecutableRegionExpression fused_payload;
		fused_payload.plan = op.payloads[payload_idx].Copy(true, false);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			fused_payload.plan.source_index = AddSljitCombinedInputSource(
			    fused_payload.plan.source_index, combined_sources, &combined_source_not_null, &input_not_null,
			    &combined_source_min_values, &combined_source_max_values, &input_min_values, &input_max_values);
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !fused_payload.plan.expression_tree) {
			return true;
		}
		has_typed_payload = true;
		vector<idx_t> local_sources = fused_payload.plan.expression_tree_source_indices;
		if (local_sources.empty()) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*fused_payload.plan.expression_tree, local_sources, combined_sources,
		                                         &combined_source_not_null, &input_not_null,
		                                         &combined_source_min_values, &combined_source_max_values,
		                                         &input_min_values, &input_max_values);
		fused_payloads.push_back(std::move(fused_payload));
	}
	if (!has_typed_payload) {
		return true;
	}

	for (auto &payload : fused_payloads) {
		payload.input_source_indices = combined_sources;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
	    codegen_payloads, op.sink_info.aggregates, op.sink_info.groups, op.group_expressions,
	    op.sink_info.aggregate_contract, combined_source_not_null, combined_source_min_values,
	    combined_source_max_values, fused_function, fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(fused_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		executable.fused_payload_update_owns_group_lookup = true;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static bool TryBuildGroupedFusedTypedExpressionAggregateUpdate(const SljitNativeAggregateUpdatePlan &op,
                                                               SljitExecutableAggregateUpdate &executable,
                                                               string &error) {
	if (!op.use_primitive_payloads || !op.use_grouped_state_addresses || op.use_perfect_hash_group_lookup ||
	    op.payloads.empty() || op.payloads.size() != op.sink_info.aggregates.size()) {
		return true;
	}

	vector<SljitExecutableRegionExpression> fused_payloads;
	vector<SljitNativeRegionExpressionPlan> codegen_payloads;
	vector<idx_t> combined_sources;
	fused_payloads.reserve(op.payloads.size());
	codegen_payloads.reserve(op.payloads.size());
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &aggregate = op.sink_info.aggregates[payload_idx];
		SljitExecutableRegionExpression fused_payload;
		fused_payload.plan = op.payloads[payload_idx].Copy(true, false);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			fused_payload.plan.source_index =
			    AddSljitCombinedInputSource(fused_payload.plan.source_index, combined_sources);
			fused_payloads.push_back(std::move(fused_payload));
			continue;
		}
		if (fused_payload.plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !fused_payload.plan.expression_tree) {
			return true;
		}
		has_typed_payload = true;
		vector<idx_t> local_sources = fused_payload.plan.expression_tree_source_indices;
		if (local_sources.empty()) {
			return true;
		}
		RemapSljitExpressionTreeToCombinedInputs(*fused_payload.plan.expression_tree, local_sources, combined_sources);
		fused_payloads.push_back(std::move(fused_payload));
	}
	if (!has_typed_payload) {
		return true;
	}

	for (auto &payload : fused_payloads) {
		payload.input_source_indices = combined_sources;
		payload.plan.expression_tree_source_indices = combined_sources;
		codegen_payloads.push_back(payload.plan.Copy(true, false));
	}

	SljitNativeAggregateUpdateFunction fused_function = nullptr;
	string fused_error;
	auto fused_code = BuildSljitNativeGroupedFusedTypedExpressionAggregateUpdate(
	    codegen_payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function, fused_error);
	if (fused_code && fused_function) {
		executable.payloads = std::move(fused_payloads);
		executable.fused_payload_update_code = std::move(fused_code);
		executable.fused_payload_update_function = fused_function;
		return true;
	}
	if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
		error = fused_error;
		return false;
	}
	return true;
}

static void BuildExecutableAggregateUpdateMetadata(const SljitNativeAggregateUpdatePlan &op,
                                                   SljitExecutableAggregateUpdate &executable) {
	executable.plan.sink_info = op.sink_info;
	executable.plan.input_types = op.input_types;
	executable.plan.use_primitive_payloads = op.use_primitive_payloads;
	executable.plan.use_grouped_state_addresses = op.use_grouped_state_addresses;
	executable.plan.use_perfect_hash_group_lookup = op.use_perfect_hash_group_lookup;
	executable.plan.group_expressions.reserve(op.group_expressions.size());
	for (auto &group_expression : op.group_expressions) {
		executable.plan.group_expressions.push_back(group_expression.Copy(true, false));
	}
	executable.payloads.reserve(op.payloads.size());
	for (auto &payload : op.payloads) {
		SljitExecutableRegionExpression executable_payload;
		executable_payload.plan = payload.Copy(true, false);
		executable.payloads.push_back(std::move(executable_payload));
	}
}

static bool BuildExecutableAggregateUpdatePayloadCode(const SljitNativeAggregateUpdatePlan &op,
                                                      SljitExecutableAggregateUpdate &executable, string &error,
                                                      const vector<bool> &input_not_null,
                                                      const vector<Value> &input_min_values,
                                                      const vector<Value> &input_max_values) {
	if (op.use_primitive_payloads && !op.use_grouped_state_addresses && op.payloads.size() > 1) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(op.payloads, op.sink_info.aggregates,
		                                                                         fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	if (!TryBuildUngroupedFusedTypedExpressionAggregateUpdate(op, executable, error)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (!TryBuildPerfectHashGroupedFusedTypedExpressionAggregateUpdate(op, executable, error, input_not_null,
	                                                                   input_min_values, input_max_values)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (!TryBuildGroupedFusedTypedExpressionAggregateUpdate(op, executable, error)) {
		return false;
	}
	if (executable.fused_payload_update_function) {
		return true;
	}
	if (op.use_primitive_payloads && op.use_perfect_hash_group_lookup && !op.payloads.empty()) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.groups, op.group_expressions,
		    op.sink_info.aggregate_contract, fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			executable.fused_payload_update_owns_group_lookup = true;
			return true;
		}
		if (!fused_error.empty()) {
			error = fused_error;
			return false;
		}
	}
	if (op.use_primitive_payloads && op.use_grouped_state_addresses && op.payloads.size() > 1) {
		SljitNativeAggregateUpdateFunction fused_function = nullptr;
		string fused_error;
		auto fused_code = BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
		    op.payloads, op.sink_info.aggregates, op.sink_info.aggregate_contract, fused_function, fused_error);
		if (fused_code && fused_function) {
			executable.fused_payload_update_code = std::move(fused_code);
			executable.fused_payload_update_function = fused_function;
			return true;
		}
		if (!fused_error.empty() && fused_error.rfind("unsupported", 0) != 0) {
			error = fused_error;
			return false;
		}
	}
	executable.payload_update_code.reserve(op.payloads.size());
	executable.payload_update_functions.reserve(op.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < op.payloads.size(); payload_idx++) {
		auto &payload = op.payloads[payload_idx];
		if (payload_idx >= op.sink_info.aggregates.size()) {
			error = "SLJIT aggregate update payload has no aggregate contract";
			return false;
		}
		auto primitive_kind = op.sink_info.aggregates[payload_idx].primitive_update_kind;
		SljitNativeAggregateUpdateFunction function = nullptr;
		unique_ptr<ExecutionRegionCodeHandle> code;
		if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (op.use_grouped_state_addresses) {
				code = BuildSljitNativeGroupedCountStar(function, error);
			} else {
				code = BuildSljitNativeUngroupedCountStar(function, error);
			}
			if (!code || !function) {
				if (error.empty()) {
					error = "SLJIT count-star aggregate update has no native primitive reducer";
				}
				return false;
			}
			executable.payload_update_code.push_back(std::move(code));
			executable.payload_update_functions.push_back(function);
			continue;
		}
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (op.use_grouped_state_addresses) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeGroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeGroupedSumHugeintReference(payload.integer_kind, function, error);
				} else {
					error = "SLJIT grouped aggregate reference reducer has no primitive state kind";
					return false;
				}
			} else {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64Reference(payload.integer_kind, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
					code = BuildSljitNativeUngroupedSumDoubleReference(payload.double_source_kind, function, error);
				} else {
					error = "SLJIT aggregate reference reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-constant reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
			    payload.integer_kind, payload.binary_op, payload.constant_on_left, function, error,
			    payload.check_arithmetic_overflow, payload.check_result_range, payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				error = "SLJIT aggregate binary-reference reducer only supports SUM_INT64";
				return false;
			}
			code = BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
			    payload.integer_kind, payload.binary_op, function, error, payload.check_arithmetic_overflow,
			    payload.check_result_range, payload.result_min, payload.result_max);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-constant reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-constant reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(
			    payload.double_binary_op, payload.double_source_kind, payload.constant_on_left, function, error);
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate double binary-reference reducer is not supported";
				return false;
			}
			if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				error = "SLJIT aggregate double binary-reference reducer only supports SUM_DOUBLE";
				return false;
			}
			code = BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
			    payload.double_binary_op, payload.double_source_kind, payload.double_right_source_kind, function,
			    error);
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64ExpressionTree(*payload.expression_tree, function, error);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintExpressionTree(*payload.expression_tree, function, error);
				} else {
					error = "SLJIT aggregate expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			if (op.use_grouped_state_addresses) {
				error = "SLJIT grouped aggregate typed expression-tree reducer is not supported";
				return false;
			}
			if (payload.expression_tree) {
				if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					code = BuildSljitNativeUngroupedSumInt64TypedExpressionTree(
					    *payload.expression_tree, function, error, payload.emit_flat_nullable_fast_path);
				} else if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
					code = BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(
					    *payload.expression_tree, function, error, payload.emit_flat_nullable_fast_path);
				} else {
					error = "SLJIT aggregate typed expression-tree reducer has no primitive state kind";
					return false;
				}
			}
			break;
		default:
			break;
		}
		if (!code || !function) {
			if (error.empty()) {
				error = "SLJIT aggregate update payload has no native primitive reducer";
			}
			return false;
		}
		executable.payload_update_code.push_back(std::move(code));
		executable.payload_update_functions.push_back(function);
	}
	return true;
}

static bool SljitExpressionResultNotNull(const SljitNativeRegionExpressionPlan &expr,
                                         const vector<bool> &input_not_null) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return !expr.constant_value.IsNull();
	case SljitNativeRegionExpressionKind::REFERENCE:
		return expr.source_index < input_not_null.size() && input_not_null[expr.source_index];
	default:
		return false;
	}
}

static vector<bool> BuildSljitProjectionOutputNotNull(const SljitNativeRegionOpPlan &op,
                                                      const vector<bool> &input_not_null) {
	vector<bool> result;
	result.reserve(op.projections.size());
	for (auto &projection : op.projections) {
		result.push_back(SljitExpressionResultNotNull(projection, input_not_null));
	}
	return result;
}

struct SljitExecutableInt128Range {
	hugeint_t min;
	hugeint_t max;
	LogicalType type;
};

static bool SljitExecutableValueToHugeint(const Value &value, const LogicalType &expected_type, hugeint_t &result) {
	if (value.IsNull() || value.type() != expected_type) {
		return false;
	}
	if (value.type().id() == LogicalTypeId::DATE) {
		result = Hugeint::Convert(value.GetValueUnsafe<date_t>().days);
		return true;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		result = Hugeint::Convert(value.GetValueUnsafe<bool>() ? 1 : 0);
		return true;
	case PhysicalType::INT8:
		result = Hugeint::Convert(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		result = Hugeint::Convert(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		result = Hugeint::Convert(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = Hugeint::Convert(value.GetValueUnsafe<int64_t>());
		return true;
	case PhysicalType::UINT8:
		result = Hugeint::Convert(value.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::UINT16:
		result = Hugeint::Convert(value.GetValueUnsafe<uint16_t>());
		return true;
	case PhysicalType::UINT32:
		result = Hugeint::Convert(value.GetValueUnsafe<uint32_t>());
		return true;
	case PhysicalType::UINT64:
		result = Hugeint::Convert(value.GetValueUnsafe<uint64_t>());
		return true;
	case PhysicalType::INT128:
		result = value.GetValueUnsafe<hugeint_t>();
		return true;
	default:
		return false;
	}
}

static bool SljitExecutableRangeFromInput(idx_t source_index, const LogicalType &expected_type,
                                          const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                          SljitExecutableInt128Range &result) {
	if (source_index >= input_min_values.size() || source_index >= input_max_values.size()) {
		return false;
	}
	result.type = expected_type;
	return SljitExecutableValueToHugeint(input_min_values[source_index], expected_type, result.min) &&
	       SljitExecutableValueToHugeint(input_max_values[source_index], expected_type, result.max) &&
	       result.min <= result.max;
}

static bool SljitExecutableRangeAdd(const SljitExecutableInt128Range &left, const SljitExecutableInt128Range &right,
                                    const LogicalType &result_type, SljitExecutableInt128Range &result) {
	result.type = result_type;
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TryAddInPlace(result.min, right.min) && Hugeint::TryAddInPlace(result.max, right.max);
}

static bool SljitExecutableRangeSubtract(const SljitExecutableInt128Range &left,
                                         const SljitExecutableInt128Range &right, const LogicalType &result_type,
                                         SljitExecutableInt128Range &result) {
	result.type = result_type;
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TrySubtractInPlace(result.min, right.max) && Hugeint::TrySubtractInPlace(result.max, right.min);
}

static bool SljitExecutableRangeMultiply(const SljitExecutableInt128Range &left,
                                         const SljitExecutableInt128Range &right, const LogicalType &result_type,
                                         SljitExecutableInt128Range &result) {
	hugeint_t values[4];
	if (!Hugeint::TryMultiply(left.min, right.min, values[0]) ||
	    !Hugeint::TryMultiply(left.min, right.max, values[1]) ||
	    !Hugeint::TryMultiply(left.max, right.min, values[2]) ||
	    !Hugeint::TryMultiply(left.max, right.max, values[3])) {
		return false;
	}
	result.type = result_type;
	result.min = values[0];
	result.max = values[0];
	for (idx_t value_idx = 1; value_idx < 4; value_idx++) {
		result.min = MinValue(result.min, values[value_idx]);
		result.max = MaxValue(result.max, values[value_idx]);
	}
	return true;
}

static bool SljitExecutableDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	const auto left_decimal = node.left->return_type.id() == LogicalTypeId::DECIMAL &&
	                          node.left->return_type.InternalType() == PhysicalType::INT64;
	const auto right_decimal = node.right->return_type.id() == LogicalTypeId::DECIMAL &&
	                           node.right->return_type.InternalType() == PhysicalType::INT64;
	if (!left_decimal && !right_decimal) {
		return true;
	}
	if (!left_decimal || !right_decimal || node.return_type.id() != LogicalTypeId::DECIMAL ||
	    node.return_type.InternalType() != PhysicalType::INT64) {
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

static bool SljitExecutableScaleRangeByPowerOfTen(SljitExecutableInt128Range &range, uint8_t scale_delta) {
	if (scale_delta == 0) {
		return true;
	}
	if (scale_delta >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto scale = Hugeint::Convert(NumericHelper::POWERS_OF_TEN[scale_delta]);
	return Hugeint::TryMultiply(range.min, scale, range.min) && Hugeint::TryMultiply(range.max, scale, range.max);
}

static bool SljitExecutableRangeCast(const ExecutionExpressionIR &node, const SljitExecutableInt128Range &child,
                                     SljitExecutableInt128Range &result) {
	if (!node.left) {
		return false;
	}
	result = child;
	result.type = node.return_type;
	if (node.return_type.InternalType() == node.left->return_type.InternalType()) {
		if (node.return_type.id() == LogicalTypeId::DECIMAL || node.left->return_type.id() == LogicalTypeId::DECIMAL) {
			if (node.return_type.id() != LogicalTypeId::DECIMAL ||
			    node.left->return_type.id() != LogicalTypeId::DECIMAL) {
				return false;
			}
			auto source_scale = DecimalType::GetScale(node.left->return_type);
			auto target_scale = DecimalType::GetScale(node.return_type);
			if (target_scale < source_scale) {
				return false;
			}
			return SljitExecutableScaleRangeByPowerOfTen(result, target_scale - source_scale);
		}
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DECIMAL && node.return_type.InternalType() == PhysicalType::INT64 &&
	    node.left->return_type.IsIntegral()) {
		return SljitExecutableScaleRangeByPowerOfTen(result, DecimalType::GetScale(node.return_type));
	}
	if (node.return_type.IsIntegral() && node.left->return_type.IsIntegral()) {
		return true;
	}
	return false;
}

static bool SljitExecutableExpressionTreeRange(const ExecutionExpressionIR &node, const vector<Value> &input_min_values,
                                               const vector<Value> &input_max_values,
                                               SljitExecutableInt128Range &result) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		result.type = node.return_type;
		return SljitExecutableValueToHugeint(node.constant, node.return_type, result.min) &&
		       SljitExecutableValueToHugeint(node.constant, node.return_type, result.max);
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitExecutableRangeFromInput(node.ref_index, node.return_type, input_min_values, input_max_values,
		                                     result);
	case ExecutionExpressionIRKind::CAST: {
		if (!node.left) {
			return false;
		}
		SljitExecutableInt128Range child;
		return SljitExecutableExpressionTreeRange(*node.left, input_min_values, input_max_values, child) &&
		       SljitExecutableRangeCast(node, child, result);
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right || !SljitExecutableDecimal64BinaryHasRawSemantics(node)) {
			return false;
		}
		SljitExecutableInt128Range left;
		SljitExecutableInt128Range right;
		if (!SljitExecutableExpressionTreeRange(*node.left, input_min_values, input_max_values, left) ||
		    !SljitExecutableExpressionTreeRange(*node.right, input_min_values, input_max_values, right)) {
			return false;
		}
		switch (node.binary_op) {
		case ExecutionExpressionBinaryOp::ADD:
			return SljitExecutableRangeAdd(left, right, node.return_type, result);
		case ExecutionExpressionBinaryOp::SUBTRACT:
			return SljitExecutableRangeSubtract(left, right, node.return_type, result);
		case ExecutionExpressionBinaryOp::MULTIPLY:
			return SljitExecutableRangeMultiply(left, right, node.return_type, result);
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool SljitExecutableRangeValue(const LogicalType &type, const hugeint_t &input, Value &result) {
	if (type.id() == LogicalTypeId::DATE) {
		int32_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = Value::DATE(date_t(value));
		return true;
	}
	switch (type.InternalType()) {
	case PhysicalType::INT8: {
		int8_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = Value::TINYINT(value);
		return true;
	}
	case PhysicalType::INT16: {
		int16_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::SMALLINT(value);
		return true;
	}
	case PhysicalType::INT32: {
		int32_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::INTEGER(value);
		return true;
	}
	case PhysicalType::INT64: {
		int64_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::BIGINT(value);
		return true;
	}
	case PhysicalType::INT128:
		if (type.id() != LogicalTypeId::DECIMAL) {
			result = Value::HUGEINT(input);
		} else {
			result = Value::DECIMAL(input, DecimalType::GetWidth(type), DecimalType::GetScale(type));
		}
		return true;
	default:
		return false;
	}
}

static bool SljitExpressionRangeValues(const SljitNativeRegionExpressionPlan &expr,
                                       const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                       Value &min_value, Value &max_value) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (expr.constant_value.IsNull() || expr.constant_value.type() != expr.return_type) {
			return false;
		}
		min_value = expr.constant_value;
		max_value = expr.constant_value;
		return true;
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (expr.source_index >= input_min_values.size() || expr.source_index >= input_max_values.size() ||
		    input_min_values[expr.source_index].IsNull() || input_max_values[expr.source_index].IsNull() ||
		    input_min_values[expr.source_index].type() != expr.return_type ||
		    input_max_values[expr.source_index].type() != expr.return_type) {
			return false;
		}
		min_value = input_min_values[expr.source_index];
		max_value = input_max_values[expr.source_index];
		return true;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!expr.expression_tree) {
			return false;
		}
		SljitExecutableInt128Range range;
		if (!SljitExecutableExpressionTreeRange(*expr.expression_tree, input_min_values, input_max_values, range)) {
			return false;
		}
		return SljitExecutableRangeValue(expr.return_type, range.min, min_value) &&
		       SljitExecutableRangeValue(expr.return_type, range.max, max_value);
	}
	default:
		return false;
	}
}

static vector<Value> BuildSljitProjectionOutputRanges(const SljitNativeRegionOpPlan &op,
                                                      const vector<Value> &input_min_values,
                                                      const vector<Value> &input_max_values, bool min_values) {
	vector<Value> result;
	result.reserve(op.projections.size());
	for (auto &projection : op.projections) {
		Value min_value;
		Value max_value;
		if (!SljitExpressionRangeValues(projection, input_min_values, input_max_values, min_value, max_value)) {
			result.push_back(Value());
			continue;
		}
		result.push_back(min_values ? min_value : max_value);
	}
	return result;
}

static void UpdateSljitCurrentNotNull(const SljitNativeRegionOpPlan &op, vector<bool> &current_not_null) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION:
		current_not_null = BuildSljitProjectionOutputNotNull(op, current_not_null);
		return;
	default:
		current_not_null.assign(op.output_types.size(), false);
		return;
	}
}

static void UpdateSljitCurrentRanges(const SljitNativeRegionOpPlan &op, vector<Value> &current_min_values,
                                     vector<Value> &current_max_values) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION: {
		auto next_min_values = BuildSljitProjectionOutputRanges(op, current_min_values, current_max_values, true);
		auto next_max_values = BuildSljitProjectionOutputRanges(op, current_min_values, current_max_values, false);
		current_min_values = std::move(next_min_values);
		current_max_values = std::move(next_max_values);
		return;
	}
	default:
		current_min_values.assign(op.output_types.size(), Value());
		current_max_values.assign(op.output_types.size(), Value());
		return;
	}
}

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error, const vector<bool> &input_not_null,
                                    const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                    bool build_filter_code = true, bool build_aggregate_update_payload_code = true) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.output_types = op.output_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		PrepareExecutableRegionExpression(op.filter, executable.filter, &input_not_null, !build_filter_code);
		if (!build_filter_code) {
			return true;
		}
		return CompilePreparedExecutableRegionExpression(executable.filter, true, error);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.hash_join_probe.plan = op.hash_join_probe.Copy(false);
		if (op.hash_join_probe.residual_predicate &&
		    !PrepareAndCompileExecutableRegionExpression(op.hash_join_probe.residual_filter, true,
		                                                 executable.hash_join_probe.residual_filter, error,
		                                                 &op.hash_join_probe.residual_source_not_null)) {
			return false;
		}
		return ValidateSljitHashJoinProbe(op.hash_join_probe.keys, op.hash_join_probe.equality_key_count,
		                                  op.hash_join_probe.output_mode, error);
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.hash_join_build.plan.sink_info = op.hash_join_build.sink_info;
		executable.hash_join_build.plan.input_types = op.hash_join_build.input_types;
		return true;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		executable.nested_loop_join_probe.plan.operator_index = op.nested_loop_join_probe.operator_index;
		executable.nested_loop_join_probe.plan.input_types = op.nested_loop_join_probe.input_types;
		executable.nested_loop_join_probe.plan.condition_types = op.nested_loop_join_probe.condition_types;
		executable.nested_loop_join_probe.plan.join_type = op.nested_loop_join_probe.join_type;
		executable.nested_loop_join_probe.plan.operator_info = op.nested_loop_join_probe.operator_info;
		executable.nested_loop_join_probe.plan.conditions.reserve(op.nested_loop_join_probe.conditions.size());
		executable.nested_loop_join_probe.lhs_conditions.reserve(op.nested_loop_join_probe.conditions.size());
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
			condition_plan.type = condition.type;
			condition_plan.comparison_type = condition.comparison_type;
			condition_plan.value_kind = condition.value_kind;
			condition_plan.lhs_condition = condition.lhs_condition.Copy(false, false);
			executable.nested_loop_join_probe.plan.conditions.push_back(std::move(condition_plan));

			SljitExecutableRegionExpression executable_condition;
			if (!PrepareAndCompileExecutableRegionExpression(condition.lhs_condition, false, executable_condition,
			                                                 error, &input_not_null)) {
				return false;
			}
			executable.nested_loop_join_probe.lhs_conditions.push_back(std::move(executable_condition));
		}
		executable.nested_loop_join_probe.code = BuildSljitNestedLoopJoinProbe(
		    executable.nested_loop_join_probe.plan, executable.nested_loop_join_probe.function, error);
		return executable.nested_loop_join_probe.code != nullptr &&
		       executable.nested_loop_join_probe.function != nullptr;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		executable.nested_loop_join_build.plan.sink_info = op.nested_loop_join_build.sink_info;
		executable.nested_loop_join_build.plan.input_types = op.nested_loop_join_build.input_types;
		executable.nested_loop_join_build.plan.condition_types = op.nested_loop_join_build.condition_types;
		executable.nested_loop_join_build.rhs_conditions.reserve(op.nested_loop_join_build.rhs_conditions.size());
		for (auto &condition : op.nested_loop_join_build.rhs_conditions) {
			SljitExecutableRegionExpression executable_condition;
			if (!PrepareAndCompileExecutableRegionExpression(condition, false, executable_condition, error,
			                                                 &input_not_null)) {
				return false;
			}
			executable.nested_loop_join_build.rhs_conditions.push_back(std::move(executable_condition));
		}
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		executable.order_sink.plan.sink_info = op.order_sink.sink_info;
		executable.order_sink.plan.input_types = op.order_sink.input_types;
		executable.order_sink.plan.key_types = op.order_sink.key_types;
		executable.order_sink.order_keys.reserve(op.order_sink.order_keys.size());
		for (auto &order_key : op.order_sink.order_keys) {
			SljitExecutableRegionExpression executable_order_key;
			if (!PrepareAndCompileExecutableRegionExpression(order_key, false, executable_order_key, error,
			                                                 &input_not_null)) {
				return false;
			}
			executable.order_sink.order_keys.push_back(std::move(executable_order_key));
		}
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK:
		executable.append_sink.plan.sink_info = op.append_sink.sink_info;
		executable.append_sink.plan.input_types = op.append_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		if (op.delim_join_sink.sink_info.kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			error = "SLJIT delimiter join sink executable is missing delimiter sink info";
			return false;
		}
		executable.delim_join_sink.plan.sink_info = op.delim_join_sink.sink_info;
		executable.delim_join_sink.plan.input_types = op.delim_join_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		BuildExecutableAggregateUpdateMetadata(op.aggregate_update, executable.aggregate_update);
		if (!build_aggregate_update_payload_code) {
			return true;
		}
		return BuildExecutableAggregateUpdatePayloadCode(op.aggregate_update, executable.aggregate_update, error,
		                                                 input_not_null, input_min_values, input_max_values);
	case SljitNativeRegionOpKind::PROJECTION:
		executable.projections.reserve(op.projections.size());
		for (auto &projection : op.projections) {
			SljitExecutableRegionExpression executable_projection;
			if (!PrepareAndCompileExecutableRegionExpression(projection, false, executable_projection, error,
			                                                 &input_not_null)) {
				return false;
			}
			executable.projections.push_back(std::move(executable_projection));
		}
		if (!TryBuildFlatFusedFloatingProjection(executable, error)) {
			return false;
		}
		if (!TryBuildFlatFusedFixedProjection(executable, error)) {
			return false;
		}
		return true;
	default:
		throw InternalException("Unknown SLJIT native region operator kind");
	}
}

static bool SljitCanDeferAggregateUpdatePayloadCode(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx == 0 || op_idx + 1 != ops.size() || ops[op_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[op_idx - 1].kind == SljitNativeRegionOpKind::FILTER;
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error) {
	executable.source_distinct_counts = region.source_distinct_counts;
	executable.source_min_values = region.source_min_values;
	executable.source_max_values = region.source_max_values;
	executable.ops.reserve(region.ops.size());
	auto current_not_null = region.source_not_null;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		SljitExecutableRegionOp executable_op;
		auto defer_aggregate_payload_code = SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx);
		const auto defer_filter_code = op.kind == SljitNativeRegionOpKind::FILTER && op_idx + 1 < region.ops.size() &&
		                               SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx + 1);
		if (!BuildExecutableRegionOp(op, executable_op, error, current_not_null, current_min_values, current_max_values,
		                             !defer_filter_code, !defer_aggregate_payload_code)) {
			return false;
		}
		executable.ops.push_back(std::move(executable_op));
		if (defer_aggregate_payload_code) {
			auto &aggregate_update_op = executable.ops[op_idx];
			if (!TryBuildFilteredAggregateUpdate(executable.ops[op_idx - 1], aggregate_update_op, error,
			                                     current_not_null, current_min_values, current_max_values)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable() &&
			    !CompilePreparedExecutableRegionExpression(executable.ops[op_idx - 1].filter, true, error)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable()) {
				if (!BuildExecutableAggregateUpdatePayloadCode(
				        op.aggregate_update, aggregate_update_op.aggregate_update, error, current_not_null,
				        current_min_values, current_max_values)) {
					return false;
				}
			}
		}
		UpdateSljitCurrentNotNull(op, current_not_null);
		UpdateSljitCurrentRanges(op, current_min_values, current_max_values);
	}
	return true;
}

} // namespace duckdb

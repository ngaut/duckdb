//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_projection_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_expression_codegen.hpp"

#include "sljit_native_codegen.hpp"
#include "sljit_native_double_source_helpers.hpp"

namespace duckdb {

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
	if (!expr.flat.Function()) {
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
	auto compiled = SljitCompiledFunction<SljitNativeVectorFunction>::TryCreate(std::move(code), function);
	if (!compiled.IsExecutable()) {
		if (fused_error.empty() || fused_error.rfind("SLJIT flat floating projection", 0) == 0) {
			return true;
		}
		error = fused_error;
		return false;
	}
	op.flat_fused_floating_projection_plan = std::move(direct_plan);
	op.flat_fused_floating_projection = std::move(compiled);
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
		if (!expr.flat.Function() || plan.check_arithmetic_overflow || plan.check_result_range) {
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT32) {
			return false;
		}
		break;
	case SljitNativeIntegerKind::INT64:
		if (!expr.flat.Function() || plan.check_arithmetic_overflow || plan.check_result_range) {
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
		op.flat_fused_fixed_projections.emplace_back(std::move(code), function);
	}
	return true;
}

bool SljitTryBuildFlatFusedProjections(SljitExecutableRegionOp &op, string &error) {
	return TryBuildFlatFusedFloatingProjection(op, error) && TryBuildFlatFusedFixedProjection(op, error);
}

} // namespace duckdb

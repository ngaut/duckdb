//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_executable_expression_codegen.hpp"
#include "sljit_projection_composition.hpp"

#include "duckdb/common/types.hpp"

namespace duckdb {

struct SljitProjectionChainPrimitive {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	shared_ptr<SljitExecutableRegionOp> bound_composed_projection;

	bool HasBoundComposedProjection() const {
		return bound_composed_projection != nullptr;
	}
};

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker = nullptr);

static bool SljitBuildProjectionChainSemanticProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker = nullptr);

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                 idx_t final_projection_idx) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx) {
		return false;
	}
	for (idx_t op_idx = first_projection_idx; op_idx <= final_projection_idx; op_idx++) {
		if (ops[op_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	return true;
}

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx) {
	return SljitCanBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static bool SljitTryBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                 idx_t final_projection_idx, SljitProjectionChainPrimitive &primitive) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		return false;
	}
	SljitProjectionChainPrimitive candidate;
	candidate.first_projection_idx = first_projection_idx;
	candidate.final_projection_idx = final_projection_idx;
	if (first_projection_idx != final_projection_idx) {
		auto composed_projection = make_shared_ptr<SljitExecutableRegionOp>();
		if (SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
		                                                *composed_projection)) {
			candidate.bound_composed_projection = std::move(composed_projection);
		}
	}
	primitive = std::move(candidate);
	return true;
}

static bool SljitTryBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                                 SljitProjectionChainPrimitive &primitive) {
	return SljitTryBindProjectionChainPrimitive(ops, projection_idx, projection_idx, primitive);
}

static bool SljitBuildProjectionChainSemanticProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx ||
	    ops[first_projection_idx].Kind() != SljitNativeRegionOpKind::PROJECTION ||
	    ops[final_projection_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
		if (blocker) {
			*blocker = "shape";
		}
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> current_projection;
	auto &first_projection = ops[first_projection_idx];
	current_projection.reserve(first_projection.Projections().size());
	for (auto &projection : first_projection.Projections()) {
		current_projection.push_back(SljitCopySemanticExpressionPlan(projection));
	}

	for (idx_t projection_idx = first_projection_idx + 1; projection_idx <= final_projection_idx; projection_idx++) {
		auto &projection_op = ops[projection_idx];
		if (projection_op.Kind() != SljitNativeRegionOpKind::PROJECTION) {
			if (blocker) {
				*blocker = "chain_shape";
			}
			return false;
		}
		vector<SljitNativeRegionExpressionPlan> next_projection;
		next_projection.reserve(projection_op.Projections().size());
		for (idx_t output_idx = 0; output_idx < projection_op.Projections().size(); output_idx++) {
			auto &projection = projection_op.Projections()[output_idx];
			auto composed = make_uniq<SljitNativeRegionExpressionPlan>();
			if (!TryComposeNativeSemanticProjection(current_projection, SljitCopySemanticExpressionPlan(projection),
			                                        *composed, false)) {
				if (blocker) {
					*blocker = "compose_output_" + to_string(output_idx);
				}
				return false;
			}
			if (output_idx >= projection_op.output_types.size() ||
			    composed->return_type != projection_op.output_types[output_idx]) {
				if (blocker) {
					*blocker = "return_type";
				}
				return false;
			}
			next_projection.push_back(std::move(*composed));
		}
		current_projection = std::move(next_projection);
	}

	auto &final_projection = ops[final_projection_idx];
	composed_projection = SljitExecutableRegionOp();
	composed_projection.SetKind(SljitNativeRegionOpKind::PROJECTION);
	composed_projection.operator_index = final_projection.operator_index;
	composed_projection.input_types = ops[first_projection_idx].input_types;
	composed_projection.input_not_null = ops[first_projection_idx].input_not_null;
	composed_projection.input_min_values = ops[first_projection_idx].input_min_values;
	composed_projection.input_max_values = ops[first_projection_idx].input_max_values;
	composed_projection.output_types = final_projection.output_types;
	composed_projection.output_not_null = final_projection.output_not_null;
	composed_projection.Projections().reserve(current_projection.size());
	for (idx_t output_idx = 0; output_idx < current_projection.size(); output_idx++) {
		auto &projection_plan = current_projection[output_idx];
		auto projection = make_uniq<SljitExecutableRegionExpression>();
		SljitPrepareExecutableRegionExpression(projection_plan, *projection, nullptr, true);
		composed_projection.Projections().push_back(std::move(*projection));
	}
	return composed_projection.Projections().size() == final_projection.Projections().size();
}

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker) {
	if (!SljitBuildProjectionChainSemanticProjection(ops, first_projection_idx, final_projection_idx,
	                                                 composed_projection, blocker)) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < composed_projection.Projections().size(); output_idx++) {
		string compile_error;
		if (!SljitCompilePreparedExecutableRegionExpression(composed_projection.Projections()[output_idx], false,
		                                                    compile_error)) {
			if (blocker) {
				*blocker = "compile_output_" + to_string(output_idx);
			}
			return false;
		}
	}
	return true;
}

} // namespace duckdb

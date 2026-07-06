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

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                 idx_t final_projection_idx) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx) {
		return false;
	}
	for (idx_t op_idx = first_projection_idx; op_idx <= final_projection_idx; op_idx++) {
		if (ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	return true;
}

static bool SljitCanBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx) {
	return SljitCanBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static SljitProjectionChainPrimitive SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t first_projection_idx,
                                                                       idx_t final_projection_idx) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		throw InternalException("SLJIT projection-chain primitive cannot bind requested operator");
	}
	SljitProjectionChainPrimitive primitive;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
	if (first_projection_idx != final_projection_idx) {
		auto composed_projection = make_shared_ptr<SljitExecutableRegionOp>();
		if (SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx,
		                                                *composed_projection)) {
			primitive.bound_composed_projection = std::move(composed_projection);
		}
	}
	return primitive;
}

static SljitProjectionChainPrimitive
SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                  shared_ptr<SljitExecutableRegionOp> bound_projection) {
	if (!SljitCanBindProjectionChainPrimitive(ops, projection_idx) || !bound_projection ||
	    bound_projection->kind != SljitNativeRegionOpKind::PROJECTION) {
		throw InternalException("SLJIT projection-chain primitive cannot bind requested projection override");
	}
	SljitProjectionChainPrimitive primitive;
	primitive.first_projection_idx = projection_idx;
	primitive.final_projection_idx = projection_idx;
	primitive.bound_composed_projection = std::move(bound_projection);
	return primitive;
}

static SljitProjectionChainPrimitive SljitBindProjectionChainPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                       idx_t projection_idx) {
	return SljitBindProjectionChainPrimitive(ops, projection_idx, projection_idx);
}

static bool SljitOutputTypesAreFixedWidth(const vector<LogicalType> &types) {
	for (auto &type : types) {
		if (!TypeIsConstantSize(type.InternalType())) {
			return false;
		}
	}
	return true;
}

static bool SljitProjectionOutputsAreFixedWidth(const SljitExecutableRegionOp &projection_op) {
	return projection_op.kind == SljitNativeRegionOpKind::PROJECTION &&
	       SljitOutputTypesAreFixedWidth(projection_op.output_types);
}

static bool SljitBuildReferenceProjectionOutputMap(const SljitExecutableRegionOp &projection_op,
                                                   const SljitExecutableRegionOp &reference_projection_op,
                                                   vector<idx_t> &output_to_projection) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    reference_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    !SljitProjectionOutputsAreFixedWidth(reference_projection_op)) {
		return false;
	}
	output_to_projection.clear();
	output_to_projection.reserve(reference_projection_op.projections.size());
	for (auto &reference_expr : reference_projection_op.projections) {
		auto &reference = reference_expr.plan;
		if (reference.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    reference.source_index >= projection_op.projections.size() ||
		    reference.return_type != projection_op.projections[reference.source_index].plan.return_type) {
			output_to_projection.clear();
			return false;
		}
		output_to_projection.push_back(reference.source_index);
	}
	return true;
}

static bool SljitBuildProjectionChainComposedProjection(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        SljitExecutableRegionOp &composed_projection,
                                                        optional_ptr<string> blocker) {
	if (first_projection_idx >= ops.size() || final_projection_idx >= ops.size() ||
	    first_projection_idx > final_projection_idx ||
	    ops[first_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION ||
	    ops[final_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		if (blocker) {
			*blocker = "shape";
		}
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> current_projection;
	auto &first_projection = ops[first_projection_idx];
	current_projection.reserve(first_projection.projections.size());
	for (auto &projection : first_projection.projections) {
		current_projection.push_back(projection.plan.Copy(true, false));
	}

	for (idx_t projection_idx = first_projection_idx + 1; projection_idx <= final_projection_idx; projection_idx++) {
		auto &projection_op = ops[projection_idx];
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
			if (blocker) {
				*blocker = "chain_shape";
			}
			return false;
		}
		vector<SljitNativeRegionExpressionPlan> next_projection;
		next_projection.reserve(projection_op.projections.size());
		for (idx_t output_idx = 0; output_idx < projection_op.projections.size(); output_idx++) {
			auto &projection = projection_op.projections[output_idx];
			auto composed = make_uniq<SljitNativeRegionExpressionPlan>();
			if (!TryComposeNativeProjection(current_projection, projection.plan, *composed, false)) {
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
	composed_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	composed_projection.operator_index = final_projection.operator_index;
	composed_projection.input_types = ops[first_projection_idx].input_types;
	composed_projection.output_types = final_projection.output_types;
	composed_projection.output_not_null = final_projection.output_not_null;
	composed_projection.projections.reserve(current_projection.size());
	for (idx_t output_idx = 0; output_idx < current_projection.size(); output_idx++) {
		auto &projection_plan = current_projection[output_idx];
		auto projection = make_uniq<SljitExecutableRegionExpression>();
		SljitPrepareExecutableRegionExpression(projection_plan, *projection, nullptr, true);
		string compile_error;
		if (!SljitCompilePreparedExecutableRegionExpression(*projection, false, compile_error)) {
			if (blocker) {
				*blocker = "compile_output_" + to_string(output_idx);
			}
			return false;
		}
		composed_projection.projections.push_back(std::move(*projection));
	}
	return composed_projection.projections.size() == final_projection.projections.size();
}

} // namespace duckdb

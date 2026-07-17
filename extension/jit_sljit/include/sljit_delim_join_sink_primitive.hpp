//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_delim_join_sink_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_projection_chain_primitive.hpp"
#include "sljit_projection_source_runtime.hpp"

namespace duckdb {

struct SljitDelimJoinSinkPrimitive {
	idx_t first_projection_idx = DConstants::INVALID_INDEX;
	idx_t final_projection_idx = DConstants::INVALID_INDEX;
	idx_t sink_idx = DConstants::INVALID_INDEX;
	idx_t selected_hash_join_idx = DConstants::INVALID_INDEX;
	shared_ptr<SljitExecutableRegionOp> bound_projection;

	bool HasProjection() const {
		return first_projection_idx != DConstants::INVALID_INDEX;
	}

	bool HasSelectedHashJoinInput() const {
		return selected_hash_join_idx != DConstants::INVALID_INDEX;
	}
};

static bool SljitProjectionIsReferencePreserving(const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		idx_t source_idx;
		if (!SljitTryGetSingleSourceReferenceProjectionIndex(projection_op.projections[projection_idx], source_idx) ||
		    source_idx >= projection_op.input_types.size() ||
		    projection_op.input_types[source_idx] != projection_op.output_types[projection_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildReferencePreservingProjectionChain(const vector<SljitExecutableRegionOp> &ops,
                                                            idx_t first_projection_idx, idx_t final_projection_idx,
                                                            SljitExecutableRegionOp &projection_op) {
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		return false;
	}
	if (!SljitBuildProjectionChainComposedProjection(ops, first_projection_idx, final_projection_idx, projection_op)) {
		return false;
	}
	return SljitProjectionIsReferencePreserving(projection_op);
}

static bool SljitTryBindProjectedDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        idx_t sink_idx, SljitDelimJoinSinkPrimitive &primitive) {
	if (sink_idx >= ops.size() || sink_idx != ops.size() - 1 ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK || first_projection_idx > final_projection_idx ||
	    final_projection_idx + 1 != sink_idx) {
		return false;
	}
	auto projection_op = make_uniq<SljitExecutableRegionOp>();
	if (!SljitTryBuildReferencePreservingProjectionChain(ops, first_projection_idx, final_projection_idx,
	                                                     *projection_op)) {
		return false;
	}
	if (projection_op->output_types != ops[sink_idx].delim_join_sink.plan.input_types) {
		return false;
	}
	SljitDelimJoinSinkPrimitive candidate;
	candidate.first_projection_idx = first_projection_idx;
	candidate.final_projection_idx = final_projection_idx;
	candidate.sink_idx = sink_idx;
	candidate.bound_projection = std::move(projection_op);
	primitive = std::move(candidate);
	return true;
}

static bool SljitTryBindSelectedHashJoinDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                               const SljitHashJoinProbeSelectionPrimitive &selection,
                                                               idx_t sink_idx, SljitDelimJoinSinkPrimitive &primitive) {
	const auto hash_join_idx = selection.hash_join_idx;
	if (sink_idx >= ops.size() || sink_idx != ops.size() - 1 || hash_join_idx >= sink_idx ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK ||
	    ops[hash_join_idx].output_types != ops[sink_idx].delim_join_sink.plan.input_types) {
		return false;
	}
	SljitDelimJoinSinkPrimitive candidate;
	candidate.sink_idx = sink_idx;
	candidate.selected_hash_join_idx = hash_join_idx;
	primitive = candidate;
	return true;
}

} // namespace duckdb

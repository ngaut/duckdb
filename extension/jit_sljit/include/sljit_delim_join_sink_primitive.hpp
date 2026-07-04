//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_delim_join_sink_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_projection_chain_runtime.hpp"

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

static bool SljitCanBindProjectedDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t first_projection_idx, idx_t final_projection_idx,
                                                        idx_t sink_idx) {
	if (sink_idx >= ops.size() || sink_idx != ops.size() - 1 ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK || first_projection_idx > final_projection_idx ||
	    final_projection_idx + 1 != sink_idx) {
		return false;
	}
	SljitExecutableRegionOp projection_op;
	if (!SljitTryBuildReferencePreservingProjectionChain(ops, first_projection_idx, final_projection_idx,
	                                                     projection_op)) {
		return false;
	}
	return projection_op.output_types == ops[sink_idx].delim_join_sink.plan.input_types;
}

static bool SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                               idx_t hash_join_idx, idx_t sink_idx) {
	if (sink_idx >= ops.size() || sink_idx != ops.size() - 1 || hash_join_idx >= sink_idx ||
	    ops[sink_idx].kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return false;
	}
	return ops[hash_join_idx].output_types == ops[sink_idx].delim_join_sink.plan.input_types;
}

static SljitDelimJoinSinkPrimitive
SljitBindSelectedHashJoinDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                idx_t sink_idx) {
	if (!SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, hash_join_idx, sink_idx)) {
		throw InternalException("SLJIT delimiter join sink primitive cannot bind selected hash-join input");
	}
	SljitDelimJoinSinkPrimitive primitive;
	primitive.sink_idx = sink_idx;
	primitive.selected_hash_join_idx = hash_join_idx;
	return primitive;
}

static SljitDelimJoinSinkPrimitive SljitBindProjectedDelimJoinSinkPrimitive(
    const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t sink_idx) {
	if (!SljitCanBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx, sink_idx)) {
		throw InternalException("SLJIT delimiter join sink primitive cannot bind requested projection tail");
	}
	auto projection_op = make_shared_ptr<SljitExecutableRegionOp>();
	if (!SljitTryBuildReferencePreservingProjectionChain(ops, first_projection_idx, final_projection_idx,
	                                                     *projection_op)) {
		throw InternalException("SLJIT delimiter join sink primitive failed to build its projection contract");
	}
	SljitDelimJoinSinkPrimitive primitive;
	primitive.first_projection_idx = first_projection_idx;
	primitive.final_projection_idx = final_projection_idx;
	primitive.sink_idx = sink_idx;
	primitive.bound_projection = std::move(projection_op);
	return primitive;
}

static bool SljitCanBindDelimJoinSinkPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                               const SljitDelimJoinSinkPrimitive &primitive) {
	if (primitive.sink_idx >= ops.size() || primitive.sink_idx != ops.size() - 1 ||
	    ops[primitive.sink_idx].kind != SljitNativeRegionOpKind::DELIM_JOIN_SINK) {
		return false;
	}
	if (!primitive.HasProjection()) {
		if (!primitive.HasSelectedHashJoinInput()) {
			return true;
		}
		return SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, primitive.selected_hash_join_idx,
		                                                          primitive.sink_idx);
	}
	if (!primitive.bound_projection) {
		return false;
	}
	return SljitCanBindProjectedDelimJoinSinkPrimitive(ops, primitive.first_projection_idx,
	                                                   primitive.final_projection_idx, primitive.sink_idx);
}

} // namespace duckdb

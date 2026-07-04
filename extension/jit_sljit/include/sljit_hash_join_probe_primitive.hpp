//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

struct SljitHashJoinProbeInputRemap {
	vector<idx_t> key_input_indices;
	vector<idx_t> residual_probe_source_indices;

	bool HasKeyInputRemap() const {
		return !key_input_indices.empty();
	}

	bool HasResidualProbeSourceRemap() const {
		return !residual_probe_source_indices.empty();
	}
};

struct SljitHashJoinProbeMaterializePrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
};

struct SljitHashJoinProbeSelectionPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
	SljitHashJoinProbeInputRemap input_remap;
	vector<idx_t> output_column_map;
	idx_t output_projection_idx = DConstants::INVALID_INDEX;

	bool HasOutputColumnMap() const {
		return !output_column_map.empty();
	}
};

struct SljitMarkProbeFilterBoundaryPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t downstream_projection_idx = DConstants::INVALID_INDEX;
	bool apply_filter_selection = false;
	bool preserve_selected_hash_join = false;
	bool allow_marker_omission = false;
};

static bool SljitCanBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::NONE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
}

static SljitHashJoinProbeMaterializePrimitive
SljitBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                           bool source_key0_int64_to_int32_unchecked = false) {
	if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
		throw InternalException("SLJIT hash join materialize primitive cannot bind requested operator");
	}
	SljitHashJoinProbeMaterializePrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	return primitive;
}

static bool SljitCanBindHashJoinProbeSelectionPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       (ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	        ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY);
}

static SljitHashJoinProbeSelectionPrimitive SljitBindHashJoinProbeSelectionPrimitive(
    const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeInputRemap input_remap = {}, vector<idx_t> output_column_map = {},
    idx_t output_projection_idx = DConstants::INVALID_INDEX) {
	if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		throw InternalException("SLJIT hash join selection primitive cannot bind requested operator");
	}
	if (output_projection_idx != DConstants::INVALID_INDEX &&
	    (output_projection_idx >= ops.size() ||
	     ops[output_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		throw InternalException("SLJIT hash join selection primitive cannot bind output producer projection");
	}
	SljitHashJoinProbeSelectionPrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	primitive.input_remap = std::move(input_remap);
	primitive.output_column_map = std::move(output_column_map);
	primitive.output_projection_idx = output_projection_idx;
	return primitive;
}

static bool SljitCanBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                         idx_t hash_join_idx, idx_t filter_idx) {
	return hash_join_idx < ops.size() && filter_idx < ops.size() &&
	       ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
}

static bool SljitCanBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                         const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, primitive.hash_join_idx, primitive.filter_idx)) {
		return false;
	}
	if (primitive.downstream_projection_idx != DConstants::INVALID_INDEX &&
	    (primitive.downstream_projection_idx >= ops.size() ||
	     ops[primitive.downstream_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	if (primitive.allow_marker_omission &&
	    (!primitive.apply_filter_selection || primitive.downstream_projection_idx == DConstants::INVALID_INDEX)) {
		return false;
	}
	return true;
}

static SljitMarkProbeFilterBoundaryPrimitive
SljitBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                          idx_t filter_idx, bool apply_filter_selection = false,
                                          idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
                                          bool preserve_selected_hash_join = false,
                                          bool allow_marker_omission = false) {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx)) {
		throw InternalException("SLJIT MARK probe filter boundary primitive cannot bind requested operators");
	}
	if (downstream_projection_idx != DConstants::INVALID_INDEX &&
	    (downstream_projection_idx >= ops.size() ||
	     ops[downstream_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		throw InternalException("SLJIT MARK probe filter boundary primitive cannot bind downstream projection");
	}
	if (allow_marker_omission && (!apply_filter_selection || downstream_projection_idx == DConstants::INVALID_INDEX)) {
		throw InternalException("SLJIT MARK probe filter boundary marker omission requires a filtered projection");
	}
	SljitMarkProbeFilterBoundaryPrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.filter_idx = filter_idx;
	primitive.apply_filter_selection = apply_filter_selection;
	primitive.preserve_selected_hash_join = preserve_selected_hash_join;
	primitive.allow_marker_omission = allow_marker_omission;
	primitive.downstream_projection_idx = downstream_projection_idx;
	return primitive;
}

} // namespace duckdb

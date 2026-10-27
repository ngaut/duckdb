//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_batch_layout.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_mark_probe_filter_mode.hpp"

namespace duckdb {

struct SljitHashJoinProbeMaterializePrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
};

struct SljitHashJoinProbeSelectionPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
	shared_ptr<const SljitNativeHashJoinProbePlan> input_plan;
	shared_ptr<const SljitBatchLayout> output_layout;
};

struct SljitHashJoinBuildSinkPrimitive {
	idx_t sink_idx = DConstants::INVALID_INDEX;
	idx_t projection_idx = DConstants::INVALID_INDEX;
	bool direct_source_ingress = false;

	bool HasProjection() const {
		return projection_idx != DConstants::INVALID_INDEX;
	}
};

struct SljitMarkProbeFilterBoundaryPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	bool apply_filter_selection = false;
	SljitMarkProbeFilterMode filter_mode = SljitMarkProbeFilterMode::NONE;
	SljitMarkProbeFilterBoundaryMarkerMode marker_mode = SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS;
	SljitMarkProbeFilterBoundaryStrategy strategy = SljitMarkProbeFilterBoundaryStrategy::MATERIALIZE;
	shared_ptr<const SljitBatchLayout> output_layout;
};

static bool SljitCanBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::NONE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
}

static bool SljitTryBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t hash_join_idx,
                                                          SljitHashJoinProbeMaterializePrimitive &primitive,
                                                          bool source_key0_int64_to_int32_unchecked = false) {
	if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
		return false;
	}
	SljitHashJoinProbeMaterializePrimitive candidate;
	candidate.hash_join_idx = hash_join_idx;
	candidate.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	primitive = candidate;
	return true;
}

static bool SljitCanBindHashJoinProbeSelectionPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       (ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	        ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY);
}

static bool SljitTryBindHashJoinProbeSelectionPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                        SljitHashJoinProbeSelectionPrimitive &primitive,
                                                        bool source_key0_int64_to_int32_unchecked = false) {
	if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return false;
	}
	SljitHashJoinProbeSelectionPrimitive candidate;
	candidate.hash_join_idx = hash_join_idx;
	candidate.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	primitive = std::move(candidate);
	return true;
}

static bool SljitCanBindHashJoinBuildSinkPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t sink_idx,
                                                   idx_t projection_idx = DConstants::INVALID_INDEX) {
	if (sink_idx >= ops.size() || ops[sink_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD ||
	    ops[sink_idx].hash_join_build.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		return false;
	}
	return projection_idx == DConstants::INVALID_INDEX ||
	       (projection_idx < ops.size() && ops[projection_idx].kind == SljitNativeRegionOpKind::PROJECTION);
}

static bool SljitTryBindHashJoinBuildSinkPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t sink_idx,
                                                   idx_t projection_idx, SljitHashJoinBuildSinkPrimitive &primitive) {
	if (!SljitCanBindHashJoinBuildSinkPrimitive(ops, sink_idx, projection_idx)) {
		return false;
	}
	SljitHashJoinBuildSinkPrimitive candidate;
	candidate.sink_idx = sink_idx;
	candidate.projection_idx = projection_idx;
	primitive = candidate;
	return true;
}

static bool SljitCanBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                         idx_t hash_join_idx, idx_t filter_idx) {
	return hash_join_idx < ops.size() && filter_idx < ops.size() &&
	       ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
}

bool SljitTryBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                  idx_t filter_idx, bool apply_filter_selection,
                                                  idx_t downstream_projection_idx, bool materialize_filter_selection,
                                                  SljitMarkProbeFilterBoundaryPrimitive &primitive);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_reduction_lane.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_lane_contract.hpp"

namespace duckdb {

struct SljitGroupedReductionLaneBinding {
	const SljitAggregatePayloadDescriptor *descriptor = nullptr;
	const ExecutionPrimitiveAggregateUpdateLane *runtime_lane = nullptr;
	idx_t state_offset = 0;
};

static inline bool SljitTryBindGroupedReductionLane(const ExecutionRegionAggregateContract &contract,
                                                    const SljitAggregatePayloadDescriptor &descriptor,
                                                    const ExecutionPrimitiveAggregateUpdateLane *runtime_lane,
                                                    SljitGroupedReductionLaneBinding &result) {
	result = SljitGroupedReductionLaneBinding();
	if (!runtime_lane || descriptor.aggregate_index >= contract.grouped_state_offsets.size() ||
	    !SljitAggregatePayloadDescriptorMatchesLane(descriptor, *runtime_lane)) {
		return false;
	}
	const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
	if (runtime_lane->state_offset != state_offset) {
		return false;
	}
	result.descriptor = &descriptor;
	result.runtime_lane = runtime_lane;
	result.state_offset = state_offset;
	return true;
}

static inline bool
SljitTryBindGroupedReductionLanes(const ExecutionRegionAggregateContract &contract,
                                  const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                  const vector<const ExecutionPrimitiveAggregateUpdateLane *> &runtime_lanes,
                                  vector<SljitGroupedReductionLaneBinding> &result) {
	result.clear();
	if (payload_descriptors.size() != runtime_lanes.size()) {
		return false;
	}
	result.resize(payload_descriptors.size());
	for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
		if (!SljitTryBindGroupedReductionLane(contract, payload_descriptors[payload_idx], runtime_lanes[payload_idx],
		                                      result[payload_idx])) {
			result.clear();
			return false;
		}
	}
	return true;
}

static inline bool SljitGroupedReductionLanesReady(const vector<SljitGroupedReductionLaneBinding> &lanes) {
	for (auto &lane : lanes) {
		if (!lane.descriptor || !lane.runtime_lane) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

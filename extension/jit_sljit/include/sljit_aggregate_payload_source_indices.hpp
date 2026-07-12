//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_source_indices.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_fused_payload_sources.hpp"

namespace duckdb {

template <class HANDLE_COUNT_STAR, class HANDLE_FUSED_SOURCE, class CHECK_DIRECT_PAYLOAD, class HANDLE_DIRECT_PAYLOAD,
          class SET_BLOCKER>
static bool SljitTryBuildAggregatePayloadSourceIndices(
    const SljitExecutableAggregateUpdate &aggregate_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    bool &uses_fused_update, const char *payload_reference_blocker, HANDLE_COUNT_STAR &&handle_count_star,
    HANDLE_FUSED_SOURCE &&handle_fused_source, CHECK_DIRECT_PAYLOAD &&check_direct_payload,
    HANDLE_DIRECT_PAYLOAD &&handle_direct_payload, SET_BLOCKER &&set_blocker) {
	uses_fused_update = false;
	vector<idx_t> fused_payload_sources;
	if (SljitFusedAggregatePayloadsUseTypedExpressionTrees(aggregate_update.payloads,
	                                                       aggregate_update.payload_descriptors) &&
	    SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregate_update.payload_descriptors,
	                                                fused_payload_sources)) {
		for (auto source_idx : fused_payload_sources) {
			if (!handle_fused_source(source_idx)) {
				return false;
			}
		}
		uses_fused_update = true;
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = aggregate_update.payloads[payload_idx].plan;
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return set_blocker("count_star_payload");
			}
			if (!handle_count_star()) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.child_types.size() != 1) {
			return set_blocker("payload_contract");
		}
		if (!check_direct_payload(aggregate, payload_idx)) {
			return false;
		}
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    payload.source_index != aggregate.payload_index) {
			return set_blocker(payload_reference_blocker);
		}
		if (!handle_direct_payload(aggregate, payload_idx)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

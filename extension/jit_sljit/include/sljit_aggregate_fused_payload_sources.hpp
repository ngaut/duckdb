//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_fused_payload_sources.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

static bool
SljitFusedAggregatePayloadsUseTypedExpressionTrees(const vector<SljitExecutableRegionExpression> &payloads,
                                                   const vector<SljitAggregatePayloadDescriptor> &descriptors) {
	if (payloads.size() != descriptors.size()) {
		return false;
	}
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!descriptors[payload_idx].has_payload) {
			continue;
		}
		if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			has_typed_payload = true;
			continue;
		}
		if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			continue;
		}
		return false;
	}
	return has_typed_payload;
}

static bool
SljitFusedGroupedAggregatePayloadsUseReferenceAdapter(const vector<SljitExecutableRegionExpression> &payloads,
                                                      const vector<SljitAggregatePayloadDescriptor> &descriptors) {
	if (payloads.size() != descriptors.size()) {
		return false;
	}
	bool has_payload_reference = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!descriptors[payload_idx].has_payload) {
			continue;
		}
		if (payloads[payload_idx].plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return false;
		}
		has_payload_reference = true;
	}
	return has_payload_reference;
}

static bool
SljitFusedGroupedAggregatePayloadsUseRuntimeInputAdapter(const vector<SljitExecutableRegionExpression> &payloads,
                                                         const vector<SljitAggregatePayloadDescriptor> &descriptors) {
	return SljitFusedAggregatePayloadsUseTypedExpressionTrees(payloads, descriptors) ||
	       SljitFusedGroupedAggregatePayloadsUseReferenceAdapter(payloads, descriptors);
}

} // namespace duckdb

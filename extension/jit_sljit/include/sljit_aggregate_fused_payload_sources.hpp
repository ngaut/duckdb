//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_fused_payload_sources.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/exception.hpp"

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

enum class SljitFusedTypedPayloadSourceStatus : uint8_t {
	READY,
	UNSUPPORTED_PAYLOADS,
	MISSING_SOURCES,
	SOURCE_MISMATCH,
	NO_PAYLOADS
};

struct SljitFusedTypedPayloadSourceResult {
	optional_ptr<const vector<idx_t>> sources;
	SljitFusedTypedPayloadSourceStatus status = SljitFusedTypedPayloadSourceStatus::UNSUPPORTED_PAYLOADS;
};

static SljitFusedTypedPayloadSourceResult
SljitGetFusedTypedPayloadCombinedSourceIndices(const vector<SljitExecutableRegionExpression> &payloads,
                                               const vector<SljitAggregatePayloadDescriptor> &descriptors) {
	SljitFusedTypedPayloadSourceResult result;
	bool supported_payloads = payloads.size() == descriptors.size();
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!descriptors[payload_idx].has_payload) {
			auto &source_indices = payloads[payload_idx].input_source_indices;
			if (source_indices.empty()) {
				continue;
			}
			if (!result.sources) {
				result.sources = source_indices;
			} else if (*result.sources != source_indices) {
				result.status = SljitFusedTypedPayloadSourceStatus::SOURCE_MISMATCH;
				result.sources = nullptr;
				return result;
			}
			continue;
		}
		auto &plan = payloads[payload_idx].plan;
		if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE &&
		    plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			supported_payloads = false;
			break;
		}
		auto &source_indices = payloads[payload_idx].input_source_indices;
		if (source_indices.empty()) {
			result.status = SljitFusedTypedPayloadSourceStatus::MISSING_SOURCES;
			return result;
		}
		if (!result.sources) {
			result.sources = source_indices;
		} else if (*result.sources != source_indices) {
			result.status = SljitFusedTypedPayloadSourceStatus::SOURCE_MISMATCH;
			result.sources = nullptr;
			return result;
		}
	}
	if (!supported_payloads) {
		return result;
	}
	if (!result.sources) {
		result.status = SljitFusedTypedPayloadSourceStatus::NO_PAYLOADS;
		return result;
	}
	result.status = SljitFusedTypedPayloadSourceStatus::READY;
	return result;
}

static const vector<idx_t> &SljitRequireFusedTypedPayloadCombinedSourceIndices(
    const vector<SljitExecutableRegionExpression> &payloads, const vector<SljitAggregatePayloadDescriptor> &descriptors,
    const char *missing_sources_message, const char *source_mismatch_message, const char *no_payloads_message) {
	auto result = SljitGetFusedTypedPayloadCombinedSourceIndices(payloads, descriptors);
	switch (result.status) {
	case SljitFusedTypedPayloadSourceStatus::READY:
		return *result.sources;
	case SljitFusedTypedPayloadSourceStatus::MISSING_SOURCES:
		throw InternalException(missing_sources_message);
	case SljitFusedTypedPayloadSourceStatus::SOURCE_MISMATCH:
		throw InternalException(source_mismatch_message);
	case SljitFusedTypedPayloadSourceStatus::NO_PAYLOADS:
	case SljitFusedTypedPayloadSourceStatus::UNSUPPORTED_PAYLOADS:
		throw InternalException(no_payloads_message);
	default:
		throw InternalException("Unknown SLJIT fused typed aggregate payload source status");
	}
}

static optional_ptr<const vector<bool>>
SljitGetFusedTypedPayloadCombinedSourceNotNull(const vector<SljitExecutableRegionExpression> &payloads,
                                               const vector<SljitAggregatePayloadDescriptor> &descriptors,
                                               idx_t source_count) {
	optional_ptr<const vector<bool>> result;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!descriptors[payload_idx].has_payload) {
			continue;
		}
		auto &source_not_null = payloads[payload_idx].input_source_not_null;
		if (source_not_null.size() != source_count) {
			return nullptr;
		}
		if (!result) {
			result = source_not_null;
		} else if (*result != source_not_null) {
			throw InternalException("SLJIT fused typed aggregate payload source facts are not normalized");
		}
	}
	return result;
}

static bool SljitTryGetFusedTypedPayloadCombinedSources(const vector<SljitExecutableRegionExpression> &payloads,
                                                        const vector<SljitAggregatePayloadDescriptor> &descriptors,
                                                        vector<idx_t> &combined_sources) {
	auto result = SljitGetFusedTypedPayloadCombinedSourceIndices(payloads, descriptors);
	if (result.status != SljitFusedTypedPayloadSourceStatus::READY) {
		combined_sources.clear();
		return false;
	}
	combined_sources = *result.sources;
	return true;
}

} // namespace duckdb

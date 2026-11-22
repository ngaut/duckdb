//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_partial_fusion.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_aggregate_partial_fusion.hpp"

#include "sljit_region_aggregate_payload_fusion.hpp"
#include "sljit_region_aggregate_projection_rewrite.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

bool TryPartiallyFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                    SljitNativeRegionOpPlan &projection,
                                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                                    bool render_diagnostics) {
	if (projection.Kind() != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.AggregateUpdate().sink_info;
	if (!aggregate_update.AggregateUpdate().CanInitializePrimitivePayloads()) {
		return false;
	}
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink) || sink.aggregates.empty() || sink.groups.empty()) {
		return false;
	}

	idx_t group_projection_count = 0;
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.Projections().size()) {
			return false;
		}
		group_projection_count = MaxValue<idx_t>(group_projection_count, group.input_index + 1);
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, true, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.Projections().size()) {
			return false;
		}
		auto payload = projection.Projections()[aggregate.payload_index].Copy();
		if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true, true)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	vector<SljitNativeRegionExpressionPlan> rewritten_projections;
	vector<LogicalType> rewritten_types;
	rewritten_projections.reserve(group_projection_count + input_types.size());
	rewritten_types.reserve(group_projection_count + input_types.size());
	for (idx_t projection_idx = 0; projection_idx < group_projection_count; projection_idx++) {
		rewritten_projections.push_back(projection.Projections()[projection_idx].Copy());
		rewritten_types.push_back(projection.output_types[projection_idx]);
	}
	if (!RewriteSljitDirectPayloadSourcesThroughPartialProjection(input_types, payloads, rewritten_projections,
	                                                              rewritten_types, render_diagnostics)) {
		return false;
	}
	if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics)) {
		return false;
	}
	const bool use_perfect_hash_group_lookup = SljitPerfectHashGroupLookupSupported(sink, payloads);
	if (!use_perfect_hash_group_lookup && !SljitGroupedStateAddressPayloadsSupported(sink, payloads)) {
		return false;
	}
	const bool fused_payload_projection = SljitPrimitiveAggregatePayloadsContainNonReference(payloads, sink);
	const bool shrank_projection = rewritten_projections.size() < projection.Projections().size();
	if (!fused_payload_projection && !shrank_projection) {
		return false;
	}

	projection.Projections() = std::move(rewritten_projections);
	projection.output_types = std::move(rewritten_types);
	aggregate_update.AggregateUpdate().input_types = projection.output_types;
	aggregate_update.AggregateUpdate().payloads = std::move(payloads);
	aggregate_update.AggregateUpdate().InitializeProjectionComposedPrimitivePayloads();
	aggregate_update.AggregateUpdate().use_grouped_state_addresses = true;
	aggregate_update.AggregateUpdate().use_perfect_hash_group_lookup = use_perfect_hash_group_lookup;
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
		                                     "primitive_payload_projection_partially_composed=true");
		if (aggregate_update.AggregateUpdate().use_perfect_hash_group_lookup) {
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
			                                     "grouped_state_lookup=generated-perfect-hash");
		} else {
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
			                                     "grouped_state_lookup=native-state-address");
		}
	}
	return true;
}

bool TryPartiallyFuseNativeProjectionIntoRegularHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                    SljitNativeRegionOpPlan &projection,
                                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                                    bool render_diagnostics) {
	if (projection.Kind() != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.AggregateUpdate().sink_info;
	if (!aggregate_update.AggregateUpdate().CanInitializePrimitivePayloads()) {
		return false;
	}
	if (sink.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink.aggregates.empty() ||
	    sink.groups.empty() ||
	    sink.contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY ||
	    !sink.contract.grouped_state_layout_ready) {
		return false;
	}

	idx_t group_projection_count = 0;
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.Projections().size()) {
			return false;
		}
		group_projection_count = MaxValue<idx_t>(group_projection_count, group.input_index + 1);
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, true, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.Projections().size()) {
			return false;
		}
		auto payload = projection.Projections()[aggregate.payload_index].Copy();
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true)) {
				return false;
			}
		} else {
			SljitAggregatePayloadDescriptor descriptor;
			if (!SljitTryBindAggregatePayloadDescriptor(payload, aggregate, descriptor)) {
				return false;
			}
		}
		payloads.push_back(std::move(payload));
	}

	vector<SljitNativeRegionExpressionPlan> rewritten_projections;
	vector<LogicalType> rewritten_types;
	rewritten_projections.reserve(group_projection_count + input_types.size());
	rewritten_types.reserve(group_projection_count + input_types.size());
	for (idx_t projection_idx = 0; projection_idx < group_projection_count; projection_idx++) {
		rewritten_projections.push_back(projection.Projections()[projection_idx].Copy());
		rewritten_types.push_back(projection.output_types[projection_idx]);
	}
	if (!RewriteSljitDirectPayloadSourcesThroughPartialProjection(input_types, payloads, rewritten_projections,
	                                                              rewritten_types, render_diagnostics)) {
		return false;
	}
	if (!TryNormalizeGroupedTypedAggregatePayloads(payloads, sink, render_diagnostics)) {
		return false;
	}

	// Admit exactly what the generated grouped typed update compiles: it has no COUNT lane, so a typed SUM next to
	// a COUNT payload stays with DuckDB's update
	if (!SljitGroupedStateAddressTypedPayloadsSupported(sink, payloads)) {
		return false;
	}

	projection.Projections() = std::move(rewritten_projections);
	projection.output_types = std::move(rewritten_types);
	aggregate_update.AggregateUpdate().input_types = projection.output_types;
	aggregate_update.AggregateUpdate().payloads = std::move(payloads);
	aggregate_update.AggregateUpdate().InitializeProjectionComposedPrimitivePayloads();
	aggregate_update.AggregateUpdate().use_grouped_state_addresses = true;
	aggregate_update.AggregateUpdate().use_perfect_hash_group_lookup = false;
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
		                                     "primitive_payload_projection_partially_composed=true");
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.AggregateUpdate(),
		                                     "grouped_state_lookup=native-state-address");
	}
	return true;
}

} // namespace duckdb

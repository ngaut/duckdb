//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_payload_fusion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_perfect_hash_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_native_plan.hpp"
#include "sljit_projection_composition.hpp"
#include "sljit_region_plan_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

//! perfect_hash_state: the grouped states are a generated-lookup perfect hash's, whose kernels take DOUBLE trees; a
//! hash aggregate's DOUBLE sums keep the reference paths that preaggregate group runs
static bool SljitPrimitiveAggregatePayloadSupported(SljitNativeRegionExpressionPlan &payload,
                                                    const ExecutionRegionAggregateInput &aggregate,
                                                    bool grouped_state = false, bool perfect_hash_state = false) {
	SljitAggregatePayloadDescriptor descriptor;
	if (!SljitTryBindAggregatePayloadDescriptor(payload, aggregate, descriptor)) {
		return false;
	}
	if (!descriptor.has_payload) {
		return true;
	}
	if (descriptor.value_abi == SljitAggregatePayloadValueABI::VALIDITY_ONLY) {
		// the ungrouped and perfect-hash kernels count the rows whose typed tree is not NULL
		return payload.kind == SljitNativeRegionExpressionKind::REFERENCE ||
		       ((!grouped_state || perfect_hash_state) &&
		        payload.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE && payload.expression_tree &&
		        BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false).supported);
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
		switch (payload.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			payload.double_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
			return true;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			return !grouped_state;
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			// the kernels add a DOUBLE tree's rows to each state in source order
			return (!grouped_state || perfect_hash_state) && payload.expression_tree &&
			       SljitAggregateTypedPayloadPlanSupported(
			           BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false), descriptor);
		default:
			return false;
		}
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && descriptor.IsDoubleWord()) {
		return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
	}
	SljitNativeIntegerKind aggregate_payload_kind;
	if (!descriptor.TryGetTypedIntegerKind(aggregate_payload_kind)) {
		return false;
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload.integer_kind = aggregate_payload_kind;
			return true;
		}
		if (!payload.expression_tree) {
			return false;
		}
		SljitNativeIntegerKind typed_tree_kind;
		if (!SljitTypedExpressionTreeIsSupported(*payload.expression_tree) ||
		    !TryGetSljitTypedExpressionTreeResultKind(*payload.expression_tree, typed_tree_kind) ||
		    typed_tree_kind != aggregate_payload_kind) {
			return false;
		}
		payload.kind = SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
		payload.integer_kind = typed_tree_kind;
		return true;
	}
	if (descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
		return false;
	}
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		payload.integer_kind = aggregate_payload_kind;
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return payload.integer_kind == aggregate_payload_kind;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!payload.expression_tree || !SljitTypedExpressionTreeIsSupported(*payload.expression_tree)) {
			return false;
		}
		SljitNativeIntegerKind typed_tree_kind;
		return TryGetSljitTypedExpressionTreeResultKind(*payload.expression_tree, typed_tree_kind) &&
		       typed_tree_kind == aggregate_payload_kind;
	}
	default:
		return false;
	}
}

static bool SljitAggregateUpdateUsesGeneratedPerfectHashLookup(const ExecutionRegionAggregateSinkInfo &sink) {
	return sink.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	       sink.contract.native_grouped_state_contract.status == ExecutionRegionStateContractStatus::READY;
}

static bool
SljitGroupedStateAddressPrimitivePayloadsSupported(const ExecutionRegionAggregateSinkInfo &sink,
                                                   const vector<SljitNativeRegionExpressionPlan> &payloads) {
	if (payloads.empty() || payloads.size() != sink.aggregates.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], sink.aggregates[payload_idx],
		                                                         sink.contract)) {
			return false;
		}
	}
	return true;
}

static bool SljitGroupedStateAddressTypedPayloadsSupported(const ExecutionRegionAggregateSinkInfo &sink,
                                                           const vector<SljitNativeRegionExpressionPlan> &payloads) {
	auto &contract = sink.contract;
	if (!contract.grouped_state_layout_ready || payloads.empty() || payloads.size() != sink.aggregates.size()) {
		return false;
	}
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		auto &payload = payloads[payload_idx];
		SljitAggregatePayloadDescriptor descriptor;
		if (!SljitFusedGroupedAggregatePayloadSupported(payload, aggregate, contract, &descriptor)) {
			return false;
		}
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
		    payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			continue;
		}
		if (payload.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !payload.expression_tree) {
			return false;
		}
		auto payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
		if (!SljitAggregateTypedPayloadPlanSupported(payload_plan, descriptor)) {
			return false;
		}
		has_typed_payload = true;
	}
	return has_typed_payload;
}

static bool SljitGroupedStateAddressPayloadsSupported(const ExecutionRegionAggregateSinkInfo &sink,
                                                      const vector<SljitNativeRegionExpressionPlan> &payloads) {
	auto &contract = sink.contract;
	if ((sink.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	     sink.Kind() != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) ||
	    contract.native_grouped_state_contract.status != ExecutionRegionStateContractStatus::READY ||
	    !contract.grouped_state_layout_ready) {
		return false;
	}
	return SljitGroupedStateAddressPrimitivePayloadsSupported(sink, payloads) ||
	       SljitGroupedStateAddressTypedPayloadsSupported(sink, payloads);
}

static bool SljitPrimitiveAggregatePayloadsContainNonReference(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                               const ExecutionRegionAggregateSinkInfo &sink) {
	if (payloads.size() != sink.aggregates.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (sink.aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (payloads[payload_idx].kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return true;
		}
	}
	return false;
}

static void AppendSljitAggregateUpdateDiagnostic(SljitNativeAggregateUpdatePlan &aggregate_update,
                                                 const string &diagnostic) {
	if (!aggregate_update.ir.empty()) {
		aggregate_update.ir += ";";
	}
	aggregate_update.ir += diagnostic;
}

static bool TryGetSljitExpressionTreePassthroughReference(const ExecutionExpressionIR &node, idx_t &ref_index) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		ref_index = node.ref_index;
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CAST && !node.try_cast && node.left &&
	    node.return_type == node.left->return_type) {
		return TryGetSljitExpressionTreePassthroughReference(*node.left, ref_index);
	}
	return false;
}

static bool TrySimplifySljitExpressionTreeReferencePayload(SljitNativeRegionExpressionPlan &payload) {
	if (!payload.expression_tree) {
		return false;
	}
	idx_t ref_index;
	if (!TryGetSljitExpressionTreePassthroughReference(*payload.expression_tree, ref_index)) {
		return false;
	}
	auto source_index = ref_index < payload.expression_tree_source_indices.size()
	                        ? payload.expression_tree_source_indices[ref_index]
	                        : ref_index;
	payload.kind = SljitNativeRegionExpressionKind::REFERENCE;
	payload.source_index = source_index;
	payload.return_type = payload.expression_tree->return_type;
	payload.expression_tree.reset();
	payload.expression_tree_source_indices.clear();
	return true;
}

static bool TryNormalizeGroupedAggregatePayloads(vector<SljitNativeRegionExpressionPlan> &payloads,
                                                 const ExecutionRegionAggregateSinkInfo &sink, bool render_diagnostics,
                                                 const char *reference_reason, const char *typed_reason,
                                                 bool perfect_hash_state) {
	if (!SljitPrimitiveAggregatePayloadsContainNonReference(payloads, sink)) {
		return true;
	}

	vector<SljitNativeRegionExpressionPlan> normalized_payloads;
	normalized_payloads.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}
		if (TrySimplifySljitExpressionTreeReferencePayload(payloads[payload_idx])) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}
		if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			normalized_payloads.push_back(payloads[payload_idx].Copy());
			continue;
		}

		auto tree = CopySljitExpressionPlanAsInputTree(payloads[payload_idx]);
		if (!tree) {
			return false;
		}
		SljitNativeRegionExpressionPlan typed_payload;
		if (!TryBuildSljitNativeTypedExpressionTreePlan(*tree, typed_payload)) {
			return false;
		}
		const bool simplified_reference_payload = TrySimplifySljitExpressionTreeReferencePayload(typed_payload);
		if (!SljitPrimitiveAggregatePayloadSupported(typed_payload, aggregate, true, perfect_hash_state)) {
			return false;
		}
		if (render_diagnostics) {
			typed_payload.ir = string(simplified_reference_payload ? reference_reason : typed_reason) + "(" +
			                   payloads[payload_idx].ir + ")";
		}
		normalized_payloads.push_back(std::move(typed_payload));
	}
	payloads = std::move(normalized_payloads);
	return true;
}

static bool TryNormalizePerfectHashAggregatePayloads(vector<SljitNativeRegionExpressionPlan> &payloads,
                                                     const ExecutionRegionAggregateSinkInfo &sink,
                                                     bool render_diagnostics) {
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink)) {
		return true;
	}
	return TryNormalizeGroupedAggregatePayloads(payloads, sink, render_diagnostics, "perfect-hash-reference-payload",
	                                            "perfect-hash-typed-payload", true);
}

static bool TryNormalizeGroupedTypedAggregatePayloads(vector<SljitNativeRegionExpressionPlan> &payloads,
                                                      const ExecutionRegionAggregateSinkInfo &sink,
                                                      bool render_diagnostics) {
	return TryNormalizeGroupedAggregatePayloads(payloads, sink, render_diagnostics, "grouped-reference-payload",
	                                            "grouped-typed-payload", false);
}

//! A filtered aggregate's payload over the sink input: its CASE WHEN filter THEN child END
static bool TryLowerSljitFilteredAggregatePayload(const ExecutionRegionAggregateInput &aggregate,
                                                  SljitNativeRegionExpressionPlan &payload, bool render_diagnostics) {
	D_ASSERT(aggregate.has_filter);
	string error;
	return aggregate.child_expressions.size() == 1 &&
	       TryLowerNativeRegionExpression(aggregate.child_expressions[0], false, payload, error, render_diagnostics);
}

//! The payload of an aggregate reading one value per row, computed from the input of the projection that feeds the
//! aggregate: the projection's output it reads, or a filtered aggregate's payload composed through the projection
static bool TryBuildSljitAggregatePayloadThroughProjection(const vector<SljitNativeRegionExpressionPlan> &projections,
                                                           const ExecutionRegionAggregateInput &aggregate,
                                                           SljitNativeRegionExpressionPlan &payload,
                                                           bool render_diagnostics) {
	if (aggregate.has_filter) {
		SljitNativeRegionExpressionPlan input_payload;
		return TryLowerSljitFilteredAggregatePayload(aggregate, input_payload, render_diagnostics) &&
		       TryComposeNativeSemanticProjection(projections, input_payload, payload, render_diagnostics);
	}
	if (aggregate.child_count != 1 || aggregate.payload_index >= projections.size()) {
		return false;
	}
	payload = projections[aggregate.payload_index].Copy();
	return true;
}

static bool TryBuildSljitPrimitiveReferencePayload(const vector<LogicalType> &input_types,
                                                   const ExecutionRegionAggregateInput &aggregate,
                                                   SljitNativeRegionExpressionPlan &payload, bool grouped_state,
                                                   bool render_diagnostics) {
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		if (aggregate.child_count != 0) {
			return false;
		}
		payload.kind = SljitNativeRegionExpressionKind::CONSTANT;
		payload.return_type = LogicalType::BIGINT;
		if (render_diagnostics) {
			payload.ir = "primitive-count-star";
		}
		return SljitPrimitiveAggregatePayloadSupported(payload, aggregate, grouped_state);
	}
	if (aggregate.has_filter) {
		return TryLowerSljitFilteredAggregatePayload(aggregate, payload, render_diagnostics) &&
		       SljitPrimitiveAggregatePayloadSupported(payload, aggregate, grouped_state);
	}
	if (aggregate.child_count != 1 || aggregate.payload_index >= input_types.size()) {
		return false;
	}
	payload.kind = SljitNativeRegionExpressionKind::REFERENCE;
	payload.source_index = aggregate.payload_index;
	payload.return_type = input_types[aggregate.payload_index];
	if (render_diagnostics) {
		payload.ir = "primitive-reference";
	}
	return SljitPrimitiveAggregatePayloadSupported(payload, aggregate, grouped_state);
}

static bool SljitPerfectHashGroupLookupSupported(
    const ExecutionRegionAggregateSinkInfo &sink, const vector<SljitNativeRegionExpressionPlan> &payloads,
    optional_ptr<const vector<SljitNativeRegionExpressionPlan>> group_expressions = nullptr) {
	auto &contract = sink.contract;
	if (sink.Kind() != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	    contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH || !contract.grouped_state_layout_ready ||
	    payloads.empty() || payloads.size() != sink.aggregates.size() ||
	    contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	static const vector<SljitNativeRegionExpressionPlan> empty_group_expressions;
	auto &resolved_group_expressions = group_expressions ? *group_expressions : empty_group_expressions;
	vector<SljitPerfectHashGroupPlan> primitive_group_plans;
	const bool primitive_group_lookup_supported =
	    TryBuildSljitPerfectHashGroupPlans(sink.groups, resolved_group_expressions, contract, primitive_group_plans) &&
	    !primitive_group_plans.empty();
	vector<SljitPerfectHashGroupPlan> typed_group_plans;
	if (!TryBuildSljitPerfectHashGroupPlans(sink.groups, resolved_group_expressions, contract, typed_group_plans,
	                                        true) ||
	    typed_group_plans.empty()) {
		return false;
	}

	bool primitive_payloads_supported = true;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], sink.aggregates[payload_idx],
		                                                         contract)) {
			primitive_payloads_supported = false;
			break;
		}
	}
	if (primitive_payloads_supported && primitive_group_lookup_supported) {
		return true;
	}

	SljitFusedAggregateCodegenPlan codegen_plan;
	if (!BuildSljitFusedAggregateCodegenPlan(payloads, sink.aggregates, codegen_plan)) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitPerfectHashAggregatePayloadSupported(payloads[payload_idx], sink.aggregates[payload_idx], contract)) {
			return false;
		}
	}
	return true;
}
} // namespace duckdb

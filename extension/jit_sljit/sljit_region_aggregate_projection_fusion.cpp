//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_projection_fusion.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_aggregate_projection_fusion.hpp"

#include "sljit_region_aggregate_payload_fusion.hpp"
#include "sljit_region_aggregate_projection_rewrite.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

static bool SljitPrimitiveAggregatePayloadCanEraseProjection(const SljitNativeRegionExpressionPlan &payload) {
	if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		return true;
	}
	return payload.references_region_input;
}

static bool SljitPerfectHashSignedSourceMatchesInput(SljitNativeSignedIntegerWidth width, const LogicalType &type) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return type.InternalType() == PhysicalType::INT8;
	case SljitNativeSignedIntegerWidth::INT16:
		return type.InternalType() == PhysicalType::INT16;
	case SljitNativeSignedIntegerWidth::INT32:
		return type.InternalType() == PhysicalType::INT32;
	case SljitNativeSignedIntegerWidth::INT64:
		return type.InternalType() == PhysicalType::INT64;
	default:
		return false;
	}
}

static bool TryGetSljitPerfectHashGroupKind(const LogicalType &type, SljitNativeIntegerKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		kind = SljitNativeIntegerKind::INT8;
		return true;
	case PhysicalType::UINT8:
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool SljitExpressionTreeReferencesMatchInputTypes(const ExecutionExpressionIR &node,
                                                         const vector<idx_t> &source_indices,
                                                         const vector<LogicalType> &input_types) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (node.ref_index >= source_indices.size()) {
			return false;
		}
		auto source_index = source_indices[node.ref_index];
		if (source_index >= input_types.size()) {
			return false;
		}
		return node.return_type == input_types[source_index];
	}
	if (node.left && !SljitExpressionTreeReferencesMatchInputTypes(*node.left, source_indices, input_types)) {
		return false;
	}
	if (node.right && !SljitExpressionTreeReferencesMatchInputTypes(*node.right, source_indices, input_types)) {
		return false;
	}
	if (node.else_node && !SljitExpressionTreeReferencesMatchInputTypes(*node.else_node, source_indices, input_types)) {
		return false;
	}
	for (auto &child : node.children) {
		if (!child || !SljitExpressionTreeReferencesMatchInputTypes(*child, source_indices, input_types)) {
			return false;
		}
	}
	return true;
}

static bool SljitPerfectHashGroupExpressionCanEraseProjection(const vector<LogicalType> &input_types,
                                                              const SljitNativeRegionExpressionPlan &expr,
                                                              const ExecutionRegionGroupInput &group) {
	if (expr.return_type.InternalType() != group.type.InternalType()) {
		return false;
	}
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (expr.source_index >= input_types.size()) {
			return false;
		}
		return expr.references_region_input;
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		if (expr.source_index >= input_types.size()) {
			return false;
		}
		return SljitPerfectHashSignedSourceMatchesInput(expr.cast_source_width, input_types[expr.source_index]);
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		if (expr.source_index >= input_types.size() ||
		    !SljitPerfectHashSignedSourceMatchesInput(expr.cast_source_width, input_types[expr.source_index])) {
			return false;
		}
		switch (expr.cast_target_width) {
		case SljitNativeSignedIntegerWidth::INT8:
			return group.type.InternalType() == PhysicalType::INT8;
		case SljitNativeSignedIntegerWidth::INT16:
			return group.type.InternalType() == PhysicalType::INT16;
		case SljitNativeSignedIntegerWidth::INT32:
			return group.type.InternalType() == PhysicalType::INT32;
		case SljitNativeSignedIntegerWidth::INT64:
			return group.type.InternalType() == PhysicalType::INT64;
		default:
			return false;
		}
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		if (expr.source_index >= input_types.size()) {
			return false;
		}
		return input_types[expr.source_index].id() == LogicalTypeId::VARCHAR &&
		       group.type.InternalType() == PhysicalType::UINT8 && expr.string_compress_target_size == sizeof(uint8_t);
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!expr.expression_tree) {
			return false;
		}
		if (!SljitExpressionTreeReferencesMatchInputTypes(*expr.expression_tree, expr.expression_tree_source_indices,
		                                                  input_types)) {
			return false;
		}
		SljitNativeIntegerKind group_kind;
		auto tree_plan = BuildSljitTypedExpressionTreePlan(*expr.expression_tree, false);
		return tree_plan.supported && TryGetSljitPerfectHashGroupKind(group.type, group_kind) &&
		       tree_plan.result_kind == group_kind;
	}
	default:
		return false;
	}
}

bool TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(const vector<LogicalType> &input_types,
                                                         SljitNativeRegionOpPlan &projection,
                                                         SljitNativeRegionOpPlan &aggregate_update,
                                                         bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || sink.aggregates.empty()) {
		return false;
	}
	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(sink.aggregates.size());
	for (auto &aggregate : sink.aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitNativeRegionExpressionPlan payload;
			if (!TryBuildSljitPrimitiveReferencePayload(input_types, aggregate, payload, false, render_diagnostics)) {
				return false;
			}
			payloads.push_back(std::move(payload));
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (!SljitPrimitiveAggregatePayloadCanEraseProjection(payload)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate)) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}

	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update, "payload_update=generated-primitive");
	}
	aggregate_update.output_types = projection.output_types;
	return true;
}

bool TryFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                           SljitNativeRegionOpPlan &projection,
                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                           bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	if (aggregate_update.aggregate_update.use_primitive_payloads) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (!SljitAggregateUpdateUsesGeneratedPerfectHashLookup(sink) || sink.aggregates.empty() || sink.groups.empty()) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> group_expressions;
	group_expressions.reserve(sink.groups.size());
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= projection.projections.size()) {
			return false;
		}
		auto group_expression = projection.projections[group.input_index].Copy();
		if (!SljitPerfectHashGroupExpressionCanEraseProjection(input_types, group_expression, group)) {
			return false;
		}
		if (render_diagnostics && !group_expression.ir.empty()) {
			group_expression.ir = "perfect-hash-group(" + group_expression.ir + ")";
		}
		group_expressions.push_back(std::move(group_expression));
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
		if (aggregate.child_count != 1 || aggregate.payload_index >= projection.projections.size()) {
			return false;
		}
		auto payload = projection.projections[aggregate.payload_index].Copy();
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (!SljitPrimitiveAggregatePayloadSupported(payload, aggregate, true)) {
				return false;
			}
		} else if (payload.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
		           !aggregate.primitive_update_ready) {
			return false;
		}
		payloads.push_back(std::move(payload));
	}
	if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics) ||
	    !SljitPerfectHashGroupLookupSupported(sink, payloads, group_expressions)) {
		return false;
	}

	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	aggregate_update.aggregate_update.group_expressions = std::move(group_expressions);
	aggregate_update.aggregate_update.use_primitive_payloads = true;
	aggregate_update.aggregate_update.use_grouped_state_addresses = true;
	aggregate_update.aggregate_update.use_perfect_hash_group_lookup = true;
	aggregate_update.output_types = input_types;
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
		                                     "primitive_payload_projection_composed=true");
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
		                                     "perfect_hash_group_projection_composed=true");
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
		                                     "grouped_state_lookup=generated-perfect-hash");
	}
	return true;
}

bool TryComposePrimitiveAggregatePayloadsThroughProjection(const vector<LogicalType> &input_types,
                                                           const SljitNativeRegionOpPlan &projection,
                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                           bool render_diagnostics) {
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_update.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !aggregate_update.aggregate_update.use_primitive_payloads) {
		return false;
	}
	auto &sink = aggregate_update.aggregate_update.sink_info;
	if (sink.aggregates.empty() || sink.aggregates.size() != aggregate_update.aggregate_update.payloads.size()) {
		return false;
	}

	if (sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	    aggregate_update.aggregate_update.use_perfect_hash_group_lookup) {
		auto mark_blocker = [&](const string &blocker) {
			if (!render_diagnostics) {
				return;
			}
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
			                                     "perfect_hash_payload_projection_compose_blocker=" + blocker);
		};
		if (sink.groups.empty() || aggregate_update.aggregate_update.group_expressions.size() != sink.groups.size()) {
			mark_blocker("group-expression-count");
			return false;
		}

		vector<SljitNativeRegionExpressionPlan> group_expressions;
		group_expressions.reserve(aggregate_update.aggregate_update.group_expressions.size());
		for (idx_t group_idx = 0; group_idx < aggregate_update.aggregate_update.group_expressions.size(); group_idx++) {
			auto &group = sink.groups[group_idx];
			auto &group_expression = aggregate_update.aggregate_update.group_expressions[group_idx];
			SljitNativeRegionExpressionPlan composed;
			if (!TryComposeNativeProjection(projection.projections, group_expression, composed, render_diagnostics)) {
				mark_blocker("group-expression-compose");
				return false;
			}
			if (!SljitPerfectHashGroupExpressionCanEraseProjection(input_types, composed, group)) {
				mark_blocker("group-expression-erase");
				return false;
			}
			group_expressions.push_back(std::move(composed));
		}

		vector<SljitNativeRegionExpressionPlan> payloads;
		payloads.reserve(aggregate_update.aggregate_update.payloads.size());
		for (idx_t payload_idx = 0; payload_idx < aggregate_update.aggregate_update.payloads.size(); payload_idx++) {
			auto &aggregate = sink.aggregates[payload_idx];
			auto &payload = aggregate_update.aggregate_update.payloads[payload_idx];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				payloads.push_back(payload.Copy());
				continue;
			}
			SljitNativeRegionExpressionPlan composed;
			if (!TryComposeNativeProjection(projection.projections, payload, composed, render_diagnostics)) {
				mark_blocker("payload-compose");
				return false;
			}
			if (!SljitPrimitiveAggregatePayloadSupported(composed, aggregate, true)) {
				mark_blocker("payload-supported");
				return false;
			}
			payloads.push_back(std::move(composed));
		}
		if (!TryNormalizePerfectHashAggregatePayloads(payloads, sink, render_diagnostics)) {
			mark_blocker("payload-normalize");
			return false;
		}
		if (!SljitPerfectHashGroupLookupSupported(sink, payloads, group_expressions)) {
			mark_blocker("lookup-supported");
			return false;
		}

		aggregate_update.output_types = input_types;
		aggregate_update.aggregate_update.input_types = input_types;
		aggregate_update.aggregate_update.payloads = std::move(payloads);
		aggregate_update.aggregate_update.group_expressions = std::move(group_expressions);
		if (render_diagnostics) {
			AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
			                                     "perfect_hash_payload_projection_composed=true");
		}
		return true;
	}

	if (sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return false;
	}

	vector<SljitNativeRegionExpressionPlan> payloads;
	payloads.reserve(aggregate_update.aggregate_update.payloads.size());
	for (idx_t payload_idx = 0; payload_idx < aggregate_update.aggregate_update.payloads.size(); payload_idx++) {
		auto &aggregate = sink.aggregates[payload_idx];
		auto &payload = aggregate_update.aggregate_update.payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			payloads.push_back(payload.Copy());
			continue;
		}
		SljitNativeRegionExpressionPlan composed;
		if (!TryComposeNativeProjection(projection.projections, payload, composed, render_diagnostics)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadCanEraseProjection(composed)) {
			return false;
		}
		if (!SljitPrimitiveAggregatePayloadSupported(composed, aggregate)) {
			return false;
		}
		payloads.push_back(std::move(composed));
	}

	aggregate_update.output_types = input_types;
	aggregate_update.aggregate_update.input_types = input_types;
	aggregate_update.aggregate_update.payloads = std::move(payloads);
	if (render_diagnostics) {
		AppendSljitAggregateUpdateDiagnostic(aggregate_update.aggregate_update,
		                                     "primitive_payload_projection_composed=true");
	}
	return true;
}

} // namespace duckdb

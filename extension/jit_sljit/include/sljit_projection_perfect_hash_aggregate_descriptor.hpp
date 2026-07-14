//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_perfect_hash_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_descriptor.hpp"

namespace duckdb {

static bool SljitEnsureJoinProjectionAggregateInputSlot(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                        const SljitExecutableRegionOp &projection_op, idx_t input_idx) {
	if (input_idx >= projection_op.output_types.size()) {
		return false;
	}
	while (descriptor.input_sources.size() <= input_idx) {
		auto slot_idx = descriptor.input_sources.size();
		SljitJoinProjectionAggregateInputSource source;
		source.kind = SljitJoinProjectionAggregateInputKind::UNUSED;
		source.projection_idx = DConstants::INVALID_INDEX;
		source.input_idx = DConstants::INVALID_INDEX;
		source.type = projection_op.output_types[slot_idx];
		descriptor.input_sources.push_back(std::move(source));
		descriptor.output_to_projection.push_back(DConstants::INVALID_INDEX);
		descriptor.input_types.push_back(projection_op.output_types[slot_idx]);
	}
	return true;
}

static bool SljitProjectionInputSourceMatches(const SljitJoinProjectionAggregateInputSource &lhs,
                                              const SljitJoinProjectionAggregateInputSource &rhs) {
	return lhs.kind == rhs.kind && lhs.projection_idx == rhs.projection_idx && lhs.input_idx == rhs.input_idx &&
	       lhs.type == rhs.type;
}

static bool SljitTryAssignJoinProjectionAggregateInputAt(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                         const SljitExecutableRegionOp &projection_op, idx_t input_idx,
                                                         SljitJoinProjectionAggregateInputSource incoming_source,
                                                         idx_t output_projection_idx) {
	if (!SljitEnsureJoinProjectionAggregateInputSlot(descriptor, projection_op, input_idx)) {
		return false;
	}
	auto &existing_source = descriptor.input_sources[input_idx];
	if (existing_source.kind != SljitJoinProjectionAggregateInputKind::UNUSED) {
		return SljitProjectionInputSourceMatches(existing_source, incoming_source) &&
		       descriptor.input_types[input_idx] == incoming_source.type &&
		       descriptor.output_to_projection[input_idx] == output_projection_idx;
	}
	auto input_type = incoming_source.type;
	existing_source = std::move(incoming_source);
	descriptor.output_to_projection[input_idx] = output_projection_idx;
	descriptor.input_types[input_idx] = std::move(input_type);
	return true;
}

static bool SljitTryResolveJoinLHSProjectionInputSource(const ExecutionHashJoinProbeBinding &binding,
                                                        const SljitExecutableRegionOp &projection_op,
                                                        idx_t projection_idx,
                                                        SljitJoinProjectionAggregateInputSource &source) {
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
	    join_output_source_index >= binding.lhs_output_column_indices.size() ||
	    join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	auto &output_type = projection_op.output_types[projection_idx];
	if (remapped_expr.plan.return_type != output_type ||
	    binding.output_types[join_output_source_index] != output_type) {
		return false;
	}
	source = SljitJoinProjectionAggregateInputSource();
	source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
	source.projection_idx = DConstants::INVALID_INDEX;
	source.input_idx = binding.lhs_output_column_indices[join_output_source_index];
	source.type = output_type;
	return true;
}

static bool SljitTrySetJoinProjectionAggregateInputAt(const ExecutionHashJoinProbeBinding &binding,
                                                      SljitJoinProjectionAggregateDescriptor &descriptor,
                                                      const SljitExecutableRegionOp &projection_op,
                                                      idx_t projection_idx, idx_t input_idx) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	SljitJoinProjectionAggregateInputSource source;
	idx_t output_projection_idx = DConstants::INVALID_INDEX;
	if (!SljitTryResolveJoinLHSProjectionInputSource(binding, projection_op, projection_idx, source)) {
		auto &projection = projection_op.projections[projection_idx];
		auto &output_type = projection_op.output_types[projection_idx];
		if (projection.plan.return_type != output_type) {
			return false;
		}
		source.kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
		source.projection_idx = projection_idx;
		source.input_idx = DConstants::INVALID_INDEX;
		source.type = output_type;
		output_projection_idx = projection_idx;
	}
	return SljitTryAssignJoinProjectionAggregateInputAt(descriptor, projection_op, input_idx, std::move(source),
	                                                    output_projection_idx);
}

static bool SljitTryCollectPerfectHashGroupExpressionSources(const SljitExecutableAggregateUpdate &aggregate_update,
                                                             const vector<ExecutionRegionGroupInput> &groups,
                                                             vector<uint8_t> &required_semantic_inputs) {
	auto mark_source = [&](idx_t source_idx) {
		return SljitAddProjectionSourceColumn(source_idx, required_semantic_inputs.size(), required_semantic_inputs);
	};
	auto mark_sources = [&](const vector<idx_t> &source_indices) {
		return SljitAddProjectionSourceColumns(source_indices, required_semantic_inputs.size(),
		                                       required_semantic_inputs);
	};
	auto ignore_constant = [&](const SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto mark_predicate = [&](const SljitNativeRegionExpressionPlan &plan) {
		if (plan.predicate) {
			return SljitAddProjectionPredicateSourceColumns(*plan.predicate, required_semantic_inputs.size(),
			                                                required_semantic_inputs);
		}
		return mark_sources(plan.expression_tree_source_indices);
	};
	if (aggregate_update.plan.group_expressions.empty()) {
		for (auto &group : groups) {
			if (!mark_source(group.input_index)) {
				return false;
			}
		}
		return true;
	}
	if (aggregate_update.plan.group_expressions.size() != groups.size()) {
		return false;
	}
	for (auto &group_expression : aggregate_update.plan.group_expressions) {
		auto plan = group_expression.Copy(true, false);
		if (!SljitTryApplyProjectionPlanSources(plan, mark_source, mark_sources, ignore_constant, mark_predicate)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildProjectionPerfectHashAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	auto &projection_op = descriptor.Projection();
	if (sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || !aggregate_update.plan.use_perfect_hash_group_lookup ||
	    !aggregate_update.fused_payload_update_owns_group_lookup || !aggregate_update.fused_payload_update.Function() ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size() ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return descriptor.Block("aggregate_shape");
	}
	vector<uint8_t> required_semantic_inputs(projection_op.projections.size(), 0);
	if (!SljitTryCollectPerfectHashGroupExpressionSources(aggregate_update, sink_info.groups,
	                                                      required_semantic_inputs)) {
		return descriptor.Block("group_sources");
	}
	vector<idx_t> payload_source_indices;
	auto mark_count_star_payload = []() {
		return true;
	};
	auto mark_fused_payload_source = [&](idx_t semantic_projection_idx) {
		if (!SljitAddProjectionSourceColumn(semantic_projection_idx, required_semantic_inputs.size(),
		                                    required_semantic_inputs)) {
			return false;
		}
		payload_source_indices.push_back(semantic_projection_idx);
		return true;
	};
	auto check_direct_payload = [](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	auto mark_direct_payload = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		if (!SljitAddProjectionSourceColumn(aggregate.payload_index, required_semantic_inputs.size(),
		                                    required_semantic_inputs)) {
			return false;
		}
		payload_source_indices.push_back(aggregate.payload_index);
		return true;
	};
	SljitAggregatePayloadSourceLayout payload_source_layout;
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, payload_source_layout, "payload_contract", mark_count_star_payload,
	        mark_fused_payload_source, check_direct_payload, mark_direct_payload,
	        [&](const char *blocker) { return descriptor.Block(blocker); })) {
		return false;
	}
	for (idx_t semantic_idx = 0; semantic_idx < required_semantic_inputs.size(); semantic_idx++) {
		if (!required_semantic_inputs[semantic_idx]) {
			continue;
		}
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, semantic_idx,
		                                            projection_idx) ||
		    !SljitTrySetJoinProjectionAggregateInputAt(binding, descriptor, projection_op, projection_idx,
		                                               semantic_idx)) {
			return descriptor.Block("input_projection");
		}
	}
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		SljitNativeRegionExpressionPlan reference_group;
		reference_group.kind = SljitNativeRegionExpressionKind::REFERENCE;
		reference_group.return_type = group.type;
		reference_group.source_index = group.input_index;
		auto &group_expression = aggregate_update.plan.group_expressions.empty()
		                             ? reference_group
		                             : aggregate_update.plan.group_expressions[group_idx];
		idx_t source_semantic_idx;
		if (!SljitTryFindSingleProjectionPlanSource(group_expression, source_semantic_idx)) {
			return descriptor.Block("input_group_source");
		}
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, source_semantic_idx,
		                                            projection_idx)) {
			return descriptor.Block("input_group_projection");
		}
		ExecutionRowPointerGroupKeySource group_source;
		SljitInitializeInputVectorGroupKeySource(source_semantic_idx, projection_op.output_types[projection_idx],
		                                         group.type, group_source);
		if (!SljitTryFinalizeRowPointerGroupKeySource(group_expression, group.type, group_source)) {
			return descriptor.Block("input_group_expression");
		}
		descriptor.group_sources.push_back(std::move(group_source));
	}
	descriptor.payload_source_indices = std::move(payload_source_indices);
	descriptor.payload_source_layout = payload_source_layout;
	if (descriptor.input_sources.empty()) {
		return descriptor.Block("input_sources");
	}
	descriptor.MarkReady();
	return true;
}

} // namespace duckdb

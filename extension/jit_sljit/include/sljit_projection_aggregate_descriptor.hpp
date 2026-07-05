//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_descriptor.hpp"
#include "sljit_post_join_projection_runtime.hpp"

namespace duckdb {

static bool SljitTryBuildProjectionUngroupedAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr);

static bool SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    idx_t aggregate_idx, SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	if (aggregate_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return descriptor.Block("hash_join_binding");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return descriptor.Block("hash_join_shape");
	}
	auto &aggregate_op = ops[aggregate_idx];
	if (aggregate_op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return SljitTryBuildProjectionUngroupedAggregateDescriptor(binding, aggregate_op, descriptor,
		                                                           producer_projection_op);
	}
	return SljitTryBuildProjectionRowPointerAggregateDescriptor(binding, aggregate_op, descriptor,
	                                                            semantic_to_projection, producer_projection_op);
}

static bool SljitTryBuildPreparedProjectionAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    idx_t projection_idx, SljitExecutableRegionOp &projection_op, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	descriptor.BorrowProjection(projection_idx, projection_op);
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, hash_join_idx, aggregate_idx, descriptor, semantic_to_projection, producer_projection_op);
}

static bool SljitTryBuildProjectionChainAggregateDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                            SljitRegionExecutionScratch &scratch,
                                                            SljitPostJoinProjectionStrategy &post_join_projection,
                                                            idx_t aggregate_idx,
                                                            SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (aggregate_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	SljitExecutableRegionOp semantic_projection;
	string compose_blocker;
	if (!SljitBuildProjectionChainComposedProjection(ops, post_join_projection.first_projection_idx,
	                                                 post_join_projection.final_projection_idx, semantic_projection,
	                                                 optional_ptr<string>(&compose_blocker))) {
		if (compose_blocker.empty()) {
			compose_blocker = "semantic_compose";
		} else {
			compose_blocker = "semantic_compose_" + compose_blocker;
		}
		return descriptor.Block(compose_blocker.c_str());
	}
	descriptor.OwnProjection(post_join_projection.final_projection_idx, std::move(semantic_projection));
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, post_join_projection.hash_join_idx, aggregate_idx, descriptor);
}

static bool SljitTryBuildSingleProjectionAggregateDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                             SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
                                                             idx_t projection_idx, idx_t aggregate_idx,
                                                             SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (projection_idx >= ops.size()) {
		return descriptor.Block("operator_bounds");
	}
	return SljitTryBuildPreparedProjectionAggregateDescriptor(ops, scratch, hash_join_idx, projection_idx,
	                                                          ops[projection_idx], aggregate_idx, descriptor);
}

static bool SljitTryMarkProjectionAggregateRequiredOutput(const SljitExecutableRegionOp &projection_op,
                                                          idx_t projection_idx,
                                                          vector<uint8_t> &required_projection_outputs) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= required_projection_outputs.size()) {
		return false;
	}
	required_projection_outputs[projection_idx] = 1;
	return true;
}

static bool SljitTryBuildProjectionAggregateRequiredOutputs(const SljitExecutableRegionOp &projection_op,
                                                            SljitExecutableRegionOp &aggregate_op,
                                                            vector<uint8_t> &required_projection_outputs) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	required_projection_outputs.assign(projection_op.projections.size(), 0);
	for (auto &group : sink_info.groups) {
		if (!SljitTryMarkProjectionAggregateRequiredOutput(projection_op, group.input_index,
		                                                   required_projection_outputs)) {
			return false;
		}
	}
	if (aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return false;
	}
	auto mark_count_star_payload = []() {
		return true;
	};
	auto mark_payload_source = [&](idx_t semantic_projection_idx) {
		return SljitTryMarkProjectionAggregateRequiredOutput(projection_op, semantic_projection_idx,
		                                                     required_projection_outputs);
	};
	auto check_direct_payload = [](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	auto mark_direct_payload = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		return SljitTryMarkProjectionAggregateRequiredOutput(projection_op, aggregate.payload_index,
		                                                     required_projection_outputs);
	};
	bool uses_fused_payload_update;
	return SljitTryBuildAggregatePayloadSourceIndices(
	    aggregate_update, sink_info.aggregates, uses_fused_payload_update, "payload_contract", mark_count_star_payload,
	    mark_payload_source, check_direct_payload, mark_direct_payload, [](const char *) { return false; });
}

static bool SljitTryAddExecutableExpressionSourceColumns(const SljitExecutableRegionExpression &expr,
                                                         idx_t input_column_count,
                                                         vector<uint8_t> &referenced_columns) {
	if (!expr.input_source_indices.empty()) {
		return SljitAddProjectionSourceColumns(expr.input_source_indices, input_column_count, referenced_columns);
	}
	return SljitAddProjectionExpressionSourceColumns(expr.plan, input_column_count, referenced_columns);
}

static bool SljitTryRemapProjectionSourceIndex(idx_t &source_index, const vector<idx_t> &projection_to_input) {
	if (source_index >= projection_to_input.size() ||
	    projection_to_input[source_index] == DConstants::INVALID_INDEX) {
		return false;
	}
	source_index = projection_to_input[source_index];
	return true;
}

static bool SljitTryRemapProjectionSourceIndices(vector<idx_t> &source_indices,
                                                 const vector<idx_t> &projection_to_input) {
	for (auto &source_index : source_indices) {
		if (!SljitTryRemapProjectionSourceIndex(source_index, projection_to_input)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryRemapProjectionPredicateSourceIndices(SljitNativePredicate &predicate,
                                                          const vector<idx_t> &projection_to_input) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapProjectionSourceIndex(source_index, projection_to_input);
	};
	auto remap_sources = [&](SljitNativePredicate &predicate) {
		return SljitTryRemapProjectionSourceIndices(predicate.source_indices, projection_to_input);
	};
	auto ignore_sources = [&](SljitNativePredicate &) {
		return true;
	};
	return SljitTryApplyProjectionPredicateSources(predicate, remap_source, remap_sources, ignore_sources);
}

static bool SljitTryRemapProjectionPlanSourceIndices(SljitNativeRegionExpressionPlan &plan,
                                                     const vector<idx_t> &projection_to_input) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapProjectionSourceIndex(source_index, projection_to_input);
	};
	auto remap_sources = [&](vector<idx_t> &source_indices) {
		return SljitTryRemapProjectionSourceIndices(source_indices, projection_to_input);
	};
	auto ignore_constant = [&](SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto remap_predicate = [&](SljitNativeRegionExpressionPlan &plan) {
		if (plan.predicate) {
			return SljitTryRemapProjectionPredicateSourceIndices(*plan.predicate, projection_to_input);
		}
		return SljitTryRemapProjectionSourceIndices(plan.expression_tree_source_indices, projection_to_input);
	};
	return SljitTryApplyProjectionPlanSources(plan, remap_source, remap_sources, ignore_constant, remap_predicate);
}

static bool SljitTryBuildUngroupedAggregateRequiredProjectionOutputs(
    SljitExecutableRegionOp &projection_op, SljitExecutableRegionOp &aggregate_op,
    vector<uint8_t> &required_projection_outputs, optional_ptr<vector<idx_t>> fused_payload_sources = nullptr) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || !sink_info.groups.empty() ||
	    !aggregate_update.plan.use_primitive_payloads || aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return false;
	}
	if (!aggregate_update.fused_payload_update_function &&
	    aggregate_update.payload_update_functions.size() != sink_info.aggregates.size()) {
		return false;
	}

	required_projection_outputs.assign(projection_op.projections.size(), 0);
	vector<idx_t> combined_sources;
	if (aggregate_update.fused_payload_update_function &&
	    SljitFusedAggregatePayloadsUseTypedExpressionTrees(aggregate_update.payloads, sink_info.aggregates) &&
	    SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, sink_info.aggregates,
	                                                combined_sources)) {
		for (auto source_idx : combined_sources) {
			if (!SljitTryMarkProjectionAggregateRequiredOutput(projection_op, source_idx,
			                                                   required_projection_outputs)) {
				return false;
			}
		}
		if (fused_payload_sources) {
			*fused_payload_sources = std::move(combined_sources);
		}
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.child_types.size() != 1) {
			return false;
		}
		if (!SljitTryAddExecutableExpressionSourceColumns(aggregate_update.payloads[payload_idx],
		                                                  projection_op.projections.size(),
		                                                  required_projection_outputs)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryAddJoinProjectionAggregateRequiredInput(
    const ExecutionHashJoinProbeBinding &binding, SljitJoinProjectionAggregateDescriptor &descriptor,
    vector<idx_t> &projection_to_input, idx_t projection_idx,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	auto &projection_op = descriptor.Projection();
	if (projection_idx >= projection_to_input.size()) {
		return false;
	}
	if (projection_to_input[projection_idx] != DConstants::INVALID_INDEX) {
		return true;
	}
	idx_t input_idx;
	if (!SljitTryAddJoinLHSInputAggregateInputFromProjection(binding, descriptor, projection_op, projection_idx,
	                                                         input_idx, producer_projection_op) &&
	    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
		return false;
	}
	projection_to_input[projection_idx] = input_idx;
	return true;
}

static bool SljitTryBuildRemappedUngroupedAggregatePayloads(
    SljitExecutableAggregateUpdate &aggregate_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<idx_t> &projection_to_input, const vector<idx_t> &fused_payload_sources,
    vector<SljitExecutableRegionExpression> &remapped_payloads) {
	remapped_payloads.clear();
	remapped_payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		remapped_payloads.emplace_back();
		SljitBuildBorrowedProjectionExpression(payload, remapped_payloads.back());
	}
	if (aggregate_update.fused_payload_update_function && !fused_payload_sources.empty()) {
		vector<idx_t> compact_sources;
		compact_sources.reserve(fused_payload_sources.size());
		for (auto source_idx : fused_payload_sources) {
			if (source_idx >= projection_to_input.size() ||
			    projection_to_input[source_idx] == DConstants::INVALID_INDEX) {
				return false;
			}
			compact_sources.push_back(projection_to_input[source_idx]);
		}
		for (auto &payload : remapped_payloads) {
			payload.input_source_indices = compact_sources;
			if (payload.input_source_not_null.size() != compact_sources.size()) {
				payload.input_source_not_null.assign(compact_sources.size(), false);
			}
		}
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < remapped_payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = remapped_payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (!payload.input_source_indices.empty()) {
			if (!SljitTryRemapProjectionSourceIndices(payload.input_source_indices, projection_to_input)) {
				return false;
			}
			if (payload.plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
			    payload.plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				continue;
			}
		}
		if (!SljitTryRemapProjectionPlanSourceIndices(payload.plan, projection_to_input)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildProjectionUngroupedAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op) {
	auto &projection_op = descriptor.Projection();
	vector<uint8_t> required_projection_outputs;
	vector<idx_t> fused_payload_sources;
	if (!SljitTryBuildUngroupedAggregateRequiredProjectionOutputs(
	        projection_op, aggregate_op, required_projection_outputs, optional_ptr<vector<idx_t>>(&fused_payload_sources))) {
		return descriptor.Block("ungrouped_payload_sources");
	}
	vector<idx_t> projection_to_input(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t projection_idx = 0; projection_idx < required_projection_outputs.size(); projection_idx++) {
		if (!required_projection_outputs[projection_idx]) {
			continue;
		}
		if (!SljitTryAddJoinProjectionAggregateRequiredInput(binding, descriptor, projection_to_input, projection_idx,
		                                                     producer_projection_op)) {
			return descriptor.Block("ungrouped_input_source");
		}
	}
	if (!SljitTryBuildRemappedUngroupedAggregatePayloads(aggregate_op.aggregate_update,
	                                                     aggregate_op.aggregate_update.plan.sink_info.aggregates,
	                                                     projection_to_input, fused_payload_sources,
	                                                     descriptor.remapped_payloads)) {
		return descriptor.Block("ungrouped_payload_remap");
	}
	descriptor.MarkReady();
	return true;
}

static bool SljitTryBuildMappedPostJoinProjectionAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor, const vector<idx_t> &output_column_map,
    idx_t output_projection_idx = DConstants::INVALID_INDEX) {
	if (!scratch.HasOperatorBinding(post_join_projection.hash_join_idx)) {
		return descriptor.Block("hash_join_binding");
	}
	auto &binding = scratch.OperatorBinding(post_join_projection.hash_join_idx).hash_join_probe;
	SljitExecutableRegionOp semantic_projection;
	optional_ptr<SljitExecutableRegionOp> semantic_projection_ptr;
	if (post_join_projection.first_projection_idx == post_join_projection.final_projection_idx) {
		if (post_join_projection.first_projection_idx >= ops.size()) {
			return descriptor.Block("operator_bounds");
		}
		semantic_projection_ptr = &ops[post_join_projection.first_projection_idx];
	} else {
		string compose_blocker;
		if (!SljitBuildProjectionChainComposedProjection(ops, post_join_projection.first_projection_idx,
		                                                 post_join_projection.final_projection_idx, semantic_projection,
		                                                 optional_ptr<string>(&compose_blocker))) {
			if (compose_blocker.empty()) {
				compose_blocker = "semantic_compose";
			} else {
				compose_blocker = "semantic_compose_" + compose_blocker;
			}
			return descriptor.Block(compose_blocker.c_str());
		}
		semantic_projection_ptr = &semantic_projection;
	}
	vector<uint8_t> required_projection_outputs;
	if (aggregate_idx >= ops.size() || !SljitTryBuildProjectionAggregateRequiredOutputs(
	                                       *semantic_projection_ptr, ops[aggregate_idx], required_projection_outputs)) {
		return descriptor.Block("required_projection_outputs");
	}
	SljitExecutableRegionOp mapped_projection;
	string map_blocker;
	if (!SljitTryBuildHashJoinMappedProjection(output_column_map, binding, *semantic_projection_ptr, mapped_projection,
	                                           optional_ptr<string>(&map_blocker),
	                                           optional_ptr<const vector<uint8_t>>(&required_projection_outputs))) {
		if (map_blocker.empty()) {
			map_blocker = "mapped_projection";
		}
		return descriptor.Block(map_blocker.c_str());
	}
	descriptor.OwnProjection(post_join_projection.final_projection_idx, std::move(mapped_projection));
	optional_ptr<SljitExecutableRegionOp> producer_projection_op;
	if (output_projection_idx != DConstants::INVALID_INDEX) {
		if (output_projection_idx >= ops.size() ||
		    ops[output_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return descriptor.Block("producer_projection");
		}
		producer_projection_op = &ops[output_projection_idx];
	}
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, post_join_projection.hash_join_idx, aggregate_idx, descriptor, nullptr, producer_projection_op);
}

static bool
SljitJoinProjectionAggregateInputsUseOnlyProjectionOutputs(const SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.input_sources.size() != descriptor.output_to_projection.size()) {
		return false;
	}
	for (idx_t input_idx = 0; input_idx < descriptor.input_sources.size(); input_idx++) {
		auto &source = descriptor.input_sources[input_idx];
		if (source.kind != SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT ||
		    source.projection_idx != descriptor.output_to_projection[input_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitBuildKeyDomainFitsInt32(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
		return true;
	default:
		return false;
	}
}

static bool SljitBuildKeyDomainFitsSignedTarget(PhysicalType source_type, PhysicalType target_type) {
	switch (target_type) {
	case PhysicalType::INT8:
		switch (source_type) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			return true;
		default:
			return false;
		}
	case PhysicalType::INT16:
		switch (source_type) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::UINT8:
			return true;
		default:
			return false;
		}
	case PhysicalType::INT32:
		return SljitBuildKeyDomainFitsInt32(source_type);
	default:
		return false;
	}
}

static void SljitApplyJoinProjectionGroupCastProofs(vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                    bool source_key0_range_fits_int32) {
	for (auto &source : group_sources) {
		if (source.cast_kind != ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
		    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16 &&
		    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8) {
			source.unchecked_integral_cast = false;
			continue;
		}
		const bool source_key0_range_proven = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
		                                      source_key0_range_fits_int32 && source.hash_join_condition_idx == 0;
		const bool matched_build_key_proves_range =
		    source.hash_join_condition_idx != DConstants::INVALID_INDEX &&
		    SljitBuildKeyDomainFitsSignedTarget(source.hash_join_build_key_physical_type, source.target_physical_type);
		source.unchecked_integral_cast = source_key0_range_proven || matched_build_key_proves_range;
	}
}

static bool SljitTryBuildPostJoinProjectionAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor, optional_ptr<const vector<idx_t>> output_column_map = nullptr,
    idx_t output_projection_idx = DConstants::INVALID_INDEX) {
	if (descriptor.Built()) {
		if (!descriptor.ProducerOutputColumnMapMatches(output_column_map)) {
			return descriptor.Block("producer_shape_changed");
		}
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	descriptor.SetProducerOutputColumnMap(output_column_map);
	if (output_column_map) {
		return SljitTryBuildMappedPostJoinProjectionAggregateDescriptor(
		    ops, scratch, post_join_projection, aggregate_idx, descriptor, *output_column_map, output_projection_idx);
	}
	if (post_join_projection.first_projection_idx == post_join_projection.final_projection_idx) {
		return SljitTryBuildSingleProjectionAggregateDescriptor(ops, scratch, post_join_projection.hash_join_idx,
		                                                        post_join_projection.first_projection_idx,
		                                                        aggregate_idx, descriptor);
	}
	return SljitTryBuildProjectionChainAggregateDescriptor(ops, scratch, post_join_projection, aggregate_idx,
	                                                       descriptor);
}

static SljitExecutableRegionExpression SljitIdentityProjectionExpression(idx_t source_idx, const LogicalType &type) {
	SljitExecutableRegionExpression expression;
	expression.plan.kind = SljitNativeRegionExpressionKind::REFERENCE;
	expression.plan.return_type = type;
	expression.plan.source_index = source_idx;
	expression.plan.references_region_input = true;
	return expression;
}

static bool SljitTryBuildSelectedJoinAggregateInputDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                              SljitRegionExecutionScratch &scratch,
                                                              idx_t hash_join_idx, idx_t aggregate_idx,
                                                              SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	if (aggregate_idx >= ops.size() || !scratch.HasOperatorBinding(hash_join_idx)) {
		return descriptor.Block("operator_bounds");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return descriptor.Block("hash_join_shape");
	}
	auto &aggregate_update = ops[aggregate_idx].aggregate_update;
	if (aggregate_update.plan.input_types.size() != binding.output_types.size()) {
		return descriptor.Block("input_shape");
	}

	SljitExecutableRegionOp identity_projection;
	identity_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	identity_projection.input_types = binding.output_types;
	identity_projection.output_types = aggregate_update.plan.input_types;
	identity_projection.output_not_null.assign(identity_projection.output_types.size(), false);
	identity_projection.projections.reserve(identity_projection.output_types.size());
	for (idx_t input_idx = 0; input_idx < identity_projection.output_types.size(); input_idx++) {
		if (identity_projection.output_types[input_idx] != binding.output_types[input_idx]) {
			return descriptor.Block("input_type");
		}
		identity_projection.projections.push_back(
		    SljitIdentityProjectionExpression(input_idx, identity_projection.output_types[input_idx]));
	}
	descriptor.OwnProjection(DConstants::INVALID_INDEX, std::move(identity_projection));
	if (aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return SljitTryBuildProjectionUngroupedAggregateDescriptor(binding, ops[aggregate_idx], descriptor);
	}
	auto &projection_op = descriptor.Projection();
	for (idx_t input_idx = 0; input_idx < projection_op.output_types.size(); input_idx++) {
		SljitJoinProjectionAggregateInputSource source;
		source.kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
		source.projection_idx = input_idx;
		source.type = projection_op.output_types[input_idx];
		descriptor.input_sources.push_back(std::move(source));
		descriptor.output_to_projection.push_back(input_idx);
		descriptor.input_types.push_back(projection_op.output_types[input_idx]);
	}
	descriptor.MarkReady();
	return true;
}

} // namespace duckdb

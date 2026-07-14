//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_ungrouped_descriptor.hpp"
#include "sljit_projection_perfect_hash_aggregate_descriptor.hpp"

namespace duckdb {

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
	if (aggregate_op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) {
		return SljitTryBuildProjectionPerfectHashAggregateDescriptor(binding, aggregate_op, descriptor,
		                                                             semantic_to_projection);
	}
	return SljitTryBuildProjectionRowPointerAggregateDescriptor(binding, aggregate_op, descriptor,
	                                                            semantic_to_projection, producer_projection_op);
}

static bool SljitTryBuildPostJoinSemanticProjection(vector<SljitExecutableRegionOp> &ops,
                                                    SljitPostJoinProjectionStrategy &post_join_projection,
                                                    SljitJoinProjectionAggregateDescriptor &descriptor,
                                                    unique_ptr<SljitExecutableRegionOp> &owned_projection,
                                                    optional_ptr<SljitExecutableRegionOp> &semantic_projection) {
	if (post_join_projection.first_projection_idx == post_join_projection.final_projection_idx) {
		if (post_join_projection.first_projection_idx >= ops.size()) {
			return descriptor.Block("operator_bounds");
		}
		semantic_projection = &ops[post_join_projection.first_projection_idx];
		return true;
	}
	owned_projection = make_uniq<SljitExecutableRegionOp>();
	string compose_blocker;
	if (!SljitBuildProjectionChainComposedProjection(ops, post_join_projection.first_projection_idx,
	                                                 post_join_projection.final_projection_idx, *owned_projection,
	                                                 optional_ptr<string>(&compose_blocker))) {
		if (compose_blocker.empty()) {
			compose_blocker = "semantic_compose";
		} else {
			compose_blocker = "semantic_compose_" + compose_blocker;
		}
		return descriptor.Block(compose_blocker.c_str());
	}
	semantic_projection = owned_projection.get();
	return true;
}

static bool SljitTryBuildUnmappedPostJoinProjectionAggregateDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t aggregate_idx,
    SljitJoinProjectionAggregateDescriptor &descriptor) {
	unique_ptr<SljitExecutableRegionOp> owned_projection;
	optional_ptr<SljitExecutableRegionOp> semantic_projection;
	if (!SljitTryBuildPostJoinSemanticProjection(ops, post_join_projection, descriptor, owned_projection,
	                                             semantic_projection)) {
		return false;
	}
	if (owned_projection) {
		descriptor.OwnProjection(post_join_projection.final_projection_idx, std::move(*owned_projection));
	} else {
		descriptor.BorrowProjection(post_join_projection.first_projection_idx, *semantic_projection);
	}
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, post_join_projection.hash_join_idx, aggregate_idx, descriptor);
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
	if ((sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	     sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	     sink_info.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) ||
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
	SljitAggregatePayloadSourceLayout payload_source_layout;
	return SljitTryBuildAggregatePayloadSourceIndices(
	    aggregate_update, sink_info.aggregates, payload_source_layout, "payload_contract", mark_count_star_payload,
	    mark_payload_source, check_direct_payload, mark_direct_payload, [](const char *) { return false; });
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
	unique_ptr<SljitExecutableRegionOp> owned_projection;
	optional_ptr<SljitExecutableRegionOp> semantic_projection;
	if (!SljitTryBuildPostJoinSemanticProjection(ops, post_join_projection, descriptor, owned_projection,
	                                             semantic_projection)) {
		return false;
	}
	vector<uint8_t> required_projection_outputs;
	if (aggregate_idx >= ops.size() || !SljitTryBuildProjectionAggregateRequiredOutputs(
	                                       *semantic_projection, ops[aggregate_idx], required_projection_outputs)) {
		return descriptor.Block("required_projection_outputs");
	}
	auto mapped_projection = make_uniq<SljitExecutableRegionOp>();
	string map_blocker;
	if (!SljitTryBuildHashJoinMappedProjection(output_column_map, binding, *semantic_projection, *mapped_projection,
	                                           optional_ptr<string>(&map_blocker),
	                                           optional_ptr<const vector<uint8_t>>(&required_projection_outputs))) {
		if (map_blocker.empty()) {
			map_blocker = "mapped_projection";
		}
		return descriptor.Block(map_blocker.c_str());
	}
	descriptor.OwnProjection(post_join_projection.final_projection_idx, std::move(*mapped_projection));
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
		if (!ExecutionGroupKeyCastIsNarrowingIntegral(source.cast_kind)) {
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
	return SljitTryBuildUnmappedPostJoinProjectionAggregateDescriptor(ops, scratch, post_join_projection, aggregate_idx,
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

static bool SljitTryBuildSelectedJoinIdentityProjection(const ExecutionHashJoinProbeBinding &binding,
                                                        const SljitExecutableRegionOp &aggregate_op,
                                                        SljitJoinProjectionAggregateDescriptor &descriptor) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	if (aggregate_update.plan.input_types.size() != binding.output_types.size()) {
		return descriptor.Block("input_shape");
	}

	auto identity_projection = make_uniq<SljitExecutableRegionOp>();
	identity_projection->kind = SljitNativeRegionOpKind::PROJECTION;
	identity_projection->input_types = binding.output_types;
	identity_projection->output_types = aggregate_update.plan.input_types;
	identity_projection->output_not_null.assign(identity_projection->output_types.size(), false);
	identity_projection->projections.reserve(identity_projection->output_types.size());
	for (idx_t input_idx = 0; input_idx < identity_projection->output_types.size(); input_idx++) {
		if (identity_projection->output_types[input_idx] != binding.output_types[input_idx]) {
			return descriptor.Block("input_type");
		}
		identity_projection->projections.push_back(
		    SljitIdentityProjectionExpression(input_idx, identity_projection->output_types[input_idx]));
	}
	descriptor.OwnProjection(DConstants::INVALID_INDEX, std::move(*identity_projection));
	return true;
}

static bool SljitTryBuildMappedSelectedJoinAggregateInputDescriptor(vector<SljitExecutableRegionOp> &ops,
                                                                    SljitRegionExecutionScratch &scratch,
                                                                    idx_t hash_join_idx, idx_t aggregate_idx,
                                                                    SljitJoinProjectionAggregateDescriptor &descriptor,
                                                                    optional_ptr<const vector<idx_t>> output_column_map,
                                                                    idx_t output_projection_idx) {
	if (!output_column_map) {
		return descriptor.Block("producer_output_map");
	}
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
	if (output_projection_idx == DConstants::INVALID_INDEX || output_projection_idx >= ops.size() ||
	    ops[output_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
		return descriptor.Block("producer_projection");
	}
	if (!SljitTryBuildSelectedJoinIdentityProjection(binding, ops[aggregate_idx], descriptor)) {
		return false;
	}
	auto producer_projection_op = optional_ptr<SljitExecutableRegionOp>(&ops[output_projection_idx]);
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(
	    ops, scratch, hash_join_idx, aggregate_idx, descriptor, nullptr, producer_projection_op);
}

static bool SljitTryBuildSelectedJoinAggregateInputDescriptor(
    vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
    idx_t aggregate_idx, SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> output_column_map = nullptr,
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
		return SljitTryBuildMappedSelectedJoinAggregateInputDescriptor(
		    ops, scratch, hash_join_idx, aggregate_idx, descriptor, output_column_map, output_projection_idx);
	}
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
	if (!SljitTryBuildSelectedJoinIdentityProjection(binding, ops[aggregate_idx], descriptor)) {
		return false;
	}
	return SljitTryBuildPreparedProjectionAggregateDescriptorFromProjection(ops, scratch, hash_join_idx, aggregate_idx,
	                                                                        descriptor);
}

} // namespace duckdb

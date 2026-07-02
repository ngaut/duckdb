//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"
#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

template <class HANDLE_COUNT_STAR, class HANDLE_FUSED_SOURCE, class CHECK_DIRECT_PAYLOAD, class HANDLE_DIRECT_PAYLOAD,
          class SET_BLOCKER>
static bool SljitTryBuildAggregatePayloadSourceIndices(
    SljitExecutableAggregateUpdate &aggregate_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    bool &uses_fused_update, const char *payload_reference_blocker, HANDLE_COUNT_STAR &&handle_count_star,
    HANDLE_FUSED_SOURCE &&handle_fused_source, CHECK_DIRECT_PAYLOAD &&check_direct_payload,
    HANDLE_DIRECT_PAYLOAD &&handle_direct_payload, SET_BLOCKER &&set_blocker) {
	uses_fused_update = false;
	vector<idx_t> fused_payload_sources;
	if (aggregate_update.fused_payload_update_function &&
	    SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregates, fused_payload_sources)) {
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

static bool SljitTryAddJoinProjectionAggregateInput(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                    vector<idx_t> &projection_to_input, idx_t projection_idx,
                                                    idx_t &input_idx) {
	auto &projection_op = descriptor.Projection();
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	if (projection_to_input[projection_idx] != DConstants::INVALID_INDEX) {
		input_idx = projection_to_input[projection_idx];
		return true;
	}
	auto &projection = projection_op.projections[projection_idx];
	auto &output_type = projection_op.output_types[projection_idx];
	if (projection.plan.return_type != output_type) {
		return false;
	}
	input_idx = descriptor.output_to_projection.size();
	projection_to_input[projection_idx] = input_idx;
	descriptor.output_to_projection.push_back(projection_idx);
	descriptor.input_types.push_back(output_type);
	return true;
}

static bool SljitTryBuildProjectionRowPointerAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	auto &projection_op = descriptor.Projection();
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    !aggregate_update.plan.use_primitive_payloads || !aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return descriptor.Block("aggregate_shape");
	}
	if (!SljitTryBuildRowPointerGroupKeySources(binding, projection_op, aggregate_op, descriptor.group_sources,
	                                            semantic_to_projection)) {
		return descriptor.Block("group_sources");
	}

	vector<idx_t> projection_to_input(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		auto &group_source = descriptor.group_sources[group_idx];
		if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		idx_t projection_idx;
		idx_t input_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, group.input_index,
		                                            projection_idx) ||
		    projection_op.output_types[projection_idx].InternalType() != group.type.InternalType() ||
		    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
			return descriptor.Block("input_group_projection");
		}
		SljitInitializeInputVectorGroupKeySource(input_idx, group.type.InternalType(), group.type, group_source);
		group_source.ready = true;
		group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	}

	auto add_count_star_payload = [&]() {
		descriptor.payload_source_indices.push_back(DConstants::INVALID_INDEX);
		return true;
	};
	auto add_fused_payload_source = [&](idx_t semantic_projection_idx) {
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, semantic_projection_idx,
		                                            projection_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		idx_t input_idx;
		if (!SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	auto add_direct_payload_source = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, aggregate.payload_index,
		                                            projection_idx)) {
			return descriptor.Block("payload_projection");
		}
		idx_t input_idx;
		if (!SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx) ||
		    descriptor.input_types[input_idx].InternalType() != aggregate.child_types[0].InternalType()) {
			return descriptor.Block("payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	bool uses_fused_payload_update;
	auto check_direct_payload = [&](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, uses_fused_payload_update, "payload_contract",
	        add_count_star_payload, add_fused_payload_source, check_direct_payload, add_direct_payload_source,
	        [&](const char *blocker) { return descriptor.Block(blocker); })) {
		return false;
	}
	D_ASSERT(!uses_fused_payload_update || !descriptor.payload_source_indices.empty());
	if (descriptor.payload_source_indices.empty()) {
		return descriptor.Block("payload_sources");
	}
	descriptor.MarkReady();
	return true;
}

template <class INPUT_COLUMN_IS_OMITTED>
static bool SljitTryBuildProjectionInputRowPointerGroupDescriptor(SljitExecutableRegionOp &projection_op,
                                                                  SljitExecutableRegionOp &aggregate_op,
                                                                  DataChunk &payload_input,
                                                                  const vector<idx_t> &group_projection_indices,
                                                                  INPUT_COLUMN_IS_OMITTED input_column_is_omitted,
                                                                  SljitJoinProjectionAggregateDescriptor &descriptor) {
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != group_projection_indices.size() || descriptor.payload_source_indices.empty()) {
		return descriptor.Block("aggregate_shape");
	}
	descriptor.group_sources.clear();
	descriptor.group_sources.reserve(sink_info.groups.size());
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		const auto projection_idx = group_projection_indices[group_idx];
		if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
			return descriptor.Block("group_key_index");
		}
		SljitExecutableRegionExpression remapped_expr;
		idx_t input_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
		                                                   input_source_idx)) {
			return descriptor.Block("group_key_projection");
		}
		if (input_source_idx >= payload_input.ColumnCount()) {
			return descriptor.Block("group_key_source");
		}
		if (input_column_is_omitted(input_source_idx)) {
			return descriptor.Block("group_key_omitted_input");
		}
		auto &input_type = payload_input.data[input_source_idx].GetType();
		ExecutionRowPointerGroupKeySource group_source;
		SljitInitializeInputVectorGroupKeySource(input_source_idx, input_type.InternalType(), group.type, group_source);
		if (!SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source)) {
			return descriptor.Block("group_key_cast");
		}
		descriptor.group_sources.push_back(std::move(group_source));
	}
	if (descriptor.group_sources.empty()) {
		return descriptor.Block("group_sources");
	}
	descriptor.MarkReady();
	return true;
}

template <class INPUT_COLUMN_IS_OMITTED>
static bool SljitTryBuildProjectionInputRowPointerAggregateDescriptor(
    SljitExecutableRegionOp &projection_op, SljitExecutableRegionOp &aggregate_op, DataChunk &payload_input,
    const vector<idx_t> &group_projection_indices, const vector<idx_t> &payload_source_indices,
    const char *disabled_reason, INPUT_COLUMN_IS_OMITTED input_column_is_omitted,
    SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	descriptor.BorrowProjection(projection_op);
	if (disabled_reason) {
		return descriptor.Block(disabled_reason);
	}
	if (payload_source_indices.empty()) {
		return descriptor.Block("payload_sources");
	}
	descriptor.payload_source_indices = payload_source_indices;
	return SljitTryBuildProjectionInputRowPointerGroupDescriptor(
	    projection_op, aggregate_op, payload_input, group_projection_indices, input_column_is_omitted, descriptor);
}

static bool SljitDescriptorUsesRowPointerGroupSource(const SljitJoinProjectionAggregateDescriptor &descriptor) {
	for (auto &source : descriptor.group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb

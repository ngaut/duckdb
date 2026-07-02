//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_final_projection_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_descriptor.hpp"

namespace duckdb {

static bool SljitBuildFinalSplitPayloadDescriptor(SljitFinalProjectionAggregateBridge &bridge,
                                                  SljitExecutableRegionOp &final_projection_op,
                                                  SljitExecutableRegionOp &aggregate_op, DataChunk &payload_input) {
	if (bridge.split_payload_descriptor.Built()) {
		return bridge.split_payload_descriptor.Ready();
	}
	auto set_blocker = [&](const char *blocker) {
		bridge.split_payload_uses_fused_update = false;
		return bridge.split_payload_descriptor.Block(blocker);
	};
	if (final_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return set_blocker("operator_kind");
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &aggregate_plan = aggregate_update.plan;
	auto &sink_info = aggregate_plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    sink_info.aggregates.empty() || !aggregate_plan.use_primitive_payloads ||
	    !aggregate_plan.use_grouped_state_addresses || aggregate_plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup) {
		return set_blocker("aggregate_shape");
	}
	if (aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return set_blocker("payload_count");
	}
	bridge.group_key_types.clear();
	bridge.group_projection_indices.clear();
	bridge.group_key_types.reserve(sink_info.groups.size());
	bridge.group_projection_indices.reserve(sink_info.groups.size());
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		if (!group.supported_reference || group.input_index != group_idx ||
		    group.input_index >= final_projection_op.projections.size() ||
		    group.input_index >= final_projection_op.output_types.size()) {
			return set_blocker("group_key_not_dense");
		}
		auto &group_projection = final_projection_op.projections[group.input_index].plan;
		if (group_projection.return_type.InternalType() != group.type.InternalType() ||
		    final_projection_op.output_types[group.input_index].InternalType() != group.type.InternalType()) {
			return set_blocker("group_key_type");
		}
		bridge.group_projection_indices.push_back(group.input_index);
		bridge.group_key_types.push_back(final_projection_op.output_types[group.input_index]);
	}
	bridge.payload_source_indices.clear();
	bridge.payload_source_indices.reserve(sink_info.aggregates.size());
	auto add_count_star_payload = [&]() {
		bridge.payload_source_indices.push_back(DConstants::INVALID_INDEX);
		return true;
	};
	auto add_fused_payload_source = [&](idx_t projection_source_idx) {
		if (projection_source_idx >= final_projection_op.projections.size() ||
		    projection_source_idx >= final_projection_op.output_types.size()) {
			return set_blocker("fused_payload_source");
		}
		SljitExecutableRegionExpression remapped_expr;
		idx_t input_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(final_projection_op.projections[projection_source_idx],
		                                                   remapped_expr, input_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
			return set_blocker("fused_payload_projection");
		}
		if (input_source_idx >= payload_input.ColumnCount() ||
		    final_projection_op.output_types[projection_source_idx] != payload_input.data[input_source_idx].GetType()) {
			return set_blocker("fused_payload_type");
		}
		bridge.payload_source_indices.push_back(input_source_idx);
		return true;
	};
	auto add_direct_payload_source = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		auto &payload_projection = final_projection_op.projections[aggregate.payload_index].plan;
		if (payload_projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    payload_projection.source_index >= payload_input.ColumnCount()) {
			return set_blocker("payload_source");
		}
		if (payload_projection.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
		    payload_input.data[payload_projection.source_index].GetType().InternalType() !=
		        aggregate.child_types[0].InternalType()) {
			return set_blocker("payload_type");
		}
		bridge.payload_source_indices.push_back(payload_projection.source_index);
		return true;
	};
	auto check_direct_payload = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		if (aggregate.payload_index >= final_projection_op.projections.size()) {
			return set_blocker("payload_contract");
		}
		return true;
	};
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, bridge.split_payload_uses_fused_update,
	        "payload_not_final_reference", add_count_star_payload, add_fused_payload_source, check_direct_payload,
	        add_direct_payload_source, set_blocker)) {
		return false;
	}
	bridge.split_payload_descriptor.MarkReady();
	return true;
}

template <class INPUT_COLUMN_IS_OMITTED>
static bool SljitBuildFinalRowPointerGroupDescriptor(SljitFinalProjectionAggregateBridge &bridge,
                                                     SljitExecutableRegionOp &final_projection_op,
                                                     SljitExecutableRegionOp &aggregate_op, DataChunk &payload_input,
                                                     INPUT_COLUMN_IS_OMITTED input_column_is_omitted) {
	const char *disabled_reason = nullptr;
	if (!SljitBuildFinalSplitPayloadDescriptor(bridge, final_projection_op, aggregate_op, payload_input)) {
		disabled_reason = bridge.split_payload_descriptor.blocker.empty()
		                      ? "payload_descriptor"
		                      : bridge.split_payload_descriptor.blocker.c_str();
	}
	return SljitTryBuildProjectionInputRowPointerAggregateDescriptor(
	    final_projection_op, aggregate_op, payload_input, bridge.group_projection_indices,
	    bridge.payload_source_indices, disabled_reason, input_column_is_omitted, bridge.row_pointer_aggregate);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_direct_update_capability_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
    idx_t count) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (execute_sel != nullptr || count != input.size() || !plan.use_primitive_payloads ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != payload_lanes.size()) {
		return false;
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < sink_info.aggregates.size(); aggregate_idx++) {
		auto &aggregate = sink_info.aggregates[aggregate_idx];
		auto lane = payload_lanes[aggregate_idx];
		if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index) {
			return false;
		}
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.child_indices.size() != 1) {
			return false;
		}
		auto &payload = aggregate_update.payloads[aggregate_idx].plan;
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    payload.source_index != aggregate.child_indices[0] || payload.source_index >= input.ColumnCount() ||
		    input.data[payload.source_index].GetType().InternalType() != lane->payload_type) {
			return false;
		}
	}
	return true;
}

static bool SljitCanExecuteDirectNewGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
    idx_t count) {
	return !scratch.DirectNewAggregateUpdateDisabled(op_idx) &&
	       SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count);
}

static bool SljitCanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
    idx_t count) {
	return !scratch.DirectAppendNewAggregateUpdateDisabled(op_idx) &&
	       SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count);
}

static bool SljitCanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
    idx_t count) {
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) ||
	    !SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count)) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1 || sink_info.groups[0].input_index >= input.ColumnCount()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		if (!lane || lane->kind != sink_info.aggregates[payload_idx].primitive_update_kind ||
		    !SljitPreaggregatedPrimitivePayloadSupported(lane->kind, lane->payload_type)) {
			return false;
		}
	}
	return true;
}

static bool SljitCanExecuteDirectGroupedFusedPayloadUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
    idx_t count) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || execute_sel != nullptr || count != input.size() ||
	    !plan.use_primitive_payloads || !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup || !aggregate_update.fused_payload_update_function ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != payload_lanes.size()) {
		return false;
	}
	return SljitFusedGroupedAggregatePayloadsUseRuntimeInputAdapter(aggregate_update.payloads, sink_info.aggregates);
}

enum class SljitFusedTypedPayloadSourceOverrideStatus : uint8_t { NONE, READY, INVALID };

static SljitFusedTypedPayloadSourceOverrideStatus
SljitGetFusedTypedPayloadSourceOverrideStatus(SljitExecutableAggregateUpdate &aggregate_update,
                                              const vector<ExecutionRegionAggregateInput> &aggregates,
                                              DataChunk &payload_input, const vector<idx_t> &payload_source_indices) {
	if (!aggregate_update.fused_payload_update_function) {
		return SljitFusedTypedPayloadSourceOverrideStatus::NONE;
	}
	vector<idx_t> fused_payload_sources;
	if (!SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregates, fused_payload_sources)) {
		return SljitFusedTypedPayloadSourceOverrideStatus::NONE;
	}
	if (payload_source_indices.size() != fused_payload_sources.size()) {
		return SljitFusedTypedPayloadSourceOverrideStatus::INVALID;
	}
	for (auto source_idx : payload_source_indices) {
		if (source_idx >= payload_input.ColumnCount()) {
			return SljitFusedTypedPayloadSourceOverrideStatus::INVALID;
		}
	}
	return SljitFusedTypedPayloadSourceOverrideStatus::READY;
}

static bool SljitCanExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, idx_t count,
    bool &payload_sources_are_fused_override) {
	payload_sources_are_fused_override = false;
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || count != payload_input.size() || count < 2 ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR || !plan.use_primitive_payloads ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.groups.size() != group_sources.size() ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != payload_lanes.size() ||
	    !ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources)) {
		return false;
	}
	auto fused_override_status = SljitGetFusedTypedPayloadSourceOverrideStatus(aggregate_update, sink_info.aggregates,
	                                                                           payload_input, payload_source_indices);
	if (fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::INVALID) {
		return false;
	}
	payload_sources_are_fused_override = fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::READY;
	const bool payload_sources_match_aggregates =
	    !payload_sources_are_fused_override && sink_info.aggregates.size() == payload_source_indices.size();
	if (!payload_sources_match_aggregates && !payload_sources_are_fused_override) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		auto lane = payload_lanes[payload_idx];
		if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index ||
		    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
		    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset ||
		    lane->kind != aggregate.primitive_update_kind ||
		    !SljitPreaggregatedPrimitivePayloadSupported(lane->kind, lane->payload_type)) {
			return false;
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0 || (payload_sources_match_aggregates &&
			                                   payload_source_indices[payload_idx] != DConstants::INVALID_INDEX)) {
				return false;
			}
			continue;
		}
		if (payload_sources_match_aggregates) {
			const auto source_idx = payload_source_indices[payload_idx];
			if (source_idx >= payload_input.ColumnCount() ||
			    payload_input.data[source_idx].GetType().InternalType() != lane->payload_type) {
				return false;
			}
		} else {
			if (op.aggregate_update.payloads[payload_idx].plan.return_type.InternalType() != lane->payload_type) {
				return false;
			}
		}
	}
	return true;
}

static bool SljitCanResolveDirectNewGroupedStateAddresses(SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                          SljitExecutableRegionOp &op, DataChunk &input,
                                                          const SelectionVector *execute_sel, idx_t count) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	return !scratch.DirectNewAggregateUpdateDisabled(op_idx) && execute_sel == nullptr && count == input.size() &&
	       plan.use_primitive_payloads && plan.use_grouped_state_addresses && !plan.use_perfect_hash_group_lookup &&
	       !aggregate_update.fused_payload_update_owns_group_lookup &&
	       sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE && !sink_info.groups.empty();
}

} // namespace duckdb

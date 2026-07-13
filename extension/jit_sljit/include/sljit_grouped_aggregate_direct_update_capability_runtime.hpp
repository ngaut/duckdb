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
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool
SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(SljitExecutableRegionOp &op, DataChunk &input,
                                                    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
                                                    const SelectionVector *execute_sel, idx_t count) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (execute_sel != nullptr || count != input.size() || !plan.UsesPrimitivePayloads() ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != aggregate_update.payload_descriptors.size() ||
	    sink_info.aggregates.size() != reduction_lanes.size()) {
		return false;
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < sink_info.aggregates.size(); aggregate_idx++) {
		auto &aggregate = sink_info.aggregates[aggregate_idx];
		auto &lane_binding = reduction_lanes[aggregate_idx];
		if (!lane_binding.descriptor || !lane_binding.runtime_lane) {
			return false;
		}
		auto &descriptor = *lane_binding.descriptor;
		if (!descriptor.has_payload) {
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
		    input.data[payload.source_index].GetType().InternalType() != descriptor.input_type) {
			return false;
		}
	}
	return true;
}

static bool SljitCanExecuteDirectNewGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count) {
	return !scratch.DirectNewAggregateUpdateDisabled(op_idx) &&
	       SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, reduction_lanes, execute_sel, count);
}

static bool SljitCanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count) {
	return !scratch.DirectAppendNewAggregateUpdateDisabled(op_idx) &&
	       SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, reduction_lanes, execute_sel, count);
}

static bool SljitCanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count) {
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) ||
	    !SljitCanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, reduction_lanes, execute_sel, count)) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1 || sink_info.groups[0].input_index >= input.ColumnCount()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &descriptor = op.aggregate_update.payload_descriptors[payload_idx];
		auto payload_type = descriptor.has_payload ? descriptor.input_type : PhysicalType::INVALID;
		if (!SljitPreaggregatedPrimitivePayloadSupported(descriptor.primitive_kind, payload_type)) {
			return false;
		}
	}
	return true;
}

static bool SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count) {
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || execute_sel != nullptr || count != input.size() ||
	    !plan.UsesPrimitivePayloads() || !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup || !aggregate_update.fused_payload_update.Function() ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != reduction_lanes.size() || !SljitGroupedReductionLanesReady(reduction_lanes)) {
		return false;
	}
	return SljitFusedGroupedAggregatePayloadsUseRuntimeInputAdapter(aggregate_update.payloads,
	                                                                aggregate_update.payload_descriptors);
}

enum class SljitFusedTypedPayloadSourceOverrideStatus : uint8_t { NONE, READY, INVALID };

static SljitFusedTypedPayloadSourceOverrideStatus
SljitGetFusedTypedPayloadSourceOverrideStatus(const SljitExecutableAggregateUpdate &aggregate_update,
                                              const DataChunk &payload_input,
                                              const vector<idx_t> &payload_source_indices) {
	if (!aggregate_update.fused_payload_update.Function()) {
		return SljitFusedTypedPayloadSourceOverrideStatus::NONE;
	}
	if (!SljitFusedAggregatePayloadsUseTypedExpressionTrees(aggregate_update.payloads,
	                                                        aggregate_update.payload_descriptors)) {
		return SljitFusedTypedPayloadSourceOverrideStatus::NONE;
	}
	vector<idx_t> fused_payload_sources;
	if (!SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregate_update.payload_descriptors,
	                                                 fused_payload_sources)) {
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

static bool
SljitCanPreaggregateInputVectorFusedPrimitivePayloads(SljitExecutableRegionOp &op, DataChunk &input,
                                                      const vector<idx_t> &payload_source_indices,
                                                      const vector<SljitGroupedReductionLaneBinding> &reduction_lanes) {
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (!aggregate_update.fused_payload_update.Function() || aggregate_update.fused_payload_update_owns_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || !aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != aggregate_update.payload_descriptors.size() ||
	    sink_info.aggregates.size() != reduction_lanes.size() || !SljitGroupedReductionLanesReady(reduction_lanes)) {
		return false;
	}
	auto fused_override_status =
	    SljitGetFusedTypedPayloadSourceOverrideStatus(aggregate_update, input, payload_source_indices);
	if (fused_override_status != SljitFusedTypedPayloadSourceOverrideStatus::READY) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		auto &descriptor = *reduction_lanes[payload_idx].descriptor;
		if (!SljitPreaggregatedPrimitivePayloadSupported(
		        descriptor.primitive_kind, descriptor.has_payload ? descriptor.input_type : PhysicalType::INVALID)) {
			return false;
		}
		if (!descriptor.has_payload && aggregate.child_count != 0) {
			return false;
		}
		if (descriptor.has_payload &&
		    aggregate_update.payloads[payload_idx].plan.return_type.InternalType() != descriptor.input_type) {
			return false;
		}
	}
	return true;
}

static bool SljitCanPreaggregateRowPointerGroupSources(DataChunk &payload_input,
                                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.empty()) {
		return false;
	}
	for (auto &source : group_sources) {
		switch (source.source_kind) {
		case ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD:
			if (!source.ready) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR:
			if (!SljitPreaggregationInputVectorGroupSourceSupported(payload_input, source)) {
				return false;
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool SljitCanExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    idx_t count, bool &uses_generated_payload_preaggregation) {
	uses_generated_payload_preaggregation = false;
	auto &aggregate_update = op.aggregate_update;
	auto &plan = aggregate_update.plan;
	auto &sink_info = plan.sink_info;
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || count != payload_input.size() || count < 2 ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR || !plan.UsesPrimitivePayloads() ||
	    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    sink_info.groups.size() != group_sources.size() ||
	    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != aggregate_update.payload_descriptors.size() ||
	    sink_info.aggregates.size() != reduction_lanes.size() || !SljitGroupedReductionLanesReady(reduction_lanes) ||
	    !SljitCanPreaggregateRowPointerGroupSources(payload_input, group_sources)) {
		return false;
	}
	auto fused_override_status =
	    SljitGetFusedTypedPayloadSourceOverrideStatus(aggregate_update, payload_input, payload_source_indices);
	if (fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::INVALID) {
		return false;
	}
	uses_generated_payload_preaggregation = fused_override_status == SljitFusedTypedPayloadSourceOverrideStatus::READY;
	const bool payload_sources_match_aggregates =
	    !uses_generated_payload_preaggregation && sink_info.aggregates.size() == payload_source_indices.size();
	if (!payload_sources_match_aggregates && !uses_generated_payload_preaggregation) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		auto &descriptor = *reduction_lanes[payload_idx].descriptor;
		if (!SljitPreaggregatedPrimitivePayloadSupported(
		        descriptor.primitive_kind, descriptor.has_payload ? descriptor.input_type : PhysicalType::INVALID)) {
			return false;
		}
		if (!descriptor.has_payload) {
			if (aggregate.child_count != 0 || (payload_sources_match_aggregates &&
			                                   payload_source_indices[payload_idx] != DConstants::INVALID_INDEX)) {
				return false;
			}
			continue;
		}
		if (payload_sources_match_aggregates) {
			const auto source_idx = payload_source_indices[payload_idx];
			if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT &&
			    source_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			if (source_idx >= payload_input.ColumnCount() ||
			    payload_input.data[source_idx].GetType().InternalType() != descriptor.input_type) {
				return false;
			}
		} else {
			if (op.aggregate_update.payloads[payload_idx].plan.return_type.InternalType() != descriptor.input_type) {
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
	       plan.UsesPrimitivePayloads() && plan.use_grouped_state_addresses && !plan.use_perfect_hash_group_lookup &&
	       !aggregate_update.fused_payload_update_owns_group_lookup &&
	       sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE && !sink_info.groups.empty();
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_aggregate_payload_lane_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool
SljitPerfectHashGroupExpressionsUseTypedTree(const vector<SljitNativeRegionExpressionPlan> &group_expressions) {
	for (auto &group_expression : group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	return false;
}

static void SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const vector<bool> &group_source_not_null, const ExecutionRegionAggregateContract &contract,
    const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    const ExecutionPerfectAggregateStateAddressLayout &layout, DataChunk &input, const SelectionVector *execute_sel,
    idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	if (!function) {
		throw InternalException("SLJIT fused perfect-hash aggregate update is missing generated code");
	}
	if (!layout.ready || !layout.data || !layout.group_is_set || layout.total_groups == 0 || layout.tuple_size == 0) {
		auto blocker = layout.blocker.empty() ? "perfect-hash-state-layout-missing" : layout.blocker;
		throw InternalException("SLJIT fused perfect-hash aggregate state layout is incomplete: %s", blocker.c_str());
	}
	if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < payload_descriptors.size()) {
		throw InternalException("SLJIT fused perfect-hash aggregate state contract is incomplete");
	}
	if (payload_descriptors.size() != payloads.size()) {
		throw InternalException("SLJIT fused perfect-hash aggregate primitive payload count mismatch");
	}
	if (contract.perfect_required_bits.size() != groups.size() ||
	    contract.perfect_group_minima.size() != groups.size()) {
		throw InternalException("SLJIT fused perfect-hash aggregate group contract is incomplete");
	}
	if (!group_expressions.empty() && group_expressions.size() != groups.size()) {
		throw InternalException("SLJIT fused perfect-hash aggregate group expression count mismatch");
	}
	if (group_source_not_null.size() != groups.size()) {
		throw InternalException("SLJIT fused perfect-hash aggregate group source fact count mismatch");
	}
	if (reduction_lanes.size() != payload_descriptors.size() || !SljitGroupedReductionLanesReady(reduction_lanes)) {
		throw InternalException("SLJIT fused perfect-hash aggregate primitive lane layout mismatch");
	}

	const bool typed_payloads = SljitFusedAggregatePayloadsUseTypedExpressionTrees(payloads, payload_descriptors) ||
	                            SljitPerfectHashGroupExpressionsUseTypedTree(group_expressions);
	optional_ptr<const vector<idx_t>> combined_sources;
	if (typed_payloads) {
		combined_sources = SljitRequireFusedTypedPayloadCombinedSourceIndices(
		    payloads, payload_descriptors, "SLJIT fused perfect-hash typed aggregate payload is missing sources",
		    "SLJIT fused perfect-hash typed aggregate payload sources are not normalized",
		    "SLJIT fused perfect-hash typed aggregate payload has no typed payloads");
		adapter_scratch.PreparePerfectHash(combined_sources->size(), groups.size());
	} else {
		adapter_scratch.PreparePerfectHash(payloads.size(), groups.size());
	}
	auto &group_sources = adapter_scratch.group_sources;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		if (!group.supported_reference) {
			throw InternalException("SLJIT fused perfect-hash aggregate group source is unsupported");
		}
		SljitNativeRegionExpressionPlan reference_group;
		reference_group.kind = SljitNativeRegionExpressionKind::REFERENCE;
		reference_group.return_type = group.type;
		reference_group.source_index = group.input_index;
		auto &group_expression = group_expressions.empty() ? reference_group : group_expressions[group_idx];
		if (group_expression.return_type.InternalType() != group.type.InternalType()) {
			throw InternalException("SLJIT fused perfect-hash aggregate group expression is unsupported");
		}
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			if (!typed_payloads || !group_expression.expression_tree) {
				throw InternalException("SLJIT fused perfect-hash typed aggregate group expression is unsupported");
			}
			continue;
		}
		if (group_expression.source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT fused perfect-hash aggregate group expression is unsupported");
		}
		auto &group_format =
		    group_sources.PrepareFormat(input, group_expression.source_index, group_idx,
		                                "SLJIT fused perfect-hash aggregate group expression is unsupported");
		if (group_expression.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			group_sources.SetData(group_idx,
			                      NativeIntegerSourceData(group_format, SljitPerfectHashGroupIntegerKind(group.type)));
		} else if (group_expression.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		           group_expression.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			group_sources.SetData(group_idx,
			                      NativeSignedIntegerSourceData(group_format, group_expression.cast_source_width));
		} else if (group_expression.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS &&
		           group.type.InternalType() == PhysicalType::UINT8 &&
		           group_expression.string_compress_target_size == sizeof(uint8_t)) {
			group_sources.SetData(group_idx, reinterpret_cast<const_data_ptr_t>(group_format.data));
		} else {
			throw InternalException("SLJIT fused perfect-hash aggregate group expression is unsupported");
		}
		group_sources.FinishSource(group_idx, execute_sel, count, group_source_not_null[group_idx]);
	}

	auto &payload_sources = adapter_scratch.payload_sources;
	if (typed_payloads) {
		SljitPrepareTypedAggregatePayloadSources(
		    input, *combined_sources, execute_sel, count, payload_sources,
		    "SLJIT fused perfect-hash typed aggregate source is out of range",
		    SljitGetFusedTypedPayloadCombinedSourceNotNull(payloads, payload_descriptors, combined_sources->size())
		        .get());
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = payload_descriptors[payload_idx];
		auto &lane = *reduction_lanes[payload_idx].runtime_lane;
		auto &plan = payloads[payload_idx].plan;
		if (SljitSkipCountStarPrimitivePayload(
		        descriptor, lane, "SLJIT fused perfect-hash count-star aggregate has unexpected payload")) {
			continue;
		}
		SljitRequireAggregatePayloadPrimitiveLane(
		    lane, "SLJIT fused perfect-hash aggregate primitive lane has unsupported state kind");
		if (typed_payloads) {
			SljitValidateTypedAggregatePayloadPlan(
			    plan, *combined_sources, lane.payload_type,
			    "SLJIT fused perfect-hash typed aggregate source is out of range",
			    "SLJIT fused perfect-hash typed aggregate payload is unsupported",
			    "SLJIT fused perfect-hash aggregate primitive payload type mismatch");
		} else {
			if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE || plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT fused perfect-hash aggregate payload source is unsupported");
			}
			if (plan.return_type.InternalType() != lane.payload_type) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive payload type mismatch");
			}
		}
		if (typed_payloads) {
			continue;
		}
		payload_sources.PrepareIntegerSource(
		    input, plan.source_index, payload_idx, plan.integer_kind, execute_sel, count,
		    "SLJIT fused perfect-hash aggregate payload source is unsupported",
		    SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null, 0));
	}

	const auto native_execute_sel =
	    execute_sel ? execute_sel->data() : payload_sources.CanonicalizeCommonSelection(group_sources);
	const auto source_common_sel =
	    typed_payloads && !native_execute_sel ? payload_sources.CanonicalizeCommonSourceSelection() : nullptr;
	const bool flat_no_selection = payload_sources.FlatNoSelection(native_execute_sel, source_common_sel) &&
	                               group_sources.FlatNoSelection(native_execute_sel, source_common_sel);
	const bool all_valid = payload_sources.AllValid() && group_sources.AllValid();
	const bool flat_all_valid = flat_no_selection && all_valid;
	const auto group_selection_all_present = group_sources.AllSelectionsPresent();

	SljitNativeVectorInput native_input;
	native_input.execute_sel = native_execute_sel;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array =
	    typed_payloads ? payload_sources.SelectionArray() : payload_sources.SelectionArrayOrNull();
	native_input.source_common_sel = source_common_sel;
	native_input.source_validity_array =
	    typed_payloads ? payload_sources.ValidityArray() : payload_sources.ValidityArrayOrNull();
	native_input.group_data_array = group_sources.DataArray();
	native_input.group_sel_array =
	    typed_payloads ? group_sources.SelectionArray() : group_sources.SelectionArrayOrNull();
	native_input.group_validity_array =
	    typed_payloads ? group_sources.ValidityArray() : group_sources.ValidityArrayOrNull();
	if (typed_payloads) {
		native_input.expression_tree_flat_no_selection = flat_no_selection;
		native_input.expression_tree_flat_all_valid = flat_all_valid;
		native_input.expression_tree_all_valid = all_valid;
		native_input.group_selection_all_present = group_selection_all_present;
	}
	native_input.perfect_hash_state_data = layout.data;
	native_input.perfect_hash_group_is_set = layout.group_is_set;
	native_input.perfect_hash_total_groups = layout.total_groups;
	native_input.perfect_hash_tuple_size = layout.tuple_size;
	native_input.perfect_hash_aggregate_state_offset = layout.aggregate_state_offset;
	native_input.error_message = "Perfect hash aggregate group exceeded total groups; source statistics may be corrupt";
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(function, native_input);
}

} // namespace duckdb

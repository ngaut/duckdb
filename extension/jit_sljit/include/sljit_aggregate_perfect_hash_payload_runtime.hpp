//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_lane_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

static bool SljitTryPreparePerfectHashStringDictionaryContributions(
    idx_t count, const SljitNativeRegionExpressionPlan &group_expression, Vector &group_source,
    const sel_t *group_selection, SljitPerfectHashDictionaryGroupCache &cache,
    SljitPerfectHashDictionaryGroupRuntime &runtime) {
	static constexpr idx_t MAX_DICTIONARY_SIZE = 20000;
	if (group_expression.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	    group_expression.string_compress_target_size != sizeof(uint8_t)) {
		return false;
	}
	if (group_source.GetVectorType() != VectorType::DICTIONARY_VECTOR ||
	    DictionaryVector::Child(group_source).GetVectorType() != VectorType::FLAT_VECTOR || !group_selection) {
		return false;
	}
	const auto dictionary_size = DictionaryVector::DictionarySize(group_source);
	const auto &dictionary_id = DictionaryVector::DictionaryId(group_source);
	if (!dictionary_size.IsValid() || dictionary_size.GetIndex() == 0 ||
	    dictionary_size.GetIndex() > MAX_DICTIONARY_SIZE) {
		return false;
	}
	const auto size = dictionary_size.GetIndex();
	const bool identity_changed = cache.dictionary_size != size || cache.dictionary_id != dictionary_id;
	if (identity_changed) {
		cache.contributions.assign(size, DConstants::INVALID_INDEX);
		cache.active_indices.resize(size);
		cache.dictionary_id = dictionary_id;
		cache.dictionary_size = size;
		cache.active_count = 0;
		cache.disabled = false;
	} else if (dictionary_id.empty()) {
		cache.disabled = cache.disabled || cache.active_count >= (count + 1) / 2;
		for (idx_t active_idx = 0; active_idx < cache.active_count; active_idx++) {
			cache.contributions[cache.active_indices[active_idx]] = DConstants::INVALID_INDEX;
		}
		cache.active_count = 0;
	}
	if (cache.disabled) {
		return false;
	}
	runtime.contributions = cache.contributions.data();
	runtime.active_indices = cache.active_indices.data();
	runtime.active_count = &cache.active_count;
	return true;
}

static void SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const vector<bool> &group_source_not_null, const ExecutionRegionAggregateContract &contract,
    const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
    SljitAggregatePayloadSourceLayout payload_source_layout, const vector<idx_t> &combined_payload_sources,
    const vector<bool> &combined_payload_source_not_null,
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

	const bool uses_combined_payload_sources =
	    payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED;
	if (uses_combined_payload_sources) {
		if (combined_payload_source_not_null.size() != combined_payload_sources.size()) {
			throw InternalException("SLJIT fused perfect-hash aggregate payload source facts are not normalized");
		}
		adapter_scratch.PreparePerfectHash(combined_payload_sources.size(), groups.size());
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
			if (!uses_combined_payload_sources || !group_expression.expression_tree) {
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
		SljitTryPreparePerfectHashStringDictionaryContributions(
		    count, group_expression, input.data[group_expression.source_index],
		    group_sources.SelectionArray()[group_idx], adapter_scratch.perfect_hash_dictionary_group_caches[group_idx],
		    adapter_scratch.perfect_hash_dictionary_groups[group_idx]);
	}

	auto &payload_sources = adapter_scratch.payload_sources;
	if (uses_combined_payload_sources) {
		SljitPrepareFusedAggregatePayloadSources(input, combined_payload_sources, execute_sel, count, payload_sources,
		                                         "SLJIT fused perfect-hash aggregate source is out of range",
		                                         &combined_payload_source_not_null);
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
		if (uses_combined_payload_sources) {
			SljitValidateFusedAggregatePayloadPlan(
			    plan, combined_payload_sources, lane.payload_type,
			    "SLJIT fused perfect-hash aggregate source is out of range",
			    "SLJIT fused perfect-hash aggregate payload is unsupported",
			    "SLJIT fused perfect-hash aggregate primitive payload type mismatch");
		} else {
			if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE || plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT fused perfect-hash aggregate payload source is unsupported");
			}
			if (plan.return_type.InternalType() != lane.payload_type) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive payload type mismatch");
			}
		}
		if (uses_combined_payload_sources) {
			continue;
		}
		payload_sources.PrepareIntegerSource(
		    input, plan.source_index, payload_idx, plan.integer_kind, execute_sel, count,
		    "SLJIT fused perfect-hash aggregate payload source is unsupported",
		    SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null, 0));
	}

	bool dictionary_group_contributions_ready = !execute_sel && !groups.empty();
	for (auto &dictionary_group : adapter_scratch.perfect_hash_dictionary_groups) {
		dictionary_group_contributions_ready = dictionary_group_contributions_ready && dictionary_group.contributions;
	}
	const auto native_execute_sel = execute_sel ? execute_sel->data()
	                                            : (dictionary_group_contributions_ready
	                                                   ? nullptr
	                                                   : payload_sources.CanonicalizeCommonSelection(group_sources));
	const auto source_common_sel = uses_combined_payload_sources && !native_execute_sel
	                                   ? payload_sources.CanonicalizeCommonSourceSelection()
	                                   : nullptr;
	// Payload expressions and group keys can expose different physical vector shapes. In particular, numeric payload
	// columns can be flat while compressed string group keys retain dictionary selections. Keep those facts
	// independent: expression lowering only consumes payload facts, group lookup only consumes group facts, and legacy
	// loops consume their explicit conjunction.
	const bool payload_flat_no_selection = payload_sources.FlatNoSelection(native_execute_sel, source_common_sel);
	const bool payload_all_valid = payload_sources.AllValid();
	const bool payload_flat_all_valid = payload_flat_no_selection && payload_all_valid;
	const bool group_flat_no_selection = group_sources.FlatNoSelection(native_execute_sel);
	const bool group_all_valid = group_sources.AllValid();
	const bool group_flat_all_valid = group_flat_no_selection && group_all_valid;
	const auto group_selection_all_present = group_sources.AllSelectionsPresent();

	SljitNativeVectorInput native_input;
	native_input.execute_sel = native_execute_sel;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array =
	    uses_combined_payload_sources ? payload_sources.SelectionArray() : payload_sources.SelectionArrayOrNull();
	native_input.source_common_sel = source_common_sel;
	native_input.source_validity_array =
	    uses_combined_payload_sources ? payload_sources.ValidityArray() : payload_sources.ValidityArrayOrNull();
	native_input.group_data_array = group_sources.DataArray();
	native_input.perfect_hash_dictionary_groups =
	    dictionary_group_contributions_ready ? adapter_scratch.perfect_hash_dictionary_groups.data() : nullptr;
	native_input.group_sel_array =
	    uses_combined_payload_sources ? group_sources.SelectionArray() : group_sources.SelectionArrayOrNull();
	native_input.group_validity_array =
	    uses_combined_payload_sources ? group_sources.ValidityArray() : group_sources.ValidityArrayOrNull();
	if (uses_combined_payload_sources) {
		native_input.expression_tree_flat_no_selection = payload_flat_no_selection;
		native_input.expression_tree_flat_all_valid = payload_flat_all_valid;
		native_input.expression_tree_all_valid = payload_all_valid;
		native_input.perfect_hash_group_flat_all_valid = group_flat_all_valid;
		native_input.perfect_hash_group_all_valid = group_all_valid;
		native_input.perfect_hash_inputs_flat_no_selection = payload_flat_no_selection && group_flat_no_selection;
		native_input.perfect_hash_inputs_all_valid = payload_all_valid && group_all_valid;
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

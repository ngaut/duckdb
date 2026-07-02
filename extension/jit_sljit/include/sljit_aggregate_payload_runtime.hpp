//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_filtered_payload_runtime.hpp"
#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_aggregate_perfect_hash_payload_runtime.hpp"
#include "sljit_aggregate_primitive_payload_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static void SljitExecuteFusedTypedExpressionAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
    const SelectionVector *execute_sel, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	auto &combined_sources = SljitRequireFusedTypedPayloadCombinedSourceIndices(
	    payloads, aggregates, "SLJIT fused typed aggregate payload is missing combined sources",
	    "SLJIT fused typed aggregate payload sources are not normalized",
	    "SLJIT fused typed aggregate payload has no typed payloads");

	adapter_scratch.PrepareFiltered(combined_sources.size(), aggregates.size());
	auto &payload_sources = adapter_scratch.payload_sources;
	auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
	auto &aggregate_hugeint_values = adapter_scratch.aggregate_hugeint_values;
	auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
	auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;

	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto lane = lanes[payload_idx];
		if (!lane) {
			throw InternalException("SLJIT fused typed aggregate primitive lane missing for aggregate %llu",
			                        static_cast<unsigned long long>(aggregates[payload_idx].aggregate_index));
		}
		auto &aggregate = aggregates[payload_idx];
		if (lane->kind != aggregate.primitive_update_kind) {
			throw InternalException("SLJIT fused typed aggregate primitive lane kind mismatch");
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused typed aggregate count-star lane is incomplete: %s",
				                        blocker.c_str());
			}
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			aggregate_row_counts[payload_idx] = lane->row_count;
			continue;
		}
		if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
		    lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			throw InternalException("SLJIT fused typed aggregate primitive lane has unsupported state kind");
		}
		if (payloads[payload_idx].plan.return_type.InternalType() != lane->payload_type) {
			throw InternalException("SLJIT fused typed aggregate primitive payload type mismatch");
		}
		if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (payloads[payload_idx].plan.source_index >= combined_sources.size()) {
				throw InternalException("SLJIT fused typed aggregate reference source is out of range");
			}
		} else if (payloads[payload_idx].plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		           !payloads[payload_idx].plan.expression_tree) {
			throw InternalException("SLJIT fused typed aggregate payload is unsupported");
		}
		const auto has_sum_state = (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 && lane->sum_int64_value) ||
		                           (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && lane->sum_hugeint_value);
		if (!lane->ready || !has_sum_state || !lane->state_is_set || !lane->row_count) {
			auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
			throw InternalException("SLJIT fused typed aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
		} else {
			aggregate_hugeint_values[payload_idx] = lane->sum_hugeint_value;
		}
		aggregate_state_is_sets[payload_idx] = lane->state_is_set;
		aggregate_row_counts[payload_idx] = lane->row_count;
	}

	for (idx_t source_idx = 0; source_idx < combined_sources.size(); source_idx++) {
		auto input_index = combined_sources[source_idx];
		payload_sources.PrepareTypedExpressionSource(
		    input, input_index, source_idx, execute_sel, count,
		    "SLJIT fused typed aggregate expression-tree source is out of range");
	}

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array = payload_sources.SelectionArray();
	native_input.source_validity_array = payload_sources.ValidityArray();
	native_input.expression_tree_flat_no_selection = payload_sources.FlatNoSelection(execute_sel);
	native_input.expression_tree_flat_all_valid = payload_sources.FlatAllValid(execute_sel);
	native_input.expression_tree_all_valid = payload_sources.AllValid();
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

static void SljitExecuteFusedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
    const SelectionVector *execute_sel, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	if (!function) {
		throw InternalException("SLJIT fused aggregate primitive payload update is missing generated code");
	}
	if (aggregates.size() != payloads.size()) {
		throw InternalException("SLJIT fused aggregate primitive payload count mismatch");
	}
	if (SljitFusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates)) {
		SljitExecuteFusedTypedExpressionAggregatePayloadUpdate(payloads, function, aggregates, lanes, input,
		                                                       execute_sel, count, adapter_scratch);
		return;
	}

	adapter_scratch.PrepareUngrouped(payloads.size());
	auto &payload_sources = adapter_scratch.payload_sources;
	auto &right_payload_sources = adapter_scratch.right_payload_sources;
	auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
	auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
	auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
	auto &constants = adapter_scratch.constants;

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto lane = lanes[payload_idx];
		if (!lane) {
			throw InternalException("SLJIT fused aggregate primitive lane missing for aggregate %llu",
			                        static_cast<unsigned long long>(aggregates[payload_idx].aggregate_index));
		}
		auto &plan = payloads[payload_idx].plan;
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused aggregate count-star lane is incomplete: %s", blocker.c_str());
			}
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			aggregate_row_counts[payload_idx] = lane->row_count;
			continue;
		}
		if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
			throw InternalException("SLJIT fused aggregate primitive lane has unsupported state kind");
		}
		if (plan.return_type.InternalType() != lane->payload_type) {
			throw InternalException("SLJIT fused aggregate primitive payload type mismatch");
		}
		if (!lane->ready || !lane->sum_int64_value || !lane->state_is_set || !lane->row_count) {
			auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
			throw InternalException("SLJIT fused aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
		aggregate_int64_values[payload_idx] = lane->sum_int64_value;
		aggregate_state_is_sets[payload_idx] = lane->state_is_set;
		aggregate_row_counts[payload_idx] = lane->row_count;

		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate reference source is out of range");
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate binary source is out of range");
			constants[payload_idx] = plan.constant;
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate binary source is out of range");
			right_payload_sources.PrepareIntegerSource(input, plan.right_source_index, payload_idx, plan.integer_kind,
			                                           execute_sel, count,
			                                           "SLJIT fused aggregate binary source is out of range");
			break;
		default:
			throw InternalException("SLJIT fused aggregate primitive payload has no runtime input adapter");
		}
	}

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.right_source_data_array = right_payload_sources.DataArray();
	native_input.source_sel_array = payload_sources.SelectionArrayOrNull();
	native_input.right_source_sel_array = right_payload_sources.SelectionArrayOrNull();
	native_input.source_validity_array = payload_sources.ValidityArrayOrNull();
	native_input.right_source_validity_array = right_payload_sources.ValidityArrayOrNull();
	native_input.constants = constants.data();
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

static void SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates, const ExecutionRegionAggregateContract &contract,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
    const uintptr_t *grouped_state_addresses, const sel_t *grouped_state_address_sel,
    const SelectionVector *execute_sel, bool state_addresses_by_loop_index, idx_t count,
    SljitAggregatePayloadAdapterScratch &adapter_scratch,
    optional_ptr<const vector<idx_t>> input_source_indices_override = nullptr) {
	if (!function) {
		throw InternalException("SLJIT fused grouped aggregate primitive payload update is missing generated code");
	}
	if (!grouped_state_addresses) {
		throw InternalException("SLJIT fused grouped aggregate primitive payload update is missing state addresses");
	}
	if (aggregates.size() != payloads.size()) {
		throw InternalException("SLJIT fused grouped aggregate primitive payload count mismatch");
	}
	if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < aggregates.size()) {
		throw InternalException("SLJIT fused grouped aggregate state layout is incomplete");
	}

	if (SljitFusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates)) {
		optional_ptr<const vector<idx_t>> combined_sources;
		combined_sources = SljitRequireFusedTypedPayloadCombinedSourceIndices(
		    payloads, aggregates, "SLJIT fused grouped typed aggregate payload is missing sources",
		    "SLJIT fused grouped typed aggregate payload sources are not normalized",
		    "SLJIT fused grouped typed aggregate payload has no typed payloads");
		if (input_source_indices_override) {
			if (input_source_indices_override->size() != combined_sources->size()) {
				throw InternalException("SLJIT fused grouped typed aggregate payload source override size mismatch");
			}
			combined_sources = input_source_indices_override;
		}

		adapter_scratch.PrepareGrouped(combined_sources->size());
		auto &payload_sources = adapter_scratch.payload_sources;

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT fused grouped typed aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregate.aggregate_index));
			}
			if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
				throw InternalException("SLJIT fused grouped typed aggregate state offset is out of range");
			}
			if (!lane->ready || lane->state_size == 0) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused grouped typed aggregate primitive lane is incomplete: %s",
				                        blocker.c_str());
			}
			if (lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
			    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
			    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
				throw InternalException("SLJIT fused grouped typed aggregate primitive lane layout mismatch");
			}
			auto &plan = payloads[payload_idx].plan;
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (aggregate.child_count != 0) {
					throw InternalException("SLJIT fused grouped typed count-star aggregate has unexpected payload");
				}
				continue;
			}
			if (!AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
				throw InternalException(
				    "SLJIT fused grouped typed aggregate primitive lane has unsupported state kind");
			}
			if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				if (plan.source_index >= combined_sources->size()) {
					throw InternalException("SLJIT fused grouped typed aggregate reference source is out of range");
				}
			} else if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree) {
				throw InternalException("SLJIT fused grouped typed aggregate payload is unsupported");
			}
			if (plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT fused grouped typed aggregate primitive payload type mismatch");
			}
		}

		for (idx_t source_idx = 0; source_idx < combined_sources->size(); source_idx++) {
			auto input_index = (*combined_sources)[source_idx];
			payload_sources.PrepareTypedExpressionSource(input, input_index, source_idx, execute_sel, count,
			                                             "SLJIT fused grouped typed aggregate source is out of range");
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_data_array = payload_sources.DataArray();
		native_input.source_sel_array = payload_sources.SelectionArray();
		native_input.source_validity_array = payload_sources.ValidityArray();
		native_input.expression_tree_flat_no_selection = payload_sources.FlatNoSelection(execute_sel);
		native_input.expression_tree_flat_all_valid = payload_sources.FlatAllValid(execute_sel);
		native_input.expression_tree_all_valid = payload_sources.AllValid();
		native_input.aggregate_state_addresses = grouped_state_addresses;
		native_input.aggregate_state_address_sel = grouped_state_address_sel;
		native_input.aggregate_state_addresses_by_loop_index = state_addresses_by_loop_index;
		native_input.count = count;
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return;
	}

	adapter_scratch.PrepareGrouped(payloads.size());
	auto &payload_sources = adapter_scratch.payload_sources;
	if (input_source_indices_override && input_source_indices_override->size() != payloads.size()) {
		throw InternalException("SLJIT fused grouped aggregate payload source override size mismatch");
	}

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto lane = lanes[payload_idx];
		if (!lane) {
			throw InternalException("SLJIT fused grouped aggregate primitive lane missing for aggregate %llu",
			                        static_cast<unsigned long long>(aggregate.aggregate_index));
		}
		if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
			throw InternalException("SLJIT fused grouped aggregate state offset is out of range");
		}
		if (!lane->ready || lane->state_size == 0) {
			auto blocker = lane->blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane->blocker;
			throw InternalException("SLJIT fused grouped aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
		if (lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
			throw InternalException("SLJIT fused grouped aggregate primitive lane layout mismatch");
		}
		auto &plan = payloads[payload_idx].plan;
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				throw InternalException("SLJIT fused grouped count-star aggregate has unexpected payload");
			}
			continue;
		}
		if (!AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
			throw InternalException("SLJIT fused grouped aggregate primitive lane has unsupported state kind");
		}
		if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			throw InternalException("SLJIT fused grouped aggregate payload has unsupported expression kind");
		}
		if (plan.return_type.InternalType() != lane->payload_type) {
			throw InternalException("SLJIT fused grouped aggregate primitive payload type mismatch");
		}
		const auto source_index =
		    input_source_indices_override ? (*input_source_indices_override)[payload_idx] : plan.source_index;
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT) {
			payload_sources.PrepareValiditySource(input, source_index, payload_idx, execute_sel, count,
			                                      "SLJIT fused grouped aggregate reference source is out of range");
		} else {
			payload_sources.PrepareIntegerSource(input, source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count,
			                                     "SLJIT fused grouped aggregate reference source is out of range");
		}
	}

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array = payload_sources.SelectionArrayOrNull();
	native_input.source_validity_array = payload_sources.ValidityArrayOrNull();
	native_input.aggregate_state_addresses = grouped_state_addresses;
	native_input.aggregate_state_address_sel = grouped_state_address_sel;
	native_input.aggregate_state_addresses_by_loop_index = state_addresses_by_loop_index;
	native_input.count = count;
	native_input.has_error = false;
	function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

} // namespace duckdb

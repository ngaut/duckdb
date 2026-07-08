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
#include "sljit_aggregate_payload_lane_runtime.hpp"
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
		auto &lane = SljitRequireAggregatePrimitiveLane(
		    lanes, aggregates, payload_idx, "SLJIT fused typed aggregate primitive lane missing for aggregate %llu");
		auto &aggregate = aggregates[payload_idx];
		if (lane.kind != aggregate.primitive_update_kind) {
			throw InternalException("SLJIT fused typed aggregate primitive lane kind mismatch");
		}
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitBindUngroupedCountStarPrimitiveLane(lane, aggregate_int64_values, aggregate_row_counts, payload_idx,
			                                         "SLJIT fused typed aggregate count-star lane is incomplete: %s");
			continue;
		}
		if (lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
		    lane.kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			throw InternalException("SLJIT fused typed aggregate primitive lane has unsupported state kind");
		}
		SljitValidateTypedAggregatePayloadPlan(payloads[payload_idx].plan, combined_sources, lane.payload_type,
		                                       "SLJIT fused typed aggregate reference source is out of range",
		                                       "SLJIT fused typed aggregate payload is unsupported",
		                                       "SLJIT fused typed aggregate primitive payload type mismatch");
		SljitBindUngroupedSumPrimitiveLane(lane, aggregate_int64_values, aggregate_hugeint_values,
		                                   aggregate_state_is_sets, aggregate_row_counts, payload_idx,
		                                   "SLJIT fused typed aggregate primitive lane is incomplete: %s");
	}

	SljitPrepareTypedAggregatePayloadSources(input, combined_sources, execute_sel, count, payload_sources,
	                                         "SLJIT fused typed aggregate expression-tree source is out of range",
	                                         SljitGetFusedTypedPayloadCombinedSourceNotNull(payloads, aggregates,
	                                                                                        combined_sources.size())
	                                             .get());

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	SljitBindTypedAggregatePayloadSources(native_input, payload_sources, execute_sel);
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(function, native_input);
}

enum class SljitSingleFusedPrimitiveAggregatePayloadKind : uint8_t {
	INVALID,
	REFERENCE,
	INTEGER_BINARY_CONSTANT,
	INTEGER_BINARY_REFERENCES
};

struct SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate {
	bool ready = false;
	SljitSingleFusedPrimitiveAggregatePayloadKind kind = SljitSingleFusedPrimitiveAggregatePayloadKind::INVALID;
	SljitNativeAggregateUpdateFunction function = nullptr;
	const ExecutionPrimitiveAggregateUpdateLane *lane = nullptr;
	idx_t source_index = DConstants::INVALID_INDEX;
	idx_t right_source_index = DConstants::INVALID_INDEX;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	int64_t constant = 0;
	bool source_known_not_null = false;
	bool right_source_known_not_null = false;
};

static bool SljitBindSingleFusedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate &bound) {
	bound = SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate();
	if (!function || payloads.size() != 1 || aggregates.size() != 1) {
		return false;
	}
	auto &payload = payloads[0];
	auto &plan = payload.plan;
	auto &lane = SljitRequireAggregatePrimitiveLane(
	    lanes, aggregates, 0, "SLJIT single fused aggregate primitive lane missing for aggregate %llu");
	if (lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
		return false;
	}
	if (plan.return_type.InternalType() != lane.payload_type) {
		throw InternalException("SLJIT single fused aggregate primitive payload type mismatch");
	}
	SljitValidateUngroupedPrimitiveLaneState(lane, "SLJIT single fused aggregate primitive lane is incomplete: %s",
	                                         "aggregate-primitive-lane-incomplete");

	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		bound.kind = SljitSingleFusedPrimitiveAggregatePayloadKind::REFERENCE;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		bound.kind = SljitSingleFusedPrimitiveAggregatePayloadKind::INTEGER_BINARY_CONSTANT;
		bound.constant = plan.constant;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		bound.kind = SljitSingleFusedPrimitiveAggregatePayloadKind::INTEGER_BINARY_REFERENCES;
		bound.right_source_index = plan.right_source_index;
		bound.right_source_known_not_null = SljitInputSourceKnownNotNull(payload.input_source_not_null, 1);
		break;
	default:
		return false;
	}

	bound.ready = true;
	bound.function = function;
	bound.lane = &lane;
	bound.source_index = plan.source_index;
	bound.integer_kind = plan.integer_kind;
	bound.source_known_not_null = SljitInputSourceKnownNotNull(payload.input_source_not_null, 0);
	return true;
}

static bool SljitExecuteBoundSingleFusedPrimitiveAggregatePayloadUpdate(
    const SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate &bound, DataChunk &input,
    const SelectionVector *execute_sel, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	if (!bound.ready) {
		return false;
	}
	if (!bound.function || !bound.lane) {
		throw InternalException("SLJIT bound single fused aggregate payload update is incomplete");
	}
	auto &lane = *bound.lane;
	adapter_scratch.payload_sources.Resize(1);
	adapter_scratch.right_payload_sources.Resize(1);
	auto &payload_sources = adapter_scratch.payload_sources;
	auto &right_payload_sources = adapter_scratch.right_payload_sources;

	payload_sources.PrepareIntegerSource(input, bound.source_index, 0, bound.integer_kind, execute_sel, count,
	                                     "SLJIT bound single fused aggregate source is out of range",
	                                     bound.source_known_not_null);
	if (bound.kind == SljitSingleFusedPrimitiveAggregatePayloadKind::INTEGER_BINARY_REFERENCES) {
		right_payload_sources.PrepareIntegerSource(input, bound.right_source_index, 0, bound.integer_kind, execute_sel,
		                                           count,
		                                           "SLJIT bound single fused aggregate binary source is out of range",
		                                           bound.right_source_known_not_null);
	}

	SljitNativeVectorInput native_input;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_data = payload_sources.DataArray()[0];
	native_input.source_sel = payload_sources.SelectionArray()[0];
	native_input.source_validity = payload_sources.ValidityArray()[0];
	native_input.right_source_data = right_payload_sources.DataArray()[0];
	native_input.right_source_sel = right_payload_sources.SelectionArray()[0];
	native_input.right_source_validity = right_payload_sources.ValidityArray()[0];
	native_input.constant = bound.constant;
	native_input.aggregate_int64_value = lane.sum_int64_value;
	native_input.aggregate_state_is_set = lane.state_is_set;
	native_input.aggregate_row_count = lane.row_count;
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(bound.function, native_input);
	return true;
}

static bool SljitTryExecuteSingleFusedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
    const SelectionVector *execute_sel, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate bound;
	if (!SljitBindSingleFusedPrimitiveAggregatePayloadUpdate(payloads, function, aggregates, lanes, bound)) {
		return false;
	}
	return SljitExecuteBoundSingleFusedPrimitiveAggregatePayloadUpdate(bound, input, execute_sel, count,
	                                                                   adapter_scratch);
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
	if (SljitTryExecuteSingleFusedPrimitiveAggregatePayloadUpdate(payloads, function, aggregates, lanes, input,
	                                                              execute_sel, count, adapter_scratch)) {
		return;
	}

	adapter_scratch.PrepareUngrouped(payloads.size());
	auto &payload_sources = adapter_scratch.payload_sources;
	auto &right_payload_sources = adapter_scratch.right_payload_sources;
	auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
	auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
	auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
	auto &constants = adapter_scratch.constants;
	const_data_ptr_t single_source_data = nullptr;
	const_data_ptr_t single_right_source_data = nullptr;
	const sel_t *single_source_sel = nullptr;
	const sel_t *single_right_source_sel = nullptr;
	const validity_t *single_source_validity = nullptr;
	const validity_t *single_right_source_validity = nullptr;
	int64_t single_constant = 0;
	int64_t *single_aggregate_int64_value = nullptr;
	bool *single_aggregate_state_is_set = nullptr;
	idx_t *single_aggregate_row_count = nullptr;

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &lane = SljitRequireAggregatePrimitiveLane(
		    lanes, aggregates, payload_idx, "SLJIT fused aggregate primitive lane missing for aggregate %llu");
		auto &plan = payloads[payload_idx].plan;
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitBindUngroupedCountStarPrimitiveLane(lane, aggregate_int64_values, aggregate_row_counts, payload_idx,
			                                         "SLJIT fused aggregate count-star lane is incomplete: %s");
			continue;
		}
		if (plan.return_type.InternalType() != lane.payload_type) {
			throw InternalException("SLJIT fused aggregate primitive payload type mismatch");
		}
		SljitBindUngroupedInt64SumPrimitiveLane(
		    lane, aggregate_int64_values, aggregate_state_is_sets, aggregate_row_counts, payload_idx,
		    "SLJIT fused aggregate primitive lane has unsupported state kind",
		    "SLJIT fused aggregate primitive lane is incomplete: %s");
		if (payloads.size() == 1) {
			single_aggregate_int64_value = lane.sum_int64_value;
			single_aggregate_state_is_set = lane.state_is_set;
			single_aggregate_row_count = lane.row_count;
		}

		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate reference source is out of range",
			                                     SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null,
			                                                                  0));
			if (payloads.size() == 1) {
				single_source_data = payload_sources.DataArray()[0];
				single_source_sel = payload_sources.SelectionArray()[0];
				single_source_validity = payload_sources.ValidityArray()[0];
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate binary source is out of range",
			                                     SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null,
			                                                                  0));
			constants[payload_idx] = plan.constant;
			if (payloads.size() == 1) {
				single_source_data = payload_sources.DataArray()[0];
				single_source_sel = payload_sources.SelectionArray()[0];
				single_source_validity = payload_sources.ValidityArray()[0];
				single_constant = plan.constant;
			}
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			payload_sources.PrepareIntegerSource(input, plan.source_index, payload_idx, plan.integer_kind, execute_sel,
			                                     count, "SLJIT fused aggregate binary source is out of range",
			                                     SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null,
			                                                                  0));
			right_payload_sources.PrepareIntegerSource(input, plan.right_source_index, payload_idx, plan.integer_kind,
			                                           execute_sel, count,
			                                           "SLJIT fused aggregate binary source is out of range",
			                                           SljitInputSourceKnownNotNull(
			                                               payloads[payload_idx].input_source_not_null, 1));
			if (payloads.size() == 1) {
				single_source_data = payload_sources.DataArray()[0];
				single_source_sel = payload_sources.SelectionArray()[0];
				single_source_validity = payload_sources.ValidityArray()[0];
				single_right_source_data = right_payload_sources.DataArray()[0];
				single_right_source_sel = right_payload_sources.SelectionArray()[0];
				single_right_source_validity = right_payload_sources.ValidityArray()[0];
			}
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
	if (payloads.size() == 1) {
		native_input.source_data = single_source_data;
		native_input.source_sel = single_source_sel;
		native_input.source_validity = single_source_validity;
		native_input.right_source_data = single_right_source_data;
		native_input.right_source_sel = single_right_source_sel;
		native_input.right_source_validity = single_right_source_validity;
		native_input.constant = single_constant;
		native_input.aggregate_int64_value = single_aggregate_int64_value;
		native_input.aggregate_state_is_set = single_aggregate_state_is_set;
		native_input.aggregate_row_count = single_aggregate_row_count;
	}
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(function, native_input);
}

static void SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
    const vector<ExecutionRegionAggregateInput> &aggregates, const ExecutionRegionAggregateContract &contract,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
    const uintptr_t *grouped_state_addresses, const sel_t *grouped_state_address_sel,
    const SelectionVector *execute_sel, bool state_addresses_by_loop_index, idx_t count,
    SljitAggregatePayloadAdapterScratch &adapter_scratch,
    optional_ptr<const vector<idx_t>> input_source_indices_override = nullptr,
    optional_ptr<const vector<bool>> input_source_not_null_override = nullptr) {
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
			if (input_source_not_null_override &&
			    input_source_not_null_override->size() != input_source_indices_override->size()) {
				throw InternalException(
				    "SLJIT fused grouped typed aggregate payload source fact override size mismatch");
			}
			combined_sources = input_source_indices_override;
		}

		adapter_scratch.PrepareGrouped(combined_sources->size());
		auto &payload_sources = adapter_scratch.payload_sources;

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto &lane = SljitRequireAggregatePrimitiveLane(
			    lanes, aggregates, payload_idx,
			    "SLJIT fused grouped typed aggregate primitive lane missing for aggregate %llu");
			SljitValidateGroupedPrimitiveLaneLayout(
			    aggregate, contract, lane, "SLJIT fused grouped typed aggregate state offset is out of range",
			    "SLJIT fused grouped typed aggregate primitive lane is incomplete: %s",
			    "SLJIT fused grouped typed aggregate primitive lane layout mismatch");
			auto &plan = payloads[payload_idx].plan;
			if (SljitSkipCountStarPrimitivePayload(
			        aggregate, lane, "SLJIT fused grouped typed count-star aggregate has unexpected payload")) {
				continue;
			}
			SljitRequireAggregatePayloadPrimitiveLane(
			    lane, "SLJIT fused grouped typed aggregate primitive lane has unsupported state kind");
			SljitValidateTypedAggregatePayloadPlan(
			    plan, *combined_sources, lane.payload_type,
			    "SLJIT fused grouped typed aggregate reference source is out of range",
			    "SLJIT fused grouped typed aggregate payload is unsupported",
			    "SLJIT fused grouped typed aggregate primitive payload type mismatch");
		}

		const auto combined_source_not_null =
		    input_source_indices_override
		        ? input_source_not_null_override
		        : SljitGetFusedTypedPayloadCombinedSourceNotNull(payloads, aggregates, combined_sources->size());
		SljitPrepareTypedAggregatePayloadSources(input, *combined_sources, execute_sel, count, payload_sources,
		                                         "SLJIT fused grouped typed aggregate source is out of range",
		                                         combined_source_not_null.get());

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		SljitBindTypedAggregatePayloadSources(native_input, payload_sources, execute_sel);
		native_input.aggregate_state_addresses = grouped_state_addresses;
		native_input.aggregate_state_address_sel = grouped_state_address_sel;
		native_input.aggregate_state_addresses_by_loop_index = state_addresses_by_loop_index;
		native_input.count = count;
		native_input.has_error = false;
		SljitExecuteNativeAggregatePayloadFunction(function, native_input);
		return;
	}

	adapter_scratch.PrepareGrouped(payloads.size());
	auto &payload_sources = adapter_scratch.payload_sources;
	if (input_source_indices_override && input_source_indices_override->size() != payloads.size()) {
		throw InternalException("SLJIT fused grouped aggregate payload source override size mismatch");
	}
	if (input_source_not_null_override &&
	    (!input_source_indices_override ||
	     input_source_not_null_override->size() != input_source_indices_override->size())) {
		throw InternalException("SLJIT fused grouped aggregate payload source fact override size mismatch");
	}

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &lane = SljitRequireAggregatePrimitiveLane(
		    lanes, aggregates, payload_idx, "SLJIT fused grouped aggregate primitive lane missing for aggregate %llu");
		SljitValidateGroupedPrimitiveLaneLayout(
		    aggregate, contract, lane, "SLJIT fused grouped aggregate state offset is out of range",
		    "SLJIT fused grouped aggregate primitive lane is incomplete: %s",
		    "SLJIT fused grouped aggregate primitive lane layout mismatch");
		auto &plan = payloads[payload_idx].plan;
		if (SljitSkipCountStarPrimitivePayload(aggregate, lane,
		                                       "SLJIT fused grouped count-star aggregate has unexpected payload")) {
			continue;
		}
		SljitRequireAggregatePayloadPrimitiveLane(
		    lane, "SLJIT fused grouped aggregate primitive lane has unsupported state kind");
		SljitValidateReferenceAggregatePayloadPlan(plan, lane.payload_type,
		                                           "SLJIT fused grouped aggregate payload has unsupported expression kind",
		                                           "SLJIT fused grouped aggregate primitive payload type mismatch");
		const auto source_index =
		    input_source_indices_override ? (*input_source_indices_override)[payload_idx] : plan.source_index;
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT) {
			const auto known_not_null =
			    input_source_indices_override
			        ? input_source_not_null_override &&
			              SljitInputSourceKnownNotNull(*input_source_not_null_override, payload_idx)
			        : SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null, 0);
			payload_sources.PrepareValiditySource(input, source_index, payload_idx, execute_sel, count,
			                                      "SLJIT fused grouped aggregate reference source is out of range",
			                                      known_not_null);
		} else {
			const auto known_not_null =
			    input_source_indices_override
			        ? input_source_not_null_override &&
			              SljitInputSourceKnownNotNull(*input_source_not_null_override, payload_idx)
			        : SljitInputSourceKnownNotNull(payloads[payload_idx].input_source_not_null, 0);
			if (lane.kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				payload_sources.PrepareTypedExpressionSource(
				    input, source_index, payload_idx, execute_sel, count,
				    "SLJIT fused grouped aggregate reference source is out of range", known_not_null);
			} else {
				payload_sources.PrepareIntegerSource(
				    input, source_index, payload_idx, plan.integer_kind, execute_sel, count,
				    "SLJIT fused grouped aggregate reference source is out of range", known_not_null);
			}
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
	SljitExecuteNativeAggregatePayloadFunction(function, native_input);
}

} // namespace duckdb

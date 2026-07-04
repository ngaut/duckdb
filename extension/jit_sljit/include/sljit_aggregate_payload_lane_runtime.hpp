//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_payload_lane_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static const ExecutionPrimitiveAggregateUpdateLane &SljitRequireAggregatePrimitiveLane(
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
    const vector<ExecutionRegionAggregateInput> &aggregates, idx_t payload_idx, const char *message) {
	auto lane = lanes[payload_idx];
	if (!lane) {
		throw InternalException(message, static_cast<unsigned long long>(aggregates[payload_idx].aggregate_index));
	}
	return *lane;
}

static void SljitThrowIncompletePrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane, const char *message,
                                              const char *default_blocker) {
	auto blocker = lane.blocker.empty() ? string(default_blocker) : lane.blocker;
	throw InternalException(message, blocker.c_str());
}

static void SljitValidateUngroupedCountStarPrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                         const char *message) {
	if (!lane.ready || !lane.sum_int64_value || !lane.row_count) {
		SljitThrowIncompletePrimitiveLane(lane, message, "aggregate-count-star-lane-incomplete");
	}
}

static void SljitBindUngroupedCountStarPrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                     vector<int64_t *> &aggregate_int64_values,
                                                     vector<idx_t *> &aggregate_row_counts, idx_t payload_idx,
                                                     const char *message) {
	SljitValidateUngroupedCountStarPrimitiveLane(lane, message);
	aggregate_int64_values[payload_idx] = lane.sum_int64_value;
	aggregate_row_counts[payload_idx] = lane.row_count;
}

static void SljitBindUngroupedSumPrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                               vector<int64_t *> &aggregate_int64_values,
                                               vector<hugeint_t *> &aggregate_hugeint_values,
                                               vector<bool *> &aggregate_state_is_sets,
                                               vector<idx_t *> &aggregate_row_counts, idx_t payload_idx,
                                               const char *message) {
	const auto has_sum_state = (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64 && lane.sum_int64_value) ||
	                           (lane.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && lane.sum_hugeint_value);
	if (!lane.ready || !has_sum_state || !lane.state_is_set || !lane.row_count) {
		SljitThrowIncompletePrimitiveLane(lane, message, "aggregate-primitive-lane-incomplete");
	}
	if (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
		aggregate_int64_values[payload_idx] = lane.sum_int64_value;
	} else {
		aggregate_hugeint_values[payload_idx] = lane.sum_hugeint_value;
	}
	aggregate_state_is_sets[payload_idx] = lane.state_is_set;
	aggregate_row_counts[payload_idx] = lane.row_count;
}

static void SljitBindUngroupedInt64SumPrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                    vector<int64_t *> &aggregate_int64_values,
                                                    vector<bool *> &aggregate_state_is_sets,
                                                    vector<idx_t *> &aggregate_row_counts, idx_t payload_idx,
                                                    const char *unsupported_message, const char *incomplete_message) {
	if (lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
		throw InternalException(unsupported_message);
	}
	if (!lane.ready || !lane.sum_int64_value || !lane.state_is_set || !lane.row_count) {
		SljitThrowIncompletePrimitiveLane(lane, incomplete_message, "aggregate-primitive-lane-incomplete");
	}
	aggregate_int64_values[payload_idx] = lane.sum_int64_value;
	aggregate_state_is_sets[payload_idx] = lane.state_is_set;
	aggregate_row_counts[payload_idx] = lane.row_count;
}

static void SljitValidateGroupedPrimitiveLaneState(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                   const char *message, const char *default_blocker) {
	if (!lane.ready || lane.state_size == 0) {
		SljitThrowIncompletePrimitiveLane(lane, message, default_blocker);
	}
}

static void SljitValidateGroupedPrimitiveLaneLayout(const ExecutionRegionAggregateInput &aggregate,
                                                    const ExecutionRegionAggregateContract &contract,
                                                    const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                    const char *offset_message, const char *incomplete_message,
                                                    const char *layout_mismatch_message) {
	if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
		throw InternalException(offset_message);
	}
	SljitValidateGroupedPrimitiveLaneState(lane, incomplete_message, "aggregate-primitive-grouped-lane-incomplete");
	if (lane.state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
	    lane.state_value_offset != aggregate.primitive_update_state_value_offset ||
	    lane.state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
		throw InternalException(layout_mismatch_message);
	}
}

static bool SljitSkipCountStarPrimitivePayload(const ExecutionRegionAggregateInput &aggregate,
                                               const ExecutionPrimitiveAggregateUpdateLane &lane,
                                               const char *unexpected_payload_message) {
	if (lane.kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return false;
	}
	if (aggregate.child_count != 0) {
		throw InternalException(unexpected_payload_message);
	}
	return true;
}

static void SljitRequireAggregatePayloadPrimitiveLane(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                      const char *unsupported_message) {
	if (!AggregatePrimitiveUpdateRequiresPayload(lane.kind)) {
		throw InternalException(unsupported_message);
	}
}

static void SljitValidateTypedAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &plan,
                                                   const vector<idx_t> &source_indices, PhysicalType payload_type,
                                                   const char *out_of_range_message,
                                                   const char *unsupported_payload_message,
                                                   const char *type_mismatch_message) {
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (plan.source_index >= source_indices.size()) {
			throw InternalException(out_of_range_message);
		}
	} else if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree) {
		throw InternalException(unsupported_payload_message);
	}
	if (plan.return_type.InternalType() != payload_type) {
		throw InternalException(type_mismatch_message);
	}
}

static void SljitValidateReferenceAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &plan,
                                                       PhysicalType payload_type,
                                                       const char *unsupported_payload_message,
                                                       const char *type_mismatch_message) {
	if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
		throw InternalException(unsupported_payload_message);
	}
	if (plan.return_type.InternalType() != payload_type) {
		throw InternalException(type_mismatch_message);
	}
}

static void SljitValidateUngroupedPrimitiveLaneState(const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                     const char *message, const char *default_blocker) {
	const auto has_sum_state = (AggregatePrimitiveUpdateUsesInt64State(lane.kind) && lane.sum_int64_value) ||
	                           (AggregatePrimitiveUpdateUsesHugeintState(lane.kind) && lane.sum_hugeint_value) ||
	                           (AggregatePrimitiveUpdateUsesDoubleState(lane.kind) && lane.sum_double_value);
	const auto needs_state_is_set = AggregatePrimitiveUpdateHasStateIsSet(lane.kind);
	if (!lane.ready || !has_sum_state || (needs_state_is_set && !lane.state_is_set) || !lane.row_count) {
		SljitThrowIncompletePrimitiveLane(lane, message, default_blocker);
	}
}

static void SljitPrepareTypedAggregatePayloadSources(DataChunk &input, const vector<idx_t> &source_indices,
                                                     const SelectionVector *execute_sel, idx_t count,
                                                     SljitSourceVectorScratch &payload_sources,
                                                     const char *out_of_range_message,
                                                     const vector<bool> *source_not_null = nullptr) {
	for (idx_t source_idx = 0; source_idx < source_indices.size(); source_idx++) {
		const auto known_not_null = source_not_null && SljitInputSourceKnownNotNull(*source_not_null, source_idx);
		payload_sources.PrepareTypedExpressionSource(input, source_indices[source_idx], source_idx, execute_sel, count,
		                                             out_of_range_message, known_not_null);
	}
}

static void SljitBindTypedAggregatePayloadSources(SljitNativeVectorInput &native_input,
                                                  SljitSourceVectorScratch &payload_sources,
                                                  const SelectionVector *execute_sel) {
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array = payload_sources.SelectionArray();
	native_input.source_validity_array = payload_sources.ValidityArray();
	native_input.expression_tree_flat_no_selection = payload_sources.FlatNoSelection(execute_sel);
	native_input.expression_tree_flat_all_valid = payload_sources.FlatAllValid(execute_sel);
	native_input.expression_tree_all_valid = payload_sources.AllValid();
}

static void SljitExecuteNativeAggregatePayloadFunction(SljitNativeAggregateUpdateFunction function,
                                                       SljitNativeVectorInput &native_input) {
	SljitExecuteNativeFunction(function, native_input);
}

} // namespace duckdb

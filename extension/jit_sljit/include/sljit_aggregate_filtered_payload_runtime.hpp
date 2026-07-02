//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_filtered_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static void SljitExecuteFilteredPrimitiveAggregateUpdate(
    SljitExecutableFilteredAggregateUpdate &filtered_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input, idx_t count,
    SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	if (!filtered_update.function) {
		throw InternalException("SLJIT filtered aggregate primitive payload update is missing generated code");
	}
	if (aggregates.size() != filtered_update.payloads.size() || aggregates.size() != lanes.size()) {
		throw InternalException("SLJIT filtered aggregate primitive payload count mismatch");
	}

	adapter_scratch.PrepareFiltered(filtered_update.input_source_indices.size(), aggregates.size());
	auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
	auto &aggregate_hugeint_values = adapter_scratch.aggregate_hugeint_values;
	auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
	auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto lane = lanes[payload_idx];
		if (!lane) {
			throw InternalException("SLJIT filtered aggregate primitive lane is missing");
		}
		auto &aggregate = aggregates[payload_idx];
		if (lane->kind != aggregate.primitive_update_kind) {
			throw InternalException("SLJIT filtered aggregate primitive lane kind mismatch");
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT filtered aggregate count-star lane is incomplete: %s", blocker.c_str());
			}
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			aggregate_row_counts[payload_idx] = lane->row_count;
			continue;
		}
		if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
		    lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			throw InternalException("SLJIT filtered aggregate primitive lane has unsupported state kind");
		}
		if (filtered_update.payloads[payload_idx].plan.return_type.InternalType() != lane->payload_type) {
			throw InternalException("SLJIT filtered aggregate primitive payload type mismatch");
		}
		auto has_sum_state = (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 && lane->sum_int64_value) ||
		                     (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && lane->sum_hugeint_value);
		if (!lane->ready || !has_sum_state || !lane->state_is_set || !lane->row_count) {
			auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
			throw InternalException("SLJIT filtered aggregate primitive lane is incomplete: %s", blocker.c_str());
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
		} else {
			aggregate_hugeint_values[payload_idx] = lane->sum_hugeint_value;
		}
		aggregate_state_is_sets[payload_idx] = lane->state_is_set;
		aggregate_row_counts[payload_idx] = lane->row_count;
	}

	auto &payload_sources = adapter_scratch.payload_sources;
	for (idx_t source_idx = 0; source_idx < filtered_update.input_source_indices.size(); source_idx++) {
		auto input_index = filtered_update.input_source_indices[source_idx];
		payload_sources.PrepareTypedExpressionSource(input, input_index, source_idx, nullptr, count,
		                                             "SLJIT filtered aggregate expression-tree source is out of range");
	}

	SljitNativeVectorInput native_input;
	native_input.source_data_array = payload_sources.DataArray();
	native_input.source_sel_array = payload_sources.SelectionArray();
	native_input.source_validity_array = payload_sources.ValidityArray();
	native_input.expression_tree_flat_no_selection =
	    payload_sources.FlatNoSelection(static_cast<const SelectionVector *>(nullptr));
	native_input.expression_tree_flat_all_valid =
	    payload_sources.FlatAllValid(static_cast<const SelectionVector *>(nullptr));
	native_input.expression_tree_all_valid = payload_sources.AllValid();
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	filtered_update.function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
}

} // namespace duckdb

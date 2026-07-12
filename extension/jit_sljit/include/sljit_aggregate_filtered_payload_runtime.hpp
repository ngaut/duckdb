//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_filtered_payload_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_lane_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static void
SljitExecuteFilteredPrimitiveAggregateUpdate(SljitExecutableFilteredAggregateUpdate &filtered_update,
                                             const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                             const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
                                             DataChunk &input, idx_t count,
                                             SljitAggregatePayloadAdapterScratch &adapter_scratch) {
	if (!filtered_update.compiled.Function()) {
		throw InternalException("SLJIT filtered aggregate primitive payload update is missing generated code");
	}
	if (payload_descriptors.size() != filtered_update.payloads.size() || payload_descriptors.size() != lanes.size()) {
		throw InternalException("SLJIT filtered aggregate primitive payload count mismatch");
	}

	adapter_scratch.PrepareFiltered(filtered_update.input_source_indices.size(), payload_descriptors.size());
	auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
	auto &aggregate_hugeint_values = adapter_scratch.aggregate_hugeint_values;
	auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
	auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
	for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
		auto &lane =
		    SljitRequireAggregatePayloadLane(lanes, payload_descriptors, payload_idx,
		                                     "SLJIT filtered aggregate primitive lane invalid for aggregate %llu");
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			SljitBindUngroupedCountStarPrimitiveLane(lane, aggregate_int64_values, aggregate_row_counts, payload_idx,
			                                         "SLJIT filtered aggregate count-star lane is incomplete: %s");
			continue;
		}
		if (lane.kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
		    lane.kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			throw InternalException("SLJIT filtered aggregate primitive lane has unsupported state kind");
		}
		if (filtered_update.payloads[payload_idx].plan.return_type.InternalType() != lane.payload_type) {
			throw InternalException("SLJIT filtered aggregate primitive payload type mismatch");
		}
		SljitBindUngroupedSumPrimitiveLane(lane, aggregate_int64_values, aggregate_hugeint_values,
		                                   aggregate_state_is_sets, aggregate_row_counts, payload_idx,
		                                   "SLJIT filtered aggregate primitive lane is incomplete: %s");
	}

	auto &payload_sources = adapter_scratch.payload_sources;
	SljitPrepareTypedAggregatePayloadSources(
	    input, filtered_update.input_source_indices, nullptr, count, payload_sources,
	    "SLJIT filtered aggregate expression-tree source is out of range", &filtered_update.input_source_not_null);

	SljitNativeVectorInput native_input;
	SljitBindTypedAggregatePayloadSources(native_input, payload_sources, nullptr);
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(filtered_update.compiled.Function(), native_input);
}

} // namespace duckdb

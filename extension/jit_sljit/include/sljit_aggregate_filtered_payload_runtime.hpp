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

//! The matches of a string comparison for each entry of the dictionary its source is in this batch, when that
//! dictionary is no larger than the batch and is the data the kernel indexes; nullptr otherwise
static const uint8_t *SljitStringCompareDictionaryMatches(DataChunk &input, idx_t column_index,
                                                          const_data_ptr_t source_data, const string &constant,
                                                          idx_t count, vector<uint8_t> &matches) {
	auto &source = input.data[column_index];
	if (source.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	auto dictionary_size = DictionaryVector::DictionarySize(source);
	auto &dictionary = DictionaryVector::Child(source);
	if (!dictionary_size.IsValid() || dictionary_size.GetIndex() > count ||
	    dictionary.GetVectorType() != VectorType::FLAT_VECTOR ||
	    const_data_ptr_cast(FlatVector::GetData<string_t>(dictionary)) != source_data) {
		return nullptr;
	}
	auto entries = FlatVector::GetData<string_t>(dictionary);
	const string_t constant_value(constant);
	matches.resize(dictionary_size.GetIndex());
	for (idx_t entry_idx = 0; entry_idx < matches.size(); entry_idx++) {
		// a NULL entry's match is never read: a row with a NULL takes the kernel's generic path
		matches[entry_idx] = string_t::StringComparisonOperators::Equals(entries[entry_idx], constant_value);
	}
	return matches.data();
}

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
	SljitPrepareFusedAggregatePayloadSources(
	    input, filtered_update.input_source_indices, nullptr, count, payload_sources,
	    "SLJIT filtered aggregate expression-tree source is out of range", &filtered_update.input_source_not_null);

	SljitNativeVectorInput native_input;
	SljitBindFusedAggregatePayloadSources(native_input, payload_sources, nullptr);
	if (!filtered_update.string_compares.empty()) {
		auto &storage = adapter_scratch.string_match_storage;
		auto &matches = adapter_scratch.string_matches;
		storage.resize(filtered_update.string_compares.size());
		matches.resize(filtered_update.string_compares.size());
		for (idx_t compare_idx = 0; compare_idx < matches.size(); compare_idx++) {
			auto &compare = filtered_update.string_compares[compare_idx];
			matches[compare_idx] = SljitStringCompareDictionaryMatches(
			    input, filtered_update.input_source_indices[compare.source],
			    payload_sources.DataArray()[compare.source], compare.constant, count, storage[compare_idx]);
		}
		native_input.string_matches = matches.data();
	}
	native_input.aggregate_int64_values = aggregate_int64_values.data();
	native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
	native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
	native_input.aggregate_row_counts = aggregate_row_counts.data();
	native_input.count = count;
	native_input.has_error = false;
	SljitExecuteNativeAggregatePayloadFunction(filtered_update.compiled.Function(), native_input);
}

} // namespace duckdb

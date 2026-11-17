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

//! Whether a batch compares a string source once per entry of its dictionary: the source is a dictionary vector whose
//! dictionary is flat and no larger than the batch
static bool SljitStringCompareHasDictionary(const Vector &source, idx_t count) {
	if (source.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return false;
	}
	auto dictionary_size = DictionaryVector::DictionarySize(source);
	return dictionary_size.IsValid() && dictionary_size.GetIndex() <= count &&
	       DictionaryVector::Child(source).GetVectorType() == VectorType::FLAT_VECTOR;
}

//! A string comparison's result for each entry of its source's dictionary in this batch, FALSE for a NULL entry, when
//! the batch compares that source per entry and its dictionary is the data the kernel indexes; nullptr otherwise
static const uint8_t *SljitStringCompareDictionaryMatches(const Vector &source, const_data_ptr_t source_data,
                                                          const SljitStringCompareMatches &compare, idx_t count,
                                                          vector<uint8_t> &matches) {
	if (!SljitStringCompareHasDictionary(source, count)) {
		return nullptr;
	}
	auto &dictionary = DictionaryVector::Child(source);
	auto entries = FlatVector::GetData<string_t>(dictionary);
	if (const_data_ptr_cast(entries) != source_data) {
		return nullptr;
	}
	auto &validity = FlatVector::Validity(dictionary);
	const string_t constant(compare.constant);
	matches.resize(DictionaryVector::DictionarySize(source).GetIndex());
	for (idx_t entry_idx = 0; entry_idx < matches.size(); entry_idx++) {
		matches[entry_idx] =
		    validity.RowIsValid(entry_idx) &&
		    string_t::StringComparisonOperators::Equals(entries[entry_idx], constant) == compare.compare_equal;
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

	// A source the filter reads only through string comparisons that a batch computes per dictionary entry needs no
	// look at its rows' validity: its NULL entries compare FALSE, which decides the filter as NULL would. It is
	// prepared as valid, so its NULLs leave the batch on the fast path. Its validity is restored for the rows other
	// sources' NULLs send to the generic path, unless the kernel runs those rows on its fast loops too.
	auto &source_indices = filtered_update.input_source_indices;
	auto &compared_valid = adapter_scratch.string_compared_valid_sources;
	compared_valid = filtered_update.input_source_not_null;
	compared_valid.resize(source_indices.size(), false);
	for (idx_t source_idx = 0; source_idx < filtered_update.string_compare_only_sources.size(); source_idx++) {
		compared_valid[source_idx] = compared_valid[source_idx] ||
		                             (filtered_update.string_compare_only_sources[source_idx] &&
		                              SljitStringCompareHasDictionary(input.data[source_indices[source_idx]], count));
	}
	auto &payload_sources = adapter_scratch.payload_sources;
	SljitPrepareFusedAggregatePayloadSources(input, source_indices, nullptr, count, payload_sources,
	                                         "SLJIT filtered aggregate expression-tree source is out of range",
	                                         &compared_valid);

	SljitNativeVectorInput native_input;
	SljitBindFusedAggregatePayloadSources(native_input, payload_sources, nullptr);
	if (!filtered_update.string_compares.empty()) {
		auto &storage = adapter_scratch.string_match_storage;
		auto &matches = adapter_scratch.string_matches;
		storage.resize(filtered_update.string_compares.size());
		matches.resize(filtered_update.string_compares.size());
		for (idx_t compare_idx = 0; compare_idx < matches.size(); compare_idx++) {
			auto &compare = filtered_update.string_compares[compare_idx];
			auto &source = input.data[source_indices[compare.source]];
			matches[compare_idx] = SljitStringCompareDictionaryMatches(
			    source, payload_sources.DataArray()[compare.source], compare, count, storage[compare_idx]);
			if (filtered_update.fast_null_rows ||
			    compare.source >= filtered_update.string_compare_only_sources.size() ||
			    !filtered_update.string_compare_only_sources[compare.source] ||
			    !SljitStringCompareHasDictionary(source, count)) {
				continue;
			}
			if (!matches[compare_idx]) {
				throw InternalException("SLJIT filtered aggregate compared a dictionary source without its results");
			}
			auto &validity = FlatVector::Validity(DictionaryVector::Child(source));
			payload_sources.SetValidity(compare.source, validity.CannotHaveNull() ? nullptr : validity.GetData());
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

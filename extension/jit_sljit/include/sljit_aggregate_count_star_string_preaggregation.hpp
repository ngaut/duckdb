//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_count_star_string_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_count_star_projection_input.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <array>

namespace duckdb {

struct SljitCountStarStringGroupSignature {
	idx_t size = 0;
	uint32_t prefix = 0;
};

static SljitCountStarStringGroupSignature SljitCountStarStringGroupSignatureOf(const string_t &key) {
	SljitCountStarStringGroupSignature result;
	result.size = key.GetSize();
	result.prefix = key.GetPrefixIntegerComparable();
	return result;
}

static bool SljitCountStarStringGroupSignatureMatches(const SljitCountStarStringGroupSignature &left,
                                                      const SljitCountStarStringGroupSignature &right) {
	return left.size == right.size && left.prefix == right.prefix;
}

template <class T>
static bool AccumulatePreaggregatedStringCompressedCountStarKey(
    const string_t &key, std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &lookup_keys,
    std::array<SljitCountStarStringGroupSignature, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &lookup_signatures,
    std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &compressed_keys,
    std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts, idx_t &group_count) {
	const auto signature = SljitCountStarStringGroupSignatureOf(key);
	idx_t group_idx = 0;
	for (; group_idx < group_count; group_idx++) {
		if (SljitCountStarStringGroupSignatureMatches(lookup_signatures[group_idx], signature) &&
		    lookup_keys[group_idx] == key) {
			break;
		}
	}
	if (group_idx == group_count) {
		if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
			return false;
		}
		T compressed_key;
		if (!TryStringCompressValue(key, compressed_key)) {
			return false;
		}
		lookup_keys[group_count] = key;
		lookup_signatures[group_count] = signature;
		compressed_keys[group_count] = compressed_key;
		counts[group_count] = 0;
		group_count++;
	}
	counts[group_idx]++;
	return true;
}

template <class T>
static bool TryPreaggregateStringCompressedCountStarGroupsTemplated(Vector &source, const SelectionVector *execute_sel,
                                                                    idx_t count, DataChunk &compact_groups,
                                                                    vector<int64_t> &count_deltas) {
	if (source.GetVectorType() == VectorType::FLAT_VECTOR) {
		auto &validity = FlatVector::Validity(source);
		if (validity.CannotHaveNull() || (!execute_sel && validity.CheckAllValid(count))) {
			auto source_data = FlatVector::GetData<string_t>(source);
			std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> lookup_keys;
			std::array<SljitCountStarStringGroupSignature, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> lookup_signatures;
			std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> compressed_keys;
			std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
			idx_t group_count = 0;
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				const auto source_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
				if (!AccumulatePreaggregatedStringCompressedCountStarKey(source_data[source_idx], lookup_keys,
				                                                         lookup_signatures, compressed_keys, counts,
				                                                         group_count)) {
					return false;
				}
			}
			MaterializePreaggregatedCountStarGroups(compressed_keys, counts, group_count, compact_groups, count_deltas);
			return true;
		}
	}

	UnifiedVectorFormat format;
	source.ToUnifiedFormat(format);
	auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
	auto source_sel = format.sel;
	auto &source_validity = format.validity;
	const bool can_have_null = source_validity.CanHaveNull();
	std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> lookup_keys;
	std::array<SljitCountStarStringGroupSignature, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> lookup_signatures;
	std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> compressed_keys;
	std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
	idx_t group_count = 0;

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
		const auto source_idx = source_sel->get_index(input_idx);
		if (can_have_null && !source_validity.RowIsValid(source_idx)) {
			return false;
		}
		if (!AccumulatePreaggregatedStringCompressedCountStarKey(
		        source_data[source_idx], lookup_keys, lookup_signatures, compressed_keys, counts, group_count)) {
			return false;
		}
	}

	MaterializePreaggregatedCountStarGroups(compressed_keys, counts, group_count, compact_groups, count_deltas);
	return true;
}

static bool TryGetStringCompressedCountStarProjectionSource(const SljitExecutableRegionOp &projection_op,
                                                            DataChunk &input, DataChunk &compact_groups, idx_t count,
                                                            idx_t &source_index, PhysicalType &compressed_type) {
	SljitStringCompressedCountStarProjectionInput projection_input;
	if (count == 0 || !SljitTryReadStringCompressedCountStarProjectionInput(projection_op, projection_input) ||
	    !SljitStringCompressedCountStarProjectionInputMatchesChunk(projection_input, input) ||
	    compact_groups.ColumnCount() != 1 || compact_groups.data[0].GetType() != projection_op.output_types[0]) {
		return false;
	}
	source_index = projection_input.source_index;
	compressed_type = projection_input.compressed_type;
	return true;
}

static bool TryPreaggregateProjectedCountStarGroups(const SljitExecutableRegionOp &projection_op, DataChunk &input,
                                                    const SelectionVector *execute_sel, idx_t count,
                                                    DataChunk &compact_groups, vector<int64_t> &count_deltas) {
	idx_t source_index;
	PhysicalType compressed_type;
	if (!TryGetStringCompressedCountStarProjectionSource(projection_op, input, compact_groups, count, source_index,
	                                                     compressed_type)) {
		return false;
	}
	switch (compressed_type) {
	case PhysicalType::UINT8:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint8_t>(input.data[source_index], execute_sel,
		                                                                        count, compact_groups, count_deltas);
	case PhysicalType::UINT16:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint16_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT32:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint32_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT64:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint64_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT128:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uhugeint_t>(
		    input.data[source_index], execute_sel, count, compact_groups, count_deltas);
	default:
		return false;
	}
}

} // namespace duckdb

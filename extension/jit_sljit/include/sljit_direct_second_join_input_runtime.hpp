//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_second_join_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

struct SljitDirectSecondJoinInputProjection {
	idx_t source_input_idx = DConstants::INVALID_INDEX;
	idx_t string_payload_idx = DConstants::INVALID_INDEX;
	idx_t date_payload_idx = DConstants::INVALID_INDEX;
	idx_t first_int64_payload_idx = DConstants::INVALID_INDEX;
	idx_t second_int64_payload_idx = DConstants::INVALID_INDEX;
};

static bool SljitTryDirectBuildSecondJoinInput(const SljitDirectSecondJoinInputProjection &descriptor,
                                               SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
                                               SelectionVector &first_match_selection, Vector &first_row_pointers,
                                               idx_t count, DataChunk &second_join_input, idx_t target_offset = 0) {
	if (descriptor.source_input_idx >= source_chunk.ColumnCount() || second_join_input.ColumnCount() != 5 ||
	    !scratch.HasOperatorBinding(1) || target_offset + count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	auto &layout = scratch.OperatorBinding(1).hash_join_probe.table_layout;
	const auto max_payload_idx =
	    MaxValue(MaxValue(descriptor.string_payload_idx, descriptor.date_payload_idx),
	             MaxValue(descriptor.first_int64_payload_idx, descriptor.second_int64_payload_idx));
	if (!layout.ready || max_payload_idx >= layout.layout_offsets.size()) {
		return false;
	}
	const auto date_payload_offset = layout.layout_offsets[descriptor.date_payload_idx];
	const auto first_int64_payload_offset = layout.layout_offsets[descriptor.first_int64_payload_idx];
	const auto second_int64_payload_offset = layout.layout_offsets[descriptor.second_int64_payload_idx];
	const auto string_payload_offset = layout.layout_offsets[descriptor.string_payload_idx];

	UnifiedVectorFormat source_format;
	source_chunk.data[descriptor.source_input_idx].ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<int64_t>(source_format);
	auto source_sel = source_format.sel;
	auto &source_validity = source_format.validity;
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(first_row_pointers);

	const auto result_count = target_offset + count;
	for (auto &column : second_join_input.data) {
		column.SetVectorType(VectorType::FLAT_VECTOR);
		auto &validity = FlatVector::ValidityMutable(column);
		validity.Reset(result_count);
		validity.SetAllValid(result_count);
		FlatVector::SetSize(column, count_t(result_count));
	}
	auto out_source = FlatVector::GetDataMutable<int64_t>(second_join_input.data[0]);
	auto out_string_payload = FlatVector::GetDataMutable<string_t>(second_join_input.data[1]);
	auto out_date_payload = FlatVector::GetDataMutable<int32_t>(second_join_input.data[2]);
	auto out_first_int64_payload = FlatVector::GetDataMutable<int64_t>(second_join_input.data[3]);
	auto out_second_int64_payload = FlatVector::GetDataMutable<int64_t>(second_join_input.data[4]);

	for (idx_t local_idx = 0; local_idx < count; local_idx++) {
		const auto out_idx = target_offset + local_idx;
		const auto probe_idx = first_match_selection.get_index(local_idx);
		const auto source_idx = source_sel->get_index(probe_idx);
		if (source_validity.RowIsValid(source_idx) == false) {
			return false;
		}
		auto row_location = row_pointer_data[local_idx];
		if (!row_location) {
			return false;
		}
		out_source[out_idx] = source_data[source_idx];
		out_date_payload[out_idx] = Load<int32_t>(row_location + date_payload_offset);
		out_first_int64_payload[out_idx] = Load<int64_t>(row_location + first_int64_payload_offset);
		out_second_int64_payload[out_idx] = Load<int64_t>(row_location + second_int64_payload_offset);
		if (!SljitTryDecodeInlineCompressedString16Value(Load<uhugeint_t>(row_location + string_payload_offset),
		                                                 out_string_payload[out_idx])) {
			return false;
		}
	}
	second_join_input.SetChildCardinality(result_count);
	return true;
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_runtime_batch_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

enum class SljitRuntimeBatchOwnership : uint8_t { MATERIALIZED_CHUNK, SELECTED_REFERENCE };

struct SljitRuntimeBatchView {
	DataChunk *chunk = nullptr;
	const SelectionVector *selection = nullptr;
	const SelectionVector *hash_join_build_selection = nullptr;
	Vector *hash_join_row_pointers = nullptr;
	const vector<idx_t> *hash_join_output_column_map = nullptr;
	idx_t count = 0;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t hash_join_output_projection_idx = DConstants::INVALID_INDEX;
	SljitRuntimeBatchOwnership ownership = SljitRuntimeBatchOwnership::MATERIALIZED_CHUNK;
	bool source_key0_int64_to_int32_matches_are_proven = false;

	bool HasChunk() const {
		return chunk != nullptr;
	}

	bool IsMaterializedChunk() const {
		return ownership == SljitRuntimeBatchOwnership::MATERIALIZED_CHUNK && selection == nullptr;
	}

	bool HasHashJoinSelection() const {
		return hash_join_idx != DConstants::INVALID_INDEX && selection && hash_join_build_selection &&
		       hash_join_row_pointers;
	}

	DataChunk &Chunk() const {
		if (!chunk) {
			throw InternalException("SLJIT runtime batch view has no chunk");
		}
		return *chunk;
	}
};

static SljitRuntimeBatchView SljitRuntimeBatchViewFromChunk(DataChunk &chunk) {
	SljitRuntimeBatchView view;
	view.chunk = &chunk;
	view.count = chunk.size();
	return view;
}

static SljitRuntimeBatchView SljitRuntimeBatchViewFromChunk(DataChunk &chunk, const SelectionVector *selection,
                                                            idx_t count) {
	SljitRuntimeBatchView view;
	view.chunk = &chunk;
	view.selection = selection;
	view.count = count;
	view.ownership =
	    selection ? SljitRuntimeBatchOwnership::SELECTED_REFERENCE : SljitRuntimeBatchOwnership::MATERIALIZED_CHUNK;
	return view;
}

static SljitRuntimeBatchView SljitRuntimeBatchViewFromHashJoinSelection(
    DataChunk &join_input, const SelectionVector &match_selection, const SelectionVector &build_selection,
    Vector &row_pointers, idx_t selected_count, idx_t hash_join_idx, bool source_key0_int64_to_int32_matches_are_proven,
    const vector<idx_t> *hash_join_output_column_map = nullptr,
    idx_t hash_join_output_projection_idx = DConstants::INVALID_INDEX) {
	SljitRuntimeBatchView view;
	view.chunk = &join_input;
	view.selection = &match_selection;
	view.hash_join_build_selection = &build_selection;
	view.hash_join_row_pointers = &row_pointers;
	view.hash_join_output_column_map = hash_join_output_column_map;
	view.count = selected_count;
	view.hash_join_idx = hash_join_idx;
	view.hash_join_output_projection_idx = hash_join_output_projection_idx;
	view.ownership = SljitRuntimeBatchOwnership::SELECTED_REFERENCE;
	view.source_key0_int64_to_int32_matches_are_proven = source_key0_int64_to_int32_matches_are_proven;
	return view;
}

} // namespace duckdb

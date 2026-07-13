//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_runtime_batch_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"

namespace duckdb {

enum class SljitRuntimeBatchOwnership : uint8_t { MATERIALIZED_CHUNK, SELECTED_REFERENCE };

struct SljitRuntimeHashJoinSelection {
	DataChunk *chunk = nullptr;
	const SelectionVector *match_selection = nullptr;
	const SelectionVector *build_selection = nullptr;
	Vector *row_pointers = nullptr;
	const vector<idx_t> *output_column_map = nullptr;
	idx_t count = 0;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t output_projection_idx = DConstants::INVALID_INDEX;
	ExecutionHashJoinProbeOutputProof output_proof;

	DataChunk &Input() const {
		if (!chunk) {
			throw InternalException("SLJIT runtime hash-join selection has no input chunk");
		}
		return *chunk;
	}

	const SelectionVector &MatchSelection() const {
		if (!match_selection) {
			throw InternalException("SLJIT runtime hash-join selection has no match selection");
		}
		return *match_selection;
	}

	const SelectionVector &BuildSelection() const {
		if (!build_selection) {
			throw InternalException("SLJIT runtime hash-join selection has no build selection");
		}
		return *build_selection;
	}

	Vector &RowPointers() const {
		if (!row_pointers) {
			throw InternalException("SLJIT runtime hash-join selection has no row pointers");
		}
		return *row_pointers;
	}

	optional_ptr<const vector<idx_t>> OutputColumnMap() const {
		return optional_ptr<const vector<idx_t>>(output_column_map);
	}

	bool SourceKey0Int64ToInt32MatchesAreProven() const {
		return output_proof.source_key0_int64_to_int32;
	}

	bool ExactSourceFilterMatches() const {
		return output_proof.ExactSourceFilterMatches();
	}
};

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
	ExecutionHashJoinProbeOutputProof hash_join_output_proof;

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

	bool TryGetHashJoinSelection(SljitRuntimeHashJoinSelection &selected) const {
		if (!HasHashJoinSelection()) {
			return false;
		}
		selected.chunk = chunk;
		selected.match_selection = selection;
		selected.build_selection = hash_join_build_selection;
		selected.row_pointers = hash_join_row_pointers;
		selected.output_column_map = hash_join_output_column_map;
		selected.count = count;
		selected.hash_join_idx = hash_join_idx;
		selected.output_projection_idx = hash_join_output_projection_idx;
		selected.output_proof = hash_join_output_proof;
		return true;
	}

	SljitRuntimeHashJoinSelection BindHashJoinSelection(const char *consumer_name) const {
		SljitRuntimeHashJoinSelection selected;
		if (!TryGetHashJoinSelection(selected)) {
			throw InternalException("%s requires a selected hash-join batch view", consumer_name);
		}
		return selected;
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

static const SelectionVector &SljitBindHashJoinMatchSelection(const ExecutionHashJoinProbeOutputProof &output_proof,
                                                              const SelectionVector &explicit_match_selection) {
	return output_proof.match_selection_is_identity ? *FlatVector::IncrementalSelectionVector()
	                                                : explicit_match_selection;
}

static SljitRuntimeBatchView SljitRuntimeBatchViewFromHashJoinSelection(
    DataChunk &join_input, const SelectionVector &match_selection, const SelectionVector &build_selection,
    Vector &row_pointers, idx_t selected_count, idx_t hash_join_idx,
    const ExecutionHashJoinProbeOutputProof &output_proof, const vector<idx_t> *hash_join_output_column_map = nullptr,
    idx_t hash_join_output_projection_idx = DConstants::INVALID_INDEX) {
	SljitRuntimeBatchView view;
	view.chunk = &join_input;
	view.selection = &SljitBindHashJoinMatchSelection(output_proof, match_selection);
	view.hash_join_build_selection = &build_selection;
	view.hash_join_row_pointers = &row_pointers;
	view.hash_join_output_column_map = hash_join_output_column_map;
	view.count = selected_count;
	view.hash_join_idx = hash_join_idx;
	view.hash_join_output_projection_idx = hash_join_output_projection_idx;
	view.ownership = SljitRuntimeBatchOwnership::SELECTED_REFERENCE;
	view.hash_join_output_proof = output_proof;
	return view;
}

static DataChunk &SljitBindRuntimeBatchInput(const SljitRuntimeBatchView &input, const char *consumer_name) {
	if (!input.HasChunk()) {
		throw InternalException("%s requires an input chunk", consumer_name);
	}
	auto &chunk = input.Chunk();
	if (input.count > chunk.size()) {
		throw InternalException("%s count exceeds input chunk cardinality", consumer_name);
	}
	if (!input.selection && input.count != chunk.size()) {
		throw InternalException("%s requires a selection for partial chunk input", consumer_name);
	}
	return chunk;
}

static DataChunk &SljitBindMaterializedRuntimeBatchInput(const SljitRuntimeBatchView &input,
                                                         const char *consumer_name) {
	auto &chunk = SljitBindRuntimeBatchInput(input, consumer_name);
	if (!input.IsMaterializedChunk()) {
		throw InternalException("%s requires a materialized batch view", consumer_name);
	}
	if (input.count != chunk.size()) {
		throw InternalException("%s count does not match input chunk cardinality", consumer_name);
	}
	return chunk;
}

} // namespace duckdb

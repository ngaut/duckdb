//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_output_reference_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_source.hpp"

namespace duckdb {

static bool SljitTryBuildAllValidHashJoinOutputReference(const ExecutionHashJoinProbeBinding &binding,
                                                         DataChunk &join_input, const SelectionVector &match_selection,
                                                         Vector &row_pointers, idx_t join_output_source_index,
                                                         const LogicalType &source_type, idx_t count,
                                                         Vector &reference) {
	if (!binding.ready) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count) {
		const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
		if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != source_type ||
		    !SljitSourceVectorRowsAllValid(join_input.data[input_col], count, &match_selection)) {
			return false;
		}
		reference.Slice(join_input.data[input_col], match_selection, count);
		return true;
	}
	if (!binding.hash_table || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	if (rhs_col_idx >= binding.rhs_output_column_count) {
		return false;
	}
	SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, reference);
	return SljitSourceVectorRowsAllValid(reference, count);
}

static bool SljitTryCopyAllValidHashJoinOutputReferenceToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                               DataChunk &join_input,
                                                               const SelectionVector &match_selection,
                                                               Vector &row_pointers, idx_t join_output_source_index,
                                                               Vector &target, idx_t current_size, idx_t count) {
	Vector reference(target.GetType());
	if (!SljitTryBuildAllValidHashJoinOutputReference(binding, join_input, match_selection, row_pointers,
	                                                  join_output_source_index, target.GetType(), count, reference)) {
		return false;
	}
	target.Copy(reference, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
	return true;
}

static bool SljitTryMaterializeHashJoinOutputReferenceToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                              DataChunk &join_input,
                                                              const SelectionVector &match_selection,
                                                              Vector &row_pointers, idx_t join_output_source_index,
                                                              Vector &target, idx_t current_size, idx_t count) {
	if (!binding.ready || !binding.hash_table ||
	    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    target.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !SljitDirectProjectionBatchSupportsType(target.GetType()) ||
	    FlatVector::GetCapacity(target) < current_size + count) {
		return false;
	}
	if (join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	if (binding.output_types[join_output_source_index] != target.GetType()) {
		return false;
	}

	return SljitTryCopyAllValidHashJoinOutputReferenceToBatch(binding, join_input, match_selection, row_pointers,
	                                                          join_output_source_index, target, current_size, count);
}

} // namespace duckdb

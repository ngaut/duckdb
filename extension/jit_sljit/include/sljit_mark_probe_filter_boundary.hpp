//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_mark_probe_filter_boundary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_mark_probe_filter_mode.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

enum class SljitMarkProbeFilterBoundaryMarkerMode : uint8_t {
	OMIT_MARKER,
	REFERENCE_TRUE,
	REFERENCE_FALSE,
	MATERIALIZE_FLAGS
};

struct SljitMarkProbeFilterBoundary {
	SljitRuntimeBatchView input;
	DataChunk *output = nullptr;
	const SelectionVector *mark_flags = nullptr;
	idx_t count = 0;
};

struct SljitMarkProbeFilterBoundaryResult {
	SljitRuntimeBatchView output;
	const SelectionVector *selection = nullptr;
	idx_t selected_count = 0;

	DataChunk &OutputChunk() const {
		return output.Chunk();
	}

	const SelectionVector *ExecuteSelection() const {
		return selection;
	}
};

struct SljitBuiltMarkProbeFilterBoundary {
	SljitMarkProbeFilterBoundary boundary;
	SljitMarkProbeFilterBoundaryResult result;
	bool built = false;

	DataChunk &OutputChunk() const {
		return result.OutputChunk();
	}

	const SelectionVector *ExecuteSelection() const {
		return result.ExecuteSelection();
	}

	idx_t SelectedCount() const {
		return result.selected_count;
	}
};

static idx_t SljitSelectMarkProbeMatches(const SelectionVector &mark_flags, idx_t count, SelectionVector &selection) {
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (mark_flags.get_index(row_idx) != 0) {
			selection.set_index(selected_count++, row_idx);
		}
	}
	return selected_count;
}

static idx_t SljitSelectMarkProbeNonMatches(const ExecutionHashJoinProbeBinding &binding,
                                            const SljitRuntimeBatchView &input, const SelectionVector &mark_flags,
                                            idx_t count, SelectionVector &selection) {
	if (!binding.ready || !binding.hash_table) {
		throw InternalException("SLJIT MARK probe non-match selection requires a bound hash join table");
	}
	if (!input.HasChunk()) {
		throw InternalException("SLJIT MARK probe non-match selection requires an input chunk");
	}
	auto &input_chunk = input.Chunk();
	if (count > input.count || count > input_chunk.size()) {
		throw InternalException("SLJIT MARK probe non-match selection count exceeds input size");
	}
	if (binding.hash_table->has_null) {
		return 0;
	}

	vector<UnifiedVectorFormat> nullable_key_formats;
	for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
		if (binding.hash_table->NullValuesAreEqual(key_idx)) {
			continue;
		}
		auto input_col = binding.probe_key_input_indices[key_idx];
		if (input_col >= input_chunk.ColumnCount()) {
			throw InternalException("SLJIT MARK probe key column index out of range");
		}
		UnifiedVectorFormat key_format;
		input_chunk.data[input_col].ToUnifiedFormat(key_format);
		if (key_format.validity.CanHaveNull()) {
			nullable_key_formats.push_back(std::move(key_format));
		}
	}

	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (mark_flags.get_index(row_idx) != 0) {
			continue;
		}
		bool key_has_null = false;
		for (auto &key_format : nullable_key_formats) {
			auto key_row = key_format.sel->get_index(row_idx);
			if (!key_format.validity.RowIsValidUnsafe(key_row)) {
				key_has_null = true;
				break;
			}
		}
		if (!key_has_null) {
			selection.set_index(selected_count++, row_idx);
		}
	}
	return selected_count;
}

static void SljitSelectMarkProbeFilterBoundaryRows(const ExecutionHashJoinProbeBinding &binding,
                                                   const SljitMarkProbeFilterBoundary &boundary,
                                                   SljitMarkProbeFilterMode mode, SelectionVector &selection,
                                                   SljitMarkProbeFilterBoundaryResult &result) {
	if (!boundary.mark_flags) {
		throw InternalException("SLJIT MARK probe filter boundary selection requires mark flags");
	}
	idx_t selected_count;
	switch (mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		selected_count = SljitSelectMarkProbeMatches(*boundary.mark_flags, boundary.count, selection);
		break;
	case SljitMarkProbeFilterMode::NON_MATCHES:
		selected_count =
		    SljitSelectMarkProbeNonMatches(binding, boundary.input, *boundary.mark_flags, boundary.count, selection);
		break;
	default:
		throw InternalException("SLJIT MARK probe filter has no direct selection mode");
	}
	result.selected_count = selected_count;
	result.selection = selected_count == 0 || selected_count == boundary.count ? nullptr : &selection;
}

static const char *SljitMarkProbeFilterSelectionPath(SljitMarkProbeFilterMode mode) {
	switch (mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		return "mark_probe_match_selection";
	case SljitMarkProbeFilterMode::NON_MATCHES:
		return "mark_probe_nonmatch_selection";
	default:
		return "mark_probe_unknown_selection";
	}
}

static bool SljitMarkProbeFilterBoundaryOmitsMarker(SljitMarkProbeFilterBoundaryMarkerMode marker_mode) {
	return marker_mode == SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER;
}

static bool SljitCanBindMarkProbeFilterBoundary(const ExecutionHashJoinProbeBinding &binding,
                                                const SljitMarkProbeFilterBoundary &boundary,
                                                SljitMarkProbeFilterBoundaryMarkerMode marker_mode) {
	const auto expected_column_count =
	    binding.lhs_output_column_indices.size() + (SljitMarkProbeFilterBoundaryOmitsMarker(marker_mode) ? 0 : 1);
	if (!binding.ready || binding.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE ||
	    binding.correlated_mark_counts_required || !boundary.input.HasChunk() || boundary.input.selection ||
	    !boundary.output || boundary.count > boundary.input.count || boundary.count > boundary.input.Chunk().size() ||
	    boundary.output->ColumnCount() != expected_column_count) {
		return false;
	}
	return true;
}

static bool SljitTryReferenceMarkProbeBoundaryLHS(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                                  DataChunk &output) {
	output.Reset();
	for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
		auto input_col = binding.lhs_output_column_indices[col_idx];
		if (input_col >= input.ColumnCount() || input.data[input_col].GetType() != output.data[col_idx].GetType()) {
			return false;
		}
		output.data[col_idx].Reference(input.data[input_col]);
	}
	return true;
}

static bool SljitMarkProbeBoundaryMarkerIsBoolean(DataChunk &output) {
	return output.data.back().GetType().id() == LogicalTypeId::BOOLEAN;
}

static void SljitFinishMarkProbeFilterBoundaryOutput(DataChunk &output, idx_t count,
                                                     SljitMarkProbeFilterBoundaryResult &result) {
	output.SetChildCardinality(count);
	result.output = SljitRuntimeBatchViewFromChunk(output, nullptr, count);
}

static void SljitBuildReferencedTrueMarkProbeBoundaryMarker(DataChunk &output, idx_t count) {
	auto &mark_vector = output.data.back();
	mark_vector.Reference(Value::BOOLEAN(true), count_t(count));
}

static void SljitBuildReferencedFalseMarkProbeBoundaryMarker(DataChunk &output, idx_t count) {
	auto &mark_vector = output.data.back();
	mark_vector.Reference(Value::BOOLEAN(false), count_t(count));
}

static bool SljitBuildMaterializedMarkProbeBoundaryMarker(const ExecutionHashJoinProbeBinding &binding,
                                                          SljitMarkProbeFilterBoundary &boundary, DataChunk &input,
                                                          DataChunk &output) {
	if (!boundary.mark_flags || !binding.hash_table) {
		return false;
	}
	auto &mark_vector = output.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(mark_vector, count_t(boundary.count));
	auto bool_result = FlatVector::GetDataMutable<bool>(mark_vector);
	auto &mask = FlatVector::ValidityMutable(mark_vector);
	mask.SetAllValid(boundary.count);
	for (idx_t row_idx = 0; row_idx < boundary.count; row_idx++) {
		bool_result[row_idx] = boundary.mark_flags->get_index(row_idx) != 0;
	}

	for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
		if (binding.hash_table->NullValuesAreEqual(key_idx)) {
			continue;
		}
		auto input_col = binding.probe_key_input_indices[key_idx];
		if (input_col >= input.ColumnCount()) {
			throw InternalException("SLJIT MARK probe key column index out of range");
		}
		UnifiedVectorFormat key_data;
		input.data[input_col].ToUnifiedFormat(key_data);
		if (!key_data.validity.CanHaveNull()) {
			continue;
		}
		for (idx_t row_idx = 0; row_idx < boundary.count; row_idx++) {
			auto key_row = key_data.sel->get_index(row_idx);
			if (!key_data.validity.RowIsValidUnsafe(key_row)) {
				mask.SetInvalid(row_idx);
			}
		}
	}
	if (binding.hash_table->has_null) {
		for (idx_t row_idx = 0; row_idx < boundary.count; row_idx++) {
			if (!bool_result[row_idx]) {
				mask.SetInvalid(row_idx);
			}
		}
	}
	return true;
}

static bool SljitTryBuildMarkProbeFilterBoundary(const ExecutionHashJoinProbeBinding &binding,
                                                 SljitMarkProbeFilterBoundary &boundary,
                                                 SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
                                                 SljitMarkProbeFilterBoundaryResult &result) {
	if (!SljitCanBindMarkProbeFilterBoundary(binding, boundary, marker_mode)) {
		return false;
	}

	auto &input = boundary.input.Chunk();
	auto &output = *boundary.output;
	if (!SljitTryReferenceMarkProbeBoundaryLHS(binding, input, output)) {
		return false;
	}
	switch (marker_mode) {
	case SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER:
		break;
	case SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE:
		if (!SljitMarkProbeBoundaryMarkerIsBoolean(output)) {
			return false;
		}
		SljitBuildReferencedTrueMarkProbeBoundaryMarker(output, boundary.count);
		break;
	case SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_FALSE:
		if (!SljitMarkProbeBoundaryMarkerIsBoolean(output)) {
			return false;
		}
		SljitBuildReferencedFalseMarkProbeBoundaryMarker(output, boundary.count);
		break;
	case SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS:
		if (!SljitMarkProbeBoundaryMarkerIsBoolean(output)) {
			return false;
		}
		if (!SljitBuildMaterializedMarkProbeBoundaryMarker(binding, boundary, input, output)) {
			return false;
		}
		break;
	default:
		throw InternalException("Unsupported SLJIT MARK probe filter boundary marker mode");
	}
	SljitFinishMarkProbeFilterBoundaryOutput(output, boundary.count, result);
	return true;
}

static SljitMarkProbeFilterBoundaryMarkerMode
SljitChooseMarkProbeFilterBoundaryMarkerMode(const SljitExecutableRegionOp &hash_join_op,
                                             const SljitExecutableRegionOp &projection_op,
                                             SljitMarkProbeFilterMode mark_filter_mode,
                                             bool allow_marker_omission = false) {
	if (hash_join_op.output_types.empty()) {
		throw InternalException("SLJIT MARK probe boundary has no marker output");
	}
	const auto marker_idx = hash_join_op.output_types.size() - 1;
	const bool projection_reads_marker =
	    SljitProjectionReferencesInputColumn(projection_op, hash_join_op.output_types.size(), marker_idx);
	if (allow_marker_omission && !projection_reads_marker) {
		return SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER;
	}
	const bool can_reference_marker_output =
	    mark_filter_mode == SljitMarkProbeFilterMode::MATCHES || !projection_reads_marker;
	return can_reference_marker_output ? SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE
	                                   : SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS;
}

static SljitMarkProbeFilterBoundaryMarkerMode
SljitChooseMaterializedMarkProbeFilterBoundaryMarkerMode(SljitMarkProbeFilterMode mark_filter_mode) {
	switch (mark_filter_mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		return SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE;
	case SljitMarkProbeFilterMode::NON_MATCHES:
		return SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_FALSE;
	default:
		return SljitMarkProbeFilterBoundaryMarkerMode::MATERIALIZE_FLAGS;
	}
}

static bool SljitTryBuildSelectedMarkProbeFilterBoundary(const ExecutionHashJoinProbeBinding &binding,
                                                         SljitMarkProbeFilterBoundary &boundary,
                                                         const SelectionVector &selection, idx_t selected_count,
                                                         SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
                                                         SljitMarkProbeFilterBoundaryResult &result) {
	if (!SljitCanBindMarkProbeFilterBoundary(binding, boundary, marker_mode)) {
		return false;
	}

	auto &input = boundary.input.Chunk();
	auto &output = *boundary.output;
	output.Reset();
	for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
		auto input_col = binding.lhs_output_column_indices[col_idx];
		if (input_col >= input.ColumnCount() || input.data[input_col].GetType() != output.data[col_idx].GetType()) {
			return false;
		}
		output.data[col_idx].Slice(input.data[input_col], selection, selected_count);
	}
	switch (marker_mode) {
	case SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER:
		break;
	case SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE:
		if (!SljitMarkProbeBoundaryMarkerIsBoolean(output)) {
			return false;
		}
		SljitBuildReferencedTrueMarkProbeBoundaryMarker(output, selected_count);
		break;
	case SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_FALSE:
		if (!SljitMarkProbeBoundaryMarkerIsBoolean(output)) {
			return false;
		}
		SljitBuildReferencedFalseMarkProbeBoundaryMarker(output, selected_count);
		break;
	default:
		return false;
	}
	SljitFinishMarkProbeFilterBoundaryOutput(output, selected_count, result);
	return true;
}

template <class SCRATCH>
static bool SljitTryBuildMarkProbeFilterBoundaryFromInput(SCRATCH &scratch, idx_t hash_join_idx, DataChunk &input,
                                                          DataChunk &join_output, idx_t mark_count,
                                                          SljitMarkProbeFilterBoundaryMarkerMode marker_mode,
                                                          SljitBuiltMarkProbeFilterBoundary &built_boundary) {
	built_boundary = SljitBuiltMarkProbeFilterBoundary();
	auto input_view = SljitRuntimeBatchViewFromChunk(input, nullptr, mark_count);
	built_boundary.boundary =
	    SljitMarkProbeFilterBoundary {input_view, &join_output, &scratch.FilterSelection(hash_join_idx), mark_count};
	if (!scratch.HasOperatorBinding(hash_join_idx)) {
		return false;
	}
	built_boundary.built =
	    SljitTryBuildMarkProbeFilterBoundary(scratch.OperatorBinding(hash_join_idx).hash_join_probe,
	                                         built_boundary.boundary, marker_mode, built_boundary.result);
	return built_boundary.built;
}

template <class SCRATCH>
static void SljitSelectMarkProbeFilterProjectionBoundaryRows(ExecutionRegionRuntime &runtime, SCRATCH &scratch,
                                                             idx_t hash_join_idx, idx_t filter_idx,
                                                             SljitExecutableRegionOp &filter_op,
                                                             SljitMarkProbeFilterMode mark_filter_mode,
                                                             SljitBuiltMarkProbeFilterBoundary &built_boundary) {
	if (!built_boundary.built) {
		throw InternalException("SLJIT MARK probe projection boundary selection requires a built boundary");
	}
	auto selection_stage_start = SljitRegionStageStart(runtime);
	auto &mark_selection = scratch.FilterSelection(filter_idx);
	SljitSelectMarkProbeFilterBoundaryRows(scratch.OperatorBinding(hash_join_idx).hash_join_probe,
	                                       built_boundary.boundary, mark_filter_mode, mark_selection,
	                                       built_boundary.result);
	RecordSljitRegionStageRuntimePath(runtime, filter_idx, filter_op.kind,
	                                  SljitMarkProbeFilterSelectionPath(mark_filter_mode), selection_stage_start);
}

} // namespace duckdb

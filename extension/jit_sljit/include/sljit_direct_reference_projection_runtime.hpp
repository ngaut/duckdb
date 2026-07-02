//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_reference_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_reference_projection_compaction_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include <array>

namespace duckdb {

template <class FLUSH>
static bool SljitTryAppendComparedReferenceProjectionBatch(
    ExecutionRegionRuntime &runtime, idx_t filter_idx, SljitExecutableRegionOp &filter_op,
    SljitExecutableRegionOp &projection_op, SljitExecutableRegionOp &append_trace_op, DataChunk &source_chunk,
    SljitDataChunkBatch &filtered_batch, idx_t projection_source_index, FLUSH &&flush_filtered_batch, bool &stopped) {
	stopped = false;
	auto &filter = filter_op.filter.plan;
	if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
	    filter.source_index >= source_chunk.ColumnCount() || filter.right_source_index >= source_chunk.ColumnCount()) {
		return false;
	}
	filtered_batch.Ensure(runtime.GetAllocator(), projection_op.output_types);
	auto &batch = filtered_batch.chunk;
	if (batch.ColumnCount() != 1 || batch.data[0].GetType() != projection_op.output_types[0]) {
		return false;
	}

	UnifiedVectorFormat left_format;
	UnifiedVectorFormat right_format;
	UnifiedVectorFormat project_format;
	auto &left = source_chunk.data[filter.source_index];
	auto &right = source_chunk.data[filter.right_source_index];
	auto &project = source_chunk.data[projection_source_index];
	if (!SljitDirectReferenceProjectionAllValidSource(left_format, left, source_chunk.size()) ||
	    !SljitDirectReferenceProjectionAllValidSource(right_format, right, source_chunk.size()) ||
	    !SljitDirectReferenceProjectionAllValidSource(project_format, project, source_chunk.size())) {
		return false;
	}
	auto left_data = NativeIntegerSourceData(left_format, filter.integer_kind);
	auto right_data = NativeIntegerSourceData(right_format, filter.integer_kind);
	auto project_data = project_format.data;
	auto left_sel = SljitNormalizedSourceSelectionData(left_format);
	auto right_sel = SljitNormalizedSourceSelectionData(right_format);
	auto project_sel = SljitNormalizedSourceSelectionData(project_format);

	auto stage_start = SljitRegionStageStart(runtime);
	idx_t appended_count = 0;
	idx_t source_offset = 0;
	while (source_offset < source_chunk.size()) {
		const auto current_size = batch.size();
		if (current_size == STANDARD_VECTOR_SIZE) {
			if (flush_filtered_batch()) {
				stopped = true;
				return true;
			}
			continue;
		}
		const auto append_capacity = STANDARD_VECTOR_SIZE - current_size;
		const auto input_count = MinValue<idx_t>(append_capacity, source_chunk.size() - source_offset);
		idx_t selected_count = 0;
		if (!SljitTryCompactComparedReferenceValues(
		        filter.integer_kind, projection_op.output_types[0], left_data, left_sel, right_data, right_sel,
		        project_data, project_sel, input_count, source_offset, filter.compare_op,
		        FlatVector::GetDataMutable(batch.data[0]), current_size, selected_count)) {
			return false;
		}
		if (selected_count > 0) {
			SljitFinishDirectProjectionBatchTargets(batch, current_size + selected_count);
			appended_count += selected_count;
		}
		source_offset += input_count;
		if (batch.size() == STANDARD_VECTOR_SIZE && flush_filtered_batch()) {
			stopped = true;
			return true;
		}
	}
	RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+direct_compact_reference_projection",
	                                        stage_start);
	RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_compact_reference_projection", appended_count);
	RecordSljitRegionMaterializationBoundary(runtime, append_trace_op.kind, "filtered_input_batch", appended_count);
	return true;
}

template <class FLUSH>
static bool SljitTryAppendSelectedReferenceProjectionBatch(ExecutionRegionRuntime &runtime,
                                                           SljitRegionExecutionScratch &scratch, idx_t filter_idx,
                                                           SljitExecutableRegionOp &filter_op,
                                                           SljitExecutableRegionOp &projection_op,
                                                           SljitExecutableRegionOp &append_trace_op,
                                                           DataChunk &source_chunk, SljitDataChunkBatch &filtered_batch,
                                                           FLUSH &&flush_filtered_batch, bool &stopped) {
	stopped = false;
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.empty() ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	static constexpr idx_t MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS = 16;
	const auto direct_projection_count = projection_op.projections.size();
	if (direct_projection_count > MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS) {
		return false;
	}

	std::array<idx_t, MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> projection_source_indices;
	for (idx_t projection_idx = 0; projection_idx < direct_projection_count; projection_idx++) {
		auto &projection_expr = projection_op.projections[projection_idx];
		auto &projection = projection_expr.plan;
		idx_t projection_source_index;
		if (!SljitTryReadDirectReferenceProjectionSource(projection_expr, projection_source_index) ||
		    projection_source_index >= source_chunk.ColumnCount()) {
			return false;
		}
		auto &project_source = source_chunk.data[projection_source_index];
		if (projection.return_type != project_source.GetType() ||
		    projection.return_type != projection_op.output_types[projection_idx] ||
		    !DirectAppendSupportsFixedSizeType(projection.return_type)) {
			return false;
		}
		projection_source_indices[projection_idx] = projection_source_index;
	}

	filtered_batch.Ensure(runtime.GetAllocator(), projection_op.output_types);
	auto &batch = filtered_batch.chunk;
	if (batch.ColumnCount() != direct_projection_count) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < direct_projection_count; projection_idx++) {
		if (batch.data[projection_idx].GetType() != projection_op.output_types[projection_idx]) {
			return false;
		}
	}
	if (direct_projection_count == 1 &&
	    SljitTryAppendComparedReferenceProjectionBatch(runtime, filter_idx, filter_op, projection_op, append_trace_op,
	                                                   source_chunk, filtered_batch, projection_source_indices[0],
	                                                   flush_filtered_batch, stopped)) {
		return true;
	}

	std::array<UnifiedVectorFormat, MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> project_formats;
	std::array<const sel_t *, MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> project_source_sels;
	std::array<const_data_ptr_t, MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> project_source_data;
	project_source_sels.fill(nullptr);
	project_source_data.fill(nullptr);
	for (idx_t projection_idx = 0; projection_idx < direct_projection_count; projection_idx++) {
		auto &project_source = source_chunk.data[projection_source_indices[projection_idx]];
		if (!SljitDirectReferenceProjectionAllValidSource(project_formats[projection_idx], project_source,
		                                                  source_chunk.size()) ||
		    batch.data[projection_idx].GetVectorType() != VectorType::FLAT_VECTOR ||
		    FlatVector::GetCapacity(batch.data[projection_idx]) < STANDARD_VECTOR_SIZE) {
			return false;
		}
		project_source_sels[projection_idx] = SljitNormalizedSourceSelectionData(project_formats[projection_idx]);
		project_source_data[projection_idx] = project_formats[projection_idx].data;
	}

	auto &filter_selection = scratch.FilterSelection(filter_idx);
	auto &filter_scratch = scratch.ExpressionAdapterScratch(filter_idx, 0);
	auto stage_start = SljitRegionStageStart(runtime);
	auto selected_count = SljitSelectFilter(filter_op, source_chunk, filter_selection, filter_scratch);
	RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+direct_reference_projection",
	                                        stage_start);
	if (selected_count == 0) {
		return true;
	}
	auto filter_selection_ptr = selected_count == source_chunk.size() ? nullptr : &filter_selection;

	idx_t appended_count = 0;
	idx_t source_offset = 0;
	while (source_offset < selected_count) {
		const auto current_size = batch.size();
		if (current_size == STANDARD_VECTOR_SIZE) {
			if (flush_filtered_batch()) {
				stopped = true;
				return true;
			}
			continue;
		}
		const auto append_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE - current_size, selected_count - source_offset);
		for (idx_t projection_idx = 0; projection_idx < direct_projection_count; projection_idx++) {
			if (!SljitCopySelectedFixedValues(projection_op.output_types[projection_idx],
			                                  project_source_data[projection_idx], project_source_sels[projection_idx],
			                                  filter_selection_ptr, source_offset, append_count,
			                                  FlatVector::GetDataMutable(batch.data[projection_idx]), current_size)) {
				return false;
			}
		}
		SljitFinishDirectProjectionBatchTargets(batch, current_size + append_count);
		source_offset += append_count;
		appended_count += append_count;
		if (batch.size() == STANDARD_VECTOR_SIZE && flush_filtered_batch()) {
			stopped = true;
			return true;
		}
	}
	RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_selected_reference_projection", appended_count);
	RecordSljitRegionMaterializationBoundary(runtime, append_trace_op.kind, "filtered_input_batch", appended_count);
	return true;
}

} // namespace duckdb

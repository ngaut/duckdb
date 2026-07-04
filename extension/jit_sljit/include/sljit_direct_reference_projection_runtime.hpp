//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_reference_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_batch_runtime.hpp"
#include "sljit_direct_reference_projection_compaction_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include <array>

namespace duckdb {

static constexpr idx_t SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS = 16;

struct SljitDirectReferenceProjectionPlan {
	idx_t count = 0;
	std::array<idx_t, SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> source_indices;
	std::array<UnifiedVectorFormat, SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> source_formats;
	std::array<const sel_t *, SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> source_sels;
	std::array<const_data_ptr_t, SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS> source_data;
};

static bool SljitTryPrepareDirectReferenceProjectionPlan(SljitExecutableRegionOp &projection_op,
                                                         DataChunk &source_chunk, DataChunk &target_chunk,
                                                         idx_t target_capacity,
                                                         SljitDirectReferenceProjectionPlan &plan) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.empty() ||
	    projection_op.projections.size() != projection_op.output_types.size() ||
	    projection_op.projections.size() > SLJIT_MAX_DIRECT_REFERENCE_PROJECTION_COLUMNS ||
	    source_chunk.size() > STANDARD_VECTOR_SIZE) {
		return false;
	}
	plan.count = projection_op.projections.size();
	plan.source_sels.fill(nullptr);
	plan.source_data.fill(nullptr);
	if (target_chunk.ColumnCount() != plan.count) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < plan.count; projection_idx++) {
		auto &projection_expr = projection_op.projections[projection_idx];
		auto &projection = projection_expr.plan;
		idx_t projection_source_index;
		if (!SljitTryReadDirectReferenceProjectionSource(projection_expr, projection_source_index) ||
		    projection_source_index >= source_chunk.ColumnCount()) {
			return false;
		}
		auto &project_source = source_chunk.data[projection_source_index];
		auto &target = target_chunk.data[projection_idx];
		if (projection.return_type != project_source.GetType() ||
		    projection.return_type != projection_op.output_types[projection_idx] ||
		    target.GetType() != projection.return_type || !DirectAppendSupportsFixedSizeType(projection.return_type) ||
		    target.GetVectorType() != VectorType::FLAT_VECTOR || FlatVector::GetCapacity(target) < target_capacity ||
		    !SljitDirectReferenceProjectionAllValidSource(plan.source_formats[projection_idx], project_source,
		                                                  source_chunk.size())) {
			return false;
		}
		plan.source_indices[projection_idx] = projection_source_index;
		plan.source_sels[projection_idx] = SljitNormalizedSourceSelectionData(plan.source_formats[projection_idx]);
		plan.source_data[projection_idx] = plan.source_formats[projection_idx].data;
	}
	return true;
}

static bool SljitCopySelectedReferenceProjectionPlanToTargets(const SljitDirectReferenceProjectionPlan &plan,
                                                              SljitExecutableRegionOp &projection_op,
                                                              DataChunk &target_chunk,
                                                              const SelectionVector *filter_selection,
                                                              idx_t source_offset, idx_t count, idx_t target_offset) {
	if (target_offset + count > STANDARD_VECTOR_SIZE || target_chunk.ColumnCount() != plan.count) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < plan.count; projection_idx++) {
		auto target_data = FlatVector::GetDataMutable(target_chunk.data[projection_idx]);
		if (!SljitCopySelectedFixedValues(projection_op.output_types[projection_idx], plan.source_data[projection_idx],
		                                  plan.source_sels[projection_idx], filter_selection, source_offset, count,
		                                  target_data, target_offset)) {
			return false;
		}
	}
	return true;
}

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
	filtered_batch.Ensure(runtime.GetAllocator(), projection_op.output_types);
	auto &batch = filtered_batch.chunk;
	SljitDirectReferenceProjectionPlan projection_plan;
	if (!SljitTryPrepareDirectReferenceProjectionPlan(projection_op, source_chunk, batch, STANDARD_VECTOR_SIZE,
	                                                  projection_plan)) {
		return false;
	}
	const auto direct_projection_count = projection_plan.count;
	if (direct_projection_count == 1 &&
	    SljitTryAppendComparedReferenceProjectionBatch(runtime, filter_idx, filter_op, projection_op, append_trace_op,
	                                                   source_chunk, filtered_batch, projection_plan.source_indices[0],
	                                                   flush_filtered_batch, stopped)) {
		return true;
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
		if (!SljitCopySelectedReferenceProjectionPlanToTargets(projection_plan, projection_op, batch,
		                                                       filter_selection_ptr, source_offset, append_count,
		                                                       current_size)) {
			return false;
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

static bool SljitTryMaterializeSelectedReferenceProjectionToChunk(ExecutionRegionRuntime &runtime,
                                                                  SljitRegionExecutionScratch &scratch,
                                                                  idx_t filter_idx, SljitExecutableRegionOp &filter_op,
                                                                  SljitExecutableRegionOp &projection_op,
                                                                  SljitExecutableRegionOp &boundary_op,
                                                                  DataChunk &source_chunk, DataChunk &target_chunk) {
	target_chunk.Reset();
	if (source_chunk.size() == 0) {
		return true;
	}
	SljitDirectReferenceProjectionPlan projection_plan;
	if (!SljitTryPrepareDirectReferenceProjectionPlan(projection_op, source_chunk, target_chunk, source_chunk.size(),
	                                                  projection_plan)) {
		return false;
	}
	auto &filter_selection = scratch.FilterSelection(filter_idx);
	auto &filter_scratch = scratch.ExpressionAdapterScratch(filter_idx, 0);
	auto stage_start = SljitRegionStageStart(runtime);
	auto selected_count = SljitSelectFilter(filter_op, source_chunk, filter_selection, filter_scratch);
	if (selected_count > 0) {
		auto filter_selection_ptr = selected_count == source_chunk.size() ? nullptr : &filter_selection;
		if (!SljitCopySelectedReferenceProjectionPlanToTargets(projection_plan, projection_op, target_chunk,
		                                                       filter_selection_ptr, 0, selected_count, 0)) {
			return false;
		}
		SljitFinishDirectProjectionBatchTargets(target_chunk, selected_count);
		RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_selected_reference_projection", selected_count);
		RecordSljitRegionMaterializationBoundary(runtime, boundary_op.kind, "filtered_input_batch", selected_count);
	}
	RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+direct_reference_projection",
	                                        stage_start);
	return true;
}

} // namespace duckdb

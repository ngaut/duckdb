//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_reference_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_executable.hpp"

namespace duckdb {

static bool SljitTryAppendReferenceProjectionToBatch(DataChunk &batch, DataChunk &input,
                                                     const SljitExecutableRegionOp &projection_op,
                                                     const SelectionVector &selection, idx_t count) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != batch.ColumnCount()) {
		return false;
	}
	const auto current_size = batch.size();
	if (current_size + count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx].plan;
		if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    projection.source_index >= input.ColumnCount()) {
			return false;
		}
		auto &target = batch.data[projection_idx];
		auto &source = input.data[projection.source_index];
		if (target.size() != current_size || target.GetType() != source.GetType() ||
		    projection.return_type != source.GetType()) {
			return false;
		}
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx].plan;
		batch.data[projection_idx].Append(input.data[projection.source_index], selection, count);
	}
	batch.CheckCardinality(current_size + count);
	return true;
}

static bool SljitTryReferenceProjection(DataChunk &output, DataChunk &input,
                                        const SljitExecutableRegionOp &projection_op) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != output.ColumnCount()) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx].plan;
		if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    projection.source_index >= input.ColumnCount()) {
			return false;
		}
		auto &source = input.data[projection.source_index];
		if (projection.return_type != source.GetType() || output.data[projection_idx].GetType() != source.GetType()) {
			return false;
		}
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx].plan;
		output.data[projection_idx].Reference(input.data[projection.source_index]);
	}
	output.SetChildCardinality(input.size());
	return true;
}

static bool SljitTrySliceReferenceProjection(DataChunk &output, DataChunk &input,
                                             const SljitExecutableRegionOp &projection_op,
                                             const SelectionVector *selection, idx_t count) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != output.ColumnCount() || count > input.size()) {
		return false;
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx];
		idx_t source_index;
		if (!SljitTryGetSingleSourceReferenceProjectionIndex(projection, source_index) ||
		    source_index >= input.ColumnCount()) {
			return false;
		}
		auto &source = input.data[source_index];
		if (projection.plan.return_type != source.GetType() ||
		    projection_op.output_types[projection_idx] != source.GetType() ||
		    output.data[projection_idx].GetType() != source.GetType()) {
			return false;
		}
	}
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		idx_t source_index;
		auto source_found =
		    SljitTryGetSingleSourceReferenceProjectionIndex(projection_op.projections[projection_idx], source_index);
		D_ASSERT(source_found);
		if (selection) {
			output.data[projection_idx].Slice(input.data[source_index], *selection, count);
		} else {
			output.data[projection_idx].Reference(input.data[source_index]);
		}
	}
	output.SetChildCardinality(count);
	return true;
}

} // namespace duckdb

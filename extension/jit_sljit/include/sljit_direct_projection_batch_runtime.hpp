//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_batch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <chrono>

namespace duckdb {

static void SljitBuildPostFusedProjectionSkip(const vector<uint8_t> &skip_projection,
                                              const vector<uint8_t> &fused_projection,
                                              vector<uint8_t> &post_fused_skip) {
	post_fused_skip = skip_projection;
	if (post_fused_skip.size() < fused_projection.size()) {
		post_fused_skip.resize(fused_projection.size(), 0);
	}
	for (idx_t projection_idx = 0; projection_idx < fused_projection.size(); projection_idx++) {
		if (fused_projection[projection_idx]) {
			post_fused_skip[projection_idx] = 1;
		}
	}
}

static bool SljitDirectProjectionBatchSupportsType(const LogicalType &type) {
	return DirectAppendSupportsFixedSizeType(type) || type.id() == LogicalTypeId::VARCHAR;
}

static bool SljitSelectedProjectionHasGeneratedExpression(const SljitExecutableRegionOp &projection_op,
                                                          const vector<uint8_t> &skip_projection) {
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		if (projection_idx < skip_projection.size() && skip_projection[projection_idx]) {
			continue;
		}
		if (projection_op.projections[projection_idx].plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return true;
		}
	}
	return false;
}

static bool SljitPrepareDirectProjectionBatchTargets(
    DataChunk &batch, SljitExecutableRegionOp &projection_op, optional_ptr<const vector<idx_t>> output_to_projection,
    DirectAppendSlice &slice, vector<uint8_t> &skip_projection, vector<idx_t> &projection_to_output,
    optional_ptr<const vector<uint8_t>> initial_skip_projection = nullptr) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.empty()) {
		return false;
	}
	const auto current_size = batch.size();
	const auto target_size = current_size + slice.count;
	if (target_size > STANDARD_VECTOR_SIZE) {
		return false;
	}

	const auto output_count = output_to_projection ? output_to_projection->size() : projection_op.projections.size();
	if (batch.ColumnCount() != output_count) {
		return false;
	}
	slice.targets.assign(projection_op.projections.size(), nullptr);
	slice.sources.assign(projection_op.projections.size(), DirectAppendColumnSource());
	skip_projection.assign(projection_op.projections.size(), output_to_projection ? 1 : 0);
	projection_to_output.assign(projection_op.projections.size(), DConstants::INVALID_INDEX);
	if (initial_skip_projection) {
		if (initial_skip_projection->size() > skip_projection.size()) {
			return false;
		}
		for (idx_t projection_idx = 0; projection_idx < initial_skip_projection->size(); projection_idx++) {
			if ((*initial_skip_projection)[projection_idx]) {
				skip_projection[projection_idx] = 1;
			}
		}
	}

	for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
		const auto projection_idx = output_to_projection ? (*output_to_projection)[output_idx] : output_idx;
		if (projection_idx >= projection_op.projections.size() || slice.targets[projection_idx]) {
			return false;
		}
		if (projection_idx < skip_projection.size() && skip_projection[projection_idx]) {
			continue;
		}
		auto &target = batch.data[output_idx];
		auto &projection = projection_op.projections[projection_idx].plan;
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != projection.return_type ||
		    !DirectAppendSupportsFixedSizeType(target.GetType()) || FlatVector::GetCapacity(target) < target_size) {
			return false;
		}
		auto target_data = FlatVector::GetDataMutable(target);
		if (!target_data) {
			return false;
		}
		slice.targets[projection_idx] = target_data + current_size * GetTypeIdSize(target.GetType().InternalType());
		skip_projection[projection_idx] = 0;
		projection_to_output[projection_idx] = output_idx;
	}
	return SljitSelectedProjectionHasGeneratedExpression(projection_op, skip_projection);
}

static void SljitRecordDirectProjectionBatchMaterialization(ExecutionRegionRuntime &runtime, idx_t projection_idx,
                                                            SljitExecutableRegionOp &projection_op,
                                                            bool remapped_projection, idx_t count,
                                                            std::chrono::steady_clock::time_point stage_start) {
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
	                              remapped_projection ? "post_join_direct_remap_batch_projection"
	                                                  : "post_join_direct_batch_projection",
	                              stage_start);
	RecordSljitRegionMaterializationBoundary(
	    runtime, projection_op.kind,
	    remapped_projection ? "direct_remap_post_join_batch_projection" : "direct_post_join_batch_projection", count);
}

static void SljitHashDirectProjectionBatch(ExecutionRegionRuntime &runtime, idx_t projection_idx,
                                           SljitExecutableRegionOp &projection_op, bool remapped_projection,
                                           DataChunk &batch, Vector &hashes) {
	auto hash_start = SljitRegionStageStart(runtime);
	batch.Hash(hashes);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
	                              remapped_projection ? "post_join_direct_remap_batch_projection_hash"
	                                                  : "post_join_direct_batch_projection_hash",
	                              hash_start);
}

static void SljitFinishDirectProjectionBatchMaterialization(ExecutionRegionRuntime &runtime, idx_t projection_idx,
                                                            SljitExecutableRegionOp &projection_op,
                                                            bool remapped_projection, DataChunk &batch,
                                                            optional_ptr<Vector> projected_hashes, idx_t count,
                                                            std::chrono::steady_clock::time_point stage_start) {
	if (projected_hashes) {
		SljitHashDirectProjectionBatch(runtime, projection_idx, projection_op, remapped_projection, batch,
		                               *projected_hashes);
	}
	SljitRecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection, count,
	                                                stage_start);
}

static void SljitFinishDirectProjectionBatchTargets(DataChunk &batch, idx_t target_size,
                                                    bool new_rows_all_valid = true) {
	for (auto &target : batch.data) {
		if (new_rows_all_valid) {
			auto &validity = FlatVector::ValidityMutable(target);
			if (validity.CanHaveNull()) {
				for (idx_t row_idx = target.size(); row_idx < target_size; row_idx++) {
					validity.SetValid(row_idx);
				}
			}
		}
		FlatVector::SetSize(target, target_size);
	}
	batch.CheckCardinality(target_size);
}

static bool SljitFlatFusedFixedProjectionTargetsAreBound(SljitExecutableRegionOp &projection_op,
                                                         DirectAppendSlice &slice) {
	if (slice.targets.size() != projection_op.projections.size()) {
		return false;
	}
	for (auto &direct_plan : projection_op.flat_fused_fixed_projection_plans) {
		for (auto projection_idx : direct_plan.projection_indices) {
			if (projection_idx >= slice.targets.size() || !slice.targets[projection_idx]) {
				return false;
			}
		}
	}
	return true;
}

static void SljitCopyDirectProjectionResultToBatch(Vector &result, Vector &target, data_ptr_t target_data,
                                                   idx_t current_size, idx_t count) {
	const bool wrote_target =
	    result.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::GetData(result) == target_data;
	if (wrote_target) {
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.CopySel(FlatVector::Validity(result), *FlatVector::IncrementalSelectionVector(), 0,
		                        current_size, count);
	} else {
		target.Copy(result, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
	}
}

static bool SljitProjectionSkipHasAny(const vector<uint8_t> &skip_projection) {
	for (auto skip : skip_projection) {
		if (skip) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb

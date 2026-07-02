//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_expression_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <cstring>

namespace duckdb {

struct SljitDirectProjectionBatchPassthrough {
	idx_t output_idx = DConstants::INVALID_INDEX;
	Vector *source = nullptr;
	const SelectionVector *selection = nullptr;
	const char *trace_phase = "direct_batch_expression.passthrough";
};

static optional_ptr<const SljitDirectProjectionBatchPassthrough> SljitFindDirectProjectionBatchPassthrough(
    optional_ptr<const vector<SljitDirectProjectionBatchPassthrough>> passthroughs, idx_t output_idx) {
	if (!passthroughs) {
		return nullptr;
	}
	for (auto &passthrough : *passthroughs) {
		if (passthrough.output_idx == output_idx) {
			return optional_ptr<const SljitDirectProjectionBatchPassthrough>(&passthrough);
		}
	}
	return nullptr;
}

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

static bool SljitProjectionUnifiedFormatHasIdentitySelection(const UnifiedVectorFormat &format) {
	return !format.sel || format.sel == FlatVector::IncrementalSelectionVector();
}

static bool SljitTryDecodeInlineCompressedString16Value(uhugeint_t compressed_value, string_t &result) {
	auto value = BSwapIfBE(compressed_value);
	data_t compressed[sizeof(uhugeint_t)];
	memcpy(compressed, const_data_ptr_cast(&value), sizeof(uhugeint_t));
	const auto length = UnsafeNumericCast<idx_t>(compressed[0]);
	if (length > string_t::INLINE_LENGTH || length >= sizeof(uhugeint_t)) {
		return false;
	}
	char decoded[string_t::INLINE_LENGTH];
	for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
		decoded[byte_idx] = char(compressed[sizeof(uhugeint_t) - byte_idx - 1]);
	}
	result = string_t(decoded, UnsafeNumericCast<uint32_t>(length));
	return true;
}

static bool SljitTryFastDecodeInlineCompressedString16(Vector &source, idx_t count, Vector &result) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	if (!SljitProjectionUnifiedFormatHasIdentitySelection(source_format) ||
	    (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(count))) {
		return false;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.Reset(count);
	result_validity.SetAllValid(count);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto source_data = source_format.data;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!SljitTryDecodeInlineCompressedString16Value(Load<uhugeint_t>(source_data + row_idx * sizeof(uhugeint_t)),
		                                                 result_data[row_idx])) {
			return false;
		}
	}
	FlatVector::SetSize(result, count_t(count));
	return true;
}

static bool SljitTryFastInlineStringDecompressProjection(SljitExecutableRegionOp &op, DataChunk &input,
                                                         DataChunk &output, idx_t count) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.size() != output.ColumnCount()) {
		return false;
	}
	bool has_fast_decompress = false;
	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &plan = op.projections[projection_idx].plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (plan.source_index >= input.ColumnCount() ||
			    plan.return_type != input.data[plan.source_index].GetType() ||
			    output.data[projection_idx].GetType() != input.data[plan.source_index].GetType()) {
				return false;
			}
			continue;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
		    plan.string_decompress_source_size == sizeof(uhugeint_t) && plan.source_index < input.ColumnCount() &&
		    input.data[plan.source_index].GetType().InternalType() == PhysicalType::UINT128 &&
		    output.data[projection_idx].GetType().id() == LogicalTypeId::VARCHAR) {
			has_fast_decompress = true;
			continue;
		}
		return false;
	}
	if (!has_fast_decompress) {
		return false;
	}

	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &plan = op.projections[projection_idx].plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			output.data[projection_idx].Reference(input.data[plan.source_index]);
			continue;
		}
		if (!SljitTryFastDecodeInlineCompressedString16(input.data[plan.source_index], count,
		                                                output.data[projection_idx])) {
			return false;
		}
	}
	output.SetChildCardinality(count);
	return true;
}

static bool SljitProjectionTargetCanReceiveExpression(Vector &target, const LogicalType &return_type,
                                                      idx_t target_size) {
	return target.GetVectorType() == VectorType::FLAT_VECTOR && target.GetType() == return_type &&
	       SljitDirectProjectionBatchSupportsType(target.GetType()) && FlatVector::GetCapacity(target) >= target_size;
}

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
                                                            std::chrono::steady_clock::time_point stage_start,
                                                            bool used_passthrough = false) {
	if (projected_hashes) {
		SljitHashDirectProjectionBatch(runtime, projection_idx, projection_op, remapped_projection, batch,
		                               *projected_hashes);
	}
	if (used_passthrough) {
		RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_batch_passthrough_projection", count);
	}
	SljitRecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection, count,
	                                                stage_start);
}

static bool SljitTryCopyDirectProjectionPassthroughToBatch(const SljitDirectProjectionBatchPassthrough &passthrough,
                                                           Vector &target, idx_t current_size, idx_t count) {
	if (!passthrough.source || passthrough.source->GetType() != target.GetType() ||
	    target.GetVectorType() != VectorType::FLAT_VECTOR || FlatVector::GetCapacity(target) < current_size + count) {
		return false;
	}
	const auto &selection = passthrough.selection ? *passthrough.selection : *FlatVector::IncrementalSelectionVector();
	target.Copy(*passthrough.source, selection, count, 0, current_size, count);
	return true;
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

static bool SljitOrProjectionSkips(idx_t projection_count, optional_ptr<const vector<uint8_t>> left,
                                   optional_ptr<const vector<uint8_t>> right, vector<uint8_t> &merged,
                                   optional_ptr<const vector<uint8_t>> &merged_ptr) {
	merged_ptr = nullptr;
	if (!left && !right) {
		return true;
	}
	merged.assign(projection_count, 0);
	auto merge_one = [&](const vector<uint8_t> &skip) {
		if (skip.size() != projection_count) {
			return false;
		}
		for (idx_t projection_idx = 0; projection_idx < projection_count; projection_idx++) {
			if (skip[projection_idx]) {
				merged[projection_idx] = 1;
			}
		}
		return true;
	};
	if (left && !merge_one(*left)) {
		return false;
	}
	if (right && !merge_one(*right)) {
		return false;
	}
	merged_ptr = optional_ptr<const vector<uint8_t>>(&merged);
	return true;
}

} // namespace duckdb

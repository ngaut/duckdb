//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_fixed_runtime.hpp"
#include "sljit_direct_projection_floating_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

template <class SCRATCH>
static void SljitExecuteProjection(SCRATCH &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
                                   DataChunk &output, const SelectionVector *execute_sel = nullptr,
                                   idx_t count = DConstants::INVALID_INDEX) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	if (!execute_sel && SljitTryFastInlineStringDecompressProjection(op, input, output, count)) {
		return;
	}
	auto &projection_scratch = scratch.ProjectionScratch(op_idx);
	auto fused_projection_executed =
	    ExecuteFlatFusedFloatingProjection(op, input, output, execute_sel, count, projection_scratch);
	if (fused_projection_executed && op.flat_fused_floating_projection_plan.covers_all_projections) {
		output.SetChildCardinality(count);
		return;
	}
	for (idx_t col_idx = 0; col_idx < op.projections.size(); col_idx++) {
		if (fused_projection_executed && projection_scratch.fused[col_idx]) {
			continue;
		}
		SljitExecuteProjectionExpression(op.projections[col_idx], input, output.data[col_idx], execute_sel, count,
		                                 scratch.ExpressionAdapterScratch(op_idx, col_idx));
	}
	output.SetChildCardinality(count);
}

template <class SCRATCH>
static void SljitExecuteFilterProjection(SCRATCH &scratch, SljitExecutableRegionOp &filter_op,
                                         SljitExecutableRegionOp &projection_op, idx_t projection_op_idx,
                                         DataChunk &input, DataChunk &output, SelectionVector &filter_selection) {
	auto &filter_scratch = scratch.ExpressionAdapterScratch(projection_op_idx - 1, 0);
	auto selected_count = SljitSelectFilter(filter_op, input, filter_selection, filter_scratch);
	if (selected_count == 0) {
		output.Reset();
		return;
	}
	auto *execute_sel = selected_count == input.size() ? nullptr : &filter_selection;
	SljitExecuteProjection(scratch, projection_op_idx, projection_op, input, output, execute_sel, selected_count);
}

static bool SljitCanExecuteFilterProjection(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
	return op_idx + 1 < ops.size() && ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION;
}

template <class SCRATCH>
static bool SljitTryExecuteFullPipelineFilterProjection(ExecutionRegionRuntime &runtime, SCRATCH &scratch,
                                                        vector<SljitExecutableRegionOp> &ops, idx_t &op_idx,
                                                        DataChunk *&current, bool &needs_more_input) {
	if (!SljitCanExecuteFilterProjection(ops, op_idx)) {
		return false;
	}
	auto &output = scratch.TemporaryChunk(op_idx + 1);
	output.Reset();
	auto stage_start = SljitRegionStageStart(runtime);
	SljitExecuteFilterProjection(scratch, ops[op_idx], ops[op_idx + 1], op_idx + 1, *current, output,
	                             scratch.FilterSelection(op_idx));
	RecordSljitRegionStageRuntimeWithSuffix(runtime, op_idx, ops[op_idx].kind, "+projection", stage_start);
	current = &output;
	op_idx++;
	needs_more_input = current->size() == 0;
	return true;
}

template <class SCRATCH>
static bool SljitTryExecuteFullPipelineSingleOperatorTransform(ExecutionRegionRuntime &runtime, SCRATCH &scratch,
                                                               idx_t op_idx, SljitExecutableRegionOp &op,
                                                               DataChunk *&current, bool &needs_more_input) {
	if (op.kind != SljitNativeRegionOpKind::FILTER && op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return false;
	}

	auto &output = scratch.TemporaryChunk(op_idx);
	output.Reset();
	if (op.kind == SljitNativeRegionOpKind::FILTER) {
		auto stage_start = SljitRegionStageStart(runtime);
		SljitExecuteFilter(op, *current, output, scratch.FilterSelection(op_idx),
		                   scratch.ExpressionAdapterScratch(op_idx, 0));
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
	} else {
		auto stage_start = SljitRegionStageStart(runtime);
		SljitExecuteProjection(scratch, op_idx, op, *current, output);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
	}
	current = &output;
	needs_more_input = current->size() == 0;
	return true;
}

template <class SCRATCH>
static void SljitPrepareOptionalPreJoinProjectionInput(ExecutionRegionRuntime &runtime, SCRATCH &scratch,
                                                       idx_t pre_join_projection_idx,
                                                       SljitExecutableRegionOp &pre_join_projection_op,
                                                       DataChunk &source_chunk, bool bypass_pre_join_projection,
                                                       DataChunk *&join_input,
                                                       const char *reference_phase = "pre_join_reference_projection",
                                                       const char *batch_phase = "pre_join_batch_projection") {
	if (bypass_pre_join_projection) {
		join_input = &source_chunk;
		return;
	}

	auto &pre_join = scratch.TemporaryChunk(pre_join_projection_idx);
	pre_join.Reset();
	auto projection_stage_start = SljitRegionStageStart(runtime);
	if (SljitTryReferenceProjection(pre_join, source_chunk, pre_join_projection_op)) {
		RecordSljitRegionStageRuntime(runtime, pre_join_projection_idx, pre_join_projection_op.kind, reference_phase,
		                              projection_stage_start);
	} else {
		SljitExecuteProjection(scratch, pre_join_projection_idx, pre_join_projection_op, source_chunk, pre_join);
		RecordSljitRegionStageRuntime(runtime, pre_join_projection_idx, pre_join_projection_op.kind, batch_phase,
		                              projection_stage_start);
	}
	join_input = pre_join.size() == 0 ? nullptr : &pre_join;
}

static bool SljitTryExecuteProjectionExpressionToBatch(SljitExecutableRegionExpression &expr, DataChunk &input,
                                                       Vector &target, idx_t current_size, idx_t count,
                                                       const SelectionVector *execute_sel,
                                                       SljitExpressionAdapterScratch &adapter_scratch) {
	if (!SljitProjectionTargetCanReceiveExpression(target, expr.plan.return_type, current_size + count)) {
		return false;
	}
	if (DirectAppendSupportsFixedSizeType(target.GetType())) {
		auto target_data =
		    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
		Vector result(expr.plan.return_type, target_data, count);
		SljitExecuteProjectionExpression(expr, input, result, execute_sel, count, adapter_scratch);
		SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
		return true;
	}
	Vector result(expr.plan.return_type);
	SljitExecuteProjectionExpression(expr, input, result, execute_sel, count, adapter_scratch);
	target.Copy(result, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
	return true;
}

template <class SCRATCH>
static void SljitExecuteProjectionExpressionsToBatch(SCRATCH &scratch, idx_t projection_idx,
                                                     SljitExecutableRegionOp &projection_op, DataChunk &input,
                                                     DataChunk &batch, const vector<uint8_t> &skip_projection,
                                                     const vector<idx_t> &projection_to_output) {
	const auto current_size = batch.size();
	const auto count = input.size();
	for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
		if (projected_idx < skip_projection.size() && skip_projection[projected_idx]) {
			continue;
		}
		if (projected_idx >= projection_to_output.size() ||
		    projection_to_output[projected_idx] == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT direct batch projection target mapping is missing");
		}
		const auto output_idx = projection_to_output[projected_idx];
		auto &target = batch.data[output_idx];
		auto &projection = projection_op.projections[projected_idx];
		auto target_data =
		    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
		Vector result(projection.plan.return_type, target_data, count);
		SljitExecuteProjectionExpression(projection, input, result, nullptr, count,
		                                 scratch.ExpressionAdapterScratch(projection_idx, projected_idx));
		SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
	}
	SljitFinishDirectProjectionBatchTargets(batch, current_size + count, false);
}

template <class SCRATCH>
static bool SljitTryDirectMaterializeFixedProjectionToBatch(
    ExecutionRegionRuntime &runtime, SCRATCH &scratch, idx_t projection_idx, SljitExecutableRegionOp &projection_op,
    DataChunk &input, DataChunk &batch, optional_ptr<const vector<idx_t>> output_to_projection = nullptr,
    optional_ptr<const vector<uint8_t>> extra_skip_projection = nullptr,
    optional_ptr<Vector> projected_hashes = nullptr) {
	if (input.size() == 0) {
		return false;
	}

	const auto current_size = batch.size();
	DirectAppendSlice slice;
	slice.source_offset = 0;
	slice.count = input.size();
	vector<uint8_t> skip_projection;
	vector<idx_t> projection_to_output;
	if (!SljitPrepareDirectProjectionBatchTargets(batch, projection_op, output_to_projection, slice, skip_projection,
	                                              projection_to_output, extra_skip_projection)) {
		return false;
	}

	SljitFixedDirectProjectionSourceCache source_cache;
	source_cache.Reset(input.ColumnCount());
	auto source_cache_ptr = optional_ptr<SljitFixedDirectProjectionSourceCache>(&source_cache);
	const auto stage_start = SljitRegionStageStart(runtime);
	auto &projection_scratch = scratch.ProjectionScratch(projection_idx);
	const bool remapped_projection = output_to_projection;
	auto fallback_to_projection_executor = [&]() {
		SljitExecuteProjectionExpressionsToBatch(scratch, projection_idx, projection_op, input, batch, skip_projection,
		                                         projection_to_output);
		SljitFinishDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection,
		                                                batch, projected_hashes, input.size(), stage_start);
		return true;
	};
	auto preflight_direct_fixed_projection = [&](optional_ptr<vector<uint8_t>> skip_projection_ptr) {
		if (!SljitTryDirectMaterializeFixedProjection(projection_op, input, nullptr, source_cache_ptr,
		                                              skip_projection_ptr)) {
			return fallback_to_projection_executor();
		}
		return false;
	};
	auto materialize_direct_fixed_projection = [&](optional_ptr<vector<uint8_t>> skip_projection_ptr,
	                                               const char *shape_changed_message) {
		if (!SljitTryDirectMaterializeFixedProjection(projection_op, input, &slice, source_cache_ptr,
		                                              skip_projection_ptr,
		                                              optional_ptr<ExecutionRegionRuntime>(&runtime), projection_idx)) {
			throw InternalException(shape_changed_message);
		}
	};
	if (TryPrepareFlatFusedFixedProjectionSources(projection_op, input, slice.source_offset, slice.count,
	                                              source_cache_ptr, projection_scratch) &&
	    SljitFlatFusedFixedProjectionTargetsAreBound(projection_op, slice)) {
		vector<uint8_t> post_fused_skip;
		SljitBuildPostFusedProjectionSkip(skip_projection, projection_scratch.fused, post_fused_skip);
		auto post_fused_skip_ptr = optional_ptr<vector<uint8_t>>(&post_fused_skip);
		if (preflight_direct_fixed_projection(post_fused_skip_ptr)) {
			return true;
		}
		BindFlatFusedFixedProjectionTargets(projection_op, slice, projection_scratch);
		RunFlatFusedFixedProjection(projection_op, slice.count, projection_scratch);
		materialize_direct_fixed_projection(
		    post_fused_skip_ptr, "SLJIT fixed fused direct batch projection source shape changed after preflight");
	} else {
		auto skip_projection_ptr = optional_ptr<vector<uint8_t>>(&skip_projection);
		if (preflight_direct_fixed_projection(skip_projection_ptr)) {
			return true;
		}
		materialize_direct_fixed_projection(skip_projection_ptr,
		                                    "SLJIT fixed direct batch projection source shape changed after preflight");
	}

	SljitFinishDirectProjectionBatchTargets(batch, current_size + input.size());
	SljitFinishDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection, batch,
	                                                projected_hashes, input.size(), stage_start);
	return true;
}

template <class SCRATCH>
static bool SljitTryMaterializeSelectedProjectionToBatch(
    ExecutionRegionRuntime &runtime, SCRATCH &scratch, idx_t projection_idx, SljitExecutableRegionOp &projection_op,
    DataChunk &input, DataChunk &batch, const vector<idx_t> &output_to_projection,
    optional_ptr<Vector> projected_hashes = nullptr,
    optional_ptr<const vector<SljitDirectProjectionBatchPassthrough>> passthroughs = nullptr) {
	if (input.size() == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    batch.ColumnCount() != output_to_projection.size() || batch.size() + input.size() > STANDARD_VECTOR_SIZE) {
		return false;
	}
	const auto current_size = batch.size();
	const auto count = input.size();
	SljitFixedDirectProjectionSourceCache source_cache;
	source_cache.Reset(input.ColumnCount());
	auto source_cache_ptr = optional_ptr<SljitFixedDirectProjectionSourceCache>(&source_cache);
	auto stage_start = SljitRegionStageStart(runtime);
	bool all_direct_fixed = true;
	bool used_passthrough = false;
	for (idx_t output_idx = 0; output_idx < output_to_projection.size(); output_idx++) {
		const auto projected_idx = output_to_projection[output_idx];
		if (projected_idx >= projection_op.projections.size()) {
			return false;
		}
		auto &projection = projection_op.projections[projected_idx];
		auto &target = batch.data[output_idx];
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != projection.plan.return_type ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return false;
		}
		auto target_data =
		    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
		auto expression_stage_start =
		    runtime.TraceRuntime() ? SljitRegionStageStart(runtime) : std::chrono::steady_clock::time_point();
		auto passthrough = SljitFindDirectProjectionBatchPassthrough(passthroughs, output_idx);
		if (passthrough && SljitTryCopyDirectProjectionPassthroughToBatch(*passthrough, target, current_size, count)) {
			all_direct_fixed = false;
			used_passthrough = true;
			if (runtime.TraceRuntime()) {
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, passthrough->trace_phase,
				                              expression_stage_start);
			}
			continue;
		}
		if (DirectAppendSupportsFixedSizeType(target.GetType()) &&
		    TryDirectMaterializeFixedExpression(projection, input, target_data, 0, count, true, source_cache_ptr)) {
			if (runtime.TraceRuntime()) {
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
				                              SljitFixedProjectionExpressionTracePhase(projection.plan),
				                              expression_stage_start);
			}
			continue;
		}
		all_direct_fixed = false;
		Vector result(projection.plan.return_type, target_data, count);
		SljitExecuteProjectionExpression(projection, input, result, nullptr, count,
		                                 scratch.ExpressionAdapterScratch(projection_idx, projected_idx));
		SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
		if (runtime.TraceRuntime()) {
			RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
			                              SljitFixedProjectionExpressionTracePhase(projection.plan),
			                              expression_stage_start);
		}
	}
	SljitFinishDirectProjectionBatchTargets(batch, current_size + count, all_direct_fixed);
	SljitFinishDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, true, batch,
	                                                projected_hashes, count, stage_start, used_passthrough);
	return true;
}

} // namespace duckdb

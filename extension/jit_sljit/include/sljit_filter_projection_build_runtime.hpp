//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_filter_projection_build_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_reference_projection_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

struct SljitDeferredPreProjectionFilterBuildShape {
	vector<idx_t> filter_identity_sources;
	vector<idx_t> output_to_pre_projection;
};

static bool SljitAddExecutableExpressionSourceColumns(const SljitExecutableRegionExpression &expression,
                                                      idx_t input_column_count, vector<uint8_t> &referenced) {
	referenced.assign(input_column_count, 0);
	auto add_local_source = [&](idx_t local_source_index) {
		auto source_index = local_source_index;
		if (!expression.input_source_indices.empty()) {
			if (local_source_index >= expression.input_source_indices.size()) {
				return false;
			}
			source_index = expression.input_source_indices[local_source_index];
		}
		return SljitAddProjectionSourceColumn(source_index, input_column_count, referenced);
	};
	auto add_local_sources = [&](const vector<idx_t> &local_source_indices) {
		for (auto local_source_index : local_source_indices) {
			if (!add_local_source(local_source_index)) {
				return false;
			}
		}
		return true;
	};
	auto accept_constant = [&](SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto add_predicate_sources = [&](SljitNativeRegionExpressionPlan &predicate_plan) {
		if (!predicate_plan.predicate) {
			return add_local_sources(predicate_plan.expression_tree_source_indices);
		}
		auto add_source_indices = [&](const SljitNativePredicate &predicate) {
			return add_local_sources(predicate.source_indices);
		};
		auto ignore_guard_sources = [&](const SljitNativePredicate &) {
			return true;
		};
		return SljitTryApplyProjectionPredicateSources(*predicate_plan.predicate, add_local_source, add_source_indices,
		                                               ignore_guard_sources);
	};
	auto plan = expression.plan.Copy(true, false);
	return SljitTryApplyProjectionPlanSources(plan, add_local_source, add_local_sources, accept_constant,
	                                          add_predicate_sources);
}

static bool SljitTryBuildDeferredPreProjectionFilterBuildShape(const SljitExecutableRegionOp &pre_projection_op,
                                                               const SljitExecutableRegionOp &filter_op,
                                                               const SljitExecutableRegionOp &build_projection_op,
                                                               SljitDeferredPreProjectionFilterBuildShape &shape) {
	shape = SljitDeferredPreProjectionFilterBuildShape();
	if (pre_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    filter_op.kind != SljitNativeRegionOpKind::FILTER ||
	    build_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    pre_projection_op.projections.size() != pre_projection_op.output_types.size() ||
	    build_projection_op.projections.size() != build_projection_op.output_types.size()) {
		return false;
	}

	vector<uint8_t> filter_sources;
	if (!SljitAddExecutableExpressionSourceColumns(filter_op.filter, pre_projection_op.output_types.size(),
	                                               filter_sources)) {
		return false;
	}
	for (idx_t source_idx = 0; source_idx < filter_sources.size(); source_idx++) {
		if (!filter_sources[source_idx]) {
			continue;
		}
		if (source_idx >= pre_projection_op.projections.size()) {
			return false;
		}
		idx_t original_source_idx;
		auto &pre_projection = pre_projection_op.projections[source_idx];
		if (!SljitTryReadDirectReferenceProjectionSource(pre_projection, original_source_idx) ||
		    original_source_idx != source_idx ||
		    pre_projection.plan.return_type != pre_projection_op.output_types[source_idx]) {
			return false;
		}
		shape.filter_identity_sources.push_back(source_idx);
	}

	shape.output_to_pre_projection.resize(build_projection_op.projections.size());
	for (idx_t output_idx = 0; output_idx < build_projection_op.projections.size(); output_idx++) {
		idx_t pre_projection_idx;
		if (!SljitTryReadDirectReferenceProjectionSource(build_projection_op.projections[output_idx],
		                                                 pre_projection_idx) ||
		    pre_projection_idx >= pre_projection_op.projections.size() ||
		    pre_projection_op.projections[pre_projection_idx].plan.return_type !=
		        build_projection_op.output_types[output_idx] ||
		    !DirectAppendSupportsFixedSizeType(build_projection_op.output_types[output_idx])) {
			return false;
		}
		shape.output_to_pre_projection[output_idx] = pre_projection_idx;
	}
	return true;
}

template <class FLUSH_FILTERED_BATCH>
static bool SljitTryAppendDeferredPreProjectionFilterBuildBatch(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
    const SljitDeferredPreProjectionFilterBuildShape &shape, idx_t pre_projection_idx,
    SljitExecutableRegionOp &pre_projection_op, idx_t filter_idx, SljitExecutableRegionOp &filter_op,
    SljitExecutableRegionOp &build_projection_op, SljitExecutableRegionOp &hash_join_op, DataChunk &source_chunk,
    SljitDataChunkBatch &filtered_batch, FLUSH_FILTERED_BATCH &&flush_filtered_batch, bool &stopped) {
	stopped = false;
	if (shape.output_to_pre_projection.empty()) {
		return false;
	}
	for (auto source_idx : shape.filter_identity_sources) {
		if (source_idx >= source_chunk.ColumnCount() ||
		    source_chunk.data[source_idx].GetType() != pre_projection_op.output_types[source_idx]) {
			return false;
		}
	}
	filtered_batch.Ensure(runtime.GetAllocator(), build_projection_op.output_types);
	auto &batch = filtered_batch.chunk;
	if (batch.ColumnCount() != build_projection_op.output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < build_projection_op.output_types.size(); output_idx++) {
		auto &target = batch.data[output_idx];
		if (target.GetType() != build_projection_op.output_types[output_idx] ||
		    target.GetVectorType() != VectorType::FLAT_VECTOR ||
		    FlatVector::GetCapacity(target) < STANDARD_VECTOR_SIZE) {
			return false;
		}
	}

	auto &filter_selection = scratch.FilterSelection(filter_idx);
	auto &filter_scratch = scratch.ExpressionAdapterScratch(filter_idx, 0);
	auto filter_stage_start = SljitRegionStageStart(runtime);
	auto selected_count = SljitSelectFilter(filter_op, source_chunk, filter_selection, filter_scratch);
	RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+source_selection",
	                                        filter_stage_start);
	if (selected_count == 0) {
		return true;
	}

	idx_t appended_count = 0;
	idx_t source_offset = 0;
	auto projection_stage_start = SljitRegionStageStart(runtime);
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
		SelectionVector selected_slice(filter_selection.data() + source_offset, append_count);
		for (idx_t output_idx = 0; output_idx < shape.output_to_pre_projection.size(); output_idx++) {
			const auto projected_idx = shape.output_to_pre_projection[output_idx];
			auto &projection = pre_projection_op.projections[projected_idx];
			auto &target = batch.data[output_idx];
			auto target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			Vector result(projection.plan.return_type, target_data, append_count);
			SljitExecuteProjectionExpression(projection, source_chunk, result, &selected_slice, append_count,
			                                 scratch.ExpressionAdapterScratch(pre_projection_idx, projected_idx));
			SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, append_count);
		}
		SljitFinishDirectProjectionBatchTargets(batch, current_size + append_count, false);
		source_offset += append_count;
		appended_count += append_count;
		if (batch.size() == STANDARD_VECTOR_SIZE && flush_filtered_batch()) {
			stopped = true;
			return true;
		}
	}
	RecordSljitRegionStageRuntimeWithSuffix(runtime, pre_projection_idx, pre_projection_op.kind,
	                                        "+deferred_selected_projection", projection_stage_start);
	RecordSljitRegionRuntimePath(runtime, pre_projection_op.kind, "deferred_selected_pre_projection", appended_count);
	RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "filtered_input_batch", appended_count);
	return true;
}

template <class FLUSH_FILTERED_BATCH, class APPEND_FILTERED_BATCH>
static bool SljitAppendFilterProjectionBuildBatch(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                  idx_t filter_idx, SljitExecutableRegionOp &filter_op,
                                                  idx_t projection_idx, SljitExecutableRegionOp &projection_op,
                                                  SljitExecutableRegionOp &hash_join_op, DataChunk &filter_input,
                                                  SljitDataChunkBatch &filtered_batch,
                                                  FLUSH_FILTERED_BATCH &&flush_filtered_batch,
                                                  APPEND_FILTERED_BATCH &&append_filtered_batch, bool &stopped) {
	stopped = false;
	bool direct_stopped = false;
	if (SljitTryAppendSelectedReferenceProjectionBatch(runtime, scratch, filter_idx, filter_op, projection_op,
	                                                   hash_join_op, filter_input, filtered_batch, flush_filtered_batch,
	                                                   direct_stopped)) {
		stopped = direct_stopped;
		return true;
	}
	auto &filtered = scratch.TemporaryChunk(projection_idx);
	filtered.Reset();
	auto filter_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteFilterProjection(scratch, filter_op, projection_op, projection_idx, filter_input, filtered,
	                             scratch.FilterSelection(filter_idx));
	RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+projection", filter_stage_start);
	stopped = append_filtered_batch(filtered);
	return true;
}

template <class APPEND_SOURCE_CHUNK, class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitRunFullPipelineFilterProjectionHashJoinBuildSinkBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    idx_t hash_join_idx, const vector<LogicalType> &filtered_batch_types, APPEND_SOURCE_CHUNK &&append_source_chunk,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	auto &hash_join_op = ops[hash_join_idx];
	SljitRouteChunkBatch filtered_batch(runtime, hash_join_idx,
	                                    optional_ptr<const SljitExecutableRegionOp>(&hash_join_op),
	                                    "filtered_input_batch_append", "filtered_input_batch");

	auto execute_filtered_batch = [&](DataChunk &input) -> bool {
		if (input.size() == 0) {
			return false;
		}
		auto sink_result = execute_native_full_pipeline_from(scratch, hash_join_idx, input);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	};

	auto flush_filtered_batch = [&]() -> bool {
		return filtered_batch.Flush(execute_filtered_batch);
	};

	auto append_filtered_batch = [&](DataChunk &filtered) -> bool {
		return filtered_batch.Append(filtered, filtered_batch_types, execute_filtered_batch);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::INTERRUPTED, flush_filtered_batch);
		}
		if (source_chunk.size() == 0) {
			return false;
		}
		return append_source_chunk(scratch, source_chunk, filtered_batch.Batch(), flush_filtered_batch,
		                           append_filtered_batch);
	};

	return SljitRunFullPipelineSourceContractLoopAfterFlush(
	    runtime, result, fetched_chunks,
	    [&]() { return processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches; },
	    execute_source_chunk, flush_filtered_batch);
}

template <class PREPARE_FILTER_INPUT, class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineFilterProjectionHashJoinBuildSinkBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    idx_t filter_idx, idx_t projection_idx, idx_t hash_join_idx, PREPARE_FILTER_INPUT &&prepare_filter_input,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	auto &filter_op = ops[filter_idx];
	auto &projection_op = ops[projection_idx];
	auto &hash_join_op = ops[hash_join_idx];
	auto append_source_chunk = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
	                               SljitDataChunkBatch &filtered_batch, auto &flush_filtered_batch,
	                               auto &append_filtered_batch) -> bool {
		DataChunk *filter_input = nullptr;
		if (prepare_filter_input(scratch, source_chunk, filter_input)) {
			return true;
		}
		if (!filter_input || filter_input->size() == 0) {
			return false;
		}
		bool stopped = false;
		SljitAppendFilterProjectionBuildBatch(runtime, scratch, filter_idx, filter_op, projection_idx, projection_op,
		                                      hash_join_op, *filter_input, filtered_batch, flush_filtered_batch,
		                                      append_filtered_batch, stopped);
		return stopped;
	};
	return SljitRunFullPipelineFilterProjectionHashJoinBuildSinkBatched(runtime, result, ops, hash_join_idx,
	                                                                    projection_op.output_types, append_source_chunk,
	                                                                    execute_native_full_pipeline_from);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineDeferredPreProjectionFilterProjectionHashJoinBuildSinkBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    idx_t pre_projection_idx, idx_t filter_idx, idx_t build_projection_idx, idx_t hash_join_idx,
    const SljitDeferredPreProjectionFilterBuildShape &shape,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	auto &pre_projection_op = ops[pre_projection_idx];
	auto &filter_op = ops[filter_idx];
	auto &build_projection_op = ops[build_projection_idx];
	auto &hash_join_op = ops[hash_join_idx];
	auto append_source_chunk = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
	                               SljitDataChunkBatch &filtered_batch, auto &flush_filtered_batch,
	                               auto &append_filtered_batch) -> bool {
		bool stopped = false;
		if (SljitTryAppendDeferredPreProjectionFilterBuildBatch(
		        runtime, scratch, shape, pre_projection_idx, pre_projection_op, filter_idx, filter_op,
		        build_projection_op, hash_join_op, source_chunk, filtered_batch, flush_filtered_batch, stopped)) {
			return stopped;
		}

		auto &projected = scratch.TemporaryChunk(pre_projection_idx);
		projected.Reset();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryReferenceProjection(projected, source_chunk, pre_projection_op)) {
			RecordSljitRegionStageRuntime(runtime, pre_projection_idx, pre_projection_op.kind, "reference_projection",
			                              projection_stage_start);
		} else {
			SljitExecuteProjection(scratch, pre_projection_idx, pre_projection_op, source_chunk, projected);
			RecordSljitRegionStageRuntime(runtime, pre_projection_idx, pre_projection_op.kind, "batch_projection",
			                              projection_stage_start);
		}
		if (projected.size() == 0) {
			return false;
		}
		SljitAppendFilterProjectionBuildBatch(runtime, scratch, filter_idx, filter_op, build_projection_idx,
		                                      build_projection_op, hash_join_op, projected, filtered_batch,
		                                      flush_filtered_batch, append_filtered_batch, stopped);
		return stopped;
	};
	return SljitRunFullPipelineFilterProjectionHashJoinBuildSinkBatched(
	    runtime, result, ops, hash_join_idx, build_projection_op.output_types, append_source_chunk,
	    execute_native_full_pipeline_from);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineGeneratedFilterProjectionHashJoinBuildSinkBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	static constexpr idx_t FILTER_IDX = 0;
	static constexpr idx_t PROJECTION_IDX = 1;
	static constexpr idx_t HASH_JOIN_IDX = 2;
	auto prepare_filter_input = [](SljitRegionExecutionScratch &, DataChunk &source_chunk,
	                               DataChunk *&filter_input) -> bool {
		filter_input = &source_chunk;
		return false;
	};
	return SljitTryExecuteFullPipelineFilterProjectionHashJoinBuildSinkBatched(
	    runtime, result, ops, FILTER_IDX, PROJECTION_IDX, HASH_JOIN_IDX, prepare_filter_input,
	    execute_native_full_pipeline_from);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineGeneratedProjectionFilterProjectionHashJoinBuildSinkBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	static constexpr idx_t PROJECTION_IDX = 0;
	static constexpr idx_t FILTER_IDX = 1;
	static constexpr idx_t FILTERED_PROJECTION_IDX = 2;
	static constexpr idx_t HASH_JOIN_IDX = 3;
	auto &projection_op = ops[PROJECTION_IDX];
	SljitDeferredPreProjectionFilterBuildShape deferred_shape;
	if (SljitTryBuildDeferredPreProjectionFilterBuildShape(ops[PROJECTION_IDX], ops[FILTER_IDX],
	                                                       ops[FILTERED_PROJECTION_IDX], deferred_shape)) {
		return SljitTryExecuteFullPipelineDeferredPreProjectionFilterProjectionHashJoinBuildSinkBatched(
		    runtime, result, ops, PROJECTION_IDX, FILTER_IDX, FILTERED_PROJECTION_IDX, HASH_JOIN_IDX, deferred_shape,
		    execute_native_full_pipeline_from);
	}
	auto prepare_filter_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
	                                DataChunk *&filter_input) -> bool {
		auto &projected = scratch.TemporaryChunk(PROJECTION_IDX);
		projected.Reset();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryReferenceProjection(projected, source_chunk, projection_op)) {
			RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, "reference_projection",
			                              projection_stage_start);
		} else {
			SljitExecuteProjection(scratch, PROJECTION_IDX, projection_op, source_chunk, projected);
			RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, "batch_projection",
			                              projection_stage_start);
		}
		filter_input = projected.size() == 0 ? nullptr : &projected;
		return false;
	};
	return SljitTryExecuteFullPipelineFilterProjectionHashJoinBuildSinkBatched(
	    runtime, result, ops, FILTER_IDX, FILTERED_PROJECTION_IDX, HASH_JOIN_IDX, prepare_filter_input,
	    execute_native_full_pipeline_from);
}

} // namespace duckdb

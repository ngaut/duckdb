//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_distinct_key_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_distinct_key_aggregate_update_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_primitive.hpp"
#include "sljit_projected_aggregate_input_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

static bool SljitTryBuildDistinctKeyReferenceView(DataChunk &view, DataChunk &source,
                                                  const SljitExecutableRegionOp &projection_op) {
	if (view.ColumnCount() != projection_op.output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < projection_op.projections.size(); output_idx++) {
		idx_t source_idx;
		if (!SljitTryGetSingleSourceReferenceProjectionIndex(projection_op.projections[output_idx], source_idx) ||
		    source_idx >= source.ColumnCount() ||
		    source.data[source_idx].GetType() != projection_op.output_types[output_idx]) {
			return false;
		}
		view.data[output_idx].Reference(source.data[source_idx]);
	}
	view.SetChildCardinality(source.size());
	return true;
}

class SljitGroupedDistinctKeySinkRuntimeState {
public:
	bool Prepare(const SljitGroupedAggregateUpdatePrimitive &primitive) {
		if (primitive.strategy != SljitGroupedAggregateUpdateStrategyKind::DISTINCT_KEY_SINK) {
			return false;
		}
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			projected_input_projection = primitive.projected_distinct_key_input_projection;
			return projected_input_projection == nullptr ||
			       projected_input_projection->kind == SljitNativeRegionOpKind::PROJECTION;
		}
		return true;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitGroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (primitive.input_kind == SljitGroupedAggregateUpdateInputKind::PROJECTED_INPUT) {
			return ExecuteProjected(runtime, result, ops, scratch, primitive, input, processed_batches);
		}
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT grouped distinct key sink");
		return ExecuteMaterialized(runtime, result, ops, scratch, primitive, input_chunk, processed_batches);
	}

private:
	bool ExecuteMaterialized(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                         vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                         const SljitGroupedAggregateUpdatePrimitive &primitive, DataChunk &input,
	                         idx_t &processed_batches) {
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		auto sink_result = SljitExecuteDistinctKeyAggregateUpdate(runtime, native_runtime, scratch,
		                                                          primitive.aggregate_idx, aggregate_op, input);
		sink_result = native_runtime.RecordSinkResult(input, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

	bool ExecuteProjected(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                      vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                      const SljitGroupedAggregateUpdatePrimitive &primitive, const SljitRuntimeBatchView &input,
	                      idx_t &processed_batches) {
		optional_ptr<SljitExecutableRegionOp> projection_op;
		if (projected_input_projection) {
			projection_op = projected_input_projection.get();
		} else {
			if (primitive.final_projection_idx >= ops.size()) {
				throw InternalException("SLJIT projected distinct key sink has an invalid projection index");
			}
			projection_op = &ops[primitive.final_projection_idx];
		}

		SljitPreparedProjectedAggregateInput prepared;
		if (!SljitPrepareProjectedAggregateInput(runtime, ops, scratch, primitive, input, *projection_op,
		                                         selected_hash_join_input, "SLJIT projected grouped distinct key sink",
		                                         prepared)) {
			return true;
		}
		if (prepared.count == 0) {
			processed_batches++;
			return false;
		}

		projected_input.Ensure(runtime.GetAllocator(), prepared.projection_op->output_types);
		auto &projected = projected_input.chunk;
		projected.Reset();
		if ((prepared.selection || prepared.count == prepared.source_chunk->size()) &&
		    SljitTryBuildDistinctKeyReferenceView(projected, *prepared.source_chunk, *prepared.projection_op)) {
			auto &aggregate_op = ops[primitive.aggregate_idx];
			auto &native_runtime = runtime.ExecutionOperators();
			auto sink_result =
			    SljitExecuteDistinctKeyAggregateUpdate(runtime, native_runtime, scratch, primitive.aggregate_idx,
			                                           aggregate_op, projected, prepared.selection, prepared.count);
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "projected_distinct_key_reference_sink",
			                             prepared.count);
			sink_result = native_runtime.RecordSinkResult(prepared.count, sink_result);
			if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
				return true;
			}
			processed_batches++;
			return false;
		}

		auto project_stage_start = SljitRegionStageStart(runtime);
		SljitExecuteProjection(scratch, primitive.final_projection_idx, *prepared.projection_op, *prepared.source_chunk,
		                       projected, prepared.selection, prepared.count);
		RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, prepared.projection_op->kind,
		                              "distinct_key_project", project_stage_start);

		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		auto sink_result = SljitExecuteDistinctKeyAggregateUpdate(runtime, native_runtime, scratch,
		                                                          primitive.aggregate_idx, aggregate_op, projected);
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "projected_distinct_key_sink", projected.size());
		sink_result = native_runtime.RecordSinkResult(projected, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

private:
	shared_ptr<SljitExecutableRegionOp> projected_input_projection;
	SljitDataChunkBatch projected_input;
	SljitDataChunkBatch selected_hash_join_input;
};

} // namespace duckdb

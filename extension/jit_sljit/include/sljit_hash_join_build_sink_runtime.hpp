//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_build_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_native_sink_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

struct SljitHashJoinBuildSinkRuntimeState {
	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitHashJoinBuildSinkPrimitive &primitive,
	             const SljitRuntimeBatchView &input) {
		if (input.count == 0) {
			return false;
		}
		const auto sink_idx = primitive.sink_idx;
		auto &sink_op = ops[sink_idx];
		DataChunk *sink_input = nullptr;
		if (!TryBindSelectedHashJoinBuildInput(runtime, ops, scratch, sink_op, primitive, input, sink_input)) {
			if (input.selection) {
				auto &source = SljitBindRuntimeBatchInput(input, "SLJIT selected hash join build sink primitive");
				selected_source_input.Ensure(runtime.GetAllocator(), source.GetTypes());
				auto &selected = selected_source_input.chunk;
				selected.Reset();
				selected.Slice(source, *input.selection, input.count);
				sink_input = &selected;
				RecordSljitRegionRuntimePath(runtime, sink_op.Kind(), "selected_source_view", sink_input->size());
			} else {
				sink_input = &SljitBindMaterializedRuntimeBatchInput(input, "SLJIT hash join build sink primitive");
				RecordSljitRegionRuntimePath(runtime, sink_op.Kind(), "runtime_batch_input", sink_input->size());
			}
		}
		auto &native_runtime = runtime.ExecutionOperators();
		auto build_result = SljitExecuteNativeHashJoinBuild(runtime, native_runtime, scratch, sink_idx, sink_op,
		                                                    *sink_input, scratch.HashJoinBuildSourceChunk(sink_idx),
		                                                    scratch.HashJoinBuildHashValues(sink_idx),
		                                                    scratch.HashJoinBuildSelection(sink_idx));
		auto sink_result = native_runtime.RecordSinkResult(sink_input->size(), build_result);
		if (primitive.direct_source_ingress) {
			RecordSljitRegionRuntimePath(runtime, sink_op.Kind(), "direct_source_ingress", sink_input->size());
			RecordSljitRegionBackendWork(runtime, sink_input->size());
		}
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		return false;
	}

private:
	SljitDataChunkBatch selected_source_input;

	bool TryBindSelectedHashJoinBuildInput(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                       SljitRegionExecutionScratch &scratch, SljitExecutableRegionOp &sink_op,
	                                       const SljitHashJoinBuildSinkPrimitive &primitive,
	                                       const SljitRuntimeBatchView &input, DataChunk *&sink_input) {
		SljitRuntimeHashJoinSelection selected;
		if (!input.TryGetHashJoinSelection(selected)) {
			return false;
		}
		auto hash_join_idx = selected.hash_join_idx;
		if (hash_join_idx >= ops.size()) {
			throw InternalException("SLJIT hash join build sink received an invalid selected producer");
		}
		auto &hash_join_op = ops[hash_join_idx];
		auto source = SljitBindSelectedHashJoinOutputSource(runtime, ops, scratch, selected);
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		vector<uint8_t> required_columns;
		if (primitive.HasProjection()) {
			return TryBindProjectedSelectedHashJoinBuildInput(runtime, ops, scratch, sink_op, primitive, selected,
			                                                  hash_join_op, source, join_output, required_columns,
			                                                  sink_input);
		}
		if (!SljitBuildHashJoinBuildRequiredInputColumns(sink_op.HashJoinBuild().plan.sink_info,
		                                                 hash_join_op.output_types.size(), required_columns)) {
			throw InternalException("SLJIT hash join build sink could not bind required input columns");
		}
		join_output.Reset();
		if (SljitTryMaterializeHashJoinRequiredSources(
		        runtime, hash_join_idx, hash_join_op, source, selected.MatchSelection(), selected.BuildSelection(),
		        selected.RowPointers(), selected.count, required_columns, join_output)) {
			RecordSljitRegionRuntimePath(runtime, sink_op.Kind(), "selected_required_sources", join_output.size());
			sink_input = &join_output;
			return true;
		}
		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, hash_join_idx, hash_join_op, source,
		                                                      selected.MatchSelection(), selected.BuildSelection(),
		                                                      selected.RowPointers(), selected.count, join_output)) {
			throw InternalException("SLJIT hash join build sink could not materialize selected hash join input");
		}
		RecordSljitRegionRuntimePath(runtime, sink_op.Kind(), "selected_full_output", join_output.size());
		sink_input = &join_output;
		return true;
	}

	bool TryBindProjectedSelectedHashJoinBuildInput(
	    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	    SljitExecutableRegionOp &sink_op, const SljitHashJoinBuildSinkPrimitive &primitive,
	    SljitRuntimeHashJoinSelection &selected, SljitExecutableRegionOp &hash_join_op,
	    const SljitHashJoinOutputSource &source, DataChunk &join_output, vector<uint8_t> &required_columns,
	    DataChunk *&sink_input) {
		const auto projection_idx = primitive.projection_idx;
		auto &projection_op = ops[projection_idx];
		auto &projected = scratch.TemporaryChunk(projection_idx);
		projected.Reset();
		if (!SljitBuildHashJoinBuildRequiredInputColumns(sink_op.HashJoinBuild().plan.sink_info,
		                                                 projection_op.output_types.size(), required_columns)) {
			throw InternalException("SLJIT hash join build sink could not bind projected required columns");
		}
		if (SljitTryMaterializeHashJoinRequiredProjectionViews(
		        runtime, projection_idx, projection_op, source, selected.MatchSelection(), selected.BuildSelection(),
		        selected.RowPointers(), selected.count, required_columns, projected)) {
			RecordSljitRegionRuntimePath(runtime, hash_join_op.Kind(), "projected_hash_build_views", projected.size());
			sink_input = &projected;
			return true;
		}
		join_output.Reset();
		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, selected.hash_join_idx, hash_join_op, source,
		                                                      selected.MatchSelection(), selected.BuildSelection(),
		                                                      selected.RowPointers(), selected.count, join_output)) {
			throw InternalException("SLJIT hash join build sink could not materialize selected hash join input");
		}
		projected.Reset();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		SljitExecuteProjection(runtime, scratch, projection_idx, projection_op, join_output, projected);
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "hash_build_projection",
		                              projection_stage_start);
		RecordSljitRegionRuntimePath(runtime, hash_join_op.Kind(), "projected_hash_build_full_output",
		                             projected.size());
		sink_input = &projected;
		return true;
	}
};

} // namespace duckdb

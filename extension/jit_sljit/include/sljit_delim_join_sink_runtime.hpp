//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_delim_join_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_delim_join_sink_primitive.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_projection_reference_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

struct SljitDelimJoinSinkRuntimeState {
	SljitDataChunkBatch projected_input;
	SljitDataChunkBatch selected_sink_batch;
	vector<uint8_t> selected_hash_join_referenced_columns;

	bool Prepare(ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops,
	             const SljitDelimJoinSinkPrimitive &primitive) {
		if (!SljitCanBindDelimJoinSinkPrimitive(ops, primitive)) {
			return false;
		}
		if (primitive.HasProjection()) {
			projected_input.Ensure(runtime.GetAllocator(), primitive.bound_projection->output_types);
		}
		if (primitive.HasSelectedHashJoinInput()) {
			auto &sink_input_types = ops[primitive.sink_idx].delim_join_sink.plan.input_types;
			projected_input.Ensure(runtime.GetAllocator(), sink_input_types);
			selected_hash_join_referenced_columns.assign(sink_input_types.size(), 1);
		}
		return true;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitDelimJoinSinkPrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		auto &sink_input = SinkInput(runtime, ops, scratch, primitive, input);
		if (sink_input.size() == 0) {
			return false;
		}
		if (primitive.HasSelectedHashJoinInput()) {
			return AppendSelectedSinkInput(runtime, result, ops, scratch, primitive, sink_input, processed_batches);
		}
		return ExecuteSinkInput(runtime, result, ops, scratch, primitive, sink_input, processed_batches);
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitDelimJoinSinkPrimitive &primitive,
	           idx_t &processed_batches) {
		auto execute_batch = [&](DataChunk &batch) {
			return ExecuteSinkInput(runtime, result, ops, scratch, primitive, batch, processed_batches);
		};
		return SljitFlushDataChunkBatch(selected_sink_batch.chunk, execute_batch);
	}

private:
	bool AppendSelectedSinkInput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                             vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                             const SljitDelimJoinSinkPrimitive &primitive, DataChunk &sink_input,
	                             idx_t &processed_batches) {
		selected_sink_batch.EnsureFromChunk(runtime.GetAllocator(), sink_input);
		auto execute_batch = [&](DataChunk &batch) {
			return ExecuteSinkInput(runtime, result, ops, scratch, primitive, batch, processed_batches);
		};
		auto flush_batch = [&]() {
			return Flush(runtime, result, ops, scratch, primitive, processed_batches);
		};
		return SljitAppendChunkToInitializedBatch(runtime, selected_sink_batch.chunk, sink_input, primitive.sink_idx,
		                                          optional_ptr<const SljitExecutableRegionOp>(&ops[primitive.sink_idx]),
		                                          "selected_sink_batch_append", flush_batch, execute_batch);
	}

	bool ExecuteSinkInput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                      vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                      const SljitDelimJoinSinkPrimitive &primitive, DataChunk &sink_input,
	                      idx_t &processed_batches) {
		auto sink_result = ExecuteSink(runtime, ops, scratch, primitive.sink_idx, sink_input);
		sink_result = runtime.ExecutionOperators().RecordSinkResult(sink_input, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}
	DataChunk &SinkInput(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                     SljitRegionExecutionScratch &scratch, const SljitDelimJoinSinkPrimitive &primitive,
	                     const SljitRuntimeBatchView &input) {
		if (primitive.HasProjection()) {
			return ProjectInput(runtime, primitive, input);
		}
		if (primitive.HasSelectedHashJoinInput()) {
			return MaterializeSelectedHashJoinInput(runtime, ops, scratch, primitive, input);
		}
		return SljitBindMaterializedRuntimeBatchInput(input, "SLJIT delimiter join sink");
	}

	DataChunk &MaterializeSelectedHashJoinInput(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                            SljitRegionExecutionScratch &scratch,
	                                            const SljitDelimJoinSinkPrimitive &primitive,
	                                            const SljitRuntimeBatchView &input) {
		auto selected = input.BindHashJoinSelection("SLJIT delimiter join sink");
		if (selected.hash_join_idx != primitive.selected_hash_join_idx) {
			throw InternalException("SLJIT delimiter join sink expected selected hash-join input");
		}
		if (!scratch.HasOperatorBinding(selected.hash_join_idx)) {
			throw InternalException("SLJIT delimiter join sink selected input has no hash-join binding");
		}
		auto &binding = scratch.OperatorBinding(selected.hash_join_idx).hash_join_probe;
		auto &output = projected_input.chunk;
		if (!binding.ready || binding.output_types != ops[primitive.sink_idx].delim_join_sink.plan.input_types ||
		    output.ColumnCount() != binding.output_types.size() ||
		    selected_hash_join_referenced_columns.size() != binding.output_types.size()) {
			throw InternalException("SLJIT delimiter join sink selected input contract mismatch");
		}
		output.Reset();
		auto view_stage_start = SljitRegionStageStart(runtime);
		const bool built_view = SljitTryBuildSelectedHashJoinOutputColumnViews(
		    binding, input, selected_hash_join_referenced_columns, output);
		if (!built_view) {
			output.Reset();
			if (!SljitTryMaterializeSelectedHashJoinOutputColumns(binding, input, selected_hash_join_referenced_columns,
			                                                      output)) {
				throw InternalException("SLJIT delimiter join sink could not build selected hash-join input");
			}
		}
		const char *view_stage = built_view ? "selected_delim_view" : "selected_delim_materialization";
		RecordSljitRegionStageRuntime(runtime, selected.hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE,
		                              view_stage, view_stage_start);
		if (!built_view) {
			RecordSljitRegionRuntimeDelegation(runtime, SljitNativeRegionOpKind::DELIM_JOIN_SINK,
			                                   "selected_delim_materialization", output.size());
		}
		RecordSljitRegionRuntimePath(
		    runtime, SljitNativeRegionOpKind::DELIM_JOIN_SINK,
		    built_view ? "hash_join_selected_delim_view" : "hash_join_selected_delim_materialized", output.size());
		return output;
	}

	DataChunk &ProjectInput(ExecutionRegionRuntime &runtime, const SljitDelimJoinSinkPrimitive &primitive,
	                        const SljitRuntimeBatchView &input) {
		if (!primitive.bound_projection || !SljitProjectionIsReferencePreserving(*primitive.bound_projection)) {
			throw InternalException("SLJIT delimiter join sink projection contract is not reference-preserving");
		}
		auto &source = SljitBindRuntimeBatchInput(input, "SLJIT delimiter join sink projection");
		auto &output = projected_input.chunk;
		output.Reset();
		auto project_stage_start = SljitRegionStageStart(runtime);
		if (!SljitTrySliceReferenceProjection(output, source, *primitive.bound_projection, input.selection,
		                                      input.count)) {
			throw InternalException("SLJIT delimiter join sink could not slice its reference projection");
		}
		RecordSljitRegionStageRuntime(runtime, primitive.final_projection_idx, SljitNativeRegionOpKind::PROJECTION,
		                              "reference_slice", project_stage_start);
		RecordSljitRegionRuntimePath(runtime, SljitNativeRegionOpKind::DELIM_JOIN_SINK,
		                             "reference_projection_delim_sink", output.size());
		return output;
	}

	SinkResultType ExecuteSink(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                           SljitRegionExecutionScratch &scratch, idx_t sink_idx, DataChunk &input) {
		auto &op = ops[sink_idx];
		auto sink_stage_start = SljitRegionStageStart(runtime);
		auto &binding = SljitBindRecordedNativeSink(
		    runtime, runtime.ExecutionOperators(), scratch, sink_idx, op.kind, input, op.delim_join_sink.plan.sink_info,
		    "delim-join-sink-runtime-binding-failed", "SLJIT delimiter join sink primitive");
		if (!binding.ready || !binding.delim_join_sink.ready) {
			throw InternalException("SLJIT delimiter join sink primitive received an invalid binding");
		}
		auto result = ExecutionSinkDelimJoin(binding.delim_join_sink, input);
		RecordSljitRegionStageRuntime(runtime, sink_idx, op.kind, "sink_update", sink_stage_start);
		return result;
	}
};

} // namespace duckdb

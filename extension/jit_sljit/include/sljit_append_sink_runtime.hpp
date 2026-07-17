//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_append_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_append_sink_primitive.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

class SljitAppendSinkRuntimeState {
public:
	void Prepare(ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops,
	             const SljitAppendSinkPrimitive &primitive) {
		auto &input_types = ops[primitive.sink_idx].append_sink.plan.input_types;
		selected_input.Ensure(runtime.GetAllocator(), input_types);
		referenced_columns.assign(input_types.size(), 1);
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitAppendSinkPrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		auto selected = input.BindHashJoinSelection("SLJIT append sink primitive");
		if (selected.hash_join_idx != primitive.selected_hash_join_idx || selected.output_column_map) {
			throw InternalException("SLJIT append sink primitive received an incompatible selected hash-join input");
		}
		if (!scratch.HasOperatorBinding(selected.hash_join_idx)) {
			throw InternalException("SLJIT append sink primitive selected input has no hash-join binding");
		}

		auto &source_binding = scratch.OperatorBinding(selected.hash_join_idx).hash_join_probe;
		auto &sink_op = ops[primitive.sink_idx];
		auto &sink_input = selected_input.chunk;
		if (!source_binding.ready || source_binding.output_types != sink_op.append_sink.plan.input_types ||
		    sink_input.ColumnCount() != source_binding.output_types.size() ||
		    referenced_columns.size() != source_binding.output_types.size()) {
			throw InternalException("SLJIT append sink primitive selected input contract mismatch");
		}

		sink_input.Reset();
		auto view_stage_start = SljitRegionStageStart(runtime);
		const bool built_view =
		    SljitTryBuildSelectedHashJoinOutputColumnViews(source_binding, input, referenced_columns, sink_input);
		if (!built_view) {
			sink_input.Reset();
			if (!SljitTryMaterializeSelectedHashJoinOutputColumns(source_binding, input, referenced_columns,
			                                                      sink_input)) {
				throw InternalException("SLJIT append sink primitive could not build selected hash-join output");
			}
		}
		const char *view_stage = built_view ? "selected_append_view" : "selected_append_materialization";
		RecordSljitRegionStageRuntime(runtime, selected.hash_join_idx, SljitNativeRegionOpKind::HASH_JOIN_PROBE,
		                              view_stage, view_stage_start);
		RecordSljitRegionRuntimePath(runtime, SljitNativeRegionOpKind::APPEND_SINK,
		                             built_view ? "hash_join_selected_append_view"
		                                        : "hash_join_selected_append_materialized",
		                             sink_input.size());

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

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitAppendSinkPrimitive &primitive,
	           idx_t &processed_batches) {
		auto execute_batch = [&](DataChunk &batch) {
			return ExecuteSinkInput(runtime, result, ops, scratch, primitive, batch, processed_batches);
		};
		return SljitFlushDataChunkBatch(selected_sink_batch.chunk, execute_batch);
	}

private:
	bool ExecuteSinkInput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                      vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                      const SljitAppendSinkPrimitive &primitive, DataChunk &sink_input, idx_t &processed_batches) {
		auto &sink_op = ops[primitive.sink_idx];
		auto sink_stage_start = SljitRegionStageStart(runtime);
		auto &binding = SljitBindRecordedNativeSink(
		    runtime, runtime.ExecutionOperators(), scratch, primitive.sink_idx, sink_op.kind, sink_input,
		    sink_op.append_sink.plan.sink_info, "append-sink-runtime-binding-failed", "SLJIT append sink primitive");
		if (!binding.ready || !binding.append_sink.ready) {
			throw InternalException("SLJIT append sink primitive received an invalid binding");
		}
		auto sink_result = ExecutionSinkAppend(binding.append_sink, sink_input);
		RecordSljitRegionStageRuntime(runtime, primitive.sink_idx, sink_op.kind, "selected_append_sink",
		                              sink_stage_start);
		sink_result = runtime.ExecutionOperators().RecordSinkResult(sink_input, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

	SljitDataChunkBatch selected_input;
	SljitDataChunkBatch selected_sink_batch;
	vector<uint8_t> referenced_columns;
};

} // namespace duckdb

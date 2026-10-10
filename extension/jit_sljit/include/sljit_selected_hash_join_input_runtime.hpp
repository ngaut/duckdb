//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_selected_hash_join_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

static ExecutionHashJoinProbeBinding *
SljitTryGetSelectedHashJoinSourceBinding(const SljitRuntimeBatchView &selected_input, idx_t op_count,
                                         SljitRegionExecutionScratch &scratch, const char *context) {
	SljitRuntimeHashJoinSelection selected;
	if (!selected_input.TryGetHashJoinSelection(selected)) {
		return nullptr;
	}
	const auto source_hash_join_idx = selected.hash_join_idx;
	if (source_hash_join_idx >= op_count) {
		return nullptr;
	}
	if (!scratch.HasOperatorBinding(source_hash_join_idx)) {
		throw InternalException(string(context) + " has no source hash-join binding");
	}
	auto &source_binding = scratch.OperatorBinding(source_hash_join_idx).hash_join_probe;
	if (!source_binding.ready || source_binding.output_types.empty()) {
		throw InternalException(string(context) + " has an incomplete source hash-join binding");
	}
	return &source_binding;
}

//! Materializes the join output columns a projection reads for the rows of a selected probe view
static bool SljitTryPrepareSelectedHashJoinProjectionInput(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, vector<SljitExecutableRegionOp> &ops,
    const SljitExecutableRegionOp &projection, const SljitRuntimeBatchView &input,
    SljitDataChunkBatch &selected_hash_join_input, DataChunk *&input_chunk) {
	SljitRuntimeHashJoinSelection selected;
	if (!input.TryGetHashJoinSelection(selected)) {
		return false;
	}
	if (selected.hash_join_idx >= ops.size()) {
		throw InternalException("SLJIT selected projection input has an invalid hash join index");
	}
	auto source = SljitBindSelectedHashJoinOutputSource(runtime, ops, scratch, selected);
	auto &output_types = source.binding.output_types;
	vector<uint8_t> referenced_columns;
	if (!SljitBuildProjectionSourceColumnSet(projection, output_types.size(), nullptr, nullptr, referenced_columns)) {
		throw InternalException("SLJIT selected projection input could not collect projection sources");
	}

	selected_hash_join_input.Ensure(runtime.GetAllocator(), output_types);
	auto &selected_output = selected_hash_join_input.chunk;
	selected_output.Reset();
	if (!SljitTryMaterializeSelectedHashJoinOutputColumns(source, selected, referenced_columns, selected_output)) {
		throw InternalException("SLJIT selected projection input could not materialize hash-join output columns");
	}
	input_chunk = &selected_output;
	return selected_output.size() > 0;
}

class SljitSelectedHashJoinInputRuntime {
public:
	SljitSelectedHashJoinInputRuntime(ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops_p,
	                                  SljitRegionExecutionScratch &scratch_p)
	    : runtime(runtime_p), ops(ops_p), scratch(scratch_p), selected_hash_join_inputs(ops_p.size()),
	      selected_source_inputs(ops_p.size()) {
	}

	//! The chunk the probe of target_hash_join_idx runs on, whatever view the step ahead of it hands over: the
	//! selected rows of an upstream join's output, the rows a generated filter selected from the source chunk, or a
	//! materialized chunk
	DataChunk &BindProbeInput(idx_t target_hash_join_idx, const SljitRuntimeBatchView &input, const char *context) {
		if (input.HasHashJoinSelection()) {
			DataChunk *join_input = nullptr;
			ExecutionHashJoinProbeBinding *target_binding = nullptr;
			if (!TryPrepareInput(target_hash_join_idx, input, true,
			                     InputBatch(selected_hash_join_inputs, target_hash_join_idx), context,
			                     "probe_input_view", join_input, target_binding)) {
				throw InternalException("%s could not prepare selected upstream hash-join input", context);
			}
			return *join_input;
		}
		if (input.selection) {
			auto &source = SljitBindRuntimeBatchInput(input, context);
			auto &selected_source = InputBatch(selected_source_inputs, target_hash_join_idx);
			selected_source.Ensure(runtime.GetAllocator(), source.GetTypes());
			auto &selected = selected_source.chunk;
			selected.Reset();
			selected.Slice(source, *input.selection, input.count);
			RecordSljitRegionRuntimePath(runtime, ops[target_hash_join_idx].kind, "selected_source_view", input.count);
			return selected;
		}
		return SljitBindMaterializedRuntimeBatchInput(input, context);
	}

private:
	ExecutionHashJoinProbeBinding *TryGetSelectedSourceBinding(const SljitRuntimeBatchView &selected_input,
	                                                           idx_t target_hash_join_idx, const char *context) {
		if (target_hash_join_idx >= ops.size()) {
			return nullptr;
		}
		return SljitTryGetSelectedHashJoinSourceBinding(selected_input, ops.size(), scratch, context);
	}

	ExecutionHashJoinProbeBinding &BindTargetHashJoin(idx_t target_hash_join_idx, DataChunk &compact_input,
	                                                  const char *context) {
		auto &target_hash_join_op = ops[target_hash_join_idx];
		auto &target_binding =
		    SljitBindRecordedNativeOperator(runtime, runtime.ExecutionOperators(), scratch, target_hash_join_idx,
		                                    target_hash_join_op, compact_input,
		                                    target_hash_join_op.hash_join_probe.plan.operator_info, context)
		        .hash_join_probe;
		if (!target_binding.ready) {
			throw InternalException(string(context) + " received an invalid target binding");
		}
		return target_binding;
	}

	static void MarkReferencedColumn(vector<uint8_t> &referenced_columns, idx_t column_idx, const char *context) {
		if (column_idx >= referenced_columns.size()) {
			throw InternalException(string(context) + " column index out of range");
		}
		referenced_columns[column_idx] = 1;
	}

	static void MarkTargetProbeInputColumns(const ExecutionHashJoinProbeBinding &target_binding,
	                                        vector<uint8_t> &referenced_columns, bool include_lhs_output_columns,
	                                        const char *context) {
		for (auto column_idx : target_binding.probe_key_input_indices) {
			MarkReferencedColumn(referenced_columns, column_idx, context);
		}
		if (include_lhs_output_columns) {
			for (auto column_idx : target_binding.lhs_output_column_indices) {
				MarkReferencedColumn(referenced_columns, column_idx, context);
			}
		}
		for (auto &residual_source : target_binding.residual_sources) {
			if (residual_source.kind == ExecutionHashJoinResidualSourceKind::PROBE) {
				MarkReferencedColumn(referenced_columns, residual_source.input_index, context);
			}
		}
	}

	bool TryPrepareInput(idx_t target_hash_join_idx, const SljitRuntimeBatchView &selected_input,
	                     bool include_lhs_output_columns, SljitDataChunkBatch &input_batch, const char *context,
	                     const char *view_stage, DataChunk *&join_input,
	                     ExecutionHashJoinProbeBinding *&target_binding) {
		join_input = nullptr;
		target_binding = nullptr;
		auto source_binding = TryGetSelectedSourceBinding(selected_input, target_hash_join_idx, context);
		if (!source_binding) {
			return false;
		}

		input_batch.Ensure(runtime.GetAllocator(), source_binding->output_types);
		auto &compact_input = input_batch.chunk;
		compact_input.Reset();
		target_binding = &BindTargetHashJoin(target_hash_join_idx, compact_input, context);

		vector<uint8_t> referenced_columns(source_binding->output_types.size(), 0);
		MarkTargetProbeInputColumns(*target_binding, referenced_columns, include_lhs_output_columns, context);
		auto selected = selected_input.BindHashJoinSelection(context);
		const auto source_hash_join_idx = selected.hash_join_idx;
		auto source = SljitBindSelectedHashJoinOutputSource(runtime, ops, scratch, selected);
		auto view_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryBuildSelectedHashJoinOutputColumnViews(source, selected, referenced_columns, compact_input)) {
			RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, view_stage,
			                              view_stage_start);
			join_input = &compact_input;
			return true;
		}

		compact_input.Reset();
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		auto materialized = ExecuteSljitRegionRecordedOperation(
		    runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, "selected_view_materialization",
		    materialize_stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    (void)recorder;
			    return SljitTryMaterializeSelectedHashJoinOutputColumns(source, selected, referenced_columns,
			                                                            compact_input);
		    });
		if (!materialized) {
			RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
			                              "selected_view_materialization_miss", materialize_stage_start);
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
		                              "selected_view_materialization", materialize_stage_start);
		RecordSljitRegionRuntimeDelegation(runtime, ops[source_hash_join_idx].kind, "selected_view_materialization",
		                                   compact_input.size());
		join_input = &compact_input;
		return true;
	}

private:
	//! Each target join owns the chunk its input is built in: in a chain of joins, the input of one join is the
	//! source the next join's input is built from, so the two never share storage
	static SljitDataChunkBatch &InputBatch(vector<SljitDataChunkBatch> &batches, idx_t target_hash_join_idx) {
		if (target_hash_join_idx >= batches.size()) {
			throw InternalException("SLJIT selected hash join input target is out of range");
		}
		return batches[target_hash_join_idx];
	}

	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	vector<SljitDataChunkBatch> selected_hash_join_inputs;
	vector<SljitDataChunkBatch> selected_source_inputs;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_selected_hash_join_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

class SljitSelectedHashJoinInputRuntime {
public:
	SljitSelectedHashJoinInputRuntime(ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops_p,
	                                  SljitRegionExecutionScratch &scratch_p)
	    : runtime(runtime_p), ops(ops_p), scratch(scratch_p) {
	}

	bool TryPrepareMarkProbeInput(idx_t mark_hash_join_idx, const SljitRuntimeBatchView &selected_input,
	                              DataChunk *&join_input, string &deferred_reason) {
		DataChunk *compact_input = nullptr;
		ExecutionHashJoinProbeBinding *target_binding = nullptr;
		if (!TryPrepareInput(mark_hash_join_idx, selected_input, true, selected_hash_join_mark_input,
		                     "SLJIT selected MARK probe input",
		                     "materialize_selected_mark_input", "materialize_selected_mark_input_miss",
		                     "selected_mark_probe_input", compact_input, target_binding, deferred_reason)) {
			return false;
		}
		if (target_binding->output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
			throw InternalException("SLJIT selected MARK probe input received an invalid MARK binding");
		}
		join_input = compact_input;
		return true;
	}

	bool TryPrepareHashProbeInput(idx_t target_hash_join_idx, const SljitRuntimeBatchView &selected_input,
	                              DataChunk *&join_input, string &deferred_reason) {
		DataChunk *compact_input = nullptr;
		ExecutionHashJoinProbeBinding *target_binding = nullptr;
		if (!TryPrepareInput(target_hash_join_idx, selected_input, true, selected_hash_join_probe_input,
		                     "SLJIT selected hash probe input", "materialize_selected_hash_probe_input",
		                     "materialize_selected_hash_probe_input_miss", "selected_hash_probe_input", compact_input,
		                     target_binding, deferred_reason)) {
			return false;
		}
		join_input = compact_input;
		return true;
	}

private:
	ExecutionHashJoinProbeBinding *TryGetSelectedSourceBinding(const SljitRuntimeBatchView &selected_input,
	                                                           idx_t target_hash_join_idx, const char *context) {
		if (!selected_input.HasHashJoinSelection()) {
			return nullptr;
		}
		const auto source_hash_join_idx = selected_input.hash_join_idx;
		if (source_hash_join_idx >= ops.size() || target_hash_join_idx >= ops.size()) {
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

	bool TryBindTargetHashJoin(idx_t target_hash_join_idx, DataChunk &compact_input, const char *context,
	                           ExecutionHashJoinProbeBinding *&target_binding, string &deferred_reason) {
		auto &target_hash_join_op = ops[target_hash_join_idx];
		ExecutionOperatorBinding *target_binding_ptr = nullptr;
		auto bind_result = SljitBindRecordedNativeOperator(
		    runtime, runtime.ExecutionOperators(), scratch, target_hash_join_idx, target_hash_join_op, compact_input,
		    target_hash_join_op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred", context,
		    target_binding_ptr, deferred_reason);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return false;
		}
		target_binding = &target_binding_ptr->hash_join_probe;
		if (!target_binding->ready) {
			throw InternalException(string(context) + " received an invalid target binding");
		}
		return true;
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
	                     const char *materialize_stage, const char *materialize_miss_stage,
	                     const char *boundary_counter, DataChunk *&join_input,
	                     ExecutionHashJoinProbeBinding *&target_binding, string &deferred_reason) {
		join_input = nullptr;
		target_binding = nullptr;
		auto source_binding = TryGetSelectedSourceBinding(selected_input, target_hash_join_idx, context);
		if (!source_binding) {
			return false;
		}

		input_batch.Ensure(runtime.GetAllocator(), source_binding->output_types);
		auto &compact_input = input_batch.chunk;
		compact_input.Reset();
		if (!TryBindTargetHashJoin(target_hash_join_idx, compact_input, context, target_binding, deferred_reason)) {
			return false;
		}

		vector<uint8_t> referenced_columns(source_binding->output_types.size(), 0);
		MarkTargetProbeInputColumns(*target_binding, referenced_columns, include_lhs_output_columns, context);

		const auto source_hash_join_idx = selected_input.hash_join_idx;
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		auto materialized = ExecuteSljitRegionRecordedOperation(
		    runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, materialize_stage, materialize_stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    (void)recorder;
			    return SljitTryMaterializeSelectedHashJoinOutputColumns(*source_binding, selected_input,
			                                                            referenced_columns, compact_input);
		    });
		if (!materialized) {
			RecordSljitRegionStageRuntimePath(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind,
			                                  materialize_miss_stage, materialize_stage_start);
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, source_hash_join_idx, ops[source_hash_join_idx].kind, materialize_stage,
		                              materialize_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, ops[source_hash_join_idx].kind, boundary_counter,
		                                         selected_input.count);
		join_input = &compact_input;
		return true;
	}

private:
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	SljitDataChunkBatch selected_hash_join_mark_input;
	SljitDataChunkBatch selected_hash_join_probe_input;
};

} // namespace duckdb

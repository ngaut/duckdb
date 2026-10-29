//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_output_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

namespace duckdb {

//! What a hash join's output columns index. A probe that runs on the source of an elided pre-join projection is
//! bound against that source chunk (its keys, residual sources and exact RHS aliases index it), while its output
//! columns (lhs_output_column_indices) index the projection's output. A consumer that does not translate through the
//! selection's column map reads the output source instead: the projection executed over the source rows, the probe
//! re-bound against that chunk, and the output proof with its RHS aliases in that chunk's layout.
struct SljitHashJoinOutputSource {
	DataChunk &input;
	ExecutionHashJoinProbeBinding &binding;
	ExecutionHashJoinProbeOutputProof output_proof;
};

static SljitHashJoinOutputSource SljitBindHashJoinOutputSource(ExecutionRegionRuntime &runtime,
                                                               vector<SljitExecutableRegionOp> &ops,
                                                               SljitRegionExecutionScratch &scratch,
                                                               idx_t hash_join_idx, DataChunk &join_input,
                                                               ExecutionHashJoinProbeOutputProof output_proof,
                                                               optional_ptr<const SljitBatchLayout> output_layout) {
	if (hash_join_idx >= ops.size() || !scratch.HasOperatorBinding(hash_join_idx)) {
		throw InternalException("SLJIT hash join output source requires a bound probe");
	}
	auto &probe_binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!output_layout) {
		return SljitHashJoinOutputSource {join_input, probe_binding, output_proof};
	}
	const auto output_projection_idx = output_layout->ProducerIndex();
	if (output_projection_idx >= ops.size() ||
	    ops[output_projection_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
		throw InternalException("SLJIT hash join output source has no elided projection to execute");
	}
	auto &projection_op = ops[output_projection_idx];
	auto &projected_input = scratch.TemporaryChunk(output_projection_idx);
	projected_input.Reset();
	SljitExecuteProjection(runtime, scratch, output_projection_idx, projection_op, join_input, projected_input);

	auto &hash_join_op = ops[hash_join_idx];
	auto &output_binding = scratch.OutputOperatorBinding(hash_join_idx);
	if (!scratch.HasOutputOperatorBinding(hash_join_idx)) {
		auto bind_result =
		    runtime.ExecutionOperators().BindOperator(hash_join_op.operator_index, projected_input,
		                                              hash_join_op.HashJoinProbe().plan.operator_info, output_binding);
		if (bind_result != ExecutionOperatorBindResult::READY || !output_binding.hash_join_probe.ready) {
			throw InternalException("SLJIT hash join output source could not bind the probe to its projected input: %s",
			                        output_binding.blocker);
		}
		scratch.MarkOutputOperatorBindingReady(hash_join_idx);
	}
	auto &output_probe = output_binding.hash_join_probe;
	// The projected input holds the keys in their joined type, so no source-key reinterpretation applies
	output_proof.source_key0_int64_to_int32 = false;
	if (output_proof.ExactSourceFilterMatches()) {
		for (auto alias : output_probe.exact_rhs_output_probe_input_indices) {
			if (alias == DConstants::INVALID_INDEX) {
				throw InternalException("SLJIT hash join output source has no projected alias for an exact RHS column");
			}
		}
		output_proof.SetExactSourceFilterMatches(output_probe.exact_rhs_output_probe_input_indices,
		                                         output_proof.match_selection_is_identity);
	}
	return SljitHashJoinOutputSource {projected_input, output_probe, output_proof};
}

//! The output source of a selected probe view: the chunk the probe ran on, or the elided projection over it
static SljitHashJoinOutputSource SljitBindSelectedHashJoinOutputSource(ExecutionRegionRuntime &runtime,
                                                                       vector<SljitExecutableRegionOp> &ops,
                                                                       SljitRegionExecutionScratch &scratch,
                                                                       const SljitRuntimeHashJoinSelection &selected) {
	return SljitBindHashJoinOutputSource(runtime, ops, scratch, selected.hash_join_idx, selected.SourceInput(),
	                                     selected.output_proof, selected.OutputLayout());
}

} // namespace duckdb

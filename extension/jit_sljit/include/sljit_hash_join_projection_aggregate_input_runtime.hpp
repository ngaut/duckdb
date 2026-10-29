//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_aggregate_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_batch_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_hash_join_rhs_projection_runtime.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool SljitTryReferenceHashJoinProjectionAggregateInputsToChunk(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    const vector<SljitJoinProjectionAggregateInputSource> &input_sources, idx_t count, DataChunk &result) {
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) || result.ColumnCount() != input_sources.size()) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD || !binding.hash_table) {
		return false;
	}
	for (idx_t column_idx = 0; column_idx < input_sources.size(); column_idx++) {
		const auto &source = input_sources[column_idx];
		if (source.kind == SljitJoinProjectionAggregateInputKind::UNUSED) {
			continue;
		}
		if (source.kind != SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT ||
		    source.input_idx >= join_input.ColumnCount() ||
		    join_input.data[source.input_idx].GetType() != source.type ||
		    result.data[column_idx].GetType() != source.type) {
			return false;
		}
	}
	const auto stage_start = SljitRegionStageStart(runtime);
	for (idx_t column_idx = 0; column_idx < input_sources.size(); column_idx++) {
		const auto &source = input_sources[column_idx];
		if (source.kind != SljitJoinProjectionAggregateInputKind::UNUSED) {
			result.data[column_idx].Slice(join_input.data[source.input_idx], match_selection, count);
		}
	}
	result.SetChildCardinality(count);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(),
	                              "projection_aggregate_reference_payload_view", stage_start);
	RecordSljitRegionRuntimePath(runtime, projection_op.Kind(), "projection_aggregate.reference_payload_view", count);
	return true;
}

static bool SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    const SelectionVector &build_selection, Vector &row_pointers,
    const vector<SljitJoinProjectionAggregateInputSource> &input_sources, idx_t count, DataChunk &result,
    optional_ptr<string> blocker = nullptr) {
	auto block = [&](const char *reason) {
		if (blocker) {
			*blocker = reason;
		}
		return false;
	};
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) || result.ColumnCount() != input_sources.size() ||
	    result.size() + count > STANDARD_VECTOR_SIZE) {
		return block("shape");
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
		return block("hash_join_shape");
	}
	const auto current_size = result.size();
	bool referenced_direct_input = false;
	bool materialized_reference = false;
	bool materialized_computed = false;
	const auto stage_start = SljitRegionStageStart(runtime);
	for (idx_t output_idx = 0; output_idx < input_sources.size(); output_idx++) {
		const auto &source = input_sources[output_idx];
		auto &target = result.data[output_idx];
		if (source.kind == SljitJoinProjectionAggregateInputKind::UNUSED) {
			continue;
		}
		if (target.GetType() != source.type) {
			return block("input_layout_type");
		}
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
			if (current_size != 0 || source.input_idx >= join_input.ColumnCount() ||
			    join_input.data[source.input_idx].GetType() != source.type) {
				return block("lhs_input");
			}
			target.Slice(join_input.data[source.input_idx], match_selection, count);
			referenced_direct_input = true;
			continue;
		}
		if (!source.expression) {
			return block("unbound_input");
		}
		const bool reference = source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT;
		if (!(reference && perfect_hash_join) &&
		    (target.GetVectorType() != VectorType::FLAT_VECTOR ||
		     (!reference && !SljitDirectProjectionBatchSupportsType(target.GetType())) ||
		     FlatVector::GetCapacity(target) < current_size + count)) {
			return block("projection_target");
		}
		if (source.IsBuildInput()) {
			bool generated_source = false;
			if (!SljitTryMaterializeBoundHashJoinRHSProjectionToBatch(runtime, binding, source, build_selection,
			                                                          row_pointers, target, current_size, count,
			                                                          generated_source)) {
				return block("rhs_projection");
			}
			if (generated_source) {
				RecordSljitRegionRuntimePath(runtime, projection_op.Kind(), "rhs_row_pointer_generated_projection",
				                             count);
			}
		} else if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_EXPRESSION) {
			D_ASSERT(source.adapter);
			auto &adapter = *source.adapter;
			if (!SljitTryExecuteProjectionExpressionToBatch(*source.expression, join_input, target, current_size, count,
			                                                &match_selection, adapter, &runtime)) {
				return block("lhs_projection");
			}
		} else if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_MIXED_EXPRESSION) {
			D_ASSERT(source.adapter);
			auto &adapter = *source.adapter;
			if (!SljitTryMaterializeBoundHashJoinMixedProjectionToBatch(runtime, binding, source, join_input,
			                                                            match_selection, row_pointers, target,
			                                                            current_size, count, adapter)) {
				return block("mixed_projection");
			}
		} else {
			return block("input_source_kind");
		}
		materialized_reference |= reference;
		materialized_computed |= !reference;
	}
	result.SetChildCardinality(current_size + count);
	if (referenced_direct_input) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(),
		                              "projection_aggregate_reference_payload_view", stage_start);
		RecordSljitRegionRuntimePath(runtime, projection_op.Kind(), "projection_aggregate.reference_payload_view",
		                             count);
	}
	if (materialized_reference) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "post_join_reference_projection",
		                              stage_start);
	}
	if (materialized_computed) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "post_join_computed_projection",
		                              stage_start);
	}
	return true;
}

} // namespace duckdb

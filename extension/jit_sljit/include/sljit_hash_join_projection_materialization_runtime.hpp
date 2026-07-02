//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_materialization_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_output_reference_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_hash_join_rhs_projection_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

struct SljitDirectHashJoinProjectionBatchContext {
	optional_ptr<ExecutionHashJoinProbeBinding> binding;
	idx_t current_size = 0;
	idx_t target_size = 0;
	idx_t output_count = 0;
	idx_t lhs_column_count = 0;
	idx_t rhs_column_count = 0;
	idx_t join_output_column_count = 0;
	bool regular_hash_join = false;
};

static bool SljitPrepareDirectHashJoinProjectionBatch(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
                                                      SljitExecutableRegionOp &projection_op, DataChunk &batch,
                                                      optional_ptr<const vector<idx_t>> output_to_projection,
                                                      idx_t count, vector<uint8_t> &skip_projection,
                                                      SljitDirectHashJoinProjectionBatchContext &context) {
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx)) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
	const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
	if (!binding.ready || (!regular_hash_join && !perfect_hash_join) || (regular_hash_join && !binding.hash_table) ||
	    (perfect_hash_join && !binding.perfect_layout.ready) ||
	    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
		return false;
	}

	const auto current_size = batch.size();
	const auto target_size = current_size + count;
	const auto output_count = output_to_projection ? output_to_projection->size() : projection_op.projections.size();
	if (batch.ColumnCount() != output_count || target_size > STANDARD_VECTOR_SIZE) {
		return false;
	}
	if (skip_projection.empty()) {
		skip_projection.assign(projection_op.projections.size(), 0);
	} else if (skip_projection.size() != projection_op.projections.size()) {
		return false;
	}

	context.binding = optional_ptr<ExecutionHashJoinProbeBinding>(&binding);
	context.current_size = current_size;
	context.target_size = target_size;
	context.output_count = output_count;
	context.lhs_column_count = binding.lhs_output_column_indices.size();
	context.rhs_column_count = binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD
	                               ? binding.rhs_output_column_count
	                               : 0;
	context.join_output_column_count = context.lhs_column_count + context.rhs_column_count;
	context.regular_hash_join = regular_hash_join;
	return true;
}

static bool SljitTryGetDirectHashJoinProjectionOutput(const SljitExecutableRegionOp &projection_op,
                                                      optional_ptr<const vector<idx_t>> output_to_projection,
                                                      idx_t output_idx, idx_t &projected_idx) {
	projected_idx = output_to_projection ? (*output_to_projection)[output_idx] : output_idx;
	return projected_idx < projection_op.projections.size();
}

template <class MATERIALIZE_OUTPUT>
static bool SljitForEachDirectHashJoinProjectionOutput(const SljitExecutableRegionOp &projection_op,
                                                       optional_ptr<const vector<idx_t>> output_to_projection,
                                                       const SljitDirectHashJoinProjectionBatchContext &context,
                                                       vector<uint8_t> &skip_projection,
                                                       MATERIALIZE_OUTPUT materialize_output, bool &materialized_any) {
	materialized_any = false;
	for (idx_t output_idx = 0; output_idx < context.output_count; output_idx++) {
		idx_t projected_idx;
		if (!SljitTryGetDirectHashJoinProjectionOutput(projection_op, output_to_projection, output_idx,
		                                               projected_idx)) {
			return false;
		}
		if (skip_projection[projected_idx]) {
			continue;
		}
		if (!materialize_output(output_idx, projected_idx)) {
			continue;
		}
		skip_projection[projected_idx] = 1;
		materialized_any = true;
	}
	return true;
}

static bool SljitTryMaterializeHashJoinReferenceProjectionsToBatch(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    Vector &row_pointers, DataChunk &batch, optional_ptr<const vector<idx_t>> output_to_projection, idx_t count,
    vector<uint8_t> &skip_projection) {
	SljitDirectHashJoinProjectionBatchContext context;
	if (!SljitPrepareDirectHashJoinProjectionBatch(scratch, hash_join_idx, projection_op, batch, output_to_projection,
	                                               count, skip_projection, context)) {
		return false;
	}
	auto &binding = *context.binding;
	const auto stage_start = SljitRegionStageStart(runtime);

	bool materialized_any;
	auto materialize_output = [&](idx_t output_idx, idx_t projected_idx) -> bool {
		auto &expr = projection_op.projections[projected_idx];
		SljitExecutableRegionExpression remapped_expr;
		idx_t join_output_source_index;
		if (!SljitTryBuildSingleSourceProjectionExpression(expr, remapped_expr, join_output_source_index) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
		    join_output_source_index >= context.join_output_column_count) {
			return false;
		}
		auto &plan = expr.plan;
		auto &target = batch.data[output_idx];
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != plan.return_type ||
		    FlatVector::GetCapacity(target) < context.target_size) {
			return false;
		}

		if (!SljitTryCopyAllValidHashJoinOutputReferenceToBatch(binding, join_input, match_selection, row_pointers,
		                                                        join_output_source_index, target, context.current_size,
		                                                        count)) {
			return false;
		}
		return true;
	};
	if (!SljitForEachDirectHashJoinProjectionOutput(projection_op, output_to_projection, context, skip_projection,
	                                                materialize_output, materialized_any)) {
		return false;
	}

	if (materialized_any) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              "post_join_direct_reference_projection", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_post_join_reference_projection",
		                                         count);
	}
	return materialized_any;
}

static bool SljitTryReferenceHashJoinLHSProjectionSourcesToChunk(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    optional_ptr<const vector<idx_t>> output_to_projection, idx_t count, DataChunk &result) {
	if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) || !output_to_projection ||
	    result.ColumnCount() != output_to_projection->size()) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD || !binding.hash_table) {
		return false;
	}

	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	vector<idx_t> input_columns;
	input_columns.reserve(output_to_projection->size());
	for (idx_t output_idx = 0; output_idx < output_to_projection->size(); output_idx++) {
		const auto projected_idx = (*output_to_projection)[output_idx];
		if (projected_idx >= projection_op.projections.size() || projected_idx >= projection_op.output_types.size()) {
			return false;
		}
		auto &expr = projection_op.projections[projected_idx];
		SljitExecutableRegionExpression remapped_expr;
		idx_t join_output_source_index;
		if (!SljitTryBuildSingleSourceProjectionExpression(expr, remapped_expr, join_output_source_index) ||
		    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
		    join_output_source_index >= lhs_column_count) {
			return false;
		}

		const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
		if (input_col >= join_input.ColumnCount() ||
		    expr.plan.return_type != projection_op.output_types[projected_idx] ||
		    join_input.data[input_col].GetType() != expr.plan.return_type ||
		    result.data[output_idx].GetType() != expr.plan.return_type) {
			return false;
		}
		input_columns.push_back(input_col);
	}
	if (input_columns.size() != output_to_projection->size()) {
		return false;
	}

	const auto stage_start = SljitRegionStageStart(runtime);
	for (idx_t output_idx = 0; output_idx < input_columns.size(); output_idx++) {
		result.data[output_idx].Slice(join_input.data[input_columns[output_idx]], match_selection, count);
	}
	result.SetChildCardinality(count);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
	                              "post_join_direct_reference_payload_view", stage_start);
	RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_reference_payload_view", count);
	return true;
}

static bool SljitTryMaterializeHashJoinComputedProjectionsToBatch(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, idx_t projection_idx,
    SljitExecutableRegionOp &projection_op, DataChunk &join_input, const SelectionVector &match_selection,
    Vector &row_pointers, DataChunk &batch, optional_ptr<const vector<idx_t>> output_to_projection, idx_t count,
    vector<uint8_t> &skip_projection) {
	SljitDirectHashJoinProjectionBatchContext context;
	if (!SljitPrepareDirectHashJoinProjectionBatch(scratch, hash_join_idx, projection_op, batch, output_to_projection,
	                                               count, skip_projection, context)) {
		return false;
	}
	auto &binding = *context.binding;

	bool materialized_any;
	const auto stage_start = SljitRegionStageStart(runtime);
	auto materialize_output = [&](idx_t output_idx, idx_t projected_idx) -> bool {
		auto &source_expr = projection_op.projections[projected_idx];
		if (source_expr.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			return false;
		}
		auto &target = batch.data[output_idx];
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
		    !SljitDirectProjectionBatchSupportsType(target.GetType()) ||
		    FlatVector::GetCapacity(target) < context.target_size) {
			return false;
		}

		SljitExecutableRegionExpression remapped_expr;
		if (!SljitTryBuildHashJoinProbeLHSProjectionExpression(binding, join_input, source_expr, remapped_expr)) {
			if (!context.regular_hash_join) {
				return false;
			}
			auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
			bool used_row_pointer_generated_source = false;
			if (!SljitTryMaterializeHashJoinComputedRHSProjectionToBatch(
			        binding, source_expr, row_pointers, batch, output_idx, context.current_size, count, adapter_scratch,
			        used_row_pointer_generated_source)) {
				if (!SljitTryMaterializeHashJoinMixedProjectionToBatch(binding, source_expr, join_input,
				                                                       match_selection, row_pointers, batch, output_idx,
				                                                       context.current_size, count, adapter_scratch)) {
					return false;
				}
			}
			if (used_row_pointer_generated_source) {
				RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_rhs_row_pointer_generated_projection",
				                             count);
			}
			return true;
		}
		auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
		if (!SljitTryExecuteProjectionExpressionToBatch(remapped_expr, join_input, target, context.current_size, count,
		                                                &match_selection, adapter_scratch)) {
			return false;
		}
		return true;
	};
	if (!SljitForEachDirectHashJoinProjectionOutput(projection_op, output_to_projection, context, skip_projection,
	                                                materialize_output, materialized_any)) {
		return false;
	}

	if (materialized_any) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              "post_join_direct_computed_projection", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_post_join_computed_projection",
		                                         count);
	}
	return materialized_any;
}

static bool SljitTryDirectMaterializeHashJoinProjectionSourcesToBatch(
    ExecutionRegionRuntime &runtime, const vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    idx_t hash_join_idx, idx_t projection_idx, SljitExecutableRegionOp &projection_op, DataChunk &join_input,
    const SelectionVector &match_selection, Vector &row_pointers, DataChunk &join_source, DataChunk &batch,
    optional_ptr<const vector<idx_t>> output_to_projection = nullptr, optional_ptr<Vector> projected_hashes = nullptr,
    optional_ptr<const vector<uint8_t>> extra_skip_projection = nullptr) {
	if (join_source.size() == 0 || hash_join_idx >= ops.size() || !scratch.HasOperatorBinding(hash_join_idx)) {
		return false;
	}
	vector<uint8_t> direct_reference_skip;
	if (extra_skip_projection) {
		if (extra_skip_projection->size() != projection_op.projections.size()) {
			return false;
		}
		direct_reference_skip = *extra_skip_projection;
	}
	const bool direct_references_materialized = SljitTryMaterializeHashJoinReferenceProjectionsToBatch(
	    runtime, scratch, hash_join_idx, projection_idx, projection_op, join_input, match_selection, row_pointers,
	    batch, output_to_projection, join_source.size(), direct_reference_skip);
	const bool direct_computed_materialized = SljitTryMaterializeHashJoinComputedProjectionsToBatch(
	    runtime, scratch, hash_join_idx, projection_idx, projection_op, join_input, match_selection, row_pointers,
	    batch, output_to_projection, join_source.size(), direct_reference_skip);
	const bool direct_projection_materialized = direct_references_materialized || direct_computed_materialized;
	auto direct_projection_skip_ptr = SljitProjectionSkipHasAny(direct_reference_skip)
	                                      ? optional_ptr<const vector<uint8_t>>(&direct_reference_skip)
	                                      : nullptr;
	if (direct_projection_materialized &&
	    SljitSelectedProjectionOutputsAreSkipped(projection_op, output_to_projection, direct_reference_skip)) {
		SljitFinishDirectProjectionBatchTargets(batch, batch.size() + join_source.size(),
		                                        !direct_computed_materialized);
		if (projected_hashes) {
			SljitHashDirectProjectionBatch(runtime, projection_idx, projection_op, output_to_projection != nullptr,
			                               batch, *projected_hashes);
		}
		SljitRecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op,
		                                                output_to_projection != nullptr, join_source.size(),
		                                                SljitRegionStageStart(runtime));
		return true;
	}
	vector<uint8_t> referenced_columns;
	if (!SljitBuildProjectionSourceColumnSet(projection_op, join_source.ColumnCount(), output_to_projection,
	                                         direct_projection_skip_ptr, referenced_columns)) {
		return false;
	}
	auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
	auto stage_start = SljitRegionStageStart(runtime);
	auto &hash_join_op = ops[hash_join_idx];
	auto materialized = ExecuteSljitRegionRecordedOperation(
	    runtime, hash_join_idx, hash_join_op.kind, "materialize_projection_sources", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return ExecutionMaterializeHashJoinProbeProjectionSources(
		        binding, join_input, row_pointers, match_selection, join_source.size(), referenced_columns, join_source,
		        recorder, optional_ptr<const SelectionVector>(&scratch.HashJoinBuildSelection(hash_join_idx)));
	    });
	if (!materialized) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_projection_sources",
	                              stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "projection_source", join_source.size());
	return SljitTryDirectMaterializeFixedProjectionToBatch(runtime, scratch, projection_idx, projection_op, join_source,
	                                                       batch, output_to_projection, direct_projection_skip_ptr,
	                                                       projected_hashes);
}

} // namespace duckdb

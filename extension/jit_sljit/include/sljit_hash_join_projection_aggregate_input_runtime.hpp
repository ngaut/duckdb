//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_aggregate_input_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_batch_runtime.hpp"
#include "sljit_hash_join_output_reference_runtime.hpp"
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

	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	vector<idx_t> input_columns;
	input_columns.reserve(input_sources.size());
	for (idx_t output_idx = 0; output_idx < input_sources.size(); output_idx++) {
		auto &source = input_sources[output_idx];
		idx_t input_col = DConstants::INVALID_INDEX;
		LogicalType input_type;
		switch (source.kind) {
		case SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT: {
			const auto projected_idx = source.projection_idx;
			if (projected_idx >= projection_op.projections.size() ||
			    projected_idx >= projection_op.output_types.size()) {
				return false;
			}
			auto &expr = projection_op.projections[projected_idx];
			idx_t join_output_source_index;
			if (!SljitTryGetSingleSourceReferenceProjectionIndex(expr, join_output_source_index) ||
			    join_output_source_index >= lhs_column_count) {
				return false;
			}
			input_col = binding.lhs_output_column_indices[join_output_source_index];
			input_type = expr.plan.return_type;
			if (input_type != projection_op.output_types[projected_idx]) {
				return false;
			}
			break;
		}
		case SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT:
			input_col = source.input_idx;
			input_type = source.type;
			break;
		default:
			return false;
		}
		if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != input_type ||
		    result.data[output_idx].GetType() != input_type) {
			return false;
		}
		input_columns.push_back(input_col);
	}
	if (input_columns.size() != input_sources.size()) {
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
	auto block_message = [&](string reason) {
		if (blocker) {
			*blocker = std::move(reason);
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
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	const auto rhs_column_count = binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD
	                                  ? binding.rhs_output_column_count
	                                  : 0;
	const auto join_output_column_count = lhs_column_count + rhs_column_count;
	bool referenced_direct_input = false;
	bool materialized_reference = false;
	bool materialized_computed = false;
	const auto stage_start = SljitRegionStageStart(runtime);

	for (idx_t output_idx = 0; output_idx < input_sources.size(); output_idx++) {
		auto &source = input_sources[output_idx];
		auto &target = result.data[output_idx];
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
			const auto input_col = source.input_idx;
			if (input_col >= join_input.ColumnCount()) {
				return block_message("lhs_input_bounds_output_" + to_string(output_idx) + "_input_" +
				                     to_string(input_col) + "_columns_" + to_string(join_input.ColumnCount()));
			}
			if (join_input.data[input_col].GetType() != source.type || target.GetType() != source.type) {
				return block_message("lhs_input_type_output_" + to_string(output_idx) + "_input_" +
				                     to_string(input_col) + "_expected_" + source.type.ToString() + "_actual_" +
				                     join_input.data[input_col].GetType().ToString() + "_target_" +
				                     target.GetType().ToString());
			}
			target.Slice(join_input.data[input_col], match_selection, count);
			referenced_direct_input = true;
			continue;
		}
		if (source.kind != SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT) {
			return block("input_source_kind");
		}
		const auto projected_idx = source.projection_idx;
		if (projected_idx >= projection_op.projections.size() || projected_idx >= projection_op.output_types.size()) {
			return block("projection_index");
		}
		auto &source_expr = projection_op.projections[projected_idx];
		if (target.GetType() != source_expr.plan.return_type ||
		    source_expr.plan.return_type != projection_op.output_types[projected_idx]) {
			return block("projection_type");
		}

		SljitExecutableRegionExpression remapped_reference;
		idx_t join_output_source_index;
		if (SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_reference, join_output_source_index) &&
		    SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan) &&
		    join_output_source_index < join_output_column_count) {
			if (join_output_source_index < lhs_column_count) {
				if (current_size != 0) {
					return block("lhs_append");
				}
				const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
				if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != target.GetType()) {
					return block("lhs_reference");
				}
				target.Slice(join_input.data[input_col], match_selection, count);
			} else {
				if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
					return block("rhs_output_mode");
				}
				const auto rhs_col_idx = join_output_source_index - lhs_column_count;
				if (regular_hash_join) {
					if (target.GetVectorType() != VectorType::FLAT_VECTOR) {
						return block("rhs_reference_target_vector");
					}
					if (FlatVector::GetCapacity(target) < current_size + count) {
						return block_message("rhs_reference_capacity_current_" + to_string(current_size) + "_count_" +
						                     to_string(count) + "_capacity_" +
						                     to_string(FlatVector::GetCapacity(target)));
					}
					if (!SljitTryGatherHashJoinRHSReferenceProjectionToBatch(
					        binding, remapped_reference.plan, rhs_col_idx, row_pointers, target, current_size, count)) {
						return block_message("rhs_reference_gather_projection_" + to_string(projected_idx) + "_rhs_" +
						                     to_string(rhs_col_idx) + "_kind_" +
						                     to_string(static_cast<int>(remapped_reference.plan.kind)) + "_source_" +
						                     to_string(remapped_reference.plan.source_index) + "_return_" +
						                     remapped_reference.plan.return_type.ToString() + "_target_" +
						                     target.GetType().ToString());
					}
				} else {
					if (current_size != 0 ||
					    binding.perfect_layout.rhs_dictionary_buffers.size() !=
					        binding.perfect_layout.rhs_output_column_count ||
					    rhs_col_idx >= binding.perfect_layout.rhs_output_column_count ||
					    target.GetType() != binding.perfect_layout.rhs_output_types[rhs_col_idx]) {
						return block("rhs_perfect_reference");
					}
					target.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_col_idx], build_selection,
					                  count);
				}
			}
			materialized_reference = true;
			continue;
		}

		if (target.GetVectorType() != VectorType::FLAT_VECTOR ||
		    !SljitDirectProjectionBatchSupportsType(target.GetType()) ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return block("computed_target");
		}
		SljitExecutableRegionExpression remapped_expr;
		if (!SljitTryBuildHashJoinProbeLHSProjectionExpression(binding, join_input, source_expr, remapped_expr)) {
			auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
			bool used_row_pointer_generated_source = false;
			if (regular_hash_join) {
				if (!SljitTryMaterializeHashJoinComputedRHSProjectionToBatch(
				        binding, source_expr, row_pointers, result, output_idx, current_size, count, adapter_scratch,
				        used_row_pointer_generated_source)) {
					if (!SljitTryMaterializeHashJoinMixedProjectionToBatch(
					        binding, source_expr, join_input, match_selection, row_pointers, result, output_idx,
					        current_size, count, adapter_scratch)) {
						return block("computed_projection");
					}
				}
			} else if (perfect_hash_join) {
				if (!SljitTryMaterializePerfectHashJoinComputedRHSProjectionToBatch(
				        binding, source_expr, build_selection, result, output_idx, current_size, count,
				        adapter_scratch)) {
					return block("computed_projection");
				}
			} else {
				return block("computed_hash_join_shape");
			}
			if (used_row_pointer_generated_source) {
				RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_rhs_row_pointer_generated_projection",
				                             count);
			}
		} else {
			auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
			if (!SljitTryExecuteProjectionExpressionToBatch(remapped_expr, join_input, target, current_size, count,
			                                                &match_selection, adapter_scratch)) {
				return block("lhs_projection");
			}
		}
		materialized_computed = true;
	}

	result.SetChildCardinality(current_size + count);
	if (referenced_direct_input) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              "post_join_direct_reference_payload_view", stage_start);
		RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_reference_payload_view", count);
	}
	if (materialized_reference) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              "post_join_direct_reference_projection", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_post_join_reference_projection",
		                                         count);
	}
	if (materialized_computed) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              "post_join_direct_computed_projection", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_post_join_computed_projection",
		                                         count);
	}
	return true;
}

} // namespace duckdb

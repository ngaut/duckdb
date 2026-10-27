//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_executable_expression_codegen.hpp"
#include "sljit_hash_join_output_source_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
namespace duckdb {

static bool SljitTryCollectHashJoinProjectionExpressionSources(const SljitExecutableRegionExpression &expr,
                                                               idx_t input_column_count, vector<uint8_t> &referenced) {
	referenced.assign(input_column_count, 0);
	if (!expr.input_source_indices.empty()) {
		return SljitAddProjectionSourceColumns(expr.input_source_indices, input_column_count, referenced);
	}
	auto &plan = expr.plan;
	auto add_source = [&](idx_t source_index) {
		return SljitAddProjectionSourceColumn(source_index, input_column_count, referenced);
	};
	auto add_sources = [&](const vector<idx_t> &source_indices) {
		return SljitAddProjectionSourceColumns(source_indices, input_column_count, referenced);
	};
	auto add_predicate_sources = [&](const SljitNativeRegionExpressionPlan &predicate_plan) {
		if (!predicate_plan.expression_tree_source_indices.empty()) {
			return SljitAddProjectionSourceColumns(predicate_plan.expression_tree_source_indices, input_column_count,
			                                       referenced);
		}
		if (!predicate_plan.predicate) {
			return false;
		}
		return SljitAddProjectionSourceColumns(predicate_plan.predicate->source_indices, input_column_count,
		                                       referenced);
	};
	auto add_constant_sources = [&](const SljitNativeRegionExpressionPlan &constant_plan) {
		return SljitAddProjectionSourceColumns(constant_plan.expression_tree_source_indices, input_column_count,
		                                       referenced);
	};
	return SljitTryApplyProjectionPlanSources(plan, add_source, add_sources, add_constant_sources,
	                                          add_predicate_sources);
}

static bool SljitProjectionReferencesInputColumn(const SljitExecutableRegionOp &projection_op, idx_t input_column_count,
                                                 idx_t column_idx) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
		return true;
	}
	for (auto &projection : projection_op.projections) {
		vector<uint8_t> referenced;
		if (!SljitTryCollectHashJoinProjectionExpressionSources(projection, input_column_count, referenced)) {
			return true;
		}
		if (column_idx < referenced.size() && referenced[column_idx]) {
			return true;
		}
	}
	return false;
}

static void SljitGatherHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers,
                                         idx_t count, idx_t rhs_col_idx, Vector &result) {
	ExecutionGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, result);
}

static bool SljitSelectedHashJoinSelectionIsIdentity(const SelectionVector &selection, idx_t count) {
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (selection.get_index(row_idx) != row_idx) {
			return false;
		}
	}
	return true;
}

//! Builds the referenced join output columns of the rows a selection keeps. The output source supplies the chunk the
//! probe's output indices address and the exact RHS aliases in that chunk's layout; the selection supplies the rows
static bool SljitTryBuildSelectedHashJoinOutputColumns(const SljitHashJoinOutputSource &source,
                                                       const SljitRuntimeHashJoinSelection &selected,
                                                       const vector<uint8_t> &referenced_columns, DataChunk &result,
                                                       bool allow_regular_rhs_gather) {
	auto &binding = source.binding;
	if (!binding.ready || referenced_columns.size() != binding.output_types.size() ||
	    result.ColumnCount() != binding.output_types.size()) {
		return false;
	}
	auto &source_chunk = source.input;
	const auto count = selected.count;
	const bool all_probe_rows_selected =
	    count == source_chunk.size() && SljitSelectedHashJoinSelectionIsIdentity(selected.MatchSelection(), count);
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	for (idx_t lhs_idx = 0; lhs_idx < lhs_column_count; lhs_idx++) {
		if (!referenced_columns[lhs_idx]) {
			continue;
		}
		const auto input_col = binding.lhs_output_column_indices[lhs_idx];
		if (input_col >= source_chunk.ColumnCount()) {
			return false;
		}
		if (all_probe_rows_selected) {
			result.data[lhs_idx].Reference(source_chunk.data[input_col]);
		} else {
			result.data[lhs_idx].Slice(source_chunk.data[input_col], selected.MatchSelection(), count);
		}
	}
	if (binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		const auto rhs_output_count = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE
		                                  ? binding.perfect_layout.rhs_output_column_count
		                                  : binding.rhs_output_column_count;
		for (idx_t rhs_idx = 0; rhs_idx < rhs_output_count; rhs_idx++) {
			const auto output_col = lhs_column_count + rhs_idx;
			if (output_col >= referenced_columns.size() || !referenced_columns[output_col]) {
				continue;
			}
			if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
				if (source.output_proof.ExactSourceFilterMatches()) {
					auto &rhs_output_probe_input_indices = source.output_proof.ExactRHSOutputProbeInputIndices();
					if (rhs_idx >= rhs_output_probe_input_indices.size()) {
						return false;
					}
					const auto input_col = rhs_output_probe_input_indices[rhs_idx];
					if (input_col == DConstants::INVALID_INDEX || input_col >= source_chunk.ColumnCount()) {
						return false;
					}
					if (all_probe_rows_selected) {
						result.data[output_col].Reference(source_chunk.data[input_col]);
					} else {
						result.data[output_col].Slice(source_chunk.data[input_col], selected.MatchSelection(), count);
					}
					continue;
				}
				if (!allow_regular_rhs_gather) {
					return false;
				}
				SljitGatherHashJoinRHSColumn(binding, selected.RowPointers(), count, rhs_idx, result.data[output_col]);
				continue;
			}
			if (binding.layout_kind != ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE ||
			    rhs_idx >= binding.perfect_layout.rhs_dictionary_buffers.size()) {
				return false;
			}
			result.data[output_col].Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_idx],
			                                   selected.BuildSelection(), count);
		}
	} else {
		for (idx_t output_col = lhs_column_count; output_col < referenced_columns.size(); output_col++) {
			if (referenced_columns[output_col]) {
				return false;
			}
		}
	}
	result.SetChildCardinality(count);
	return true;
}

static bool SljitTryMaterializeSelectedHashJoinOutputColumns(const SljitHashJoinOutputSource &source,
                                                             const SljitRuntimeHashJoinSelection &selected,
                                                             const vector<uint8_t> &referenced_columns,
                                                             DataChunk &result) {
	return SljitTryBuildSelectedHashJoinOutputColumns(source, selected, referenced_columns, result, true);
}

static bool SljitTryBuildSelectedHashJoinOutputColumnViews(const SljitHashJoinOutputSource &source,
                                                           const SljitRuntimeHashJoinSelection &selected,
                                                           const vector<uint8_t> &referenced_columns,
                                                           DataChunk &result) {
	return SljitTryBuildSelectedHashJoinOutputColumns(source, selected, referenced_columns, result, false);
}

} // namespace duckdb

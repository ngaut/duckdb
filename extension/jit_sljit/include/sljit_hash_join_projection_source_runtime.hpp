//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_executable_expression_codegen.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

static bool SljitTryRemapHashJoinProjectionSourceIndex(const vector<idx_t> &source_map, idx_t &source_index,
                                                       optional_ptr<idx_t> failed_source_index = nullptr) {
	if (source_index >= source_map.size() || source_map[source_index] == DConstants::INVALID_INDEX) {
		if (failed_source_index) {
			*failed_source_index = source_index;
		}
		return false;
	}
	source_index = source_map[source_index];
	return true;
}

static bool SljitTryRemapHashJoinProjectionSourceIndices(const vector<idx_t> &source_map, vector<idx_t> &source_indices,
                                                         optional_ptr<idx_t> failed_source_index = nullptr) {
	for (auto &source_index : source_indices) {
		if (!SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index, failed_source_index)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryRemapHashJoinProjectionPredicateSources(const vector<idx_t> &source_map,
                                                            SljitNativePredicate &predicate,
                                                            optional_ptr<idx_t> failed_source_index = nullptr) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index, failed_source_index);
	};
	auto remap_no_sources_before_scalars = [&](SljitNativePredicate &) {
		return true;
	};
	auto remap_vector_sources = [&](SljitNativePredicate &predicate) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate.source_indices,
		                                                    failed_source_index) &&
		       SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate.guard_source_indices,
		                                                    failed_source_index);
	};
	return SljitTryApplyProjectionPredicateSources(predicate, remap_source, remap_no_sources_before_scalars,
	                                               remap_vector_sources);
}

static bool SljitTryRemapHashJoinProjectionPlanSources(const vector<idx_t> &source_map,
                                                       SljitNativeRegionExpressionPlan &plan,
                                                       optional_ptr<idx_t> failed_source_index = nullptr) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index, failed_source_index);
	};
	auto remap_sources = [&](vector<idx_t> &source_indices) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, source_indices, failed_source_index);
	};
	auto remap_predicate = [&](SljitNativeRegionExpressionPlan &predicate_plan) {
		return predicate_plan.predicate &&
		       SljitTryRemapHashJoinProjectionPredicateSources(source_map, *predicate_plan.predicate,
		                                                       failed_source_index) &&
		       SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate_plan.expression_tree_source_indices,
		                                                    failed_source_index);
	};
	auto remap_constant = [&](SljitNativeRegionExpressionPlan &constant_plan) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, constant_plan.expression_tree_source_indices,
		                                                    failed_source_index);
	};
	return SljitTryApplyProjectionPlanSources(plan, remap_source, remap_sources, remap_constant, remap_predicate);
}

static bool SljitTryRemapHashJoinProjectionExpressionSources(const vector<idx_t> &source_map,
                                                             SljitExecutableRegionExpression &expr,
                                                             optional_ptr<idx_t> failed_source_index = nullptr) {
	if (!SljitTryRemapHashJoinProjectionPlanSources(source_map, expr.plan, failed_source_index)) {
		return false;
	}
	for (auto &input_source_idx : expr.input_source_indices) {
		if (!SljitTryRemapHashJoinProjectionSourceIndex(source_map, input_source_idx, failed_source_index)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildHashJoinMappedProjection(
    const vector<idx_t> &source_map, const ExecutionHashJoinProbeBinding &source_binding,
    SljitExecutableRegionOp &projection_op, SljitExecutableRegionOp &mapped_projection, optional_ptr<string> blocker,
    optional_ptr<const vector<uint8_t>> required_projection_outputs = nullptr) {
	if (source_map.size() > source_binding.output_types.size()) {
		if (blocker) {
			*blocker = "source_map_shape";
		}
		return false;
	}
	vector<idx_t> full_source_map = source_map;
	full_source_map.reserve(source_binding.output_types.size());
	for (idx_t source_idx = source_map.size(); source_idx < source_binding.output_types.size(); source_idx++) {
		full_source_map.push_back(source_idx);
	}

	mapped_projection = SljitExecutableRegionOp();
	mapped_projection.kind = SljitNativeRegionOpKind::PROJECTION;
	mapped_projection.input_types = source_binding.output_types;
	mapped_projection.output_types = projection_op.output_types;
	mapped_projection.output_not_null = projection_op.output_not_null;
	mapped_projection.projections.reserve(projection_op.projections.size());
	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		auto &projection = projection_op.projections[projection_idx];
		auto mapped_plan = projection.plan.Copy(true, false);
		if (required_projection_outputs) {
			if (projection_idx >= required_projection_outputs->size()) {
				if (blocker) {
					*blocker = "required_projection_outputs";
				}
				mapped_projection.projections.clear();
				return false;
			}
			if ((*required_projection_outputs)[projection_idx] == 0) {
				SljitExecutableRegionExpression mapped_expression;
				SljitBuildBorrowedProjectionExpression(projection, mapped_expression);
				mapped_projection.projections.push_back(std::move(mapped_expression));
				continue;
			}
		}
		idx_t failed_source_index = DConstants::INVALID_INDEX;
		if (!SljitTryRemapHashJoinProjectionPlanSources(full_source_map, mapped_plan,
		                                                optional_ptr<idx_t>(&failed_source_index))) {
			if (blocker) {
				const auto mapped_source_index = failed_source_index < full_source_map.size()
				                                     ? full_source_map[failed_source_index]
				                                     : DConstants::INVALID_INDEX;
				*blocker = "remap_projection_sources_projection_" + std::to_string(projection_idx) + "_source_" +
				           std::to_string(failed_source_index) + "_map_size_" + std::to_string(full_source_map.size()) +
				           "_mapped_" + std::to_string(mapped_source_index);
			}
			mapped_projection.projections.clear();
			return false;
		}
		SljitExecutableRegionExpression mapped_expression;
		SljitPrepareExecutableRegionExpression(mapped_plan, mapped_expression);
		string compile_error;
		if (!SljitCompilePreparedExecutableRegionExpression(mapped_expression, false, compile_error)) {
			if (blocker) {
				*blocker = "compile_mapped_projection_" + std::to_string(projection_idx) + "_" + compile_error;
			}
			mapped_projection.projections.clear();
			return false;
		}
		mapped_projection.projections.push_back(std::move(mapped_expression));
	}
	return mapped_projection.projections.size() == projection_op.projections.size();
}

static bool SljitTryBuildHashJoinMappedFilter(const vector<idx_t> &source_map,
                                              const ExecutionHashJoinProbeBinding &source_binding,
                                              SljitExecutableRegionOp &filter_op,
                                              SljitExecutableRegionOp &mapped_filter, optional_ptr<string> blocker) {
	if (source_map.size() > source_binding.output_types.size()) {
		if (blocker) {
			*blocker = "source_map_shape";
		}
		return false;
	}
	vector<idx_t> full_source_map = source_map;
	full_source_map.reserve(source_binding.output_types.size());
	for (idx_t source_idx = source_map.size(); source_idx < source_binding.output_types.size(); source_idx++) {
		full_source_map.push_back(source_idx);
	}

	D_ASSERT(filter_op.filter);
	auto mapped_plan = filter_op.filter->expression.plan.Copy(true, false);
	idx_t failed_source_index = DConstants::INVALID_INDEX;
	if (!SljitTryRemapHashJoinProjectionPlanSources(full_source_map, mapped_plan,
	                                                optional_ptr<idx_t>(&failed_source_index))) {
		if (blocker) {
			const auto mapped_source_index = failed_source_index < full_source_map.size()
			                                     ? full_source_map[failed_source_index]
			                                     : DConstants::INVALID_INDEX;
			*blocker = "remap_filter_source_" + std::to_string(failed_source_index) + "_map_size_" +
			           std::to_string(full_source_map.size()) + "_mapped_" + std::to_string(mapped_source_index);
		}
		return false;
	}

	mapped_filter = SljitExecutableRegionOp();
	mapped_filter.kind = SljitNativeRegionOpKind::FILTER;
	mapped_filter.input_types = source_binding.output_types;
	mapped_filter.output_types = source_binding.output_types;
	string compile_error;
	if (!SljitPrepareAndCompileExecutableFilter(mapped_plan, mapped_filter, compile_error)) {
		if (blocker) {
			*blocker = "compile_mapped_filter_" + compile_error;
		}
		mapped_filter = SljitExecutableRegionOp();
		return false;
	}
	return true;
}

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

static bool SljitSelectedProjectionOutputsAreSkipped(const SljitExecutableRegionOp &projection_op,
                                                     optional_ptr<const vector<idx_t>> output_to_projection,
                                                     const vector<uint8_t> &skip_projection) {
	if (output_to_projection) {
		for (auto projected_idx : *output_to_projection) {
			if (projected_idx >= projection_op.projections.size() || projected_idx >= skip_projection.size() ||
			    !skip_projection[projected_idx]) {
				return false;
			}
		}
		return true;
	}
	if (skip_projection.size() < projection_op.projections.size()) {
		return false;
	}
	for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
		if (!skip_projection[projected_idx]) {
			return false;
		}
	}
	return true;
}

static bool SljitTryMapHashJoinProbeLHSOutputColumn(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
                                                    idx_t &source_index) {
	if (source_index >= binding.lhs_output_column_indices.size()) {
		return false;
	}
	auto input_col = binding.lhs_output_column_indices[source_index];
	if (input_col >= join_input.ColumnCount()) {
		return false;
	}
	source_index = input_col;
	return true;
}

static bool SljitTryMapHashJoinProbeLHSOutputColumns(const ExecutionHashJoinProbeBinding &binding,
                                                     DataChunk &join_input, vector<idx_t> &source_indices) {
	for (auto &source_index : source_indices) {
		if (!SljitTryMapHashJoinProbeLHSOutputColumn(binding, join_input, source_index)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryMapHashJoinProbeLHSProjectionPlanSources(const ExecutionHashJoinProbeBinding &binding,
                                                             DataChunk &join_input,
                                                             SljitNativeRegionExpressionPlan &plan) {
	auto map_source = [&](idx_t &source_index) {
		return SljitTryMapHashJoinProbeLHSOutputColumn(binding, join_input, source_index);
	};
	auto map_sources = [&](vector<idx_t> &source_indices) {
		return SljitTryMapHashJoinProbeLHSOutputColumns(binding, join_input, source_indices);
	};
	auto map_constant = [&](SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto reject_predicate = [&](SljitNativeRegionExpressionPlan &) {
		return false;
	};
	return SljitTryApplyProjectionPlanSources(plan, map_source, map_sources, map_constant, reject_predicate);
}

static bool SljitTryBuildHashJoinProbeLHSProjectionExpression(const ExecutionHashJoinProbeBinding &binding,
                                                              DataChunk &join_input,
                                                              const SljitExecutableRegionExpression &source,
                                                              SljitExecutableRegionExpression &target) {
	SljitBuildBorrowedProjectionExpression(source, target);
	if (!SljitTryMapHashJoinProbeLHSProjectionPlanSources(binding, join_input, target.plan)) {
		return false;
	}
	if (!target.input_source_indices.empty()) {
		return SljitTryMapHashJoinProbeLHSOutputColumns(binding, join_input, target.input_source_indices);
	}
	return true;
}

static void SljitGatherHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers,
                                         idx_t count, idx_t rhs_col_idx, Vector &result) {
	if (ExecutionTryDirectGatherHashJoinRHSFixedColumn(binding, row_pointers, count, rhs_col_idx, result)) {
		return;
	}
	D_ASSERT(binding.hash_table);
	binding.hash_table->GatherRHSColumn(row_pointers, *FlatVector::IncrementalSelectionVector(), count, rhs_col_idx,
	                                    result);
}

static bool SljitSelectedHashJoinSelectionIsIdentity(const SelectionVector &selection, idx_t count) {
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (selection.get_index(row_idx) != row_idx) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildSelectedHashJoinOutputColumns(const ExecutionHashJoinProbeBinding &binding,
                                                       const SljitRuntimeBatchView &selected_input,
                                                       const vector<uint8_t> &referenced_columns, DataChunk &result,
                                                       bool allow_regular_rhs_gather) {
	SljitRuntimeHashJoinSelection selected;
	if (!binding.ready || !selected_input.TryGetHashJoinSelection(selected) ||
	    referenced_columns.size() != binding.output_types.size() ||
	    result.ColumnCount() != binding.output_types.size()) {
		return false;
	}
	auto &source_chunk = selected.Input();
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
				if (selected.ExactSourceFilterMatches()) {
					auto &rhs_output_probe_input_indices = selected.output_proof.ExactRHSOutputProbeInputIndices();
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

static bool SljitTryMaterializeSelectedHashJoinOutputColumns(const ExecutionHashJoinProbeBinding &binding,
                                                             const SljitRuntimeBatchView &selected_input,
                                                             const vector<uint8_t> &referenced_columns,
                                                             DataChunk &result) {
	return SljitTryBuildSelectedHashJoinOutputColumns(binding, selected_input, referenced_columns, result, true);
}

static bool SljitTryBuildSelectedHashJoinOutputColumnViews(const ExecutionHashJoinProbeBinding &binding,
                                                           const SljitRuntimeBatchView &selected_input,
                                                           const vector<uint8_t> &referenced_columns,
                                                           DataChunk &result) {
	return SljitTryBuildSelectedHashJoinOutputColumns(binding, selected_input, referenced_columns, result, false);
}

} // namespace duckdb

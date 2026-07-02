//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projection_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

static bool SljitTryRemapHashJoinProjectionSourceIndex(const vector<idx_t> &source_map, idx_t &source_index) {
	if (source_index >= source_map.size() || source_map[source_index] == DConstants::INVALID_INDEX) {
		return false;
	}
	source_index = source_map[source_index];
	return true;
}

static bool SljitTryRemapHashJoinProjectionSourceIndices(const vector<idx_t> &source_map,
                                                         vector<idx_t> &source_indices) {
	for (auto &source_index : source_indices) {
		if (!SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryRemapHashJoinProjectionPredicateSources(const vector<idx_t> &source_map,
                                                            SljitNativePredicate &predicate) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index);
	};
	auto remap_no_sources_before_scalars = [&](SljitNativePredicate &) {
		return true;
	};
	auto remap_vector_sources = [&](SljitNativePredicate &predicate) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate.source_indices) &&
		       SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate.guard_source_indices);
	};
	return SljitTryApplyProjectionPredicateSources(predicate, remap_source, remap_no_sources_before_scalars,
	                                               remap_vector_sources);
}

static bool SljitTryRemapHashJoinProjectionPlanSources(const vector<idx_t> &source_map,
                                                       SljitNativeRegionExpressionPlan &plan) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapHashJoinProjectionSourceIndex(source_map, source_index);
	};
	auto remap_sources = [&](vector<idx_t> &source_indices) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, source_indices);
	};
	auto remap_predicate = [&](SljitNativeRegionExpressionPlan &predicate_plan) {
		return predicate_plan.predicate &&
		       SljitTryRemapHashJoinProjectionPredicateSources(source_map, *predicate_plan.predicate) &&
		       SljitTryRemapHashJoinProjectionSourceIndices(source_map, predicate_plan.expression_tree_source_indices);
	};
	auto remap_constant = [&](SljitNativeRegionExpressionPlan &constant_plan) {
		return SljitTryRemapHashJoinProjectionSourceIndices(source_map, constant_plan.expression_tree_source_indices);
	};
	return SljitTryApplyProjectionPlanSources(plan, remap_source, remap_sources, remap_constant, remap_predicate);
}

static bool SljitTryRemapHashJoinProjectionExpressionInputSources(const vector<idx_t> &source_map,
                                                                  SljitExecutableRegionExpression &expr) {
	for (auto &input_source_idx : expr.input_source_indices) {
		if (!SljitTryRemapHashJoinProjectionSourceIndex(source_map, input_source_idx)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryRemapHashJoinProjectionExpressionSources(const vector<idx_t> &source_map,
                                                             SljitExecutableRegionExpression &expr) {
	if (!SljitTryRemapHashJoinProjectionPlanSources(source_map, expr.plan)) {
		return false;
	}
	return SljitTryRemapHashJoinProjectionExpressionInputSources(source_map, expr);
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

} // namespace duckdb

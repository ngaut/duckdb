//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_projection_aggregate_input_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_join_aggregate_projection.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_projection_composition.hpp"

namespace duckdb {

//! Publish one access recipe per aggregate input slot. Expression analysis and source mapping
//! happen here once; runtime dispatch consumes the bound slot and current batch storage.
static bool SljitTryBindJoinProjectionAggregateInputs(const ExecutionHashJoinProbeBinding &binding,
                                                      const vector<LogicalType> &input_types,
                                                      SljitJoinProjectionAggregateDescriptor &descriptor,
                                                      const vector<SljitExecutableRegionExpression> &prepared) {
	const auto input_column_count = input_types.size();
	if (binding.lhs_output_column_indices.size() > binding.output_types.size()) {
		return descriptor.Block("input_layout_output_shape");
	}
	const auto rhs_count = binding.output_types.size() - binding.lhs_output_column_indices.size();
	for (auto &source : descriptor.input_sources) {
		if (source.kind == SljitJoinProjectionAggregateInputKind::UNUSED) {
			continue;
		}
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
			if (source.input_idx >= input_column_count || input_types[source.input_idx] != source.type) {
				return descriptor.Block("input_layout_probe_type");
			}
			continue;
		}
		if (source.kind != SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT ||
		    source.projection_idx >= prepared.size() ||
		    source.type != prepared[source.projection_idx].plan.return_type) {
			return descriptor.Block("input_layout_projection");
		}
		auto expression = make_uniq<SljitExecutableRegionExpression>();
		SljitBuildBorrowedProjectionExpression(prepared[source.projection_idx], *expression);
		auto single = make_uniq<SljitExecutableRegionExpression>();
		idx_t column_idx;
		const bool single_source = SljitTryBuildSingleSourceProjectionExpression(*expression, *single, column_idx);
		if (single_source && column_idx >= input_column_count + rhs_count) {
			return descriptor.Block("input_layout_output_bounds");
		}
		if (single_source && SljitProjectionIsSingleSourceReferenceLike(single->plan) &&
		    column_idx < input_column_count) {
			source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
			source.input_idx = column_idx;
			if (input_types[column_idx] != source.type) {
				return descriptor.Block("input_layout_probe_type");
			}
			continue;
		}
		if (single_source && column_idx >= input_column_count) {
			source.kind = SljitProjectionIsSingleSourceReferenceLike(single->plan)
			                  ? SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT
			                  : SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_EXPRESSION;
			source.input_idx = column_idx - input_column_count;
			source.expression = std::move(single);
		} else {
			vector<uint8_t> referenced;
			if (!SljitTryCollectHashJoinProjectionExpressionSources(*expression, input_column_count + rhs_count,
			                                                        referenced)) {
				return descriptor.Block("input_layout_expression_sources");
			}
			bool lhs_only = true;
			for (idx_t idx = input_column_count; idx < referenced.size(); idx++) {
				lhs_only &= !referenced[idx];
			}
			if (lhs_only) {
				source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_EXPRESSION;
			} else {
				if (binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
					return descriptor.Block("input_layout_mixed_storage");
				}
				vector<idx_t> source_map(referenced.size(), DConstants::INVALID_INDEX);
				for (idx_t idx = 0; idx < referenced.size(); idx++) {
					if (!referenced[idx]) {
						continue;
					}
					source_map[idx] = source.expression_sources.size();
					SljitJoinProjectionAggregateInputSource input;
					if (idx < input_column_count) {
						input.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
						input.input_idx = idx;
						input.type = input_types[idx];
					} else {
						input.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT;
						input.input_idx = idx - input_column_count;
						input.type = binding.output_types[binding.lhs_output_column_indices.size() + input.input_idx];
					}
					source.expression_sources.push_back(std::move(input));
				}
				if (!SljitTryRemapExecutableExpressionInputs(source_map, *expression)) {
					return descriptor.Block("input_layout_expression_remap");
				}
				source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_MIXED_EXPRESSION;
			}
			source.expression = std::move(expression);
		}
		if (source.kind != SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT) {
			source.adapter = make_uniq<SljitExpressionAdapterScratch>();
		}
	}
	return true;
}

} // namespace duckdb

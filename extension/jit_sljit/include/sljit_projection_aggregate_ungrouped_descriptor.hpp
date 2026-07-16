//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_ungrouped_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_descriptor.hpp"

namespace duckdb {

static bool SljitTryMarkProjectionAggregateRequiredOutput(const SljitExecutableRegionOp &projection_op,
                                                          idx_t projection_idx,
                                                          vector<uint8_t> &required_projection_outputs) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= required_projection_outputs.size()) {
		return false;
	}
	required_projection_outputs[projection_idx] = 1;
	return true;
}

static bool SljitTryAddExecutableExpressionSourceColumns(const SljitExecutableRegionExpression &expr,
                                                         idx_t input_column_count,
                                                         vector<uint8_t> &referenced_columns) {
	if (!expr.input_source_indices.empty()) {
		return SljitAddProjectionSourceColumns(expr.input_source_indices, input_column_count, referenced_columns);
	}
	return SljitAddProjectionExpressionSourceColumns(expr.plan, input_column_count, referenced_columns);
}

static bool SljitTryRemapProjectionSourceIndex(idx_t &source_index, const vector<idx_t> &projection_to_input) {
	if (source_index >= projection_to_input.size() || projection_to_input[source_index] == DConstants::INVALID_INDEX) {
		return false;
	}
	source_index = projection_to_input[source_index];
	return true;
}

static bool SljitTryRemapProjectionSourceIndices(vector<idx_t> &source_indices,
                                                 const vector<idx_t> &projection_to_input) {
	for (auto &source_index : source_indices) {
		if (!SljitTryRemapProjectionSourceIndex(source_index, projection_to_input)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryRemapProjectionPredicateSourceIndices(SljitNativePredicate &predicate,
                                                          const vector<idx_t> &projection_to_input) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapProjectionSourceIndex(source_index, projection_to_input);
	};
	auto remap_sources = [&](SljitNativePredicate &predicate) {
		return SljitTryRemapProjectionSourceIndices(predicate.source_indices, projection_to_input);
	};
	auto ignore_sources = [&](SljitNativePredicate &) {
		return true;
	};
	return SljitTryApplyProjectionPredicateSources(predicate, remap_source, remap_sources, ignore_sources);
}

static bool SljitTryRemapProjectionPlanSourceIndices(SljitNativeRegionExpressionPlan &plan,
                                                     const vector<idx_t> &projection_to_input) {
	auto remap_source = [&](idx_t &source_index) {
		return SljitTryRemapProjectionSourceIndex(source_index, projection_to_input);
	};
	auto remap_sources = [&](vector<idx_t> &source_indices) {
		return SljitTryRemapProjectionSourceIndices(source_indices, projection_to_input);
	};
	auto ignore_constant = [&](SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto remap_predicate = [&](SljitNativeRegionExpressionPlan &plan) {
		if (plan.predicate) {
			return SljitTryRemapProjectionPredicateSourceIndices(*plan.predicate, projection_to_input);
		}
		return SljitTryRemapProjectionSourceIndices(plan.expression_tree_source_indices, projection_to_input);
	};
	return SljitTryApplyProjectionPlanSources(plan, remap_source, remap_sources, ignore_constant, remap_predicate);
}

static bool SljitTryBuildUngroupedAggregateRequiredProjectionOutputs(
    const SljitExecutableRegionOp &projection_op, const SljitExecutableRegionOp &aggregate_op,
    vector<uint8_t> &required_projection_outputs, optional_ptr<vector<idx_t>> fused_payload_sources = nullptr) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || !sink_info.groups.empty() ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return false;
	}
	if (!aggregate_update.fused_payload_update.Function() &&
	    aggregate_update.payload_updates.size() != sink_info.aggregates.size()) {
		return false;
	}

	required_projection_outputs.assign(projection_op.projections.size(), 0);
	if (aggregate_update.fused_payload_update.Function() &&
	    aggregate_update.payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		if (aggregate_update.combined_payload_source_not_null.size() !=
		    aggregate_update.combined_payload_source_indices.size()) {
			throw InternalException("SLJIT fused projection aggregate payload source layout is not normalized");
		}
		auto &combined_sources = aggregate_update.combined_payload_source_indices;
		for (auto source_idx : combined_sources) {
			if (!SljitTryMarkProjectionAggregateRequiredOutput(projection_op, source_idx,
			                                                   required_projection_outputs)) {
				return false;
			}
		}
		if (fused_payload_sources) {
			*fused_payload_sources = combined_sources;
		}
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.child_types.size() != 1) {
			return false;
		}
		if (!SljitTryAddExecutableExpressionSourceColumns(aggregate_update.payloads[payload_idx],
		                                                  projection_op.projections.size(),
		                                                  required_projection_outputs)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryAddJoinProjectionAggregateRequiredInput(
    const ExecutionHashJoinProbeBinding &binding, SljitJoinProjectionAggregateDescriptor &descriptor,
    vector<idx_t> &projection_to_input, idx_t projection_idx,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	auto &projection_op = descriptor.Projection();
	if (projection_idx >= projection_to_input.size()) {
		return false;
	}
	if (projection_to_input[projection_idx] != DConstants::INVALID_INDEX) {
		return true;
	}
	idx_t input_idx;
	if (!SljitTryAddJoinLHSInputAggregateInputFromProjection(binding, descriptor, projection_op, projection_idx,
	                                                         input_idx, producer_projection_op) &&
	    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
		return false;
	}
	projection_to_input[projection_idx] = input_idx;
	return true;
}

static bool SljitTryBuildRemappedUngroupedAggregatePayloads(
    const SljitExecutableAggregateUpdate &aggregate_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<idx_t> &projection_to_input, const vector<idx_t> &fused_payload_sources,
    vector<SljitExecutableRegionExpression> &remapped_payloads) {
	remapped_payloads.clear();
	remapped_payloads.reserve(aggregate_update.payloads.size());
	for (auto &payload : aggregate_update.payloads) {
		remapped_payloads.emplace_back();
		SljitBuildBorrowedProjectionExpression(payload, remapped_payloads.back());
	}
	if (aggregate_update.payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		vector<idx_t> compact_sources;
		compact_sources.reserve(fused_payload_sources.size());
		for (auto source_idx : fused_payload_sources) {
			if (source_idx >= projection_to_input.size() ||
			    projection_to_input[source_idx] == DConstants::INVALID_INDEX) {
				return false;
			}
			compact_sources.push_back(projection_to_input[source_idx]);
		}
		for (auto &payload : remapped_payloads) {
			payload.input_source_indices = compact_sources;
			payload.input_source_not_null = aggregate_update.combined_payload_source_not_null;
		}
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < remapped_payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = remapped_payloads[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (!payload.input_source_indices.empty()) {
			if (!SljitTryRemapProjectionSourceIndices(payload.input_source_indices, projection_to_input)) {
				return false;
			}
			if (payload.plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
			    payload.plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				continue;
			}
		}
		if (!SljitTryRemapProjectionPlanSourceIndices(payload.plan, projection_to_input)) {
			return false;
		}
	}
	return true;
}

static void SljitBindHashJoinDirectUngroupedAggregateDescriptor(const ExecutionHashJoinProbeBinding &binding,
                                                                SljitExecutableRegionOp &aggregate_op,
                                                                SljitJoinProjectionAggregateDescriptor &descriptor) {
	auto &direct = descriptor.direct_ungrouped_aggregate;
	direct = SljitHashJoinDirectUngroupedAggregateDescriptor();
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE || !sink_info.groups.empty() ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || aggregate_update.plan.use_grouped_state_addresses ||
	    sink_info.aggregates.size() != 1 || aggregate_update.payload_descriptors.size() != 1 ||
	    descriptor.remapped_payloads.size() != 1) {
		return;
	}
	auto &payload_descriptor = aggregate_update.payload_descriptors[0];
	if (payload_descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		if (!payload_descriptor.has_payload) {
			direct.primitive_kind = AggregatePrimitiveUpdateKind::COUNT_STAR;
		}
		return;
	}
	if (payload_descriptor.primitive_kind != AggregatePrimitiveUpdateKind::COUNT &&
	    payload_descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return;
	}
	idx_t aggregate_input_idx;
	if (!SljitTryGetExecutableReferenceInputIndex(descriptor.remapped_payloads[0], aggregate_input_idx) ||
	    aggregate_input_idx >= descriptor.input_sources.size()) {
		return;
	}
	auto &input_source = descriptor.input_sources[aggregate_input_idx];
	if (input_source.kind != SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT ||
	    input_source.projection_idx >= descriptor.Projection().projections.size()) {
		return;
	}
	idx_t join_output_idx;
	if (!SljitTryGetSingleSourceReferenceProjectionIndex(
	        descriptor.Projection().projections[input_source.projection_idx], join_output_idx)) {
		return;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_idx < lhs_column_count || join_output_idx >= binding.output_types.size()) {
		return;
	}
	const auto rhs_output_idx = join_output_idx - lhs_column_count;
	if (rhs_output_idx >= binding.rhs_output_column_count) {
		return;
	}
	auto &rhs_type = binding.output_types[join_output_idx];
	const auto rhs_physical_type = rhs_type.InternalType();
	if (input_source.type != rhs_type || payload_descriptor.input_type != rhs_physical_type ||
	    !ExecutionHashJoinRHSFixedColumnTypeSupported(rhs_type) ||
	    (payload_descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT &&
	     rhs_physical_type != PhysicalType::INT64)) {
		return;
	}
	direct.primitive_kind = payload_descriptor.primitive_kind;
	direct.rhs_output_idx = rhs_output_idx;
	direct.rhs_type = rhs_type;
	direct.rhs_physical_type = rhs_physical_type;
}

static bool SljitTryBuildProjectionUngroupedAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor, optional_ptr<SljitExecutableRegionOp> producer_projection_op) {
	auto &projection_op = descriptor.Projection();
	vector<uint8_t> required_projection_outputs;
	vector<idx_t> fused_payload_sources;
	if (!SljitTryBuildUngroupedAggregateRequiredProjectionOutputs(
	        projection_op, aggregate_op, required_projection_outputs,
	        optional_ptr<vector<idx_t>>(&fused_payload_sources))) {
		return descriptor.Block("ungrouped_payload_sources");
	}
	vector<idx_t> projection_to_input(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t projection_idx = 0; projection_idx < required_projection_outputs.size(); projection_idx++) {
		if (!required_projection_outputs[projection_idx]) {
			continue;
		}
		if (!SljitTryAddJoinProjectionAggregateRequiredInput(binding, descriptor, projection_to_input, projection_idx,
		                                                     producer_projection_op)) {
			return descriptor.Block("ungrouped_input_source");
		}
	}
	if (!SljitTryBuildRemappedUngroupedAggregatePayloads(
	        aggregate_op.aggregate_update, aggregate_op.aggregate_update.plan.sink_info.aggregates, projection_to_input,
	        fused_payload_sources, descriptor.remapped_payloads)) {
		return descriptor.Block("ungrouped_payload_remap");
	}
	descriptor.payload_source_layout = aggregate_op.aggregate_update.payload_source_layout;
	if (descriptor.payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		if (descriptor.remapped_payloads.empty()) {
			return descriptor.Block("ungrouped_payload_sources");
		}
		descriptor.payload_source_indices = descriptor.remapped_payloads.front().input_source_indices;
		descriptor.payload_source_not_null = descriptor.remapped_payloads.front().input_source_not_null;
	}
	SljitBindHashJoinDirectUngroupedAggregateDescriptor(binding, aggregate_op, descriptor);
	descriptor.MarkReady();
	return true;
}

} // namespace duckdb

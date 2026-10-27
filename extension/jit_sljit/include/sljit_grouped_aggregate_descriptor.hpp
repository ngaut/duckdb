//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_source_indices.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_projection_composition.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool SljitTryAddJoinProjectionAggregateInput(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                    vector<idx_t> &projection_to_input, idx_t projection_idx,
                                                    idx_t &input_idx) {
	auto &projection_op = descriptor.Projection();
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	if (projection_to_input[projection_idx] != DConstants::INVALID_INDEX) {
		input_idx = projection_to_input[projection_idx];
		return true;
	}
	auto &projection = projection_op.projections[projection_idx];
	auto &output_type = projection_op.output_types[projection_idx];
	if (projection.plan.return_type != output_type) {
		return false;
	}
	input_idx = descriptor.input_sources.size();
	projection_to_input[projection_idx] = input_idx;
	SljitJoinProjectionAggregateInputSource source;
	source.kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
	source.projection_idx = projection_idx;
	source.type = output_type;
	descriptor.input_sources.push_back(std::move(source));
	return true;
}

static bool SljitTryAddJoinLHSInputAggregateInput(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                  idx_t input_column_idx, const LogicalType &input_type,
                                                  idx_t &input_idx) {
	for (idx_t existing_idx = 0; existing_idx < descriptor.input_sources.size(); existing_idx++) {
		auto &source = descriptor.input_sources[existing_idx];
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT &&
		    source.input_idx == input_column_idx && source.type == input_type) {
			input_idx = existing_idx;
			return true;
		}
	}
	input_idx = descriptor.input_sources.size();
	SljitJoinProjectionAggregateInputSource source;
	source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
	source.input_idx = input_column_idx;
	source.type = input_type;
	descriptor.input_sources.push_back(std::move(source));
	return true;
}

//! Resolve a producer output by its declared ordinal, never by reverse-searching physical inputs.
static bool SljitTryResolveProducerProbeInput(const SljitJoinProjectionAggregateDescriptor &descriptor,
                                              optional_ptr<SljitExecutableRegionOp> producer_projection_op,
                                              idx_t producer_output_idx, idx_t &probe_input_idx,
                                              const LogicalType *&source_type,
                                              SljitExecutableRegionExpression &producer_expr) {
	if (!producer_projection_op || !descriptor.producer_layout ||
	    producer_output_idx >= descriptor.producer_layout->ColumnCount() ||
	    producer_output_idx >= producer_projection_op->projections.size() ||
	    producer_output_idx >= producer_projection_op->output_types.size()) {
		return false;
	}
	const auto &column = descriptor.producer_layout->Column(producer_output_idx);
	idx_t source_idx;
	if (!column.HasSource() ||
	    !SljitTryBuildSingleSourceProjectionExpression(producer_projection_op->projections[producer_output_idx],
	                                                   producer_expr, source_idx) ||
	    source_idx != column.source_index || source_idx >= producer_projection_op->input_types.size() ||
	    producer_projection_op->input_types[source_idx] != column.source_type ||
	    producer_expr.plan.return_type != column.type ||
	    producer_projection_op->output_types[producer_output_idx] != column.type) {
		return false;
	}
	probe_input_idx = source_idx;
	source_type = &producer_projection_op->input_types[source_idx];
	return true;
}

static bool SljitTryAddJoinLHSInputAggregateInputFromProjection(
    const ExecutionHashJoinProbeBinding &binding, SljitJoinProjectionAggregateDescriptor &descriptor,
    SljitExecutableRegionOp &projection_op, idx_t projection_idx, idx_t &input_idx,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
		return false;
	}
	if (descriptor.producer_layout) {
		if (join_output_source_index >= binding.lhs_output_column_indices.size()) {
			return false;
		}
		const auto producer_output_idx = binding.lhs_output_column_indices[join_output_source_index];
		idx_t input_col;
		const LogicalType *producer_source_type = nullptr;
		SljitExecutableRegionExpression producer_expr;
		if (!SljitTryResolveProducerProbeInput(descriptor, producer_projection_op, producer_output_idx, input_col,
		                                       producer_source_type, producer_expr) ||
		    !producer_source_type || !SljitProjectionIsSingleSourceReferenceLike(producer_expr.plan)) {
			return false;
		}
		auto &projection_type = projection_op.output_types[projection_idx];
		if (remapped_expr.plan.return_type != projection_type || *producer_source_type != projection_type) {
			return false;
		}
		return SljitTryAddJoinLHSInputAggregateInput(descriptor, input_col, projection_type, input_idx);
	}
	if (join_output_source_index >= binding.lhs_output_column_indices.size() ||
	    join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	auto &source_type = binding.output_types[join_output_source_index];
	auto &projection_type = projection_op.output_types[projection_idx];
	if (remapped_expr.plan.return_type != projection_type || source_type != projection_type) {
		return false;
	}
	return SljitTryAddJoinLHSInputAggregateInput(
	    descriptor, binding.lhs_output_column_indices[join_output_source_index], source_type, input_idx);
}

static bool SljitInputVectorGroupKeyHasOriginalInput(const ExecutionRowPointerGroupKeySource &group_source) {
	return group_source.ready && group_source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       group_source.input_vector_index != DConstants::INVALID_INDEX &&
	       group_source.source_type.InternalType() != PhysicalType::INVALID;
}

static bool SljitProjectionOutputKnownNotNull(SljitExecutableRegionOp &projection_op, idx_t projection_idx) {
	return projection_idx < projection_op.output_not_null.size() && projection_op.output_not_null[projection_idx];
}

static bool SljitProjectionReferencesAllValidHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding,
                                                               SljitExecutableRegionOp &projection_op,
                                                               idx_t projection_idx) {
	if (projection_idx >= projection_op.projections.size()) {
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count) {
		return false;
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	return ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) && rhs_source.all_valid;
}

static bool SljitProjectionOutputIsCountOnePayload(const ExecutionHashJoinProbeBinding &binding,
                                                   SljitExecutableRegionOp &projection_op, idx_t projection_idx) {
	return SljitProjectionOutputKnownNotNull(projection_op, projection_idx) ||
	       SljitProjectionReferencesAllValidHashJoinRHSColumn(binding, projection_op, projection_idx);
}

static bool SljitTryMapProducerGroupSource(
    const ExecutionHashJoinProbeBinding &binding, const SljitNativeHashJoinProbePlan &probe_plan,
    const SljitJoinProjectionAggregateDescriptor &descriptor, SljitExecutableRegionOp &projection_op,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op, const ExecutionRegionGroupInput &group,
    ExecutionRowPointerGroupKeySource &group_source, bool &mapped, string &blocker) {
	mapped = false;
	if (!producer_projection_op || !descriptor.producer_layout ||
	    group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
		return true;
	}
	const auto projection_idx = group.input_index;
	if (!SljitProjectionHasOutput(projection_op, projection_idx)) {
		blocker = "producer_group_projection_index";
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index)) {
		blocker = "producer_group_projection_source";
		return false;
	}
	idx_t probe_input_idx;
	idx_t condition_idx;
	bool repeats_with_row_pointer;
	if (!SljitTryResolveHashJoinMatchedProbeInputForOutput(binding, join_output_source_index, probe_input_idx,
	                                                       condition_idx, repeats_with_row_pointer)) {
		return true;
	}
	// LHS output indices address the producer's output; RHS equality aliases address the
	// already-remapped probe input. Recover the original key output from the semantic probe plan.
	idx_t producer_output_idx;
	if (join_output_source_index < binding.lhs_output_column_indices.size()) {
		producer_output_idx = binding.lhs_output_column_indices[join_output_source_index];
	} else {
		if (condition_idx >= probe_plan.keys.size()) {
			blocker = "producer_group_key";
			return false;
		}
		producer_output_idx = probe_plan.keys[condition_idx].key_input_index;
	}
	SljitExecutableRegionExpression producer_expr;
	const LogicalType *producer_source_type = nullptr;
	if (!SljitTryResolveProducerProbeInput(descriptor, producer_projection_op, producer_output_idx, probe_input_idx,
	                                       producer_source_type, producer_expr) ||
	    !producer_source_type) {
		blocker = "producer_group_output_map";
		return false;
	}
	const auto &source_type = *producer_source_type;
	if (!repeats_with_row_pointer) {
		condition_idx = DConstants::INVALID_INDEX;
		SljitTryGetHashJoinLHSInputConditionIndex(binding, probe_input_idx,
		                                          binding.output_types[join_output_source_index], condition_idx);
	}
	ExecutionRowPointerGroupKeySource producer_group_source;
	SljitInitializeInputVectorGroupKeySource(probe_input_idx, source_type, group.type, producer_group_source,
	                                         condition_idx);
	producer_group_source.input_vector_repeats_with_row_pointer = repeats_with_row_pointer;
	SljitAttachHashJoinBuildConditionType(binding, producer_group_source, condition_idx);
	vector<SljitNativeRegionExpressionPlan> producer_projection;
	producer_projection.push_back(SljitCopySemanticExpressionPlan(producer_expr));
	SljitNativeRegionExpressionPlan composed_group_plan;
	if (!TryComposeNativeProjection(producer_projection, remapped_expr.plan, composed_group_plan, false)) {
		blocker = "producer_group_compose_producer_kind_" + to_string(static_cast<int>(producer_expr.plan.kind)) +
		          "_group_kind_" + to_string(static_cast<int>(remapped_expr.plan.kind));
		return false;
	}
	if (!SljitTryFinalizeRowPointerGroupKeySource(composed_group_plan, group.type, producer_group_source)) {
		blocker = "producer_group_cast_kind_" + to_string(static_cast<int>(composed_group_plan.kind)) + "_source_" +
		          source_type.ToString() + "_return_" + composed_group_plan.return_type.ToString() + "_target_" +
		          group.type.ToString() + "_source_index_" + to_string(composed_group_plan.source_index);
		return false;
	}
	group_source = std::move(producer_group_source);
	mapped = true;
	return true;
}

static bool SljitTryBuildProjectionRowPointerAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, const SljitNativeHashJoinProbePlan &probe_plan,
    SljitExecutableRegionOp &aggregate_op, SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	auto &projection_op = descriptor.Projection();
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || !aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return descriptor.Block("aggregate_shape");
	}
	string group_source_blocker;
	vector<uint8_t> group_source_uses_projection_output;
	if (!SljitTryBuildRowPointerGroupKeySources(binding, projection_op, aggregate_op, descriptor.group_sources,
	                                            optional_ptr<vector<uint8_t>>(&group_source_uses_projection_output),
	                                            optional_ptr<string>(&group_source_blocker))) {
		if (group_source_blocker.empty()) {
			group_source_blocker = "group_sources";
		}
		return descriptor.Block(group_source_blocker.c_str());
	}
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		bool mapped = false;
		string producer_group_blocker;
		if (!SljitTryMapProducerGroupSource(binding, probe_plan, descriptor, projection_op, producer_projection_op,
		                                    sink_info.groups[group_idx], descriptor.group_sources[group_idx], mapped,
		                                    producer_group_blocker)) {
			if (producer_group_blocker.empty()) {
				producer_group_blocker = "producer_group_source";
			}
			return descriptor.Block(producer_group_blocker.c_str());
		}
		if (mapped) {
			group_source_uses_projection_output[group_idx] = 0;
		}
	}

	vector<idx_t> projection_to_input(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		auto &group_source = descriptor.group_sources[group_idx];
		if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		idx_t input_idx;
		const bool uses_projection_output = group_idx < group_source_uses_projection_output.size() &&
		                                    group_source_uses_projection_output[group_idx] != 0;
		if (!uses_projection_output && SljitInputVectorGroupKeyHasOriginalInput(group_source) &&
		    SljitInputVectorGroupKeySourceSupportsMaterialization(group_source)) {
			if (!SljitTryAddJoinLHSInputAggregateInput(descriptor, group_source.input_vector_index,
			                                           group_source.source_type, input_idx)) {
				return descriptor.Block("input_group_source");
			}
			group_source.input_vector_index = input_idx;
		} else {
			const auto projection_idx = group.input_index;
			if (!SljitProjectionHasOutput(projection_op, projection_idx) ||
			    projection_op.output_types[projection_idx].InternalType() != group.type.InternalType() ||
			    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
				return descriptor.Block("input_group_projection");
			}
			SljitInitializeInputVectorGroupKeySource(input_idx, group.type, group.type, group_source);
			group_source.ready = true;
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
		}
	}
	auto add_count_star_payload = [&]() {
		descriptor.payload_source_indices.push_back(DConstants::INVALID_INDEX);
		return true;
	};
	auto add_fused_payload_source = [&](idx_t semantic_projection_idx) {
		const auto projection_idx = semantic_projection_idx;
		if (!SljitProjectionHasOutput(projection_op, projection_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		idx_t input_idx;
		if (!SljitTryAddJoinLHSInputAggregateInputFromProjection(binding, descriptor, projection_op, projection_idx,
		                                                         input_idx, producer_projection_op) &&
		    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	auto add_direct_payload_source = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		const auto projection_idx = aggregate.payload_index;
		if (!SljitProjectionHasOutput(projection_op, projection_idx)) {
			return descriptor.Block("payload_projection");
		}
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT &&
		    SljitProjectionOutputIsCountOnePayload(binding, projection_op, projection_idx)) {
			descriptor.payload_source_indices.push_back(DConstants::INVALID_INDEX);
			return true;
		}
		idx_t input_idx;
		if ((!SljitTryAddJoinLHSInputAggregateInputFromProjection(binding, descriptor, projection_op, projection_idx,
		                                                          input_idx, producer_projection_op) &&
		     !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) ||
		    descriptor.input_sources[input_idx].type.InternalType() != aggregate.child_types[0].InternalType()) {
			return descriptor.Block("payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	SljitAggregatePayloadSourceLayout payload_source_layout;
	auto check_direct_payload = [&](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, payload_source_layout, "payload_contract", add_count_star_payload,
	        add_fused_payload_source, check_direct_payload, add_direct_payload_source,
	        [&](const char *blocker) { return descriptor.Block(blocker); })) {
		return false;
	}
	D_ASSERT(payload_source_layout != SljitAggregatePayloadSourceLayout::FUSED_COMBINED ||
	         !descriptor.payload_source_indices.empty());
	if (descriptor.payload_source_indices.empty()) {
		return descriptor.Block("payload_sources");
	}
	descriptor.payload_source_layout = payload_source_layout;
	return true;
}

static bool SljitDescriptorUsesRowPointerGroupSource(const SljitJoinProjectionAggregateDescriptor &descriptor) {
	for (auto &source : descriptor.group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb

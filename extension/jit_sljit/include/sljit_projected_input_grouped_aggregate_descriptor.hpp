//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_input_grouped_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_source_indices.hpp"
#include "sljit_executable_range_analysis.hpp"
#include "sljit_expression_plan_tree.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"
#include "sljit_projection_chain_primitive.hpp"

namespace duckdb {

struct SljitProjectedInputGroupedAggregateDescriptor {
	SljitExecutableRegionOp projection;
	vector<ExecutionRowPointerGroupKeySource> group_sources;
	vector<idx_t> payload_projection_indices;
	vector<idx_t> payload_source_indices;
	vector<bool> payload_source_not_null;
	SljitAggregatePayloadSourceLayout payload_source_layout = SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;
	vector<LogicalType> input_types;

	bool Ready() const {
		return projection.kind == SljitNativeRegionOpKind::PROJECTION && !group_sources.empty() &&
		       !payload_source_indices.empty() && payload_source_not_null.size() == payload_source_indices.size();
	}
};

static bool SljitProjectedInputSignedAffineGroupType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool SljitProjectedInputAffineOffsetFitsType(const LogicalType &type, int64_t value) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return value >= NumericLimits<int8_t>::Minimum() && value <= NumericLimits<int8_t>::Maximum();
	case LogicalTypeId::SMALLINT:
		return value >= NumericLimits<int16_t>::Minimum() && value <= NumericLimits<int16_t>::Maximum();
	case LogicalTypeId::INTEGER:
		return value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum();
	case LogicalTypeId::BIGINT:
		return true;
	default:
		return false;
	}
}

static bool SljitProjectedInputExpressionRangesFitDeclaredTypes(const SljitExecutableRegionOp &projection_op,
                                                                const ExecutionExpressionIR &node) {
	SljitExecutableInt128Range range;
	Value min_value;
	Value max_value;
	if (!SljitExecutableExpressionTreeRange(node, projection_op.input_min_values, projection_op.input_max_values,
	                                        range) ||
	    range.type != node.return_type || !SljitExecutableRangeValue(node.return_type, range.min, min_value) ||
	    !SljitExecutableRangeValue(node.return_type, range.max, max_value)) {
		return false;
	}
	if (node.left && !SljitProjectedInputExpressionRangesFitDeclaredTypes(projection_op, *node.left)) {
		return false;
	}
	if (node.right && !SljitProjectedInputExpressionRangesFitDeclaredTypes(projection_op, *node.right)) {
		return false;
	}
	return true;
}

static bool SljitTryReadProjectedInputInvariant(const SljitExecutableRegionOp &projection_op,
                                                const ExecutionExpressionIR &node, int64_t &value,
                                                idx_t &validity_guard_idx) {
	validity_guard_idx = DConstants::INVALID_INDEX;
	if (!SljitProjectedInputSignedAffineGroupType(node.return_type)) {
		return false;
	}
	hugeint_t min_value;
	hugeint_t max_value;
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return SljitExecutableValueToHugeint(node.constant, node.return_type, min_value) &&
		       Hugeint::TryCast(min_value, value);
	}
	if (node.kind != ExecutionExpressionIRKind::REFERENCE || node.ref_index >= projection_op.input_types.size() ||
	    node.ref_index >= projection_op.input_min_values.size() ||
	    node.ref_index >= projection_op.input_max_values.size() ||
	    projection_op.input_types[node.ref_index] != node.return_type ||
	    !SljitExecutableValueToHugeint(projection_op.input_min_values[node.ref_index], node.return_type, min_value) ||
	    !SljitExecutableValueToHugeint(projection_op.input_max_values[node.ref_index], node.return_type, max_value) ||
	    min_value != max_value || !Hugeint::TryCast(min_value, value)) {
		return false;
	}
	validity_guard_idx = node.ref_index;
	return true;
}

static bool SljitTryReadProjectedInputVariableReference(const SljitExecutableRegionOp &projection_op,
                                                        const ExecutionExpressionIR &node,
                                                        const LogicalType &equivalence_type, idx_t &source_idx) {
	if (node.kind != ExecutionExpressionIRKind::REFERENCE || node.return_type != equivalence_type ||
	    node.ref_index >= projection_op.input_types.size() ||
	    projection_op.input_types[node.ref_index] != equivalence_type) {
		return false;
	}
	source_idx = node.ref_index;
	return true;
}

static bool SljitTryBuildAffineProjectedInputGroupSource(const SljitExecutableRegionOp &projection_op,
                                                         const ExecutionRegionGroupInput &group,
                                                         ExecutionRowPointerGroupKeySource &group_source) {
	if (group.input_index >= projection_op.projections.size() ||
	    group.input_index >= projection_op.output_types.size() ||
	    projection_op.output_types[group.input_index] != group.type ||
	    !SljitProjectedInputSignedAffineGroupType(group.type)) {
		return false;
	}
	auto &plan = projection_op.projections[group.input_index].plan;
	auto expression_tree = CopySljitExpressionPlanAsInputTree(plan);
	if (plan.return_type != group.type || !expression_tree || expression_tree->return_type != group.type ||
	    projection_op.input_min_values.size() != projection_op.input_types.size() ||
	    projection_op.input_max_values.size() != projection_op.input_types.size() ||
	    !SljitProjectedInputExpressionRangesFitDeclaredTypes(projection_op, *expression_tree)) {
		return false;
	}
	auto affine_expression = expression_tree.get();
	while (affine_expression->kind == ExecutionExpressionIRKind::CAST) {
		if (affine_expression->try_cast || !affine_expression->left ||
		    !SljitProjectedInputSignedAffineGroupType(affine_expression->return_type) ||
		    !SljitProjectedInputSignedAffineGroupType(affine_expression->left->return_type)) {
			return false;
		}
		affine_expression = affine_expression->left.get();
	}
	if (affine_expression->kind != ExecutionExpressionIRKind::BINARY || !affine_expression->left ||
	    !affine_expression->right ||
	    (affine_expression->binary_op != ExecutionExpressionBinaryOp::ADD &&
	     affine_expression->binary_op != ExecutionExpressionBinaryOp::SUBTRACT) ||
	    !SljitProjectedInputSignedAffineGroupType(affine_expression->return_type)) {
		return false;
	}
	const auto &equivalence_type = affine_expression->return_type;

	idx_t source_idx = DConstants::INVALID_INDEX;
	idx_t validity_guard_idx = DConstants::INVALID_INDEX;
	int64_t offset = 0;
	int64_t invariant;
	if (SljitTryReadProjectedInputVariableReference(projection_op, *affine_expression->left, equivalence_type,
	                                                source_idx) &&
	    SljitTryReadProjectedInputInvariant(projection_op, *affine_expression->right, invariant, validity_guard_idx)) {
		if (affine_expression->binary_op == ExecutionExpressionBinaryOp::SUBTRACT) {
			if (invariant == NumericLimits<int64_t>::Minimum()) {
				return false;
			}
			offset = -invariant;
		} else {
			offset = invariant;
		}
	} else if (affine_expression->binary_op == ExecutionExpressionBinaryOp::ADD &&
	           SljitTryReadProjectedInputVariableReference(projection_op, *affine_expression->right, equivalence_type,
	                                                       source_idx) &&
	           SljitTryReadProjectedInputInvariant(projection_op, *affine_expression->left, invariant,
	                                               validity_guard_idx)) {
		offset = invariant;
	} else {
		return false;
	}
	if (source_idx >= projection_op.input_types.size() || projection_op.input_types[source_idx] != equivalence_type ||
	    !SljitProjectedInputAffineOffsetFitsType(equivalence_type, offset) ||
	    (validity_guard_idx != DConstants::INVALID_INDEX &&
	     (validity_guard_idx >= projection_op.input_types.size() ||
	      projection_op.input_types[validity_guard_idx] != equivalence_type))) {
		return false;
	}

	SljitInitializeInputVectorGroupKeySource(source_idx, equivalence_type, group.type, group_source);
	group_source.ready = true;
	group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	group_source.output_transform_kind = ExecutionGroupKeyOutputTransformKind::ADD_CONSTANT;
	group_source.output_transform_constant = offset;
	const bool source_not_null =
	    source_idx < projection_op.input_not_null.size() && projection_op.input_not_null[source_idx];
	const bool guard_not_null =
	    validity_guard_idx == DConstants::INVALID_INDEX ||
	    (validity_guard_idx < projection_op.input_not_null.size() && projection_op.input_not_null[validity_guard_idx]);
	group_source.output_transform_validity_guard_index =
	    guard_not_null ? DConstants::INVALID_INDEX : validity_guard_idx;
	group_source.all_valid = source_not_null && guard_not_null;
	return true;
}

static bool
SljitProjectedInputGroupedAggregateCanUseSourceInput(const SljitProjectedInputGroupedAggregateDescriptor &descriptor) {
	if (descriptor.payload_projection_indices.size() != descriptor.payload_source_indices.size()) {
		return false;
	}
	if (descriptor.payload_source_not_null.size() != descriptor.payload_source_indices.size()) {
		return false;
	}
	for (auto &group_source : descriptor.group_sources) {
		if (!SljitInputVectorGroupKeySourceSupportsMaterialization(group_source)) {
			return false;
		}
	}
	for (idx_t payload_idx = 0; payload_idx < descriptor.payload_projection_indices.size(); payload_idx++) {
		const auto projection_idx = descriptor.payload_projection_indices[payload_idx];
		const auto source_idx = descriptor.payload_source_indices[payload_idx];
		if (projection_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (source_idx == DConstants::INVALID_INDEX || source_idx >= descriptor.input_types.size()) {
			return false;
		}
	}
	return true;
}

static bool SljitTryBuildProjectedInputGroupSource(const SljitExecutableRegionOp &projection_op,
                                                   const ExecutionRegionGroupInput &group,
                                                   ExecutionRowPointerGroupKeySource &group_source,
                                                   optional_ptr<string> blocker = nullptr) {
	auto block = [&](string reason) {
		if (blocker) {
			*blocker = std::move(reason);
		}
		return false;
	};
	if (group.input_index >= projection_op.projections.size() ||
	    group.input_index >= projection_op.output_types.size() ||
	    projection_op.output_types[group.input_index].InternalType() != group.type.InternalType()) {
		return block("projection");
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t source_idx;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[group.input_index], remapped_expr,
	                                                   source_idx)) {
		if (SljitTryBuildAffineProjectedInputGroupSource(projection_op, group, group_source)) {
			return true;
		}
		return block("group_equivalence_kind_" +
		             to_string(static_cast<int>(projection_op.projections[group.input_index].plan.kind)));
	}
	if (source_idx >= projection_op.input_types.size()) {
		return block("source_bounds_" + to_string(source_idx) + "_inputs_" +
		             to_string(projection_op.input_types.size()));
	}
	SljitInitializeInputVectorGroupKeySource(source_idx, projection_op.input_types[source_idx], group.type,
	                                         group_source);
	group_source.all_valid =
	    source_idx < projection_op.input_not_null.size() && projection_op.input_not_null[source_idx];
	if (!SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source)) {
		if (SljitTryBuildAffineProjectedInputGroupSource(projection_op, group, group_source)) {
			return true;
		}
		return block("finalize_kind_" + to_string(static_cast<int>(remapped_expr.plan.kind)) + "_source_" +
		             to_string(source_idx) + "_source_type_" + projection_op.input_types[source_idx].ToString() +
		             "_group_type_" + group.type.ToString() + "_return_type_" +
		             remapped_expr.plan.return_type.ToString());
	}
	return true;
}

static bool SljitTryResolveProjectedInputPayloadSource(const SljitExecutableRegionOp &projection_op,
                                                       idx_t projection_idx, idx_t &source_idx, bool &source_not_null) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   source_idx) ||
	    source_idx >= projection_op.input_types.size() ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
		return false;
	}
	source_not_null = SljitInputSourceKnownNotNull(remapped_expr.input_source_not_null, 0);
	return projection_op.output_types[projection_idx] == projection_op.input_types[source_idx];
}

static bool SljitTryBindProjectedInputPayloadOutput(const SljitExecutableRegionOp &projection_op, idx_t projection_idx,
                                                    idx_t &source_idx, bool &source_not_null) {
	source_not_null = false;
	if (SljitTryResolveProjectedInputPayloadSource(projection_op, projection_idx, source_idx, source_not_null)) {
		return true;
	}
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size() ||
	    projection_op.projections[projection_idx].plan.return_type != projection_op.output_types[projection_idx]) {
		return false;
	}
	source_idx = DConstants::INVALID_INDEX;
	return true;
}

static bool SljitTryBuildProjectedInputGroupedAggregateDescriptor(
    const vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t aggregate_idx, optional_ptr<SljitProjectedInputGroupedAggregateDescriptor> descriptor = nullptr,
    optional_ptr<string> blocker = nullptr) {
	auto block = [&](string reason) {
		if (blocker) {
			*blocker = std::move(reason);
		}
		return false;
	};
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx) ||
	    aggregate_idx >= ops.size() || ops[aggregate_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return block("shape");
	}
	auto &aggregate_op = ops[aggregate_idx];
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    !aggregate_update.plan.UsesPrimitivePayloads() || !aggregate_update.plan.use_grouped_state_addresses ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    aggregate_update.payloads.size() != sink_info.aggregates.size()) {
		return block("aggregate_shape");
	}

	auto semantic_projection = make_uniq<SljitExecutableRegionOp>();
	if (!SljitBuildProjectionChainSemanticProjection(ops, first_projection_idx, final_projection_idx,
	                                                 *semantic_projection)) {
		return block("projection_compose");
	}
	vector<ExecutionRowPointerGroupKeySource> group_sources;
	group_sources.reserve(sink_info.groups.size());
	for (auto &group : sink_info.groups) {
		ExecutionRowPointerGroupKeySource group_source;
		string group_blocker;
		if (!SljitTryBuildProjectedInputGroupSource(*semantic_projection, group, group_source,
		                                            optional_ptr<string>(&group_blocker))) {
			return block("group" + to_string(group_sources.size()) + "_source_" + group_blocker);
		}
		group_sources.push_back(std::move(group_source));
	}

	vector<idx_t> payload_source_indices;
	vector<bool> payload_source_not_null;
	vector<idx_t> payload_projection_indices;
	auto add_count_star_payload = [&]() {
		payload_projection_indices.push_back(DConstants::INVALID_INDEX);
		payload_source_indices.push_back(DConstants::INVALID_INDEX);
		payload_source_not_null.push_back(false);
		return true;
	};
	auto add_fused_payload_source = [&](idx_t projection_idx) {
		idx_t source_idx;
		bool source_not_null;
		if (!SljitTryBindProjectedInputPayloadOutput(*semantic_projection, projection_idx, source_idx,
		                                             source_not_null)) {
			return block("fused_payload_source");
		}
		payload_projection_indices.push_back(projection_idx);
		payload_source_indices.push_back(source_idx);
		payload_source_not_null.push_back(source_not_null);
		return true;
	};
	auto check_direct_payload = [&](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	auto add_direct_payload_source = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		idx_t source_idx;
		bool source_not_null;
		if (aggregate.payload_index >= semantic_projection->output_types.size() ||
		    semantic_projection->output_types[aggregate.payload_index] != aggregate.child_types[0]) {
			return block("direct_payload_type");
		}
		if (!SljitTryBindProjectedInputPayloadOutput(*semantic_projection, aggregate.payload_index, source_idx,
		                                             source_not_null)) {
			return block("direct_payload_source");
		}
		payload_projection_indices.push_back(aggregate.payload_index);
		payload_source_indices.push_back(source_idx);
		payload_source_not_null.push_back(source_not_null);
		return true;
	};
	SljitAggregatePayloadSourceLayout payload_source_layout;
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, payload_source_layout, "payload_projection", add_count_star_payload,
	        add_fused_payload_source, check_direct_payload, add_direct_payload_source,
	        [&](const char *reason) { return block(reason); })) {
		return false;
	}
	if (payload_source_indices.empty()) {
		return block("payload_sources");
	}
	if (descriptor) {
		descriptor->projection = std::move(*semantic_projection);
		descriptor->group_sources = std::move(group_sources);
		descriptor->payload_projection_indices = std::move(payload_projection_indices);
		descriptor->payload_source_indices = std::move(payload_source_indices);
		descriptor->payload_source_not_null = std::move(payload_source_not_null);
		descriptor->payload_source_layout = payload_source_layout;
		descriptor->input_types = descriptor->projection.input_types;
	}
	return true;
}

} // namespace duckdb

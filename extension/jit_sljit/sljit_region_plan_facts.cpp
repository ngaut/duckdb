//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_facts.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_aggregate_contract_utils.hpp"
#include "sljit_hash_join_probe_codegen_validation.hpp"

namespace duckdb {

static bool SljitNativeRegionExpressionGeneratesCode(const SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::CONSTANT:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
		return false;
	default:
		return true;
	}
}

static bool SljitNativeRegionExpressionsGenerateCode(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	for (auto &expr : expressions) {
		if (SljitNativeRegionExpressionGeneratesCode(expr)) {
			return true;
		}
	}
	return false;
}

static bool SljitNativeHashJoinProbeGeneratesDeferredCode(const SljitNativeHashJoinProbePlan &plan) {
	string unused_error;
	return SljitValidateHashJoinProbePlan(plan, unused_error);
}

static bool SljitNativeRegionOpGeneratesMachineCode(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.filter);
	case SljitNativeRegionOpKind::PROJECTION:
		return SljitNativeRegionExpressionsGenerateCode(op.projections);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return SljitNativeHashJoinProbeGeneratesDeferredCode(op.hash_join_probe) ||
		       (op.hash_join_probe.residual_predicate &&
		        SljitNativeRegionExpressionGeneratesCode(op.hash_join_probe.residual_filter));
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return SljitNativeRegionExpressionsGenerateCode(op.nested_loop_join_build.rhs_conditions);
	case SljitNativeRegionOpKind::ORDER_SINK:
		return SljitNativeRegionExpressionsGenerateCode(op.order_sink.order_keys);
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return false;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return op.aggregate_update.UsesPrimitivePayloads();
	default:
		return false;
	}
}

static bool SljitNativeRegionOpIsNativeSink(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return op.hash_join_build.sink_info.kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
		       op.hash_join_build.sink_info.hash_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return op.nested_loop_join_build.sink_info.kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD &&
		       op.nested_loop_join_build.sink_info.nested_loop_join_contract.native_build_contract.status ==
		           ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::APPEND_SINK:
		return (op.append_sink.sink_info.kind == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
		        op.append_sink.sink_info.kind == ExecutionRegionSinkKind::MATERIALIZATION) &&
		       op.append_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::ORDER_SINK:
		return op.order_sink.sink_info.kind == ExecutionRegionSinkKind::SORT &&
		       op.order_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return op.delim_join_sink.sink_info.kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK &&
		       op.delim_join_sink.sink_info.native_sink_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
		auto &sink = op.aggregate_update.sink_info;
		return (sink.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
		        sink.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) &&
		       sink.aggregate_contract.native_state_update_contract.status == ExecutionRegionStateContractStatus::READY;
	}
	default:
		return false;
	}
}

static string SljitNativeRegionOpBoundaryBlocker(const SljitNativeRegionOpPlan &op) {
	if (op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::PROJECTION) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return string();
	}
	if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		if (op.aggregate_update.UsesPrimitivePayloads()) {
			return string();
		}
		if (SljitNativeRegionOpIsNativeSink(op)) {
			return string();
		}
		return "operator-contract-blocker:aggregate-update-generated-payload-missing";
	}
	if (SljitNativeRegionOpIsNativeSink(op)) {
		return string();
	}
	return "operator-contract-blocker:unsupported-operator-boundary;stage=" +
	       string(SljitNativeRegionOpKindName(op.kind));
}

static bool SljitRegionPlanBooleanType(const LogicalType &type) {
	return type.id() == LogicalTypeId::BOOLEAN && type.InternalType() == PhysicalType::BOOL;
}

static bool SljitRegionPlanLocalSourceReferencesColumn(const vector<idx_t> &input_source_indices,
                                                       idx_t local_source_index, idx_t column_index) {
	if (input_source_indices.empty()) {
		return local_source_index == column_index;
	}
	return local_source_index < input_source_indices.size() && input_source_indices[local_source_index] == column_index;
}

static bool SljitRegionPlanPredicateReferencesMarker(const SljitNativePredicate &predicate, idx_t marker_index) {
	if (!SljitRegionPlanBooleanType(predicate.return_type)) {
		return false;
	}
	switch (predicate.kind) {
	case SljitNativePredicateKind::REFERENCE:
		return predicate.source_index == marker_index;
	case SljitNativePredicateKind::NOT:
		return predicate.child && SljitRegionPlanPredicateReferencesMarker(*predicate.child, marker_index);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child && SljitRegionPlanPredicateReferencesMarker(*child, marker_index)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

static bool SljitRegionPlanExpressionTreeReferencesMarker(const ExecutionExpressionIR &node,
                                                          const vector<idx_t> &input_source_indices,
                                                          idx_t marker_index) {
	if (!SljitRegionPlanBooleanType(node.return_type)) {
		return false;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return SljitRegionPlanLocalSourceReferencesColumn(input_source_indices, node.ref_index, marker_index);
	}
	if (node.left && SljitRegionPlanExpressionTreeReferencesMarker(*node.left, input_source_indices, marker_index)) {
		return true;
	}
	return node.right && SljitRegionPlanExpressionTreeReferencesMarker(*node.right, input_source_indices, marker_index);
}

static bool SljitRegionPlanFilterReferencesMarkProbeMarker(const SljitNativeRegionOpPlan &hash_join_op,
                                                           const SljitNativeRegionOpPlan &filter_op) {
	if (hash_join_op.output_types.empty() || filter_op.kind != SljitNativeRegionOpKind::FILTER) {
		return false;
	}
	const auto marker_index = hash_join_op.output_types.size() - 1;
	if (!SljitRegionPlanBooleanType(hash_join_op.output_types[marker_index]) ||
	    !SljitRegionPlanBooleanType(filter_op.filter.return_type)) {
		return false;
	}
	auto &filter = filter_op.filter;
	switch (filter.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return filter.source_index == marker_index;
	case SljitNativeRegionExpressionKind::PREDICATE:
		return filter.predicate && SljitRegionPlanPredicateReferencesMarker(*filter.predicate, marker_index);
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return filter.expression_tree &&
		       SljitRegionPlanExpressionTreeReferencesMarker(*filter.expression_tree,
		                                                     filter.expression_tree_source_indices, marker_index);
	default:
		return false;
	}
}

static bool SljitRegionPlanHasWeakMarkFilterProbe(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx + 1 >= ops.size() || ops[op_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return false;
	}
	auto &hash_join_op = ops[op_idx];
	auto &plan = hash_join_op.hash_join_probe;
	if (plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE || plan.perfect_hash_probe ||
	    plan.residual_predicate || plan.equality_key_count <= 1) {
		return false;
	}
	return SljitRegionPlanFilterReferencesMarkProbeMarker(hash_join_op, ops[op_idx + 1]);
}

static bool SljitRegionPlanHasDirectSourceHashBuild(const vector<SljitNativeRegionOpPlan> &ops) {
	if (ops.empty() || ops.back().kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		if (ops[op_idx].kind != SljitNativeRegionOpKind::FILTER &&
		    ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
	}
	return true;
}

static bool SljitPredicateContainsOnlyStringSets(const SljitNativePredicate &predicate) {
	if (predicate.kind == SljitNativePredicateKind::STRING_EQUAL_CONSTANT ||
	    (predicate.kind == SljitNativePredicateKind::STRING_IN_LIST_CONSTANT && !predicate.not_in)) {
		return true;
	}
	if (predicate.kind != SljitNativePredicateKind::CONJUNCTION ||
	    predicate.conjunction_op != ExecutionExpressionConjunctionOp::AND || predicate.children.empty()) {
		return false;
	}
	for (auto &child : predicate.children) {
		if (!child || !SljitPredicateContainsOnlyStringSets(*child)) {
			return false;
		}
	}
	return true;
}

static bool SljitRegionPlanHasWeakStringSetSourceHashBuild(const vector<SljitNativeRegionOpPlan> &ops) {
	if (!SljitRegionPlanHasDirectSourceHashBuild(ops)) {
		return false;
	}
	bool has_filter = false;
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		if (op.kind == SljitNativeRegionOpKind::FILTER) {
			has_filter = true;
			if (op.filter.kind != SljitNativeRegionExpressionKind::PREDICATE || !op.filter.predicate ||
			    !SljitPredicateContainsOnlyStringSets(*op.filter.predicate)) {
				return false;
			}
		} else if (op.kind == SljitNativeRegionOpKind::PROJECTION &&
		           SljitNativeRegionExpressionsGenerateCode(op.projections)) {
			return false;
		}
	}
	return has_filter;
}

static bool SljitRegionPlanExpressionIsPerfectHashReferenceGlue(const SljitNativeRegionExpressionPlan &expression) {
	switch (expression.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		return true;
	default:
		return false;
	}
}

static bool
SljitRegionPlanExpressionsArePerfectHashReferenceGlue(const vector<SljitNativeRegionExpressionPlan> &expressions) {
	for (auto &expression : expressions) {
		if (!SljitRegionPlanExpressionIsPerfectHashReferenceGlue(expression)) {
			return false;
		}
	}
	return true;
}

static bool SljitRegionPlanAggregateHasOnlyReferencePayloads(const SljitNativeAggregateUpdatePlan &aggregate) {
	auto &aggregates = aggregate.sink_info.aggregates;
	if (aggregate.payloads.size() != aggregates.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregate.payloads.size(); payload_idx++) {
		auto &payload = aggregate.payloads[payload_idx];
		auto &aggregate_input = aggregates[payload_idx];
		if (aggregate_input.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (payload.kind != SljitNativeRegionExpressionKind::CONSTANT) {
				return false;
			}
			continue;
		}
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE) {
			return false;
		}
	}
	return true;
}

static bool SljitRegionPlanHasReferenceOnlyStringPerfectHashAggregate(const vector<SljitNativeRegionOpPlan> &ops) {
	if (ops.empty() || ops.back().kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		if (op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    !SljitRegionPlanExpressionsArePerfectHashReferenceGlue(op.projections)) {
			return false;
		}
	}
	auto &aggregate = ops.back().aggregate_update;
	bool has_string_group_compression = false;
	for (auto &group_expression : aggregate.group_expressions) {
		has_string_group_compression =
		    has_string_group_compression || group_expression.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS;
	}
	return aggregate.sink_info.aggregate_contract.kind == ExecutionRegionAggregateOperatorKind::PERFECT_HASH &&
	       aggregate.use_perfect_hash_group_lookup && has_string_group_compression &&
	       SljitRegionPlanExpressionsArePerfectHashReferenceGlue(aggregate.group_expressions) &&
	       SljitRegionPlanAggregateHasOnlyReferencePayloads(aggregate);
}

void DisableSljitRegionFlatNullableFastPath(SljitNativeRegionPlan &region) {
	auto set_expression = [&](SljitNativeRegionExpressionPlan &expr) {
		expr.emit_flat_nullable_fast_path = false;
	};
	auto set_expressions = [&](vector<SljitNativeRegionExpressionPlan> &expressions) {
		for (auto &expr : expressions) {
			set_expression(expr);
		}
	};
	for (auto &op : region.ops) {
		set_expression(op.filter);
		set_expression(op.hash_join_probe.residual_filter);
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			set_expression(condition.lhs_condition);
		}
		set_expressions(op.nested_loop_join_build.rhs_conditions);
		set_expressions(op.order_sink.order_keys);
		set_expressions(op.aggregate_update.payloads);
		set_expressions(op.aggregate_update.group_expressions);
		set_expressions(op.projections);
	}
}

bool SljitNativeRegionHasExecutableBodyGap(const SljitNativeRegionPlan &region, string &blocker) {
	bool generates_machine_code = false;
	for (auto &op : region.ops) {
		auto op_blocker = SljitNativeRegionOpBoundaryBlocker(op);
		if (!op_blocker.empty()) {
			blocker = std::move(op_blocker);
			return true;
		}
		generates_machine_code = generates_machine_code || SljitNativeRegionOpGeneratesMachineCode(op);
	}
	if (!generates_machine_code) {
		if (region.uses_scan_filters && SljitRegionPlanHasDirectSourceHashBuild(region.ops)) {
			return false;
		}
		blocker = "SLJIT native region emits no generated machine code";
		return true;
	}
	return false;
}

void AddSljitNativeRegionCapabilityFacts(ExecutionRegionLoweringPlan &lowering_plan,
                                         const SljitNativeRegionPlan &native_region) {
	const bool direct_source_hash_build = SljitRegionPlanHasDirectSourceHashBuild(native_region.ops);
	if (SljitRegionPlanHasWeakStringSetSourceHashBuild(native_region.ops)) {
		lowering_plan.AddBackendWeakAcceleratedWorkCapability();
	}
	if (SljitRegionPlanHasReferenceOnlyStringPerfectHashAggregate(native_region.ops)) {
		lowering_plan.AddBackendReferenceOnlyStringPerfectHashAggregateCapability();
	}
	for (auto not_null : native_region.source_not_null) {
		lowering_plan.AddBackendSourceValidityCapability(not_null);
	}
	for (idx_t op_idx = 0; op_idx < native_region.ops.size(); op_idx++) {
		auto &op = native_region.ops[op_idx];
		if (SljitRegionPlanHasWeakMarkFilterProbe(native_region.ops, op_idx)) {
			lowering_plan.AddBackendWeakAcceleratedWorkCapability();
		}
		switch (op.kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			lowering_plan.AddBackendHashJoinProbeCapability(
			    op.hash_join_probe.perfect_hash_probe, op.hash_join_probe.residual_predicate,
			    op.hash_join_probe.equality_key_count, op.hash_join_probe.keys.size());
			for (auto &key : op.hash_join_probe.keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.key_type);
			}
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			for (auto &key : op.hash_join_build.sink_info.hash_join_keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.type);
			}
			lowering_plan.AddBackendHashJoinBuildCapability();
			if (direct_source_hash_build) {
				lowering_plan.AddBackendDirectHashJoinBuildCapability();
			}
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
			for (auto &condition : op.nested_loop_join_probe.conditions) {
				lowering_plan.AddBackendJoinKeyTypeCapability(condition.type);
			}
			lowering_plan.AddBackendNestedLoopJoinProbeCapability();
			break;
		case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			for (auto &condition_type : op.nested_loop_join_build.condition_types) {
				lowering_plan.AddBackendJoinKeyTypeCapability(condition_type);
			}
			lowering_plan.AddBackendNestedLoopJoinBuildCapability();
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
			auto &aggregate_update = op.aggregate_update;
			for (auto &group : aggregate_update.sink_info.groups) {
				lowering_plan.AddBackendGroupKeyTypeCapability(group.type);
			}
			for (auto &payload : aggregate_update.payloads) {
				lowering_plan.AddBackendPayloadTypeCapability(payload.return_type);
			}
			lowering_plan.AddBackendAggregateUpdateCapability(
			    aggregate_update.sink_info.aggregate_contract.kind, aggregate_update.UsesPrimitivePayloads(),
			    aggregate_update.use_grouped_state_addresses, aggregate_update.use_perfect_hash_group_lookup);
			auto &aggregate_contract = aggregate_update.sink_info.aggregate_contract;
			if (SljitAggregateSinkCanUseDistinctKeySink(aggregate_update.sink_info) &&
			    aggregate_contract.grouping_set_count == 1 && aggregate_contract.distinct_aggregate_count == 1 &&
			    aggregate_contract.distinct_table_count == 1 && aggregate_update.sink_info.aggregates.size() == 1) {
				lowering_plan.AddBackendDistinctKeyFastInsertCapability();
			}
			break;
		}
		default:
			break;
		}
	}
}

} // namespace duckdb

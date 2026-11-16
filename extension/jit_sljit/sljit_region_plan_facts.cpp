//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_facts.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

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
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.Filter());
	case SljitNativeRegionOpKind::PROJECTION:
		return SljitNativeRegionExpressionsGenerateCode(op.Projections());
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return SljitNativeHashJoinProbeGeneratesDeferredCode(op.HashJoinProbe()) ||
		       (op.HashJoinProbe().residual_predicate &&
		        SljitNativeRegionExpressionGeneratesCode(op.HashJoinProbe().residual_filter));
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return false;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return op.AggregateUpdate().UsesPrimitivePayloads();
	default:
		return false;
	}
}

static bool SljitNativeRegionOpIsNativeSink(const SljitNativeRegionOpPlan &op) {
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return op.HashJoinBuild().sink_info.contract.native_build_contract.status ==
		       ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::APPEND_SINK:
		return (op.AppendSink().sink_info.Kind() == ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK ||
		        op.AppendSink().sink_info.Kind() == ExecutionRegionSinkKind::MATERIALIZATION) &&
		       op.AppendSink().sink_info.Native().status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::ORDER_SINK:
		return op.OrderSink().sink_info.native_contract.status == ExecutionRegionStateContractStatus::READY;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
		auto &sink = op.AggregateUpdate().sink_info;
		return (sink.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		        sink.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE ||
		        sink.Kind() == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) &&
		       sink.contract.native_state_update_contract.status == ExecutionRegionStateContractStatus::READY;
	}
	default:
		return false;
	}
}

static string SljitNativeRegionOpBoundaryBlocker(const SljitNativeRegionOpPlan &op) {
	if (op.Kind() == SljitNativeRegionOpKind::FILTER || op.Kind() == SljitNativeRegionOpKind::PROJECTION) {
		return string();
	}
	if (op.Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return string();
	}
	if (op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		if (op.AggregateUpdate().UsesPrimitivePayloads()) {
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
	       string(SljitNativeRegionOpKindName(op.Kind()));
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
	if (hash_join_op.output_types.empty() || filter_op.Kind() != SljitNativeRegionOpKind::FILTER) {
		return false;
	}
	const auto marker_index = hash_join_op.output_types.size() - 1;
	if (!SljitRegionPlanBooleanType(hash_join_op.output_types[marker_index]) ||
	    !SljitRegionPlanBooleanType(filter_op.Filter().return_type)) {
		return false;
	}
	auto &filter = filter_op.Filter();
	switch (filter.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return filter.source_index == marker_index;
	case SljitNativeRegionExpressionKind::PREDICATE:
		return filter.predicate && SljitRegionPlanPredicateReferencesMarker(*filter.predicate, marker_index);
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return filter.expression_tree &&
		       SljitRegionPlanExpressionTreeReferencesMarker(*filter.expression_tree,
		                                                     filter.expression_tree_source_indices, marker_index);
	default:
		return false;
	}
}

static bool SljitRegionPlanHasWeakMarkFilterProbe(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx + 1 >= ops.size() || ops[op_idx].Kind() != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return false;
	}
	auto &hash_join_op = ops[op_idx];
	auto &plan = hash_join_op.HashJoinProbe();
	if (plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE || plan.perfect_hash_probe ||
	    plan.residual_predicate || plan.equality_key_count <= 1) {
		return false;
	}
	return SljitRegionPlanFilterReferencesMarkProbeMarker(hash_join_op, ops[op_idx + 1]);
}

static bool SljitRegionPlanHasDirectSourceHashBuild(const vector<SljitNativeRegionOpPlan> &ops) {
	if (ops.empty() || ops.back().Kind() != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		if (ops[op_idx].Kind() != SljitNativeRegionOpKind::FILTER &&
		    ops[op_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
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
		if (op.Kind() == SljitNativeRegionOpKind::FILTER) {
			has_filter = true;
			if (op.Filter().kind != SljitNativeRegionExpressionKind::PREDICATE || !op.Filter().predicate ||
			    !SljitPredicateContainsOnlyStringSets(*op.Filter().predicate)) {
				return false;
			}
		} else if (op.Kind() == SljitNativeRegionOpKind::PROJECTION &&
		           SljitNativeRegionExpressionsGenerateCode(op.Projections())) {
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
	if (ops.empty() || ops.back().Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
		auto &op = ops[op_idx];
		if (op.Kind() != SljitNativeRegionOpKind::PROJECTION ||
		    !SljitRegionPlanExpressionsArePerfectHashReferenceGlue(op.Projections())) {
			return false;
		}
	}
	auto &aggregate = ops.back().AggregateUpdate();
	bool has_string_group_compression = false;
	for (auto &group_expression : aggregate.group_expressions) {
		has_string_group_compression =
		    has_string_group_compression || group_expression.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS;
	}
	return aggregate.sink_info.contract.kind == ExecutionRegionAggregateOperatorKind::PERFECT_HASH &&
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
		switch (op.Kind()) {
		case SljitNativeRegionOpKind::FILTER:
			set_expression(op.Filter());
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			set_expression(op.HashJoinProbe().residual_filter);
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			set_expressions(op.AggregateUpdate().payloads);
			set_expressions(op.AggregateUpdate().group_expressions);
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			set_expressions(op.Projections());
			break;
		default:
			break;
		}
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
	// DuckDB's scan runs the region's compiled filters only when the region owns all of its scan filters
	const bool compiles_scan_filters =
	    region.scan_filter_mode == ExecutionRegionScanFilterMode::ALL && !region.scan_filters.empty();
	if (!generates_machine_code && !compiles_scan_filters) {
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
		switch (op.Kind()) {
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			lowering_plan.AddBackendHashJoinProbeCapability(
			    op.HashJoinProbe().perfect_hash_probe, op.HashJoinProbe().residual_predicate,
			    op.HashJoinProbe().equality_key_count, op.HashJoinProbe().keys.size());
			for (auto &key : op.HashJoinProbe().keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.key_type);
			}
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			for (auto &key : op.HashJoinBuild().sink_info.keys) {
				lowering_plan.AddBackendJoinKeyTypeCapability(key.type);
			}
			lowering_plan.AddBackendHashJoinBuildCapability();
			if (direct_source_hash_build) {
				lowering_plan.AddBackendDirectHashJoinBuildCapability();
			}
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
			auto &aggregate_update = op.AggregateUpdate();
			for (auto &group : aggregate_update.sink_info.groups) {
				lowering_plan.AddBackendGroupKeyTypeCapability(group.type);
			}
			for (auto &payload : aggregate_update.payloads) {
				lowering_plan.AddBackendPayloadTypeCapability(payload.return_type);
			}
			lowering_plan.AddBackendAggregateUpdateCapability(
			    aggregate_update.sink_info.contract.kind, aggregate_update.UsesPrimitivePayloads(),
			    aggregate_update.use_grouped_state_addresses, aggregate_update.use_perfect_hash_group_lookup);
			break;
		}
		default:
			break;
		}
	}
}

} // namespace duckdb

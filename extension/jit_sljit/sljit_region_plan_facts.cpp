//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan_facts.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static bool SljitNativeRegionExpressionGeneratesCode(const SljitNativeRegionExpressionPlan &expr) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::CONSTANT:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
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

static bool SljitNativeRegionOpGeneratesMachineCode(const SljitNativeRegionOpPlan &op) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return SljitNativeRegionExpressionGeneratesCode(op.filter);
	case SljitNativeRegionOpKind::PROJECTION:
		return SljitNativeRegionExpressionsGenerateCode(op.projections);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return !op.hash_join_probe.keys.empty() && op.hash_join_probe.equality_key_count > 0 &&
		       op.hash_join_probe.equality_key_count <= op.hash_join_probe.keys.size() &&
		       op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::NONE;
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return false;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return op.nested_loop_join_probe.join_type == ExecutionRegionJoinType::INNER &&
		       op.nested_loop_join_probe.conditions.size() == 1;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return SljitNativeRegionExpressionsGenerateCode(op.nested_loop_join_build.rhs_conditions);
	case SljitNativeRegionOpKind::ORDER_SINK:
		return SljitNativeRegionExpressionsGenerateCode(op.order_sink.order_keys);
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return false;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			return op.aggregate_update.use_primitive_payloads || op.aggregate_update.use_distinct_count_pointer_update;
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
		if (op.aggregate_update.use_primitive_payloads || op.aggregate_update.use_distinct_count_pointer_update) {
			return string();
		}
		return "operator-contract-blocker:aggregate-update-generated-payload-missing";
	}
	if (SljitNativeRegionOpIsNativeSink(op)) {
		return string();
	}
	return "operator-contract-blocker:whole-vectorized-operator-boundary;stage=" +
	       string(SljitNativeRegionOpKindName(op.kind));
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
		blocker = "SLJIT native region emits no generated machine code";
		return true;
	}
	return false;
}

void AddSljitNativeRegionCapabilityFacts(ExecutionRegionLoweringPlan &lowering_plan,
                                         const SljitNativeRegionPlan &native_region) {
	for (auto not_null : native_region.source_not_null) {
		lowering_plan.AddBackendSourceValidityCapability(not_null);
	}
	for (auto &op : native_region.ops) {
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
			    aggregate_update.sink_info.aggregate_contract.kind, aggregate_update.use_primitive_payloads,
			    aggregate_update.use_grouped_state_addresses, aggregate_update.use_perfect_hash_group_lookup);
			break;
		}
		default:
			break;
		}
	}
}

} // namespace duckdb

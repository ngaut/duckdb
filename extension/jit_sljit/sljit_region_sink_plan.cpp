//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_sink_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static string DescribeSljitAggregateSinkInput(const ExecutionRegionAggregateInput &aggregate) {
	string result = "aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_children=";
	result += std::to_string(aggregate.child_count);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_index=";
	result += std::to_string(aggregate.payload_index);
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_payload_references=";
	result += aggregate.supported_payload_references ? "ready" : "missing";
	result += ";aggregate";
	result += std::to_string(aggregate.aggregate_index);
	result += "_primitive_update=";
	result += aggregate.primitive_update_ready ? "ready" : "missing";
	if (!aggregate.primitive_update_blocker.empty()) {
		result += ";aggregate";
		result += std::to_string(aggregate.aggregate_index);
		result += "_primitive_update_blocker=";
		result += aggregate.primitive_update_blocker;
	}
	return result;
}

static string ValidateSljitAggregateUpdateSink(const ExecutionRegionSinkInfo &sink) {
	auto &contract = sink.aggregate_contract;
	if (contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockerOrReason(contract.native_state_update_contract.blocker,
		                            "aggregate native state-update contract is not ready");
	}
	if (sink.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    sink.kind != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	    sink.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return "aggregate update sink kind mismatch";
	}
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, sink.aggregates, sink.groups);
	if (!blocker.empty()) {
		return blocker;
	}
	return string();
}

static SljitRegionNodePlan PlanSljitAggregateUpdateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("aggregate sink is missing native sink IR");
	}
	auto blocker = ValidateSljitAggregateUpdateSink(*node.sink);
	if (!blocker.empty()) {
		return SljitRegionBoundaryNode(std::move(blocker));
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::AGGREGATE_UPDATE;
	native_op.aggregate_update.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.aggregate_update.ir = node.sink->ir;
	}

	auto &contract = node.sink->aggregate_contract;
	string reason = "native aggregate update sink contract";
	if (render_diagnostics) {
		reason += ";requires=aggregate_update_runtime_binding";
		if (!contract.native_state_update_contract.required_capability.empty()) {
			reason += ";requires=" + contract.native_state_update_contract.required_capability;
		}
		reason += ";sink_kind=" + string(ExecutionRegionSinkKindToString(node.sink->kind));
		reason += ";aggregate_operator_kind=" + string(ExecutionRegionAggregateOperatorKindToString(contract.kind));
		reason += ";aggregate_count=" + std::to_string(contract.aggregate_count);
		reason += ";group_count=" + std::to_string(contract.group_count);
		reason += ";payload_type_count=" + std::to_string(contract.payload_type_count);
		for (auto &aggregate : node.sink->aggregates) {
			reason += ";aggregate" + std::to_string(aggregate.aggregate_index) + "_function=" + aggregate.function_name;
			reason += ";";
			reason += DescribeSljitAggregateSinkInput(aggregate);
		}
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitHashAggregateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash aggregate sink is missing native sink IR");
	}
	auto &lookup_contract = node.sink->aggregate_contract.native_hash_lookup_contract;
	if (lookup_contract.status != ExecutionRegionStateContractStatus::READY) {
		auto blocker = SljitBlockerOrReason(lookup_contract.blocker, "hash-aggregate-native-lookup-contract-missing");
		string reason = "hash aggregate update requires generated hash lookup ownership";
		if (render_diagnostics) {
			reason += ";native_hash_aggregate_lookup_blocker=" + blocker;
		}
		AppendSljitReasonPart(reason, node.sink->aggregate_contract.hash_lookup_layout_ir, render_diagnostics);
		AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
		return SljitRegionBoundaryNode(std::move(reason));
	}
	return PlanSljitAggregateUpdateSinkNode(node, render_diagnostics);
}

static SljitRegionNodePlan PlanSljitHashAggregateDistinctSinkNode(const ExecutionRegionNode &node,
                                                                  bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink is missing native sink IR");
	}
	auto &distinct_contract = node.sink->aggregate_contract.native_distinct_state_update_contract;
	if (distinct_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitRegionBoundaryNode("hash aggregate distinct state-update contract missing;blocker=" +
		                               distinct_contract.blocker);
	}
	if (node.sink->groups.empty()) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink has no group bindings");
	}
	for (auto &group : node.sink->groups) {
		if (!group.supported_reference) {
			auto reason = group.reason.empty() ? "hash aggregate distinct group binding unsupported" : group.reason;
			return SljitRegionBoundaryNode("hash aggregate distinct sink group unsupported;group_index=" +
			                               std::to_string(group.group_index) + ";" + reason);
		}
	}
	if (node.sink->aggregates.empty()) {
		return SljitRegionBoundaryNode("hash aggregate distinct sink has no aggregate payload bindings");
	}
	for (auto &aggregate : node.sink->aggregates) {
		if (!aggregate.distinct) {
			return SljitRegionBoundaryNode("hash aggregate distinct sink received non-distinct aggregate");
		}
	}

	string reason = "hash aggregate distinct state-update native lowering requires distinct aggregate contract";
	if (render_diagnostics) {
		reason += ";aggregate-state-update=distinct-contract-boundary";
		reason += ";aggregate_count=" + std::to_string(node.sink->aggregate_contract.aggregate_count);
		reason += ";group_count=" + std::to_string(node.sink->aggregate_contract.group_count);
	}
	AppendSljitReasonPart(reason, node.sink->reason, render_diagnostics);
	return SljitRegionBoundaryNode(std::move(reason));
}

static SljitRegionNodePlan PlanSljitHashJoinSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash join sink is missing native sink IR");
	}
	auto &contract = node.sink->hash_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("hash join build native lowering requires hash join contract IR");
	}
	if (contract.native_build_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_build_contract.blocker,
		                                    "hash join build native contract is not ready");
	}
	if (!contract.build_sink_shape_ready) {
		return SljitBlockedContractBoundary(contract.build_sink_shape_blocker,
		                                    "hash join build native sink shape is not ready");
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::HASH_JOIN_BUILD;
	native_op.hash_join_build.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.hash_join_build.ir = "hash_join_build_native<execution=primitive-protocol-build,payload_columns=" +
		                               std::to_string(contract.payload_column_count) +
		                               ",keys=" + std::to_string(contract.condition_count) + ">";
	}

	string reason = "native hash join build contract";
	if (render_diagnostics) {
		reason += ";requires=hash_join_build_runtime_binding;requires=hash_join_build_prepare;"
		          "requires=hash_join_build_hash;requires=hash_join_build_append";
		reason += ";native_hash_join_build_contract_status=";
		reason += ExecutionRegionStateContractStatusToString(contract.native_build_contract.status);
		if (!contract.native_build_contract.blocker.empty()) {
			reason += ";native_hash_join_build_blocker=" + contract.native_build_contract.blocker;
		}
		reason += ";build_sink_shape_ready=true";
	}
	AppendSljitReasonPart(reason, native_op.hash_join_build.ir, render_diagnostics);
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitNestedLoopJoinSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("nested loop join sink is missing native sink IR");
	}
	auto &contract = node.sink->nested_loop_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("nested loop join build native lowering requires nested loop join contract IR");
	}
	if (contract.native_build_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_build_contract.blocker,
		                                    "nested loop join build native contract is not ready");
	}
	if (!contract.build_sink_shape_ready) {
		return SljitBlockedContractBoundary(contract.build_sink_shape_blocker,
		                                    "nested loop join build native sink shape is not ready");
	}
	if (contract.join_type != ExecutionRegionJoinType::INNER) {
		return SljitRegionBoundaryNode("nested loop join build native lowering supports inner joins only;join_type=" +
		                               string(ExecutionRegionJoinTypeToString(contract.join_type)));
	}
	if (!contract.complex_join || contract.simple_join) {
		return SljitRegionBoundaryNode("nested loop join build native lowering requires complex join contract");
	}
	if (contract.filter_pushdown) {
		return SljitRegionBoundaryNode("nested loop join build native lowering does not support filter pushdown state");
	}
	if (!contract.conditions_ready) {
		return SljitBlockedContractBoundary(
		    contract.condition_blocker, "nested loop join build native lowering requires ready condition expressions");
	}
	if (contract.conditions.size() != contract.condition_types.size()) {
		return SljitRegionBoundaryNode("nested loop join build native lowering condition count mismatch");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD;
	native_op.nested_loop_join_build.sink_info = *node.sink;
	native_op.nested_loop_join_build.condition_types = contract.condition_types;
	if (render_diagnostics) {
		native_op.nested_loop_join_build.ir = node.sink->ir;
	}
	native_op.nested_loop_join_build.rhs_conditions.reserve(contract.conditions.size());
	for (auto &condition : contract.conditions) {
		if (!condition.rhs_expression_ready || !condition.rhs_expression.root) {
			return SljitBlockedContractBoundary(
			    condition.rhs_expression_blocker,
			    "nested loop join build native lowering requires lowered RHS condition IR");
		}
		SljitNativeRegionExpressionPlan rhs_condition;
		string error;
		if (!TryLowerNativeRegionExpression(condition.rhs_expression, false, rhs_condition, error,
		                                    render_diagnostics)) {
			string reason = "nested loop join build native lowering has unsupported RHS condition expression";
			if (!error.empty()) {
				reason += ";" + error;
			}
			return SljitRegionBoundaryNode(std::move(reason));
		}
		native_op.nested_loop_join_build.rhs_conditions.push_back(std::move(rhs_condition));
	}

	string reason = "native nested loop join build sink contract";
	if (render_diagnostics) {
		reason += ";requires=native_sink_runtime_binding;requires=native_nested_loop_join_build_sink;"
		          "build_sink_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static void AppendSljitRequiredCapability(string &reason, const string &capability) {
	if (!capability.empty()) {
		reason += ";requires=" + capability;
	}
}

static string BuildSljitNativeSinkReason(const char *label, const char *runtime_binding,
                                         const ExecutionRegionSinkInfo &sink,
                                         const ExecutionRegionNativeOperatorContract &contract,
                                         bool render_diagnostics) {
	string reason = string(label);
	if (render_diagnostics) {
		reason += ";requires=";
		reason += runtime_binding;
		AppendSljitRequiredCapability(reason, contract.required_capability);
		reason += ";sink_kind=" + string(ExecutionRegionSinkKindToString(sink.kind));
	}
	return reason;
}

static void AppendSljitSinkIR(string &reason, const ExecutionRegionSinkInfo &sink, bool render_diagnostics) {
	AppendSljitReasonPart(reason, sink.ir, render_diagnostics);
}

static SljitRegionNodePlan PlanSljitSimpleNativeSinkNode(const ExecutionRegionNode &node,
                                                         SljitNativeRegionOpKind native_kind,
                                                         const char *contract_label, const char *runtime_binding,
                                                         const char *blocked_reason, bool render_diagnostics) {
	auto &contract = node.sink->native_sink_contract;
	if (contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.blocker, blocked_reason);
	}
	SljitNativeRegionOpPlan native_op;
	native_op.kind = native_kind;
	switch (native_kind) {
	case SljitNativeRegionOpKind::APPEND_SINK:
		native_op.append_sink.sink_info = *node.sink;
		if (render_diagnostics) {
			native_op.append_sink.ir = node.sink->ir;
		}
		break;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		native_op.delim_join_sink.sink_info = *node.sink;
		if (render_diagnostics) {
			native_op.delim_join_sink.ir = node.sink->ir;
		}
		break;
	default:
		throw InternalException("Unsupported simple SLJIT native sink kind");
	}

	auto reason = BuildSljitNativeSinkReason(contract_label, runtime_binding, *node.sink, contract, render_diagnostics);
	AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static SljitRegionNodePlan PlanSljitOrderSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	auto &contract = node.sink->native_sink_contract;
	auto &order_contract = node.sink->order_contract;
	if (contract.status != ExecutionRegionStateContractStatus::READY) {
		auto reason = SljitBlockerOrReason(contract.blocker, "ordered-sink-contract-not-ready");
		if (order_contract.present) {
			reason += ";ordered-sink-contract-present";
			reason += order_contract.all_order_keys_ready ? ";order_keys_ready=true" : ";order_keys_ready=false";
			if (!order_contract.order_key_blocker.empty()) {
				reason += ";order_key_blocker=" + order_contract.order_key_blocker;
			}
		}
		return SljitRegionBoundaryNode(reason);
	}
	if (!order_contract.present) {
		return SljitRegionBoundaryNode("ordered-sink-contract-missing");
	}
	if (!order_contract.all_order_keys_ready) {
		return SljitBlockedContractBoundary(order_contract.order_key_blocker, "ordered-sink-order-keys-not-ready");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::ORDER_SINK;
	native_op.order_sink.sink_info = *node.sink;
	if (render_diagnostics) {
		native_op.order_sink.ir = node.sink->ir;
	}
	native_op.order_sink.order_keys.reserve(order_contract.order_keys.size());
	native_op.order_sink.key_types.reserve(order_contract.order_keys.size());
	for (auto &key : order_contract.order_keys) {
		SljitNativeRegionExpressionPlan key_plan;
		string error;
		if (!TryLowerNativeRegionExpression(key.expression, false, key_plan, error, render_diagnostics)) {
			string reason = "ordered-sink-order-key-native-lowering-unsupported";
			if (!error.empty()) {
				reason += ";" + error;
			}
			if (!key.reason.empty()) {
				reason += ";order_key_blocker=" + key.reason;
			}
			return SljitRegionBoundaryNode(std::move(reason));
		}
		native_op.order_sink.key_types.push_back(key.type);
		native_op.order_sink.order_keys.push_back(std::move(key_plan));
	}

	auto reason = BuildSljitNativeSinkReason("ordered sink contract", "ordered_sink_runtime_binding", *node.sink,
	                                         contract, render_diagnostics);
	if (render_diagnostics) {
		reason += ";operator_kind=" + string(ExecutionRegionOperatorKindToString(order_contract.kind));
		reason += ";order_keys=" + std::to_string(native_op.order_sink.order_keys.size());
	}
	AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

static void SetSljitNativeSinkInputTypes(SljitNativeRegionOpPlan &op, const vector<LogicalType> &input_types) {
	op.output_types = input_types;
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		op.hash_join_build.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		op.nested_loop_join_build.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::ORDER_SINK:
		op.order_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::APPEND_SINK:
		op.append_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		op.delim_join_sink.input_types = input_types;
		break;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		op.aggregate_update.input_types = input_types;
		break;
	default:
		break;
	}
}

static void SetSljitNativeSinkInputTypes(SljitRegionNodePlan &sink, const vector<LogicalType> &input_types) {
	if (sink.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(sink)) {
		auto &native_op = SljitRegionNodeLastNativeOp(sink);
		SetSljitNativeSinkInputTypes(native_op, input_types);
	}
}

static SljitRegionNodePlan SljitFullPipelineSinkBoundary(const ExecutionRegionNode &node,
                                                         const SljitRegionNodePlan &native_sink, string reason,
                                                         const char *lowering_reason_key, bool render_diagnostics) {
	if (render_diagnostics && !native_sink.reason.empty()) {
		reason += ";";
		reason += lowering_reason_key;
		reason += "=";
		reason += native_sink.reason;
	}
	if (node.sink) {
		AppendSljitSinkIR(reason, *node.sink, render_diagnostics);
	}
	if (!node.blocker_reason.empty()) {
		reason += ";boundary=" + node.blocker_reason;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

SljitRegionNodePlan PlanSljitSinkNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                      bool render_diagnostics) {
	if (!node.sink) {
		return SljitNodeBlockerBoundary(node, "sink node is missing native sink IR");
	}

	SljitRegionNodePlan sink;
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		sink = PlanSljitHashJoinSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		sink = PlanSljitNestedLoopJoinSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		sink = PlanSljitHashAggregateSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_DISTINCT_SINK:
		sink = PlanSljitHashAggregateDistinctSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		sink = PlanSljitAggregateUpdateSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		sink = PlanSljitSimpleNativeSinkNode(node, SljitNativeRegionOpKind::APPEND_SINK, "append sink contract",
		                                     "append_sink_runtime_binding", "append sink contract is not ready",
		                                     render_diagnostics);
		break;
	case ExecutionRegionSinkKind::SORT:
		sink = PlanSljitOrderSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		sink = PlanSljitSimpleNativeSinkNode(node, SljitNativeRegionOpKind::DELIM_JOIN_SINK,
		                                     "delimiter join sink contract", "delim_join_sink_runtime_binding",
		                                     "delim join sink contract is not ready", render_diagnostics);
		break;
	default:
		return SljitNodeBlockerBoundary(node, "sink kind is outside SLJIT native sink lowering");
	}

	SetSljitNativeSinkInputTypes(sink, input_types);
	return sink;
}

SljitRegionNodePlan PlanSljitFullPipelineSinkNode(const ExecutionRegionNode &node,
                                                  const vector<LogicalType> &input_types, bool render_diagnostics) {
	if (!node.sink) {
		return SljitNodeBlockerBoundary(node, "full pipeline sink node is missing native sink IR");
	}

	auto native_sink = PlanSljitSinkNode(node, input_types, render_diagnostics);
	if (native_sink.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(native_sink)) {
		if (render_diagnostics) {
			native_sink.reason += ";full-pipeline-native-sink";
		}
		return native_sink;
	}
	if (node.sink->kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		return SljitFullPipelineSinkBoundary(
		    node, native_sink, "full pipeline hash join build sink requires native hash build primitive lowering",
		    "hash-join-build-contract-boundary", render_diagnostics);
	}
	if (node.sink->kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD) {
		return SljitFullPipelineSinkBoundary(
		    node, native_sink,
		    "full pipeline nested loop join build sink requires native nested loop join build lowering",
		    "nested-loop-join-build-contract-boundary", render_diagnostics);
	}
	return SljitFullPipelineSinkBoundary(node, native_sink, "full pipeline sink requires native sink contract",
	                                     "native-sink-lowering", render_diagnostics);
}

} // namespace duckdb

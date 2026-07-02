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
		sink = PlanSljitHashJoinBuildSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		sink = PlanSljitNestedLoopJoinBuildSinkNode(node, render_diagnostics);
		break;
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		sink = PlanSljitHashAggregateSinkNode(node, render_diagnostics);
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

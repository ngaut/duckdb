//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_sink_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

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
	if (sink.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    sink.Kind() != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE &&
	    sink.Kind() != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE) {
		return "aggregate update sink kind mismatch";
	}
	if (sink.Kind() != sink.Aggregate().Kind()) {
		return "aggregate update sink operator kind mismatch";
	}
	auto &contract = sink.Aggregate().contract;
	if (contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockerOrReason(contract.native_state_update_contract.blocker,
		                            "aggregate native state-update contract is not ready");
	}
	auto blocker = ExecutionRegionAggregateNativeStateUpdateBlocker(contract, sink.Aggregate().aggregates,
	                                                                sink.Aggregate().groups);
	if (!blocker.empty()) {
		return blocker;
	}
	return string();
}

SljitRegionNodePlan PlanSljitAggregateUpdateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("aggregate sink is missing native sink IR");
	}
	auto blocker = ValidateSljitAggregateUpdateSink(*node.sink);
	if (!blocker.empty()) {
		return SljitRegionBoundaryNode(std::move(blocker));
	}
	SljitNativeRegionOpPlan native_op;
	native_op.SetKind(SljitNativeRegionOpKind::AGGREGATE_UPDATE);
	native_op.AggregateUpdate().sink_info = node.sink->Aggregate();
	if (render_diagnostics) {
		native_op.AggregateUpdate().ir = node.sink->ir;
	}

	auto &contract = node.sink->Aggregate().contract;
	string reason = "native aggregate update sink contract";
	if (render_diagnostics) {
		reason += ";requires=aggregate_update_runtime_binding";
		if (!contract.native_state_update_contract.required_capability.empty()) {
			reason += ";requires=" + contract.native_state_update_contract.required_capability;
		}
		reason += ";sink_kind=" + string(ExecutionRegionSinkKindToString(node.sink->Kind()));
		reason += ";aggregate_operator_kind=" + string(ExecutionRegionAggregateOperatorKindToString(contract.kind));
		reason += ";aggregate_count=" + std::to_string(contract.aggregate_count);
		reason += ";group_count=" + std::to_string(contract.group_count);
		reason += ";payload_type_count=" + std::to_string(contract.payload_type_count);
		for (auto &aggregate : node.sink->Aggregate().aggregates) {
			reason += ";aggregate" + std::to_string(aggregate.aggregate_index) + "_function=" + aggregate.function_name;
			reason += ";";
			reason += DescribeSljitAggregateSinkInput(aggregate);
		}
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

SljitRegionNodePlan PlanSljitHashAggregateSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	if (!node.sink) {
		return SljitRegionBoundaryNode("hash aggregate sink is missing native sink IR");
	}
	auto sink = PlanSljitAggregateUpdateSinkNode(node, render_diagnostics);
	if (render_diagnostics && sink.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(sink)) {
		auto &native_op = SljitRegionNodeLastNativeOp(sink);
		if (!native_op.AggregateUpdate().ir.empty()) {
			native_op.AggregateUpdate().ir += ";";
		}
		native_op.AggregateUpdate().ir += "grouped_state_lookup=native-state-address";
		sink.reason += ";grouped_state_lookup=native-state-address";
	}
	return sink;
}

} // namespace duckdb

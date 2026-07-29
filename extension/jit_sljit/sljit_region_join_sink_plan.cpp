#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static SljitRegionNodePlan PlanSljitHashJoinBuildSinkNodeInternal(const ExecutionRegionNode &node,
                                                                  bool render_diagnostics) {
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
		auto diagnostic = "hash_join_build_native<execution=primitive-protocol-build,payload_columns=" +
		                  std::to_string(contract.payload_column_count) +
		                  ",keys=" + std::to_string(contract.condition_count) + ">";
		AppendSljitReasonPart(reason, diagnostic, true);
	}
	AppendSljitReasonPart(reason, node.sink->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

SljitRegionNodePlan PlanSljitHashJoinBuildSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	auto result = PlanSljitHashJoinBuildSinkNodeInternal(node, render_diagnostics);
	if (result.kind != ExecutionRegionLoweringKind::BOUNDARY) {
		return result;
	}
	const bool contract_ready =
	    node.sink && node.sink->hash_join_contract.present &&
	    node.sink->hash_join_contract.native_build_contract.status == ExecutionRegionStateContractStatus::READY;
	result.fusion_blocker = contract_ready ? "sink-contract-blocker:hash-join-build-native-lowering;" + result.reason
	                                       : "sink-contract-blocker:hash-join-build-contract-missing";
	return result;
}

static SljitRegionNodePlan PlanSljitNestedLoopJoinBuildSinkNodeInternal(const ExecutionRegionNode &node,
                                                                        bool render_diagnostics) {
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

SljitRegionNodePlan PlanSljitNestedLoopJoinBuildSinkNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	auto result = PlanSljitNestedLoopJoinBuildSinkNodeInternal(node, render_diagnostics);
	if (result.kind != ExecutionRegionLoweringKind::BOUNDARY) {
		return result;
	}
	const bool contract_ready =
	    node.sink && node.sink->nested_loop_join_contract.present &&
	    node.sink->nested_loop_join_contract.native_build_contract.status == ExecutionRegionStateContractStatus::READY;
	result.fusion_blocker =
	    contract_ready ? "sink-contract-blocker:nested-loop-join-build-native-lowering-missing;" + result.reason
	                   : "sink-contract-blocker:nested-loop-join-build-contract-missing";
	return result;
}

} // namespace duckdb

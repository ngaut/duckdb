#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static SljitRegionNodePlan PlanSljitHashJoinBuildSinkNodeInternal(const ExecutionRegionNode &node,
                                                                  bool render_diagnostics) {
	if (!node.sink || node.sink->Kind() != ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		return SljitRegionBoundaryNode("hash join sink is missing native sink IR");
	}
	auto &contract = node.sink->HashJoin().contract;
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
	native_op.SetKind(SljitNativeRegionOpKind::HASH_JOIN_BUILD);
	native_op.HashJoinBuild().sink_info = node.sink->HashJoin();

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
	    node.sink && node.sink->Kind() == ExecutionRegionSinkKind::HASH_JOIN_BUILD &&
	    node.sink->HashJoin().contract.present &&
	    node.sink->HashJoin().contract.native_build_contract.status == ExecutionRegionStateContractStatus::READY;
	result.fusion_blocker = contract_ready ? "sink-contract-blocker:hash-join-build-native-lowering;" + result.reason
	                                       : "sink-contract-blocker:hash-join-build-contract-missing";
	return result;
}

} // namespace duckdb

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static bool SljitNativeContractReady(bool present, const ExecutionRegionNativeOperatorContract &native_contract) {
	return present && native_contract.status == ExecutionRegionStateContractStatus::READY;
}

static void AddSljitContractBlocker(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                    bool contract_ready, const char *ready_error, const char *missing_error,
                                    const char *ready_blocker, const char *missing_blocker,
                                    const string &ready_reason = string()) {
	backend_error = contract_ready ? ready_error : missing_error;
	if (!contract_ready) {
		lowering_plan.AddFusionBlocker(missing_blocker);
		return;
	}
	string blocker = ready_blocker;
	if (!ready_reason.empty()) {
		blocker += ";";
		blocker += ready_reason;
	}
	lowering_plan.AddFusionBlocker(std::move(blocker));
}

void AddSljitFullPipelineSinkBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                      const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan,
                                      const ExecutionRegionContract &contract) {
	if (node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY || !ExecutionRegionABIIsFullPipeline(contract.abi) ||
	    !node.sink) {
		return;
	}
	switch (node.sink->kind) {
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(node.sink->hash_join_contract.present,
		                                                     node.sink->hash_join_contract.native_build_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, build_contract_ready,
		                        "SLJIT full-pipeline hash join build sink rejected by native hash join build lowering",
		                        "SLJIT full-pipeline hash join build sink requires a native hash join build contract",
		                        "sink-contract-blocker:hash-join-build-native-lowering",
		                        "sink-contract-blocker:hash-join-build-contract-missing");
		break;
	}
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD: {
		auto build_contract_ready = SljitNativeContractReady(
		    node.sink->nested_loop_join_contract.present, node.sink->nested_loop_join_contract.native_build_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, build_contract_ready,
		                        "SLJIT full-pipeline nested loop join build sink rejected by native lowering",
		                        "SLJIT full-pipeline nested loop join build sink requires a native build contract",
		                        "sink-contract-blocker:nested-loop-join-build-native-lowering-missing",
		                        "sink-contract-blocker:nested-loop-join-build-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

void AddSljitOperatorContractBlockers(ExecutionRegionLoweringPlan &lowering_plan, string &backend_error,
                                      const ExecutionRegionNode &node, const SljitRegionNodePlan &node_plan) {
	if (node_plan.kind != ExecutionRegionLoweringKind::BOUNDARY || !node.operator_info) {
		return;
	}
	switch (node.operator_info->kind) {
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->hash_join_contract.present,
		                             node.operator_info->hash_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT hash join probe rejected by native hash join lowering",
		                        "SLJIT hash join probe requires a native hash join probe contract",
		                        "operator-contract-blocker:hash-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:hash-join-probe-contract-missing", node_plan.reason);
		break;
	}
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE: {
		auto probe_contract_ready =
		    SljitNativeContractReady(node.operator_info->nested_loop_join_contract.present,
		                             node.operator_info->nested_loop_join_contract.native_probe_contract);
		AddSljitContractBlocker(lowering_plan, backend_error, probe_contract_ready,
		                        "SLJIT nested loop join probe rejected by native lowering",
		                        "SLJIT nested loop join probe requires a native probe contract",
		                        "operator-contract-blocker:nested-loop-join-probe-native-lowering-missing",
		                        "operator-contract-blocker:nested-loop-join-probe-contract-missing", node_plan.reason);
		break;
	}
	default:
		break;
	}
}

} // namespace duckdb

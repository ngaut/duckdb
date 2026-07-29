#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static bool TryGetSljitNestedLoopJoinValueKind(const LogicalType &type, SljitNativeNestedLoopJoinValueKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT32:
		kind = SljitNativeNestedLoopJoinValueKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeNestedLoopJoinValueKind::INT64;
		return true;
	case PhysicalType::INT128:
		kind = SljitNativeNestedLoopJoinValueKind::INT128;
		return true;
	case PhysicalType::DOUBLE:
		kind = SljitNativeNestedLoopJoinValueKind::DOUBLE;
		return true;
	default:
		return false;
	}
}

static SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNodeInternal(const ExecutionRegionNode &node,
                                                                            const vector<LogicalType> &input_types,
                                                                            bool render_diagnostics) {
	if (!node.operator_info) {
		return SljitRegionBoundaryNode("nested loop join probe operator is missing typed operator IR");
	}
	auto &contract = node.operator_info->nested_loop_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering requires nested loop join contract IR");
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_probe_contract.blocker,
		                                    "nested loop join probe native contract is not ready");
	}
	if (!contract.native_probe_shape_ready) {
		return SljitBlockedContractBoundary(contract.native_probe_shape_blocker,
		                                    "nested loop join probe native shape is not ready");
	}
	if (contract.join_type != ExecutionRegionJoinType::INNER) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering supports inner joins only;join_type=" +
		                               string(ExecutionRegionJoinTypeToString(contract.join_type)));
	}
	if (!contract.complex_join || contract.simple_join) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering requires complex join contract");
	}
	if (!contract.conditions_ready) {
		return SljitBlockedContractBoundary(
		    contract.condition_blocker, "nested loop join probe native lowering requires ready condition expressions");
	}
	if (contract.conditions.size() != 1 || contract.condition_types.size() != 1 ||
	    contract.comparison_types.size() != 1) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering currently requires one condition");
	}
	if (input_types.size() != contract.lhs_input_types.size()) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering input width does not match contract");
	}

	auto &condition = contract.conditions[0];
	if (!condition.lhs_expression_ready || !condition.lhs_expression.root) {
		return SljitBlockedContractBoundary(condition.lhs_expression_blocker,
		                                    "nested loop join probe native lowering requires lowered LHS condition IR");
	}
	if (condition.comparison_type != contract.comparison_types[0]) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering condition comparison mismatch");
	}
	SljitNativeNestedLoopJoinValueKind value_kind;
	if (!TryGetSljitNestedLoopJoinValueKind(condition.type, value_kind)) {
		return SljitRegionBoundaryNode("nested loop join probe native lowering has unsupported condition type " +
		                               condition.type.ToString());
	}
	SljitNativeRegionExpressionPlan lhs_condition;
	string error;
	if (!TryLowerNativeRegionExpression(condition.lhs_expression, false, lhs_condition, error, render_diagnostics)) {
		string reason = "nested loop join probe native lowering has unsupported LHS condition expression";
		if (!error.empty()) {
			reason += ";" + error;
		}
		return SljitRegionBoundaryNode(std::move(reason));
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE;
	native_op.operator_index = node.operator_index;
	native_op.output_types = node.output_types;
	native_op.nested_loop_join_probe.operator_index = node.operator_index;
	native_op.nested_loop_join_probe.input_types = input_types;
	native_op.nested_loop_join_probe.condition_types = contract.condition_types;
	native_op.nested_loop_join_probe.join_type = contract.join_type;
	native_op.nested_loop_join_probe.operator_info = *node.operator_info;

	SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
	condition_plan.lhs_condition = std::move(lhs_condition);
	condition_plan.type = condition.type;
	condition_plan.comparison_type = condition.comparison_type;
	condition_plan.value_kind = value_kind;
	native_op.nested_loop_join_probe.conditions.push_back(std::move(condition_plan));

	string reason = "generated native nested loop join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_nested_loop_join_probe_cursor;"
		          "requires=primitive_compare_stub;native_probe_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                             const vector<LogicalType> &input_types,
                                                             bool render_diagnostics) {
	auto result = PlanSljitNestedLoopJoinProbeOperatorNodeInternal(node, input_types, render_diagnostics);
	if (result.kind != ExecutionRegionLoweringKind::BOUNDARY) {
		return result;
	}
	const bool contract_ready = node.operator_info && node.operator_info->nested_loop_join_contract.present &&
	                            node.operator_info->nested_loop_join_contract.native_probe_contract.status ==
	                                ExecutionRegionStateContractStatus::READY;
	result.fusion_blocker =
	    contract_ready ? "operator-contract-blocker:nested-loop-join-probe-native-lowering-missing;" + result.reason
	                   : "operator-contract-blocker:nested-loop-join-probe-contract-missing";
	return result;
}

} // namespace duckdb

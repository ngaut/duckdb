//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_join_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static bool TryGetSljitHashJoinKeyKind(const LogicalType &type, SljitNativeHashJoinKeyKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		kind = SljitNativeHashJoinKeyKind::UINT8;
		return true;
	case PhysicalType::INT8:
		kind = SljitNativeHashJoinKeyKind::INT8;
		return true;
	case PhysicalType::UINT16:
		kind = SljitNativeHashJoinKeyKind::UINT16;
		return true;
	case PhysicalType::INT16:
		kind = SljitNativeHashJoinKeyKind::INT16;
		return true;
	case PhysicalType::UINT32:
		kind = SljitNativeHashJoinKeyKind::UINT32;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeHashJoinKeyKind::INT32;
		return true;
	case PhysicalType::UINT64:
		kind = SljitNativeHashJoinKeyKind::UINT64;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeHashJoinKeyKind::INT64;
		return true;
	case PhysicalType::UINT128:
		kind = SljitNativeHashJoinKeyKind::UINT128;
		return true;
	case PhysicalType::INT128:
		kind = SljitNativeHashJoinKeyKind::INT128;
		return true;
	default:
		return false;
	}
}

static bool SljitHashJoinEqualityComparisonSupported(ExecutionRegionComparisonType comparison_type) {
	return comparison_type == ExecutionRegionComparisonType::EQUAL ||
	       comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
}

static bool SljitHashJoinMatchPredicateSupported(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::NOT_EQUAL:
	case ExecutionRegionComparisonType::LESS_THAN:
	case ExecutionRegionComparisonType::GREATER_THAN:
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return true;
	default:
		return false;
	}
}

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

SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                       const vector<LogicalType> &input_types,
                                                       bool render_diagnostics) {
	if (!node.operator_info) {
		return SljitRegionBoundaryNode("hash join probe operator is missing typed operator IR");
	}
	auto &contract = node.operator_info->hash_join_contract;
	if (!contract.present) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires hash join contract IR");
	}
	if (contract.native_probe_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitBlockedContractBoundary(contract.native_probe_contract.blocker,
		                                    "hash join probe native contract is not ready");
	}
	if (!contract.native_probe_shape_ready) {
		return SljitBlockedContractBoundary(contract.native_probe_shape_blocker,
		                                    "hash join probe native shape is not ready");
	}
	if (node.operator_info->hash_join_keys.size() != contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering key count does not match contract");
	}
	if (contract.layout_offsets.size() < contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires hash table key layout offsets");
	}
	if (contract.equality_condition_count == 0 || contract.equality_condition_count > contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires an equality-key prefix");
	}

	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	keys.reserve(node.operator_info->hash_join_keys.size());
	for (idx_t key_idx = 0; key_idx < node.operator_info->hash_join_keys.size(); key_idx++) {
		auto &key = node.operator_info->hash_join_keys[key_idx];
		auto comparison_type = contract.comparison_types[key_idx];
		auto equality_key = key_idx < contract.equality_condition_count;
		if (equality_key && !SljitHashJoinEqualityComparisonSupported(comparison_type)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported equality comparison " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!equality_key && !SljitHashJoinMatchPredicateSupported(comparison_type)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported match predicate " +
			                               string(SljitHashJoinComparisonToString(comparison_type)));
		}
		if (!key.supported_reference) {
			return SljitRegionBoundaryNode(
			    key.reason.empty() ? "hash join probe native lowering requires supported reference keys" : key.reason);
		}
		if (key.input_index >= input_types.size()) {
			return SljitRegionBoundaryNode("hash join probe native lowering key input index is outside operator input");
		}
		SljitNativeHashJoinKeyKind key_kind;
		if (!TryGetSljitHashJoinKeyKind(key.type, key_kind)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported key type " +
			                               key.type.ToString());
		}
		if (!equality_key &&
		    (key_kind == SljitNativeHashJoinKeyKind::INT128 || key_kind == SljitNativeHashJoinKeyKind::UINT128)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported 128-bit match predicate " +
			                               key.type.ToString());
		}
		SljitNativeHashJoinProbeKeyPlan key_plan;
		key_plan.key_input_index = key.input_index;
		key_plan.key_layout_offset = contract.layout_offsets[key_idx];
		key_plan.key_type = key.type;
		key_plan.key_kind = key_kind;
		key_plan.comparison_type = comparison_type;
		key_plan.equality_key = equality_key;
		key_plan.null_equal = equality_key && comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
		keys.push_back(std::move(key_plan));
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::HASH_JOIN_PROBE;
	native_op.operator_index = node.operator_index;
	native_op.output_types = node.output_types;
	native_op.hash_join_probe.operator_index = node.operator_index;
	native_op.hash_join_probe.input_types = input_types;
	native_op.hash_join_probe.keys = std::move(keys);
	native_op.hash_join_probe.equality_key_count = contract.equality_condition_count;
	native_op.hash_join_probe.found_match_offset = contract.tuple_size;
	native_op.hash_join_probe.pointer_offset = contract.pointer_offset;
	native_op.hash_join_probe.output_mode = contract.native_probe_output_mode;
	native_op.hash_join_probe.operator_info = *node.operator_info;
	if (contract.perfect_hash_probe_shape_ready) {
		if (native_op.hash_join_probe.keys.size() != 1 || native_op.hash_join_probe.equality_key_count != 1) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires one equality key");
		}
		auto perfect_key_kind = native_op.hash_join_probe.keys[0].key_kind;
		if (perfect_key_kind == SljitNativeHashJoinKeyKind::INT128 ||
		    perfect_key_kind == SljitNativeHashJoinKeyKind::UINT128) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering does not support 128-bit keys");
		}
		if (native_op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		    native_op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires inner output mode");
		}
		native_op.hash_join_probe.perfect_hash_probe = true;
	}
	if (contract.residual_predicate || contract.residual_info) {
		if (contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
			return SljitRegionBoundaryNode(
			    "hash join probe native lowering has unsupported residual mark-build-only contract");
		}
		if (!contract.residual_expression_ready || !contract.residual_expression.root) {
			return SljitBlockedContractBoundary(
			    contract.residual_expression_blocker,
			    "hash join probe native lowering requires lowered residual predicate IR");
		}
		SljitNativeRegionExpressionPlan residual_filter;
		string residual_error;
		if (!TryLowerNativeRegionExpression(contract.residual_expression, true, residual_filter, residual_error,
		                                    render_diagnostics)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported residual predicate;" +
			                               residual_error);
		}
		native_op.hash_join_probe.residual_predicate = true;
		native_op.hash_join_probe.residual_filter = std::move(residual_filter);
		native_op.hash_join_probe.residual_source_types.resize(contract.residual_sources.size());
		native_op.hash_join_probe.residual_source_not_null.resize(contract.residual_sources.size(), false);
		for (auto &source : contract.residual_sources) {
			if (source.source_index >= native_op.hash_join_probe.residual_source_types.size()) {
				return SljitRegionBoundaryNode("hash join probe native lowering residual source index is out of range");
			}
			if (source.kind == ExecutionHashJoinResidualSourceKind::PROBE && source.input_index >= input_types.size()) {
				return SljitRegionBoundaryNode(
				    "hash join probe native lowering residual probe source is outside operator input");
			}
			native_op.hash_join_probe.residual_source_types[source.source_index] = source.type;
			native_op.hash_join_probe.residual_source_not_null[source.source_index] = source.not_null;
		}
	}
	auto mark_build_match = ExecutionRegionJoinTypePropagatesBuildSide(contract.join_type);
	native_op.hash_join_probe.mark_build_match = mark_build_match && !native_op.hash_join_probe.residual_predicate;
	native_op.hash_join_probe.mark_build_match_after_residual =
	    mark_build_match && native_op.hash_join_probe.residual_predicate;
	if (render_diagnostics) {
		native_op.hash_join_probe.ir =
		    "hash_join_probe_native<hash_keys=" + std::to_string(native_op.hash_join_probe.equality_key_count) +
		    ",conditions=";
		native_op.hash_join_probe.ir += DescribeSljitHashJoinProbeKeys(native_op.hash_join_probe.keys, "|");
		native_op.hash_join_probe.ir += ",probe_shape=native";
		if (native_op.hash_join_probe.perfect_hash_probe) {
			native_op.hash_join_probe.ir += ",perfect_hash_probe_shape=native";
		} else {
			native_op.hash_join_probe.ir +=
			    ",perfect_hash_probe_shape=" + (contract.perfect_hash_probe_shape_blocker.empty()
			                                        ? string("not_applicable")
			                                        : contract.perfect_hash_probe_shape_blocker);
		}
		native_op.hash_join_probe.ir +=
		    ",output_mode=" + string(SljitHashJoinProbeOutputModeToString(native_op.hash_join_probe.output_mode));
		if (native_op.hash_join_probe.mark_build_match) {
			AppendSljitHashJoinProbeMarkOffsets(native_op.hash_join_probe.ir, "mark_build_match",
			                                    native_op.hash_join_probe);
		}
		if (native_op.hash_join_probe.mark_build_match_after_residual) {
			AppendSljitHashJoinProbeMarkOffsets(native_op.hash_join_probe.ir, "mark_build_match_after_residual",
			                                    native_op.hash_join_probe);
		}
		if (native_op.hash_join_probe.residual_predicate) {
			native_op.hash_join_probe.ir += ",residual_predicate=true";
			native_op.hash_join_probe.ir +=
			    ",residual_sources=" + std::to_string(native_op.hash_join_probe.residual_source_types.size());
			native_op.hash_join_probe.ir += ",residual_ir=(" + native_op.hash_join_probe.residual_filter.ir + ")";
		}
		native_op.hash_join_probe.ir += ">";
	}

	string reason = "generated native hash join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_hash_join_table_layout;"
		          "native-hash-join-probe-executable=ready;native_probe_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

SljitRegionNodePlan PlanSljitNestedLoopJoinProbeOperatorNode(const ExecutionRegionNode &node,
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
	if (render_diagnostics) {
		condition_plan.ir = "condition0<kind=" + string(SljitNestedLoopJoinValueKindToString(value_kind)) +
		                    ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type)) +
		                    ",lhs=(" + condition_plan.lhs_condition.ir + ")>";
	}
	native_op.nested_loop_join_probe.conditions.push_back(std::move(condition_plan));
	if (render_diagnostics) {
		native_op.nested_loop_join_probe.ir =
		    "nested_loop_join_probe_native<join_type=" + string(ExecutionRegionJoinTypeToString(contract.join_type)) +
		    ",conditions=1,value_kind=" + string(SljitNestedLoopJoinValueKindToString(value_kind)) +
		    ",comparison=" + string(SljitHashJoinComparisonToString(condition.comparison_type)) + ">";
	}

	string reason = "generated native nested loop join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_nested_loop_join_probe_cursor;"
		          "requires=primitive_compare_stub;native_probe_shape_ready=true";
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

} // namespace duckdb

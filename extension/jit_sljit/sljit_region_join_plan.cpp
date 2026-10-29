//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_join_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

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

static SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNodeInternal(
    const ExecutionRegionNode &node, const vector<ExecutionRegionHashJoinKeyInput> &probe_keys,
    const vector<LogicalType> &input_types, const vector<bool> &input_not_null, bool render_diagnostics) {
	if (!node.operator_info || node.operator_info->Kind() != ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
		return SljitRegionBoundaryNode("hash join probe operator is missing typed operator IR");
	}
	auto &contract = node.operator_info->HashJoin().contract;
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
	if (probe_keys.size() != contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering key count does not match contract");
	}
	if (contract.layout_offsets.size() < contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires hash table key layout offsets");
	}
	if (contract.equality_condition_count == 0 || contract.equality_condition_count > contract.condition_count) {
		return SljitRegionBoundaryNode("hash join probe native lowering requires an equality-key prefix");
	}

	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	keys.reserve(probe_keys.size());
	for (idx_t key_idx = 0; key_idx < probe_keys.size(); key_idx++) {
		auto &key = probe_keys[key_idx];
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
			return SljitRegionBoundaryNode("hash join probe native lowering key input index " +
			                               std::to_string(key.input_index) + " is outside operator input count " +
			                               std::to_string(input_types.size()));
		}
		SljitNativeHashJoinKeyKind key_kind;
		if (!SljitTryGetHashJoinKeyKind(key.type, key_kind)) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported key type " +
			                               key.type.ToString());
		}
		if (!equality_key && !SljitHashJoinKeyKindIsWord(key_kind)) {
			return SljitRegionBoundaryNode(
			    "hash join probe native lowering has unsupported multi-word match predicate " + key.type.ToString());
		}
		SljitNativeHashJoinProbeKeyPlan key_plan;
		key_plan.key_input_index = key.input_index;
		key_plan.key_layout_offset = contract.layout_offsets[key_idx];
		key_plan.key_type = key.type;
		key_plan.key_kind = key_kind;
		key_plan.comparison_type = comparison_type;
		key_plan.equality_key = equality_key;
		key_plan.null_equal = equality_key && comparison_type == ExecutionRegionComparisonType::NOT_DISTINCT_FROM;
		key_plan.source_known_not_null = key.input_index < input_not_null.size() && input_not_null[key.input_index];
		keys.push_back(std::move(key_plan));
	}

	SljitNativeRegionOpPlan native_op;
	native_op.SetKind(SljitNativeRegionOpKind::HASH_JOIN_PROBE);
	native_op.operator_index = node.operator_index;
	native_op.output_types = node.output_types;
	native_op.HashJoinProbe().operator_index = node.operator_index;
	native_op.HashJoinProbe().input_types = input_types;
	native_op.HashJoinProbe().keys = std::move(keys);
	native_op.HashJoinProbe().equality_key_count = contract.equality_condition_count;
	native_op.HashJoinProbe().found_match_offset = contract.tuple_size;
	native_op.HashJoinProbe().pointer_offset = contract.pointer_offset;
	native_op.HashJoinProbe().output_mode = contract.native_probe_output_mode;
	native_op.HashJoinProbe().operator_info = *node.operator_info;
	native_op.HashJoinProbe().operator_info.HashJoin().keys = probe_keys;
	if (contract.perfect_hash_probe_shape_ready) {
		if (native_op.HashJoinProbe().keys.size() != 1 || native_op.HashJoinProbe().equality_key_count != 1) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires one equality key");
		}
		auto perfect_key_kind = native_op.HashJoinProbe().keys[0].key_kind;
		if (perfect_key_kind == SljitNativeHashJoinKeyKind::STRING) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires an integer key");
		}
		if (native_op.HashJoinProbe().output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		    native_op.HashJoinProbe().output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
			return SljitRegionBoundaryNode("perfect hash join probe native lowering requires inner output mode");
		}
		native_op.HashJoinProbe().perfect_hash_probe = true;
	}
	if (contract.residual_predicate || contract.residual_info) {
		// A generated mark probe decides each row (or build row) at its first key match, which a residual predicate
		// can still reject
		if (contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY ||
		    contract.native_probe_output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
			return SljitRegionBoundaryNode("hash join probe native lowering has unsupported residual mark contract");
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
		native_op.HashJoinProbe().residual_predicate = true;
		native_op.HashJoinProbe().residual_filter = std::move(residual_filter);
		native_op.HashJoinProbe().residual_source_types.resize(contract.residual_sources.size());
		native_op.HashJoinProbe().residual_source_not_null.resize(contract.residual_sources.size(), false);
		for (auto &source : contract.residual_sources) {
			if (source.source_index >= native_op.HashJoinProbe().residual_source_types.size()) {
				return SljitRegionBoundaryNode("hash join probe native lowering residual source index is out of range");
			}
			if (source.kind == ExecutionHashJoinResidualSourceKind::PROBE && source.input_index >= input_types.size()) {
				return SljitRegionBoundaryNode(
				    "hash join probe native lowering residual probe source is outside operator input");
			}
			native_op.HashJoinProbe().residual_source_types[source.source_index] = source.type;
			native_op.HashJoinProbe().residual_source_not_null[source.source_index] = source.not_null;
		}
	}
	auto mark_build_match = ExecutionRegionJoinTypePropagatesBuildSide(contract.join_type);
	native_op.HashJoinProbe().mark_build_match = mark_build_match && !native_op.HashJoinProbe().residual_predicate;
	native_op.HashJoinProbe().mark_build_match_after_residual =
	    mark_build_match && native_op.HashJoinProbe().residual_predicate;

	string reason = "generated native hash join probe";
	if (render_diagnostics) {
		reason += ";requires=native_operator_runtime_binding;requires=native_hash_join_table_layout;"
		          "native-hash-join-probe-executable=ready;native_probe_shape_ready=true";
		reason += ";native_probe_key0_input=" + std::to_string(native_op.HashJoinProbe().keys[0].key_input_index);
		reason += ";native_probe_output_mode=" +
		          string(SljitHashJoinProbeOutputModeToString(native_op.HashJoinProbe().output_mode));
	}
	AppendSljitReasonPart(reason, node.operator_info->ir, render_diagnostics);
	return SljitNativeNode(std::move(native_op), std::move(reason));
}

// Probe keys computed from the input become columns the probe reads like any other: a projection ahead of the probe
// passes the input through and appends the computed keys, and the keys bind as references to them. The join's output
// selects its probe columns by index, so the appended keys never leave the probe.
static bool TryPlanSljitHashJoinProbeKeyProjection(const ExecutionRegionNode &node,
                                                   const vector<LogicalType> &input_types,
                                                   vector<ExecutionRegionHashJoinKeyInput> &probe_keys,
                                                   SljitRegionNodePlan &key_projection, bool render_diagnostics) {
	ExecutionRegionNode projection_node;
	projection_node.kind = ExecutionRegionNodeKind::PROJECTION;
	projection_node.operator_index = node.operator_index;
	projection_node.output_types = input_types;
	for (idx_t col_idx = 0; col_idx < input_types.size(); col_idx++) {
		auto fragment = make_uniq<ExecutionExpressionFragment>();
		fragment->expression_index = col_idx;
		fragment->return_type = input_types[col_idx];
		fragment->root = MakeSljitReferenceExpression(col_idx, input_types[col_idx]);
		projection_node.projections.push_back(std::move(fragment));
	}
	for (auto &key : probe_keys) {
		if (key.supported_reference || !key.expression_ready) {
			continue;
		}
		key.input_index = projection_node.output_types.size();
		key.supported_reference = true;
		projection_node.output_types.push_back(key.type);
		projection_node.projections.push_back(make_uniq<ExecutionExpressionFragment>(key.expression));
	}
	key_projection = PlanSljitProjectionNode(projection_node, input_types, render_diagnostics);
	return key_projection.kind == ExecutionRegionLoweringKind::NATIVE && SljitRegionNodeHasNativeOps(key_projection);
}

SljitRegionNodePlan PlanSljitHashJoinProbeOperatorNode(const ExecutionRegionNode &node,
                                                       const vector<LogicalType> &input_types,
                                                       const vector<bool> &input_not_null, bool render_diagnostics) {
	if (!node.operator_info || node.operator_info->Kind() != ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE) {
		return SljitRegionBoundaryNode("hash join probe operator is missing typed operator IR");
	}
	auto probe_keys = node.operator_info->HashJoin().keys;
	auto probe_input_types = input_types;
	auto probe_input_not_null = input_not_null;
	SljitRegionNodePlan key_projection;
	for (auto &key : probe_keys) {
		if (key.supported_reference || !key.expression_ready) {
			continue;
		}
		if (!TryPlanSljitHashJoinProbeKeyProjection(node, input_types, probe_keys, key_projection,
		                                            render_diagnostics)) {
			auto result = SljitRegionBoundaryNode("hash join probe key expression is unsupported by SLJIT projection;" +
			                                      key_projection.reason);
			result.fusion_blocker =
			    "operator-contract-blocker:hash-join-probe-native-lowering-missing;" + result.reason;
			return result;
		}
		probe_input_types = SljitRegionNodeLastNativeOp(key_projection).output_types;
		probe_input_not_null.resize(probe_input_types.size(), false);
		break;
	}
	auto result = PlanSljitHashJoinProbeOperatorNodeInternal(node, probe_keys, probe_input_types, probe_input_not_null,
	                                                         render_diagnostics);
	if (result.kind != ExecutionRegionLoweringKind::BOUNDARY) {
		if (!key_projection.native_ops.empty()) {
			result.native_ops.insert(result.native_ops.begin(),
			                         std::make_move_iterator(key_projection.native_ops.begin()),
			                         std::make_move_iterator(key_projection.native_ops.end()));
		}
		return result;
	}
	const bool contract_ready = node.operator_info && node.operator_info->HashJoin().contract.present &&
	                            node.operator_info->HashJoin().contract.native_probe_contract.status ==
	                                ExecutionRegionStateContractStatus::READY;
	result.fusion_blocker = contract_ready
	                            ? "operator-contract-blocker:hash-join-probe-native-lowering-missing;" + result.reason
	                            : "operator-contract-blocker:hash-join-probe-contract-missing";
	return result;
}

} // namespace duckdb

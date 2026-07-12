//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_update_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"

#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

static bool
SljitPerfectHashGroupExpressionsUseTypedTree(const vector<SljitNativeRegionExpressionPlan> &group_expressions) {
	for (auto &group_expression : group_expressions) {
		if (group_expression.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return true;
		}
	}
	return false;
}

bool TryBuildSljitPerfectHashFusedUpdatePlan(
    const ExecutionExpressionIR *predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitPerfectHashFusedUpdatePlan &result, string &error) {
	result = SljitPerfectHashFusedUpdatePlan();
	const bool typed_group_expressions = SljitPerfectHashGroupExpressionsUseTypedTree(group_expressions);
	if (!TryBuildSljitPerfectHashGroupPlans(groups, group_expressions, contract, result.group_plans,
	                                        typed_group_expressions) ||
	    result.group_plans.empty() || !contract.grouped_state_layout_ready ||
	    !BuildSljitFusedTypedAggregateCodegenPlan(payloads, aggregates, result.codegen_plan, typed_group_expressions)) {
		error = "unsupported fused perfect-hash typed aggregate payload shape";
		return false;
	}
	for (auto &group_plan : result.group_plans) {
		if (group_plan.expression_kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			continue;
		}
		if (!group_plan.expression_tree) {
			error = "unsupported fused perfect-hash typed aggregate group shape";
			return false;
		}
		auto tree_plan = BuildSljitTypedExpressionTreePlan(*group_plan.expression_tree, false);
		if (!tree_plan.supported) {
			error = "unsupported fused perfect-hash typed aggregate group shape";
			return false;
		}
		result.codegen_plan.tree_node_count += tree_plan.node_count;
		result.codegen_plan.fast_path_supported =
		    result.codegen_plan.fast_path_supported && tree_plan.fast_path.fast_path_supported;
	}
	if (predicate) {
		auto predicate_plan = BuildSljitTypedExpressionTreePlan(*predicate, false);
		if (!predicate_plan.supported || !predicate_plan.result_is_bool) {
			error = "unsupported filtered fused perfect-hash aggregate predicate shape";
			return false;
		}
		result.codegen_plan.tree_node_count += predicate_plan.node_count;
		result.codegen_plan.fast_path_supported =
		    result.codegen_plan.fast_path_supported && predicate_plan.fast_path.fast_path_supported;
	}
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		error = "unsupported fused perfect-hash typed aggregate domain size";
		return false;
	}
	result.perfect_hash_group_count = idx_t(1) << contract.perfect_required_bits_total;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedTypedAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                     contract)) {
			error = "unsupported fused perfect-hash typed aggregate payload shape";
			return false;
		}
	}

	const auto tree_local_size = NumericCast<sljit_sw>(result.codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	result.state_pointer_offset = tree_local_size;
	result.group_index_offset = result.state_pointer_offset + NumericCast<sljit_sw>(sizeof(uintptr_t));
	result.binary_shared_value_offset = result.group_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	result.local_size = result.binary_shared_value_offset;
	if (result.codegen_plan.binary_shared_payload) {
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	auto payloads_not_null = BuildSljitAggregatePayloadNotNull(payloads, aggregates, source_not_null);
	TryBuildSljitLocalPerfectHashAggregatePlan(aggregates, contract, payloads_not_null, result.local_size,
	                                           result.local_aggregate_plan);
	AnnotateSljitLocalPerfectHashAggregatePlan(result.local_aggregate_plan, payloads, aggregates, source_min_values,
	                                           source_max_values);
	result.source_data_hoists = BuildSljitPerfectHashSourceDataPointerHoists(payloads);
	result.hoist_source_data_pointers = SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS &&
	                                    result.source_data_hoists.size() >= result.group_plans.size() &&
	                                    !result.source_data_hoists.empty();
	const bool group_data_pointer_hoist_candidate =
	    !result.hoist_source_data_pointers && SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS && result.group_plans.size() <= 2;
	if (!result.local_aggregate_plan.enabled) {
		TryBuildSljitDeferredPerfectHashFlagPlan(aggregates, contract, result.local_size, result.deferred_flag_plan);
	}
	result.fast_source_data_hoists =
	    result.codegen_plan.fast_path_supported
	        ? (result.hoist_source_data_pointers ? BuildSljitPerfectHashSourceDataPointerHoists(payloads, 3, true)
	                                             : BuildSljitPerfectHashSpareFastSourceDataPointerHoists(payloads))
	        : result.source_data_hoists;
	if (!result.hoist_source_data_pointers) {
		result.source_data_hoists.clear();
	} else if (result.fast_source_data_hoists.size() < result.source_data_hoists.size()) {
		result.fast_source_data_hoists = result.source_data_hoists;
	}
	result.hoist_group_data_pointers = group_data_pointer_hoist_candidate;
	result.hoist_fast_source_data_pointers = !result.fast_source_data_hoists.empty() &&
	                                         (!result.hoist_source_data_pointers ||
	                                          result.fast_source_data_hoists.size() > result.source_data_hoists.size());
	result.hoist_fast_group_data_array_base =
	    result.hoist_source_data_pointers && result.codegen_plan.fast_path_supported &&
	    result.local_aggregate_plan.sparse && SljitCanPrecomputePerfectHashStringGroupOffset(result.group_plans);
	result.dedicated_state_register =
	    SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG && !result.local_aggregate_plan.enabled;
	result.dedicated_group_index_register = SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG &&
	                                        result.local_aggregate_plan.enabled && !result.local_aggregate_plan.sparse;
	result.state_pointer_reg = result.dedicated_state_register ? SLJIT_PERFECT_HASH_STATE_REG : SLJIT_S4;
	result.group_index_reg = result.dedicated_group_index_register ? SLJIT_PERFECT_HASH_GROUP_INDEX_REG : SLJIT_S4;
	result.saved_register_count = (result.dedicated_state_register || result.local_aggregate_plan.sparse)
	                                  ? SLJIT_PERFECT_HASH_SAVED_REG_COUNT
	                                  : NumericCast<sljit_s32>(7);
	if (result.dedicated_group_index_register) {
		result.saved_register_count = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
	}
	if (result.hoist_group_data_pointers || result.hoist_source_data_pointers) {
		result.saved_register_count = SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT;
	}
	return true;
}

} // namespace duckdb

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
	    !BuildSljitFusedAggregateCodegenPlan(payloads, aggregates, result.codegen_plan)) {
		error = "unsupported fused perfect-hash aggregate payload shape";
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
		error = "unsupported fused perfect-hash aggregate domain size";
		return false;
	}
	result.perfect_hash_group_count = idx_t(1) << contract.perfect_required_bits_total;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx], contract)) {
			error = "unsupported fused perfect-hash aggregate payload shape";
			return false;
		}
	}

	const auto tree_local_size = NumericCast<sljit_sw>(result.codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	result.state_pointer_offset = tree_local_size;
	result.group_index_offset = result.state_pointer_offset + NumericCast<sljit_sw>(sizeof(uintptr_t));
	result.binary_shared_value_offset = result.group_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	result.local_size = result.binary_shared_value_offset;
	if (result.codegen_plan.shared_binary.Enabled()) {
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	if (predicate && result.codegen_plan.fast_path_supported) {
		auto predicate_simd_plan = TryPlanSljitTypedExpressionTreeSimd(*predicate);
		// The shared hybrid cost contract compares scalar predicate work with mask
		// dispatch overhead. Terminals consume that decision instead of embedding
		// their own expression-shape threshold.
		if (SljitTypedExpressionTreeSimdHybridFilterProfitable(predicate_simd_plan)) {
			result.predicate_simd_plan = std::move(predicate_simd_plan);
			result.predicate_simd_mask_offset = (result.local_size + 15) & ~sljit_sw(15);
			result.local_size = result.predicate_simd_mask_offset + 24;
			auto vector_register_count = result.predicate_simd_plan.constant_count +
			                             result.predicate_simd_plan.max_live_temps +
			                             (result.predicate_simd_plan.needs_all_ones ? idx_t(1) : idx_t(0));
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
			// ARM64 keeps one horizontal mask-reduction destination live so uniform
			// groups avoid materializing a full scalar movemask.
			vector_register_count++;
#endif
			result.scratch_register_count = 5 | SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_register_count));
		}
	}
	auto payloads_not_null = BuildSljitAggregatePayloadNotNull(payloads, aggregates, source_not_null);
	auto batch_lower_never_overflows =
	    BuildSljitDensePerfectHashLowerNeverOverflows(payloads, aggregates, source_min_values, source_max_values);
	TryBuildSljitDensePerfectHashAggregateReductionPlan(aggregates, contract, payloads_not_null,
	                                                    batch_lower_never_overflows, result.local_size,
	                                                    result.dense_reduction_plan);
	result.source_data_hoists = BuildSljitPerfectHashSourceDataPointerHoists(payloads);
	result.hoist_source_data_pointers = SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS &&
	                                    result.source_data_hoists.size() >= result.group_plans.size() &&
	                                    !result.source_data_hoists.empty();
	const bool group_data_pointer_hoist_candidate =
	    !result.hoist_source_data_pointers && SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS && result.group_plans.size() <= 2;
	if (!result.dense_reduction_plan.Ready()) {
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
	result.dedicated_state_register =
	    SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG && !result.dense_reduction_plan.Ready();
	result.dedicated_reduction_state_register =
	    SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG && result.dense_reduction_plan.Ready();
	// In each flat fast-path row, S7 starts as a transient group-data-array base
	// and becomes the direct perfect-hash state pointer only after every group
	// key has consumed it. This removes repeated input-struct loads without
	// borrowing registers owned by the typed SIMD expression emitter.
	result.hoist_fast_group_data_array_base =
	    result.hoist_source_data_pointers && result.codegen_plan.fast_path_supported &&
	    (result.dense_reduction_plan.Ready() || result.dedicated_state_register) &&
	    SljitCanPrecomputePerfectHashStringGroupOffset(result.group_plans);
	result.state_pointer_reg = result.dedicated_state_register ? SLJIT_PERFECT_HASH_STATE_REG : SLJIT_S4;
	result.reduction_state_reg =
	    result.dedicated_reduction_state_register ? SLJIT_PERFECT_HASH_REDUCTION_STATE_REG : SLJIT_S4;
	result.saved_register_count =
	    result.dedicated_state_register ? SLJIT_PERFECT_HASH_SAVED_REG_COUNT : NumericCast<sljit_s32>(7);
	if (result.dedicated_reduction_state_register) {
		result.saved_register_count = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
	}
	if (result.hoist_group_data_pointers || result.hoist_source_data_pointers) {
		result.saved_register_count = SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT;
	}
	return true;
}

} // namespace duckdb

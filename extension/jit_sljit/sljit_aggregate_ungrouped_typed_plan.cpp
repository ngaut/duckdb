//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_typed_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_ungrouped_typed_codegen.hpp"

#include "sljit_aggregate_source_hoist_codegen.hpp"

#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

static vector<sljit_s32> BuildSljitUngroupedAggregateSourceDataPointerRegs(idx_t max_hoists,
                                                                           bool include_fast_spare_reg) {
	vector<sljit_s32> result;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	if (max_hoists > 0) {
		result.push_back(SLJIT_S8);
	}
	if (max_hoists > 1) {
		result.push_back(SLJIT_S9);
	}
	if (include_fast_spare_reg && max_hoists > 2) {
		result.push_back(SLJIT_S6);
	}
#endif
	return result;
}

static vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitUngroupedAggregateSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    idx_t max_hoists, bool include_fast_spare_reg,
                                                    idx_t min_use_count) {
	auto regs = BuildSljitUngroupedAggregateSourceDataPointerRegs(max_hoists, include_fast_spare_reg);
	return BuildSljitAggregateSourceDataPointerHoists(payloads, regs, min_use_count);
}

bool TryBuildSljitUngroupedFusedAggregateUpdatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                                    SljitUngroupedFusedAggregateUpdatePlan &result, string &error) {
	result = SljitUngroupedFusedAggregateUpdatePlan();
	if (!BuildSljitFusedAggregateCodegenPlan(payloads, aggregates, result.codegen_plan)) {
		error = "unsupported fused aggregate payload shape";
		return false;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(result.codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	result.local_sum_offsets.assign(payloads.size(), -1);
	result.local_sum_upper_offsets.assign(payloads.size(), -1);
	result.saw_value_offsets.assign(payloads.size(), -1);
	result.local_size = tree_local_size;
	if (result.codegen_plan.conditional_shared_payload) {
		result.shared_fast_value_offset = result.local_size;
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	result.use_conditional_hugeint_register_accumulators =
	    SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS && result.codegen_plan.conditional_shared_payload &&
	    aggregates[result.codegen_plan.shared_lane].primitive_update_kind ==
	        AggregatePrimitiveUpdateKind::SUM_HUGEINT &&
	    aggregates[result.codegen_plan.conditional_lane].primitive_update_kind ==
	        AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	if (result.use_conditional_hugeint_register_accumulators) {
		result.register_accumulator_used_offset = result.local_size;
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		result.local_sum_offsets[payload_idx] = result.local_size;
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			result.local_sum_upper_offsets[payload_idx] = result.local_size;
			result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		}
		result.saw_value_offsets[payload_idx] = result.local_size;
		result.local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}

	result.source_data_hoists = BuildSljitUngroupedAggregateSourceDataPointerHoists(payloads, 2, false, 2);
	result.fast_source_data_hoists = result.codegen_plan.fast_path_supported
	                                     ? BuildSljitUngroupedAggregateSourceDataPointerHoists(payloads, 3, true, 1)
	                                     : result.source_data_hoists;
	if (result.fast_source_data_hoists.size() < result.source_data_hoists.size()) {
		result.fast_source_data_hoists = result.source_data_hoists;
	}
	result.hoist_source_data_pointers = !result.source_data_hoists.empty();
	result.hoist_fast_source_data_pointers = !result.fast_source_data_hoists.empty();
	result.saved_register_count = result.hoist_source_data_pointers || result.hoist_fast_source_data_pointers
	                                  ? NumericCast<sljit_s32>(10)
	                                  : SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT;
	if (result.use_conditional_hugeint_register_accumulators) {
		result.saved_register_count = NumericCast<sljit_s32>(14);
	}
	return true;
}

} // namespace duckdb

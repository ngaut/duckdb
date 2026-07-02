//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_typed_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_typed_payload_codegen.hpp"

namespace duckdb {

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 14
static constexpr bool SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS = true;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_LOWER_REG = SLJIT_S10;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_UPPER_REG = SLJIT_S11;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG = SLJIT_S12;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG = SLJIT_S13;
#else
static constexpr bool SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS = false;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_LOWER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_UPPER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG = SLJIT_R0;
#endif

struct SljitUngroupedTypedAggregateUpdatePlan {
	SljitFusedTypedAggregateCodegenPlan codegen_plan;
	vector<sljit_sw> local_sum_offsets;
	vector<sljit_sw> local_sum_upper_offsets;
	vector<sljit_sw> saw_value_offsets;
	vector<SljitTypedExpressionTreeDataPointerHoist> source_data_hoists;
	vector<SljitTypedExpressionTreeDataPointerHoist> fast_source_data_hoists;
	sljit_sw local_size = 0;
	sljit_sw shared_fast_value_offset = -1;
	sljit_sw register_accumulator_used_offset = -1;
	bool use_conditional_hugeint_register_accumulators = false;
	bool hoist_source_data_pointers = false;
	bool hoist_fast_source_data_pointers = false;
	sljit_s32 saved_register_count = 0;
};

bool TryBuildSljitUngroupedTypedAggregateUpdatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                                    SljitUngroupedTypedAggregateUpdatePlan &result, string &error);

} // namespace duckdb

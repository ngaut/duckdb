//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_typed_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_register_layout.hpp"

namespace duckdb {

struct SljitUngroupedFusedAggregateUpdatePlan {
	SljitFusedAggregateCodegenPlan codegen_plan;
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

bool TryBuildSljitUngroupedFusedAggregateUpdatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                                    SljitUngroupedFusedAggregateUpdatePlan &result, string &error);

} // namespace duckdb

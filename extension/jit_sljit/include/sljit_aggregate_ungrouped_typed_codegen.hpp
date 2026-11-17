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
	vector<SljitTypedExpressionTreeLoopHoist> source_data_hoists;
	vector<SljitTypedExpressionTreeLoopHoist> fast_source_data_hoists;
	sljit_sw local_size = 0;
	sljit_sw shared_fast_value_offset = -1;
	sljit_sw register_accumulator_used_offset = -1;
	//! The stack slot of the selection a selected fast loop reads its logical rows through
	sljit_sw logical_selection_offset = -1;
	bool use_conditional_hugeint_register_accumulators = false;
	//! The register pair each payload's fast loops accumulate into, or invalid when it accumulates on the stack
	vector<idx_t> register_accumulator_pairs;
	bool hoist_source_data_pointers = false;
	bool hoist_fast_source_data_pointers = false;
	sljit_s32 scratch_register_count = 5;
	sljit_s32 saved_register_count = 0;
};

bool TryBuildSljitUngroupedFusedAggregateUpdatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                                    SljitUngroupedFusedAggregateUpdatePlan &result, string &error);

} // namespace duckdb

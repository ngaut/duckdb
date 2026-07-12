//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_update_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_perfect_hash_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

struct SljitPerfectHashFusedUpdateEmitContext {
	struct sljit_compiler *compiler;
	const ExecutionExpressionIR *predicate;
	const vector<SljitNativeRegionExpressionPlan> &payloads;
	const vector<ExecutionRegionAggregateInput> &aggregates;
	const vector<SljitPerfectHashGroupPlan> &group_plans;
	const ExecutionRegionAggregateContract &contract;
	const SljitFusedTypedAggregateCodegenPlan &codegen_plan;
	const SljitLocalPerfectHashAggregatePlan &local_aggregate_plan;
	const SljitDeferredPerfectHashFlagPlan &deferred_flag_plan;
	vector<SljitExpressionTreeOverflowJumps> &overflows;
	vector<sljit_jump *> &group_out_of_range;
	idx_t perfect_hash_group_count;
	sljit_sw state_pointer_offset;
	sljit_sw group_index_offset;
	sljit_sw binary_shared_value_offset;
	bool hoist_group_data_pointers;
	bool dedicated_state_register;
	sljit_s32 state_pointer_reg;
	sljit_s32 group_index_reg;
};

struct SljitPerfectHashGroupLookupOptions {
	bool check_group_validity = false;
	bool materialize_state_pointer = false;
	bool defer_flags = false;
	bool direct_group_index = false;
	bool expression_fast_path = false;
	bool expression_all_valid = false;
	bool expression_no_source_selection = false;
	bool mark_local_payloads_seen = false;
	bool use_fast_group_data_array_base = false;
	bool mark_local_group = true;
	bool increment_count_seen = true;
	bool group_selection_all_present = false;
	sljit_s32 group_sel_array_base_reg = 0;
	sljit_s32 group_data_array_base_reg_override = 0;
	const vector<SljitTypedExpressionTreeDataPointerHoist> *expression_data_hoists = nullptr;
};

struct SljitPerfectHashPayloadUpdateOptions {
	bool fast_path = false;
	bool all_valid = false;
	bool no_source_selection = false;
	const vector<SljitTypedExpressionTreeDataPointerHoist> *payload_data_hoists = nullptr;
	const vector<SljitSparseLocalRunCachedLane> *run_cached_lanes = nullptr;
};

struct SljitPerfectHashFusedUpdatePlan {
	vector<SljitPerfectHashGroupPlan> group_plans;
	SljitFusedTypedAggregateCodegenPlan codegen_plan;
	SljitLocalPerfectHashAggregatePlan local_aggregate_plan;
	SljitDeferredPerfectHashFlagPlan deferred_flag_plan;
	vector<SljitSparseLocalRunCachedLane> sparse_run_cached_lanes;
	vector<SljitTypedExpressionTreeDataPointerHoist> source_data_hoists;
	vector<SljitTypedExpressionTreeDataPointerHoist> fast_source_data_hoists;
	idx_t perfect_hash_group_count = 0;
	sljit_sw local_size = 0;
	sljit_sw state_pointer_offset = -1;
	sljit_sw group_index_offset = -1;
	sljit_sw binary_shared_value_offset = -1;
	sljit_sw sparse_run_cached_group_offset = -1;
	sljit_sw sparse_run_cached_pointer_offset = -1;
	sljit_sw sparse_run_cached_start_offset = -1;
	sljit_sw sparse_run_cached_count_offset = -1;
	bool sparse_run_cache_enabled = false;
	bool sparse_run_cache_count_accepted_rows = false;
	bool hoist_source_data_pointers = false;
	bool hoist_group_data_pointers = false;
	bool hoist_fast_source_data_pointers = false;
	bool hoist_fast_group_data_array_base = false;
	bool dedicated_state_register = false;
	bool dedicated_group_index_register = false;
	sljit_s32 state_pointer_reg = SLJIT_S4;
	sljit_s32 group_index_reg = SLJIT_S4;
	sljit_s32 saved_register_count = 0;
};

bool TryBuildSljitPerfectHashFusedUpdatePlan(
    const ExecutionExpressionIR *predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitPerfectHashFusedUpdatePlan &result, string &error);

void EmitSljitPerfectHashFusedUpdateLoops(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashFusedUpdatePlan &update_plan);
vector<sljit_jump *>
EmitSljitPerfectHashPredicateSkipJumps(const SljitPerfectHashFusedUpdateEmitContext &context, bool fast_path,
                                       bool all_valid, bool no_source_selection,
                                       const vector<SljitTypedExpressionTreeDataPointerHoist> *predicate_data_hoists);
void EmitSljitPerfectHashPredicateSkipLabel(struct sljit_compiler *compiler,
                                            const vector<sljit_jump *> &predicate_skip_jumps);
void EmitSljitPerfectHashGroupLookup(const SljitPerfectHashFusedUpdateEmitContext &context,
                                     const SljitPerfectHashGroupLookupOptions &options);
void EmitSljitPerfectHashPayloadUpdates(const SljitPerfectHashFusedUpdateEmitContext &context,
                                        const SljitPerfectHashPayloadUpdateOptions &options);
void EmitLoadSljitCommonSelectedSourceIndex(struct sljit_compiler *compiler);

} // namespace duckdb

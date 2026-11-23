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
#include "sljit_typed_expression_simd_codegen.hpp"

namespace duckdb {

struct SljitPerfectHashFusedUpdateEmitContext {
	struct sljit_compiler *compiler;
	const ExecutionExpressionIR *predicate;
	const vector<SljitNativeRegionExpressionPlan> &payloads;
	const vector<ExecutionRegionAggregateInput> &aggregates;
	const vector<SljitPerfectHashGroupPlan> &group_plans;
	const ExecutionRegionAggregateContract &contract;
	const SljitFusedAggregateCodegenPlan &codegen_plan;
	const SljitDensePerfectHashAggregateReductionPlan &dense_reduction_plan;
	vector<SljitPerfectHashFirstTouchSite> &first_touch_sites;
	vector<SljitArithmeticOverflowSite> &overflows;
	vector<sljit_jump *> &group_out_of_range;
	idx_t perfect_hash_group_count;
	sljit_sw state_pointer_offset;
	sljit_sw group_index_offset;
	sljit_sw binary_shared_value_offset;
	bool hoist_group_data_pointers;
	bool dedicated_state_register;
	sljit_s32 state_pointer_reg;
	sljit_s32 reduction_state_reg;
	bool states_markable_on_first_touch;
	//! Stack slot of a guarded sum's mask, or -1 when the fast rows branch on the payloads that may skip them
	sljit_sw guard_offset;
};

struct SljitPerfectHashGroupLookupOptions {
	bool check_group_validity = false;
	bool materialize_state_pointer = false;
	//! Mark the group and its states set out of line, on the group's first touch, instead of on every row. Only for
	//! rows whose payload values are valid: a state is then set as soon as its group is.
	bool mark_states_on_first_touch = false;
	bool direct_group_index = false;
	bool expression_fast_path = false;
	bool expression_all_valid = false;
	bool expression_no_source_selection = false;
	bool use_fast_group_data_array_base = false;
	bool group_selection_all_present = false;
	bool use_dictionary_group_contributions = false;
	sljit_s32 group_sel_array_base_reg = 0;
	sljit_s32 group_data_array_base_reg_override = 0;
	sljit_s32 group_dictionary_runtime_array_base_reg = 0;
	const vector<SljitTypedExpressionTreeLoopHoist> *expression_data_hoists = nullptr;
};

struct SljitPerfectHashPayloadUpdateOptions {
	bool fast_path = false;
	bool all_valid = false;
	bool no_source_selection = false;
	//! Flat rows whose payload sources may be NULL (SljitPerfectHashFusedUpdatePlan::null_checked_loop): a payload
	//! skips its update when one of its sources is NULL. The source validity array must be in the native vector
	//! layout's optional invariant register.
	bool null_rows = false;
	//! The group lookup marks the states set on the group's first touch: the updates leave the flags alone
	bool states_marked_on_first_touch = false;
	const vector<SljitTypedExpressionTreeLoopHoist> *payload_data_hoists = nullptr;
};

struct SljitPerfectHashFusedUpdatePlan {
	vector<SljitPerfectHashGroupPlan> group_plans;
	SljitFusedAggregateCodegenPlan codegen_plan;
	SljitDensePerfectHashAggregateReductionPlan dense_reduction_plan;
	//! Every aggregate's state is a count or a sum, whose set flag the first touch of its group can set
	bool states_markable_on_first_touch = false;
	SljitTypedExpressionTreeSimdPlan predicate_simd_plan;
	vector<SljitTypedExpressionTreeLoopHoist> source_data_hoists;
	vector<SljitTypedExpressionTreeLoopHoist> fast_source_data_hoists;
	idx_t perfect_hash_group_count = 0;
	sljit_sw local_size = 0;
	sljit_sw state_pointer_offset = -1;
	sljit_sw group_index_offset = -1;
	sljit_sw binary_shared_value_offset = -1;
	sljit_sw predicate_simd_mask_offset = -1;
	//! Every payload that may skip a fast row is a COUNT or a 64-bit sum of CASE WHEN c THEN v END whose v cannot raise
	//! an error: the fast rows update them without a branch (a COUNT adds c, a sum adds v masked by -c, kept in this
	//! stack slot, and ORs c into its set flag). -1 otherwise
	sljit_sw guard_offset = -1;
	//! Flat batches whose payload sources hold NULLs run a fast loop: every payload and the predicate are null-strict
	//! (a payload skips its update on a NULL source, the predicate decides NULL as FALSE) and every group is a column
	bool null_checked_loop = false;
	bool hoist_source_data_pointers = false;
	bool hoist_group_data_pointers = false;
	bool hoist_fast_source_data_pointers = false;
	bool hoist_fast_group_data_array_base = false;
	bool dedicated_state_register = false;
	bool dedicated_reduction_state_register = false;
	sljit_s32 state_pointer_reg = SLJIT_S4;
	sljit_s32 reduction_state_reg = SLJIT_S4;
	sljit_s32 saved_register_count = 0;
	sljit_s32 scratch_register_count = 5;
};

bool TryBuildSljitPerfectHashFusedUpdatePlan(
    const ExecutionExpressionIR *predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitPerfectHashFusedUpdatePlan &result, string &error);

void EmitSljitPerfectHashFusedUpdateLoops(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashFusedUpdatePlan &update_plan);
void EmitSljitPerfectHashGroupLookup(const SljitPerfectHashFusedUpdateEmitContext &context,
                                     const SljitPerfectHashGroupLookupOptions &options);
void EmitSljitPerfectHashPayloadUpdates(const SljitPerfectHashFusedUpdateEmitContext &context,
                                        const SljitPerfectHashPayloadUpdateOptions &options);
vector<sljit_jump *>
EmitSljitPerfectHashPredicateSkipJumps(const SljitPerfectHashFusedUpdateEmitContext &context, bool fast_path,
                                       bool all_valid, bool no_source_selection,
                                       const vector<SljitTypedExpressionTreeLoopHoist> *predicate_data_hoists);
void EmitSljitPerfectHashPredicateSkipLabel(struct sljit_compiler *compiler,
                                            const vector<sljit_jump *> &predicate_skip_jumps);
void EmitLoadSljitCommonSelectedSourceIndex(struct sljit_compiler *compiler);

} // namespace duckdb

#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_perfect_hash_codegen.hpp"
#include "sljit_aggregate_perfect_hash_local_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitPerfectHashUpdateLoopOptions {
	bool reset_index = false;
	bool direct_logical_index = false;
	bool predicate_fast_path = false;
	bool all_valid = false;
	bool no_source_selection = false;
	const vector<SljitTypedExpressionTreeDataPointerHoist> *predicate_data_hoists = nullptr;
	bool load_fast_group_data_array_base = false;
	bool load_fast_group_dictionary_runtime_array_base = false;
	SljitPerfectHashGroupLookupOptions group_lookup;
	bool load_common_selected_source_index = false;
	SljitPerfectHashPayloadUpdateOptions payload_update;
};

static void EmitSljitPerfectHashFastGroupDataArrayBase(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_data_array));
}

static void EmitSljitPerfectHashFastGroupDictionaryRuntimeArrayBase(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
}

static void EmitSljitPerfectHashRowUpdate(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashGroupLookupOptions &group_lookup,
                                          const SljitPerfectHashPayloadUpdateOptions &payload_update,
                                          bool load_common_selected_source_index) {
	auto compiler = context.compiler;
	EmitSljitPerfectHashGroupLookup(context, group_lookup);
	if (load_common_selected_source_index) {
		EmitLoadSljitCommonSelectedSourceIndex(compiler);
	}
	EmitSljitPerfectHashPayloadUpdates(context, payload_update);
}

static sljit_jump *EmitSljitPerfectHashUpdateLoop(const SljitPerfectHashFusedUpdateEmitContext &context,
                                                  const SljitPerfectHashUpdateLoopOptions &options) {
	auto compiler = context.compiler;
	if (options.reset_index) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler, options.direct_logical_index);
	auto predicate_skip_jumps =
	    EmitSljitPerfectHashPredicateSkipJumps(context, options.predicate_fast_path, options.all_valid,
	                                           options.no_source_selection, options.predicate_data_hoists);
	auto group_lookup = options.group_lookup;
	group_lookup.expression_fast_path = options.payload_update.fast_path;
	group_lookup.expression_all_valid = options.payload_update.all_valid;
	group_lookup.expression_no_source_selection = options.payload_update.no_source_selection;
	group_lookup.expression_data_hoists = options.payload_update.payload_data_hoists;
	if (options.load_fast_group_data_array_base) {
		EmitSljitPerfectHashFastGroupDataArrayBase(compiler);
	}
	if (options.load_fast_group_dictionary_runtime_array_base) {
		EmitSljitPerfectHashFastGroupDictionaryRuntimeArrayBase(compiler);
	}
	EmitSljitPerfectHashRowUpdate(context, group_lookup, options.payload_update,
	                              options.load_common_selected_source_index);
	EmitSljitPerfectHashPredicateSkipLabel(compiler, predicate_skip_jumps);
	EmitNextSljitNativeVectorLoop(compiler, loop);
	return done;
}

static void EmitSljitPerfectHashFastSourceDataHoists(struct sljit_compiler *compiler,
                                                     const SljitPerfectHashFusedUpdatePlan &update_plan) {
	if (!update_plan.hoist_fast_source_data_pointers) {
		return;
	}
	for (idx_t hoist_idx = update_plan.source_data_hoists.size();
	     hoist_idx < update_plan.fast_source_data_hoists.size(); hoist_idx++) {
		auto &hoist = update_plan.fast_source_data_hoists[hoist_idx];
		sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
		               SljitPointerArrayOffset(hoist.source_index));
	}
}

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashDirectGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                         bool use_fast_group_data_array_base);
static SljitPerfectHashPayloadUpdateOptions SljitPerfectHashPayloadUpdateOptionsForLoop(
    bool fast_path, bool all_valid, bool no_source_selection,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *payload_data_hoists);

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashDirectGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                         bool use_fast_group_data_array_base) {
	SljitPerfectHashGroupLookupOptions result;
	result.materialize_state_pointer = !context.dense_reduction_plan.Ready();
	result.defer_flags = context.deferred_flag_plan.enabled;
	result.direct_group_index = true;
	result.use_fast_group_data_array_base = use_fast_group_data_array_base;
	return result;
}

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashSelectedGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context) {
	SljitPerfectHashGroupLookupOptions result;
	result.materialize_state_pointer = !context.dense_reduction_plan.Ready();
	result.defer_flags = context.deferred_flag_plan.enabled;
	return result;
}

static SljitPerfectHashPayloadUpdateOptions SljitPerfectHashPayloadUpdateOptionsForLoop(
    bool fast_path, bool all_valid, bool no_source_selection,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *payload_data_hoists) {
	SljitPerfectHashPayloadUpdateOptions result;
	result.fast_path = fast_path;
	result.all_valid = all_valid;
	result.no_source_selection = no_source_selection;
	result.payload_data_hoists = payload_data_hoists;
	return result;
}

static SljitPerfectHashUpdateLoopOptions
SljitPerfectHashDictionaryGroupLoopOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                           const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists,
                                           bool payload_uses_common_selection) {
	SljitPerfectHashUpdateLoopOptions result;
	result.reset_index = true;
	result.direct_logical_index = true;
	result.all_valid = true;
	result.predicate_data_hoists = data_hoists;
	result.load_fast_group_dictionary_runtime_array_base = SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG;
	result.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
	result.group_lookup.group_selection_all_present = true;
	result.group_lookup.use_dictionary_group_contributions = true;
	result.group_lookup.group_sel_array_base_reg = SLJIT_S6;
	result.group_lookup.group_dictionary_runtime_array_base_reg =
	    SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG ? SLJIT_PERFECT_HASH_STATE_REG : 0;
	result.load_common_selected_source_index = payload_uses_common_selection;
	result.payload_update =
	    SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, payload_uses_common_selection, data_hoists);
	return result;
}

static sljit_jump *
EmitSljitPerfectHashFlatFastLoop(const SljitPerfectHashFusedUpdateEmitContext &context,
                                 const SljitPerfectHashFusedUpdatePlan &update_plan,
                                 const vector<SljitTypedExpressionTreeDataPointerHoist> *fast_data_hoists) {
	auto compiler = context.compiler;
	if (update_plan.predicate_simd_plan.supported) {
		EmitSljitTypedExpressionTreeSimdHybridFilterLoop(
		    compiler, *context.predicate, update_plan.predicate_simd_plan, update_plan.predicate_simd_mask_offset,
		    [&]() {
			    EmitLoadFusedAggregateExecuteIndex(compiler, true);
			    if (update_plan.hoist_fast_group_data_array_base) {
				    EmitSljitPerfectHashFastGroupDataArrayBase(compiler);
			    }
			    auto lookup =
			        SljitPerfectHashDirectGroupLookupOptions(context, update_plan.hoist_fast_group_data_array_base);
			    lookup.expression_fast_path = true;
			    lookup.expression_all_valid = true;
			    lookup.expression_data_hoists = fast_data_hoists;
			    EmitSljitPerfectHashRowUpdate(
			        context, lookup, SljitPerfectHashPayloadUpdateOptionsForLoop(true, true, false, fast_data_hoists),
			        false);
		    });
	}
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler, true);
	auto predicate_skip_jumps = EmitSljitPerfectHashPredicateSkipJumps(context, true, true, false, fast_data_hoists);
	if (update_plan.hoist_fast_group_data_array_base) {
		EmitSljitPerfectHashFastGroupDataArrayBase(compiler);
	}
	auto lookup = SljitPerfectHashDirectGroupLookupOptions(context, update_plan.hoist_fast_group_data_array_base);
	lookup.expression_fast_path = true;
	lookup.expression_all_valid = true;
	lookup.expression_data_hoists = fast_data_hoists;
	EmitSljitPerfectHashRowUpdate(
	    context, lookup, SljitPerfectHashPayloadUpdateOptionsForLoop(true, true, false, fast_data_hoists), false);
	EmitSljitPerfectHashPredicateSkipLabel(compiler, predicate_skip_jumps);
	EmitNextSljitNativeVectorLoop(compiler, fast_loop);
	return fast_done;
}

void EmitSljitPerfectHashFusedUpdateLoops(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashFusedUpdatePlan &update_plan) {
	auto compiler = context.compiler;
	const auto data_hoists = update_plan.hoist_source_data_pointers ? &update_plan.source_data_hoists : nullptr;
	const auto fast_data_hoists =
	    update_plan.hoist_fast_source_data_pointers ? &update_plan.fast_source_data_hoists : data_hoists;
	const bool can_use_common_selected_group_data_base_reg =
	    update_plan.dedicated_state_register || update_plan.dense_reduction_plan.Ready();

	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *logical_fast_done = nullptr;
	struct sljit_jump *common_source_dictionary_group_fast_done = nullptr;
	struct sljit_jump *selected_source_dictionary_group_fast_done = nullptr;
	struct sljit_jump *common_selected_group_present_fast_done = nullptr;
	struct sljit_jump *common_selected_fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;

	if (update_plan.codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitPerfectHashFastSourceDataHoists(compiler, update_plan);
		fast_done = EmitSljitPerfectHashFlatFastLoop(context, update_plan, fast_data_hoists);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitPerfectHashFastSourceDataHoists(compiler, update_plan);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto use_source_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		SljitPerfectHashUpdateLoopOptions loop_options;
		loop_options.reset_index = true;
		loop_options.all_valid = true;
		loop_options.no_source_selection = true;
		loop_options.predicate_data_hoists = fast_data_hoists;
		loop_options.group_lookup = SljitPerfectHashDirectGroupLookupOptions(context, false);
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, true, fast_data_hoists);
		logical_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_source_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_common_sel));
		auto use_per_source_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
		auto use_decoded_common_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));
		loop_options = SljitPerfectHashDictionaryGroupLoopOptions(context, data_hoists, true);
		common_source_dictionary_group_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_decoded_common_group_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_selection_all_present));
		auto use_nullable_common_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));

		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.direct_logical_index = true;
		loop_options.all_valid = true;
		loop_options.predicate_data_hoists = data_hoists;
		loop_options.load_fast_group_data_array_base = can_use_common_selected_group_data_base_reg;
		loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
		loop_options.group_lookup.group_selection_all_present = true;
		loop_options.group_lookup.group_sel_array_base_reg = SLJIT_S6;
		loop_options.group_lookup.group_data_array_base_reg_override =
		    can_use_common_selected_group_data_base_reg ? SLJIT_PERFECT_HASH_STATE_REG : 0;
		loop_options.load_common_selected_source_index = true;
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, true, data_hoists);
		common_selected_group_present_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_nullable_common_selected_loop, sljit_emit_label(compiler));
		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.direct_logical_index = true;
		loop_options.all_valid = true;
		loop_options.predicate_data_hoists = fast_data_hoists;
		loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
		loop_options.load_common_selected_source_index = true;
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, true, fast_data_hoists);
		common_selected_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_per_source_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
		auto use_decoded_per_source_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));
		loop_options = SljitPerfectHashDictionaryGroupLoopOptions(context, data_hoists, false);
		selected_source_dictionary_group_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_decoded_per_source_group_loop, sljit_emit_label(compiler));
		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.all_valid = true;
		loop_options.predicate_data_hoists = fast_data_hoists;
		loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, false, fast_data_hoists);
		selected_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	SljitPerfectHashUpdateLoopOptions loop_options;
	loop_options.group_lookup.check_group_validity = true;
	loop_options.group_lookup.materialize_state_pointer = !update_plan.dense_reduction_plan.Ready();
	loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, false, false, data_hoists);
	auto done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

	auto done_label = sljit_emit_label(compiler);
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	if (logical_fast_done) {
		sljit_set_label(logical_fast_done, done_label);
	}
	if (common_source_dictionary_group_fast_done) {
		sljit_set_label(common_source_dictionary_group_fast_done, done_label);
	}
	if (selected_source_dictionary_group_fast_done) {
		sljit_set_label(selected_source_dictionary_group_fast_done, done_label);
	}
	if (common_selected_group_present_fast_done) {
		sljit_set_label(common_selected_group_present_fast_done, done_label);
	}
	if (common_selected_fast_done) {
		sljit_set_label(common_selected_fast_done, done_label);
	}
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
	}
	sljit_set_label(done, done_label);
}

} // namespace duckdb

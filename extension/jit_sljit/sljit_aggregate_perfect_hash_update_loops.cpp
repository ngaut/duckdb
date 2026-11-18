#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_perfect_hash_codegen.hpp"
#include "sljit_aggregate_perfect_hash_local_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

static sljit_s32 SljitPerfectHashGroupSelectionArrayBaseReg() {
	return GetSljitNativeVectorRegisterLayout().optional_invariant;
}

static void EmitSljitPerfectHashGroupSelectionArrayBase(struct sljit_compiler *compiler) {
	const auto group_selection_array_base = SljitPerfectHashGroupSelectionArrayBaseReg();
	if (group_selection_array_base == 0) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, group_selection_array_base, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_sel_array));
}

struct SljitPerfectHashUpdateLoopOptions {
	bool reset_index = false;
	bool direct_logical_index = false;
	bool predicate_fast_path = false;
	bool all_valid = false;
	bool no_source_selection = false;
	const vector<SljitTypedExpressionTreeLoopHoist> *predicate_data_hoists = nullptr;
	bool load_fast_group_data_array_base = false;
	bool load_fast_group_dictionary_runtime_array_base = false;
	SljitPerfectHashGroupLookupOptions group_lookup;
	bool load_common_selected_source_index = false;
	SljitPerfectHashPayloadUpdateOptions payload_update;
};

static void EmitSljitPerfectHashFastGroupDataArrayBase(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, GetSljitPerfectHashRegisterLayout().state, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_data_array));
}

static void EmitSljitPerfectHashFastGroupDictionaryRuntimeArrayBase(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, GetSljitPerfectHashRegisterLayout().state, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
}

static void EmitSljitPerfectHashRowUpdate(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashGroupLookupOptions &group_lookup,
                                          const SljitPerfectHashPayloadUpdateOptions &payload_update,
                                          bool load_common_selected_source_index) {
	auto compiler = context.compiler;
	D_ASSERT(!group_lookup.mark_states_on_first_touch || payload_update.all_valid);
	EmitSljitPerfectHashGroupLookup(context, group_lookup);
	if (load_common_selected_source_index) {
		EmitLoadSljitCommonSelectedSourceIndex(compiler);
	}
	auto updates = payload_update;
	updates.states_marked_on_first_touch = group_lookup.mark_states_on_first_touch;
	EmitSljitPerfectHashPayloadUpdates(context, updates);
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
		sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.location.operand, 0, SLJIT_MEM1(SLJIT_S5),
		               SljitPointerArrayOffset(hoist.source_index));
	}
}

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashDirectGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                         bool use_fast_group_data_array_base);
static SljitPerfectHashPayloadUpdateOptions
SljitPerfectHashPayloadUpdateOptionsForLoop(bool fast_path, bool all_valid, bool no_source_selection,
                                            const vector<SljitTypedExpressionTreeLoopHoist> *payload_data_hoists);

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashDirectGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                         bool use_fast_group_data_array_base) {
	SljitPerfectHashGroupLookupOptions result;
	result.materialize_state_pointer = !context.dense_reduction_plan.Ready();
	result.mark_states_on_first_touch = context.states_markable_on_first_touch;
	result.direct_group_index = true;
	result.use_fast_group_data_array_base = use_fast_group_data_array_base;
	return result;
}

static SljitPerfectHashGroupLookupOptions
SljitPerfectHashSelectedGroupLookupOptions(const SljitPerfectHashFusedUpdateEmitContext &context) {
	SljitPerfectHashGroupLookupOptions result;
	result.materialize_state_pointer = !context.dense_reduction_plan.Ready();
	result.mark_states_on_first_touch = context.states_markable_on_first_touch;
	return result;
}

static SljitPerfectHashPayloadUpdateOptions
SljitPerfectHashPayloadUpdateOptionsForLoop(bool fast_path, bool all_valid, bool no_source_selection,
                                            const vector<SljitTypedExpressionTreeLoopHoist> *payload_data_hoists) {
	SljitPerfectHashPayloadUpdateOptions result;
	result.fast_path = fast_path;
	result.all_valid = all_valid;
	result.no_source_selection = no_source_selection;
	result.payload_data_hoists = payload_data_hoists;
	return result;
}

static SljitPerfectHashUpdateLoopOptions
SljitPerfectHashDictionaryGroupLoopOptions(const SljitPerfectHashFusedUpdateEmitContext &context,
                                           const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                           bool payload_uses_common_selection) {
	SljitPerfectHashUpdateLoopOptions result;
	result.reset_index = true;
	result.direct_logical_index = true;
	result.all_valid = true;
	result.predicate_data_hoists = data_hoists;
	const auto &registers = GetSljitPerfectHashRegisterLayout();
	result.load_fast_group_dictionary_runtime_array_base = registers.has_dedicated_state;
	result.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
	result.group_lookup.group_selection_all_present = true;
	result.group_lookup.use_dictionary_group_contributions = true;
	result.group_lookup.group_sel_array_base_reg = SljitPerfectHashGroupSelectionArrayBaseReg();
	result.group_lookup.group_dictionary_runtime_array_base_reg = registers.has_dedicated_state ? registers.state : 0;
	result.load_common_selected_source_index = payload_uses_common_selection;
	result.payload_update =
	    SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, payload_uses_common_selection, data_hoists);
	return result;
}

// The packed-predicate hybrid loop emits this body once per matching lane: load the
// selected execute index, re-establish the hoisted group array base the packed loop
// may have clobbered, and update the row through the fast expression path.
static void EmitSljitPerfectHashSimdLaneRowUpdate(const SljitPerfectHashFusedUpdateEmitContext &context,
                                                  SljitPerfectHashGroupLookupOptions lookup,
                                                  const SljitPerfectHashPayloadUpdateOptions &payload_update,
                                                  const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                  void (*emit_hoisted_array_base)(struct sljit_compiler *)) {
	EmitLoadFusedAggregateExecuteIndex(context.compiler, true);
	if (emit_hoisted_array_base) {
		emit_hoisted_array_base(context.compiler);
	}
	lookup.expression_fast_path = true;
	lookup.expression_all_valid = true;
	lookup.expression_data_hoists = data_hoists;
	EmitSljitPerfectHashRowUpdate(context, lookup, payload_update, false);
}

static sljit_jump *EmitSljitPerfectHashFlatFastLoop(const SljitPerfectHashFusedUpdateEmitContext &context,
                                                    const SljitPerfectHashFusedUpdatePlan &update_plan,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *fast_data_hoists) {
	auto compiler = context.compiler;
	if (update_plan.predicate_simd_plan.supported) {
		EmitSljitTypedExpressionTreeSimdHybridFilterLoop(
		    compiler, *context.predicate, update_plan.predicate_simd_plan, update_plan.predicate_simd_mask_offset,
		    [&]() {
			    EmitSljitPerfectHashSimdLaneRowUpdate(
			        context,
			        SljitPerfectHashDirectGroupLookupOptions(context, update_plan.hoist_fast_group_data_array_base),
			        SljitPerfectHashPayloadUpdateOptionsForLoop(true, true, false, fast_data_hoists), fast_data_hoists,
			        update_plan.hoist_fast_group_data_array_base ? EmitSljitPerfectHashFastGroupDataArrayBase
			                                                     : nullptr);
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

// Flat rows with valid group keys whose payload sources may be NULL. Group and state flags are set per row: a state
// whose payloads were all NULL keeps its flag unset.
static vector<sljit_jump *>
EmitSljitPerfectHashNullCheckedLoop(const SljitPerfectHashFusedUpdateEmitContext &context,
                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	auto compiler = context.compiler;
	const auto &registers = GetSljitNativeVectorRegisterLayout();
	if (registers.HasOptionalInvariant()) {
		// the source validity array: the fast loops may keep a source data pointer in this register
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.optional_invariant, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_validity_array));
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	vector<sljit_jump *> exits {sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0)};
	auto loop = sljit_emit_label(compiler);
	auto emit_latch = [&]() {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), loop);
		exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	};
	EmitLoadFusedAggregateExecuteIndex(compiler, true);
	vector<sljit_jump *> predicate_false;
	if (context.predicate) {
		if (SljitTypedExpressionTreeEvaluatesAnyBits(*context.predicate)) {
			// Forcing a NULL leaf FALSE only lowers an AND/OR predicate, so a row the predicate rejects on the
			// leftover values of its NULLs is rejected: it falls through to the back-edge, and only a row it accepts
			// tests its NULLs, out of line
			idx_t raw_spill_index = 0;
			auto raw_true = EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, *context.predicate, raw_spill_index,
			                                                            context.overflows, data_hoists);
			emit_latch();
			SetSljitJumpLabels(raw_true, sljit_emit_label(compiler));
		}
		idx_t spill_index = 0;
		predicate_false = EmitSljitTypedExpressionTreeNullableFastJumps(
		    compiler, *context.predicate, false, spill_index, context.overflows,
		    SljitTypedExpressionTreeFastIndexMode::FLAT, data_hoists);
	}
	auto lookup = SljitPerfectHashDirectGroupLookupOptions(context, false);
	lookup.mark_states_on_first_touch = false;
	auto payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(true, false, false, data_hoists);
	payload_update.null_rows = true;
	EmitSljitPerfectHashRowUpdate(context, lookup, payload_update, false);
	EmitSljitPerfectHashPredicateSkipLabel(compiler, predicate_false);
	emit_latch();
	return exits;
}

//! The dictionary loops run only when every group key caches dictionary contributions; other plans omit them
static bool SljitPerfectHashGroupsCacheDictionaryContributions(const vector<SljitPerfectHashGroupPlan> &groups) {
	for (auto &group : groups) {
		if (!SljitPerfectHashGroupKeyCachesDictionaryContributions(group.expression_kind,
		                                                           group.string_compress_target_size)) {
			return false;
		}
	}
	return !groups.empty();
}

static sljit_jump *
EmitSljitPerfectHashFlatPayloadDictionaryGroupLoop(const SljitPerfectHashFusedUpdateEmitContext &context,
                                                   const SljitPerfectHashFusedUpdatePlan &update_plan,
                                                   const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	auto compiler = context.compiler;
	EmitSljitPerfectHashGroupSelectionArrayBase(compiler);
	auto loop_options = SljitPerfectHashDictionaryGroupLoopOptions(context, data_hoists, false);
	loop_options.reset_index = false;
	loop_options.predicate_fast_path = true;
	loop_options.predicate_data_hoists = data_hoists;
	loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(true, true, false, data_hoists);
	if (update_plan.predicate_simd_plan.supported) {
		EmitSljitTypedExpressionTreeSimdHybridFilterLoop(
		    compiler, *context.predicate, update_plan.predicate_simd_plan, update_plan.predicate_simd_mask_offset,
		    [&]() {
			    EmitSljitPerfectHashSimdLaneRowUpdate(context, loop_options.group_lookup, loop_options.payload_update,
			                                          data_hoists,
			                                          loop_options.load_fast_group_dictionary_runtime_array_base
			                                              ? EmitSljitPerfectHashFastGroupDictionaryRuntimeArrayBase
			                                              : nullptr);
		    });
	}
	return EmitSljitPerfectHashUpdateLoop(context, loop_options);
}

static sljit_jump *EmitSljitPerfectHashFlatPayloadSelectedGroupLoop(
    const SljitPerfectHashFusedUpdateEmitContext &context, const SljitPerfectHashFusedUpdatePlan &update_plan,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists, bool use_group_data_array_base_reg) {
	auto compiler = context.compiler;
	EmitSljitPerfectHashGroupSelectionArrayBase(compiler);
	SljitPerfectHashUpdateLoopOptions loop_options;
	loop_options.direct_logical_index = true;
	loop_options.predicate_fast_path = true;
	loop_options.all_valid = true;
	loop_options.predicate_data_hoists = data_hoists;
	loop_options.load_fast_group_data_array_base = use_group_data_array_base_reg;
	loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
	loop_options.group_lookup.group_selection_all_present = true;
	loop_options.group_lookup.group_sel_array_base_reg = SljitPerfectHashGroupSelectionArrayBaseReg();
	loop_options.group_lookup.group_data_array_base_reg_override =
	    use_group_data_array_base_reg ? GetSljitPerfectHashRegisterLayout().state : 0;
	loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(true, true, false, data_hoists);
	if (update_plan.predicate_simd_plan.supported) {
		EmitSljitTypedExpressionTreeSimdHybridFilterLoop(
		    compiler, *context.predicate, update_plan.predicate_simd_plan, update_plan.predicate_simd_mask_offset,
		    [&]() {
			    EmitSljitPerfectHashSimdLaneRowUpdate(
			        context, loop_options.group_lookup, loop_options.payload_update, data_hoists,
			        loop_options.load_fast_group_data_array_base ? EmitSljitPerfectHashFastGroupDataArrayBase
			                                                     : nullptr);
		    });
	}
	return EmitSljitPerfectHashUpdateLoop(context, loop_options);
}

void EmitSljitPerfectHashFusedUpdateLoops(const SljitPerfectHashFusedUpdateEmitContext &context,
                                          const SljitPerfectHashFusedUpdatePlan &update_plan) {
	auto compiler = context.compiler;
	const auto data_hoists = update_plan.hoist_source_data_pointers ? &update_plan.source_data_hoists : nullptr;
	const auto fast_data_hoists =
	    update_plan.hoist_fast_source_data_pointers ? &update_plan.fast_source_data_hoists : data_hoists;
	const bool can_use_common_selected_group_data_base_reg =
	    update_plan.dedicated_state_register || update_plan.dedicated_reduction_state_register;

	struct sljit_jump *fast_done = nullptr;
	const bool dictionary_groups = SljitPerfectHashGroupsCacheDictionaryContributions(context.group_plans);
	struct sljit_jump *flat_payload_dictionary_group_done = nullptr;
	struct sljit_jump *flat_payload_selected_group_done = nullptr;
	struct sljit_jump *logical_fast_done = nullptr;
	struct sljit_jump *common_source_dictionary_group_fast_done = nullptr;
	struct sljit_jump *selected_source_dictionary_group_fast_done = nullptr;
	struct sljit_jump *common_selected_group_present_fast_done = nullptr;
	struct sljit_jump *common_selected_fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	vector<sljit_jump *> null_checked_exits;
	struct sljit_jump *flat_nullable_done = nullptr;
	struct sljit_jump *use_nullable_group_loop = nullptr;
	struct sljit_jump *use_partially_selected_group_loop = nullptr;

	if (update_plan.codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_group_flat_all_valid));
		auto use_selected_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitPerfectHashFastSourceDataHoists(compiler, update_plan);
		fast_done = EmitSljitPerfectHashFlatFastLoop(context, update_plan, fast_data_hoists);

		sljit_set_label(use_selected_group_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_group_all_valid));
		use_nullable_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		if (dictionary_groups) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
			auto use_uncached_selected_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			flat_payload_dictionary_group_done =
			    EmitSljitPerfectHashFlatPayloadDictionaryGroupLoop(context, update_plan, data_hoists);
			sljit_set_label(use_uncached_selected_group_loop, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_selection_all_present));
		use_partially_selected_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		flat_payload_selected_group_done = EmitSljitPerfectHashFlatPayloadSelectedGroupLoop(
		    context, update_plan, data_hoists, can_use_common_selected_group_data_base_reg);

		auto generic_loop_label = sljit_emit_label(compiler);
		sljit_set_label(use_generic_loop, generic_loop_label);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_inputs_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitPerfectHashFastSourceDataHoists(compiler, update_plan);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_inputs_flat_no_selection));
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
		if (dictionary_groups) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
			auto use_decoded_common_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
			EmitSljitPerfectHashGroupSelectionArrayBase(compiler);
			loop_options = SljitPerfectHashDictionaryGroupLoopOptions(context, data_hoists, true);
			common_source_dictionary_group_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);
			sljit_set_label(use_decoded_common_group_loop, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_selection_all_present));
		auto use_nullable_common_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitPerfectHashGroupSelectionArrayBase(compiler);

		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.direct_logical_index = true;
		loop_options.all_valid = true;
		loop_options.predicate_data_hoists = data_hoists;
		loop_options.load_fast_group_data_array_base = can_use_common_selected_group_data_base_reg;
		loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
		loop_options.group_lookup.group_selection_all_present = true;
		loop_options.group_lookup.group_sel_array_base_reg = SljitPerfectHashGroupSelectionArrayBaseReg();
		loop_options.group_lookup.group_data_array_base_reg_override =
		    can_use_common_selected_group_data_base_reg ? GetSljitPerfectHashRegisterLayout().state : 0;
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
		if (dictionary_groups) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
			auto use_decoded_per_source_group_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			EmitSljitPerfectHashGroupSelectionArrayBase(compiler);
			loop_options = SljitPerfectHashDictionaryGroupLoopOptions(context, data_hoists, false);
			selected_source_dictionary_group_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);
			sljit_set_label(use_decoded_per_source_group_loop, sljit_emit_label(compiler));
		}
		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.all_valid = true;
		loop_options.predicate_data_hoists = fast_data_hoists;
		loop_options.group_lookup = SljitPerfectHashSelectedGroupLookupOptions(context);
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, true, false, fast_data_hoists);
		selected_fast_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);

		auto generic_selected_loop_label = sljit_emit_label(compiler);
		sljit_set_label(use_generic_selected_loop, generic_selected_loop_label);
		sljit_set_label(use_nullable_group_loop, generic_selected_loop_label);
		sljit_set_label(use_partially_selected_group_loop, generic_selected_loop_label);

		// Flat inputs with nullable group keys or payloads: every input is read at the row's position, and validity
		// is checked as in the generic loop. Group flags are set per row: a group whose payloads were all NULL keeps
		// its states unset.
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_inputs_flat_no_selection));
		auto inputs_selected = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		if (update_plan.null_checked_loop) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_group_all_valid));
			auto groups_nullable = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			null_checked_exits = EmitSljitPerfectHashNullCheckedLoop(context, data_hoists);
			sljit_set_label(groups_nullable, sljit_emit_label(compiler));
		}
		loop_options = SljitPerfectHashUpdateLoopOptions();
		loop_options.reset_index = true;
		loop_options.direct_logical_index = true;
		loop_options.no_source_selection = true;
		loop_options.predicate_data_hoists = data_hoists;
		loop_options.group_lookup.check_group_validity = true;
		loop_options.group_lookup.materialize_state_pointer = !update_plan.dense_reduction_plan.Ready();
		loop_options.group_lookup.direct_group_index = true;
		loop_options.payload_update = SljitPerfectHashPayloadUpdateOptionsForLoop(false, false, true, data_hoists);
		flat_nullable_done = EmitSljitPerfectHashUpdateLoop(context, loop_options);
		auto generic_label = sljit_emit_label(compiler);
		sljit_set_label(inputs_selected, generic_label);
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
	if (flat_payload_dictionary_group_done) {
		sljit_set_label(flat_payload_dictionary_group_done, done_label);
	}
	if (flat_payload_selected_group_done) {
		sljit_set_label(flat_payload_selected_group_done, done_label);
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
	SetSljitJumpLabels(null_checked_exits, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
}

} // namespace duckdb

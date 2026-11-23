#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

namespace duckdb {

//! Leaves the expression value in R2. Returns whether the value can be NULL: R3 then holds its validity, and the
//! caller branches on it once the registers the evaluation borrows (S4 holds the source selections) are restored.
//! Evaluates the expression into R2 and returns whether the slot path left its validity in R3. On a fast loop a
//! payload whose CASE results yield NULL for some rows takes one of null_exits on them (given null_exits)
static bool EmitSljitPerfectHashExpressionTreeValue(const SljitPerfectHashFusedUpdateEmitContext &context,
                                                    const ExecutionExpressionIR &expression, bool fast_path,
                                                    bool all_valid, bool no_source_selection,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                    vector<sljit_jump *> *null_exits = nullptr) {
	auto compiler = context.compiler;
	if (fast_path) {
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, expression, spill_index, context.overflows, data_hoists,
		                                         null_exits);
		return false;
	}
	if (all_valid && no_source_selection) {
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, expression, spill_index, context.overflows,
		                                                data_hoists, null_exits);
		return false;
	}
	if (all_valid) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_sel_array));
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, expression, spill_index, context.overflows,
		                                                 data_hoists, null_exits);
		return false;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	idx_t slot_index = 0;
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, expression, slot_index, context.overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	return true;
}

static sljit_jump *EmitSljitPerfectHashJumpIfExpressionNull(struct sljit_compiler *compiler, bool may_be_null) {
	return may_be_null ? sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0) : nullptr;
}

vector<sljit_jump *>
EmitSljitPerfectHashPredicateSkipJumps(const SljitPerfectHashFusedUpdateEmitContext &context, bool fast_path,
                                       bool all_valid, bool no_source_selection,
                                       const vector<SljitTypedExpressionTreeLoopHoist> *predicate_data_hoists) {
	vector<sljit_jump *> result;
	if (!context.predicate) {
		return result;
	}
	const auto predicate_may_be_null = EmitSljitPerfectHashExpressionTreeValue(
	    context, *context.predicate, fast_path, all_valid, no_source_selection, predicate_data_hoists);
	auto predicate_invalid = EmitSljitPerfectHashJumpIfExpressionNull(context.compiler, predicate_may_be_null);
	if (predicate_invalid) {
		result.push_back(predicate_invalid);
	}
	result.push_back(sljit_emit_cmp(context.compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
	return result;
}

void EmitSljitPerfectHashPredicateSkipLabel(struct sljit_compiler *compiler,
                                            const vector<sljit_jump *> &predicate_skip_jumps) {
	if (predicate_skip_jumps.empty()) {
		return;
	}
	auto predicate_skip_label = sljit_emit_label(compiler);
	for (auto jump : predicate_skip_jumps) {
		sljit_set_label(jump, predicate_skip_label);
	}
}

static void EmitSljitPerfectHashPayloadStatePointer(const SljitPerfectHashFusedUpdateEmitContext &context) {
	if (context.dedicated_state_register) {
		return;
	}
	sljit_emit_op1(context.compiler, SLJIT_MOV_P, context.state_pointer_reg, 0, SLJIT_MEM1(SLJIT_SP),
	               context.state_pointer_offset);
}

static void EmitSljitPerfectHashPayloadInvalidContinuation(struct sljit_compiler *compiler,
                                                           const vector<sljit_jump *> &payload_invalid) {
	if (payload_invalid.empty()) {
		return;
	}
	auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	SetSljitJumpLabels(payload_invalid, sljit_emit_label(compiler));
	sljit_set_label(payload_done, sljit_emit_label(compiler));
}

static void EmitSljitPerfectHashNormalizeGroupContribution(struct sljit_compiler *compiler,
                                                           const SljitPerfectHashGroupPlan &group,
                                                           bool fuse_nonempty_string_compress_bias) {
	const auto group_offset = (fuse_nonempty_string_compress_bias ? 2 : 1) - group.minimum;
	if (group_offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(group_offset));
	}
	if (group.shift != 0) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(group.shift));
	}
}

static void EmitSljitPerfectHashLoadDictionaryContributionMap(struct sljit_compiler *compiler, idx_t group_idx,
                                                              sljit_s32 dictionary_runtime_array_base_reg) {
	const auto runtime_offset = NumericCast<sljit_sw>(group_idx * sizeof(SljitPerfectHashDictionaryGroupRuntime));
	if (dictionary_runtime_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(dictionary_runtime_array_base_reg),
		               runtime_offset + offsetof(SljitPerfectHashDictionaryGroupRuntime, contributions));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
	               runtime_offset + offsetof(SljitPerfectHashDictionaryGroupRuntime, contributions));
}

static void EmitSljitPerfectHashRecordDictionaryContribution(struct sljit_compiler *compiler, idx_t group_idx,
                                                             sljit_s32 dictionary_runtime_array_base_reg) {
	EmitSljitPerfectHashLoadDictionaryContributionMap(compiler, group_idx, dictionary_runtime_array_base_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3, SLJIT_R2, 0);

	const auto runtime_offset = NumericCast<sljit_sw>(group_idx * sizeof(SljitPerfectHashDictionaryGroupRuntime));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
	               runtime_offset + offsetof(SljitPerfectHashDictionaryGroupRuntime, active_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_dictionary_groups));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R4),
	               runtime_offset + offsetof(SljitPerfectHashDictionaryGroupRuntime, active_indices));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_R3), 2, SLJIT_R1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

void EmitSljitPerfectHashGroupLookup(const SljitPerfectHashFusedUpdateEmitContext &context,
                                     const SljitPerfectHashGroupLookupOptions &options) {
	auto compiler = context.compiler;
	auto &group_plans = context.group_plans;
	auto &dense_reduction_plan = context.dense_reduction_plan;
	bool group_index_initialized = false;
	const auto group_index_mode =
	    options.direct_group_index
	        ? SljitFusedAggregateGroupIndexMode::LOGICAL
	        : (options.group_selection_all_present ? SljitFusedAggregateGroupIndexMode::SELECTED_PRESENT
	                                               : SljitFusedAggregateGroupIndexMode::SELECTED_NULLABLE);
	const bool use_precomputed_string_offset = options.direct_group_index && !options.check_group_validity &&
	                                           SljitCanPrecomputePerfectHashStringGroupOffset(group_plans);
	if (use_precomputed_string_offset) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, SLJIT_STRING_T_SHIFT);
	}
	if (options.check_group_validity) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
		group_index_initialized = true;
	}
	for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
		auto &group = group_plans[group_idx];
		// A nonempty one-byte string compresses to `prefix + 1`. Combine that
		// unit bias with the later perfect-hash minimum adjustment, but retain
		// the general lowering when string prefixes are disabled.
		const bool fuse_nonempty_string_compress_bias =
		    group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS && group.minimum != 0 &&
		    string_t::PREFIX_LENGTH > 0;
		sljit_jump *group_is_null = nullptr;
		if (options.use_dictionary_group_contributions) {
			D_ASSERT(options.group_selection_all_present);
			D_ASSERT(group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS);
			EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1, group_index_mode,
			                                       options.group_sel_array_base_reg);
			EmitSljitPerfectHashLoadDictionaryContributionMap(compiler, group_idx,
			                                                  options.group_dictionary_runtime_array_base_reg);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3);
			auto contribution_ready = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, -1);
			const auto group_data_reg =
			    context.hoist_group_data_pointers ? SljitPerfectHashGroupDataPointerReg(group_idx) : SLJIT_R0;
			EmitLoadFusedAggregateGroupData(compiler, group_idx, group, SLJIT_R1, SLJIT_R2,
			                                context.hoist_group_data_pointers, group_data_reg, false, 0,
			                                fuse_nonempty_string_compress_bias);
			EmitSljitPerfectHashNormalizeGroupContribution(compiler, group, fuse_nonempty_string_compress_bias);
			EmitSljitPerfectHashRecordDictionaryContribution(compiler, group_idx,
			                                                 options.group_dictionary_runtime_array_base_reg);
			sljit_set_label(contribution_ready, sljit_emit_label(compiler));
		} else if (group.expression_kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			if (!group.expression_tree) {
				throw InternalException("SLJIT perfect-hash typed group expression is missing IR");
			}
			if (group_index_initialized) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), context.group_index_offset, SLJIT_S4, 0);
			}
			const auto group_may_be_null = EmitSljitPerfectHashExpressionTreeValue(
			    context, *group.expression_tree, options.expression_fast_path, options.expression_all_valid,
			    options.expression_no_source_selection, options.expression_data_hoists);
			if (group_index_initialized) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
			}
			group_is_null = EmitSljitPerfectHashJumpIfExpressionNull(compiler, group_may_be_null);
		} else {
			EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1, group_index_mode,
			                                       options.group_sel_array_base_reg);
			if (options.check_group_validity) {
				group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
			}
			const auto group_data_reg =
			    context.hoist_group_data_pointers ? SljitPerfectHashGroupDataPointerReg(group_idx) : SLJIT_R0;
			const auto group_data_array_base_reg = options.use_fast_group_data_array_base
			                                           ? GetSljitPerfectHashRegisterLayout().state
			                                           : options.group_data_array_base_reg_override;
			EmitLoadFusedAggregateGroupData(
			    compiler, group_idx, group, SLJIT_R1, SLJIT_R2, context.hoist_group_data_pointers, group_data_reg,
			    use_precomputed_string_offset, group_data_array_base_reg, fuse_nonempty_string_compress_bias);
		}
		if (!options.use_dictionary_group_contributions) {
			EmitSljitPerfectHashNormalizeGroupContribution(compiler, group, fuse_nonempty_string_compress_bias);
		}
		if (group_index_initialized) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_R2, 0);
			group_index_initialized = true;
		}
		if (group_is_null) {
			auto group_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto group_null_label = sljit_emit_label(compiler);
			sljit_set_label(group_is_null, group_null_label);
			sljit_set_label(group_done, sljit_emit_label(compiler));
		}
	}
	context.group_out_of_range.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S4, 0, SLJIT_IMM,
	                                                    NumericCast<sljit_sw>(context.perfect_hash_group_count)));
	if (!options.materialize_state_pointer) {
		EmitMarkSljitDensePerfectHashGroupSeen(compiler, dense_reduction_plan, SLJIT_S4);
		EmitSljitPerfectHashReductionStatePointer(compiler, dense_reduction_plan, SLJIT_S4,
		                                          context.reduction_state_reg);
		if (context.reduction_state_reg == SLJIT_S4) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), context.state_pointer_offset,
			               context.reduction_state_reg, 0);
		}
		return;
	}
	if (options.mark_states_on_first_touch) {
		EmitSljitPerfectHashJumpIfFirstTouch(compiler, SLJIT_S4, context.first_touch_sites);
	} else {
		EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S4);
	}
	const auto computed_state_reg = context.dedicated_state_register ? context.state_pointer_reg : SLJIT_S4;
	EmitSljitPerfectHashStatePointer(compiler, SLJIT_S4, computed_state_reg);
	if (!context.dedicated_state_register) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), context.state_pointer_offset, SLJIT_S4, 0);
	}
}

void EmitSljitPerfectHashPayloadUpdates(const SljitPerfectHashFusedUpdateEmitContext &context,
                                        const SljitPerfectHashPayloadUpdateOptions &options) {
	auto compiler = context.compiler;
	auto &payloads = context.payloads;
	auto &contract = context.contract;
	auto &codegen_plan = context.codegen_plan;
	auto &descriptors = codegen_plan.payload_descriptors;
	auto &dense_reduction_plan = context.dense_reduction_plan;
	const bool use_reduction = dense_reduction_plan.Ready();
	// Direct perfect-hash lookup leaves S4 dead after producing the state in S7.
	// Flat and logical typed expressions never use saved registers, so keep the
	// shared binary intermediate in S4 for those paths. Selected expressions own
	// S4 for their selection array and retain the stack spill.
	const bool use_binary_shared_value_register = codegen_plan.shared_binary.Enabled() && options.all_valid &&
	                                              context.dedicated_state_register &&
	                                              (options.fast_path || options.no_source_selection);
	const auto binary_shared_value_reg = use_binary_shared_value_register ? SLJIT_S4 : 0;
	const auto shared_index_mode = options.fast_path
	                                   ? SljitAggregateExpressionIndexMode::FLAT
	                                   : (options.no_source_selection ? SljitAggregateExpressionIndexMode::LOGICAL
	                                                                  : SljitAggregateExpressionIndexMode::SELECTED);
	const bool selected_shared_payload = shared_index_mode == SljitAggregateExpressionIndexMode::SELECTED;
	const bool state_clobbers_selected_source_array =
	    use_reduction ? context.reduction_state_reg == SLJIT_S4 : !context.dedicated_state_register;
	if (codegen_plan.shared_binary.Enabled() && options.all_valid) {
		EmitSljitSharedBinaryPayloadBase(compiler, codegen_plan.shared_binary, binary_shared_value_reg,
		                                 context.binary_shared_value_offset, shared_index_mode, context.overflows,
		                                 options.payload_data_hoists, selected_shared_payload);
	}
	// Rows with NULLs: consecutive payloads that read the same sources skip their updates on one test of them
	vector<idx_t> null_checked_sources;
	vector<sljit_jump *> source_null_jumps;
	auto end_null_checked_run = [&]() {
		SetSljitJumpLabels(source_null_jumps, sljit_emit_label(compiler));
		source_null_jumps.clear();
		null_checked_sources.clear();
	};
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = descriptors[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		if (options.null_rows) {
			vector<idx_t> sources;
			if (descriptor.has_payload) {
				if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
					sources.push_back(payloads[payload_idx].source_index);
				} else {
					D_ASSERT(SljitTypedExpressionTreeIsNullStrict(*payloads[payload_idx].expression_tree));
					CollectSljitTypedExpressionTreeReferences(*payloads[payload_idx].expression_tree, sources);
				}
			}
			if (sources != null_checked_sources || sources.empty()) {
				end_null_checked_run();
				for (auto source_index : sources) {
					source_null_jumps.push_back(EmitJumpIfSljitTypedExpressionTreeFastSourceNull(
					    compiler, source_index, SljitTypedExpressionTreeFastIndexMode::FLAT,
					    options.payload_data_hoists));
				}
				null_checked_sources = std::move(sources);
			}
		}
		// the payload's value in R2 (COUNT drops it), or a jump to skip this aggregate's update on a NULL row
		vector<sljit_jump *> payload_invalid;
		if (!descriptor.has_payload) {
			// COUNT(*)
		} else if (codegen_plan.shared_binary.Enabled() && options.all_valid &&
		           codegen_plan.shared_binary.lanes[payload_idx].matched) {
			EmitSljitSharedBinaryPayloadLane(compiler, codegen_plan.shared_binary.lanes[payload_idx],
			                                 binary_shared_value_reg, context.binary_shared_value_offset,
			                                 shared_index_mode, context.overflows, options.payload_data_hoists,
			                                 selected_shared_payload && state_clobbers_selected_source_array);
		} else if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			const bool selected = !options.fast_path && !options.no_source_selection;
			const bool check_validity = !options.all_valid && !options.null_rows;
			if (descriptor.ReadsValue()) {
				auto reference_invalid = EmitLoadFusedAggregateReferenceValue(
				    compiler, payloads[payload_idx], selected, check_validity, SLJIT_S3, options.payload_data_hoists);
				if (reference_invalid) {
					payload_invalid.push_back(reference_invalid);
				}
			} else if (check_validity) {
				payload_invalid.push_back(
				    EmitJumpIfFusedAggregateReferenceNull(compiler, payloads[payload_idx], selected, SLJIT_S3));
			}
		} else {
			const auto payload_may_be_null = EmitSljitPerfectHashExpressionTreeValue(
			    context, *payloads[payload_idx].expression_tree, options.fast_path, options.all_valid,
			    options.no_source_selection, options.payload_data_hoists, &payload_invalid);
			if (payload_may_be_null) {
				payload_invalid.push_back(EmitSljitPerfectHashJumpIfExpressionNull(compiler, true));
			}
		}
		if (use_reduction) {
			if (context.reduction_state_reg == SLJIT_S4) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.state_pointer_offset);
			}
			if (descriptor.ReadsValue()) {
				EmitSljitDensePerfectHashAccumulate(compiler, dense_reduction_plan.lanes[payload_idx],
				                                    descriptor.primitive_kind, context.reduction_state_reg, SLJIT_R2);
			} else {
				EmitSljitDensePerfectHashIncrementCount(compiler, dense_reduction_plan.lanes[payload_idx],
				                                        context.reduction_state_reg);
			}
		} else {
			EmitSljitPerfectHashPayloadStatePointer(context);
			if (descriptor.ReadsValue()) {
				EmitSljitGroupedAggregateAccumulateSumImmediate(
				    compiler, descriptor.primitive_kind, context.state_pointer_reg, state_offset,
				    descriptor.state_value_offset, descriptor.state_is_set_offset, SLJIT_R2,
				    options.states_marked_on_first_touch);
			} else {
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, context.state_pointer_reg, state_offset,
				                                                 descriptor.state_value_offset);
			}
		}
		EmitSljitPerfectHashPayloadInvalidContinuation(compiler, payload_invalid);
	}
	end_null_checked_run();
}

void EmitLoadSljitCommonSelectedSourceIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_common_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
}

} // namespace duckdb

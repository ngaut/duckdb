#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

namespace duckdb {

static sljit_jump *EmitSljitPerfectHashExpressionTreeValue(
    const SljitPerfectHashFusedUpdateEmitContext &context, const ExecutionExpressionIR &expression, bool fast_path,
    bool all_valid, bool no_source_selection, const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	auto compiler = context.compiler;
	if (fast_path) {
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, expression, spill_index, context.overflows, data_hoists);
		return nullptr;
	}
	if (all_valid && no_source_selection) {
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, expression, spill_index, context.overflows,
		                                                data_hoists);
		return nullptr;
	}
	if (all_valid) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_sel_array));
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, expression, spill_index, context.overflows,
		                                                 data_hoists);
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	idx_t slot_index = 0;
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, expression, slot_index, context.overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	auto invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	return invalid;
}

vector<sljit_jump *>
EmitSljitPerfectHashPredicateSkipJumps(const SljitPerfectHashFusedUpdateEmitContext &context, bool fast_path,
                                       bool all_valid, bool no_source_selection,
                                       const vector<SljitTypedExpressionTreeDataPointerHoist> *predicate_data_hoists) {
	vector<sljit_jump *> result;
	if (!context.predicate) {
		return result;
	}
	auto predicate_invalid = EmitSljitPerfectHashExpressionTreeValue(context, *context.predicate, fast_path, all_valid,
	                                                                 no_source_selection, predicate_data_hoists);
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
                                                           sljit_jump *payload_invalid) {
	if (!payload_invalid) {
		return;
	}
	auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(payload_invalid, sljit_emit_label(compiler));
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
	auto &deferred_flag_plan = context.deferred_flag_plan;
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
			group_is_null = EmitSljitPerfectHashExpressionTreeValue(
			    context, *group.expression_tree, options.expression_fast_path, options.expression_all_valid,
			    options.expression_no_source_selection, options.expression_data_hoists);
			if (group_index_initialized) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
			}
		} else {
			EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1, group_index_mode,
			                                       options.group_sel_array_base_reg);
			if (options.check_group_validity) {
				group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
			}
			const auto group_data_reg =
			    context.hoist_group_data_pointers ? SljitPerfectHashGroupDataPointerReg(group_idx) : SLJIT_R0;
			const auto group_data_array_base_reg = options.use_fast_group_data_array_base
			                                           ? SLJIT_PERFECT_HASH_STATE_REG
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
		if (context.group_index_reg == SLJIT_S4) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), context.group_index_offset, SLJIT_S4, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, context.group_index_reg, 0, SLJIT_S4, 0);
		}
		EmitMarkSljitDensePerfectHashGroupSeen(compiler, dense_reduction_plan, context.group_index_reg);
		return;
	}
	if (options.defer_flags) {
		EmitMarkSljitDeferredPerfectHashGroupSeen(compiler, deferred_flag_plan, SLJIT_S4);
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
	auto &deferred_flag_plan = context.deferred_flag_plan;
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
	if (codegen_plan.shared_binary.Enabled() && options.all_valid) {
		// Group lookup borrows S4 to assemble the perfect-hash index. The selected
		// expression emitter owns S4 as the source-selection-array base, so restore
		// that loop invariant before entering the shared payload contract.
		if (shared_index_mode == SljitAggregateExpressionIndexMode::SELECTED) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, source_sel_array));
		}
		EmitSljitSharedBinaryPayloadBase(compiler, codegen_plan.shared_binary, binary_shared_value_reg,
		                                 context.binary_shared_value_offset, shared_index_mode, context.overflows,
		                                 options.payload_data_hoists);
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = descriptors[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (dense_reduction_plan.Ready()) {
				if (context.group_index_reg == SLJIT_S4) {
					sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
				}
				EmitSljitDensePerfectHashIncrementCount(compiler, dense_reduction_plan.lanes[payload_idx],
				                                        context.group_index_reg);
				continue;
			}
			EmitSljitPerfectHashPayloadStatePointer(context);
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(compiler, context.state_pointer_reg,
				                                                       state_offset, descriptor.state_value_offset);
			} else {
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, context.state_pointer_reg, state_offset,
				                                                 descriptor.state_value_offset);
			}
			continue;
		}

		sljit_jump *payload_invalid = nullptr;
		if (codegen_plan.shared_binary.Enabled() && options.all_valid &&
		    codegen_plan.shared_binary.lanes[payload_idx].matched) {
			EmitSljitSharedBinaryPayloadLane(compiler, codegen_plan.shared_binary.lanes[payload_idx],
			                                 binary_shared_value_reg, context.binary_shared_value_offset,
			                                 shared_index_mode, context.overflows, options.payload_data_hoists);
		} else if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload_invalid = EmitLoadFusedTypedAggregateReferenceValue(
			    compiler, payloads[payload_idx], !options.fast_path && !options.no_source_selection, !options.all_valid,
			    SLJIT_S3, options.payload_data_hoists);
		} else {
			payload_invalid = EmitSljitPerfectHashExpressionTreeValue(
			    context, *payloads[payload_idx].expression_tree, options.fast_path, options.all_valid,
			    options.no_source_selection, options.payload_data_hoists);
		}
		if (dense_reduction_plan.Ready()) {
			if (context.group_index_reg == SLJIT_S4) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
			}
			EmitSljitDensePerfectHashAccumulate(compiler, dense_reduction_plan.lanes[payload_idx],
			                                    descriptor.primitive_kind, context.group_index_reg, SLJIT_R2);
			EmitSljitPerfectHashPayloadInvalidContinuation(compiler, payload_invalid);
			continue;
		}

		EmitSljitPerfectHashPayloadStatePointer(context);
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(
				    compiler, context.state_pointer_reg, state_offset, descriptor.state_value_offset, SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, context.state_pointer_reg, state_offset,
				                                                  descriptor.state_value_offset,
				                                                  descriptor.state_is_set_offset, SLJIT_R2);
			}
		} else {
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(
				    compiler, context.state_pointer_reg, state_offset, descriptor.state_value_offset, SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(compiler, context.state_pointer_reg, state_offset,
				                                                    descriptor.state_value_offset,
				                                                    descriptor.state_is_set_offset, SLJIT_R2);
			}
		}
		EmitSljitPerfectHashPayloadInvalidContinuation(compiler, payload_invalid);
	}
}

void EmitLoadSljitCommonSelectedSourceIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_common_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
}

} // namespace duckdb

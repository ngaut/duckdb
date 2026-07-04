#include "sljit_aggregate_perfect_hash_update_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/types/cast_helpers.hpp"

#include "sljitLir.h"

namespace duckdb {

static sljit_jump *
EmitSljitPerfectHashExpressionTreeValue(const SljitPerfectHashFusedUpdateEmitContext &context,
                                        const ExecutionExpressionIR &expression, bool fast_path, bool all_valid,
                                        bool no_source_selection,
                                        const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
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
	auto predicate_invalid =
	    EmitSljitPerfectHashExpressionTreeValue(context, *context.predicate, fast_path, all_valid, no_source_selection,
	                                            predicate_data_hoists);
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

void EmitSljitPerfectHashGroupLookup(const SljitPerfectHashFusedUpdateEmitContext &context,
                                     const SljitPerfectHashGroupLookupOptions &options) {
	auto compiler = context.compiler;
	auto &group_plans = context.group_plans;
	auto &local_aggregate_plan = context.local_aggregate_plan;
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
		sljit_jump *group_is_null = nullptr;
		if (group.expression_kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
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
			EmitLoadFusedAggregateGroupData(compiler, group_idx, group, SLJIT_R1, SLJIT_R2,
			                                context.hoist_group_data_pointers, group_data_reg,
			                                use_precomputed_string_offset, group_data_array_base_reg);
		}
		const auto group_offset = 1 - group.minimum;
		if (group_offset != 0) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group_offset));
		}
		if (group.shift != 0) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group.shift));
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
		if (!options.mark_local_group) {
			return;
		}
		if (!local_aggregate_plan.sparse) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), context.group_index_offset, SLJIT_S4, 0);
		}
		EmitMarkSljitLocalPerfectHashGroupSeen(compiler, local_aggregate_plan, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
		                                       options.mark_local_payloads_seen, options.increment_count_seen);
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
	auto &aggregates = context.aggregates;
	auto &contract = context.contract;
	auto &codegen_plan = context.codegen_plan;
	auto &local_aggregate_plan = context.local_aggregate_plan;
	auto &deferred_flag_plan = context.deferred_flag_plan;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (local_aggregate_plan.enabled) {
				if (local_aggregate_plan.sparse && payload_idx == local_aggregate_plan.sparse_count_seen_lane) {
					continue;
				}
				if (local_aggregate_plan.sparse) {
					EmitSljitSparseLocalPerfectHashIncrementCount(compiler, local_aggregate_plan.lanes[payload_idx],
					                                              SLJIT_PERFECT_HASH_STATE_REG);
				} else {
					sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
					EmitSljitLocalPerfectHashIncrementCount(compiler, local_aggregate_plan.lanes[payload_idx],
					                                        SLJIT_S4);
				}
				continue;
			}
			EmitSljitPerfectHashPayloadStatePointer(context);
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(
				    compiler, context.state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset);
			} else {
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, context.state_pointer_reg, state_offset,
				                                                 aggregate.primitive_update_state_value_offset);
			}
			continue;
		}

		sljit_jump *payload_invalid = nullptr;
		if (codegen_plan.binary_shared_payload && options.all_valid &&
		    payload_idx == codegen_plan.binary_dependent_lane) {
			EmitSljitBinarySharedPayloadValueReg(compiler, codegen_plan, context.binary_shared_value_offset,
			                                     options.fast_path, options.no_source_selection, context.overflows,
			                                     options.payload_data_hoists);
		} else if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload_invalid = EmitLoadFusedTypedAggregateReferenceValue(
			    compiler, payloads[payload_idx], !options.fast_path && !options.no_source_selection, !options.all_valid,
			    SLJIT_S3, options.payload_data_hoists);
		} else {
			payload_invalid = EmitSljitPerfectHashExpressionTreeValue(
			    context, *payloads[payload_idx].expression_tree, options.fast_path, options.all_valid,
			    options.no_source_selection, options.payload_data_hoists);
		}
		if (codegen_plan.binary_shared_payload && options.all_valid && payload_idx == codegen_plan.binary_base_lane) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), context.binary_shared_value_offset, SLJIT_R2, 0);
		}

		if (local_aggregate_plan.enabled) {
			if (local_aggregate_plan.sparse) {
				auto cached_lane = options.run_cached_lanes && options.all_valid
				                       ? FindSljitSparseLocalRunCachedLane(*options.run_cached_lanes, payload_idx)
				                       : nullptr;
				if (cached_lane) {
					EmitSljitSparseLocalRunCacheAccumulate(compiler, local_aggregate_plan.lanes[payload_idx],
					                                       aggregate.primitive_update_kind, cached_lane->lower_reg,
					                                       SLJIT_PERFECT_HASH_STATE_REG, SLJIT_R2);
				} else {
					EmitSljitSparseLocalPerfectHashAccumulate(
					    compiler, local_aggregate_plan.lanes[payload_idx], aggregate.primitive_update_kind,
					    SLJIT_PERFECT_HASH_STATE_REG, SLJIT_R2, !options.all_valid);
				}
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), context.group_index_offset);
				EmitSljitLocalPerfectHashAccumulate(compiler, local_aggregate_plan.lanes[payload_idx],
				                                    aggregate.primitive_update_kind, SLJIT_S4, SLJIT_R2);
			}
			EmitSljitPerfectHashPayloadInvalidContinuation(compiler, payload_invalid);
			continue;
		}

		EmitSljitPerfectHashPayloadStatePointer(context);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(
				    compiler, context.state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
				    SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateInt64Immediate(
				    compiler, context.state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			}
		} else {
			if (deferred_flag_plan.enabled && options.all_valid) {
				EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(
				    compiler, context.state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
				    SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(
				    compiler, context.state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
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

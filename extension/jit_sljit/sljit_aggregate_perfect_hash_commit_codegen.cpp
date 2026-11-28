//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_commit_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "sljit_aggregate_perfect_hash_local_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static void EmitLoadSljitPerfectHashCommitLaneValue(struct sljit_compiler *compiler, sljit_s32 local_state_reg,
                                                    sljit_sw lane_offset, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_MEM1(local_state_reg), lane_offset);
}

static sljit_jump *EmitJumpIfSljitPerfectHashCommitLaneZero(struct sljit_compiler *compiler, sljit_s32 local_state_reg,
                                                            sljit_sw lane_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, local_state_reg, lane_offset, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

static void EmitSljitPerfectHashCommitCount(struct sljit_compiler *compiler,
                                            const SljitDensePerfectHashAggregateReductionLane &lane,
                                            sljit_s32 state_reg, sljit_s32 local_state_reg, idx_t state_offset,
                                            idx_t value_offset, bool count_known_nonzero) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, local_state_reg, lane.count_offset, SLJIT_R2);
	sljit_jump *no_count = nullptr;
	if (!count_known_nonzero) {
		no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	if (no_count) {
		sljit_set_label(no_count, sljit_emit_label(compiler));
	}
}

static void EmitSljitPerfectHashCommitInt64Value(struct sljit_compiler *compiler,
                                                 const SljitDensePerfectHashAggregateReductionLane &lane,
                                                 sljit_s32 state_reg, sljit_s32 local_state_reg, idx_t state_offset,
                                                 idx_t value_offset, idx_t state_is_set_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, local_state_reg, lane.lower_offset, SLJIT_R2);
	EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, state_reg, state_offset, value_offset,
	                                                  state_is_set_offset, SLJIT_R2);
}

static void EmitSljitPerfectHashCommitInt64(struct sljit_compiler *compiler,
                                            const SljitDensePerfectHashAggregateReductionLane &lane,
                                            sljit_s32 state_reg, sljit_s32 local_state_reg, idx_t state_offset,
                                            idx_t value_offset, idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitPerfectHashCommitLaneZero(compiler, local_state_reg, lane.saw_offset);
	}
	EmitSljitPerfectHashCommitInt64Value(compiler, lane, state_reg, local_state_reg, state_offset, value_offset,
	                                     state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitPerfectHashCommitHugeintValue(struct sljit_compiler *compiler,
                                                   const SljitDensePerfectHashAggregateReductionLane &lane,
                                                   sljit_s32 state_reg, sljit_s32 local_state_reg, idx_t state_offset,
                                                   idx_t value_offset, idx_t state_is_set_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, local_state_reg, lane.lower_offset, SLJIT_R2);
	if (lane.upper_offset >= 0) {
		EmitLoadSljitPerfectHashCommitLaneValue(compiler, local_state_reg, lane.upper_offset, SLJIT_R4);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_IMM, 0);
	}
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	if (lane.upper_offset < 0) {
		// A proven non-overflowing lower word still represents a signed hugeint delta.
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	}
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, state_reg, state_offset, state_is_set_offset);
}

static void EmitSljitPerfectHashCommitHugeint(struct sljit_compiler *compiler,
                                              const SljitDensePerfectHashAggregateReductionLane &lane,
                                              sljit_s32 state_reg, sljit_s32 local_state_reg, idx_t state_offset,
                                              idx_t value_offset, idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitPerfectHashCommitLaneZero(compiler, local_state_reg, lane.saw_offset);
	}
	EmitSljitPerfectHashCommitHugeintValue(compiler, lane, state_reg, local_state_reg, state_offset, value_offset,
	                                       state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitPerfectHashCommitPayloads(struct sljit_compiler *compiler,
                                               const SljitDensePerfectHashAggregateReductionPlan &reduction_plan,
                                               const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                               const ExecutionRegionAggregateContract &contract, sljit_s32 state_reg,
                                               sljit_s32 local_state_reg, bool payload_values_known_seen) {
	for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
		auto &descriptor = payload_descriptors[payload_idx];
		auto &lane = reduction_plan.lanes[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		switch (descriptor.primitive_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			EmitSljitPerfectHashCommitCount(compiler, lane, state_reg, local_state_reg, state_offset,
			                                descriptor.state_value_offset, true);
			break;
		case AggregatePrimitiveUpdateKind::COUNT:
			EmitSljitPerfectHashCommitCount(compiler, lane, state_reg, local_state_reg, state_offset,
			                                descriptor.state_value_offset, false);
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			EmitSljitPerfectHashCommitInt64(compiler, lane, state_reg, local_state_reg, state_offset,
			                                descriptor.state_value_offset, descriptor.state_is_set_offset,
			                                payload_values_known_seen || lane.value_always_seen);
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			EmitSljitPerfectHashCommitHugeint(compiler, lane, state_reg, local_state_reg, state_offset,
			                                  descriptor.state_value_offset, descriptor.state_is_set_offset,
			                                  payload_values_known_seen || lane.value_always_seen);
			break;
		default:
			throw InternalException("Unsupported SLJIT local perfect-hash aggregate commit kind");
		}
	}
}

static void EmitSljitPerfectHashLocalReductionCommit(struct sljit_compiler *compiler,
                                                     const SljitDensePerfectHashAggregateReductionPlan &reduction_plan,
                                                     const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                                     const ExecutionRegionAggregateContract &contract) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
	                           NumericCast<sljit_sw>(reduction_plan.group_count));
	EmitSljitPerfectHashReductionStatePointer(compiler, reduction_plan, SLJIT_S1, SLJIT_S2);
	sljit_jump *group_not_seen;
	if (reduction_plan.count_seen_lane == DConstants::INVALID_INDEX) {
		group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, reduction_plan.group_seen_offset, SLJIT_S1);
	} else {
		auto &count_lane = reduction_plan.lanes[reduction_plan.count_seen_lane];
		group_not_seen = EmitJumpIfSljitPerfectHashCommitLaneZero(compiler, SLJIT_S2, count_lane.count_offset);
	}
	EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S1, SljitPerfectHashTableRegisters());
	EmitSljitPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4, SljitPerfectHashTableRegisters());
	EmitSljitPerfectHashCommitPayloads(compiler, reduction_plan, payload_descriptors, contract, SLJIT_S4, SLJIT_S2,
	                                   false);
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

void EmitSljitDensePerfectHashAggregateReductionCommit(
    struct sljit_compiler *compiler, const SljitDensePerfectHashAggregateReductionPlan &reduction_plan,
    const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
    const ExecutionRegionAggregateContract &contract) {
	if (!reduction_plan.Ready()) {
		return;
	}
	EmitSljitPerfectHashLocalReductionCommit(compiler, reduction_plan, payload_descriptors, contract);
}

void EmitSljitPerfectHashFirstTouchBlocks(struct sljit_compiler *compiler,
                                          const vector<SljitPerfectHashFirstTouchSite> &sites,
                                          const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                          const ExecutionRegionAggregateContract &contract) {
	// S4 holds the group; R0 and R1 are free at every site
	for (auto &site : sites) {
		sljit_set_label(site.first_touch, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_group_states_marked));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 0, SLJIT_IMM, 1);
		EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S4, SljitPerfectHashTableRegisters());
		EmitSljitPerfectHashStatePointer(compiler, SLJIT_S4, SLJIT_R0, SljitPerfectHashTableRegisters());
		for (auto &descriptor : payload_descriptors) {
			if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
			    descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
				continue;
			}
			EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, SLJIT_R0,
			                                                contract.grouped_state_offsets[descriptor.aggregate_index],
			                                                descriptor.state_is_set_offset);
		}
		sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), site.resume);
	}
}

} // namespace duckdb

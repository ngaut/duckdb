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

template <class EMIT_GROUP>
static void EmitSljitDensePerfectHashSeenGroupCommitLoop(struct sljit_compiler *compiler, idx_t group_count,
                                                         sljit_sw group_seen_offset, sljit_sw count_seen_offset,
                                                         EMIT_GROUP &&emit_group) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(group_count));
	sljit_jump *group_not_seen;
	if (count_seen_offset < 0) {
		group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, group_seen_offset, SLJIT_S1);
	} else {
		group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, count_seen_offset, SLJIT_S1);
	}
	EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S1);
	EmitSljitPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4);
	emit_group();
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

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
	EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S1);
	EmitSljitPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4);
	EmitSljitPerfectHashCommitPayloads(compiler, reduction_plan, payload_descriptors, contract, SLJIT_S4, SLJIT_S2,
	                                   false);
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);
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

void EmitSljitDeferredPerfectHashFlagsCommit(struct sljit_compiler *compiler,
                                             const SljitDeferredPerfectHashFlagPlan &deferred_plan,
                                             const vector<SljitAggregatePayloadDescriptor> &payload_descriptors,
                                             const ExecutionRegionAggregateContract &contract) {
	if (!deferred_plan.enabled) {
		return;
	}
	EmitSljitDensePerfectHashSeenGroupCommitLoop(
	    compiler, deferred_plan.group_count, deferred_plan.group_seen_offset, -1, [&]() {
		    for (idx_t payload_idx = 0; payload_idx < payload_descriptors.size(); payload_idx++) {
			    auto &descriptor = payload_descriptors[payload_idx];
			    if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				    continue;
			    }
			    const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
			    EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, SLJIT_S4, state_offset,
			                                                    descriptor.state_is_set_offset);
		    }
	    });
}

} // namespace duckdb

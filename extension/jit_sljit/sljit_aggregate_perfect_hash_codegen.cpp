#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "sljit_aggregate_perfect_hash_local_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"

namespace duckdb {

bool SljitSparseLocalUsesCountSeen(const SljitLocalPerfectHashAggregatePlan &plan) {
	return plan.count_seen_lane != DConstants::INVALID_INDEX;
}

template <class EMIT_GROUP>
static void EmitSljitLocalPerfectHashGroupIndexLoop(struct sljit_compiler *compiler, idx_t group_count,
                                                    EMIT_GROUP &&emit_group) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(group_count));
	emit_group();
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitZeroSljitSparseLocalPerfectHashLane(struct sljit_compiler *compiler,
                                                    const SljitLocalPerfectHashAggregateLane &lane,
                                                    sljit_s32 group_pointer_reg) {
	if (lane.count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.count_offset, SLJIT_IMM, 0);
	}
	if (lane.lower_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_IMM, 0);
	}
	if (lane.upper_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_IMM, 0);
	}
	if (lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 0);
	}
}

static void EmitZeroSljitSparseLocalPerfectHashPayloads(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan) {
	EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
		EmitSljitSparseLocalPerfectHashGroupPointer(compiler, plan, SLJIT_S1, SLJIT_PERFECT_HASH_STATE_REG);
		for (auto &lane : plan.lanes) {
			EmitZeroSljitSparseLocalPerfectHashLane(compiler, lane, SLJIT_PERFECT_HASH_STATE_REG);
		}
	});
}

static void EmitZeroSljitSparseLocalPerfectHashCountSentinel(struct sljit_compiler *compiler,
                                                             const SljitLocalPerfectHashAggregatePlan &plan) {
	D_ASSERT(SljitSparseLocalUsesCountSeen(plan));
	const auto &lane = plan.lanes[plan.count_seen_lane];
	D_ASSERT(lane.count_offset >= 0);
	sljit_get_local_base(compiler, SLJIT_R0, 0, plan.group_payload_offset + lane.count_offset);
	EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, plan.group_payload_stride);
	});
}

void EmitZeroSljitLocalPerfectHashAggregateArrays(struct sljit_compiler *compiler,
                                                  const SljitLocalPerfectHashAggregatePlan &plan) {
	if (!plan.enabled) {
		return;
	}
	if (plan.sparse) {
		if (plan.sparse_eager_zero) {
			EmitZeroSljitSparseLocalPerfectHashPayloads(compiler, plan);
			return;
		}
		if (SljitSparseLocalUsesCountSeen(plan)) {
			EmitZeroSljitSparseLocalPerfectHashCountSentinel(compiler, plan);
		} else {
			EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
				EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, SLJIT_S1, 0);
			});
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset, SLJIT_IMM, 0);
		return;
	}
	EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
		if (plan.count_seen_lane == DConstants::INVALID_INDEX) {
			EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, SLJIT_S1, 0);
		}
		for (auto &lane : plan.lanes) {
			if (lane.count_offset >= 0) {
				EmitStoreSljitLocalArrayImmediate(compiler, lane.count_offset, SLJIT_S1, 0);
			}
			if (lane.lower_offset >= 0) {
				EmitStoreSljitLocalArrayImmediate(compiler, lane.lower_offset, SLJIT_S1, 0);
			}
			if (lane.upper_offset >= 0) {
				EmitStoreSljitLocalArrayImmediate(compiler, lane.upper_offset, SLJIT_S1, 0);
			}
			if (lane.saw_offset >= 0) {
				EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, SLJIT_S1, 0);
			}
		}
	});
}

void EmitZeroSljitDeferredPerfectHashFlagArray(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan) {
	if (!plan.enabled) {
		return;
	}
	EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, SLJIT_S1, 0);
	});
}

void EmitMarkSljitLocalPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregatePlan &plan, sljit_s32 group_index_reg,
                                            sljit_s32 group_pointer_reg, bool mark_payloads_seen,
                                            bool increment_count_seen) {
	if (!plan.enabled) {
		return;
	}
	if (!plan.sparse) {
		if (plan.count_seen_lane == DConstants::INVALID_INDEX) {
			EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, group_index_reg, 1);
		}
		return;
	}
	EmitSljitSparseLocalPerfectHashGroupPointer(compiler, plan, group_index_reg, group_pointer_reg);
	if (plan.sparse_eager_zero) {
		if (SljitSparseLocalUsesCountSeen(plan) && increment_count_seen) {
			auto &count_seen_lane = plan.lanes[plan.count_seen_lane];
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg),
			               count_seen_lane.count_offset);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset, SLJIT_R3,
			               0);
		}
		return;
	}
	const auto use_count_seen = SljitSparseLocalUsesCountSeen(plan);
	const auto &count_seen_lane = use_count_seen ? plan.lanes[plan.count_seen_lane] : plan.lanes[0];
	sljit_jump *group_seen;
	if (use_count_seen) {
		D_ASSERT(count_seen_lane.count_offset >= 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset);
		group_seen = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	} else {
		group_seen = EmitJumpIfSljitLocalGroupSeenNonZero(compiler, plan, group_index_reg);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset);
	EmitStoreSljitLocalArrayValue(compiler, plan.active_groups_offset, SLJIT_R2, group_index_reg);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset, SLJIT_R2, 0);
	if (!use_count_seen) {
		EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, group_index_reg, 1);
	}
	for (auto &lane : plan.lanes) {
		EmitZeroSljitSparseLocalPerfectHashLane(compiler, lane, group_pointer_reg);
	}
	if (mark_payloads_seen) {
		for (auto &lane : plan.lanes) {
			if (lane.saw_offset >= 0) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
			}
		}
	}
	sljit_set_label(group_seen, sljit_emit_label(compiler));
	if (use_count_seen && increment_count_seen) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset, SLJIT_R3, 0);
	}
}

void EmitMarkSljitDeferredPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan,
                                               sljit_s32 group_index_reg) {
	if (plan.enabled) {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, group_index_reg, 1);
	}
}

void EmitSljitLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                             const SljitLocalPerfectHashAggregateLane &lane,
                                             sljit_s32 group_index_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.count_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
}

static void EmitSljitLocalPerfectHashAccumulateInt64(struct sljit_compiler *compiler,
                                                     const SljitLocalPerfectHashAggregateLane &lane,
                                                     sljit_s32 group_index_reg, sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
	if (lane.saw_offset >= 0) {
		EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
	}
}

static void EmitSljitLocalPerfectHashAccumulateHugeint(struct sljit_compiler *compiler,
                                                       const SljitLocalPerfectHashAggregateLane &lane,
                                                       sljit_s32 group_index_reg, sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	if (lane.local_lower_never_overflows) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
		if (lane.saw_offset >= 0) {
			EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
		}
		return;
	}
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R1, 0);
	auto no_upper_update = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadSljitLocalArrayValue(compiler, lane.upper_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	EmitStoreSljitLocalArrayValue(compiler, lane.upper_offset, group_index_reg, SLJIT_R3);
	sljit_set_label(no_upper_update, sljit_emit_label(compiler));
	if (lane.saw_offset >= 0) {
		EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
	}
}

void EmitSljitLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                         const SljitLocalPerfectHashAggregateLane &lane,
                                         AggregatePrimitiveUpdateKind kind, sljit_s32 group_index_reg,
                                         sljit_s32 value_reg) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitLocalPerfectHashAccumulateHugeint(compiler, lane, group_index_reg, value_reg);
	} else {
		EmitSljitLocalPerfectHashAccumulateInt64(compiler, lane, group_index_reg, value_reg);
	}
}

void EmitSljitSparseLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregateLane &lane,
                                                   sljit_s32 group_pointer_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.count_offset, SLJIT_R3, 0);
}

static void EmitSljitSparseLocalPerfectHashAccumulateInt64(struct sljit_compiler *compiler,
                                                           const SljitLocalPerfectHashAggregateLane &lane,
                                                           sljit_s32 group_pointer_reg, sljit_s32 value_reg,
                                                           bool store_saw) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
	if (store_saw && lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
	}
}

static void EmitSljitSparseLocalPerfectHashAccumulateHugeint(struct sljit_compiler *compiler,
                                                             const SljitLocalPerfectHashAggregateLane &lane,
                                                             sljit_s32 group_pointer_reg, sljit_s32 value_reg,
                                                             bool store_saw) {
	// Sparse local hugeint lanes keep a wrapped int64 lower word and signed-overflow correction count.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	if (lane.local_lower_never_overflows) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
		if (store_saw && lane.saw_offset >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
		}
		return;
	}
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_OVERFLOW, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
	auto no_overflow = sljit_emit_jump(compiler, SLJIT_NOT_OVERFLOW);
	auto negative_overflow = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	auto overflow_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(negative_overflow, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, -1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	sljit_set_label(overflow_done, sljit_emit_label(compiler));
	sljit_set_label(no_overflow, sljit_emit_label(compiler));
	if (store_saw && lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
	}
}

void EmitSljitSparseLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                               const SljitLocalPerfectHashAggregateLane &lane,
                                               AggregatePrimitiveUpdateKind kind, sljit_s32 group_pointer_reg,
                                               sljit_s32 value_reg, bool store_saw) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitSparseLocalPerfectHashAccumulateHugeint(compiler, lane, group_pointer_reg, value_reg, store_saw);
	} else {
		EmitSljitSparseLocalPerfectHashAccumulateInt64(compiler, lane, group_pointer_reg, value_reg, store_saw);
	}
}

} // namespace duckdb

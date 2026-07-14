#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "sljit_aggregate_perfect_hash_local_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"

namespace duckdb {

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

void EmitZeroSljitDensePerfectHashAggregateReduction(struct sljit_compiler *compiler,
                                                     const SljitDensePerfectHashAggregateReductionPlan &plan) {
	if (!plan.Ready()) {
		return;
	}
	EmitSljitLocalPerfectHashGroupIndexLoop(compiler, plan.group_count, [&]() {
		if (plan.count_seen_lane == DConstants::INVALID_INDEX) {
			EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, SLJIT_S1, 0);
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

void EmitMarkSljitDensePerfectHashGroupSeen(struct sljit_compiler *compiler,
                                            const SljitDensePerfectHashAggregateReductionPlan &plan,
                                            sljit_s32 group_index_reg) {
	if (!plan.Ready()) {
		return;
	}
	if (plan.count_seen_lane == DConstants::INVALID_INDEX) {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, group_index_reg, 1);
	}
}

void EmitMarkSljitDeferredPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                               const SljitDeferredPerfectHashFlagPlan &plan,
                                               sljit_s32 group_index_reg) {
	if (plan.enabled) {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, group_index_reg, 1);
	}
}

void EmitSljitDensePerfectHashIncrementCount(struct sljit_compiler *compiler,
                                             const SljitDensePerfectHashAggregateReductionLane &lane,
                                             sljit_s32 group_index_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.count_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
}

static void EmitSljitDensePerfectHashAccumulateInt64(struct sljit_compiler *compiler,
                                                     const SljitDensePerfectHashAggregateReductionLane &lane,
                                                     sljit_s32 group_index_reg, sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3, SLJIT_R3, 0);
	if (lane.saw_offset >= 0) {
		EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
	}
}

static void EmitSljitDensePerfectHashAccumulateHugeint(struct sljit_compiler *compiler,
                                                       const SljitDensePerfectHashAggregateReductionLane &lane,
                                                       sljit_s32 group_index_reg, sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, lane.lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, group_index_reg), 3);
	if (lane.batch_lower_never_overflows) {
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

void EmitSljitDensePerfectHashAccumulate(struct sljit_compiler *compiler,
                                         const SljitDensePerfectHashAggregateReductionLane &lane,
                                         AggregatePrimitiveUpdateKind kind, sljit_s32 group_index_reg,
                                         sljit_s32 value_reg) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitDensePerfectHashAccumulateHugeint(compiler, lane, group_index_reg, value_reg);
	} else {
		EmitSljitDensePerfectHashAccumulateInt64(compiler, lane, group_index_reg, value_reg);
	}
}

} // namespace duckdb

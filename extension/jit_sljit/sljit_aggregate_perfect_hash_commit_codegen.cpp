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
                                                         sljit_sw group_seen_offset, EMIT_GROUP &&emit_group) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(group_count));
	auto group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, group_seen_offset, SLJIT_S1);
	EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S1);
	EmitSljitPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4);
	emit_group();
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

enum class SljitPerfectHashCommitLaneSourceKind : uint8_t { DENSE_LOCAL_ARRAY, SPARSE_GROUP_POINTER };

struct SljitPerfectHashCommitLaneSource {
	SljitPerfectHashCommitLaneSourceKind kind;
	sljit_s32 index_reg = 0;
	sljit_s32 pointer_reg = 0;
};

static SljitPerfectHashCommitLaneSource DenseLocalCommitLaneSource(sljit_s32 group_index_reg) {
	SljitPerfectHashCommitLaneSource result;
	result.kind = SljitPerfectHashCommitLaneSourceKind::DENSE_LOCAL_ARRAY;
	result.index_reg = group_index_reg;
	return result;
}

static SljitPerfectHashCommitLaneSource SparseLocalCommitLaneSource(sljit_s32 group_pointer_reg) {
	SljitPerfectHashCommitLaneSource result;
	result.kind = SljitPerfectHashCommitLaneSourceKind::SPARSE_GROUP_POINTER;
	result.pointer_reg = group_pointer_reg;
	return result;
}

static void EmitLoadSljitPerfectHashCommitLaneValue(struct sljit_compiler *compiler,
                                                    const SljitPerfectHashCommitLaneSource &source,
                                                    sljit_sw lane_offset, sljit_s32 target_reg) {
	if (source.kind == SljitPerfectHashCommitLaneSourceKind::DENSE_LOCAL_ARRAY) {
		EmitLoadSljitLocalArrayValue(compiler, lane_offset, source.index_reg, target_reg);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_MEM1(source.pointer_reg), lane_offset);
}

static sljit_jump *EmitJumpIfSljitPerfectHashCommitLaneZero(struct sljit_compiler *compiler,
                                                            const SljitPerfectHashCommitLaneSource &source,
                                                            sljit_sw lane_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, source, lane_offset, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

static void EmitSljitPerfectHashCommitCount(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                            const SljitPerfectHashCommitLaneSource &source, idx_t state_offset,
                                            idx_t value_offset, bool count_known_nonzero) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, source, lane.count_offset, SLJIT_R2);
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
                                                 const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                                 const SljitPerfectHashCommitLaneSource &source, idx_t state_offset,
                                                 idx_t value_offset, idx_t state_is_set_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, source, lane.lower_offset, SLJIT_R2);
	EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, state_reg, state_offset, value_offset,
	                                                  state_is_set_offset, SLJIT_R2);
}

static void EmitSljitPerfectHashCommitInt64(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                            const SljitPerfectHashCommitLaneSource &source, idx_t state_offset,
                                            idx_t value_offset, idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitPerfectHashCommitLaneZero(compiler, source, lane.saw_offset);
	}
	EmitSljitPerfectHashCommitInt64Value(compiler, lane, state_reg, source, state_offset, value_offset,
	                                     state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitPerfectHashCommitHugeintValue(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                                   const SljitPerfectHashCommitLaneSource &source, idx_t state_offset,
                                                   idx_t value_offset, idx_t state_is_set_offset) {
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, source, lane.lower_offset, SLJIT_R2);
	EmitLoadSljitPerfectHashCommitLaneValue(compiler, source, lane.upper_offset, SLJIT_R4);
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	if (source.kind == SljitPerfectHashCommitLaneSourceKind::SPARSE_GROUP_POINTER) {
		// Sparse local hugeint lanes store overflow corrections; the lower sign completes the upper word.
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	}
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, state_reg, state_offset, state_is_set_offset);
}

static void EmitSljitPerfectHashCommitHugeint(struct sljit_compiler *compiler,
                                              const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                              const SljitPerfectHashCommitLaneSource &source, idx_t state_offset,
                                              idx_t value_offset, idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitPerfectHashCommitLaneZero(compiler, source, lane.saw_offset);
	}
	EmitSljitPerfectHashCommitHugeintValue(compiler, lane, state_reg, source, state_offset, value_offset,
	                                       state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitPerfectHashCommitPayloads(struct sljit_compiler *compiler,
                                               const SljitLocalPerfectHashAggregatePlan &local_plan,
                                               const vector<ExecutionRegionAggregateInput> &aggregates,
                                               const ExecutionRegionAggregateContract &contract, sljit_s32 state_reg,
                                               const SljitPerfectHashCommitLaneSource &source,
                                               bool payload_values_known_seen) {
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &lane = local_plan.lanes[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		switch (aggregate.primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			EmitSljitPerfectHashCommitCount(compiler, lane, state_reg, source, state_offset,
			                                aggregate.primitive_update_state_value_offset, true);
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			EmitSljitPerfectHashCommitInt64(
			    compiler, lane, state_reg, source, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, payload_values_known_seen || lane.value_always_seen);
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			EmitSljitPerfectHashCommitHugeint(
			    compiler, lane, state_reg, source, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, payload_values_known_seen || lane.value_always_seen);
			break;
		default:
			throw InternalException("Unsupported SLJIT local perfect-hash aggregate commit kind");
		}
	}
}

void EmitSljitLocalPerfectHashCommit(struct sljit_compiler *compiler,
                                     const SljitLocalPerfectHashAggregatePlan &local_plan,
                                     const vector<ExecutionRegionAggregateInput> &aggregates,
                                     const ExecutionRegionAggregateContract &contract, bool local_payloads_known_seen) {
	if (!local_plan.enabled) {
		return;
	}
	if (local_plan.sparse) {
		if (local_plan.sparse_eager_zero) {
			auto &count_lane = local_plan.lanes[local_plan.sparse_count_seen_lane];
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			auto loop = sljit_emit_label(compiler);
			auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
			                           NumericCast<sljit_sw>(local_plan.group_count));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
			EmitSljitSparseLocalPerfectHashGroupPointer(compiler, local_plan, SLJIT_S3, SLJIT_PERFECT_HASH_STATE_REG);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_PERFECT_HASH_STATE_REG),
			               count_lane.count_offset);
			auto no_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S3);
			EmitSljitPerfectHashStatePointer(compiler, SLJIT_S3, SLJIT_S4);
			EmitSljitPerfectHashCommitPayloads(compiler, local_plan, aggregates, contract, SLJIT_S4,
			                                   SparseLocalCommitLaneSource(SLJIT_PERFECT_HASH_STATE_REG), true);
			sljit_set_label(no_group, sljit_emit_label(compiler));
			EmitNextSljitNativeVectorLoop(compiler, loop);
			sljit_set_label(done, sljit_emit_label(compiler));
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_SP), local_plan.active_count_offset);
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitLocalArrayValue(compiler, local_plan.active_groups_offset, SLJIT_S1, SLJIT_S3);
		EmitSljitSparseLocalPerfectHashGroupPointer(compiler, local_plan, SLJIT_S3, SLJIT_PERFECT_HASH_STATE_REG);
		EmitSljitPerfectHashSetOutputGroup(compiler, SLJIT_S3);
		EmitSljitPerfectHashStatePointer(compiler, SLJIT_S3, SLJIT_S4);
		EmitSljitPerfectHashCommitPayloads(compiler, local_plan, aggregates, contract, SLJIT_S4,
		                                   SparseLocalCommitLaneSource(SLJIT_PERFECT_HASH_STATE_REG),
		                                   local_payloads_known_seen);
		EmitNextSljitNativeVectorLoop(compiler, loop);
		sljit_set_label(done, sljit_emit_label(compiler));
		return;
	}
	EmitSljitDensePerfectHashSeenGroupCommitLoop(compiler, local_plan.group_count, local_plan.group_seen_offset, [&]() {
		EmitSljitPerfectHashCommitPayloads(compiler, local_plan, aggregates, contract, SLJIT_S4,
		                                   DenseLocalCommitLaneSource(SLJIT_S1), local_payloads_known_seen);
	});
}

void EmitSljitDeferredPerfectHashFlagsCommit(struct sljit_compiler *compiler,
                                             const SljitDeferredPerfectHashFlagPlan &deferred_plan,
                                             const vector<ExecutionRegionAggregateInput> &aggregates,
                                             const ExecutionRegionAggregateContract &contract) {
	if (!deferred_plan.enabled) {
		return;
	}
	EmitSljitDensePerfectHashSeenGroupCommitLoop(
	    compiler, deferred_plan.group_count, deferred_plan.group_seen_offset, [&]() {
		    for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			    auto &aggregate = aggregates[payload_idx];
			    if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				    continue;
			    }
			    const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			    EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, SLJIT_S4, state_offset,
			                                                    aggregate.primitive_update_state_is_set_offset);
		    }
	    });
}

} // namespace duckdb

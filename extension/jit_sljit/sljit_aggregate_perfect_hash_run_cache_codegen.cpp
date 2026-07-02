#include "sljit_aggregate_perfect_hash_codegen.hpp"

namespace duckdb {

bool SljitSparseLocalUsesCountSeen(const SljitLocalPerfectHashAggregatePlan &plan) {
	return plan.sparse_count_seen_lane != DConstants::INVALID_INDEX;
}

idx_t CountSljitSparseLocalRunCacheableLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                             const vector<ExecutionRegionAggregateInput> &aggregates) {
	if (!SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS || !plan.enabled || !plan.sparse ||
	    !SljitSparseLocalUsesCountSeen(plan)) {
		return 0;
	}
	idx_t result = 0;
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		if (payload_idx >= plan.lanes.size() || payload_idx == plan.sparse_count_seen_lane) {
			continue;
		}
		auto &lane = plan.lanes[payload_idx];
		if (lane.lower_offset < 0 || !lane.value_always_seen) {
			continue;
		}
		switch (aggregates[payload_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			continue;
		}
		result++;
	}
	return result;
}

vector<SljitSparseLocalRunCachedLane>
BuildSljitSparseLocalRunCachedLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                    const vector<sljit_s32> &lower_regs) {
	vector<SljitSparseLocalRunCachedLane> result;
	if (!SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS || !plan.enabled || !plan.sparse ||
	    !SljitSparseLocalUsesCountSeen(plan)) {
		return result;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		if (result.size() >= lower_regs.size()) {
			break;
		}
		if (payload_idx >= plan.lanes.size() || payload_idx == plan.sparse_count_seen_lane) {
			continue;
		}
		auto &lane = plan.lanes[payload_idx];
		if (lane.lower_offset < 0 || !lane.value_always_seen) {
			continue;
		}
		switch (aggregates[payload_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			continue;
		}
		SljitSparseLocalRunCachedLane cached_lane;
		cached_lane.payload_idx = payload_idx;
		cached_lane.lower_reg = lower_regs[result.size()];
		result.push_back(cached_lane);
	}
	return result;
}

const SljitSparseLocalRunCachedLane *
FindSljitSparseLocalRunCachedLane(const vector<SljitSparseLocalRunCachedLane> &cached_lanes, idx_t payload_idx) {
	for (auto &cached_lane : cached_lanes) {
		if (cached_lane.payload_idx == payload_idx) {
			return &cached_lane;
		}
	}
	return nullptr;
}

void EmitSljitSparseLocalRunCacheFlush(struct sljit_compiler *compiler, const SljitLocalPerfectHashAggregatePlan &plan,
                                       const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                       sljit_s32 group_pointer_reg, sljit_sw cached_group_offset,
                                       sljit_sw cached_start_offset, sljit_s32 current_index_reg,
                                       sljit_sw cached_count_offset) {
	if (!SljitSparseLocalUsesCountSeen(plan)) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), cached_group_offset);
	auto no_cached_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, -1);
	auto &count_lane = plan.lanes[plan.sparse_count_seen_lane];
	if (cached_count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_count_offset);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_start_offset);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R3, 0, current_index_reg, 0, SLJIT_R3, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(group_pointer_reg), count_lane.count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_lane.count_offset, SLJIT_R4, 0);
	for (auto &cached_lane : cached_lanes) {
		auto &lane = plan.lanes[cached_lane.payload_idx];
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, cached_lane.lower_reg, 0);
	}
	sljit_set_label(no_cached_group, sljit_emit_label(compiler));
}

void EmitSljitSparseLocalRunCacheLoadCurrent(struct sljit_compiler *compiler,
                                             const SljitLocalPerfectHashAggregatePlan &plan,
                                             const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                             sljit_s32 group_pointer_reg, sljit_sw cached_start_offset,
                                             sljit_s32 current_index_reg, sljit_sw cached_count_offset) {
	D_ASSERT(SljitSparseLocalUsesCountSeen(plan));
	if (cached_count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_count_offset, SLJIT_IMM, 1);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_start_offset, current_index_reg, 0);
	}
	for (auto &cached_lane : cached_lanes) {
		auto &lane = plan.lanes[cached_lane.payload_idx];
		sljit_emit_op1(compiler, SLJIT_MOV, cached_lane.lower_reg, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	}
}

void EmitSljitSparseLocalRunCacheIncrementCount(struct sljit_compiler *compiler, sljit_sw cached_count_offset) {
	if (cached_count_offset < 0) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_count_offset, SLJIT_R3, 0);
}

void EmitSljitSparseLocalRunCacheAccumulate(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregateLane &lane,
                                            AggregatePrimitiveUpdateKind kind, sljit_s32 lower_reg,
                                            sljit_s32 group_pointer_reg, sljit_s32 value_reg) {
	if (kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_ADD, lower_reg, 0, lower_reg, 0, value_reg, 0);
		return;
	}
	if (lane.local_lower_never_overflows) {
		sljit_emit_op2(compiler, SLJIT_ADD, lower_reg, 0, lower_reg, 0, value_reg, 0);
		return;
	}
	// Keep the lower word live across a same-group run; upper only changes on signed 64-bit overflow.
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_OVERFLOW, lower_reg, 0, lower_reg, 0, value_reg, 0);
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
}

} // namespace duckdb

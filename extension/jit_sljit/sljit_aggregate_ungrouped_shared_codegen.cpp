//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_shared_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_ungrouped_shared_codegen.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

void EmitLoadUngroupedAggregatePointer(struct sljit_compiler *compiler, sljit_sw pointer_array_offset, idx_t lane_idx,
                                       sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0), pointer_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(target_reg), SljitPointerArrayOffset(lane_idx));
}

void EmitUngroupedAggregateAddRowCount(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 count_reg) {
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx,
	                                  SLJIT_R0);
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, count_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));
}

void EmitUngroupedAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 count_reg) {
	EmitUngroupedAggregateAddRowCount(compiler, lane_idx, count_reg);
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                                  SLJIT_R0);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, count_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

void EmitUngroupedAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitUngroupedAggregateCommitCountStar(compiler, lane_idx, SLJIT_S2);
}

void EmitUngroupedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                          sljit_sw saw_value_offset, sljit_s32 count_reg) {
	EmitUngroupedAggregateAddRowCount(compiler, lane_idx, count_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                                  SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                                  SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

void EmitUngroupedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                          sljit_sw saw_value_offset) {
	EmitUngroupedAggregateCommitSumInt64(compiler, lane_idx, local_sum_offset, saw_value_offset, SLJIT_S2);
}

void EmitUngroupedAggregateAccumulate(struct sljit_compiler *compiler, AggregatePrimitiveUpdateKind kind,
                                      sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                      sljit_sw saw_value_offset, sljit_s32 value_reg) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_lower_offset, local_upper_offset, saw_value_offset,
		                                         value_reg);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_lower_offset, saw_value_offset, value_reg);
	}
}

void EmitUngroupedAggregateAccumulateHugeintInt64Regs(struct sljit_compiler *compiler, sljit_s32 lower_reg,
                                                      sljit_s32 upper_reg, sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, lower_reg, 0, lower_reg, 0, value_reg, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_ADD, upper_reg, 0, upper_reg, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, upper_reg, 0, upper_reg, 0, SLJIT_R3, 0);
}

void EmitUngroupedAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                            sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                            sljit_sw saw_value_offset, sljit_s32 count_reg) {
	EmitUngroupedAggregateAddRowCount(compiler, lane_idx, count_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2, 0);
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_hugeint_values), lane_idx,
	                                  SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_hugeint_value), SLJIT_R0, 0);
	EmitLoadUngroupedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                                  SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set), SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

void EmitUngroupedAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                            sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                            sljit_sw saw_value_offset) {
	EmitUngroupedAggregateCommitSumHugeint(compiler, lane_idx, local_lower_offset, local_upper_offset, saw_value_offset,
	                                       SLJIT_S2);
}

} // namespace duckdb

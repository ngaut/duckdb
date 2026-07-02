//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_local_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"

namespace duckdb {

inline void EmitLoadSljitLocalArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset, sljit_s32 index_reg,
                                         sljit_s32 target_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
}

inline void EmitStoreSljitLocalArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset, sljit_s32 index_reg,
                                          sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, index_reg), 3, value_reg, 0);
}

inline void EmitStoreSljitLocalArrayImmediate(struct sljit_compiler *compiler, sljit_sw array_offset,
                                              sljit_s32 index_reg, sljit_sw value) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, index_reg), 3, SLJIT_IMM, value);
}

inline sljit_jump *EmitJumpIfSljitLocalArrayZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                 sljit_s32 index_reg) {
	EmitLoadSljitLocalArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

inline sljit_jump *EmitJumpIfSljitLocalArrayNonZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                    sljit_s32 index_reg) {
	EmitLoadSljitLocalArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

inline void EmitLoadSljitLocalByteArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset,
                                             sljit_s32 index_reg, sljit_s32 target_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 0);
}

inline void EmitStoreSljitLocalByteArrayImmediate(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                  sljit_s32 index_reg, sljit_sw value) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, index_reg), 0, SLJIT_IMM, value);
}

inline sljit_jump *EmitJumpIfSljitLocalByteArrayNonZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                        sljit_s32 index_reg) {
	EmitLoadSljitLocalByteArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

inline void EmitStoreSljitLocalGroupSeenImmediate(struct sljit_compiler *compiler,
                                                  const SljitLocalPerfectHashAggregatePlan &plan, sljit_s32 index_reg,
                                                  sljit_sw value) {
	if (plan.group_seen_is_byte) {
		EmitStoreSljitLocalByteArrayImmediate(compiler, plan.group_seen_offset, index_reg, value);
	} else {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, index_reg, value);
	}
}

inline sljit_jump *EmitJumpIfSljitLocalGroupSeenNonZero(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan,
                                                        sljit_s32 index_reg) {
	if (plan.group_seen_is_byte) {
		return EmitJumpIfSljitLocalByteArrayNonZero(compiler, plan.group_seen_offset, index_reg);
	}
	return EmitJumpIfSljitLocalArrayNonZero(compiler, plan.group_seen_offset, index_reg);
}

inline bool SljitLocalStrideIsPowerOfTwo(sljit_sw stride) {
	return stride > 0 && (stride & (stride - 1)) == 0;
}

inline sljit_sw SljitLocalStrideShift(sljit_sw stride) {
	sljit_sw shift = 0;
	while (stride > 1) {
		stride >>= 1;
		shift++;
	}
	return shift;
}

inline void EmitSljitSparseLocalPerfectHashGroupPointer(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan,
                                                        sljit_s32 group_index_reg, sljit_s32 target_reg) {
	sljit_get_local_base(compiler, target_reg, 0, plan.group_payload_offset);
	if (SljitLocalStrideIsPowerOfTwo(plan.group_payload_stride)) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R1, 0, group_index_reg, 0, SLJIT_IMM,
		               SljitLocalStrideShift(plan.group_payload_stride));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, plan.group_payload_stride);
		sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, group_index_reg, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
}

} // namespace duckdb

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

} // namespace duckdb

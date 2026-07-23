//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_loop_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static constexpr sljit_sw SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL = 0;
static constexpr sljit_s32 SLJIT_HASH_JOIN_PROBE_LOCAL_SIZE = static_cast<sljit_s32>(sizeof(sljit_sw));

static inline void EmitSaveHashJoinProbeOffset(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL, SLJIT_R1, 0);
}

static inline void EmitRestoreHashJoinProbeOffset(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL);
}

static inline void EmitApplyHashJoinBitmask(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                            sljit_s32 bitmask_reg) {
	if (bitmask_reg != 0) {
		sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, bitmask_reg, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, scratch, 0);
}

static inline void EmitLoadHashJoinPointerMask(struct sljit_compiler *compiler, sljit_s32 target,
                                               sljit_s32 pointer_mask_reg) {
	if (pointer_mask_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, pointer_mask_reg, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, pointer_mask));
}

static inline void EmitBloomFilterMaskPart(struct sljit_compiler *compiler, sljit_s32 hash_reg, sljit_s32 mask_reg,
                                           sljit_s32 scratch, sljit_s32 shift, bool first_part) {
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, hash_reg, 0, SLJIT_IMM, shift);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, scratch, 0, SLJIT_IMM, 0x3F);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	if (first_part) {
		sljit_emit_op1(compiler, SLJIT_MOV, mask_reg, 0, scratch, 0);
	} else {
		sljit_emit_op2(compiler, SLJIT_OR, mask_reg, 0, mask_reg, 0, scratch, 0);
	}
}

static inline void EmitBloomFilterMask(struct sljit_compiler *compiler, sljit_s32 hash_reg, sljit_s32 mask_reg,
                                       sljit_s32 scratch) {
#if DUCKDB_IS_BIG_ENDIAN
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 24, true);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 16, false);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 8, false);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 0, false);
#else
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 32, true);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 40, false);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 48, false);
	EmitBloomFilterMaskPart(compiler, hash_reg, mask_reg, scratch, 56, false);
#endif
}

static inline struct sljit_jump *EmitJumpIfRegularHashJoinBloomMiss(struct sljit_compiler *compiler, sljit_s32 hash_reg,
                                                                    sljit_s32 bits_reg, sljit_s32 slot_reg,
                                                                    sljit_s32 mask_reg, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, bits_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, bloom_filter_bits));
	auto no_bloom = sljit_emit_cmp(compiler, SLJIT_EQUAL, bits_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, slot_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, bloom_filter_bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, slot_reg, 0, slot_reg, 0, hash_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, slot_reg, 0, SLJIT_MEM2(bits_reg, slot_reg), 3);
	EmitBloomFilterMask(compiler, hash_reg, mask_reg, scratch);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, slot_reg, 0, mask_reg, 0);
	auto bloom_miss = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, mask_reg, 0);
	sljit_set_label(no_bloom, sljit_emit_label(compiler));
	return bloom_miss;
}

static inline void EmitStoreHashJoinMarkProbeFlag(struct sljit_compiler *compiler, sljit_sw value) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), 2, SLJIT_IMM, value);
}

static inline void SetSljitJumpLabels(const vector<struct sljit_jump *> &jumps, struct sljit_label *label) {
	for (auto &jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static inline void EmitRepeatHashJoinProbeSlot(struct sljit_compiler *compiler, struct sljit_label *probe_loop,
                                               struct sljit_jump *salt_mismatch, sljit_s32 bitmask_reg) {
	auto next_slot = sljit_emit_label(compiler);
	if (salt_mismatch) {
		sljit_set_label(salt_mismatch, next_slot);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, bitmask_reg);
	auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_probe, probe_loop);
}

static inline void EmitRepeatHashJoinProbeRow(struct sljit_compiler *compiler, struct sljit_label *row_loop) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_rows, row_loop);
}

static inline void EmitRetryHashJoinProbeSlot(struct sljit_compiler *compiler, struct sljit_label *probe_loop,
                                              struct sljit_jump *salt_mismatch,
                                              const vector<struct sljit_jump *> &equality_key_mismatches,
                                              bool restore_probe_offset, sljit_s32 bitmask_reg,
                                              const vector<struct sljit_jump *> *predicate_key_mismatches = nullptr) {
	auto restore_hash_offset = sljit_emit_label(compiler);
	SetSljitJumpLabels(equality_key_mismatches, restore_hash_offset);
	if (predicate_key_mismatches) {
		SetSljitJumpLabels(*predicate_key_mismatches, restore_hash_offset);
	}
	if (restore_probe_offset) {
		EmitRestoreHashJoinProbeOffset(compiler);
	}
	EmitRepeatHashJoinProbeSlot(compiler, probe_loop, salt_mismatch, bitmask_reg);
}

} // namespace duckdb

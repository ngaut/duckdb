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
                                            bool bitmask_reg_available) {
	if (bitmask_reg_available) {
		sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, SLJIT_S6, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, scratch, 0);
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
                                               struct sljit_jump *salt_mismatch, bool bitmask_reg_available) {
	auto next_slot = sljit_emit_label(compiler);
	if (salt_mismatch) {
		sljit_set_label(salt_mismatch, next_slot);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, bitmask_reg_available);
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
                                              bool restore_probe_offset, bool bitmask_reg_available,
                                              const vector<struct sljit_jump *> *predicate_key_mismatches = nullptr) {
	auto restore_hash_offset = sljit_emit_label(compiler);
	SetSljitJumpLabels(equality_key_mismatches, restore_hash_offset);
	if (predicate_key_mismatches) {
		SetSljitJumpLabels(*predicate_key_mismatches, restore_hash_offset);
	}
	if (restore_probe_offset) {
		EmitRestoreHashJoinProbeOffset(compiler);
	}
	EmitRepeatHashJoinProbeSlot(compiler, probe_loop, salt_mismatch, bitmask_reg_available);
}

static inline void EmitSkipHashJoinProbeRow(struct sljit_compiler *compiler, struct sljit_label *row_loop,
                                            struct sljit_jump *empty_slot,
                                            const vector<struct sljit_jump *> &source_is_null,
                                            struct sljit_jump *advance_row = nullptr,
                                            struct sljit_jump *advance_after_predicate = nullptr) {
	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(empty_slot, skip_row);
	SetSljitJumpLabels(source_is_null, skip_row);
	if (advance_row) {
		sljit_set_label(advance_row, skip_row);
	}
	if (advance_after_predicate) {
		sljit_set_label(advance_after_predicate, skip_row);
	}
	EmitRepeatHashJoinProbeRow(compiler, row_loop);
}

} // namespace duckdb

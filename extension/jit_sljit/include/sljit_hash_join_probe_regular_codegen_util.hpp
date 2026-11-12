//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_codegen_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_util.hpp"
#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_regular_input_codegen.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static inline void EmitFinishRegularHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, resume_row_pointer), SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, finished), SLJIT_IMM, 1);
}

static inline void EmitPauseRegularHashJoinProbe(struct sljit_compiler *compiler, sljit_s32 resume_row_pointer) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, resume_row_pointer), resume_row_pointer, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, finished), SLJIT_IMM, 0);
}

static inline unique_ptr<ExecutionRegionCodeHandle>
FinishSljitRegularHashJoinProbeCode(struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function,
                                    string &error, sljit_s32 selected_count_src, sljit_sw selected_count_srcw = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, selected_count), selected_count_src,
	               selected_count_srcw);
	EmitFinishRegularHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

static inline void EmitAbortHashJoinProbeWithCastError(struct sljit_compiler *compiler, sljit_s32 value_reg) {
	EmitCallHashJoinInt64ToInt32CastError(compiler, offsetof(SljitNativeRegularHashJoinProbeInput, error),
	                                      offsetof(SljitNativeRegularHashJoinProbeInput, source_key0_cast), value_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, selected_count), SLJIT_S3, 0);
	EmitPauseRegularHashJoinProbe(compiler, SLJIT_IMM);
	sljit_emit_return_void(compiler);
}

static inline void EmitCheckedHashJoinInt64ToInt32Range(struct sljit_compiler *compiler, sljit_s32 value_reg,
                                                        sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_key0_int64_to_int32_unchecked));
	auto unchecked = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	auto below_range = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Minimum()));
	auto above_range = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Maximum()));
	auto in_range = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto range_error = sljit_emit_label(compiler);
	sljit_set_label(below_range, range_error);
	sljit_set_label(above_range, range_error);
	EmitAbortHashJoinProbeWithCastError(compiler, value_reg);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(unchecked, done);
	sljit_set_label(in_range, done);
}

static inline void EmitLoadHashJoinSourceKey(struct sljit_compiler *compiler, idx_t key_idx,
                                             SljitNativeHashJoinKeyKind kind, sljit_s32 target, sljit_s32 base,
                                             sljit_s32 index, sljit_sw offset, sljit_s32 scratch,
                                             bool check_int64_to_int32_range = true) {
	if (key_idx != 0 || kind != SljitNativeHashJoinKeyKind::INT32) {
		EmitLoadHashJoinKey(compiler, kind, target, base, index, offset);
		return;
	}

	D_ASSERT(target != base);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_key0_int64_to_int32));
	auto use_int32_source = sljit_emit_cmp(compiler, SLJIT_EQUAL, target, 0, SLJIT_IMM, 0);
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(base), offset);
	} else if (offset == 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, index), 3);
	} else {
		sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index, 0, SLJIT_IMM, 3);
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, offset);
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, scratch), 0);
	}
	if (check_int64_to_int32_range) {
		EmitCheckedHashJoinInt64ToInt32Range(compiler, target, scratch);
	}
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(use_int32_source, sljit_emit_label(compiler));
	EmitLoadHashJoinKey(compiler, kind, target, base, index, offset);
	sljit_set_label(done, sljit_emit_label(compiler));
}

// Build rows store their keys unaligned, so both strings are loaded byte-wise
static inline sljit_uw SLJIT_FUNC SljitHashJoinStringKeyHash(const string_t *value) {
	return Hash(Load<string_t>(const_data_ptr_cast(value)));
}

static inline sljit_sw SLJIT_FUNC SljitHashJoinStringKeysEqual(const string_t *source, const string_t *row) {
	return Load<string_t>(const_data_ptr_cast(source)) == Load<string_t>(const_data_ptr_cast(row));
}

//! Points `address` at the string_t of the probe row's key; `offset` must not hold the source data pointer
static inline void EmitHashJoinStringKeyAddress(struct sljit_compiler *compiler, sljit_s32 address, sljit_s32 offset,
                                                sljit_s32 source_data, sljit_s32 source_index) {
	D_ASSERT(offset != source_data);
	sljit_emit_op2(compiler, SLJIT_SHL, offset, 0, source_index, 0, SLJIT_IMM, 4);
	sljit_emit_op2(compiler, SLJIT_ADD, address, 0, source_data, 0, offset, 0);
}

static inline void EmitMixHashJoinStringKeyBlock(struct sljit_compiler *compiler, sljit_s32 load_op, sljit_s32 hash_reg,
                                                 sljit_s32 address, sljit_sw offset, sljit_s32 scratch,
                                                 sljit_s32 multiplier_reg) {
	sljit_emit_op1(compiler, load_op, scratch, 0, SLJIT_MEM1(address), offset);
	sljit_emit_op2(compiler, SLJIT_XOR, hash_reg, 0, hash_reg, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, hash_reg, scratch, multiplier_reg);
}

//! Hashes the probe row's string key exactly as DuckDB's Hash(string_t) does. An inlined string mixes its length and
//! its zero-padded inline bytes here; a longer string calls DuckDB's hash. R0 is the address register: the hash phase
//! holds nothing in it, and the call preserves R1 and R3 (the hash of the preceding keys) through the spill area.
static inline void EmitHashJoinStringKeyHash(struct sljit_compiler *compiler, sljit_s32 hash_reg, sljit_s32 source_data,
                                             sljit_s32 source_index, sljit_s32 scratch, sljit_s32 multiplier_reg) {
	constexpr auto prefix_offset = static_cast<sljit_sw>(sizeof(uint32_t));
	constexpr auto block_size = static_cast<sljit_sw>(sizeof(hash_t));
	EmitHashJoinStringKeyAddress(compiler, SLJIT_R0, scratch, source_data, source_index);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(SLJIT_R0), 0);
	auto long_string =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, scratch, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_MUL, hash_reg, 0, scratch, 0, SLJIT_IMM,
	               static_cast<sljit_sw>(0xc6a4a7935bd1e995ULL));
	sljit_emit_op2(compiler, SLJIT_XOR, hash_reg, 0, hash_reg, 0, SLJIT_IMM, 0xe17a1465);
	auto empty = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	auto one_block = sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, scratch, 0, SLJIT_IMM, block_size);
	EmitMixHashJoinStringKeyBlock(compiler, SLJIT_MOV, hash_reg, SLJIT_R0, prefix_offset, scratch, multiplier_reg);
	EmitMixHashJoinStringKeyBlock(compiler, SLJIT_MOV_U32, hash_reg, SLJIT_R0, prefix_offset + block_size, scratch,
	                              multiplier_reg);
	auto blocks_mixed = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(one_block, sljit_emit_label(compiler));
	EmitMixHashJoinStringKeyBlock(compiler, SLJIT_MOV, hash_reg, SLJIT_R0, prefix_offset, scratch, multiplier_reg);
	auto finalize = sljit_emit_label(compiler);
	sljit_set_label(empty, finalize);
	sljit_set_label(blocks_mixed, finalize);
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
	auto hashed = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(long_string, sljit_emit_label(compiler));
	EmitSpillHashJoinProbeStringCallState(compiler);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1(W, P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitHashJoinStringKeyHash));
	sljit_emit_op1(compiler, SLJIT_MOV, hash_reg, 0, SLJIT_R0, 0);
	EmitRestoreHashJoinProbeStringCallState(compiler);
	sljit_set_label(hashed, sljit_emit_label(compiler));
}

//! Compares the probe row's string key with the build row's (R0) as string_t equality does: the length and prefix
//! word, then the inline word or pointer; differing words of long strings fall back to comparing their bytes.
static inline void EmitHashJoinStringKeyMismatch(struct sljit_compiler *compiler, sljit_sw key_layout_offset,
                                                 vector<struct sljit_jump *> &equality_key_mismatches,
                                                 sljit_s32 source_data, sljit_s32 source_index) {
	constexpr auto word_size = static_cast<sljit_sw>(sizeof(hash_t));
	EmitHashJoinStringKeyAddress(compiler, SLJIT_R2, SLJIT_R2, source_data, source_index);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R2), 0);
	equality_key_mismatches.push_back(
	    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), key_layout_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R2), word_size);
	auto words_equal =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), key_layout_offset + word_size);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R2), 0);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, SLJIT_R4, 0, SLJIT_IMM,
	                                                 NumericCast<sljit_sw>(string_t::INLINE_LENGTH)));
	EmitSpillHashJoinProbeStringCallState(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R0, 0, SLJIT_IMM, key_layout_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R2, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS2(W, P, P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitHashJoinStringKeysEqual));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_R0, 0);
	EmitRestoreHashJoinProbeStringCallState(compiler);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
	sljit_set_label(words_equal, sljit_emit_label(compiler));
}

static inline void EmitHashJoinKeyHashFromSourceData(struct sljit_compiler *compiler, idx_t key_idx,
                                                     SljitNativeHashJoinKeyKind key_kind, sljit_s32 hash_reg,
                                                     sljit_s32 source_data, sljit_s32 source_index, sljit_s32 scratch,
                                                     sljit_s32 multiplier_reg = 0) {
	if (key_kind == SljitNativeHashJoinKeyKind::STRING) {
		EmitHashJoinStringKeyHash(compiler, hash_reg, source_data, source_index, scratch, multiplier_reg);
		return;
	}
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinSourceKey(compiler, key_idx, key_kind, hash_reg, source_data, source_index, 0, scratch);
		EmitHashJoinKeyHash(compiler, key_kind, hash_reg, scratch, multiplier_reg);
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, hash_reg, source_data, source_index, offsetof(hugeint_t, lower), scratch);
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
	EmitLoadHashJoinKeyWord(compiler, scratch, source_data, source_index, offsetof(hugeint_t, upper), scratch);
	// Keep the hoisted source pointer live across rows. Using it as the hash scratch register corrupts the
	// saved source-data register and turns the next probe into an invalid load.
	EmitDuckDBMurmurHash64(compiler, scratch, SLJIT_TMP_R0, multiplier_reg);
	sljit_emit_op2(compiler, SLJIT_XOR, hash_reg, 0, hash_reg, 0, scratch, 0);
}

static inline void EmitHashJoinEqualityKeyMismatch(struct sljit_compiler *compiler, idx_t key_idx,
                                                   SljitNativeHashJoinKeyKind key_kind, sljit_sw key_layout_offset,
                                                   vector<struct sljit_jump *> &equality_key_mismatches,
                                                   const vector<sljit_s32> &source_data_regs,
                                                   sljit_s32 source_index_reg) {
	auto source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	if (key_kind == SljitNativeHashJoinKeyKind::STRING) {
		EmitHashJoinStringKeyMismatch(compiler, key_layout_offset, equality_key_mismatches, source_data_reg,
		                              source_index_reg);
		return;
	}
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinSourceKey(compiler, key_idx, key_kind, SLJIT_R2, source_data_reg, source_index_reg, 0, SLJIT_R3,
		                          false);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, key_layout_offset);
		EmitLoadHashJoinKey(compiler, key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
		equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, lower),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, lower)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));

	source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, upper),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, upper)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
}

} // namespace duckdb

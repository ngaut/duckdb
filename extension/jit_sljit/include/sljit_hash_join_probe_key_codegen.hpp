//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_key_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/exception.hpp"

#include "sljitLir.h"

namespace duckdb {

static inline sljit_s32 SljitHashJoinKeyLoadOp(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return SLJIT_MOV_S8;
	case SljitNativeHashJoinKeyKind::INT16:
		return SLJIT_MOV_S16;
	case SljitNativeHashJoinKeyKind::INT32:
		return SLJIT_MOV_S32;
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::UINT64:
		return SLJIT_MOV;
	case SljitNativeHashJoinKeyKind::INT128:
	case SljitNativeHashJoinKeyKind::UINT128:
		throw InternalException("128-bit SLJIT native hash join keys are loaded by word");
	case SljitNativeHashJoinKeyKind::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeHashJoinKeyKind::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeHashJoinKeyKind::UINT32:
		return SLJIT_MOV_U32;
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static inline sljit_sw SljitHashJoinKeyScale(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
	case SljitNativeHashJoinKeyKind::UINT8:
		return 0;
	case SljitNativeHashJoinKeyKind::INT16:
	case SljitNativeHashJoinKeyKind::UINT16:
		return 1;
	case SljitNativeHashJoinKeyKind::INT32:
	case SljitNativeHashJoinKeyKind::UINT32:
		return 2;
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::UINT64:
		return 3;
	case SljitNativeHashJoinKeyKind::INT128:
	case SljitNativeHashJoinKeyKind::UINT128:
		throw InternalException("128-bit SLJIT native hash join keys are indexed by generated address arithmetic");
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static inline bool SljitHashJoinKeyKindIs128(SljitNativeHashJoinKeyKind kind) {
	return kind == SljitNativeHashJoinKeyKind::INT128 || kind == SljitNativeHashJoinKeyKind::UINT128;
}

static inline bool SljitHashJoinKeyHashesAsUInt32(SljitNativeHashJoinKeyKind kind) {
	return kind != SljitNativeHashJoinKeyKind::INT64 && kind != SljitNativeHashJoinKeyKind::UINT64 &&
	       !SljitHashJoinKeyKindIs128(kind);
}

static inline bool SljitHashJoinKeyKindIsSigned(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
	case SljitNativeHashJoinKeyKind::INT16:
	case SljitNativeHashJoinKeyKind::INT32:
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::INT128:
		return true;
	default:
		return false;
	}
}

static inline sljit_s32 SljitHashJoinPredicateMismatchComparison(ExecutionRegionComparisonType comparison_type,
                                                                 SljitNativeHashJoinKeyKind key_kind) {
	const bool signed_compare = SljitHashJoinKeyKindIsSigned(key_kind);
	switch (comparison_type) {
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return SLJIT_EQUAL;
	case ExecutionRegionComparisonType::LESS_THAN:
		return signed_compare ? SLJIT_SIG_GREATER_EQUAL : SLJIT_GREATER_EQUAL;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return signed_compare ? SLJIT_SIG_LESS_EQUAL : SLJIT_LESS_EQUAL;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return signed_compare ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return signed_compare ? SLJIT_SIG_LESS : SLJIT_LESS;
	default:
		throw InternalException("Unknown SLJIT hash join match predicate");
	}
}

static inline sljit_sw DuckDBMurmurHashMultiplierImmediate() {
	return static_cast<sljit_sw>(0xd6e8feb86659fd93ULL);
}

static inline sljit_sw DuckDBNullHashImmediate() {
	return static_cast<sljit_sw>(0xbf58476d1ce4e5b9ULL);
}

static inline void EmitDuckDBMurmurMultiply(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                            sljit_s32 multiplier_reg) {
	if (multiplier_reg != 0) {
		sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, multiplier_reg, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_IMM, DuckDBMurmurHashMultiplierImmediate());
	sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, scratch, 0);
}

static inline void EmitDuckDBMurmurHash64(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                          sljit_s32 multiplier_reg = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, target, scratch, multiplier_reg);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, target, scratch, multiplier_reg);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
}

static inline void EmitLoadHashJoinKey(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind kind,
                                       sljit_s32 target, sljit_s32 base, sljit_s32 index, sljit_sw offset) {
	if (SljitHashJoinKeyKindIs128(kind)) {
		throw InternalException("128-bit SLJIT native hash join keys are loaded by word");
	}
	auto load_op = SljitHashJoinKeyLoadOp(kind);
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM1(base), offset);
		return;
	}
	if (offset == 0) {
		sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(base, index), SljitHashJoinKeyScale(kind));
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index, 0, SLJIT_IMM, SljitHashJoinKeyScale(kind));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, offset);
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(base, SLJIT_R4), 0);
}

static inline void EmitLoadHashJoinKeyWord(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 base,
                                           sljit_s32 index, sljit_sw offset, sljit_s32 scratch) {
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(base), offset);
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index, 0, SLJIT_IMM, 4);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, scratch), 0);
}

static inline void EmitHashJoinKeyHash(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind key_kind,
                                       sljit_s32 hash_reg, sljit_s32 scratch, sljit_s32 multiplier_reg = 0) {
	if (SljitHashJoinKeyHashesAsUInt32(key_kind)) {
		sljit_emit_op2(compiler, SLJIT_AND, hash_reg, 0, hash_reg, 0, SLJIT_IMM, 0xffffffffULL);
	}
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
}

static inline void EmitDuckDBCombineHashScalar(struct sljit_compiler *compiler, sljit_s32 current_hash,
                                               sljit_s32 other_hash, sljit_s32 scratch,
                                               sljit_s32 multiplier_reg = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, current_hash, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, current_hash, 0, current_hash, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, current_hash, scratch, multiplier_reg);
	sljit_emit_op2(compiler, SLJIT_XOR, current_hash, 0, current_hash, 0, other_hash, 0);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_arm64_neon_integer_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

static inline bool SljitArm64NeonIntegerBinarySupported(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	if (!sljit_has_cpu_feature(SLJIT_HAS_SIMD)) {
		return false;
	}
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return op == SljitNativeIntegerBinaryOp::ADD || op == SljitNativeIntegerBinaryOp::SUBTRACT ||
		       op == SljitNativeIntegerBinaryOp::MULTIPLY;
	case SljitNativeIntegerKind::INT64:
		return op == SljitNativeIntegerBinaryOp::ADD || op == SljitNativeIntegerBinaryOp::SUBTRACT;
	default:
		return false;
	}
#else
	return false;
#endif
}

static inline sljit_s32 SljitArm64NeonIntegerSimdType(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_32;
	case SljitNativeIntegerKind::INT64:
		return SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_64;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
}

static inline idx_t SljitArm64NeonIntegerLaneCount(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return 4;
	case SljitNativeIntegerKind::INT64:
		return 2;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
}

static inline uint32_t SljitArm64NeonIntegerBinaryInstruction(SljitNativeIntegerKind kind,
                                                              SljitNativeIntegerBinaryOp op, uint32_t dst,
                                                              uint32_t left, uint32_t right) {
	uint32_t base;
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			base = 0x4ea08400;
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			base = 0x6ea08400;
			break;
		case SljitNativeIntegerBinaryOp::MULTIPLY:
			base = 0x4ea09c00;
			break;
		default:
			throw InternalException("Unsupported ARM64 NEON int32 binary op");
		}
		break;
	case SljitNativeIntegerKind::INT64:
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			base = 0x4ee08400;
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			base = 0x6ee08400;
			break;
		default:
			throw InternalException("Unsupported ARM64 NEON int64 binary op");
		}
		break;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
	return base | (right << 16) | (left << 5) | dst;
}

static inline void EmitSljitArm64NeonIntegerBinary(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                                   SljitNativeIntegerBinaryOp op, sljit_s32 dst_vreg,
                                                   sljit_s32 left_vreg, sljit_s32 right_vreg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, dst_vreg);
	auto left = sljit_get_register_index(SLJIT_SIMD_REG_128, left_vreg);
	auto right = sljit_get_register_index(SLJIT_SIMD_REG_128, right_vreg);
	if (dst < 0 || left < 0 || right < 0) {
		throw InternalException("SLJIT ARM64 NEON register mapping is unavailable");
	}
	auto instruction =
	    SljitArm64NeonIntegerBinaryInstruction(kind, op, UnsafeNumericCast<uint32_t>(dst),
	                                           UnsafeNumericCast<uint32_t>(left), UnsafeNumericCast<uint32_t>(right));
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_arm64_neon_integer_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"
#include "sljit_platform.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

static inline bool SljitArm64NeonIntegerBinarySupported(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	const auto &capabilities = GetSljitTargetCapabilities();
	if (!capabilities.IsArm64() || !capabilities.simd_available) {
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

static inline sljit_s32 SljitArm64NeonIntegerBinarySimdOp2(SljitNativeIntegerBinaryOp op) {
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return SLJIT_SIMD_OP2_ADD;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return SLJIT_SIMD_OP2_SUB;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return SLJIT_SIMD_OP2_MUL;
	default:
		throw InternalException("Unsupported SLJIT SIMD integer binary op");
	}
}

static inline void EmitSljitArm64NeonIntegerBinary(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                                   SljitNativeIntegerBinaryOp op, sljit_s32 dst_vreg,
                                                   sljit_s32 left_vreg, sljit_s32 right_vreg) {
	auto type = SljitArm64NeonIntegerSimdType(kind) | SljitArm64NeonIntegerBinarySimdOp2(op);
	if (sljit_emit_simd_op2(compiler, type, dst_vreg, left_vreg, right_vreg, 0) != SLJIT_SUCCESS) {
		throw InternalException("SLJIT SIMD integer binary emission failed");
	}
}

} // namespace duckdb

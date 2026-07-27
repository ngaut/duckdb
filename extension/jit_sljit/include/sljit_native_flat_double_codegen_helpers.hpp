#pragma once

#include "sljit_native_flat_loop_codegen.hpp"
#include "sljit_native_double_source_helpers.hpp"
#include "sljit_native_types.hpp"
#include "sljit_platform.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

static inline bool SljitArm64NeonFloatingBinarySupported(SljitNativeDoubleBinaryOp op) {
	const auto &capabilities = GetSljitTargetCapabilities();
	if (!capabilities.IsArm64() || !capabilities.simd_available) {
		return false;
	}
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
	case SljitNativeDoubleBinaryOp::SUBTRACT:
	case SljitNativeDoubleBinaryOp::MULTIPLY:
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return true;
	default:
		return false;
	}
}

static inline sljit_s32 SljitArm64NeonFloatingSimdType(bool single_precision) {
	return SLJIT_SIMD_REG_128 | SLJIT_SIMD_FLOAT | (single_precision ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
}

static inline idx_t SljitArm64NeonFloatingLaneCount(bool single_precision) {
	return single_precision ? 4 : 2;
}

static inline uint32_t SljitArm64NeonFloatingBinaryInstruction(bool single_precision, SljitNativeDoubleBinaryOp op,
                                                               uint32_t dst, uint32_t left, uint32_t right) {
	uint32_t base;
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		base = single_precision ? 0x4e20d400 : 0x4e60d400;
		break;
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		base = single_precision ? 0x4ea0d400 : 0x4ee0d400;
		break;
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		base = single_precision ? 0x6e20dc00 : 0x6e60dc00;
		break;
	case SljitNativeDoubleBinaryOp::DIVIDE:
		base = single_precision ? 0x6e20fc00 : 0x6e60fc00;
		break;
	default:
		throw InternalException("Unsupported ARM64 NEON floating binary op");
	}
	return base | (right << 16) | (left << 5) | dst;
}

static inline void EmitSljitArm64NeonFloatingBinary(struct sljit_compiler *compiler, bool single_precision,
                                                    SljitNativeDoubleBinaryOp op, sljit_s32 dst_vreg,
                                                    sljit_s32 left_vreg, sljit_s32 right_vreg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, dst_vreg);
	auto left = sljit_get_register_index(SLJIT_SIMD_REG_128, left_vreg);
	auto right = sljit_get_register_index(SLJIT_SIMD_REG_128, right_vreg);
	if (dst < 0 || left < 0 || right < 0) {
		throw InternalException("SLJIT ARM64 NEON register mapping is unavailable");
	}
	auto instruction =
	    SljitArm64NeonFloatingBinaryInstruction(single_precision, op, UnsafeNumericCast<uint32_t>(dst),
	                                            UnsafeNumericCast<uint32_t>(left), UnsafeNumericCast<uint32_t>(right));
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

static inline sljit_s32 NativeDirectFloatingMoveOp(bool single_precision) {
	return single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
}

static inline sljit_s32 NativeDirectFloatingMemoryAlignment(bool single_precision) {
	return single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
}

static inline sljit_sw NativeDirectFloatingDataScale(bool single_precision) {
	return single_precision ? 2 : 3;
}

static inline sljit_sw NativeDirectFloatingDataWidth(bool single_precision) {
	return single_precision ? NumericCast<sljit_sw>(sizeof(float)) : NumericCast<sljit_sw>(sizeof(double));
}

static inline bool ValidateNativeFlatDoubleBinarySource(SljitNativeDoubleSourceKind kind, bool single_precision,
                                                        string &error) {
	if (!IsDirectNativeFloatingSource(kind)) {
		error = "SLJIT flat floating binary fast path only supports direct FLOAT/DOUBLE sources";
		return false;
	}
	if (single_precision != (kind == SljitNativeDoubleSourceKind::FLOAT)) {
		error = "SLJIT flat floating binary fast path source type does not match result precision";
		return false;
	}
	return true;
}

} // namespace duckdb

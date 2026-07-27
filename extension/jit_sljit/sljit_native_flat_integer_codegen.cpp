#include "sljit_native_codegen.hpp"

#include "sljit_arm64_neon_integer_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_flat_loop_codegen.hpp"

#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                          bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	auto use_simd = SljitArm64NeonIntegerBinarySupported(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), use_simd ? 5 | SLJIT_ENTER_VECTOR(3) : 5, 6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));

	auto emit_integer_constant_op = [&]() {
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			if (constant_on_left) {
				sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_S5, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			}
			break;
		case SljitNativeIntegerBinaryOp::MULTIPLY:
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			break;
		default:
			throw InternalException("Unknown SLJIT native integer binary operator");
		}
	};
	auto emit_integer_constant_row = [&](sljit_sw offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3), offset);
		emit_integer_constant_op();
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(SLJIT_S4), offset, SLJIT_R2, 0);
	};
	auto emit_integer_constant_advance = [&](sljit_sw byte_count) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, byte_count);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, byte_count);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonIntegerSimdType(kind);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(kind));
		sljit_emit_simd_replicate(compiler, simd_type, SLJIT_VR1, SLJIT_S5, 0);

		auto emit_vector_row = [&]() {
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
			if (op == SljitNativeIntegerBinaryOp::SUBTRACT && constant_on_left) {
				EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR1, SLJIT_VR0);
			} else {
				EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
			}
			sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S4), 0);
		};
		auto emit_scalar_row = [&]() {
			emit_integer_constant_row(0);
		};
		EmitSljitFlatSimdThenScalarTailLoop(compiler, SLJIT_S1, simd_lanes, 16, data_width, emit_vector_row,
		                                    emit_scalar_row, emit_integer_constant_advance);
	} else {
		EmitSljitFlatUnrolledScalarLoop(compiler, SLJIT_S1, data_width, 4, emit_integer_constant_row,
		                                emit_integer_constant_advance);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFlatIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                                  SljitNativeIntegerBinaryOp op,
                                                                                  SljitNativeVectorFunction &function,
                                                                                  string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	auto use_simd = SljitArm64NeonIntegerBinarySupported(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), use_simd ? 5 | SLJIT_ENTER_VECTOR(3) : 5, 6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));

	auto emit_integer_reference_row = [&](sljit_sw offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3), offset);
		sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S4), offset);
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(SLJIT_S5), offset, SLJIT_R2, 0);
	};
	auto emit_integer_reference_advance = [&](sljit_sw byte_count) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, byte_count);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, byte_count);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, byte_count);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonIntegerSimdType(kind);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(kind));

		auto emit_vector_row = [&]() {
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR1, SLJIT_MEM1(SLJIT_S4), 0);
			EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
			sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S5), 0);
		};
		auto emit_scalar_row = [&]() {
			emit_integer_reference_row(0);
		};
		EmitSljitFlatSimdThenScalarTailLoop(compiler, SLJIT_S1, simd_lanes, 16, data_width, emit_vector_row,
		                                    emit_scalar_row, emit_integer_reference_advance);
	} else {
		EmitSljitFlatUnrolledScalarLoop(compiler, SLJIT_S1, data_width, 4, emit_integer_reference_row,
		                                emit_integer_reference_advance);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

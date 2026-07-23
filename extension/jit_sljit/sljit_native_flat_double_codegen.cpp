#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_flat_double_codegen_helpers.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryConstant(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind source_kind,
                                         bool constant_on_left, bool single_precision,
                                         SljitNativeVectorFunction &function, string &error) {
	if (!ValidateNativeFlatDoubleBinarySource(source_kind, single_precision, error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_width = NativeDirectFloatingDataWidth(single_precision);
	auto use_simd = SljitArm64NeonFloatingBinarySupported(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2) | (use_simd ? SLJIT_ENTER_VECTOR(3) : 0), 5,
	                 0);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_S0),
	                offsetof(SljitNativeVectorInput, double_constant));
	if (single_precision) {
		sljit_emit_fop1(compiler, SLJIT_CONV_F32_FROM_F64, SLJIT_FR1, 0, SLJIT_FR1, 0);
	}

	auto emit_row = [&]() {
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_S3), 0);
		if (constant_on_left) {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR1, 0, SLJIT_FR0, 0);
		} else {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
		}
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_S4), 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonFloatingSimdType(single_precision);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonFloatingLaneCount(single_precision));
		sljit_emit_simd_replicate(compiler, simd_type, SLJIT_VR1, SLJIT_FR1, 0);

		auto emit_vector_row = [&]() {
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
			if (constant_on_left) {
				EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR1, SLJIT_VR0);
			} else {
				EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
			}
			sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S4), 0);
		};
		auto emit_advance = [&](sljit_sw byte_count) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, byte_count);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, byte_count);
		};
		EmitSljitFlatSimdThenScalarTailLoop(compiler, SLJIT_S2, simd_lanes, 16, data_width, emit_vector_row, emit_row,
		                                    emit_advance);
	} else {
		EmitSljitFlatCountedScalarLoop(compiler, emit_row, [&]() {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		});
	}

	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                           SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                           SljitNativeVectorFunction &function, string &error) {
	if (!ValidateNativeFlatDoubleBinarySource(left_kind, single_precision, error) ||
	    !ValidateNativeFlatDoubleBinarySource(right_kind, single_precision, error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_width = NativeDirectFloatingDataWidth(single_precision);
	auto use_simd = SljitArm64NeonFloatingBinarySupported(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2) | (use_simd ? SLJIT_ENTER_VECTOR(3) : 0), 6,
	                 0);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));

	auto emit_row = [&]() {
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_S3), 0);
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR1, SLJIT_MEM1(SLJIT_S4), 0);
		sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_S5), 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonFloatingSimdType(single_precision);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonFloatingLaneCount(single_precision));

		auto emit_vector_row = [&]() {
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
			sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR1, SLJIT_MEM1(SLJIT_S4), 0);
			EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
			sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S5), 0);
		};
		auto emit_advance = [&](sljit_sw byte_count) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, byte_count);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, byte_count);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, byte_count);
		};
		EmitSljitFlatSimdThenScalarTailLoop(compiler, SLJIT_S2, simd_lanes, 16, data_width, emit_vector_row, emit_row,
		                                    emit_advance);
	} else {
		EmitSljitFlatCountedScalarLoop(compiler, emit_row, [&]() {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
		});
	}

	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

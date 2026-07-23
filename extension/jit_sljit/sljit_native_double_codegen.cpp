#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

double SLJIT_FUNC SljitNativeHugeintToDouble(uint64_t lower, int64_t upper) {
	hugeint_t value;
	value.lower = lower;
	value.upper = upper;
	return Hugeint::Cast<double>(value);
}

sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op, bool single_precision) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		return single_precision ? SLJIT_ADD_F32 : SLJIT_ADD_F64;
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		return single_precision ? SLJIT_SUB_F32 : SLJIT_SUB_F64;
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		return single_precision ? SLJIT_MUL_F32 : SLJIT_MUL_F64;
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return single_precision ? SLJIT_DIV_F32 : SLJIT_DIV_F64;
	default:
		throw InternalException("Unknown SLJIT native double binary operator");
	}
}

bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::INT128_TO_DOUBLE ||
	       kind == SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
}

bool NativeDoubleSourceHasDecimalScale(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE ||
	       kind == SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
}

static void EmitScaleNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                         sljit_sw scale_offset, sljit_s32 target) {
	if (!NativeDoubleSourceHasDecimalScale(kind)) {
		return;
	}
	sljit_emit_fop2(compiler, SLJIT_DIV_F64, target, 0, target, 0, SLJIT_MEM1(SLJIT_S0), scale_offset);
}

void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                 sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset, sljit_s32 target,
                                 bool single_precision) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), data_offset);
	switch (kind) {
	case SljitNativeDoubleSourceKind::FLOAT:
		sljit_emit_fmem(compiler, SLJIT_MOV_F32 | SLJIT_MEM_ALIGNED_16, target, SLJIT_MEM2(SLJIT_R0, index_reg), 2);
		return;
	case SljitNativeDoubleSourceKind::DOUBLE:
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		return;
	case SljitNativeDoubleSourceKind::INT64_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		sljit_emit_fop1(compiler, single_precision ? SLJIT_CONV_F32_FROM_SW : SLJIT_CONV_F64_FROM_SW, target, 0,
		                SLJIT_R2, 0);
		EmitScaleNativeDoubleOperand(compiler, kind, scale_offset, target);
		return;
	case SljitNativeDoubleSourceKind::INT128_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE:
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R0, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, lower)));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, upper)));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS2(F64, W, W), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeHugeintToDouble));
		sljit_emit_fop1(compiler, SLJIT_MOV_F64, target, 0, SLJIT_RETURN_FREG, 0);
		EmitScaleNativeDoubleOperand(compiler, kind, scale_offset, target);
		return;
	default:
		throw InternalException("Unknown SLJIT native double source kind");
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                                           SljitNativeDoubleSourceKind source_kind,
                                                                           bool constant_on_left, bool single_precision,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
	auto fmem_align = single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
	auto result_data_scale = single_precision ? 2 : 3;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0, single_precision);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_S0),
		                offsetof(SljitNativeVectorInput, double_constant));
		if (single_precision) {
			sljit_emit_fop1(compiler, SLJIT_CONV_F32_FROM_F64, SLJIT_FR1, 0, SLJIT_FR1, 0);
		}
		if (constant_on_left) {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR1, 0, SLJIT_FR0, 0);
		} else {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
		}

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
		                result_data_scale);
	});
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                       SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                       SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
	auto fmem_align = single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
	auto result_data_scale = single_precision ? 2 : 3;
	auto needs_helper_spill = NativeDoubleSourceUsesHelper(left_kind) || NativeDoubleSourceUsesHelper(right_kind);
	auto spill_width = single_precision ? sizeof(float) : sizeof(double);
	sljit_sw left_spill_offset = 0;
	auto right_spill_offset = NumericCast<sljit_sw>(spill_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5,
	                 needs_helper_spill ? NumericCast<sljit_sw>(spill_width * 2) : 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitTwoSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		if (needs_helper_spill) {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0,
			                            single_precision);
			sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                left_spill_offset);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_double_scale),
			                            SLJIT_FR0, single_precision);
			sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                right_spill_offset);
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP), left_spill_offset);
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP), right_spill_offset);
		} else {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0,
			                            single_precision);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_double_scale),
			                            SLJIT_FR1, single_precision);
		}
		sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
		                result_data_scale);
	});
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

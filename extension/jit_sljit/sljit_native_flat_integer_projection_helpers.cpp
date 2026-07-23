#include "sljit_native_flat_integer_projection_codegen.hpp"

#include "sljit_codegen_internal.hpp"

#include "duckdb/common/exception.hpp"

#include <cstddef>

namespace duckdb {

bool ValidateNativeFlatIntegerProjectionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                   SljitNativeIntegerKind kind, string &error) {
	if (plan.integer_kind != kind) {
		error = "SLJIT flat integer projection cannot mix integer widths";
		return false;
	}
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		if (plan.check_arithmetic_overflow || plan.check_result_range) {
			error = "SLJIT flat integer projection only supports unchecked INTEGER expressions";
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT32) {
			error = "SLJIT flat integer projection result type does not match integer width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::INT64:
		if (plan.check_arithmetic_overflow || plan.check_result_range) {
			error = "SLJIT flat integer projection only supports unchecked BIGINT expressions";
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT64) {
			error = "SLJIT flat integer projection result type does not match integer width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::DECIMAL64:
		if (plan.return_type.id() != LogicalTypeId::DECIMAL || plan.return_type.InternalType() != PhysicalType::INT64) {
			error = "SLJIT flat integer projection DECIMAL64 result type does not match storage width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::DATE:
		if (plan.return_type.id() != LogicalTypeId::DATE || plan.return_type.InternalType() != PhysicalType::INT32) {
			error = "SLJIT flat integer projection DATE result type does not match storage width";
			return false;
		}
		break;
	default:
		error = "SLJIT flat integer projection only supports INTEGER/BIGINT/DECIMAL64/DATE result types";
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		error = "SLJIT flat integer projection only supports integer binary expressions";
		return false;
	}
}

sljit_s32 SljitFlatIntegerProjectionSourceVectorRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_VR0;
	case 1:
		return SLJIT_VR1;
	default:
		throw InternalException("SLJIT flat integer projection source vector register is out of range");
	}
}

sljit_s32 SljitFlatIntegerProjectionSourceScalarRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_R2;
	case 1:
		return SLJIT_R3;
	default:
		throw InternalException("SLJIT flat integer projection source scalar register is out of range");
	}
}

idx_t SljitFlatIntegerProjectionGroupSize() {
	static_assert(SLJIT_NUMBER_OF_SAVED_REGISTERS >= 6,
	              "flat integer projection requires five invariant registers and one result register");
	return MinValue<idx_t>(4, SLJIT_NUMBER_OF_SAVED_REGISTERS - 5);
}

sljit_s32 SljitFlatIntegerProjectionSavedRegisterCount() {
	return static_cast<sljit_s32>(5 + SljitFlatIntegerProjectionGroupSize());
}

sljit_s32 SljitFlatIntegerProjectionResultPointerRegister(idx_t group_idx) {
	switch (group_idx) {
	case 0:
		return SLJIT_S5;
	case 1:
		return SLJIT_S6;
	case 2:
		return SLJIT_S7;
	case 3:
		return SLJIT_S8;
	default:
		throw InternalException("SLJIT flat integer projection result register is out of range");
	}
}

void EmitSljitFlatIntegerProjectionOverflowReturns(struct sljit_compiler *compiler,
                                                   const vector<SljitFlatIntegerProjectionOverflowJump> &overflow_jumps,
                                                   vector<struct sljit_jump *> &overflow_returns) {
	for (auto &overflow_jump : overflow_jumps) {
		auto overflow_label = sljit_emit_label(compiler);
		sljit_set_label(overflow_jump.jump, overflow_label);
		auto overflow_message_offset = SljitPointerArrayOffset(overflow_jump.projection_index);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, overflow_messages));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), overflow_message_offset);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_message),
		               SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
		overflow_returns.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}
}

} // namespace duckdb

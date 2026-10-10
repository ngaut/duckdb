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
	const auto available_saved_registers = GetSljitTargetCapabilities().registers.MaxAddressableSavedRegisters(5);
	if (available_saved_registers <= 5) {
		throw InternalException("SLJIT flat integer projection register layout is unavailable");
	}
	return MinValue<idx_t>(4, NumericCast<idx_t>(available_saved_registers - 5));
}

sljit_s32 SljitFlatIntegerProjectionSavedRegisterCount() {
	return static_cast<sljit_s32>(5 + SljitFlatIntegerProjectionGroupSize());
}

sljit_s32 SljitFlatIntegerProjectionResultPointerRegister(idx_t group_idx) {
	if (group_idx >= SljitFlatIntegerProjectionGroupSize()) {
		throw InternalException("SLJIT flat integer projection result register is out of range");
	}
	return SljitSavedRegisterAt(NumericCast<sljit_s32>(5 + group_idx));
}

} // namespace duckdb

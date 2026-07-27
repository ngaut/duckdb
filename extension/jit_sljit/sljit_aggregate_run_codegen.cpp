//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_run_codegen.cpp
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_register_layout.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

bool SljitPrimitiveRunMachineWordSupported() {
	return GetSljitTargetCapabilities().Has64BitMachineWord();
}

bool SljitPrimitiveRunCodegenSupported() {
	return GetSljitPrimitiveRunRegisterLayout().supported;
}

static bool SljitPrimitiveRunGroupTypeSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

bool SljitPrimitiveRunGroupCastSupported(PhysicalType source_type, PhysicalType target_type,
                                         ExecutionRowPointerGroupKeyCastKind cast_kind) {
	switch (cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source_type == target_type && SljitPrimitiveRunGroupTypeSupported(source_type);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		return source_type == PhysicalType::INT64 && target_type == PhysicalType::INT32;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		return source_type == PhysicalType::INT64 && target_type == PhysicalType::INT16;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source_type == PhysicalType::INT32 && target_type == PhysicalType::INT8;
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS: {
		const bool signed_source = source_type == PhysicalType::INT8 || source_type == PhysicalType::INT16 ||
		                           source_type == PhysicalType::INT32 || source_type == PhysicalType::INT64;
		const bool unsigned_target = target_type == PhysicalType::UINT8 || target_type == PhysicalType::UINT16 ||
		                             target_type == PhysicalType::UINT32 || target_type == PhysicalType::UINT64;
		return signed_source && unsigned_target && GetTypeIdSize(source_type) > GetTypeIdSize(target_type);
	}
	default:
		return false;
	}
}

static bool SljitPrimitiveRunPayloadTypeSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		return false;
	}
}

bool SljitPrimitiveRunPayloadSupported(PhysicalType payload_type, AggregatePrimitiveUpdateKind primitive_kind,
                                       bool payload_nullable) {
	const bool needs_payload = primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                           primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	const bool payload_matches =
	    needs_payload ? SljitPrimitiveRunPayloadTypeSupported(payload_type) : payload_type == PhysicalType::INVALID;
	return payload_matches &&
	       (needs_payload || primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	        primitive_kind == AggregatePrimitiveUpdateKind::COUNT) &&
	       (!payload_nullable || primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR);
}

static bool SljitPrimitiveRunUsesBatchDelta(PhysicalType payload_type, AggregatePrimitiveUpdateKind primitive_kind) {
	if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		return true;
	}
	if (primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	    primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return false;
	}
	// The runtime ABI caps one invocation at STANDARD_VECTOR_SIZE rows. These
	// payload widths therefore have an exact signed int64_t batch delta.
	switch (payload_type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		return false;
	}
}

static bool SljitPrimitiveRunPhysicalTypeIsSigned(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static sljit_sw SljitPrimitiveRunPhysicalTypeScale(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return 0;
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return 1;
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return 2;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return 3;
	default:
		throw InternalException("Unsupported SLJIT primitive run physical type");
	}
}

static sljit_s32 SljitPrimitiveRunPhysicalTypeLoadOp(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return SLJIT_MOV_S8;
	case PhysicalType::INT16:
		return SLJIT_MOV_S16;
	case PhysicalType::INT32:
		return SLJIT_MOV_S32;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return SLJIT_MOV;
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
		return SLJIT_MOV_U8;
	case PhysicalType::UINT16:
		return SLJIT_MOV_U16;
	case PhysicalType::UINT32:
		return SLJIT_MOV_U32;
	default:
		throw InternalException("Unsupported SLJIT primitive run load type");
	}
}

static sljit_s32 SljitPrimitiveRunPhysicalTypeStoreOp(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return SLJIT_MOV_U8;
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return SLJIT_MOV_U16;
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return SLJIT_MOV32;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return SLJIT_MOV;
	default:
		throw InternalException("Unsupported SLJIT primitive run store type");
	}
}

static void EmitSljitPrimitiveRunLoadKey(struct sljit_compiler *compiler, PhysicalType group_source_type,
                                         sljit_s32 target) {
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_source_type), target, 0,
	               SLJIT_MEM2(SLJIT_S4, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(group_source_type));
}

static sljit_s32 EmitSljitPrimitiveRunOutputGroupData(struct sljit_compiler *compiler, sljit_s32 fallback) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_output_pointer_hoists) {
		return registers.output_group_data;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	return fallback;
}

static sljit_s32 EmitSljitPrimitiveRunOutputRowCounts(struct sljit_compiler *compiler, sljit_s32 fallback) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_output_pointer_hoists) {
		return registers.output_row_counts;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	return fallback;
}

static sljit_s32 EmitSljitPrimitiveRunOutputValues(struct sljit_compiler *compiler, sljit_s32 fallback,
                                                   AggregatePrimitiveUpdateKind primitive_kind) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_output_pointer_hoists) {
		return registers.output_values;
	}
	const auto offset = primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT
	                        ? offsetof(SljitNativePrimitiveRunInput, output_hugeint_values)
	                        : offsetof(SljitNativePrimitiveRunInput, output_int64_values);
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0), offset);
	return fallback;
}

static sljit_s32 EmitSljitPrimitiveRunOutputValueIsSet(struct sljit_compiler *compiler, sljit_s32 fallback) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_output_pointer_hoists) {
		return registers.output_value_is_set;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_value_is_set));
	return fallback;
}

static void EmitSljitPrimitiveRunRecordIncreasingTransition(struct sljit_compiler *compiler,
                                                            PhysicalType group_source_type) {
	const auto greater = SljitPrimitiveRunPhysicalTypeIsSigned(group_source_type) ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	auto increasing = sljit_emit_cmp(compiler, greater, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_groups_strictly_increasing), SLJIT_IMM, 0);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(increasing, done);
}

static void EmitSljitPrimitiveRunLoadExistingState(struct sljit_compiler *compiler, PhysicalType group_type,
                                                   ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                                   AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta) {
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_group_data = EmitSljitPrimitiveRunOutputGroupData(compiler, SLJIT_R6);
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_type), SLJIT_R0, 0,
	               SLJIT_MEM2(output_group_data, SLJIT_R5), SljitPrimitiveRunPhysicalTypeScale(group_type));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R6, 0);
	}
	if (use_batch_delta) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_S1, 0);
		return;
	}

	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper));
	} else {
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(output_values, SLJIT_R5), 3);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	}

	const auto output_row_counts = EmitSljitPrimitiveRunOutputRowCounts(compiler, SLJIT_R6);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3);
}

static void EmitSljitPrimitiveRunInitializeNullableOutput(struct sljit_compiler *compiler,
                                                          AggregatePrimitiveUpdateKind primitive_kind) {
	const auto output_row_counts = EmitSljitPrimitiveRunOutputRowCounts(compiler, SLJIT_R5);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_S3), 3, SLJIT_IMM, 0);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R6, 0, SLJIT_S3, 0, SLJIT_IMM, 4);
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R5, primitive_kind);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, output_values, 0, SLJIT_R6, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, lower), SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, upper), SLJIT_IMM, 0);
	} else {
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R5, primitive_kind);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_S3), 3, SLJIT_IMM, 0);
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		const auto output_value_is_set = EmitSljitPrimitiveRunOutputValueIsSet(compiler, SLJIT_R5);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_S3), 0, SLJIT_IMM, 0);
	}
}

static void EmitSljitPrimitiveRunStartGroup(struct sljit_compiler *compiler, PhysicalType group_type,
                                            ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                            AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta,
                                            bool payload_nullable) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	if (use_batch_delta) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_S1, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	}
	const auto output_group_data = EmitSljitPrimitiveRunOutputGroupData(compiler, SLJIT_R5);
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_R6, 0);
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type),
		               SLJIT_MEM2(output_group_data, SLJIT_S3), SljitPrimitiveRunPhysicalTypeScale(group_type),
		               SLJIT_R4, 0);
	} else {
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type),
		               SLJIT_MEM2(output_group_data, SLJIT_S3), SljitPrimitiveRunPhysicalTypeScale(group_type),
		               SLJIT_R0, 0);
	}
	if (payload_nullable) {
		EmitSljitPrimitiveRunInitializeNullableOutput(compiler, primitive_kind);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static sljit_jump *EmitJumpIfSljitPrimitiveRunPayloadNull(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_validity));
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R6, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM2(SLJIT_R5, SLJIT_R6), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R6, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R6, 0, SLJIT_IMM, 1, SLJIT_R6, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R6, 0, SLJIT_R6, 0, SLJIT_R5, 0);
	return sljit_emit_jump(compiler, SLJIT_EQUAL);
}

static void EmitSljitPrimitiveRunMarkValueSet(struct sljit_compiler *compiler) {
	const auto output_value_is_set = EmitSljitPrimitiveRunOutputValueIsSet(compiler, SLJIT_R5);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R6, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R6), 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunAccumulate(struct sljit_compiler *compiler, PhysicalType payload_type,
                                            AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta,
                                            bool payload_nullable) {
	D_ASSERT(!payload_nullable || !use_batch_delta);
	sljit_jump *payload_is_null = nullptr;
	if (payload_nullable) {
		payload_is_null = EmitJumpIfSljitPrimitiveRunPayloadNull(compiler);
	}
	if (use_batch_delta) {
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R4, 0,
			               SLJIT_MEM2(SLJIT_S5, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
		}
		return;
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	} else {
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R4, 0,
		               SLJIT_MEM2(SLJIT_S5, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			if (SljitPrimitiveRunPhysicalTypeIsSigned(payload_type)) {
				sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R5, 0, SLJIT_R4, 0, SLJIT_IMM, 63);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_IMM, 0);
			}
			sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
			sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_CARRY);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R5, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R6, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
		}
	}
	if (payload_nullable && (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                         primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		EmitSljitPrimitiveRunMarkValueSet(compiler);
	}
	if (payload_is_null) {
		sljit_set_label(payload_is_null, sljit_emit_label(compiler));
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunBatchDeltaState(struct sljit_compiler *compiler,
                                                 AggregatePrimitiveUpdateKind primitive_kind, bool merge_existing) {
	// R3 is the represented row delta and R1 is the aggregate delta. Only the
	// first run in an invocation can merge into the unpublished boundary group;
	// every group started by this kernel owns a fresh output slot.
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_row_counts = EmitSljitPrimitiveRunOutputRowCounts(compiler, SLJIT_R6);
	if (merge_existing) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3, SLJIT_R4, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3, SLJIT_R3, 0);
	}

	if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		if (merge_existing) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(output_values, SLJIT_R5), 3);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R4, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R3, 0);
		}
		return;
	}

	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R3, primitive_kind);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
		if (merge_existing) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper));
			sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R1, 0);
			sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_CARRY);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R2, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R6, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower), SLJIT_R4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper), SLJIT_R5, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower), SLJIT_R1, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper), SLJIT_R2, 0);
		}
	} else {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		if (merge_existing) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(output_values, SLJIT_R5), 3);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R1, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R4, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R1, 0);
		}
	}

	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_value_is_set = EmitSljitPrimitiveRunOutputValueIsSet(compiler, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunBatchDeltaFlush(struct sljit_compiler *compiler,
                                                 AggregatePrimitiveUpdateKind primitive_kind) {
	// A key-change flush runs after the next raw key has already been loaded into
	// R4. R0 no longer needs the completed key, so it carries the next key across
	// the flush temporaries for StartGroup.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	// R3 is the run's input start. Convert it to the represented row delta before
	// reusing the temporary registers.
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R3, 0, SLJIT_S1, 0, SLJIT_R3, 0);
	// The existing-state marker applies to the whole group. Split once so fresh
	// output slots avoid old-state loads and repeated marker branches.
	auto new_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunBatchDeltaState(compiler, primitive_kind, true);
	auto state_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto new_group_label = sljit_emit_label(compiler);
	sljit_set_label(new_group, new_group_label);
	EmitSljitPrimitiveRunBatchDeltaState(compiler, primitive_kind, false);
	auto state_done_label = sljit_emit_label(compiler);
	sljit_set_label(state_done, state_done_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_R0, 0);
}

static void EmitSljitPrimitiveRunFlush(struct sljit_compiler *compiler, AggregatePrimitiveUpdateKind primitive_kind,
                                       bool use_batch_delta, bool payload_nullable) {
	if (use_batch_delta) {
		EmitSljitPrimitiveRunBatchDeltaFlush(compiler, primitive_kind);
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower), SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper), SLJIT_R2, 0);
	} else {
		const auto output_values = EmitSljitPrimitiveRunOutputValues(compiler, SLJIT_R6, primitive_kind);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R1, 0);
	}
	if (!payload_nullable && (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                          primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_value_is_set = EmitSljitPrimitiveRunOutputValueIsSet(compiler, SLJIT_R6);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0, SLJIT_IMM, 1);
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_row_counts = EmitSljitPrimitiveRunOutputRowCounts(compiler, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3, SLJIT_R3, 0);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativePrimitiveRunUpdate(PhysicalType group_source_type, PhysicalType group_type,
                                   ExecutionRowPointerGroupKeyCastKind group_cast_kind, PhysicalType payload_type,
                                   AggregatePrimitiveUpdateKind primitive_kind, bool payload_nullable,
                                   SljitNativePrimitiveRunFunction &function, string &error) {
	function = nullptr;
	if (!SljitPrimitiveRunMachineWordSupported()) {
		error = "unsupported SLJIT primitive run update on a 32-bit machine word";
		return nullptr;
	}
	if (!SljitPrimitiveRunGroupCastSupported(group_source_type, group_type, group_cast_kind)) {
		error = "unsupported SLJIT primitive run group cast";
		return nullptr;
	}
	const bool needs_payload = primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                           primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	const bool use_batch_delta = !payload_nullable && SljitPrimitiveRunUsesBatchDelta(payload_type, primitive_kind);
	if (!SljitPrimitiveRunPayloadSupported(payload_type, primitive_kind, payload_nullable)) {
		error = "unsupported SLJIT primitive run aggregate payload";
		return nullptr;
	}
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (!registers.supported) {
		error = "unsupported SLJIT primitive run register layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, registers.saved_register_count, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, group_data));
	if (needs_payload) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, payload_data));
	}
	if (registers.has_output_pointer_hoists) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.output_group_data, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_group_data));
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.output_row_counts, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
		const auto output_values_offset = primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT
		                                      ? offsetof(SljitNativePrimitiveRunInput, output_hugeint_values)
		                                      : offsetof(SljitNativePrimitiveRunInput, output_int64_values);
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.output_values, 0, SLJIT_MEM1(SLJIT_S0), output_values_offset);
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, registers.output_value_is_set, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativePrimitiveRunInput, output_value_is_set));
		}
	}

	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunLoadExistingState(compiler, group_type, group_cast_kind, primitive_kind, use_batch_delta);
	auto initial_group_matches = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunStartGroup(compiler, group_type, group_cast_kind, primitive_kind, use_batch_delta,
	                                payload_nullable);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(initial_group_matches, accumulate_group);
	EmitSljitPrimitiveRunAccumulate(compiler, payload_type, primitive_kind, use_batch_delta, payload_nullable);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto run_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);
	EmitSljitPrimitiveRunFlush(compiler, primitive_kind, use_batch_delta, payload_nullable);
	auto next_group = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_group, append_group);

	auto flush_last_group = sljit_emit_label(compiler);
	sljit_set_label(run_done, flush_last_group);
	EmitSljitPrimitiveRunFlush(compiler, primitive_kind, use_batch_delta, payload_nullable);

	auto finish = sljit_emit_label(compiler);
	sljit_set_label(input_done, finish);
	sljit_set_label(output_full, finish);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

static constexpr idx_t SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET = 8;

static bool SljitPrimitiveRunLanesAreHomogeneous(const vector<PhysicalType> &payload_types,
                                                 const vector<AggregatePrimitiveUpdateKind> &primitive_kinds) {
	if (primitive_kinds.empty() || primitive_kinds.size() != payload_types.size()) {
		return false;
	}
	for (idx_t lane_idx = 1; lane_idx < primitive_kinds.size(); lane_idx++) {
		if (primitive_kinds[lane_idx] != primitive_kinds[0] || payload_types[lane_idx] != payload_types[0]) {
			return false;
		}
	}
	return true;
}

bool SljitPrimitiveRunMultiUpdateSupported(const vector<PhysicalType> &payload_types,
                                           const vector<AggregatePrimitiveUpdateKind> &primitive_kinds) {
	if (primitive_kinds.size() < 2 || primitive_kinds.size() != payload_types.size()) {
		return false;
	}
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		if (!SljitPrimitiveRunPayloadSupported(payload_types[lane_idx], primitive_kinds[lane_idx], false)) {
			return false;
		}
	}
	return primitive_kinds.size() <= SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET ||
	       SljitPrimitiveRunLanesAreHomogeneous(payload_types, primitive_kinds);
}

static void EmitSljitPrimitiveRunLoadLaneInput(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, lane_inputs));
	const auto lane_offset = static_cast<sljit_sw>(lane_idx * sizeof(SljitNativePrimitiveRunLaneInput));
	if (lane_offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_IMM, lane_offset);
	}
}

// A lane is addressed either by unrolled index (loaded into R6 before every helper
// because R6 doubles as scratch inside them) or by the homogeneous lane loop's
// persistent S5 cursor when lane_idx is INVALID_INDEX.
static sljit_s32 EmitSljitPrimitiveRunLaneBase(struct sljit_compiler *compiler, idx_t lane_idx) {
	if (lane_idx == DConstants::INVALID_INDEX) {
		return SLJIT_S5;
	}
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	return SLJIT_R6;
}

static void EmitSljitPrimitiveRunInitializeLane(struct sljit_compiler *compiler, idx_t lane_idx,
                                                AggregatePrimitiveUpdateKind primitive_kind) {
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(base),
		               offsetof(SljitNativePrimitiveRunLaneInput, output_hugeint_values));
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, lower), SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, upper), SLJIT_IMM, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(base),
		               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R5, SLJIT_S3), 3, SLJIT_IMM, 0);
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(base),
		               offsetof(SljitNativePrimitiveRunLaneInput, output_value_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R5, SLJIT_S3), 0, SLJIT_IMM, 0);
	}
}

static void EmitSljitPrimitiveRunMultiStartGroup(struct sljit_compiler *compiler, PhysicalType group_type,
                                                 ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                                 const vector<AggregatePrimitiveUpdateKind> &primitive_kinds) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R4, 0, SLJIT_R6, 0);
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R0, 0);
	} else {
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R4, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R5, SLJIT_S3), 3, SLJIT_IMM, 0);
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		EmitSljitPrimitiveRunInitializeLane(compiler, lane_idx, primitive_kinds[lane_idx]);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

struct SljitPrimitiveRunValidityJumps {
	sljit_jump *all_valid = nullptr;
	sljit_jump *is_null = nullptr;
};

static SljitPrimitiveRunValidityJumps EmitSljitPrimitiveRunValidityCheck(struct sljit_compiler *compiler,
                                                                         idx_t lane_idx) {
	SljitPrimitiveRunValidityJumps result;
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, payload_validity));
	result.all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R6, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R6), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R6, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R6, 0, SLJIT_IMM, 1, SLJIT_R6, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R6, 0, SLJIT_R6, 0, SLJIT_R4, 0);
	result.is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	return result;
}

static void EmitSljitPrimitiveRunCount(struct sljit_compiler *compiler, idx_t lane_idx) {
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3, SLJIT_R0, 0);
}

static void EmitSljitPrimitiveRunMarkValueSet(struct sljit_compiler *compiler, idx_t lane_idx) {
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_value_is_set));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, PhysicalType payload_type) {
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, payload_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R4, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3, SLJIT_R1, 0);
	EmitSljitPrimitiveRunMarkValueSet(compiler, lane_idx);
}

static void EmitSljitPrimitiveRunSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                            PhysicalType payload_type) {
	const auto base = EmitSljitPrimitiveRunLaneBase(compiler, lane_idx);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, payload_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R4, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	if (SljitPrimitiveRunPhysicalTypeIsSigned(payload_type)) {
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R1, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(base),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_hugeint_values));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R4), offsetof(hugeint_t, lower));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R4), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R0, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R4), offsetof(hugeint_t, lower), SLJIT_R5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R4), offsetof(hugeint_t, upper), SLJIT_R2, 0);
	EmitSljitPrimitiveRunMarkValueSet(compiler, lane_idx);
}

static void EmitSljitPrimitiveRunAccumulateLane(struct sljit_compiler *compiler, idx_t lane_idx,
                                                PhysicalType payload_type,
                                                AggregatePrimitiveUpdateKind primitive_kind) {
	SljitPrimitiveRunValidityJumps validity;
	if (primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		validity = EmitSljitPrimitiveRunValidityCheck(compiler, lane_idx);
	}
	auto accumulate = sljit_emit_label(compiler);
	if (validity.all_valid) {
		sljit_set_label(validity.all_valid, accumulate);
	}
	switch (primitive_kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
		EmitSljitPrimitiveRunCount(compiler, lane_idx);
		break;
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		EmitSljitPrimitiveRunSumInt64(compiler, lane_idx, payload_type);
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		EmitSljitPrimitiveRunSumHugeint(compiler, lane_idx, payload_type);
		break;
	default:
		throw InternalException("Unsupported SLJIT primitive run aggregate");
	}
	if (validity.is_null) {
		sljit_set_label(validity.is_null, sljit_emit_label(compiler));
	}
}

static void EmitSljitPrimitiveRunMultiIncrementRowCount(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3, SLJIT_R4, 0);
}

static void EmitSljitPrimitiveRunHomogeneousLaneLoopStart(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, lane_inputs));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
}

static void EmitSljitPrimitiveRunHomogeneousLaneLoopNext(struct sljit_compiler *compiler, sljit_label *loop,
                                                         idx_t lane_count) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM,
	               static_cast<sljit_sw>(sizeof(SljitNativePrimitiveRunLaneInput)));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R3, 0, SLJIT_IMM, static_cast<sljit_sw>(lane_count));
	sljit_set_label(repeat, loop);
}

static void EmitSljitPrimitiveRunHomogeneousStartGroup(struct sljit_compiler *compiler, PhysicalType group_type,
                                                       ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                                       AggregatePrimitiveUpdateKind primitive_kind, idx_t lane_count) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R4, 0, SLJIT_R6, 0);
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R0, 0);
	} else {
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R4, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R5, SLJIT_S3), 3, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunHomogeneousLaneLoopStart(compiler);
	auto lane_loop = sljit_emit_label(compiler);
	EmitSljitPrimitiveRunInitializeLane(compiler, DConstants::INVALID_INDEX, primitive_kind);
	EmitSljitPrimitiveRunHomogeneousLaneLoopNext(compiler, lane_loop, lane_count);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunHomogeneousAccumulateLanes(struct sljit_compiler *compiler, PhysicalType payload_type,
                                                            AggregatePrimitiveUpdateKind primitive_kind,
                                                            idx_t lane_count) {
	EmitSljitPrimitiveRunMultiIncrementRowCount(compiler);
	EmitSljitPrimitiveRunHomogeneousLaneLoopStart(compiler);
	auto lane_loop = sljit_emit_label(compiler);
	EmitSljitPrimitiveRunAccumulateLane(compiler, DConstants::INVALID_INDEX, payload_type, primitive_kind);
	EmitSljitPrimitiveRunHomogeneousLaneLoopNext(compiler, lane_loop, lane_count);
}

static unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePrimitiveRunHomogeneousMultiUpdate(
    PhysicalType group_source_type, PhysicalType group_type, ExecutionRowPointerGroupKeyCastKind group_cast_kind,
    PhysicalType payload_type, AggregatePrimitiveUpdateKind primitive_kind, idx_t lane_count,
    SljitNativePrimitiveRunFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, GetSljitPrimitiveRunRegisterLayout().base_saved_register_count,
	                 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, group_data));

	auto row_loop = sljit_emit_label(compiler);
	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R6, SLJIT_R5), SljitPrimitiveRunPhysicalTypeScale(group_type));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R6, 0);
	}
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunHomogeneousStartGroup(compiler, group_type, group_cast_kind, primitive_kind, lane_count);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunHomogeneousAccumulateLanes(compiler, payload_type, primitive_kind, lane_count);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto next_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_row, row_loop);

	auto finish = sljit_emit_label(compiler);
	sljit_set_label(input_done, finish);
	sljit_set_label(output_full, finish);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

static constexpr sljit_sw SLJIT_PRIMITIVE_RUN_AFFINE_VALUE_OFFSET = 0;
static constexpr sljit_sw SLJIT_PRIMITIVE_RUN_AFFINE_VALID_COUNT_OFFSET = sizeof(sljit_sw);

static sljit_sw SljitPrimitiveRunAffineLocalSize() {
	return GetSljitPrimitiveRunRegisterLayout().has_affine_accumulators ? 0 : 2 * sizeof(sljit_sw);
}

static void EmitSljitPrimitiveRunAffineReset(struct sljit_compiler *compiler) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_affine_accumulators) {
		sljit_emit_op1(compiler, SLJIT_MOV, registers.affine_value, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, registers.affine_valid_count, 0, SLJIT_IMM, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALUE_OFFSET, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALID_COUNT_OFFSET, SLJIT_IMM,
	               0);
}

static void EmitSljitPrimitiveRunAffineAccumulateValue(struct sljit_compiler *compiler, sljit_s32 value_reg) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_affine_accumulators) {
		sljit_emit_op2(compiler, SLJIT_ADD, registers.affine_value, 0, registers.affine_value, 0, value_reg, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALUE_OFFSET);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALUE_OFFSET, SLJIT_R4, 0);
}

static void EmitSljitPrimitiveRunAffineIncrementValidCount(struct sljit_compiler *compiler) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_affine_accumulators) {
		sljit_emit_op2(compiler, SLJIT_ADD, registers.affine_valid_count, 0, registers.affine_valid_count, 0, SLJIT_IMM,
		               1);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP),
	               SLJIT_PRIMITIVE_RUN_AFFINE_VALID_COUNT_OFFSET);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALID_COUNT_OFFSET, SLJIT_R4,
	               0);
}

static sljit_s32 EmitSljitPrimitiveRunAffineValueOperand(struct sljit_compiler *compiler) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_affine_accumulators) {
		return registers.affine_value;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_PRIMITIVE_RUN_AFFINE_VALUE_OFFSET);
	return SLJIT_R0;
}

static sljit_s32 EmitSljitPrimitiveRunAffineValidCountOperand(struct sljit_compiler *compiler) {
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (registers.has_affine_accumulators) {
		return registers.affine_valid_count;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SLJIT_PRIMITIVE_RUN_AFFINE_VALID_COUNT_OFFSET);
	return SLJIT_R0;
}

static void EmitSljitPrimitiveRunAffineStartFreshGroup(struct sljit_compiler *compiler, PhysicalType group_type,
                                                       ExecutionRowPointerGroupKeyCastKind group_cast_kind) {
	// R4 contains the raw source key. R2 distinguishes this fresh slot from a
	// pending boundary slot whose finalized affine deltas must be merged.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunAffineReset(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R6, 0, SLJIT_R0, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R3, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R6, 0);
	} else {
		sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R3, SLJIT_S3),
		               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R0, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunAffineStartExistingGroup(struct sljit_compiler *compiler) {
	// The previous invocation already finalized this output slot. Accumulate a
	// new shared-source delta and merge it exactly once at the next boundary.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
	EmitSljitPrimitiveRunAffineReset(compiler);
}

static sljit_jump *EmitJumpIfSljitPrimitiveRunAffinePayloadNull(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(all_valid, sljit_emit_label(compiler));
	return is_null;
}

static void EmitSljitPrimitiveRunAffineFinalize(struct sljit_compiler *compiler, bool payload_nullable) {
	// Keep the shared run representation compact. The append callback expands it
	// directly into aggregate states after group addresses become available.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto fresh_run = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, SLJIT_R5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_shared_int64_values));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
	auto value_operand = EmitSljitPrimitiveRunAffineValueOperand(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, value_operand, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, SLJIT_R5, 0);
	if (payload_nullable) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_shared_valid_counts));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
		auto valid_count_operand = EmitSljitPrimitiveRunAffineValidCountOperand(compiler);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, valid_count_operand, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, SLJIT_R5, 0);
	}
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(fresh_run, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_shared_int64_values));
	value_operand = EmitSljitPrimitiveRunAffineValueOperand(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, value_operand, 0);
	if (payload_nullable) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_shared_valid_counts));
		auto valid_count_operand = EmitSljitPrimitiveRunAffineValidCountOperand(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, valid_count_operand, 0);
	}
	sljit_set_label(done, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativePrimitiveRunAffineInt64Update(PhysicalType group_source_type, PhysicalType group_type,
                                              ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                              PhysicalType payload_type, idx_t lane_count, bool payload_nullable,
                                              SljitNativePrimitiveRunFunction &function, string &error) {
	function = nullptr;
	if (!SljitPrimitiveRunMachineWordSupported() || lane_count < 2 ||
	    !SljitPrimitiveRunPhysicalTypeIsSigned(payload_type) || payload_type == PhysicalType::INT64 ||
	    !SljitPrimitiveRunGroupCastSupported(group_source_type, group_type, group_cast_kind) ||
	    !SljitPrimitiveRunPayloadSupported(payload_type, AggregatePrimitiveUpdateKind::SUM_INT64, payload_nullable)) {
		error = "unsupported SLJIT affine int64 primitive run shape";
		return nullptr;
	}
	const auto &registers = GetSljitPrimitiveRunRegisterLayout();
	if (!registers.supported) {
		error = "unsupported SLJIT affine int64 primitive run register layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, registers.affine_saved_register_count,
	                 SljitPrimitiveRunAffineLocalSize());
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, group_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_data));

	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R6, SLJIT_R5), SljitPrimitiveRunPhysicalTypeScale(group_type));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R6, 0);
	}
	auto same_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunAffineStartFreshGroup(compiler, group_type, group_cast_kind);
	auto start_accumulating_fresh = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto start_existing_group = sljit_emit_label(compiler);
	sljit_set_label(same_existing_group, start_existing_group);
	EmitSljitPrimitiveRunAffineStartExistingGroup(compiler);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(start_accumulating_fresh, accumulate_group);
	sljit_jump *payload_is_null = nullptr;
	if (payload_nullable) {
		payload_is_null = EmitJumpIfSljitPrimitiveRunAffinePayloadNull(compiler);
	}
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R3, 0,
	               SLJIT_MEM2(SLJIT_S5, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	EmitSljitPrimitiveRunAffineAccumulateValue(compiler, SLJIT_R3);
	if (payload_nullable) {
		EmitSljitPrimitiveRunAffineIncrementValidCount(compiler);
	}
	if (payload_is_null) {
		sljit_set_label(payload_is_null, sljit_emit_label(compiler));
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto run_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);
	// Preserve the next raw key in S5 while finalization reuses every scratch
	// register. The payload pointer is rebound before the next row is consumed.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S5, 0, SLJIT_R4, 0);
	EmitSljitPrimitiveRunAffineFinalize(compiler, payload_nullable);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_S5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto transition_output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunAffineStartFreshGroup(compiler, group_type, group_cast_kind);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_data));
	auto next_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_row, accumulate_group);

	sljit_set_label(run_done, sljit_emit_label(compiler));
	EmitSljitPrimitiveRunAffineFinalize(compiler, payload_nullable);

	auto finish = sljit_emit_label(compiler);
	sljit_set_label(input_done, finish);
	sljit_set_label(output_full, finish);
	sljit_set_label(transition_output_full, finish);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePrimitiveRunMultiUpdate(
    PhysicalType group_source_type, PhysicalType group_type, ExecutionRowPointerGroupKeyCastKind group_cast_kind,
    const vector<PhysicalType> &payload_types, const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
    SljitNativePrimitiveRunFunction &function, string &error) {
	function = nullptr;
	if (!SljitPrimitiveRunMachineWordSupported()) {
		error = "unsupported SLJIT multi-lane primitive run update on a 32-bit machine word";
		return nullptr;
	}
	if (!SljitPrimitiveRunGroupCastSupported(group_source_type, group_type, group_cast_kind)) {
		error = "unsupported SLJIT multi-lane primitive run group cast";
		return nullptr;
	}
	if (!SljitPrimitiveRunMultiUpdateSupported(payload_types, primitive_kinds)) {
		error = "unsupported SLJIT multi-lane primitive run shape";
		return nullptr;
	}
	if (!GetSljitPrimitiveRunRegisterLayout().supported) {
		error = "unsupported SLJIT multi-lane primitive run register layout";
		return nullptr;
	}
	if (primitive_kinds.size() > SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET) {
		return BuildSljitNativePrimitiveRunHomogeneousMultiUpdate(group_source_type, group_type, group_cast_kind,
		                                                          payload_types[0], primitive_kinds[0],
		                                                          primitive_kinds.size(), function, error);
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, GetSljitPrimitiveRunRegisterLayout().base_saved_register_count,
	                 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, group_data));

	auto row_loop = sljit_emit_label(compiler);
	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R6, SLJIT_R5), SljitPrimitiveRunPhysicalTypeScale(group_type));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R6, 0);
	}
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunMultiStartGroup(compiler, group_type, group_cast_kind, primitive_kinds);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunMultiIncrementRowCount(compiler);
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		EmitSljitPrimitiveRunAccumulateLane(compiler, lane_idx, payload_types[lane_idx], primitive_kinds[lane_idx]);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto next_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_row, row_loop);

	auto finish = sljit_emit_label(compiler);
	sljit_set_label(input_done, finish);
	sljit_set_label(output_full, finish);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePrimitiveRunInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_run_codegen.cpp
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

bool SljitPrimitiveRunMachineWordSupported() {
#if defined(SLJIT_32BIT_ARCHITECTURE) && SLJIT_32BIT_ARCHITECTURE
	return false;
#else
	return sizeof(sljit_sw) >= sizeof(int64_t);
#endif
}

bool SljitPrimitiveRunCodegenSupported() {
	if (!SljitPrimitiveRunMachineWordSupported()) {
		return false;
	}
#if SLJIT_NUMBER_OF_SAVED_REGISTERS < 6 || (SLJIT_NUMBER_OF_REGISTERS - SLJIT_NUMBER_OF_SAVED_REGISTERS) < 7
	return false;
#else
	return true;
#endif
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
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	(void)compiler;
	(void)fallback;
	return SLJIT_S6;
#else
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	return fallback;
#endif
}

static sljit_s32 EmitSljitPrimitiveRunOutputRowCounts(struct sljit_compiler *compiler, sljit_s32 fallback) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	(void)compiler;
	(void)fallback;
	return SLJIT_S7;
#else
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	return fallback;
#endif
}

static sljit_s32 EmitSljitPrimitiveRunOutputValues(struct sljit_compiler *compiler, sljit_s32 fallback,
                                                   AggregatePrimitiveUpdateKind primitive_kind) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	(void)compiler;
	(void)fallback;
	(void)primitive_kind;
	return SLJIT_S8;
#else
	const auto offset = primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT
	                        ? offsetof(SljitNativePrimitiveRunInput, output_hugeint_values)
	                        : offsetof(SljitNativePrimitiveRunInput, output_int64_values);
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0), offset);
	return fallback;
#endif
}

static sljit_s32 EmitSljitPrimitiveRunOutputValueIsSet(struct sljit_compiler *compiler, sljit_s32 fallback) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	(void)compiler;
	(void)fallback;
	return SLJIT_S9;
#else
	sljit_emit_op1(compiler, SLJIT_MOV_P, fallback, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_value_is_set));
	return fallback;
#endif
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
#if SLJIT_NUMBER_OF_SAVED_REGISTERS < 6 || (SLJIT_NUMBER_OF_REGISTERS - SLJIT_NUMBER_OF_SAVED_REGISTERS) < 7
	error = "unsupported SLJIT primitive run register file";
	return nullptr;
#else
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, 10, 0);
#else
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, 6, 0);
#endif
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
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S7, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	const auto output_values_offset = primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT
	                                      ? offsetof(SljitNativePrimitiveRunInput, output_hugeint_values)
	                                      : offsetof(SljitNativePrimitiveRunInput, output_int64_values);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S8, 0, SLJIT_MEM1(SLJIT_S0), output_values_offset);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S9, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_value_is_set));
	}
#endif

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
#endif
}

static void EmitSljitPrimitiveRunLoadLaneInput(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, lane_inputs));
	const auto lane_offset = static_cast<sljit_sw>(lane_idx * sizeof(SljitNativePrimitiveRunLaneInput));
	if (lane_offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_IMM, lane_offset);
	}
}

static void EmitSljitPrimitiveRunMultiInitializeLane(struct sljit_compiler *compiler, idx_t lane_idx,
                                                     AggregatePrimitiveUpdateKind primitive_kind) {
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R6),
		               offsetof(SljitNativePrimitiveRunLaneInput, output_hugeint_values));
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, lower), SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R5), offsetof(hugeint_t, upper), SLJIT_IMM, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R6),
		               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R5, SLJIT_S3), 3, SLJIT_IMM, 0);
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R6),
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
		EmitSljitPrimitiveRunMultiInitializeLane(compiler, lane_idx, primitive_kinds[lane_idx]);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

struct SljitPrimitiveRunMultiValidityJumps {
	sljit_jump *all_valid = nullptr;
	sljit_jump *is_null = nullptr;
};

static SljitPrimitiveRunMultiValidityJumps EmitSljitPrimitiveRunMultiValidityCheck(struct sljit_compiler *compiler,
                                                                                   idx_t lane_idx) {
	SljitPrimitiveRunMultiValidityJumps result;
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
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

static void EmitSljitPrimitiveRunMultiCount(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3, SLJIT_R0, 0);
}

static void EmitSljitPrimitiveRunMultiMarkValueSet(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_value_is_set));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunMultiSumInt64(struct sljit_compiler *compiler, idx_t lane_idx,
                                               PhysicalType payload_type) {
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
	               offsetof(SljitNativePrimitiveRunLaneInput, payload_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R4, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
	               offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R4, SLJIT_R5), 3, SLJIT_R1, 0);
	EmitSljitPrimitiveRunMultiMarkValueSet(compiler, lane_idx);
}

static void EmitSljitPrimitiveRunMultiSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                                 PhysicalType payload_type) {
	EmitSljitPrimitiveRunLoadLaneInput(compiler, lane_idx, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
	               offsetof(SljitNativePrimitiveRunLaneInput, payload_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R4, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	if (SljitPrimitiveRunPhysicalTypeIsSigned(payload_type)) {
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R1, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R6),
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
	EmitSljitPrimitiveRunMultiMarkValueSet(compiler, lane_idx);
}

static void EmitSljitPrimitiveRunMultiAccumulateLane(struct sljit_compiler *compiler, idx_t lane_idx,
                                                     PhysicalType payload_type,
                                                     AggregatePrimitiveUpdateKind primitive_kind) {
	SljitPrimitiveRunMultiValidityJumps validity;
	if (primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		validity = EmitSljitPrimitiveRunMultiValidityCheck(compiler, lane_idx);
	}
	auto accumulate = sljit_emit_label(compiler);
	if (validity.all_valid) {
		sljit_set_label(validity.all_valid, accumulate);
	}
	switch (primitive_kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
		EmitSljitPrimitiveRunMultiCount(compiler, lane_idx);
		break;
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		EmitSljitPrimitiveRunMultiSumInt64(compiler, lane_idx, payload_type);
		break;
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		EmitSljitPrimitiveRunMultiSumHugeint(compiler, lane_idx, payload_type);
		break;
	default:
		throw InternalException("Unsupported SLJIT multi-lane primitive run aggregate");
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
	if (primitive_kinds.size() < 2 || primitive_kinds.size() != payload_types.size()) {
		error = "unsupported SLJIT multi-lane primitive run shape";
		return nullptr;
	}
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		auto primitive_kind = primitive_kinds[lane_idx];
		if (!SljitPrimitiveRunPayloadSupported(payload_types[lane_idx], primitive_kind, false)) {
			error = "unsupported SLJIT multi-lane primitive run aggregate payload";
			return nullptr;
		}
	}
#if SLJIT_NUMBER_OF_SAVED_REGISTERS < 6 || (SLJIT_NUMBER_OF_REGISTERS - SLJIT_NUMBER_OF_SAVED_REGISTERS) < 7
	error = "unsupported SLJIT multi-lane primitive run register file";
	return nullptr;
#else
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, 6, 0);
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
		EmitSljitPrimitiveRunMultiAccumulateLane(compiler, lane_idx, payload_types[lane_idx],
		                                         primitive_kinds[lane_idx]);
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
#endif
}

} // namespace duckdb

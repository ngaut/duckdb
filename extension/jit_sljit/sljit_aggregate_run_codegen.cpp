//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_run_codegen.cpp
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_kernel_frame.hpp"
#include "sljit_platform.hpp"
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
	return GetSljitTargetCapabilities().SupportsPrimitiveRunRegisterABI();
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

//! The single-lane kernel's frame values: its output pointers, loaded once, and a nullable sum's set flag, carried
//! across the group's rows
struct SljitPrimitiveRunSingleLaneValues {
	SljitOperand output_group_data {0, 0};
	SljitOperand output_row_counts {0, 0};
	SljitOperand output_values {0, 0};
	SljitOperand output_value_is_set {0, 0};
	SljitOperand set_flag {0, 0};
};

static void EmitSljitPrimitiveRunRecordIncreasingTransition(struct sljit_compiler *compiler,
                                                            PhysicalType group_source_type) {
	const auto greater = SljitPrimitiveRunPhysicalTypeIsSigned(group_source_type) ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	auto increasing = sljit_emit_cmp(compiler, greater, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_groups_strictly_increasing), SLJIT_IMM, 0);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(increasing, done);
}

static void EmitSljitPrimitiveRunLoadExistingState(struct sljit_compiler *compiler,
                                                   const SljitPrimitiveRunSingleLaneValues &values,
                                                   PhysicalType group_type,
                                                   ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                                   AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta,
                                                   bool payload_nullable) {
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_group_data = SljitKernelFrameRegister(compiler, values.output_group_data, SLJIT_R6);
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
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper));
	} else {
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(output_values, SLJIT_R5), 3);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	// a group the previous call started keeps the set flag it stored
	if (values.set_flag.operand) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_value_is_set = SljitKernelFrameRegister(compiler, values.output_value_is_set, SLJIT_R6);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, values.set_flag.operand, values.set_flag.operand_w,
		               SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0);
	}

	const auto output_row_counts = SljitKernelFrameRegister(compiler, values.output_row_counts, SLJIT_R6);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3);
}

static void EmitSljitPrimitiveRunStartGroup(struct sljit_compiler *compiler,
                                            const SljitPrimitiveRunSingleLaneValues &values, PhysicalType group_type,
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
	const auto output_group_data = SljitKernelFrameRegister(compiler, values.output_group_data, SLJIT_R5);
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
	if (values.set_flag.operand) {
		sljit_emit_op1(compiler, SLJIT_MOV, values.set_flag.operand, values.set_flag.operand_w, SLJIT_IMM, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

//! A nullable payload's row accumulates by its validity bit instead of a jump: a count adds the bit, a sum adds its
//! value masked by the bit and ORs the bit into its set flag. That takes no more instructions than testing the bit,
//! jumping and setting the flag, and never mispredicts on the NULLs' order
static void EmitSljitPrimitiveRunAccumulate(struct sljit_compiler *compiler,
                                            const SljitPrimitiveRunSingleLaneValues &values, PhysicalType payload_type,
                                            AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta,
                                            bool payload_nullable) {
	D_ASSERT(!payload_nullable || !use_batch_delta);
	if (payload_nullable) {
		D_ASSERT(primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR);
		// R6: the row's validity bit
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, payload_validity));
		EmitLoadSljitValidityBit(compiler, SLJIT_R5, SLJIT_S1, SLJIT_R6);
		if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R6, 0);
		} else {
			sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R4, 0,
			               SLJIT_MEM2(SLJIT_S5, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_IMM, 0, SLJIT_R6, 0);
			sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R5, 0);
			if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				if (SljitPrimitiveRunPhysicalTypeIsSigned(payload_type)) {
					sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R5, 0, SLJIT_R4, 0, SLJIT_IMM, 63);
				} else {
					sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_IMM, 0);
				}
				EmitSljitAddDoubleWord(compiler, SLJIT_R1, SLJIT_R2, SLJIT_R4, SLJIT_R5);
			} else {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
			}
			sljit_emit_op2(compiler, SLJIT_OR, values.set_flag.operand, values.set_flag.operand_w,
			               values.set_flag.operand, values.set_flag.operand_w, SLJIT_R6, 0);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
		return;
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
			EmitSljitAddDoubleWord(compiler, SLJIT_R1, SLJIT_R2, SLJIT_R4, SLJIT_R5);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
		}
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunBatchDeltaState(struct sljit_compiler *compiler,
                                                 const SljitPrimitiveRunSingleLaneValues &values,
                                                 AggregatePrimitiveUpdateKind primitive_kind, bool merge_existing) {
	// R3 is the represented row delta and R1 is the aggregate delta. Only the
	// first run in an invocation can merge into the unpublished boundary group;
	// every group started by this kernel owns a fresh output slot.
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_row_counts = SljitKernelFrameRegister(compiler, values.output_row_counts, SLJIT_R6);
	if (merge_existing) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3, SLJIT_R4, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_row_counts, SLJIT_R5), 3, SLJIT_R3, 0);
	}

	if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
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
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R3);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
		if (merge_existing) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper));
			EmitSljitAddDoubleWord(compiler, SLJIT_R4, SLJIT_R5, SLJIT_R1, SLJIT_R2);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower), SLJIT_R4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper), SLJIT_R5, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, lower), SLJIT_R1, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), offsetof(hugeint_t, upper), SLJIT_R2, 0);
		}
	} else {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
		if (merge_existing) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(output_values, SLJIT_R5), 3);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R1, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R4, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R1, 0);
		}
	}

	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_value_is_set = SljitKernelFrameRegister(compiler, values.output_value_is_set, SLJIT_R6);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunBatchDeltaFlush(struct sljit_compiler *compiler,
                                                 const SljitPrimitiveRunSingleLaneValues &values,
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
	EmitSljitPrimitiveRunBatchDeltaState(compiler, values, primitive_kind, true);
	auto state_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto new_group_label = sljit_emit_label(compiler);
	sljit_set_label(new_group, new_group_label);
	EmitSljitPrimitiveRunBatchDeltaState(compiler, values, primitive_kind, false);
	auto state_done_label = sljit_emit_label(compiler);
	sljit_set_label(state_done, state_done_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_R0, 0);
}

static void EmitSljitPrimitiveRunFlush(struct sljit_compiler *compiler, const SljitPrimitiveRunSingleLaneValues &values,
                                       AggregatePrimitiveUpdateKind primitive_kind, bool use_batch_delta,
                                       bool payload_nullable) {
	if (use_batch_delta) {
		EmitSljitPrimitiveRunBatchDeltaFlush(compiler, values, primitive_kind);
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, output_values, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower), SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper), SLJIT_R2, 0);
	} else {
		const auto output_values = SljitKernelFrameRegister(compiler, values.output_values, SLJIT_R6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(output_values, SLJIT_R5), 3, SLJIT_R1, 0);
	}
	if (!payload_nullable && (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                          primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_value_is_set = SljitKernelFrameRegister(compiler, values.output_value_is_set, SLJIT_R6);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0, SLJIT_IMM, 1);
	}
	if (values.set_flag.operand) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		const auto output_value_is_set = SljitKernelFrameRegister(compiler, values.output_value_is_set, SLJIT_R6);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(output_value_is_set, SLJIT_R5), 0, values.set_flag.operand,
		               values.set_flag.operand_w);
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	const auto output_row_counts = SljitKernelFrameRegister(compiler, values.output_row_counts, SLJIT_R6);
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
	if (!SljitPrimitiveRunCodegenSupported()) {
		error = "unsupported SLJIT primitive run register layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	// R0 holds the group's key, R1-R2 its value, R3 its row count, R4-R6 are scratch; S0-S5 the protocol
	SljitKernelFrame frame(7, 6);
	SljitPrimitiveRunSingleLaneValues values;
	if (payload_nullable && needs_payload) {
		values.set_flag = frame.AllocateCarried();
	}
	auto input_field = [](sljit_sw offset) {
		return SljitOperand {SLJIT_MEM1(SLJIT_S0), offset};
	};
	values.output_values = frame.AllocateInvariant(
	    input_field(primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT
	                    ? NumericCast<sljit_sw>(offsetof(SljitNativePrimitiveRunInput, output_hugeint_values))
	                    : NumericCast<sljit_sw>(offsetof(SljitNativePrimitiveRunInput, output_int64_values))));
	values.output_row_counts =
	    frame.AllocateInvariant(input_field(offsetof(SljitNativePrimitiveRunInput, output_row_counts)));
	values.output_group_data =
	    frame.AllocateInvariant(input_field(offsetof(SljitNativePrimitiveRunInput, output_group_data)));
	if (needs_payload) {
		values.output_value_is_set =
		    frame.AllocateInvariant(input_field(offsetof(SljitNativePrimitiveRunInput, output_value_is_set)));
	}
	frame.EmitEnter(compiler, SLJIT_ARGS1V(P));
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
	frame.EmitLoadInvariants(compiler);

	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunLoadExistingState(compiler, values, group_type, group_cast_kind, primitive_kind,
	                                       use_batch_delta, payload_nullable);
	auto initial_group_matches = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunStartGroup(compiler, values, group_type, group_cast_kind, primitive_kind, use_batch_delta,
	                                payload_nullable);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(initial_group_matches, accumulate_group);
	EmitSljitPrimitiveRunAccumulate(compiler, values, payload_type, primitive_kind, use_batch_delta, payload_nullable);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto run_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);
	EmitSljitPrimitiveRunFlush(compiler, values, primitive_kind, use_batch_delta, payload_nullable);
	auto next_group = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_group, append_group);

	auto flush_last_group = sljit_emit_label(compiler);
	sljit_set_label(run_done, flush_last_group);
	EmitSljitPrimitiveRunFlush(compiler, values, primitive_kind, use_batch_delta, payload_nullable);

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

//! The lanes beyond which only homogeneous lanes run (the preaggregated publish loops over those), and the lanes a run
//! kernel unrolls at most, which bounds its loop body to a few kilobytes
static constexpr idx_t SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET = 8;
static constexpr idx_t SLJIT_PRIMITIVE_RUN_LANE_LIMIT = 64;

static bool SljitPreaggregatedPrimitiveUpdateKindSupported(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return true;
	default:
		return false;
	}
}

bool SljitPreaggregatedPrimitiveStateLayoutSupported(const ExecutionPrimitiveAggregateUpdateLane &lane) {
	if (!SljitPreaggregatedPrimitiveUpdateKindSupported(lane.kind)) {
		return false;
	}
	const auto value_size = AggregatePrimitiveUpdateStateValueSize(lane.kind);
	const auto has_state_is_set = AggregatePrimitiveUpdateHasStateIsSet(lane.kind);
	const auto canonical_state_size = value_size + (has_state_is_set ? sizeof(uint64_t) : 0);
	return value_size != 0 && lane.state_value_offset == 0 && lane.state_size == canonical_state_size &&
	       (!has_state_is_set || lane.state_is_set_offset == value_size);
}

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
	       (primitive_kinds.size() <= SLJIT_PRIMITIVE_RUN_LANE_LIMIT &&
	        SljitPrimitiveRunLanesAreHomogeneous(payload_types, primitive_kinds));
}

static sljit_sw SljitPrimitiveRunLaneFieldOffset(idx_t lane_idx, sljit_sw field_offset) {
	return static_cast<sljit_sw>(lane_idx * sizeof(SljitNativePrimitiveRunLaneInput)) + field_offset;
}

static void EmitSljitPrimitiveRunLoadLanePointer(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 target,
                                                 sljit_sw field_offset) {
	// S5 is the lane array: a lane's fields sit at constant offsets from it
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S5),
	               SljitPrimitiveRunLaneFieldOffset(lane_idx, field_offset));
}

//! The affine kernel's frame values: the group's shared-source sum and valid row count, carried across its rows
struct SljitPrimitiveRunAffineValues {
	SljitOperand value {0, 0};
	SljitOperand valid_count {0, 0};
};

static void EmitSljitPrimitiveRunAffineReset(struct sljit_compiler *compiler,
                                             const SljitPrimitiveRunAffineValues &values) {
	sljit_emit_op1(compiler, SLJIT_MOV, values.value.operand, values.value.operand_w, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, values.valid_count.operand, values.valid_count.operand_w, SLJIT_IMM, 0);
}

static void EmitSljitPrimitiveRunAffineAccumulateValue(struct sljit_compiler *compiler,
                                                       const SljitPrimitiveRunAffineValues &values,
                                                       sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ADD, values.value.operand, values.value.operand_w, values.value.operand,
	               values.value.operand_w, value_reg, 0);
}

static void EmitSljitPrimitiveRunAffineIncrementValidCount(struct sljit_compiler *compiler,
                                                           const SljitPrimitiveRunAffineValues &values) {
	sljit_emit_op2(compiler, SLJIT_ADD, values.valid_count.operand, values.valid_count.operand_w,
	               values.valid_count.operand, values.valid_count.operand_w, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunAffineStartFreshGroup(struct sljit_compiler *compiler,
                                                       const SljitPrimitiveRunAffineValues &values,
                                                       PhysicalType group_type,
                                                       ExecutionRowPointerGroupKeyCastKind group_cast_kind) {
	// R4 contains the raw source key. R2 distinguishes this fresh slot from a
	// pending boundary slot whose finalized affine deltas must be merged.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunAffineReset(compiler, values);
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

static void EmitSljitPrimitiveRunAffineStartExistingGroup(struct sljit_compiler *compiler,
                                                          const SljitPrimitiveRunAffineValues &values) {
	// The previous invocation already finalized this output slot. Accumulate a
	// new shared-source delta and merge it exactly once at the next boundary.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
	EmitSljitPrimitiveRunAffineReset(compiler, values);
}

static sljit_jump *EmitJumpIfSljitPrimitiveRunAffinePayloadNull(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	auto is_null = EmitJumpIfSljitValidityBitClear(compiler, SLJIT_R3, SLJIT_S1, SLJIT_R4);
	sljit_set_label(all_valid, sljit_emit_label(compiler));
	return is_null;
}

static void EmitSljitPrimitiveRunAffineFinalize(struct sljit_compiler *compiler,
                                                const SljitPrimitiveRunAffineValues &values, bool payload_nullable) {
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
	auto value_operand = SljitKernelFrameRegister(compiler, values.value, SLJIT_R0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R5, 0, SLJIT_R5, 0, value_operand, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, SLJIT_R5, 0);
	if (payload_nullable) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_shared_valid_counts));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
		auto valid_count_operand = SljitKernelFrameRegister(compiler, values.valid_count, SLJIT_R0);
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
	value_operand = SljitKernelFrameRegister(compiler, values.value, SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3, value_operand, 0);
	if (payload_nullable) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_shared_valid_counts));
		auto valid_count_operand = SljitKernelFrameRegister(compiler, values.valid_count, SLJIT_R0);
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
	if (!SljitPrimitiveRunCodegenSupported()) {
		error = "unsupported SLJIT affine int64 primitive run register layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	// R0 holds the group's key, R1 its row count, R2 whether its slot is fresh, R3-R6 are scratch; S0-S5 the protocol
	SljitKernelFrame frame(7, 6);
	SljitPrimitiveRunAffineValues values;
	values.value = frame.AllocateCarried();
	values.valid_count = frame.AllocateCarried();
	frame.EmitEnter(compiler, SLJIT_ARGS1V(P));
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
	EmitSljitPrimitiveRunAffineStartFreshGroup(compiler, values, group_type, group_cast_kind);
	auto start_accumulating_fresh = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto start_existing_group = sljit_emit_label(compiler);
	sljit_set_label(same_existing_group, start_existing_group);
	EmitSljitPrimitiveRunAffineStartExistingGroup(compiler, values);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(start_accumulating_fresh, accumulate_group);
	sljit_jump *payload_is_null = nullptr;
	if (payload_nullable) {
		payload_is_null = EmitJumpIfSljitPrimitiveRunAffinePayloadNull(compiler);
	}
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R3, 0,
	               SLJIT_MEM2(SLJIT_S5, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	EmitSljitPrimitiveRunAffineAccumulateValue(compiler, values, SLJIT_R3);
	if (payload_nullable) {
		EmitSljitPrimitiveRunAffineIncrementValidCount(compiler, values);
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
	EmitSljitPrimitiveRunAffineFinalize(compiler, values, payload_nullable);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_S5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto transition_output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunAffineStartFreshGroup(compiler, values, group_type, group_cast_kind);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, payload_data));
	auto next_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_row, accumulate_group);

	sljit_set_label(run_done, sljit_emit_label(compiler));
	EmitSljitPrimitiveRunAffineFinalize(compiler, values, payload_nullable);

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

//! The multi-lane kernel's frame values: a group's running state (the row count; per lane its value, a hugeint sum's
//! upper word, a sum's set flag), carried across the group's rows and stored into its output slot when the group
//! ends, then each lane's payload and validity pointers. The lanes of a shared validity mask share one set flag and
//! read one mask
struct SljitPrimitiveRunMultiAccumulators {
	struct Lane {
		SljitOperand lower {0, 0};
		SljitOperand upper {0, 0};
		SljitOperand flag {0, 0};
		SljitOperand payload_data {0, 0};
		SljitOperand payload_validity {0, 0};
	};
	SljitOperand row_count {0, 0};
	vector<Lane> lanes;
};

static SljitPrimitiveRunMultiAccumulators
PlanSljitPrimitiveRunMultiAccumulators(SljitKernelFrame &frame,
                                       const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
                                       bool shared_payload_validity) {
	SljitPrimitiveRunMultiAccumulators result;
	result.row_count = frame.AllocateCarried();
	SljitOperand shared_flag {0, 0};
	for (auto primitive_kind : primitive_kinds) {
		SljitPrimitiveRunMultiAccumulators::Lane lane;
		lane.lower = frame.AllocateCarried();
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			lane.upper = frame.AllocateCarried();
		}
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			if (!shared_payload_validity) {
				lane.flag = frame.AllocateCarried();
			} else {
				if (!shared_flag.operand) {
					shared_flag = frame.AllocateCarried();
				}
				lane.flag = shared_flag;
			}
		}
		result.lanes.push_back(lane);
	}
	auto lane_field = [](idx_t lane_idx, sljit_sw offset) {
		return SljitOperand {SLJIT_MEM1(SLJIT_S5), SljitPrimitiveRunLaneFieldOffset(lane_idx, offset)};
	};
	// a shared mask is read through its first lane
	bool validity_read = false;
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		const auto primitive_kind = primitive_kinds[lane_idx];
		auto &lane = result.lanes[lane_idx];
		if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (primitive_kind != AggregatePrimitiveUpdateKind::COUNT) {
			lane.payload_data =
			    frame.AllocateInvariant(lane_field(lane_idx, offsetof(SljitNativePrimitiveRunLaneInput, payload_data)));
		}
		if (!shared_payload_validity || !validity_read) {
			lane.payload_validity = frame.AllocateInvariant(
			    lane_field(lane_idx, offsetof(SljitNativePrimitiveRunLaneInput, payload_validity)));
			validity_read = true;
		}
	}
	return result;
}

//! The lane's output slot S3 - 1 into R1 (a hugeint sum's slot address), its index into R5
static void EmitSljitPrimitiveRunMultiLaneSlot(struct sljit_compiler *compiler, idx_t lane_idx,
                                               AggregatePrimitiveUpdateKind primitive_kind, sljit_sw field_offset) {
	EmitSljitPrimitiveRunLoadLanePointer(compiler, lane_idx, SLJIT_R1, field_offset);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT &&
	    field_offset == offsetof(SljitNativePrimitiveRunLaneInput, output_hugeint_values)) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	}
}

//! Moves each accumulator to (store) or from (load) the group's output slot S3 - 1
static void EmitSljitPrimitiveRunMultiTransferState(struct sljit_compiler *compiler,
                                                    const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
                                                    const SljitPrimitiveRunMultiAccumulators &accumulators,
                                                    bool store) {
	auto transfer = [&](sljit_s32 op, sljit_s32 slot, sljit_sw slot_w, const SljitOperand &accumulator) {
		if (store) {
			sljit_emit_op1(compiler, op, slot, slot_w, accumulator.operand, accumulator.operand_w);
		} else {
			sljit_emit_op1(compiler, op, accumulator.operand, accumulator.operand_w, slot, slot_w);
		}
	};
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	transfer(SLJIT_MOV, SLJIT_MEM2(SLJIT_R1, SLJIT_R5), 3, accumulators.row_count);
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		const auto primitive_kind = primitive_kinds[lane_idx];
		auto &lane = accumulators.lanes[lane_idx];
		if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitSljitPrimitiveRunMultiLaneSlot(compiler, lane_idx, primitive_kind,
			                                   offsetof(SljitNativePrimitiveRunLaneInput, output_hugeint_values));
			transfer(SLJIT_MOV, SLJIT_MEM1(SLJIT_R1), offsetof(hugeint_t, lower), lane.lower);
			transfer(SLJIT_MOV, SLJIT_MEM1(SLJIT_R1), offsetof(hugeint_t, upper), lane.upper);
		} else {
			EmitSljitPrimitiveRunMultiLaneSlot(compiler, lane_idx, primitive_kind,
			                                   offsetof(SljitNativePrimitiveRunLaneInput, output_int64_values));
			transfer(SLJIT_MOV, SLJIT_MEM2(SLJIT_R1, SLJIT_R5), 3, lane.lower);
		}
		if (lane.flag.operand) {
			EmitSljitPrimitiveRunMultiLaneSlot(compiler, lane_idx, primitive_kind,
			                                   offsetof(SljitNativePrimitiveRunLaneInput, output_value_is_set));
			transfer(SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R1, SLJIT_R5), 0, lane.flag);
		}
	}
}

//! A row's update of one lane. bit_reg, when set, holds the row's validity bit: a count adds it, a sum adds its value
//! masked by it and ORs it into its set flag. Without it the row is valid. R0 holds the group's key; R1, R2 and R5 are
//! scratch
static void EmitSljitPrimitiveRunMultiAccumulateLane(struct sljit_compiler *compiler, idx_t lane_idx,
                                                     PhysicalType payload_type,
                                                     AggregatePrimitiveUpdateKind primitive_kind,
                                                     const SljitPrimitiveRunMultiAccumulators::Lane &lane,
                                                     sljit_s32 bit_reg) {
	if (primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		if (bit_reg) {
			sljit_emit_op2(compiler, SLJIT_ADD, lane.lower.operand, lane.lower.operand_w, lane.lower.operand,
			               lane.lower.operand_w, bit_reg, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, lane.lower.operand, lane.lower.operand_w, lane.lower.operand,
			               lane.lower.operand_w, SLJIT_IMM, 1);
		}
		return;
	}
	const auto payload_data = SljitKernelFrameRegister(compiler, lane.payload_data, SLJIT_R1);
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(payload_type), SLJIT_R1, 0,
	               SLJIT_MEM2(payload_data, SLJIT_S1), SljitPrimitiveRunPhysicalTypeScale(payload_type));
	if (bit_reg) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_IMM, 0, bit_reg, 0);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		if (SljitPrimitiveRunPhysicalTypeIsSigned(payload_type)) {
			sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		}
		sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, lane.lower.operand, lane.lower.operand_w,
		               lane.lower.operand, lane.lower.operand_w, SLJIT_R1, 0);
		sljit_emit_op2(compiler, SLJIT_ADDC, lane.upper.operand, lane.upper.operand_w, lane.upper.operand,
		               lane.upper.operand_w, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, SLJIT_ADD, lane.lower.operand, lane.lower.operand_w, lane.lower.operand,
		               lane.lower.operand_w, SLJIT_R1, 0);
	}
	if (bit_reg) {
		sljit_emit_op2(compiler, SLJIT_OR, lane.flag.operand, lane.flag.operand_w, lane.flag.operand,
		               lane.flag.operand_w, bit_reg, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, lane.flag.operand, lane.flag.operand_w, SLJIT_IMM, 1);
	}
}

//! A lane without a validity mask accumulates as valid; one with a mask by the row's bit, in R6
template <class ACCUMULATE>
static void EmitSljitPrimitiveRunMultiByValidity(struct sljit_compiler *compiler,
                                                 const SljitPrimitiveRunMultiAccumulators::Lane &lane,
                                                 const ACCUMULATE &accumulate) {
	const auto validity = SljitKernelFrameRegister(compiler, lane.payload_validity, SLJIT_R5);
	auto no_mask = sljit_emit_cmp(compiler, SLJIT_EQUAL, validity, 0, SLJIT_IMM, 0);
	EmitLoadSljitValidityBit(compiler, validity, SLJIT_S1, SLJIT_R6);
	accumulate(SLJIT_R6);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_mask, sljit_emit_label(compiler));
	accumulate(0);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitSljitPrimitiveRunMultiAccumulateRow(struct sljit_compiler *compiler,
                                                    const vector<PhysicalType> &payload_types,
                                                    const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
                                                    const SljitPrimitiveRunMultiAccumulators &accumulators,
                                                    bool shared_payload_validity) {
	auto &row_count = accumulators.row_count;
	sljit_emit_op2(compiler, SLJIT_ADD, row_count.operand, row_count.operand_w, row_count.operand, row_count.operand_w,
	               SLJIT_IMM, 1);
	idx_t representative_lane = DConstants::INVALID_INDEX;
	for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
		if (primitive_kinds[lane_idx] == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitPrimitiveRunMultiAccumulateLane(compiler, lane_idx, payload_types[lane_idx],
			                                         primitive_kinds[lane_idx], accumulators.lanes[lane_idx], 0);
			continue;
		}
		if (shared_payload_validity) {
			representative_lane = MinValue(representative_lane, lane_idx);
			continue;
		}
		EmitSljitPrimitiveRunMultiByValidity(compiler, accumulators.lanes[lane_idx], [&](sljit_s32 bit_reg) {
			EmitSljitPrimitiveRunMultiAccumulateLane(compiler, lane_idx, payload_types[lane_idx],
			                                         primitive_kinds[lane_idx], accumulators.lanes[lane_idx], bit_reg);
		});
	}
	if (representative_lane == DConstants::INVALID_INDEX) {
		return;
	}
	EmitSljitPrimitiveRunMultiByValidity(compiler, accumulators.lanes[representative_lane], [&](sljit_s32 bit_reg) {
		for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
			if (primitive_kinds[lane_idx] != AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitPrimitiveRunMultiAccumulateLane(compiler, lane_idx, payload_types[lane_idx],
				                                         primitive_kinds[lane_idx], accumulators.lanes[lane_idx],
				                                         bit_reg);
			}
		}
	});
}

//! A new group: its key, R4, becomes R0 and the next output slot's key; its state starts empty
static void EmitSljitPrimitiveRunMultiStartGroup(struct sljit_compiler *compiler, PhysicalType group_type,
                                                 ExecutionRowPointerGroupKeyCastKind group_cast_kind,
                                                 const SljitPrimitiveRunMultiAccumulators &accumulators) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	if (group_cast_kind == ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, group_cast_constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_R6, 0);
	}
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
	               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R4, 0);
	auto clear = [&](const SljitOperand &accumulator) {
		if (accumulator.operand) {
			sljit_emit_op1(compiler, SLJIT_MOV, accumulator.operand, accumulator.operand_w, SLJIT_IMM, 0);
		}
	};
	clear(accumulators.row_count);
	for (auto &lane : accumulators.lanes) {
		clear(lane.lower);
		clear(lane.upper);
		clear(lane.flag);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePrimitiveRunMultiUpdate(
    PhysicalType group_source_type, PhysicalType group_type, ExecutionRowPointerGroupKeyCastKind group_cast_kind,
    const vector<PhysicalType> &payload_types, const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
    bool shared_payload_validity, SljitNativePrimitiveRunFunction &function, string &error) {
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
	if (shared_payload_validity) {
		idx_t nullable_lane_count = 0;
		for (auto primitive_kind : primitive_kinds) {
			nullable_lane_count += primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ? 0 : 1;
		}
		if (nullable_lane_count < 2) {
			error = "unsupported SLJIT shared-validity primitive run shape";
			return nullptr;
		}
	}
	if (!SljitPrimitiveRunCodegenSupported()) {
		error = "unsupported SLJIT multi-lane primitive run register layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	// R0 holds the group's key, R1-R6 are scratch; S0-S5 the protocol
	SljitKernelFrame frame(7, 6);
	const auto accumulators = PlanSljitPrimitiveRunMultiAccumulators(frame, primitive_kinds, shared_payload_validity);
	frame.EmitEnter(compiler, SLJIT_ARGS1V(P));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, group_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, lane_inputs));
	frame.EmitLoadInvariants(compiler);

	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	// the previous call's last group continues when this call's first row has its key
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
	auto new_group = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitSljitPrimitiveRunMultiTransferState(compiler, primitive_kinds, accumulators, false);
	auto resume_group = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(new_group, sljit_emit_label(compiler));
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunMultiStartGroup(compiler, group_type, group_cast_kind, accumulators);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(resume_group, accumulate_group);
	EmitSljitPrimitiveRunMultiAccumulateRow(compiler, payload_types, primitive_kinds, accumulators,
	                                        shared_payload_validity);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto run_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	sljit_set_label(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0), accumulate_group);
	EmitSljitPrimitiveRunRecordIncreasingTransition(compiler, group_source_type);
	EmitSljitPrimitiveRunMultiTransferState(compiler, primitive_kinds, accumulators, true);
	sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), append_group);

	sljit_set_label(run_done, sljit_emit_label(compiler));
	EmitSljitPrimitiveRunMultiTransferState(compiler, primitive_kinds, accumulators, true);

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

static sljit_sw SljitPreaggregatedLaneFieldOffset(idx_t lane_idx, sljit_sw field_offset) {
	if (lane_idx == DConstants::INVALID_INDEX) {
		return field_offset;
	}
	return NumericCast<sljit_sw>(lane_idx * sizeof(SljitNativePreaggregatedPrimitiveLaneInput)) + field_offset;
}

static void EmitSljitPreaggregatedLanePointer(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 target,
                                              sljit_sw field_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S4),
	               SljitPreaggregatedLaneFieldOffset(lane_idx, field_offset));
}

struct SljitPreaggregatedStateMemory {
	sljit_s32 memory;
	sljit_sw offset;

	sljit_sw Field(sljit_sw field_offset) const {
		return offset + field_offset;
	}
};

static SljitPreaggregatedStateMemory BindSljitPreaggregatedStateMemory(struct sljit_compiler *compiler, idx_t lane_idx,
                                                                       idx_t state_offset) {
	if (lane_idx != DConstants::INVALID_INDEX) {
		return {SLJIT_MEM1(SLJIT_S5), NumericCast<sljit_sw>(state_offset)};
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S4),
	               SljitPreaggregatedLaneFieldOffset(
	                   lane_idx, offsetof(SljitNativePreaggregatedPrimitiveLaneInput, state_offset)));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_S5, 0, SLJIT_R4, 0);
	return {SLJIT_MEM1(SLJIT_R4), 0};
}

static void EmitSljitPreaggregatedCountUpdate(struct sljit_compiler *compiler, idx_t lane_idx, idx_t state_offset,
                                              bool initialize_states) {
	EmitSljitPreaggregatedLanePointer(compiler, lane_idx, SLJIT_R0,
	                                  offsetof(SljitNativePreaggregatedPrimitiveLaneInput, int64_values));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
	auto state = BindSljitPreaggregatedStateMemory(compiler, lane_idx, state_offset);
	if (!initialize_states) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, state.memory, state.offset);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.offset, SLJIT_R1, 0);
}

static void EmitSljitPreaggregatedSumInt64Update(struct sljit_compiler *compiler, idx_t lane_idx, idx_t state_offset,
                                                 bool initialize_states) {
	EmitSljitPreaggregatedLanePointer(compiler, lane_idx, SLJIT_R0,
	                                  offsetof(SljitNativePreaggregatedPrimitiveLaneInput, value_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0);
	auto value_is_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);

	EmitSljitPreaggregatedLanePointer(compiler, lane_idx, SLJIT_R0,
	                                  offsetof(SljitNativePreaggregatedPrimitiveLaneInput, int64_values));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
	auto state = BindSljitPreaggregatedStateMemory(compiler, lane_idx, state_offset);
	if (!initialize_states) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, state.memory, state.offset);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.offset, SLJIT_R1, 0);
	if (initialize_states) {
		sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.Field(sizeof(int64_t)), SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, state.memory, state.Field(sizeof(int64_t)), SLJIT_IMM, 1);
	auto value_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(value_is_null, sljit_emit_label(compiler));
	if (initialize_states) {
		auto null_state = BindSljitPreaggregatedStateMemory(compiler, lane_idx, state_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, null_state.memory, null_state.offset, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, null_state.memory, null_state.Field(sizeof(int64_t)), SLJIT_IMM, 0);
	}
	sljit_set_label(value_done, sljit_emit_label(compiler));
}

static void EmitSljitPreaggregatedSumHugeintUpdate(struct sljit_compiler *compiler, idx_t lane_idx, idx_t state_offset,
                                                   bool initialize_states) {
	EmitSljitPreaggregatedLanePointer(compiler, lane_idx, SLJIT_R0,
	                                  offsetof(SljitNativePreaggregatedPrimitiveLaneInput, value_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0);
	auto value_is_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);

	EmitSljitPreaggregatedLanePointer(compiler, lane_idx, SLJIT_R0,
	                                  offsetof(SljitNativePreaggregatedPrimitiveLaneInput, hugeint_values));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
	auto state = BindSljitPreaggregatedStateMemory(compiler, lane_idx, state_offset);
	if (!initialize_states) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, state.memory, state.Field(offsetof(hugeint_t, lower)));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, state.memory, state.Field(offsetof(hugeint_t, upper)));
		EmitSljitAddDoubleWord(compiler, SLJIT_R2, SLJIT_R3, SLJIT_R5, SLJIT_R0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.Field(offsetof(hugeint_t, lower)), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.Field(offsetof(hugeint_t, upper)), SLJIT_R3, 0);
	if (initialize_states) {
		sljit_emit_op1(compiler, SLJIT_MOV, state.memory, state.Field(sizeof(hugeint_t)), SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, state.memory, state.Field(sizeof(hugeint_t)), SLJIT_IMM, 1);
	auto value_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(value_is_null, sljit_emit_label(compiler));
	if (initialize_states) {
		auto null_state = BindSljitPreaggregatedStateMemory(compiler, lane_idx, state_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, null_state.memory, null_state.Field(offsetof(hugeint_t, lower)), SLJIT_IMM,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV, null_state.memory, null_state.Field(offsetof(hugeint_t, upper)), SLJIT_IMM,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV, null_state.memory, null_state.Field(sizeof(hugeint_t)), SLJIT_IMM, 0);
	}
	sljit_set_label(value_done, sljit_emit_label(compiler));
}

static bool
SljitPreaggregatedPrimitiveLanesAreHomogeneous(const vector<AggregatePrimitiveUpdateKind> &primitive_kinds) {
	for (idx_t lane_idx = 1; lane_idx < primitive_kinds.size(); lane_idx++) {
		if (primitive_kinds[lane_idx] != primitive_kinds[0]) {
			return false;
		}
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativePreaggregatedPrimitiveUpdate(const vector<AggregatePrimitiveUpdateKind> &primitive_kinds,
                                             const vector<idx_t> &state_offsets, bool initialize_states,
                                             SljitNativePreaggregatedPrimitiveUpdateFunction &function, string &error,
                                             bool address_selected, bool row_selected) {
	function = nullptr;
	if (!SljitPrimitiveRunMachineWordSupported() || !SljitPrimitiveRunCodegenSupported() || primitive_kinds.empty() ||
	    primitive_kinds.size() != state_offsets.size()) {
		error = "unsupported SLJIT preaggregated primitive update target";
		return nullptr;
	}
	for (auto primitive_kind : primitive_kinds) {
		if (!SljitPreaggregatedPrimitiveUpdateKindSupported(primitive_kind)) {
			error = "unsupported SLJIT preaggregated primitive update kind";
			return nullptr;
		}
	}
	const auto use_homogeneous_lane_loop = primitive_kinds.size() > SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET &&
	                                       SljitPreaggregatedPrimitiveLanesAreHomogeneous(primitive_kinds);
	if (primitive_kinds.size() > SLJIT_PRIMITIVE_RUN_UNROLLED_LANE_BUDGET && !use_homogeneous_lane_loop) {
		error = "unsupported wide heterogeneous SLJIT preaggregated primitive state layout";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto local_size =
	    row_selected && use_homogeneous_lane_loop ? NumericCast<sljit_s32>(sizeof(sljit_sw)) : sljit_s32(0);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 7, 6, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, row_selected ? SLJIT_R6 : SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, addresses));
	if (!use_homogeneous_lane_loop) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, lane_inputs));
	}

	auto row_loop = sljit_emit_label(compiler);
	const auto logical_row_register = row_selected ? SLJIT_R6 : SLJIT_S1;
	auto rows_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, logical_row_register, 0, SLJIT_S2, 0);
	if (row_selected) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, execute_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R6), 2);
	}
	if (address_selected) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, address_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_R1), 3);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM2(SLJIT_S3, logical_row_register), 3);
	}
	if (use_homogeneous_lane_loop) {
		if (row_selected) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), 0, SLJIT_R6, 0);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePreaggregatedPrimitiveUpdateInput, lane_inputs));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_IMM, 0);
		auto lane_loop = sljit_emit_label(compiler);
		switch (primitive_kinds[0]) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
			EmitSljitPreaggregatedCountUpdate(compiler, DConstants::INVALID_INDEX, 0, initialize_states);
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			EmitSljitPreaggregatedSumInt64Update(compiler, DConstants::INVALID_INDEX, 0, initialize_states);
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			EmitSljitPreaggregatedSumHugeintUpdate(compiler, DConstants::INVALID_INDEX, 0, initialize_states);
			break;
		default:
			sljit_free_compiler(compiler);
			error = "unsupported SLJIT preaggregated primitive update lane";
			return nullptr;
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(sizeof(SljitNativePreaggregatedPrimitiveLaneInput)));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, SLJIT_R6, 0, SLJIT_IMM, 1);
		auto repeat_lane =
		    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R6, 0, SLJIT_IMM, NumericCast<sljit_sw>(primitive_kinds.size()));
		sljit_set_label(repeat_lane, lane_loop);
		if (row_selected) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_SP), 0);
		}
	} else {
		for (idx_t lane_idx = 0; lane_idx < primitive_kinds.size(); lane_idx++) {
			switch (primitive_kinds[lane_idx]) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
			case AggregatePrimitiveUpdateKind::COUNT:
				EmitSljitPreaggregatedCountUpdate(compiler, lane_idx, state_offsets[lane_idx], initialize_states);
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				EmitSljitPreaggregatedSumInt64Update(compiler, lane_idx, state_offsets[lane_idx], initialize_states);
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				EmitSljitPreaggregatedSumHugeintUpdate(compiler, lane_idx, state_offsets[lane_idx], initialize_states);
				break;
			default:
				sljit_free_compiler(compiler);
				error = "unsupported SLJIT preaggregated primitive update lane";
				return nullptr;
			}
		}
	}
	sljit_emit_op2(compiler, SLJIT_ADD, logical_row_register, 0, logical_row_register, 0, SLJIT_IMM, 1);
	auto next_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_row, row_loop);

	sljit_set_label(rows_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

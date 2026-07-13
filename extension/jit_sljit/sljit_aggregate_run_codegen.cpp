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

static bool SljitPrimitiveRunGroupCastSupported(PhysicalType source_type, PhysicalType target_type) {
	if (source_type == target_type) {
		return SljitPrimitiveRunGroupTypeSupported(source_type);
	}
	return (source_type == PhysicalType::INT64 && target_type == PhysicalType::INT32) ||
	       (source_type == PhysicalType::INT64 && target_type == PhysicalType::INT16) ||
	       (source_type == PhysicalType::INT32 && target_type == PhysicalType::INT8);
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

static void EmitSljitPrimitiveRunLoadExistingState(struct sljit_compiler *compiler, PhysicalType group_type,
	                                               AggregatePrimitiveUpdateKind primitive_kind) {
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeLoadOp(group_type), SLJIT_R0, 0,
	               SLJIT_MEM2(SLJIT_R6, SLJIT_R5), SljitPrimitiveRunPhysicalTypeScale(group_type));

	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_hugeint_values));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, SLJIT_R6, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_int64_values));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3);
}

static void EmitSljitPrimitiveRunStartGroup(struct sljit_compiler *compiler, PhysicalType group_type) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_group_data));
	sljit_emit_op1(compiler, SljitPrimitiveRunPhysicalTypeStoreOp(group_type), SLJIT_MEM2(SLJIT_R5, SLJIT_S3),
	               SljitPrimitiveRunPhysicalTypeScale(group_type), SLJIT_R0, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunAccumulate(struct sljit_compiler *compiler, PhysicalType payload_type,
	                                        AggregatePrimitiveUpdateKind primitive_kind) {
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
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
}

static void EmitSljitPrimitiveRunFlush(struct sljit_compiler *compiler,
	                                   AggregatePrimitiveUpdateKind primitive_kind) {
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R5, 0, SLJIT_R5, 0, SLJIT_IMM, 4);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_hugeint_values));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R6, 0, SLJIT_R6, 0, SLJIT_R5, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, lower), SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R6), offsetof(hugeint_t, upper), SLJIT_R2, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_int64_values));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3, SLJIT_R1, 0);
	}
	if (primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, output_value_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 0, SLJIT_IMM, 1);
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R5, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_row_counts));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R6, SLJIT_R5), 3, SLJIT_R3, 0);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativePrimitiveRunUpdate(PhysicalType group_source_type, PhysicalType group_type, PhysicalType payload_type,
	                               AggregatePrimitiveUpdateKind primitive_kind,
	                               SljitNativePrimitiveRunFunction &function, string &error) {
	function = nullptr;
	if (!SljitPrimitiveRunGroupCastSupported(group_source_type, group_type)) {
		error = "unsupported SLJIT primitive run group cast";
		return nullptr;
	}
	const bool needs_payload = primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                           primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	if ((needs_payload && !SljitPrimitiveRunPayloadTypeSupported(payload_type)) ||
	    (!needs_payload && primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
	     primitive_kind != AggregatePrimitiveUpdateKind::COUNT)) {
		error = "unsupported SLJIT primitive run aggregate payload";
		return nullptr;
	}
#if SLJIT_NUMBER_OF_SAVED_REGISTERS < 6 ||                                                                      \
    (SLJIT_NUMBER_OF_REGISTERS - SLJIT_NUMBER_OF_SAVED_REGISTERS) < 7
	error = "unsupported SLJIT primitive run register file";
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
	if (needs_payload) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePrimitiveRunInput, payload_data));
	}

	auto input_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto no_existing_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	EmitSljitPrimitiveRunLoadExistingState(compiler, group_type, primitive_kind);
	auto initial_group_matches = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);

	auto append_group = sljit_emit_label(compiler);
	sljit_set_label(no_existing_group, append_group);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_capacity));
	auto output_full = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R5, 0);
	EmitSljitPrimitiveRunStartGroup(compiler, group_type);

	auto accumulate_group = sljit_emit_label(compiler);
	sljit_set_label(initial_group_matches, accumulate_group);
	EmitSljitPrimitiveRunAccumulate(compiler, payload_type, primitive_kind);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto run_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitPrimitiveRunLoadKey(compiler, group_source_type, SLJIT_R4);
	auto same_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_R0, 0);
	sljit_set_label(same_group, accumulate_group);
	EmitSljitPrimitiveRunFlush(compiler, primitive_kind);
	auto next_group = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(next_group, append_group);

	auto flush_last_group = sljit_emit_label(compiler);
	sljit_set_label(run_done, flush_last_group);
	EmitSljitPrimitiveRunFlush(compiler, primitive_kind);

	auto finish = sljit_emit_label(compiler);
	sljit_set_label(input_done, finish);
	sljit_set_label(output_full, finish);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePrimitiveRunInput, output_count), SLJIT_S3, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
#endif
}

} // namespace duckdb

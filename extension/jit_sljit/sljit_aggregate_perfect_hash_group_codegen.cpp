//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_group_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "sljit_aggregate_source_hoist_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

static vector<sljit_s32> BuildSljitPerfectHashSourceDataPointerRegs(idx_t max_hoists, bool include_fast_validity_reg) {
	vector<sljit_s32> result;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	if (max_hoists > 0) {
		result.push_back(SLJIT_S8);
	}
	if (max_hoists > 1) {
		result.push_back(SLJIT_S9);
	}
	if (include_fast_validity_reg && max_hoists > 2) {
		result.push_back(SLJIT_S6);
	}
#endif
	return result;
}

vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads, idx_t max_hoists,
                                             bool include_fast_validity_reg) {
	auto regs = BuildSljitPerfectHashSourceDataPointerRegs(max_hoists, include_fast_validity_reg);
	// Payload data pointers are invariant for the complete generated row loop.
	// A source referenced by one payload is therefore still reused once per row;
	// requiring two payload-tree references leaves saved registers idle while the
	// hot loop reloads vector metadata. The perfect-hash planner decides whether
	// source or group invariants own S8/S9, so every source selected by that layout
	// belongs in a register.
	return BuildSljitAggregateSourceDataPointerHoists(payloads, regs, 1);
}

vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSpareFastSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads) {
	vector<sljit_s32> regs;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	regs.push_back(SLJIT_S6);
#endif
	return BuildSljitAggregateSourceDataPointerHoists(payloads, regs, 2);
}

void EmitSljitPerfectHashSetOutputGroup(struct sljit_compiler *compiler, sljit_s32 group_index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, group_index_reg), 0, SLJIT_IMM, 1);
}

void EmitSljitPerfectHashStatePointer(struct sljit_compiler *compiler, sljit_s32 group_index_reg,
                                      sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_tuple_size));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, group_index_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_state_data));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
}

void EmitLoadFusedAggregateGroupSourceIndex(struct sljit_compiler *compiler, idx_t group_idx, sljit_s32 target_reg,
                                            SljitFusedAggregateGroupIndexMode mode,
                                            sljit_s32 group_sel_array_base_reg) {
	if (mode == SljitFusedAggregateGroupIndexMode::LOGICAL) {
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
		return;
	}
	if (group_sel_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_sel_array_base_reg, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));
	}
	if (mode == SljitFusedAggregateGroupIndexMode::SELECTED_PRESENT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		return;
	}
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto no_group_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_group_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_group_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_group_index, sljit_emit_label(compiler));
}

struct sljit_jump *EmitFusedAggregateJumpIfGroupValidityNull(struct sljit_compiler *compiler, idx_t group_idx,
                                                             sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_validity_array));
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static void EmitLoadFusedAggregateGroupIntegerData(struct sljit_compiler *compiler, idx_t group_idx,
                                                   SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                                   sljit_s32 target_reg, bool use_hoisted_group_data,
                                                   sljit_s32 group_data_reg, sljit_s32 group_data_array_base_reg) {
	if (use_hoisted_group_data) {
		if (group_data_reg != SLJIT_R0) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_data_reg, 0);
		}
	} else if (group_data_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(group_data_array_base_reg),
		               SljitPointerArrayOffset(group_idx));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	}
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

static void EmitLoadFusedAggregateGroupMiniStringCompressData(struct sljit_compiler *compiler, idx_t group_idx,
                                                              sljit_s32 index_reg, sljit_s32 target_reg,
                                                              bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                                              bool may_be_empty, bool use_precomputed_string_offset,
                                                              sljit_s32 group_data_array_base_reg,
                                                              bool fuse_nonempty_string_compress_bias) {
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	if (use_hoisted_group_data) {
		if (group_data_reg != SLJIT_R0) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_data_reg, 0);
		}
	} else if (group_data_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(group_data_array_base_reg),
		               SljitPointerArrayOffset(group_idx));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	}
	if (!use_precomputed_string_offset) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index_reg, 0, SLJIT_IMM, SLJIT_STRING_T_SHIFT);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);

	if (string_t::PREFIX_LENGTH > 0) {
		if (!may_be_empty) {
			// UTINYINT string compression only appears for one-byte strings here; nonzero minima rule out empty keys.
			sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
			if (!fuse_nonempty_string_compress_bias) {
				sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_IMM, 1);
			}
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
		sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, SLJIT_R3, 0, target_reg, 0);
		auto not_empty = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 0);
		sljit_set_label(not_empty, sljit_emit_label(compiler));
		return;
	}

	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	auto empty_string = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R3, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
	auto have_first_byte = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R4), 0);
	sljit_set_label(have_first_byte, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, SLJIT_R3, 0, target_reg, 0);
	auto have_result = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(empty_string, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 0);

	sljit_set_label(have_result, sljit_emit_label(compiler));
}

static void EmitLoadFusedAggregateGroupTransformedIntegerData(struct sljit_compiler *compiler, idx_t group_idx,
                                                              const SljitPerfectHashGroupPlan &group,
                                                              sljit_s32 index_reg, sljit_s32 target_reg,
                                                              bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                                              sljit_s32 group_data_array_base_reg) {
	if (use_hoisted_group_data) {
		if (group_data_reg != SLJIT_R0) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_data_reg, 0);
		}
	} else if (group_data_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(group_data_array_base_reg),
		               SljitPointerArrayOffset(group_idx));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	}
	sljit_emit_op1(compiler, NativeSignedIntegerLoadOp(group.integer_source_width), target_reg, 0,
	               SLJIT_MEM2(SLJIT_R0, index_reg), NativeSignedIntegerDataScale(group.integer_source_width));
	if (group.expression_kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS) {
		sljit_emit_op2(compiler, SLJIT_SUB, target_reg, 0, target_reg, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(group.integer_source_minimum));
	}
}

void EmitLoadFusedAggregateGroupData(struct sljit_compiler *compiler, idx_t group_idx,
                                     const SljitPerfectHashGroupPlan &group, sljit_s32 index_reg, sljit_s32 target_reg,
                                     bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                     bool use_precomputed_string_offset, sljit_s32 group_data_array_base_reg,
                                     bool fuse_nonempty_string_compress_bias) {
	if (group.expression_kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
	    group.expression_kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
		EmitLoadFusedAggregateGroupTransformedIntegerData(compiler, group_idx, group, index_reg, target_reg,
		                                                  use_hoisted_group_data, group_data_reg,
		                                                  group_data_array_base_reg);
		return;
	}
	if (group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
		EmitLoadFusedAggregateGroupMiniStringCompressData(
		    compiler, group_idx, index_reg, target_reg, use_hoisted_group_data, group_data_reg, group.minimum == 0,
		    use_precomputed_string_offset, group_data_array_base_reg, fuse_nonempty_string_compress_bias);
		return;
	}
	EmitLoadFusedAggregateGroupIntegerData(compiler, group_idx, group.integer_kind, index_reg, target_reg,
	                                       use_hoisted_group_data, group_data_reg, group_data_array_base_reg);
}

sljit_s32 SljitPerfectHashGroupDataPointerReg(idx_t group_idx) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	switch (group_idx) {
	case 0:
		return SLJIT_S8;
	case 1:
		return SLJIT_S9;
	default:
		break;
	}
#endif
	throw InternalException("SLJIT perfect-hash group data register is out of range");
}

sljit_s32 SljitPerfectHashSourceDataPointerReg(idx_t hoist_idx, bool include_fast_validity_reg) {
	if (hoist_idx < 2) {
		return SljitPerfectHashGroupDataPointerReg(hoist_idx);
	}
	if (include_fast_validity_reg && hoist_idx == 2) {
		return SLJIT_S6;
	}
	throw InternalException("SLJIT perfect-hash source data register is out of range");
}

} // namespace duckdb

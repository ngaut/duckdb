//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_chain_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

struct SljitHashJoinNextPointerLoadJumps {
	struct sljit_jump *dictionary_loaded;
	struct sljit_jump *direct_loaded;
};

static inline sljit_sw SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind kind) {
	return static_cast<sljit_sw>(static_cast<int8_t>(kind));
}

static inline void EmitLoadHashJoinProbeLayoutKind(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, layout_kind));
}

static inline SljitHashJoinNextPointerLoadJumps
EmitLoadHashJoinRuntimeChainNextPointer(struct sljit_compiler *compiler, sljit_s32 row_pointer, sljit_s32 scratch,
                                        sljit_s32 next_pointer, idx_t pointer_offset) {
	EmitLoadHashJoinProbeLayoutKind(compiler, scratch);
	auto direct_pointer = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM,
	                                     SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind::CHAIN_DIRECT));
	auto salt_direct_pointer =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM,
	                   SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind::CHAIN_SALT_DIRECT));

	sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(pointer_offset));
	sljit_emit_op1(compiler, SLJIT_MOV_P, next_pointer, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, aux_next_ptrs));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(next_pointer, scratch), 3);
	auto dictionary_loaded = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto direct_label = sljit_emit_label(compiler);
	sljit_set_label(direct_pointer, direct_label);
	sljit_set_label(salt_direct_pointer, direct_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(pointer_offset));
	return {dictionary_loaded, sljit_emit_jump(compiler, SLJIT_JUMP)};
}

static inline void EmitLoadHashJoinNextPointer(struct sljit_compiler *compiler, sljit_s32 row_pointer,
                                               sljit_s32 scratch, sljit_s32 next_pointer, idx_t pointer_offset,
                                               const SljitHashJoinProbeCodegenConfig &config,
                                               sljit_s32 aux_next_ptrs_reg = 0) {
	if (!config.HasRuntimeLayout()) {
		if (!config.ChainsLongerThanOne()) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_IMM, 0);
			return;
		}
		if (config.UsesDictionaryEmission()) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer),
			               NumericCast<sljit_sw>(pointer_offset));
			if (aux_next_ptrs_reg != 0) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(aux_next_ptrs_reg, scratch), 3);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV_P, next_pointer, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeRegularHashJoinProbeInput, aux_next_ptrs));
				sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(next_pointer, scratch), 3);
			}
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM1(row_pointer),
		               NumericCast<sljit_sw>(pointer_offset));
		return;
	}

	EmitLoadHashJoinProbeLayoutKind(compiler, scratch);
	auto no_chain = sljit_emit_cmp(compiler, SLJIT_LESS, scratch, 0, SLJIT_IMM,
	                               SljitHashJoinProbeLayoutImmediate(SljitHashJoinProbeLayoutKind::CHAIN_DIRECT));

	auto pointer_loaded =
	    EmitLoadHashJoinRuntimeChainNextPointer(compiler, row_pointer, scratch, next_pointer, pointer_offset);

	sljit_set_label(no_chain, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_IMM, 0);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(pointer_loaded.dictionary_loaded, done_label);
	sljit_set_label(pointer_loaded.direct_loaded, done_label);
}

static inline void EmitMarkHashJoinBuildChain(struct sljit_compiler *compiler, sljit_s32 row_pointer, sljit_s32 scratch,
                                              sljit_s32 next_pointer, idx_t found_match_offset, idx_t pointer_offset,
                                              const SljitHashJoinProbeCodegenConfig &config,
                                              sljit_s32 aux_next_ptrs_reg = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(found_match_offset));
	auto already_marked = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	auto mark_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(found_match_offset),
	               SLJIT_IMM, 1);
	if (config.SpecializesNoChainLayout()) {
		auto done = sljit_emit_label(compiler);
		sljit_set_label(already_marked, done);
		return;
	}
	EmitLoadHashJoinNextPointer(compiler, row_pointer, scratch, next_pointer, pointer_offset, config,
	                            aux_next_ptrs_reg);
	auto no_next = sljit_emit_cmp(compiler, SLJIT_EQUAL, row_pointer, 0, SLJIT_IMM, 0);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, mark_loop);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(already_marked, done);
	sljit_set_label(no_next, done);
}

} // namespace duckdb

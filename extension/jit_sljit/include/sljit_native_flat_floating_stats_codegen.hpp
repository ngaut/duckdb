#pragma once

#include "sljit_native_flat_loop_codegen.hpp"
#include "sljit_native_types.hpp"

#include "duckdb/common/common.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

sljit_s32 SljitFlatFloatingProjectionStatsMinRegister(idx_t projection_idx);
sljit_s32 SljitFlatFloatingProjectionStatsMaxRegister(idx_t projection_idx);

void EmitSljitFlatFloatingStatsInit(struct sljit_compiler *compiler, sljit_s32 move_op, sljit_s32 value_reg,
                                    sljit_s32 min_reg, sljit_s32 max_reg);
void EmitSljitFlatFloatingStatsUpdate(struct sljit_compiler *compiler, sljit_s32 move_op, bool single_precision,
                                      sljit_s32 value_reg, sljit_s32 min_reg, sljit_s32 max_reg);
void EmitSljitStoreFlatFloatingStats(struct sljit_compiler *compiler, idx_t projection_index, bool single_precision,
                                     sljit_s32 min_reg, sljit_s32 max_reg, sljit_s32 stats_min_base,
                                     sljit_s32 stats_max_base);

template <class EMIT_ROW, class EMIT_INCREMENT, class EMIT_STORE_STATS>
static inline void EmitSljitFlatFloatingOptionalStatsLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row,
                                                          EMIT_INCREMENT &&emit_increment,
                                                          EMIT_STORE_STATS &&emit_store_stats) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_stats_min));
	auto no_stats = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	auto stats_empty = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
	emit_row(true, true);
	emit_increment();
	auto stats_loop = sljit_emit_label(compiler);
	auto stats_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_row(false, true);
	emit_increment();
	auto repeat_stats = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_stats, stats_loop);
	sljit_set_label(stats_done, sljit_emit_label(compiler));
	emit_store_stats();
	auto stats_finished = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto no_stats_loop_label = sljit_emit_label(compiler);
	sljit_set_label(no_stats, no_stats_loop_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto done_label = EmitSljitFlatCountedScalarLoop(compiler, [&]() { emit_row(false, false); }, emit_increment);
	sljit_set_label(stats_empty, done_label);
	sljit_set_label(stats_finished, done_label);
}

} // namespace duckdb

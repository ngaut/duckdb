#pragma once

#include "duckdb/common/common.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitFlatScalarLoopLabels {
	sljit_label *loop;
	sljit_label *done;
};

template <class EMIT_ROW, class EMIT_INCREMENT>
static inline sljit_label *EmitSljitFlatCountedScalarLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row,
                                                          EMIT_INCREMENT &&emit_increment) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_row();
	emit_increment();
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	return done_label;
}

template <class EMIT_SCALAR_ROW, class EMIT_ADVANCE>
static inline SljitFlatScalarLoopLabels
EmitSljitFlatRemainingScalarLoop(struct sljit_compiler *compiler, sljit_s32 count_reg, sljit_sw scalar_data_width,
                                 EMIT_SCALAR_ROW &&emit_scalar_row, EMIT_ADVANCE &&emit_advance) {
	auto scalar_loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, count_reg, 0, SLJIT_IMM, 0);
	emit_scalar_row();
	emit_advance(scalar_data_width);
	sljit_emit_op2(compiler, SLJIT_SUB, count_reg, 0, count_reg, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, scalar_loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	return {scalar_loop, done_label};
}

template <class EMIT_ROW, class EMIT_ADVANCE>
static inline sljit_label *EmitSljitFlatUnrolledScalarLoop(struct sljit_compiler *compiler, sljit_s32 count_reg,
                                                           sljit_sw data_width, sljit_sw unroll_count,
                                                           EMIT_ROW &&emit_row, EMIT_ADVANCE &&emit_advance) {
	D_ASSERT(unroll_count > 1);

	auto unrolled_loop = sljit_emit_label(compiler);
	auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, count_reg, 0, SLJIT_IMM, unroll_count);
	for (sljit_sw row = 0; row < unroll_count; row++) {
		emit_row(row * data_width);
	}
	emit_advance(data_width * unroll_count);
	sljit_emit_op2(compiler, SLJIT_SUB, count_reg, 0, count_reg, 0, SLJIT_IMM, unroll_count);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, unrolled_loop);

	auto scalar_labels =
	    EmitSljitFlatRemainingScalarLoop(compiler, count_reg, data_width, [&]() { emit_row(0); }, emit_advance);
	sljit_set_label(tail, scalar_labels.loop);
	return scalar_labels.done;
}

template <class EMIT_VECTOR_ROW, class EMIT_SCALAR_ROW, class EMIT_ADVANCE>
static inline sljit_label *
EmitSljitFlatSimdThenScalarTailLoop(struct sljit_compiler *compiler, sljit_s32 count_reg, sljit_sw simd_lanes,
                                    sljit_sw simd_data_width, sljit_sw scalar_data_width,
                                    EMIT_VECTOR_ROW &&emit_vector_row, EMIT_SCALAR_ROW &&emit_scalar_row,
                                    EMIT_ADVANCE &&emit_advance) {
	D_ASSERT(simd_lanes > 1);
	D_ASSERT(simd_data_width > scalar_data_width);

	auto vector_loop = sljit_emit_label(compiler);
	auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, count_reg, 0, SLJIT_IMM, simd_lanes);
	emit_vector_row();
	emit_advance(simd_data_width);
	sljit_emit_op2(compiler, SLJIT_SUB, count_reg, 0, count_reg, 0, SLJIT_IMM, simd_lanes);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, vector_loop);

	auto scalar_labels =
	    EmitSljitFlatRemainingScalarLoop(compiler, count_reg, scalar_data_width, emit_scalar_row, emit_advance);
	sljit_set_label(tail, scalar_labels.loop);
	return scalar_labels.done;
}

} // namespace duckdb

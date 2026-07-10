//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_simd_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/execution_expression_ir.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitTypedExpressionTreeSimdPlan {
	bool supported = false;
	sljit_s32 simd_type = 0; // SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_32/64
	sljit_s32 elem_scale = 0; // 2 (32-bit) or 3 (64-bit)
	sljit_s32 lanes = 0;      // 4 or 2
	idx_t constant_count = 0;
	idx_t node_count = 0;
	idx_t max_live_temps = 0; // peak simultaneous vector temporaries (register pressure)
	bool needs_all_ones = false;
};

// Returns a supported plan iff the boolean predicate can be evaluated with
// packed SIMD ops profitably on the current architecture (single element width,
// no overflow-trapping arithmetic, packed ops available for every op).
SljitTypedExpressionTreeSimdPlan TryPlanSljitTypedExpressionTreeSimd(const ExecutionExpressionIR &root);

// Emits the packed-lane predicate loop for a boolean select. Assumes the flat
// all-valid fast path context (S1 = flat row base = 0 on entry, S2 = count,
// S5 = source_data_array). Advances S1 to the last full lane group; the caller's
// scalar fast loop then handles the < lanes tail. `mask_offset` is a 16-byte,
// 16-aligned scratch slot in the local frame.
void EmitSljitTypedExpressionTreeSimdSelectLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                                const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw mask_offset);

// Appends `index_reg` to the true-selection vector (shared with the scalar path).
void EmitStoreSljitTypedExpressionTreeTrueSelection(struct sljit_compiler *compiler, sljit_s32 index_reg);

// Emits the packed-lane COUNT(*)-filter loop: counts rows matching `root` with
// packed SIMD and adds the total into the running count at `count_offset`.
// Assumes the flat all-valid context (S1 = 0 on entry, S2 = count, S5 =
// source_data_array). Advances S1 to the last full lane group; the caller's
// scalar fast loop handles the < lanes tail. `mask_offset` is a 16-byte,
// 16-aligned scratch slot in the local frame.
void EmitSljitTypedExpressionTreeSimdCountLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                               const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw count_offset,
                                               sljit_sw mask_offset);

// Plan for a pure integer value expression (references, constants, add/sub/mul) at a
// required element width; used to gate SUM payloads (which must be values, not masks).
SljitTypedExpressionTreeSimdPlan TryPlanSljitTypedExpressionTreeSimdValue(const ExecutionExpressionIR &root,
                                                                         sljit_s32 want_scale);

// Emits the packed-lane SUM(payload) filter loop: for a 4-lane int32 predicate and a
// 32-bit column payload, accumulates the masked payload (widened to int64) and the
// match count, then folds both into sum_offset / count_offset and sets the aggregate
// "seen a value" flag at saw_value_offset. Assumes the flat all-valid context
// (S1 = 0 on entry, S2 = count, S5 = source_data_array). Advances S1 to the last full
// lane group; the caller's scalar fast loop handles the < lanes tail. scratch_offset
// is a 16-byte, 16-aligned scratch slot in the local frame.
void EmitSljitTypedExpressionTreeSimdSumLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &predicate,
                                             const ExecutionExpressionIR &payload,
                                             const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw sum_offset,
                                             sljit_sw count_offset, sljit_sw saw_value_offset, sljit_sw scratch_offset);

} // namespace duckdb

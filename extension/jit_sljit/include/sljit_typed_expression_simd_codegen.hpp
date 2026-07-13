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

#include <functional>

namespace duckdb {

struct SljitTypedExpressionTreeSimdPlan {
	bool supported = false;
	sljit_s32 simd_type = 0;  // SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_32/64
	sljit_s32 elem_scale = 0; // 2 (32-bit) or 3 (64-bit)
	sljit_s32 lanes = 0;      // 4 or 2
	idx_t constant_count = 0;
	idx_t node_count = 0;
	idx_t scalar_operation_count = 0; // comparisons, arithmetic, and conjunction combines
	idx_t max_live_temps = 0;         // peak simultaneous vector temporaries (register pressure)
	bool needs_all_ones = false;
	bool root_is_conjunction = false;
	bool has_or = false;
	// The predicate mixes 32-bit and 64-bit comparisons: the loop runs 4 lanes at
	// 32-bit width and 64-bit comparison masks are evaluated per half and narrowed.
	bool mixed_width = false;
	// AND-only tree: a row with any NULL referenced source cannot pass (SQL
	// three-valued logic), so the packed loops may run on nullable flat data by
	// ANDing a lane-expanded validity mask into the predicate mask. OR trees are
	// excluded (NULL OR TRUE is TRUE).
	bool nullable_capable = false;
	// Distinct source indices referenced by the predicate (validity mask inputs).
	vector<idx_t> source_refs;
};

// Returns a supported plan iff the boolean predicate can be evaluated with
// packed SIMD ops profitably on the current architecture (single element width,
// no overflow-trapping arithmetic, packed ops available for every op).
SljitTypedExpressionTreeSimdPlan TryPlanSljitTypedExpressionTreeSimd(const ExecutionExpressionIR &root);

// Hybrid loops vectorize only the predicate and retain scalar terminal work. This
// cost contract rejects predicates whose saved scalar operations cannot amortize
// mask classification and lane dispatch. AND hybrids evaluate the complete mask
// branchlessly and classify it once; OR hybrids stay scalar.
// Fully packed select/count/sum loops do not use this gate because their
// terminals vectorize too.
bool SljitTypedExpressionTreeSimdHybridFilterProfitable(const SljitTypedExpressionTreeSimdPlan &plan);

// Emits the packed-lane predicate loop for a boolean select. Assumes the flat
// all-valid fast path context (S1 = flat row base = 0 on entry, S2 = count,
// S5 = source_data_array). Uses S3/S4 as a packed-loop-local output cursor,
// advances S1 to the last full lane group, and writes selected_count back before
// the caller's scalar fast loop handles the < lanes tail. `mask_offset` is a
// 16-byte, 16-aligned scratch slot in the local frame.
void EmitSljitTypedExpressionTreeSimdSelectLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                                const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw mask_offset,
                                                const vector<idx_t> *validity_refs = nullptr);

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
                                               sljit_sw mask_offset, const vector<idx_t> *validity_refs = nullptr);

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
                                             sljit_sw count_offset, sljit_sw saw_value_offset, sljit_sw scratch_offset,
                                             const vector<idx_t> *validity_refs = nullptr);

// Emits the hybrid packed-mask filter loop: the predicate mask for `lanes` rows is
// computed with packed SIMD, then `emit_matching_row` is invoked once per lane to
// emit the scalar work for a matching row (S1 holds that row's flat index; the
// callback must preserve S1/S2/S5 and may clobber R0-R4). S3/S4/S6 are
// callback-owned saved registers: the shared loop neither reads nor writes them.
// Used when the payloads have no packed form (arbitrary aggregate payload kinds). Uniform groups advance
// directly; mixed groups test a compact scalar mask without recomputing the
// predicate, so rejected lanes do not pay scalar predicate or payload work.
// Assumes the flat all-valid context; advances S1 to the last full lane group,
// and the caller's scalar fast loop handles the tail. `mask_offset` is a 24-byte,
// 16-aligned scratch block: the first 16 bytes retain nullable lane bits and the
// final word holds the mixed mask.
void EmitSljitTypedExpressionTreeSimdHybridFilterLoop(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &predicate,
                                                      const SljitTypedExpressionTreeSimdPlan &plan,
                                                      sljit_sw mask_offset,
                                                      const std::function<void()> &emit_matching_row,
                                                      const vector<idx_t> *validity_refs = nullptr);

} // namespace duckdb

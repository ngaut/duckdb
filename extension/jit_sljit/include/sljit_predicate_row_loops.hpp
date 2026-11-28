//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_row_loops.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

//! The rows of a flat batch without NULLs from S1 to the count: a row the predicate rejects takes only the back-edge,
//! and a row it keeps jumps to emit_row and a back-edge of its own. Returns the loop's exits
template <class EMIT_ROW>
vector<sljit_jump *> EmitSljitFlatPredicateLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &predicate,
                                                vector<SljitArithmeticOverflowSite> &overflows,
                                                const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                                EMIT_ROW emit_row) {
	vector<sljit_jump *> exits {sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0)};
	auto loop = sljit_emit_label(compiler);
	idx_t spill_index = 0;
	auto kept = EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, predicate, spill_index, overflows, hoists);
	exits.push_back(EmitSljitNativeVectorLatch(compiler, loop));
	SetSljitJumpLabels(kept, sljit_emit_label(compiler));
	emit_row();
	exits.push_back(EmitSljitNativeVectorLatch(compiler, loop));
	return exits;
}

//! The rows of a flat batch that may hold NULLs, from S1 = 0 to the count, under a predicate that decides NULL as
//! FALSE (none keeps every row): a loop whose back-edge ends every rejected row. A predicate that ANDs or ORs bits
//! tests its raw values first, so a row it rejects on the leftover values of its NULLs falls straight to the back-edge,
//! and only a row it accepts tests its NULLs. emit_row_start runs before the predicate, emit_row for each kept row.
//! Returns the loop's exits
template <class EMIT_ROW_START, class EMIT_ROW>
vector<sljit_jump *> EmitSljitFlatNullablePredicateLoop(struct sljit_compiler *compiler,
                                                        optional_ptr<const ExecutionExpressionIR> predicate,
                                                        vector<SljitArithmeticOverflowSite> &overflows,
                                                        const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                                        EMIT_ROW_START emit_row_start, EMIT_ROW emit_row) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	vector<sljit_jump *> exits {sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0)};
	auto loop = sljit_emit_label(compiler);
	emit_row_start();
	vector<sljit_jump *> rejected;
	if (predicate) {
		if (SljitTypedExpressionTreeEvaluatesAnyBits(*predicate)) {
			idx_t raw_spill_index = 0;
			auto raw_true =
			    EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, *predicate, raw_spill_index, overflows, hoists);
			exits.push_back(EmitSljitNativeVectorLatch(compiler, loop));
			SetSljitJumpLabels(raw_true, sljit_emit_label(compiler));
		}
		idx_t spill_index = 0;
		rejected = EmitSljitTypedExpressionTreeNullableFastJumps(compiler, *predicate, false, spill_index, overflows,
		                                                         SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
	}
	emit_row();
	SetSljitJumpLabels(rejected, sljit_emit_label(compiler));
	exits.push_back(EmitSljitNativeVectorLatch(compiler, loop));
	return exits;
}

//! The rows of a flat batch from S1 to the count, under a predicate over rows without NULLs: a row with a NULL in one
//! of source_refs is rejected before the predicate runs, and emit_row runs for each kept row. Returns the loop's exit
template <class EMIT_ROW>
sljit_jump *
EmitSljitSourcesCheckedPredicateLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &predicate,
                                     const vector<idx_t> &source_refs, vector<SljitArithmeticOverflowSite> &overflows,
                                     const vector<SljitTypedExpressionTreeLoopHoist> *hoists, EMIT_ROW emit_row) {
	vector<sljit_jump *> done;
	auto loop = EmitSljitRowLoopHead(compiler, done);
	vector<sljit_jump *> rejected;
	for (auto source_index : source_refs) {
		rejected.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
	}
	idx_t spill_index = 0;
	auto predicate_false = EmitSljitTypedExpressionTreeFastJumps(compiler, predicate, false, spill_index, overflows,
	                                                             SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
	rejected.insert(rejected.end(), predicate_false.begin(), predicate_false.end());
	emit_row();
	SetSljitJumpLabels(rejected, sljit_emit_label(compiler));
	return EmitSljitRowLoopLatchExit(compiler, loop, done);
}

} // namespace duckdb

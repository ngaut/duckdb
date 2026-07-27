//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_native_predicate_simd_plan.hpp"
#include "sljit_predicate_codegen_helpers.hpp"

#include <cstddef>

namespace duckdb {

static void EmitResetPredicateLoopState(struct sljit_compiler *compiler, bool materialize_result) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);
	}
}

static vector<sljit_jump *> EmitSljitPartialSimdFallbackChecks(struct sljit_compiler *compiler,
                                                               const SljitNativePredicatePartialSimdPlan &partial,
                                                               const vector<bool> &source_not_null) {
	vector<sljit_jump *> fallback_jumps;
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, execute_sel));
	fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, false_sel));
	fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, true_sel));
	fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	// Four complete groups amortize constants, mask classification, and residual
	// dispatch. Smaller batches retain the existing scalar kernel.
	fallback_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S2, 0, SLJIT_IMM, NumericCast<sljit_sw>(partial.simd.lanes * 4)));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_sel));
	fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	for (auto source_ref : partial.simd.source_refs) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_ref));
		fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
	}

	bool check_validity = false;
	for (auto source_ref : partial.simd.source_refs) {
		if (source_ref >= source_not_null.size() || !source_not_null[source_ref]) {
			check_validity = true;
			break;
		}
	}
	if (check_validity) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, source_validity));
		fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
		for (auto source_ref : partial.simd.source_refs) {
			if (source_ref < source_not_null.size() && source_not_null[source_ref]) {
				continue;
			}
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
			               SljitPointerArrayOffset(source_ref));
			fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
		}
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	fallback_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S5, 0, SLJIT_IMM, 0));
	return fallback_jumps;
}

static void EmitStoreSljitPartialSimdMatch(struct sljit_compiler *compiler) {
	const auto output_selection = GetSljitNativeVectorRegisterLayout().optional_invariant;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(output_selection, SLJIT_R2), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R2, 0);
}

static void EmitSljitPartialSimdResidualRow(struct sljit_compiler *compiler, const SljitNativePredicate &residual,
                                            vector<shared_ptr<void>> &owned_data) {
	// The partial path admits only a flat execute selection, so the packed row
	// index is also the logical result index. Residual sources may still carry
	// their own selections and validity, which the ordinary predicate emitter
	// resolves from S3 without any TPCH-specific assumptions.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
	auto branches = EmitSljitPredicateBranches(compiler, residual, owned_data, residual.source_not_null, false);

	auto matched = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.true_jumps, matched);
	EmitStoreSljitPartialSimdMatch(compiler);
	auto match_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto rejected = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.false_jumps, rejected);
	SetPredicateJumpLabels(branches.null_jumps, rejected);
	sljit_set_label(match_done, sljit_emit_label(compiler));
}

static sljit_jump *EmitSljitNativePredicateLoop(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                                bool materialize_result, vector<shared_ptr<void>> &owned_data,
                                                bool sources_all_valid) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);

	auto branches =
	    EmitSljitPredicateBranches(compiler, predicate, owned_data, predicate.source_not_null, sources_all_valid);
	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.false_jumps, false_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}
	auto next_after_false = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto true_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.true_jumps, true_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	} else {
		EmitPredicateStoreTrueSelection(compiler);
	}
	auto next_after_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.null_jumps, null_label);
	if (materialize_result) {
		EmitStorePredicateResultInvalid(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_false, next_label);
	sljit_set_label(next_after_true, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	return done;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePredicate(const SljitNativePredicate &predicate,
                                                                bool materialize_result,
                                                                SljitNativePredicateFunction &function, string &error,
                                                                const ExecutionExpressionIR *typed_root,
                                                                bool *used_partial_simd) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto partial_simd = !materialize_result && typed_root
	                        ? TryPlanSljitNativePredicatePartialSimd(*typed_root, predicate.source_not_null)
	                        : SljitNativePredicatePartialSimdPlan();
	const auto &native_registers = GetSljitNativeVectorRegisterLayout();
	if (partial_simd.supported && !native_registers.HasOptionalInvariant()) {
		partial_simd = SljitNativePredicatePartialSimdPlan();
	}
	if (used_partial_simd) {
		*used_partial_simd = partial_simd.supported;
	}
	auto local_size = NumericCast<sljit_sw>(materialize_result ? 0 : SLJIT_SELECT_LOCAL_SIZE);
	if (SljitPredicateDoubleCompareUsesHelper(predicate)) {
		local_size = SLJIT_SELECT_LOCAL_SIZE + sizeof(double) * 2;
	}
	sljit_sw simd_mask_offset = 0;
	sljit_s32 scratches = 5 | SLJIT_ENTER_FLOAT(3);
	sljit_s32 saved_registers = 5;
	if (partial_simd.supported) {
		simd_mask_offset = (local_size + 15) & ~sljit_sw(15);
		local_size = simd_mask_offset + 24;
		auto vector_register_count = partial_simd.simd.constant_count + partial_simd.simd.max_live_temps +
		                             (partial_simd.simd.needs_all_ones ? idx_t(1) : idx_t(0));
		// ARM64 keeps a horizontal mask classifier live across the packed
		// predicate, avoiding a full movemask for uniform groups.
		if (GetSljitTargetCapabilities().IsArm64()) {
			vector_register_count++;
		}
		scratches |= SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_register_count));
		saved_registers = native_registers.saved_register_count;
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), scratches, saved_registers, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));

	vector<shared_ptr<void>> owned_data;
	vector<sljit_jump *> done_jumps;
	if (partial_simd.supported) {
		auto fallback_jumps = EmitSljitPartialSimdFallbackChecks(compiler, partial_simd, predicate.source_not_null);
		// Keep the append target live through packed prefix classification and the
		// scalar residual. This removes two ABI pointer loads and one null branch
		// from every matching row.
		sljit_emit_op1(compiler, SLJIT_MOV_P, native_registers.optional_invariant, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, true_sel));
		EmitResetPredicateLoopState(compiler, false);
		EmitSljitTypedExpressionTreeSimdHybridFilterLoop(
		    compiler, *partial_simd.packed_prefix, partial_simd.simd, simd_mask_offset,
		    [&]() { EmitSljitPartialSimdResidualRow(compiler, *partial_simd.residual, owned_data); });
		// Fewer than one packed group remains. Preserve the running append count and
		// evaluate the complete predicate for this bounded tail.
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, false, owned_data, false));
		SetPredicateJumpLabels(fallback_jumps, sljit_emit_label(compiler));
	}
	if (SljitPredicateUsesSourceValidity(predicate, predicate.source_not_null)) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, sources_all_valid));
		auto generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, true));
		sljit_set_label(generic_loop, sljit_emit_label(compiler));
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, false));
	} else {
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, false));
	}

	auto done_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(done_jumps, done_label);
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, selected_count),
		               SLJIT_R0, 0);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error, std::move(owned_data));
}

} // namespace duckdb

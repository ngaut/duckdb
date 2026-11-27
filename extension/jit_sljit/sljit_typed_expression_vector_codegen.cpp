//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_vector_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_source_hoist_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_register_layout.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

// Stores the root value held in R2 into result_data[S1], whose pointer result_data_reg holds when it is not 0. The
// element layout follows the root's physical type: a narrow DECIMAL128 root is exact in its 64-bit register and is
// sign-extended into the 16-byte hugeint element. Clobbers R0, R1 and R3.
static void EmitStoreSljitTypedExpressionTreeVectorResult(struct sljit_compiler *compiler,
                                                          const ExecutionExpressionIR &root,
                                                          SljitNativeIntegerKind result_kind,
                                                          sljit_s32 result_data_reg = 0) {
	if (result_data_reg) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, result_data_reg, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
	}
	if (SljitTypedExpressionTreeIsNarrowDecimal128Node(root)) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R1, 0);
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R3, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper), SLJIT_R3, 0);
		return;
	}
	sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
}

//! Stores S3 as the result's validity word at the word index of row S1 - row_bias
static void EmitStoreSljitTypedExpressionTreeValidityWord(struct sljit_compiler *compiler, sljit_sw row_bias) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_validity));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, row_bias);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3, SLJIT_S3, 0);
}

//! CASE WHEN c THEN v END over flat valid rows from row 0 (SljitTypedExpressionTreeIsFastGuardedValue): every row
//! stores v and takes c as its validity bit without a branch. S3, which indexes selected rows only in the other loops,
//! collects the bits of the current validity word, stored whole after the word's last row.
static void EmitSljitTypedExpressionTreeGuardedVectorLoop(struct sljit_compiler *compiler,
                                                          const ExecutionExpressionIR &root,
                                                          SljitNativeIntegerKind result_kind, sljit_s32 result_data_reg,
                                                          const vector<SljitTypedExpressionTreeLoopHoist> *hoists,
                                                          vector<SljitArithmeticOverflowSite> &overflows,
                                                          vector<sljit_jump *> &exits) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	idx_t spill_index = 0;
	EmitSljitTypedExpressionTreeFastGuardReg(compiler, root, spill_index, overflows,
	                                         SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
	sljit_emit_op2(compiler, SLJIT_MSHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_R2, 0);
	EmitSljitTypedExpressionTreeFastGuardedValueReg(compiler, root, spill_index, overflows,
	                                                SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
	EmitStoreSljitTypedExpressionTreeVectorResult(compiler, root, result_kind, result_data_reg);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op2u(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_S1, 0, SLJIT_IMM, 63);
	auto word_open = sljit_emit_jump(compiler, SLJIT_NOT_ZERO);
	EmitStoreSljitTypedExpressionTreeValidityWord(compiler, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_set_label(word_open, sljit_emit_label(compiler));
	sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), loop);
	// the rows ended inside a word: store its bits so far
	sljit_emit_op2u(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_S1, 0, SLJIT_IMM, 63);
	exits.push_back(sljit_emit_jump(compiler, SLJIT_ZERO));
	EmitStoreSljitTypedExpressionTreeValidityWord(compiler, 0);
	exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeTypedExpressionTree(const ExecutionExpressionIR &root,
                                                                          SljitNativeIntegerKind result_kind,
                                                                          SljitNativeVectorFunction &function,
                                                                          string &error,
                                                                          bool emit_flat_nullable_fast_path) {
	const auto tree_plan = BuildSljitTypedExpressionTreePlan(root, emit_flat_nullable_fast_path);
	if (!tree_plan.supported) {
		error =
		    "SLJIT typed expression-tree codegen only supports INT64/BOOLEAN arithmetic, comparisons, conjunctions, "
		    "null checks, coalesce, and case";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto &fast_path = tree_plan.fast_path;
	// Rows whose sources are flat and valid run the fast loop; a row whose value is NULL (a NULL CASE result) clears
	// its validity bit
	const auto fast_loop_supported = fast_path.FastPathSupportedWithNullExits();
	auto local_size = NumericCast<sljit_sw>(tree_plan.node_count * sizeof(sljit_sw) * 3);
	// A boolean root over flat valid rows computes whole lane groups as packed masks
	const auto simd_plan = fast_path.fast_path_supported && root.return_type.id() == LogicalTypeId::BOOLEAN
	                           ? TryPlanSljitTypedExpressionTreeSimd(root)
	                           : SljitTypedExpressionTreeSimdPlan();
	sljit_s32 scratches = 5;
	if (simd_plan.supported) {
		scratches |=
		    SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(SljitTypedExpressionTreeSimdMaskRegisterCount(simd_plan)));
	}
	// The fast loop keeps its loop invariants out of the row: the result and source data pointers in registers, wide
	// constants in stack slots
	const auto &registers = GetSljitProjectionRegisterLayout();
	vector<SljitTypedExpressionTreeLoopHoist> fast_hoists;
	sljit_s32 result_data_reg = 0;
	if (fast_loop_supported) {
		vector<reference<const ExecutionExpressionIR>> expressions {root};
		if (registers.has_pointer_hoists) {
			result_data_reg = registers.result_data;
			fast_hoists = BuildSljitExpressionSourceDataPointerHoists(
			    expressions, {registers.source_data[0], registers.source_data[1]}, 1);
		}
		AppendSljitTypedExpressionTreeConstantHoists(expressions, fast_hoists, local_size);
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(scratches),
	                 registers.saved_register_count, local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);

	vector<SljitArithmeticOverflowSite> overflows;
	struct sljit_jump *fast_done = nullptr;
	vector<sljit_jump *> fast_exits;
	if (fast_loop_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		if (simd_plan.supported) {
			EmitSljitTypedExpressionTreeSimdBooleanLoop(compiler, root, simd_plan);
		}
		if (result_data_reg) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, result_data_reg, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, result_data));
		}
		for (auto &hoist : fast_hoists) {
			if (hoist.source_index != DConstants::INVALID_INDEX) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.location.operand, 0, SLJIT_MEM1(SLJIT_S5),
				               SljitPointerArrayOffset(hoist.source_index));
			}
		}
		EmitSljitTypedExpressionTreeConstantHoists(compiler, fast_hoists);
		const auto hoists = fast_hoists.empty() ? nullptr : &fast_hoists;
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		if (SljitTypedExpressionTreeIsFastGuardedValue(root)) {
			EmitSljitTypedExpressionTreeGuardedVectorLoop(compiler, root, result_kind, result_data_reg, hoists,
			                                              overflows, fast_exits);
		} else {
			auto fast_loop = sljit_emit_label(compiler);
			idx_t fast_spill_index = 0;
			vector<sljit_jump *> null_exits;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows, hoists, &null_exits);
			EmitStoreSljitTypedExpressionTreeVectorResult(compiler, root, result_kind, result_data_reg);
			fast_exits.push_back(EmitSljitNativeVectorLatch(compiler, fast_loop));
			if (!null_exits.empty()) {
				SetSljitJumpLabels(null_exits, sljit_emit_label(compiler));
				EmitSetResultRowInvalid(compiler);
				fast_exits.push_back(EmitSljitNativeVectorLatch(compiler, fast_loop));
			}
		}
		sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	}
	struct sljit_jump *flat_nullable_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	if (fast_path.precheck_nulls_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto flat_nullable_loop = sljit_emit_label(compiler);
		flat_nullable_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> source_null_jumps;
		for (auto source_index : fast_path.source_refs) {
			source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t flat_nullable_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, flat_nullable_spill_index, overflows);
		EmitStoreSljitTypedExpressionTreeVectorResult(compiler, root, result_kind);
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
		EmitSetResultRowInvalid(compiler);
		sljit_set_label(flat_nullable_next, sljit_emit_label(compiler));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto flat_nullable_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(flat_nullable_repeat, flat_nullable_loop);
		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	idx_t slot_index = 0;
	auto root_slot = EmitSljitTypedExpressionTreeValue(compiler, root, slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), root_slot.valid_offset);
	auto root_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), root_slot.value_offset);
	EmitStoreSljitTypedExpressionTreeVectorResult(compiler, root, result_kind);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	EmitSetResultRowInvalid(compiler);
	sljit_set_label(row_done, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto helper_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	SetSljitJumpLabels(fast_exits, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

static bool SljitUseArm64FlatSimpleCompareSelect(const ExecutionExpressionIR &root) {
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !SljitTypedExpressionTreeComparisonSupported(root.binary_op) ||
	    !SljitTypedExpressionTreeSameIntegerKind(*root.left, *root.right)) {
		return false;
	}
	auto left_simple = root.left->kind == ExecutionExpressionIRKind::REFERENCE ||
	                   root.left->kind == ExecutionExpressionIRKind::CONSTANT;
	auto right_simple = root.right->kind == ExecutionExpressionIRKind::REFERENCE ||
	                    root.right->kind == ExecutionExpressionIRKind::CONSTANT;
	auto has_reference = root.left->kind == ExecutionExpressionIRKind::REFERENCE ||
	                     root.right->kind == ExecutionExpressionIRKind::REFERENCE;
	return left_simple && right_simple && has_reference;
#else
	(void)root;
	return false;
#endif
}

static sljit_s32 SljitArm64SimpleCompareFlags(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		return SLJIT_SET_Z;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SLJIT_SET_SIG_LESS;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SLJIT_SET_SIG_GREATER;
	default:
		throw InternalException("Unsupported ARM64 simple comparison selector operation");
	}
}

static bool SljitUseFlatConstantModuloCompareSelect(const ExecutionExpressionIR &root) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !SljitTypedExpressionTreeComparisonSupported(root.binary_op) ||
	    root.right->kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}
	auto &modulo = *root.left;
	if (modulo.kind != ExecutionExpressionIRKind::BINARY || modulo.binary_op != ExecutionExpressionBinaryOp::MODULO ||
	    !modulo.left || modulo.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !SljitTypedExpressionTreeConstantDivisorReductionSupported(modulo) ||
	    !SljitTypedExpressionTreeSameIntegerKind(modulo, *root.right)) {
		return false;
	}
	auto integer_kind = SljitTypedExpressionTreeIntegerKind(modulo);
	return integer_kind == SljitNativeIntegerKind::INT32 || integer_kind == SljitNativeIntegerKind::INT64;
}

static void EmitSljitFlatConstantModuloCompareSelect(struct sljit_compiler *compiler,
                                                     const ExecutionExpressionIR &root) {
	D_ASSERT(SljitUseFlatConstantModuloCompareSelect(root));
	auto &modulo = *root.left;
	auto &source = *modulo.left;
	auto integer_kind = SljitTypedExpressionTreeIntegerKind(modulo);
	auto divisor = SljitTypedExpressionTreeConstantValue(*modulo.right);
	auto compare_value = SljitTypedExpressionTreeConstantValue(*root.right);
	auto load_op = NativeIntegerLoadOp(integer_kind);
	auto data_scale = NativeIntegerDataScale(integer_kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto compare_type = NativeIntegerCompareJumpType(integer_kind, SljitTypedExpressionTreeCompareOp(root.binary_op));

	// Hoist the only source pointer and the selection append cursor. The modulo
	// reduction uses R0/R1 as scratch and leaves the remainder in R2.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	auto emit_lane = [&](sljit_sw lane, sljit_sw data_offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S5), data_offset);
		EmitSljitTypedExpressionTreeModulo(compiler, divisor, integer_kind, SLJIT_R2, SLJIT_R2);
		auto not_selected =
		    sljit_emit_cmp(compiler, compare_type ^ 1, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(compare_value));
		if (lane == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_S1, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_R2, 0);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_set_label(not_selected, sljit_emit_label(compiler));
	};

	static constexpr sljit_sw UNROLL = 8;
	auto unrolled_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	auto scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);
	for (sljit_sw lane = 0; lane < UNROLL; lane++) {
		emit_lane(lane, lane * data_width);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, UNROLL * data_width);
	auto unrolled_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(unrolled_repeat, unrolled_loop);

	sljit_set_label(scalar_tail, sljit_emit_label(compiler));
	auto tail_loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_lane(0, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
	auto tail_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(tail_repeat, tail_loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_S3, 0);
}

// ARM64 has no cheap integer movemask, so a single simple comparison is faster
// as a branchless generated scalar loop than as packed compare + mask reduction.
// Keep both source pointers and the append cursor in saved registers; each row
// overwrites the current candidate slot and CSET advances the cursor only for a
// match. This removes the data-dependent branch without materializing false rows.
static void EmitSljitArm64FlatSimpleCompareSelect(struct sljit_compiler *compiler, const ExecutionExpressionIR &root) {
	D_ASSERT(SljitUseArm64FlatSimpleCompareSelect(root));
	auto left_is_reference = root.left->kind == ExecutionExpressionIRKind::REFERENCE;
	auto right_is_reference = root.right->kind == ExecutionExpressionIRKind::REFERENCE;
	auto integer_kind = SljitTypedExpressionTreeIntegerKind(*root.left);

	// S5 initially owns source_data_array. Two-reference comparisons hoist the
	// right pointer first; one-reference comparisons use S5 alone.
	if (left_is_reference && right_is_reference) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(root.right->ref_index * sizeof(const_data_ptr_t)));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(root.left->ref_index * sizeof(const_data_ptr_t)));
	} else {
		auto source_index = left_is_reference ? root.left->ref_index : root.right->ref_index;
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	auto load_op = NativeIntegerLoadOp(integer_kind);
	auto data_scale = NativeIntegerDataScale(integer_kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto compare_type = NativeIntegerCompareJumpType(integer_kind, SljitTypedExpressionTreeCompareOp(root.binary_op));
	auto compare_flags = SljitArm64SimpleCompareFlags(root.binary_op);
	auto emit_lane = [&](sljit_sw lane, sljit_sw data_offset) {
		sljit_s32 left_operand;
		sljit_sw left_operand_value;
		if (left_is_reference) {
			left_operand = SLJIT_R2;
			left_operand_value = 0;
			sljit_emit_op1(compiler, load_op, left_operand, 0, SLJIT_MEM1(SLJIT_S5), data_offset);
		} else {
			left_operand = SLJIT_IMM;
			left_operand_value = NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*root.left));
		}
		sljit_s32 right_operand;
		sljit_sw right_operand_value;
		if (right_is_reference) {
			right_operand = SLJIT_R3;
			right_operand_value = 0;
			auto right_data_reg = left_is_reference ? SLJIT_S6 : SLJIT_S5;
			sljit_emit_op1(compiler, load_op, right_operand, 0, SLJIT_MEM1(right_data_reg), data_offset);
		} else {
			right_operand = SLJIT_IMM;
			right_operand_value = NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*root.right));
		}
		if (lane != 0) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
		}
		auto subtract_op = SLJIT_SUB | SLJIT_SET_Z | compare_flags;
		if (integer_kind == SljitNativeIntegerKind::INT32 || integer_kind == SljitNativeIntegerKind::DATE) {
			subtract_op |= SLJIT_32;
		}
		sljit_emit_op2(compiler, subtract_op, SLJIT_R0, 0, left_operand, left_operand_value, right_operand,
		               right_operand_value);
		sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R0, 0, compare_type & ~SLJIT_32);
		if (lane == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_S1, 0);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_R1, 0);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_R0, 0);
	};

	// Match the optimized native loop shape: independent scalar compares
	// per iteration, one loop branch, and pointer bumps only at the group edge.
	// Typed filter selection always provides true_sel; the outer C++ adapter owns
	// the optional identity-materialization policy.
	static constexpr sljit_sw UNROLL = 8;
	auto unrolled_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	auto scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);
	for (sljit_sw lane = 0; lane < UNROLL; lane++) {
		emit_lane(lane, lane * data_width);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, UNROLL * data_width);
	if (left_is_reference && right_is_reference) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S6, 0, SLJIT_S6, 0, SLJIT_IMM, UNROLL * data_width);
	}
	auto unrolled_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(unrolled_repeat, unrolled_loop);

	sljit_set_label(scalar_tail, sljit_emit_label(compiler));
	auto tail_loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_lane(0, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
	if (left_is_reference && right_is_reference) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S6, 0, SLJIT_S6, 0, SLJIT_IMM, data_width);
	}
	auto tail_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(tail_repeat, tail_loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_S3, 0);
}

void EmitStoreSljitTypedExpressionTreeTrueSelection(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, selected_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, index_reg, 0);
	sljit_set_label(no_true_sel, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R2, 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeTypedExpressionTreeSelect(const ExecutionExpressionIR &root,
                                                                                SljitNativeVectorFunction &function,
                                                                                string &error,
                                                                                bool emit_flat_nullable_fast_path) {
	const auto tree_plan = BuildSljitTypedExpressionTreePlan(root, emit_flat_nullable_fast_path);
	if (!tree_plan.supported || !tree_plan.result_is_bool) {
		error = "SLJIT typed expression-tree select codegen only supports BOOLEAN typed expression-tree predicates";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto &fast_path = tree_plan.fast_path;
	auto local_size = NumericCast<sljit_sw>(tree_plan.node_count * sizeof(sljit_sw) * 3);

	// A packed-lane loop can only run under the flat all-valid fast path (it has
	// no per-lane null handling). Reserve a 16-byte, 16-aligned scratch slot for
	// the lane mask above the scalar spill area.
	auto arm64_flat_simple_compare = fast_path.fast_path_supported && SljitUseArm64FlatSimpleCompareSelect(root);
	auto flat_constant_modulo_compare = fast_path.fast_path_supported && SljitUseFlatConstantModuloCompareSelect(root);
	auto simd_plan = fast_path.fast_path_supported && !arm64_flat_simple_compare && !flat_constant_modulo_compare
	                     ? TryPlanSljitTypedExpressionTreeSimd(root)
	                     : SljitTypedExpressionTreeSimdPlan();
	sljit_sw simd_mask_offset = 0;
	sljit_s32 simd_scratches = 5;
	if (simd_plan.supported) {
		simd_mask_offset = (local_size + 15) & ~sljit_sw(15);
		local_size = simd_mask_offset + 16;
		// Vector registers used: constants + all-ones + lane bits + validity nibble
		// + peak live temporaries.
		auto vector_regs = simd_plan.constant_count + (simd_plan.needs_all_ones ? 1 : 0) + simd_plan.max_live_temps +
		                   (simd_plan.nullable_capable ? idx_t(2) : idx_t(0));
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
		// ARM64 selection keeps one horizontal-mask reduction destination live
		// alongside the predicate temporaries.
		vector_regs++;
#endif
		simd_scratches = 5 | SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_regs));
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), simd_scratches, SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT, local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	struct sljit_jump *fast_done = nullptr;
	if (fast_path.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		if (arm64_flat_simple_compare) {
			EmitSljitArm64FlatSimpleCompareSelect(compiler, root);
			fast_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		} else if (flat_constant_modulo_compare) {
			EmitSljitFlatConstantModuloCompareSelect(compiler, root);
			fast_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		} else if (simd_plan.supported) {
			// Process full lane groups with packed SIMD; S1 is left at the tail
			// start and the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdSelectLoop(compiler, root, simd_plan, simd_mask_offset);
		}
		if (!arm64_flat_simple_compare && !flat_constant_modulo_compare) {
			auto fast_loop = sljit_emit_label(compiler);
			fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
			idx_t fast_spill_index = 0;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
			auto fast_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
			sljit_set_label(fast_false, sljit_emit_label(compiler));
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(fast_repeat, fast_loop);
		}
		sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	}
	// Flat batches that MAY contain NULLs run the packed loop with a lane-expanded
	// validity mask ANDed in (AND-only predicates: a row with a NULL referenced
	// source cannot pass); the scalar tail pre-checks sources per row.
	struct sljit_jump *nullable_simd_done = nullptr;
	if (fast_path.fast_path_supported && simd_plan.supported && simd_plan.nullable_capable) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto skip_nullable_simd = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		EmitSljitTypedExpressionTreeSimdSelectLoop(compiler, root, simd_plan, simd_mask_offset, &simd_plan.source_refs);
		auto nullable_tail_loop = sljit_emit_label(compiler);
		auto nullable_tail_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> tail_null_jumps;
		for (auto source_index : simd_plan.source_refs) {
			tail_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t nullable_tail_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, nullable_tail_spill_index, overflows);
		auto nullable_tail_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
		auto nullable_tail_skip = sljit_emit_label(compiler);
		sljit_set_label(nullable_tail_false, nullable_tail_skip);
		for (auto null_jump : tail_null_jumps) {
			sljit_set_label(null_jump, nullable_tail_skip);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto nullable_tail_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(nullable_tail_repeat, nullable_tail_loop);
		sljit_set_label(nullable_tail_done, sljit_emit_label(compiler));
		nullable_simd_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(skip_nullable_simd, sljit_emit_label(compiler));
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
		auto flat_nullable_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
		sljit_set_label(flat_nullable_false, sljit_emit_label(compiler));
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
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
	auto root_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S3);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	sljit_set_label(root_false, sljit_emit_label(compiler));
	sljit_set_label(row_done, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	if (nullable_simd_done) {
		sljit_set_label(nullable_simd_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

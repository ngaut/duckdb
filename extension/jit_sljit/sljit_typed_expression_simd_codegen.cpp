//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_simd_codegen.cpp
//
// SIMD (packed) vectorization of boolean typed-expression predicates.
//
// The scalar fast path evaluates one row per iteration with branch-based
// comparisons and short-circuit conjunctions. This path evaluates
// `lane_count` rows per iteration with packed SLJIT SIMD ops and BRANCHLESS
// mask compares / mask ANDs, then extracts the set lanes into the selection
// vector. It is entered only when the whole predicate is SIMD-profitable for
// the target architecture (see TryPlanSljitTypedExpressionTreeSimd); otherwise
// the caller falls back to the scalar fast/generic loops. Rows that do not
// form a full lane group are handled by the existing scalar tail loop.
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"

#include "sljitLir.h"

namespace duckdb {

// Packed-op capability for (element scale, op) on the current architecture.
// This is the (type, op, arch) gate: SIMD is emitted only where the PoC proved
// it wins. int64 (scale 3) multiply is unsupported on ARM NEON / pre-AVX-512
// x86 and would regress, so it is excluded here (mirrors the SLJIT-layer gate
// that returns SLJIT_ERR_UNSUPPORTED for MUL.2d).
static bool SljitSimdPackedArithProfitable(sljit_s32 scale, ExecutionExpressionBinaryOp op) {
	if (!sljit_has_cpu_feature(SLJIT_HAS_SIMD)) {
		return false;
	}
#if (defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64) ||                                                            \
    (defined(SLJIT_CONFIG_X86_64) && SLJIT_CONFIG_X86_64)
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		// 64-bit element multiply has no NEON instruction and needs AVX-512DQ on
		// x86; only 32-bit element multiply is profitable on the supported backends.
		return scale == 2;
	default:
		return false;
	}
#else
	return false;
#endif
}

static bool SljitSimdComparisonSupported(ExecutionExpressionBinaryOp op, bool &needs_negate) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		needs_negate = false;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		needs_negate = true;
		return true;
	default:
		return false;
	}
}

// Only 32-bit (INT32/DATE) and 64-bit (INT64/DECIMAL64) integer element widths
// map to a packed lane; return the SLJIT data scale (2 or 3) or false.
static bool SljitSimdNodeScale(const ExecutionExpressionIR &node, sljit_s32 &scale) {
	if (!SljitTypedExpressionTreeIsIntegerNode(node)) {
		return false;
	}
	auto data_scale = NativeIntegerDataScale(SljitTypedExpressionTreeIntegerKind(node));
	if (data_scale != 2 && data_scale != 3) {
		return false;
	}
	scale = NumericCast<sljit_s32>(data_scale);
	return true;
}

// Recursively validate SIMD eligibility and collect the facts needed to size
// the register budget. `value_scale` is the single element width the whole
// tree must share (references drive it; constants adapt).
static bool CheckSljitSimdEligible(const ExecutionExpressionIR &node, sljit_s32 &value_scale,
                                   set<int64_t> &distinct_constants, bool &needs_all_ones, idx_t &node_count) {
	node_count++;
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE: {
		sljit_s32 scale;
		if (!SljitSimdNodeScale(node, scale)) {
			return false;
		}
		if (value_scale == 0) {
			value_scale = scale;
		} else if (value_scale != scale) {
			return false; // mixed element widths cannot share one packed loop
		}
		return true;
	}
	case ExecutionExpressionIRKind::CONSTANT: {
		if (!SljitTypedExpressionTreeIsIntegerNode(node)) {
			return false;
		}
		distinct_constants.insert(SljitTypedExpressionTreeConstantValue(node));
		return true;
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right) {
			return false;
		}
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			bool negate;
			if (!SljitSimdComparisonSupported(node.binary_op, negate)) {
				return false;
			}
			needs_all_ones = needs_all_ones || negate;
			return CheckSljitSimdEligible(*node.left, value_scale, distinct_constants, needs_all_ones, node_count) &&
			       CheckSljitSimdEligible(*node.right, value_scale, distinct_constants, needs_all_ones, node_count);
		}
		// Arithmetic: packed ops wrap silently, so only accept when no overflow
		// trap is required (Step 3a).
		if (node.arithmetic_overflow_check) {
			return false;
		}
		// Check children first so `value_scale` is resolved, then validate that a
		// profitable packed op exists for that width (excludes 64-bit multiply).
		if (!CheckSljitSimdEligible(*node.left, value_scale, distinct_constants, needs_all_ones, node_count) ||
		    !CheckSljitSimdEligible(*node.right, value_scale, distinct_constants, needs_all_ones, node_count)) {
			return false;
		}
		return SljitSimdPackedArithProfitable(value_scale, node.binary_op);
	}
	case ExecutionExpressionIRKind::CONJUNCTION: {
		if (node.conjunction_op != ExecutionExpressionConjunctionOp::AND &&
		    node.conjunction_op != ExecutionExpressionConjunctionOp::OR) {
			return false;
		}
		if (node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!CheckSljitSimdEligible(*child, value_scale, distinct_constants, needs_all_ones, node_count)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

// Simulate the mask emitter's exact temp allocation/free discipline to find the
// peak simultaneous vector temporaries (register pressure). This mirrors
// EmitSljitSimdMask: a subtree's result stays live until its parent has allocated
// its own destination and finished the op (so a binary node holds left result +
// right result + destination at once); constants are persistent, not temps. `live`
// is incremented by the net temp the node's result occupies. Over-estimates only
// when the free list could reuse a slot, which is safe.
static void SljitSimdSimulateRegPressure(const ExecutionExpressionIR &node, idx_t &live, idx_t &peak) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		live++;
		peak = MaxValue<idx_t>(peak, live);
		return;
	case ExecutionExpressionIRKind::CONSTANT:
		return; // persistent broadcast register, not a temp
	case ExecutionExpressionIRKind::BINARY: {
		SljitSimdSimulateRegPressure(*node.left, live, peak);
		SljitSimdSimulateRegPressure(*node.right, live, peak);
		live++; // destination allocated while both operand results are still live
		peak = MaxValue<idx_t>(peak, live);
		if (node.left->kind != ExecutionExpressionIRKind::CONSTANT) {
			live--;
		}
		if (node.right->kind != ExecutionExpressionIRKind::CONSTANT) {
			live--;
		}
		return;
	}
	case ExecutionExpressionIRKind::CONJUNCTION: {
		SljitSimdSimulateRegPressure(*node.children[0], live, peak);
		if (node.children[0]->kind == ExecutionExpressionIRKind::CONSTANT) {
			live++; // constant first child is copied into a temp accumulator
			peak = MaxValue<idx_t>(peak, live);
		}
		for (idx_t child_idx = 1; child_idx < node.children.size(); child_idx++) {
			SljitSimdSimulateRegPressure(*node.children[child_idx], live, peak);
			if (node.children[child_idx]->kind != ExecutionExpressionIRKind::CONSTANT) {
				live--; // child mask freed after being AND/OR-ed into the accumulator
			}
		}
		return;
	}
	default:
		live++;
		peak = MaxValue<idx_t>(peak, live);
		return;
	}
}

static idx_t SljitSimdMaxLiveTemps(const ExecutionExpressionIR &node) {
	idx_t live = 0;
	idx_t peak = 0;
	SljitSimdSimulateRegPressure(node, live, peak);
	return MaxValue<idx_t>(peak, 1);
}

SljitTypedExpressionTreeSimdPlan TryPlanSljitTypedExpressionTreeSimd(const ExecutionExpressionIR &root) {
	SljitTypedExpressionTreeSimdPlan plan;
	if (getenv("DUCKDB_JIT_NO_SIMD")) {
		return plan; // A/B measurement toggle: fall back to the scalar fast path
	}
	// The root must produce a boolean mask (comparison or conjunction of them).
	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION &&
	    !(root.kind == ExecutionExpressionIRKind::BINARY &&
	      SljitTypedExpressionTreeComparisonSupported(root.binary_op))) {
		return plan;
	}
	sljit_s32 value_scale = 0;
	set<int64_t> distinct_constants;
	bool needs_all_ones = false;
	idx_t node_count = 0;
	if (!CheckSljitSimdEligible(root, value_scale, distinct_constants, needs_all_ones, node_count)) {
		return plan;
	}
	if (value_scale != 2 && value_scale != 3) {
		return plan; // no integer reference to anchor the width
	}
	// Register budget: persistent registers (constants + all-ones) plus peak live
	// temporaries plus one accumulator must fit the vector register file with
	// margin. This uses true register pressure, not node count.
	auto max_live = SljitSimdMaxLiveTemps(root);
	auto persistent = distinct_constants.size() + (needs_all_ones ? 1 : 0);
	if (persistent + max_live + 1 > 28) {
		return plan;
	}
	plan.supported = true;
	plan.elem_scale = value_scale;
	plan.lanes = value_scale == 2 ? 4 : 2;
	plan.simd_type = SLJIT_SIMD_REG_128 | (value_scale == 2 ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
	plan.constant_count = distinct_constants.size();
	plan.needs_all_ones = needs_all_ones;
	plan.node_count = node_count;
	plan.max_live_temps = max_live;
	return plan;
}

// A pure integer value expression: references, constants and add/sub/mul only.
// Excludes comparisons/conjunctions (which produce -1/0 masks, not values) so a
// SUM payload never accumulates a boolean mask.
static bool SljitSimdIsPureValueExpression(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
	case ExecutionExpressionIRKind::CONSTANT:
		return true;
	case ExecutionExpressionIRKind::BINARY:
		if (!node.left || !node.right || SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return false;
		}
		return SljitSimdIsPureValueExpression(*node.left) && SljitSimdIsPureValueExpression(*node.right);
	default:
		return false;
	}
}

SljitTypedExpressionTreeSimdPlan TryPlanSljitTypedExpressionTreeSimdValue(const ExecutionExpressionIR &root,
                                                                         sljit_s32 want_scale) {
	SljitTypedExpressionTreeSimdPlan plan;
	if (!SljitSimdIsPureValueExpression(root)) {
		return plan;
	}
	sljit_s32 value_scale = want_scale;
	set<int64_t> distinct_constants;
	bool needs_all_ones = false;
	idx_t node_count = 0;
	if (!CheckSljitSimdEligible(root, value_scale, distinct_constants, needs_all_ones, node_count)) {
		return plan;
	}
	if (value_scale != want_scale) {
		return plan; // payload width must match the predicate width
	}
	plan.supported = true;
	plan.elem_scale = want_scale;
	plan.lanes = want_scale == 2 ? 4 : 2;
	plan.simd_type = SLJIT_SIMD_REG_128 | (want_scale == 2 ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
	plan.constant_count = distinct_constants.size();
	plan.needs_all_ones = needs_all_ones; // always false: a pure value has no comparisons
	plan.node_count = node_count;
	plan.max_live_temps = SljitSimdMaxLiveTemps(root);
	return plan;
}

namespace {

// Vector-register allocator: constants and the all-ones mask occupy fixed low
// registers (persistent for the whole loop); tree temporaries are drawn from a
// free list above them.
struct SljitSimdEmitContext {
	sljit_s32 simd_type;
	sljit_s32 scale;
	map<int64_t, sljit_s32> constant_reg;
	sljit_s32 all_ones_reg = -1;
	sljit_s32 next_temp = 0;
	vector<sljit_s32> free_temps;
};

struct SljitSimdValue {
	sljit_s32 reg;
	bool is_temp;
};

sljit_s32 AllocSimdTemp(SljitSimdEmitContext &ctx) {
	if (!ctx.free_temps.empty()) {
		auto reg = ctx.free_temps.back();
		ctx.free_temps.pop_back();
		return reg;
	}
	return ctx.next_temp++;
}

void FreeSimdValue(SljitSimdEmitContext &ctx, const SljitSimdValue &value) {
	if (value.is_temp) {
		ctx.free_temps.push_back(value.reg);
	}
}

SljitSimdValue EmitSljitSimdMask(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                 SljitSimdEmitContext &ctx);

void EmitSljitSimdNot(struct sljit_compiler *compiler, SljitSimdEmitContext &ctx, sljit_s32 reg) {
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_XOR, SLJIT_VR(reg), SLJIT_VR(reg),
	                    SLJIT_VR(ctx.all_ones_reg), 0);
}

SljitSimdValue EmitSljitSimdReference(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                      SljitSimdEmitContext &ctx) {
	auto reg = AllocSimdTemp(ctx);
	// Column base pointer = source_data_array[ref_index] (S5 holds the array).
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	// Load lanes contiguously from column[S1] (S1 is the flat row base).
	sljit_emit_simd_mov(compiler, ctx.simd_type, SLJIT_VR(reg), SLJIT_MEM2(SLJIT_R0, SLJIT_S1), ctx.scale);
	return {reg, true};
}

SljitSimdValue EmitSljitSimdComparison(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                       SljitSimdEmitContext &ctx) {
	auto left = EmitSljitSimdMask(compiler, *node.left, ctx);
	auto right = EmitSljitSimdMask(compiler, *node.right, ctx);
	auto dst = AllocSimdTemp(ctx);
	auto st = ctx.simd_type;
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN: // left > right
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(left.reg),
		                    SLJIT_VR(right.reg), 0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN: // left < right == right > left
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(right.reg),
		                    SLJIT_VR(left.reg), 0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO: // !(left < right) == !(right > left)
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(right.reg),
		                    SLJIT_VR(left.reg), 0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO: // !(left > right)
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(left.reg),
		                    SLJIT_VR(right.reg), 0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPEQ, SLJIT_VR(dst), SLJIT_VR(left.reg),
		                    SLJIT_VR(right.reg), 0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPEQ, SLJIT_VR(dst), SLJIT_VR(left.reg),
		                    SLJIT_VR(right.reg), 0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	default:
		throw InternalException("Unsupported SLJIT SIMD comparison operator");
	}
	FreeSimdValue(ctx, left);
	FreeSimdValue(ctx, right);
	return {dst, true};
}

SljitSimdValue EmitSljitSimdArithmetic(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                       SljitSimdEmitContext &ctx) {
	auto left = EmitSljitSimdMask(compiler, *node.left, ctx);
	auto right = EmitSljitSimdMask(compiler, *node.right, ctx);
	auto dst = AllocSimdTemp(ctx);
	sljit_s32 op;
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
		op = SLJIT_SIMD_OP2_ADD;
		break;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		op = SLJIT_SIMD_OP2_SUB;
		break;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		op = SLJIT_SIMD_OP2_MUL;
		break;
	default:
		throw InternalException("Unsupported SLJIT SIMD arithmetic operator");
	}
	sljit_emit_simd_op2(compiler, ctx.simd_type | op, SLJIT_VR(dst), SLJIT_VR(left.reg), SLJIT_VR(right.reg), 0);
	FreeSimdValue(ctx, left);
	FreeSimdValue(ctx, right);
	return {dst, true};
}

SljitSimdValue EmitSljitSimdConjunction(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        SljitSimdEmitContext &ctx) {
	auto combine = node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? SLJIT_SIMD_OP2_AND
	                                                                             : SLJIT_SIMD_OP2_OR;
	auto acc = EmitSljitSimdMask(compiler, *node.children[0], ctx);
	if (!acc.is_temp) {
		// A constant child cannot be mutated in place; copy into a temp.
		auto reg = AllocSimdTemp(ctx);
		sljit_emit_simd_mov(compiler, ctx.simd_type, SLJIT_VR(reg), SLJIT_VR(acc.reg), 0);
		acc = {reg, true};
	}
	for (idx_t child_idx = 1; child_idx < node.children.size(); child_idx++) {
		auto child = EmitSljitSimdMask(compiler, *node.children[child_idx], ctx);
		sljit_emit_simd_op2(compiler, ctx.simd_type | combine, SLJIT_VR(acc.reg), SLJIT_VR(acc.reg),
		                    SLJIT_VR(child.reg), 0);
		FreeSimdValue(ctx, child);
	}
	return acc;
}

SljitSimdValue EmitSljitSimdMask(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                 SljitSimdEmitContext &ctx) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		return EmitSljitSimdReference(compiler, node, ctx);
	case ExecutionExpressionIRKind::CONSTANT:
		return {ctx.constant_reg.at(SljitTypedExpressionTreeConstantValue(node)), false};
	case ExecutionExpressionIRKind::BINARY:
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return EmitSljitSimdComparison(compiler, node, ctx);
		}
		return EmitSljitSimdArithmetic(compiler, node, ctx);
	case ExecutionExpressionIRKind::CONJUNCTION:
		return EmitSljitSimdConjunction(compiler, node, ctx);
	default:
		throw InternalException("Unsupported SLJIT SIMD node kind");
	}
}

// Broadcast every distinct constant (and the all-ones mask if needed) into a
// persistent vector register before the loop starts.
void EmitSljitSimdBroadcastConstants(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                     SljitSimdEmitContext &ctx) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT: {
		auto value = SljitTypedExpressionTreeConstantValue(node);
		if (ctx.constant_reg.find(value) != ctx.constant_reg.end()) {
			return;
		}
		auto reg = ctx.next_temp++;
		ctx.constant_reg[value] = reg;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, NumericCast<sljit_sw>(value));
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(reg), SLJIT_R0, 0);
		return;
	}
	case ExecutionExpressionIRKind::BINARY:
		if (node.left) {
			EmitSljitSimdBroadcastConstants(compiler, *node.left, ctx);
		}
		if (node.right) {
			EmitSljitSimdBroadcastConstants(compiler, *node.right, ctx);
		}
		return;
	case ExecutionExpressionIRKind::CONJUNCTION:
		for (auto &child : node.children) {
			EmitSljitSimdBroadcastConstants(compiler, *child, ctx);
		}
		return;
	default:
		return;
	}
}

} // namespace

void EmitSljitTypedExpressionTreeSimdSelectLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                                const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw mask_offset) {
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;

	// Persistent registers: constants first, then all-ones.
	EmitSljitSimdBroadcastConstants(compiler, root, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}

	// S1 is the flat row base (already 0 after the loop-init helper).
	auto simd_loop = sljit_emit_label(compiler);
	// Exit the packed loop when fewer than `lanes` rows remain: R0 = S1 + lanes; if R0 > S2 stop.
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto simd_done = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);

	auto mask = EmitSljitSimdMask(compiler, root, ctx);
	sljit_emit_simd_mov(compiler, ctx.simd_type | SLJIT_SIMD_STORE, SLJIT_VR(mask.reg), SLJIT_MEM1(SLJIT_SP),
	                    mask_offset);

	auto lane_load_op = plan.elem_scale == 2 ? SLJIT_MOV_S32 : SLJIT_MOV;
	auto lane_bytes = NumericCast<sljit_sw>(sljit_sw(1) << plan.elem_scale);
	for (sljit_s32 lane = 0; lane < plan.lanes; lane++) {
		sljit_emit_op1(compiler, lane_load_op, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + lane * lane_bytes);
		auto lane_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
		EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_R1);
		sljit_set_label(lane_false, sljit_emit_label(compiler));
	}

	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, simd_loop);
	sljit_set_label(simd_done, sljit_emit_label(compiler));
}

// SADALP Vd.2D, Vn.4S : sign-extend each 32-bit lane of src and pairwise-add into
// the 64-bit lanes of dst (widen + accumulate in one instruction). ARM-specific,
// emitted as a raw instruction; the SUM SIMD path is gated to ARM64 (no portable op).
static void EmitSljitSimdSadalp2d4s(struct sljit_compiler *compiler, sljit_s32 dst_vreg, sljit_s32 src_vreg) {
	// dst_vreg / src_vreg are context temp indices; map them through SLJIT_VR() the
	// same way the sljit_emit_simd_* calls do before resolving the physical register.
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(dst_vreg));
	auto src = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(src_vreg));
	uint32_t instruction = 0x4ea06800u | (UnsafeNumericCast<uint32_t>(src) << 5) | UnsafeNumericCast<uint32_t>(dst);
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

void EmitSljitTypedExpressionTreeSimdSumLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &predicate,
                                             const ExecutionExpressionIR &payload,
                                             const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw sum_offset,
                                             sljit_sw count_offset, sljit_sw saw_value_offset, sljit_sw scratch_offset) {
	// plan is the predicate plan; the packed path requires a 32-bit (4-lane) predicate.
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;
	auto sum_type = SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_64;

	// Predicate and payload constants share one broadcast map (deduplicated by value).
	EmitSljitSimdBroadcastConstants(compiler, predicate, ctx);
	EmitSljitSimdBroadcastConstants(compiler, payload, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	auto count_acc = ctx.next_temp++;
	auto sum_acc = ctx.next_temp++;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(count_acc), SLJIT_R0, 0);
	sljit_emit_simd_replicate(compiler, sum_type, SLJIT_VR(sum_acc), SLJIT_R0, 0);

	auto simd_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto simd_done = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);

	auto mask = EmitSljitSimdMask(compiler, predicate, ctx);
	// Compute the int32 payload value (column load or arithmetic) and mask it: masked
	// lanes are 0 where the predicate is false.
	auto payload_val = EmitSljitSimdMask(compiler, payload, ctx);
	if (!payload_val.is_temp) {
		// A bare constant payload shares a persistent register; copy before the in-place AND.
		auto reg = AllocSimdTemp(ctx);
		sljit_emit_simd_mov(compiler, ctx.simd_type, SLJIT_VR(reg), SLJIT_VR(payload_val.reg), 0);
		payload_val = {reg, true};
	}
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_AND, SLJIT_VR(payload_val.reg),
	                    SLJIT_VR(payload_val.reg), SLJIT_VR(mask.reg), 0);
	// sum_acc.2d += widened pairwise sum of masked.4s ; count_acc -= mask
	EmitSljitSimdSadalp2d4s(compiler, sum_acc, payload_val.reg);
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_SUB, SLJIT_VR(count_acc), SLJIT_VR(count_acc),
	                    SLJIT_VR(mask.reg), 0);
	FreeSimdValue(ctx, payload_val);
	FreeSimdValue(ctx, mask);

	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, simd_loop);
	sljit_set_label(simd_done, sljit_emit_label(compiler));

	// Horizontal count: sum the 4 int32 lanes of count_acc into R2 (this group's matches).
	sljit_emit_simd_mov(compiler, ctx.simd_type | SLJIT_SIMD_STORE, SLJIT_VR(count_acc), SLJIT_MEM1(SLJIT_SP),
	                    scratch_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), count_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), scratch_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	for (sljit_s32 lane = 1; lane < 4; lane++) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), scratch_offset + lane * 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), count_offset, SLJIT_R2, 0);
	// If this SIMD span matched any row, mark the aggregate state as set (sum != NULL).
	auto no_match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
	sljit_set_label(no_match, sljit_emit_label(compiler));

	// Horizontal sum: add the 2 int64 lanes of sum_acc into the running sum.
	sljit_emit_simd_mov(compiler, sum_type | SLJIT_SIMD_STORE, SLJIT_VR(sum_acc), SLJIT_MEM1(SLJIT_SP), scratch_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), scratch_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), scratch_offset + 8);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sum_offset, SLJIT_R2, 0);
}

void EmitSljitTypedExpressionTreeSimdCountLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                               const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw count_offset,
                                               sljit_sw mask_offset) {
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;

	EmitSljitSimdBroadcastConstants(compiler, root, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	// Per-lane match accumulator: acc[i] += 1 for every matching row in lane i.
	auto acc = ctx.next_temp++;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(acc), SLJIT_R0, 0);

	// S1 is the flat row base (0 on entry).
	auto simd_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto simd_done = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);

	auto mask = EmitSljitSimdMask(compiler, root, ctx);
	// mask lanes are all-ones (=-1) where true; acc -= mask adds 1 per matching lane.
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_SUB, SLJIT_VR(acc), SLJIT_VR(acc),
	                    SLJIT_VR(mask.reg), 0);
	FreeSimdValue(ctx, mask);

	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, simd_loop);
	sljit_set_label(simd_done, sljit_emit_label(compiler));

	// Horizontal-sum the lane accumulators into the running count slot.
	sljit_emit_simd_mov(compiler, ctx.simd_type | SLJIT_SIMD_STORE, SLJIT_VR(acc), SLJIT_MEM1(SLJIT_SP), mask_offset);
	auto lane_load_op = plan.elem_scale == 2 ? SLJIT_MOV_S32 : SLJIT_MOV;
	auto lane_bytes = NumericCast<sljit_sw>(sljit_sw(1) << plan.elem_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), count_offset);
	for (sljit_s32 lane = 0; lane < plan.lanes; lane++) {
		sljit_emit_op1(compiler, lane_load_op, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + lane * lane_bytes);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), count_offset, SLJIT_R2, 0);
}

} // namespace duckdb

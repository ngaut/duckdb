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
#if (defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64) || (defined(SLJIT_CONFIG_X86_64) && SLJIT_CONFIG_X86_64)
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
// map to a packed lane; return the SLJIT data scale (2 or 3) or false. DECIMAL64
// participates as its int64 representation, exactly like the scalar fast path.
static bool SljitSimdNodeScale(const ExecutionExpressionIR &node, sljit_s32 &scale) {
	SljitNativeIntegerKind kind;
	if (!TryGetSljitTypedExpressionTreeResultKind(node, kind)) {
		return false;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	if (data_scale != 2 && data_scale != 3) {
		return false;
	}
	scale = NumericCast<sljit_s32>(data_scale);
	return true;
}

// Recursively validate a pure VALUE subtree (references, constants, add/sub/mul)
// and resolve its single element width. `value_scale` is anchored by the first
// reference; constants adapt and are collected for later width-tagged broadcast.
static bool CheckSljitSimdEligibleValue(const ExecutionExpressionIR &node, sljit_s32 &value_scale,
                                        vector<int64_t> &constants, idx_t &node_count) {
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
			return false; // a single value expression cannot mix element widths
		}
		return true;
	}
	case ExecutionExpressionIRKind::CONSTANT: {
		sljit_s32 scale;
		if (!SljitSimdNodeScale(node, scale)) {
			return false;
		}
		constants.push_back(SljitTypedExpressionTreeConstantValue(node));
		return true;
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right || SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return false;
		}
		// Arithmetic: packed ops wrap silently, so only accept when no overflow
		// trap is required. Check children first so `value_scale` is resolved,
		// then validate a profitable packed op exists (excludes 64-bit multiply).
		if (node.arithmetic_overflow_check) {
			return false;
		}
		if (!CheckSljitSimdEligibleValue(*node.left, value_scale, constants, node_count) ||
		    !CheckSljitSimdEligibleValue(*node.right, value_scale, constants, node_count)) {
			return false;
		}
		return SljitSimdPackedArithProfitable(value_scale, node.binary_op);
	}
	default:
		return false;
	}
}

// Recursively validate a MASK subtree (comparisons combined by conjunctions).
// Each comparison anchors its own element width, so a conjunction may mix
// 32-bit and 64-bit comparisons: 64-bit masks are evaluated per half and
// narrowed to the 32-bit loop width before combining.
static bool CheckSljitSimdEligibleMask(const ExecutionExpressionIR &node,
                                       set<std::pair<int64_t, sljit_s32>> &distinct_constants, bool &needs_all_ones,
                                       idx_t &node_count, bool &has_scale32, bool &has_scale64, bool &has_or) {
	node_count++;
	switch (node.kind) {
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right) {
			return false;
		}
		bool negate;
		if (!SljitSimdComparisonSupported(node.binary_op, negate)) {
			return false;
		}
		needs_all_ones = needs_all_ones || negate;
		sljit_s32 comparison_scale = 0;
		vector<int64_t> constants;
		if (!CheckSljitSimdEligibleValue(*node.left, comparison_scale, constants, node_count) ||
		    !CheckSljitSimdEligibleValue(*node.right, comparison_scale, constants, node_count)) {
			return false;
		}
		if (comparison_scale != 2 && comparison_scale != 3) {
			return false; // no integer reference to anchor the comparison width
		}
		for (auto value : constants) {
			distinct_constants.insert({value, comparison_scale});
		}
		has_scale32 = has_scale32 || comparison_scale == 2;
		has_scale64 = has_scale64 || comparison_scale == 3;
		return true;
	}
	case ExecutionExpressionIRKind::CONJUNCTION: {
		if (node.conjunction_op != ExecutionExpressionConjunctionOp::AND &&
		    node.conjunction_op != ExecutionExpressionConjunctionOp::OR) {
			return false;
		}
		if (node.children.empty()) {
			return false;
		}
		has_or = has_or || node.conjunction_op == ExecutionExpressionConjunctionOp::OR;
		for (auto &child : node.children) {
			if (!CheckSljitSimdEligibleMask(*child, distinct_constants, needs_all_ones, node_count, has_scale32,
			                                has_scale64, has_or)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

// Resolve the anchored element scale of a value subtree (0 when only constants).
static sljit_s32 SljitSimdSubtreeScale(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE: {
		sljit_s32 scale;
		return SljitSimdNodeScale(node, scale) ? scale : 0;
	}
	case ExecutionExpressionIRKind::BINARY: {
		auto left = node.left ? SljitSimdSubtreeScale(*node.left) : 0;
		return left != 0 ? left : (node.right ? SljitSimdSubtreeScale(*node.right) : 0);
	}
	default:
		return 0;
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
	set<std::pair<int64_t, sljit_s32>> distinct_constants;
	bool needs_all_ones = false;
	idx_t node_count = 0;
	bool has_scale32 = false;
	bool has_scale64 = false;
	bool has_or = false;
	if (!CheckSljitSimdEligibleMask(root, distinct_constants, needs_all_ones, node_count, has_scale32, has_scale64,
	                                has_or)) {
		return plan;
	}
	const bool mixed = has_scale32 && has_scale64;
#if !(defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64)
	if (mixed) {
		return plan; // 64->32 mask narrowing (UZP1) is emitted as a raw ARM64 instruction
	}
#endif
	// Register budget: persistent registers (constants + all-ones) plus peak live
	// temporaries plus one accumulator must fit the vector register file with
	// margin. This uses true register pressure, not node count. Mixed-width masks
	// hold up to two extra half-masks while narrowing.
	auto max_live = SljitSimdMaxLiveTemps(root) + (mixed ? 2 : 0);
	auto persistent = distinct_constants.size() + (needs_all_ones ? 1 : 0);
	if (persistent + max_live + 1 > 28) {
		return plan;
	}
	// Mixed masks combine at the 32-bit width (4 lanes); pure trees keep their width.
	const auto loop_scale = mixed ? sljit_s32(2) : (has_scale64 ? sljit_s32(3) : sljit_s32(2));
	plan.supported = true;
	plan.mixed_width = mixed;
	plan.elem_scale = loop_scale;
	plan.lanes = loop_scale == 2 ? 4 : 2;
	plan.simd_type = SLJIT_SIMD_REG_128 | (loop_scale == 2 ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
	plan.constant_count = distinct_constants.size();
	plan.needs_all_ones = needs_all_ones;
	plan.node_count = node_count;
	plan.max_live_temps = max_live;
	// Row-skip null semantics hold only for AND-only trees.
	if (!has_or) {
		CollectSljitTypedExpressionTreeReferences(root, plan.source_refs);
		plan.nullable_capable = !plan.source_refs.empty();
	}
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
	vector<int64_t> constants;
	idx_t node_count = 0;
	if (!CheckSljitSimdEligibleValue(root, value_scale, constants, node_count)) {
		return plan;
	}
	if (value_scale != want_scale) {
		return plan; // payload width must match the predicate width
	}
	set<std::pair<int64_t, sljit_s32>> distinct_constants;
	for (auto value : constants) {
		distinct_constants.insert({value, want_scale});
	}
	plan.supported = true;
	plan.elem_scale = want_scale;
	plan.lanes = want_scale == 2 ? 4 : 2;
	plan.simd_type = SLJIT_SIMD_REG_128 | (want_scale == 2 ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
	plan.constant_count = distinct_constants.size();
	plan.needs_all_ones = false; // a pure value has no comparisons
	plan.node_count = node_count;
	plan.max_live_temps = SljitSimdMaxLiveTemps(root);
	return plan;
}

namespace {

// Vector-register allocator: constants and the all-ones mask occupy fixed low
// registers (persistent for the whole loop); tree temporaries are drawn from a
// free list above them. `scale`/`simd_type` describe the LOOP width (the width
// masks are combined at); individual comparisons may evaluate at a different
// width and narrow (mixed mode).
struct SljitSimdEmitContext {
	sljit_s32 simd_type;
	sljit_s32 scale;
	bool mixed = false;
	map<std::pair<int64_t, sljit_s32>, sljit_s32> constant_reg; // (value, element scale) -> vreg
	map<idx_t, sljit_s32> source_data_reg;                      // flat-loop source pointer hoists
	sljit_s32 all_ones_reg = -1;
	// Lane bit positions {1,2,4,8} (or {1,2}) used to expand a validity nibble
	// into a lane mask; only initialized when the loop runs on nullable data.
	sljit_s32 lane_bits_reg = -1;
	sljit_s32 next_temp = 0;
	vector<sljit_s32> free_temps;
};

struct SljitSimdValue {
	sljit_s32 reg;
	bool is_temp;
};

sljit_s32 SljitSimdTypeForScale(sljit_s32 scale) {
	return SLJIT_SIMD_REG_128 | (scale == 2 ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
}

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

void EmitSljitSimdNot(struct sljit_compiler *compiler, SljitSimdEmitContext &ctx, sljit_s32 reg) {
	// Bitwise complement via the shared all-ones register; width-agnostic.
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_XOR, SLJIT_VR(reg), SLJIT_VR(reg),
	                    SLJIT_VR(ctx.all_ones_reg), 0);
}

#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
// UZP1 Vd.4S, Vn.4S, Vm.4S : take the even-indexed 32-bit elements of Vn then Vm.
// For two 2x64-bit masks (each lane all-ones/all-zeros) this packs the low words
// into one 4x32-bit mask covering rows 0..3 — the 64->32 mask narrowing step.
void EmitSljitSimdUzp1Narrow(struct sljit_compiler *compiler, sljit_s32 dst_vreg, sljit_s32 lo_vreg,
                             sljit_s32 hi_vreg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(dst_vreg));
	auto lo = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(lo_vreg));
	auto hi = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(hi_vreg));
	uint32_t instruction = 0x4e801800u | (UnsafeNumericCast<uint32_t>(hi) << 16) |
	                       (UnsafeNumericCast<uint32_t>(lo) << 5) | UnsafeNumericCast<uint32_t>(dst);
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

// Reduce an all-ones/all-zeros predicate mask to the signed sum of its lanes.
// 4x32 uses ADDV; 2x64 uses ADDP. A second UMOV transfers the scalar result to
// a GP register. The portable sljit_emit_simd_sign movemask needs several more
// instructions on ARM64 and is reserved for genuinely mixed masks.
void EmitSljitSimdMaskLaneSum(struct sljit_compiler *compiler, sljit_s32 dst_vreg, sljit_s32 src_vreg,
                              sljit_s32 elem_scale, sljit_s32 target_reg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(dst_vreg));
	auto src = sljit_get_register_index(SLJIT_SIMD_REG_128, SLJIT_VR(src_vreg));
	auto target = sljit_get_register_index(SLJIT_GP_REGISTER, target_reg);
	uint32_t reduce;
	uint32_t move;
	if (elem_scale == 2) {
		reduce = 0x4eb1b800u | (UnsafeNumericCast<uint32_t>(src) << 5) | UnsafeNumericCast<uint32_t>(dst);
		move = 0x0e043c00u | (UnsafeNumericCast<uint32_t>(dst) << 5) | UnsafeNumericCast<uint32_t>(target);
	} else {
		D_ASSERT(elem_scale == 3);
		reduce = 0x5ef1b800u | (UnsafeNumericCast<uint32_t>(src) << 5) | UnsafeNumericCast<uint32_t>(dst);
		move = 0x4e083c00u | (UnsafeNumericCast<uint32_t>(dst) << 5) | UnsafeNumericCast<uint32_t>(target);
	}
	sljit_emit_op_custom(compiler, &reduce, sizeof(reduce));
	sljit_emit_op_custom(compiler, &move, sizeof(move));
}
#endif

// Evaluate a pure VALUE expression (references, constants, add/sub/mul) at the
// given element scale, reading rows starting at S1 + row_offset.
SljitSimdValue EmitSljitSimdValueExpr(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                      SljitSimdEmitContext &ctx, sljit_s32 scale, sljit_sw row_offset) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE: {
		auto reg = AllocSimdTemp(ctx);
		// Column base pointer = source_data_array[ref_index], unless the flat
		// selector hoisted its one or two hot sources into saved registers.
		auto hoisted = ctx.source_data_reg.find(node.ref_index);
		auto data_reg = SLJIT_R0;
		if (hoisted != ctx.source_data_reg.end()) {
			data_reg = hoisted->second;
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_P, data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
		}
		if (row_offset == 0) {
			sljit_emit_simd_mov(compiler, SljitSimdTypeForScale(scale), SLJIT_VR(reg), SLJIT_MEM2(data_reg, SLJIT_S1),
			                    scale);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, row_offset);
			sljit_emit_simd_mov(compiler, SljitSimdTypeForScale(scale), SLJIT_VR(reg), SLJIT_MEM2(data_reg, SLJIT_R1),
			                    scale);
		}
		return {reg, true};
	}
	case ExecutionExpressionIRKind::CONSTANT:
		return {ctx.constant_reg.at({SljitTypedExpressionTreeConstantValue(node), scale}), false};
	case ExecutionExpressionIRKind::BINARY: {
		auto left = EmitSljitSimdValueExpr(compiler, *node.left, ctx, scale, row_offset);
		auto right = EmitSljitSimdValueExpr(compiler, *node.right, ctx, scale, row_offset);
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
		sljit_emit_simd_op2(compiler, SljitSimdTypeForScale(scale) | op, SLJIT_VR(dst), SLJIT_VR(left.reg),
		                    SLJIT_VR(right.reg), 0);
		FreeSimdValue(ctx, left);
		FreeSimdValue(ctx, right);
		return {dst, true};
	}
	default:
		throw InternalException("Unsupported SLJIT SIMD value node kind");
	}
}

// Emit one comparison at the given element scale / row offset, producing a mask
// of that width.
SljitSimdValue EmitSljitSimdComparisonAt(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                         SljitSimdEmitContext &ctx, sljit_s32 scale, sljit_sw row_offset) {
	auto left = EmitSljitSimdValueExpr(compiler, *node.left, ctx, scale, row_offset);
	auto right = EmitSljitSimdValueExpr(compiler, *node.right, ctx, scale, row_offset);
	auto dst = AllocSimdTemp(ctx);
	auto st = SljitSimdTypeForScale(scale);
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN: // left > right
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(left.reg), SLJIT_VR(right.reg),
		                    0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN: // left < right == right > left
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(right.reg), SLJIT_VR(left.reg),
		                    0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO: // !(left < right) == !(right > left)
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(right.reg), SLJIT_VR(left.reg),
		                    0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO: // !(left > right)
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPGT, SLJIT_VR(dst), SLJIT_VR(left.reg), SLJIT_VR(right.reg),
		                    0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPEQ, SLJIT_VR(dst), SLJIT_VR(left.reg), SLJIT_VR(right.reg),
		                    0);
		break;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		sljit_emit_simd_op2(compiler, st | SLJIT_SIMD_OP2_CMPEQ, SLJIT_VR(dst), SLJIT_VR(left.reg), SLJIT_VR(right.reg),
		                    0);
		EmitSljitSimdNot(compiler, ctx, dst);
		break;
	default:
		throw InternalException("Unsupported SLJIT SIMD comparison operator");
	}
	FreeSimdValue(ctx, left);
	FreeSimdValue(ctx, right);
	return {dst, true};
}

SljitSimdValue EmitSljitSimdMask(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                 SljitSimdEmitContext &ctx);

// Emit a comparison as a LOOP-width mask. In mixed mode a 64-bit comparison is
// evaluated for both row halves and the two 2x64 masks are narrowed into one
// 4x32 mask; otherwise the comparison width equals the loop width.
SljitSimdValue EmitSljitSimdComparisonMask(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                           SljitSimdEmitContext &ctx) {
	auto comparison_scale = SljitSimdSubtreeScale(*node.left);
	if (comparison_scale == 0) {
		comparison_scale = SljitSimdSubtreeScale(*node.right);
	}
	if (comparison_scale == 0) {
		throw InternalException("SLJIT SIMD comparison has no anchored element width");
	}
	if (!ctx.mixed || comparison_scale == ctx.scale) {
		return EmitSljitSimdComparisonAt(compiler, node, ctx, ctx.scale, 0);
	}
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	D_ASSERT(comparison_scale == 3 && ctx.scale == 2);
	auto lo = EmitSljitSimdComparisonAt(compiler, node, ctx, 3, 0);
	auto hi = EmitSljitSimdComparisonAt(compiler, node, ctx, 3, 2);
	auto dst = AllocSimdTemp(ctx);
	EmitSljitSimdUzp1Narrow(compiler, dst, lo.reg, hi.reg);
	FreeSimdValue(ctx, lo);
	FreeSimdValue(ctx, hi);
	return {dst, true};
#else
	throw InternalException("SLJIT SIMD mixed-width mask narrowing requires ARM64");
#endif
}

SljitSimdValue EmitSljitSimdConjunction(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        SljitSimdEmitContext &ctx) {
	auto combine =
	    node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? SLJIT_SIMD_OP2_AND : SLJIT_SIMD_OP2_OR;
	auto acc = EmitSljitSimdMask(compiler, *node.children[0], ctx);
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
	case ExecutionExpressionIRKind::BINARY:
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return EmitSljitSimdComparisonMask(compiler, node, ctx);
		}
		// A bare value expression used where a mask is expected only happens for
		// pure-width payload evaluation; route through the value emitter.
		return EmitSljitSimdValueExpr(compiler, node, ctx, ctx.scale, 0);
	case ExecutionExpressionIRKind::REFERENCE:
	case ExecutionExpressionIRKind::CONSTANT:
		return EmitSljitSimdValueExpr(compiler, node, ctx, ctx.scale, 0);
	case ExecutionExpressionIRKind::CONJUNCTION:
		return EmitSljitSimdConjunction(compiler, node, ctx);
	default:
		throw InternalException("Unsupported SLJIT SIMD node kind");
	}
}

// Initialize the persistent lane-bit-position register {1,2,...} through the
// caller's 16-byte scratch slot (replicate cannot produce distinct lanes).
void EmitSljitSimdInitLaneBits(struct sljit_compiler *compiler, SljitSimdEmitContext &ctx, sljit_s32 lanes,
                               sljit_sw scratch_offset) {
	ctx.lane_bits_reg = ctx.next_temp++;
	auto lane_bytes = NumericCast<sljit_sw>(sljit_sw(16) / lanes);
	auto store_op = lanes == 4 ? SLJIT_MOV32 : SLJIT_MOV;
	for (sljit_s32 lane = 0; lane < lanes; lane++) {
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(SLJIT_SP), scratch_offset + lane * lane_bytes, SLJIT_IMM,
		               sljit_sw(1) << lane);
	}
	sljit_emit_simd_mov(compiler, ctx.simd_type, SLJIT_VR(ctx.lane_bits_reg), SLJIT_MEM1(SLJIT_SP), scratch_offset);
}

// AND the group's validity into `mask_reg`: combine the per-source validity bits
// for rows S1..S1+lanes-1 into a nibble (a NULL validity pointer means all-valid),
// and unless the nibble is full, expand it to lanes (broadcast, AND lane bits,
// CMPEQ lane bits) and AND it into the mask. Clobbers R0-R3.
void EmitSljitSimdValidityAnd(struct sljit_compiler *compiler, SljitSimdEmitContext &ctx,
                              const vector<idx_t> &source_refs, sljit_s32 lanes, sljit_s32 mask_reg) {
	const sljit_sw full_bits = (sljit_sw(1) << lanes) - 1;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, full_bits);
	for (auto source_index : source_refs) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
		               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
		auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		// R3 = validity_word[S1 >> 6] >> (S1 & 63); the group's bits stay inside one
		// word because the loop advances S1 by `lanes`, which divides 64.
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	}
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, full_bits);
	// Common case: every row in the group is valid — leave the mask untouched.
	auto group_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, full_bits);
	auto nibble = AllocSimdTemp(ctx);
	sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(nibble), SLJIT_R2, 0);
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_AND, SLJIT_VR(nibble), SLJIT_VR(nibble),
	                    SLJIT_VR(ctx.lane_bits_reg), 0);
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_CMPEQ, SLJIT_VR(nibble), SLJIT_VR(nibble),
	                    SLJIT_VR(ctx.lane_bits_reg), 0);
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_AND, SLJIT_VR(mask_reg), SLJIT_VR(mask_reg),
	                    SLJIT_VR(nibble), 0);
	FreeSimdValue(ctx, {nibble, true});
	sljit_set_label(group_all_valid, sljit_emit_label(compiler));
}

// Broadcast the distinct constants of a VALUE subtree at the given element scale.
void EmitSljitSimdBroadcastValueConstants(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          SljitSimdEmitContext &ctx, sljit_s32 scale) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT: {
		auto value = SljitTypedExpressionTreeConstantValue(node);
		if (ctx.constant_reg.find({value, scale}) != ctx.constant_reg.end()) {
			return;
		}
		auto reg = ctx.next_temp++;
		ctx.constant_reg[{value, scale}] = reg;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, NumericCast<sljit_sw>(value));
		sljit_emit_simd_replicate(compiler, SljitSimdTypeForScale(scale), SLJIT_VR(reg), SLJIT_R0, 0);
		return;
	}
	case ExecutionExpressionIRKind::BINARY:
		if (node.left) {
			EmitSljitSimdBroadcastValueConstants(compiler, *node.left, ctx, scale);
		}
		if (node.right) {
			EmitSljitSimdBroadcastValueConstants(compiler, *node.right, ctx, scale);
		}
		return;
	default:
		return;
	}
}

// Broadcast every distinct (value, width) constant of a MASK tree into a
// persistent vector register before the loop starts. Each comparison's constants
// broadcast at that comparison's anchored width.
void EmitSljitSimdBroadcastConstants(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                     SljitSimdEmitContext &ctx) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::BINARY: {
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			auto comparison_scale = SljitSimdSubtreeScale(*node.left);
			if (comparison_scale == 0) {
				comparison_scale = SljitSimdSubtreeScale(*node.right);
			}
			if (comparison_scale == 0) {
				comparison_scale = ctx.scale;
			}
			EmitSljitSimdBroadcastValueConstants(compiler, *node.left, ctx, comparison_scale);
			EmitSljitSimdBroadcastValueConstants(compiler, *node.right, ctx, comparison_scale);
			return;
		}
		EmitSljitSimdBroadcastValueConstants(compiler, node, ctx, ctx.scale);
		return;
	}
	case ExecutionExpressionIRKind::CONJUNCTION:
		for (auto &child : node.children) {
			EmitSljitSimdBroadcastConstants(compiler, *child, ctx);
		}
		return;
	default:
		EmitSljitSimdBroadcastValueConstants(compiler, node, ctx, ctx.scale);
		return;
	}
}

} // namespace

// Materialize the deferred identity prefix [0, S1) when a packed filter sees
// its first false lane. S3 == S1 means every preceding row passed. A completely
// all-true selection stays implicit and is represented by selected_count.
static void EmitSljitSimdEnsureIdentityPrefixMaterialized(struct sljit_compiler *compiler) {
	auto already_materialized = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_S3, 0, SLJIT_S1, 0);
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto backfill_loop = sljit_emit_label(compiler);
	auto backfill_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_R0, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_R0), 2, SLJIT_R0, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
	auto backfill_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(backfill_repeat, backfill_loop);
	auto materialized = sljit_emit_label(compiler);
	sljit_set_label(already_materialized, materialized);
	sljit_set_label(no_true_sel, materialized);
	sljit_set_label(backfill_done, materialized);
}

void EmitSljitTypedExpressionTreeSimdSelectLoop(struct sljit_compiler *compiler, const ExecutionExpressionIR &root,
                                                const SljitTypedExpressionTreeSimdPlan &plan, sljit_sw mask_offset,
                                                const vector<idx_t> *validity_refs) {
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;
	ctx.mixed = plan.mixed_width;

	// Persistent registers: constants first, then all-ones and lane bits.
	EmitSljitSimdBroadcastConstants(compiler, root, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	if (validity_refs) {
		EmitSljitSimdInitLaneBits(compiler, ctx, plan.lanes, mask_offset);
	}
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	// Persistent reduction destination; predicate temporaries start after it.
	auto mask_reduce_reg = ctx.next_temp++;
#endif

	// Flat packed selection does not use the generic-loop logical-index register
	// (S3) or the source-selection-array register (S4). Keep the append cursor in
	// those saved registers for the whole packed loop. Reloading selected_count
	// and true_sel through native_input for every true lane made result compaction
	// more expensive than the comparison itself for common column-vs-column
	// filters.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, selected_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	const bool hoist_source_data = !validity_refs && !plan.source_refs.empty() && plan.source_refs.size() <= 2;
	if (hoist_source_data) {
		// Load the second pointer first because S5 initially owns the pointer
		// array and becomes the first source pointer after the final load.
		if (plan.source_refs.size() == 2) {
			auto source_index = plan.source_refs[1];
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S5),
			               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
			ctx.source_data_reg[source_index] = SLJIT_S6;
		}
		auto source_index = plan.source_refs[0];
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
		ctx.source_data_reg[source_index] = SLJIT_S5;
	}

	// S1 is the flat row base (already 0 after the loop-init helper).
	auto simd_loop = sljit_emit_label(compiler);
	// Exit the packed loop when fewer than `lanes` rows remain: R0 = S1 + lanes; if R0 > S2 stop.
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto simd_done = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);

	auto mask = EmitSljitSimdMask(compiler, root, ctx);
	if (validity_refs) {
		EmitSljitSimdValidityAnd(compiler, ctx, *validity_refs, plan.lanes, mask.reg);
	}
	// Classify all-true/all-false masks cheaply. ARM64 uses a two-instruction
	// horizontal lane sum; other backends use their native movemask. The full
	// bitset is needed only by the mixed-lane path below.
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	EmitSljitSimdMaskLaneSum(compiler, mask_reduce_reg, mask.reg, plan.elem_scale, SLJIT_R3);
	const auto all_lanes_mask = NumericCast<sljit_sw>(-plan.lanes);
#else
	sljit_emit_simd_sign(compiler, ctx.simd_type | SLJIT_SIMD_STORE | SLJIT_32, SLJIT_VR(mask.reg), SLJIT_R3, 0);
	const auto all_lanes_mask = NumericCast<sljit_sw>((sljit_uw(1) << plan.lanes) - 1);
#endif
	auto not_all_lanes = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, all_lanes_mask);

	// All lanes pass. Defer writes while the whole prefix remains identity;
	// already-mixed vectors append the contiguous group eagerly.
	auto defer_all_true = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_S1, 0);
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_S3, 0, SLJIT_IMM, 2);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_S4, 0, SLJIT_R2, 0);
	for (sljit_s32 lane = 0; lane < plan.lanes; lane++) {
		if (lane == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM1(SLJIT_R2), 0, SLJIT_S1, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM1(SLJIT_R2), lane * sizeof(sel_t), SLJIT_R1, 0);
		}
	}
	sljit_set_label(no_true_sel, sljit_emit_label(compiler));
	auto count_all_true = sljit_emit_label(compiler);
	sljit_set_label(defer_all_true, count_all_true);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, plan.lanes);
	auto lanes_appended = sljit_emit_jump(compiler, SLJIT_JUMP);

	// A mixed mask is uncommon for high-selectivity range predicates. Test its
	// scalar bitset without reloading vector lanes from the stack.
	sljit_set_label(not_all_lanes, sljit_emit_label(compiler));
	auto no_lanes_true = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	EmitSljitSimdEnsureIdentityPrefixMaterialized(compiler);
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	sljit_emit_simd_sign(compiler, ctx.simd_type | SLJIT_SIMD_STORE | SLJIT_32, SLJIT_VR(mask.reg), SLJIT_R3, 0);
#endif
	for (sljit_s32 lane = 0; lane < plan.lanes; lane++) {
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R3, 0, SLJIT_IMM, sljit_sw(1) << lane);
		auto lane_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
		auto no_mixed_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_R1, 0);
		sljit_set_label(no_mixed_true_sel, sljit_emit_label(compiler));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_set_label(lane_false, sljit_emit_label(compiler));
	}
	auto mixed_lanes_appended = sljit_emit_jump(compiler, SLJIT_JUMP);

	// The first empty group proves the deferred identity prefix now needs to be
	// real. Later empty groups skip the backfill because S3 no longer equals S1.
	sljit_set_label(no_lanes_true, sljit_emit_label(compiler));
	EmitSljitSimdEnsureIdentityPrefixMaterialized(compiler);
	auto append_done = sljit_emit_label(compiler);
	sljit_set_label(lanes_appended, append_done);
	sljit_set_label(mixed_lanes_appended, append_done);

	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, simd_loop);
	sljit_set_label(simd_done, sljit_emit_label(compiler));
	// The scalar tail uses the eager append helper. If a partial group remains,
	// materialize a still-deferred prefix before handing control back to it.
	auto no_scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitSimdEnsureIdentityPrefixMaterialized(compiler);
	sljit_set_label(no_scalar_tail, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_S3, 0);
	if (hoist_source_data) {
		// The caller's scalar tail expects the generic source-array register map.
		EmitInitSljitNativeVectorSourceArrays(compiler);
	}
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
                                             sljit_sw count_offset, sljit_sw saw_value_offset, sljit_sw scratch_offset,
                                             const vector<idx_t> *validity_refs) {
	// plan is the predicate plan; the packed path requires a 32-bit (4-lane) predicate.
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;
	ctx.mixed = plan.mixed_width;
	auto sum_type = SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_64;

	// Predicate and payload constants share one broadcast map (deduplicated by value).
	EmitSljitSimdBroadcastConstants(compiler, predicate, ctx);
	EmitSljitSimdBroadcastConstants(compiler, payload, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	if (validity_refs) {
		EmitSljitSimdInitLaneBits(compiler, ctx, plan.lanes, scratch_offset);
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
	if (validity_refs) {
		EmitSljitSimdValidityAnd(compiler, ctx, *validity_refs, plan.lanes, mask.reg);
	}
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
                                               sljit_sw mask_offset, const vector<idx_t> *validity_refs) {
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;
	ctx.mixed = plan.mixed_width;

	EmitSljitSimdBroadcastConstants(compiler, root, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	if (validity_refs) {
		EmitSljitSimdInitLaneBits(compiler, ctx, plan.lanes, mask_offset);
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
	if (validity_refs) {
		EmitSljitSimdValidityAnd(compiler, ctx, *validity_refs, plan.lanes, mask.reg);
	}
	// mask lanes are all-ones (=-1) where true; acc -= mask adds 1 per matching lane.
	sljit_emit_simd_op2(compiler, ctx.simd_type | SLJIT_SIMD_OP2_SUB, SLJIT_VR(acc), SLJIT_VR(acc), SLJIT_VR(mask.reg),
	                    0);
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

void EmitSljitTypedExpressionTreeSimdHybridFilterLoop(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &predicate,
                                                      const SljitTypedExpressionTreeSimdPlan &plan,
                                                      sljit_sw mask_offset,
                                                      const std::function<void()> &emit_matching_row,
                                                      const vector<idx_t> *validity_refs) {
	SljitSimdEmitContext ctx;
	ctx.simd_type = plan.simd_type;
	ctx.scale = plan.elem_scale;
	ctx.mixed = plan.mixed_width;

	EmitSljitSimdBroadcastConstants(compiler, predicate, ctx);
	if (plan.needs_all_ones) {
		ctx.all_ones_reg = ctx.next_temp++;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, -1);
		sljit_emit_simd_replicate(compiler, ctx.simd_type, SLJIT_VR(ctx.all_ones_reg), SLJIT_R0, 0);
	}
	if (validity_refs) {
		EmitSljitSimdInitLaneBits(compiler, ctx, plan.lanes, mask_offset);
	}

	// S1 is the flat row base (0 on entry).
	auto simd_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto simd_done = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);

	auto mask = EmitSljitSimdMask(compiler, predicate, ctx);
	if (validity_refs) {
		EmitSljitSimdValidityAnd(compiler, ctx, *validity_refs, plan.lanes, mask.reg);
	}
	sljit_emit_simd_sign(compiler, ctx.simd_type | SLJIT_SIMD_STORE | SLJIT_32, SLJIT_VR(mask.reg), SLJIT_R3, 0);
	FreeSimdValue(ctx, mask);

	// High-selectivity predicates commonly produce an all-true mask. Run that
	// group in a compact scalar lane loop without per-lane tests. Mixed masks use
	// a second compact loop over the scalar sign bitset. Keeping one callback body
	// per control-flow class avoids multiplying a large aggregate reducer by the
	// SIMD lane count.
	const auto all_lanes_mask = NumericCast<sljit_sw>((sljit_uw(1) << plan.lanes) - 1);
	auto mixed_mask = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, all_lanes_mask);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), mask_offset + 24, SLJIT_R0, 0);
	auto all_true_loop = sljit_emit_label(compiler);
	emit_matching_row();
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + 24);
	auto repeat_all_true = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_R0, 0);
	sljit_set_label(repeat_all_true, all_true_loop);
	auto group_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(mixed_mask, sljit_emit_label(compiler));
	auto no_lanes_true = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), mask_offset + 16, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), mask_offset + 24, SLJIT_R0, 0);
	auto mixed_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + 16);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	auto lane_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	emit_matching_row();
	sljit_set_label(lane_false, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + 16);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), mask_offset + 16, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), mask_offset + 24);
	auto repeat_mixed = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_R0, 0);
	sljit_set_label(repeat_mixed, mixed_loop);
	auto mixed_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_lanes_true, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, plan.lanes);
	auto advance_done = sljit_emit_label(compiler);
	sljit_set_label(group_done, advance_done);
	sljit_set_label(mixed_done, advance_done);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, simd_loop);
	sljit_set_label(simd_done, sljit_emit_label(compiler));
}

} // namespace duckdb

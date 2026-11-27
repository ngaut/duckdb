//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_fast_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

//! A value one instruction produces: a constant, or a column's load (a narrow signed column's sign-extending load is
//! its widening cast too)
static bool SljitTypedExpressionTreeFastIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE ||
	       SljitTypedExpressionTreeIsNarrowSourceWideningCast(node);
}

static void
EmitSljitTypedExpressionTreeFastValueRegInternal(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                 SljitTypedExpressionTreeFastIndexMode index_mode,
                                                 const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);

//! The registers a fast loop reads a source's row with: the source's data pointer (its hoisted register, else loaded
//! into R0) and the row's index (a flat or logical row's loop index register, else the selected row resolved into R1)
static void EmitSljitTypedExpressionTreeFastSourceRow(struct sljit_compiler *compiler, idx_t source_index,
                                                      SljitTypedExpressionTreeFastIndexMode index_mode,
                                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                      sljit_s32 &data_reg, sljit_s32 &index_reg) {
	index_reg = SLJIT_R1;
	switch (index_mode) {
	case SljitTypedExpressionTreeFastIndexMode::FLAT:
		index_reg = SLJIT_S1;
		break;
	case SljitTypedExpressionTreeFastIndexMode::LOGICAL:
		index_reg = SLJIT_S3;
		break;
	case SljitTypedExpressionTreeFastIndexMode::SELECTED:
		EmitLoadSljitTypedExpressionTreeFastSourceIndex(compiler, source_index, SLJIT_R1, index_mode, data_hoists);
		break;
	}
	if (!TryGetSljitTypedExpressionTreeSourceDataHoist(data_hoists, source_index, data_reg)) {
		data_reg = SLJIT_R0;
		sljit_emit_op1(compiler, SLJIT_MOV_P, data_reg, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	}
}

static void EmitSljitTypedExpressionTreeFastLeafReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &leaf,
                                                    sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	const auto &node = leaf.kind == ExecutionExpressionIRKind::CAST ? *leaf.left : leaf;
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::REFERENCE);
	sljit_s32 data_reg;
	sljit_s32 index_reg;
	EmitSljitTypedExpressionTreeFastSourceRow(compiler, node.ref_index, index_mode, data_hoists, data_reg, index_reg);
	sljit_s32 load_op;
	sljit_sw scale;
	if (SljitTypedExpressionTreeIsNarrowSignedReference(node)) {
		GetSljitTypedExpressionTreeNarrowSourceLoad(node, load_op, scale);
	} else {
		auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
		load_op = NativeIntegerLoadOp(source_kind);
		scale = NativeIntegerDataScale(source_kind);
	}
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(data_reg, index_reg), scale);
}

//! A comparison, modulo or division reads a constant right operand as an immediate, so it never takes a register
static bool SljitTypedExpressionTreeFastRightIsImmediate(const ExecutionExpressionIR &node) {
	return node.right->kind == ExecutionExpressionIRKind::CONSTANT &&
	       (SljitTypedExpressionTreeComparisonSupported(node.binary_op) ||
	        node.binary_op == ExecutionExpressionBinaryOp::MODULO ||
	        node.binary_op == ExecutionExpressionBinaryOp::INTEGER_DIVIDE);
}

//! Emits the subtraction whose flags hold a comparison's result and returns the condition those flags satisfy when
//! the comparison is true
static sljit_s32 EmitSljitTypedExpressionTreeFastCompareFlags(struct sljit_compiler *compiler,
                                                              const ExecutionExpressionIR &node, sljit_s32 left_reg,
                                                              sljit_s32 right_reg) {
	auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
	                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
	const auto condition = compare_type & ~SLJIT_32;
	const auto compare_op = SLJIT_SUB | (compare_type & SLJIT_32) | SljitCompareConditionSetFlags(condition);
	if (SljitTypedExpressionTreeFastRightIsImmediate(node)) {
		sljit_emit_op2u(compiler, compare_op, left_reg, 0, SLJIT_IMM,
		                NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*node.right)));
	} else {
		sljit_emit_op2u(compiler, compare_op, left_reg, 0, right_reg, 0);
	}
	return condition;
}

static void EmitSljitTypedExpressionTreeFastBinaryOpReg(struct sljit_compiler *compiler,
                                                        const ExecutionExpressionIR &node, sljit_s32 left_reg,
                                                        sljit_s32 right_reg, sljit_s32 target_reg,
                                                        vector<SljitArithmeticOverflowSite> &overflows,
                                                        const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		// the flags of the subtraction set the result: a comparison takes no branch
		auto condition = EmitSljitTypedExpressionTreeFastCompareFlags(compiler, node, left_reg, right_reg);
		sljit_emit_op_flags(compiler, SLJIT_MOV, target_reg, 0, condition);
		return;
	}
	if (SljitTypedExpressionTreeIsDoubleNode(node)) {
		EmitSljitTypedExpressionTreeDoubleBinary(compiler, node.binary_op, target_reg, left_reg, right_reg);
		return;
	}
	if (node.binary_op == ExecutionExpressionBinaryOp::MODULO ||
	    node.binary_op == ExecutionExpressionBinaryOp::INTEGER_DIVIDE) {
		// left_reg holds the dividend and is never R0/R1 here (the fast path only parks live values in R2-R4 and on
		// the stack), so the op0 magic-multiply reduction can borrow R0/R1 freely.
		const int64_t divisor = SljitTypedExpressionTreeConstantValue(*node.right);
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		if (node.binary_op == ExecutionExpressionBinaryOp::MODULO) {
			EmitSljitTypedExpressionTreeModulo(compiler, divisor, binary_kind, left_reg, target_reg, data_hoists);
		} else {
			EmitSljitTypedExpressionTreeIntegerDivide(compiler, divisor, binary_kind, left_reg, target_reg,
			                                          data_hoists);
		}
		return;
	}
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
	}
	// The target may alias an operand: a sum or difference overwrites it in place (the overflow stub recovers it), a
	// product lands in the dead scratch R1 so both operands survive for the stub.
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
	const auto result_reg = native_op == SljitNativeIntegerBinaryOp::MULTIPLY ? SLJIT_R1 : target_reg;
	EmitSljitCheckedIntegerBinaryOp(compiler, binary_kind, native_op, result_reg, left_reg, right_reg,
	                                SljitTypedExpressionTreeOverflowChecks(node), overflows);
	if (result_reg != target_reg) {
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, result_reg, 0);
	}
}

static bool SljitTypedExpressionTreeFastIsLeafBinary(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::BINARY && node.left && node.right &&
	       SljitTypedExpressionTreeFastIsLeaf(*node.left) && SljitTypedExpressionTreeFastIsLeaf(*node.right);
}

static void
EmitSljitTypedExpressionTreeFastLeafBinaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                              vector<SljitArithmeticOverflowSite> &overflows,
                                              const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(SljitTypedExpressionTreeFastIsLeafBinary(node));
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.left, target, index_mode, data_hoists);
	if (!SljitTypedExpressionTreeFastRightIsImmediate(node)) {
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R3, index_mode, data_hoists);
	}
	EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, target, SLJIT_R3, target, overflows, data_hoists);
}

static bool SljitTypedExpressionTreeFastIsStringCompare(const ExecutionExpressionIR &node) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	return TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal);
}

//! Jumps when a DOUBLE/FLOAT comparison with a constant is `when` at the row. Clobbers R0, R1, FR0 and FR1.
static sljit_jump *EmitSljitTypedExpressionTreeFastFloatCompareJump(
    struct sljit_compiler *compiler, const SljitTypedExpressionTreeFloatCompare &compare, bool when,
    SljitTypedExpressionTreeFastIndexMode index_mode, const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	sljit_s32 data_reg;
	sljit_s32 index_reg;
	EmitSljitTypedExpressionTreeFastSourceRow(compiler, compare.source_index, index_mode, data_hoists, data_reg,
	                                          index_reg);
	EmitLoadSljitTypedExpressionTreeFloatValue(compiler, compare, data_reg, index_reg);
	return EmitSljitTypedExpressionTreeFloatCompareJump(compiler, compare, when, data_hoists);
}

struct SljitTypedExpressionTreeFastOperands {
	sljit_s32 left;
	//! unused when the right operand is an immediate
	sljit_s32 right;
};

static SljitTypedExpressionTreeFastOperands
EmitSljitTypedExpressionTreeFastBinaryOperands(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                               idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                               SljitTypedExpressionTreeFastIndexMode index_mode,
                                               const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
	                                                 data_hoists);
	if (SljitTypedExpressionTreeFastIsLeafBinary(*node.right)) {
		EmitSljitTypedExpressionTreeFastLeafBinaryReg(compiler, *node.right, SLJIT_R4, index_mode, overflows,
		                                              data_hoists);
		return {SLJIT_R2, SLJIT_R4};
	}
	if (SljitTypedExpressionTreeFastIsLeaf(*node.right)) {
		if (!SljitTypedExpressionTreeFastRightIsImmediate(node)) {
			EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R4, index_mode, data_hoists);
		}
		return {SLJIT_R2, SLJIT_R4};
	}

	auto left_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), left_offset, SLJIT_R2, 0);
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.right, spill_index, overflows, index_mode,
	                                                 data_hoists);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), left_offset);
	return {SLJIT_R4, SLJIT_R2};
}

static void EmitSljitTypedExpressionTreeFastBinaryReg(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &node, idx_t &spill_index,
                                                      vector<SljitArithmeticOverflowSite> &overflows,
                                                      SljitTypedExpressionTreeFastIndexMode index_mode,
                                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	if (SljitTypedExpressionTreeFastIsStringCompare(node)) {
		EmitSljitTypedExpressionTreeFastStringCompareReg(compiler, node, index_mode, data_hoists);
		return;
	}
	SljitTypedExpressionTreeFloatCompare float_compare;
	if (TryReadSljitTypedExpressionTreeFloatCompareConstant(node, float_compare)) {
		auto is_true =
		    EmitSljitTypedExpressionTreeFastFloatCompareJump(compiler, float_compare, true, index_mode, data_hoists);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(is_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_set_label(done, sljit_emit_label(compiler));
		return;
	}
	auto operands =
	    EmitSljitTypedExpressionTreeFastBinaryOperands(compiler, node, spill_index, overflows, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, operands.left, operands.right, SLJIT_R2, overflows,
	                                            data_hoists);
}

static void EmitSljitTypedExpressionTreeFastUnaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                     SljitTypedExpressionTreeFastIndexMode index_mode,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(node.left);
	switch (node.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
		                                                 data_hoists);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		return;
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
		                                                 data_hoists);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		return;
	case ExecutionExpressionUnaryOp::NOT:
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
		                                                 data_hoists);
		{
			auto child_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(child_false, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		return;
	case ExecutionExpressionUnaryOp::NEGATE:
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
		                                                 data_hoists);
		EmitSljitTypedExpressionTreeNegate(compiler, node, SLJIT_R2, overflows);
		return;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast unary operator");
	}
}

static void EmitSljitTypedExpressionTreeFastCastReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                    SljitTypedExpressionTreeFastIndexMode index_mode,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(node.left);
	D_ASSERT(SljitTypedExpressionTreeValueCastSupported(node));
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
	                                                 data_hoists);
	if (SljitTypedExpressionTreeIsInt32Node(*node.left)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
	if (SljitTypedExpressionTreeIsDoubleNode(node)) {
		EmitSljitTypedExpressionTreeConvertToDouble(compiler, *node.left, SLJIT_R2);
	}
}

static void
EmitSljitTypedExpressionTreeFastConjunctionReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                               idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                               SljitTypedExpressionTreeFastIndexMode index_mode,
                                               const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	vector<sljit_jump *> decisive_jumps;
	for (auto &child : node.children) {
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *child, spill_index, overflows, index_mode,
		                                                 data_hoists);
		if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		} else {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 1 : 0);
	auto done_from_default = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto decisive_label = sljit_emit_label(compiler);
	for (auto jump : decisive_jumps) {
		sljit_set_label(jump, decisive_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 0 : 1);
	sljit_set_label(done_from_default, sljit_emit_label(compiler));
}

static void EmitSljitTypedExpressionTreeFastCoalesceReg(struct sljit_compiler *compiler,
                                                        const ExecutionExpressionIR &node, idx_t &spill_index,
                                                        vector<SljitArithmeticOverflowSite> &overflows,
                                                        SljitTypedExpressionTreeFastIndexMode index_mode,
                                                        const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(!node.children.empty());
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[0], spill_index, overflows, index_mode,
	                                                 data_hoists);
}

//! Whether a CASE selects its result without a jump: one condition, an integer comparison whose flags pick between two
//! leaf results. The conditional move replaces the comparison's boolean and the jump on it, so the select takes no more
//! instructions than the branch and never mispredicts on the data. The leaves load between the comparison and the
//! select, which a selected row's index load would break (it tests the selection)
static bool SljitTypedExpressionTreeFastCaseSelects(const ExecutionExpressionIR &node,
                                                    SljitTypedExpressionTreeFastIndexMode index_mode) {
	if (node.children.size() != 2 || index_mode == SljitTypedExpressionTreeFastIndexMode::SELECTED) {
		return false;
	}
	auto &condition = *node.children[0];
	SljitTypedExpressionTreeFloatCompare float_compare;
	return condition.kind == ExecutionExpressionIRKind::BINARY && condition.left && condition.right &&
	       SljitTypedExpressionTreeComparisonSupported(condition.binary_op) &&
	       !SljitTypedExpressionTreeFastIsStringCompare(condition) &&
	       !TryReadSljitTypedExpressionTreeFloatCompareConstant(condition, float_compare) &&
	       SljitTypedExpressionTreeFastIsLeaf(*node.children[1]) && SljitTypedExpressionTreeFastIsLeaf(*node.else_node);
}

//! The comparison sets the flags; the leaf results load without touching them (a flat or logical row's load writes R0,
//! R1 and its target)
static void
EmitSljitTypedExpressionTreeFastCaseSelectReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                              SljitTypedExpressionTreeFastIndexMode index_mode,
                                              const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	auto &condition = *node.children[0];
	auto operands = EmitSljitTypedExpressionTreeFastBinaryOperands(compiler, condition, spill_index, overflows,
	                                                               index_mode, data_hoists);
	const auto holds = EmitSljitTypedExpressionTreeFastCompareFlags(compiler, condition, operands.left, operands.right);
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.else_node, SLJIT_R3, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.children[1], SLJIT_R4, index_mode, data_hoists);
	sljit_emit_select(compiler, holds, SLJIT_R2, SLJIT_R4, 0, SLJIT_R3);
}

static void EmitSljitTypedExpressionTreeFastCaseReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                    SljitTypedExpressionTreeFastIndexMode index_mode,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	if (SljitTypedExpressionTreeFastCaseSelects(node, index_mode)) {
		EmitSljitTypedExpressionTreeFastCaseSelectReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	}
	vector<sljit_jump *> done_jumps;
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = node.children[child_idx];
		auto &value = node.children[child_idx + 1];
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *condition, spill_index, overflows, index_mode,
		                                                 data_hoists);
		auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *value, spill_index, overflows, index_mode,
		                                                 data_hoists);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(condition_false, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.else_node, spill_index, overflows, index_mode,
	                                                 data_hoists);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
}

static void
EmitSljitTypedExpressionTreeFastIntegralCompressReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                    SljitTypedExpressionTreeFastIndexMode index_mode,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(node.children.size() == 2);
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[0], spill_index, overflows, index_mode,
	                                                 data_hoists);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*node.children[1])));
}

static void
EmitSljitTypedExpressionTreeFastValueRegInternal(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                 SljitTypedExpressionTreeFastIndexMode index_mode,
                                                 const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
	case ExecutionExpressionIRKind::REFERENCE:
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, node, SLJIT_R2, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::CAST:
		EmitSljitTypedExpressionTreeFastCastReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::UNARY:
		EmitSljitTypedExpressionTreeFastUnaryReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::BINARY:
		EmitSljitTypedExpressionTreeFastBinaryReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::CONJUNCTION:
		EmitSljitTypedExpressionTreeFastConjunctionReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::COALESCE:
		EmitSljitTypedExpressionTreeFastCoalesceReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::CASE:
		EmitSljitTypedExpressionTreeFastCaseReg(compiler, node, spill_index, overflows, index_mode, data_hoists);
		return;
	case ExecutionExpressionIRKind::INTRINSIC:
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS) {
			EmitSljitTypedExpressionTreeFastIntegralCompressReg(compiler, node, spill_index, overflows, index_mode,
			                                                    data_hoists);
			return;
		}
		EmitSljitTypedExpressionTreeFastStringPrefixReg(compiler, node, index_mode, data_hoists);
		return;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast node kind");
	}
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeFastJumpsInternal(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists, bool null_rows);

//! year() of a column's date into R2, through the scratch registers R0-R4; an infinite date leaves through a null
//! exit. A compress around it subtracts its constant
static void
EmitSljitTypedExpressionTreeFastNullExitDateYearReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    SljitTypedExpressionTreeFastIndexMode index_mode,
                                                    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                    vector<sljit_jump *> &null_exits) {
	if (node.intrinsic == ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS) {
		EmitSljitTypedExpressionTreeFastNullExitDateYearReg(compiler, *node.children[0], index_mode, data_hoists,
		                                                    null_exits);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*node.children[1])));
		return;
	}
	D_ASSERT(node.intrinsic == ExecutionExpressionIntrinsicKind::DATE_YEAR);
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.children[0], SLJIT_R2, index_mode, data_hoists);
	null_exits.push_back(
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::infinity().days)));
	null_exits.push_back(
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::ninfinity().days)));
	EmitSljitDateYearFromDays(compiler, SLJIT_R2, SLJIT_R2);
}

//! A node SljitTypedExpressionTreeFastNullExitSupported admits: a CASE result that is a NULL constant leaves through a
//! null exit, as does year() of an infinite date; every other result is evaluated into R2
static void EmitSljitTypedExpressionTreeFastNullExitValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists, vector<sljit_jump *> &null_exits) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT && node.constant.IsNull()) {
		null_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC && !SljitTypedExpressionTreeFastPathSupported(node)) {
		EmitSljitTypedExpressionTreeFastNullExitDateYearReg(compiler, node, index_mode, data_hoists, null_exits);
		return;
	}
	if (node.kind != ExecutionExpressionIRKind::CASE || SljitTypedExpressionTreeFastPathSupported(node)) {
		EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows, index_mode,
		                                                 data_hoists);
		return;
	}
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	auto is_null = [](const ExecutionExpressionIR &value) {
		return value.kind == ExecutionExpressionIRKind::CONSTANT && value.constant.IsNull();
	};
	vector<sljit_jump *> done_jumps;
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = *node.children[child_idx];
		auto &value = *node.children[child_idx + 1];
		const bool last = child_idx + 2 == node.children.size();
		if (is_null(value)) {
			// a true condition leaves for the null exit, a false one falls through to the next branch
			auto condition_true = EmitSljitTypedExpressionTreeFastJumpsInternal(
			    compiler, condition, true, spill_index, overflows, index_mode, data_hoists, false);
			null_exits.insert(null_exits.end(), condition_true.begin(), condition_true.end());
			continue;
		}
		auto condition_false = EmitSljitTypedExpressionTreeFastJumpsInternal(compiler, condition, false, spill_index,
		                                                                     overflows, index_mode, data_hoists, false);
		if (last && is_null(*node.else_node)) {
			// a NULL ELSE: a false last condition leaves for the null exit directly
			null_exits.insert(null_exits.end(), condition_false.begin(), condition_false.end());
			EmitSljitTypedExpressionTreeFastNullExitValueReg(compiler, value, spill_index, overflows, index_mode,
			                                                 data_hoists, null_exits);
			SetSljitJumpLabels(done_jumps, sljit_emit_label(compiler));
			return;
		}
		EmitSljitTypedExpressionTreeFastNullExitValueReg(compiler, value, spill_index, overflows, index_mode,
		                                                 data_hoists, null_exits);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		SetSljitJumpLabels(condition_false, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeFastNullExitValueReg(compiler, *node.else_node, spill_index, overflows, index_mode,
	                                                 data_hoists, null_exits);
	SetSljitJumpLabels(done_jumps, sljit_emit_label(compiler));
}

static void EmitSljitTypedExpressionTreeFastRootValueReg(struct sljit_compiler *compiler,
                                                         const ExecutionExpressionIR &node, idx_t &spill_index,
                                                         vector<SljitArithmeticOverflowSite> &overflows,
                                                         SljitTypedExpressionTreeFastIndexMode index_mode,
                                                         const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                         vector<sljit_jump *> *null_exits) {
	if (null_exits) {
		EmitSljitTypedExpressionTreeFastNullExitValueReg(compiler, node, spill_index, overflows, index_mode,
		                                                 data_hoists, *null_exits);
		return;
	}
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows, index_mode, data_hoists);
}

void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                              const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                              vector<sljit_jump *> *null_exits) {
	EmitSljitTypedExpressionTreeFastRootValueReg(compiler, node, spill_index, overflows,
	                                             SljitTypedExpressionTreeFastIndexMode::FLAT, data_hoists, null_exits);
}

//! A remainder test of the dividend n against c (see SljitTypedExpressionTreeIsRemainderTest) without computing the
//! remainder: `n % d = c` holds exactly when n >= c and d divides n - c (c > 0), or when d divides |n| (c = 0). Both
//! differences fit an unsigned 64-bit value, which the divisibility test checks.
static vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastRemainderTestJumps(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                   idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                   const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	auto &modulo = *node.left;
	const auto remainder = SljitTypedExpressionTreeConstantValue(*node.right);
	auto constants = ComputeSljitRemainderTestConstants(SljitTypedExpressionTreeConstantValue(*modulo.right));
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *modulo.left, spill_index, overflows,
	                                                 SljitTypedExpressionTreeFastIndexMode::FLAT, data_hoists);
	vector<sljit_jump *> different;
	if (remainder > 0) {
		different.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, remainder));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, remainder);
	} else {
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R0, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_XOR, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R0, 0);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R0, 0);
	}
	auto inverse = SljitTypedExpressionTreeConstantOperand(data_hoists, constants.inverse);
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R2, 0, SLJIT_R2, 0, inverse.operand, inverse.operand_w);
	if (constants.shift > 0) {
		sljit_emit_op2(compiler, SLJIT_ROTR, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(constants.shift));
	}
	auto limit = SljitTypedExpressionTreeConstantOperand(data_hoists, constants.limit);
	if (node.binary_op == ExecutionExpressionBinaryOp::COMPARE_EQUAL) {
		auto equal = sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, SLJIT_R2, 0, limit.operand, limit.operand_w);
		SetSljitJumpLabels(different, sljit_emit_label(compiler));
		return {equal};
	}
	different.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, limit.operand, limit.operand_w));
	return different;
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastLeafJumps(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when,
                                          idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                          SljitTypedExpressionTreeFastIndexMode index_mode,
                                          const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		// a constant decides every row alike: the jump is unconditional or absent
		if ((SljitTypedExpressionTreeConstantValue(node) != 0) != when) {
			return {};
		}
		return {sljit_emit_jump(compiler, SLJIT_JUMP)};
	}
	if (index_mode == SljitTypedExpressionTreeFastIndexMode::FLAT && SljitTypedExpressionTreeIsRemainderTest(node)) {
		auto true_jumps =
		    EmitSljitTypedExpressionTreeFastRemainderTestJumps(compiler, node, spill_index, overflows, data_hoists);
		if (when) {
			return true_jumps;
		}
		auto false_jump = sljit_emit_jump(compiler, SLJIT_JUMP);
		SetSljitJumpLabels(true_jumps, sljit_emit_label(compiler));
		return {false_jump};
	}
	SljitTypedExpressionTreeFloatCompare float_compare;
	if (TryReadSljitTypedExpressionTreeFloatCompareConstant(node, float_compare)) {
		return {
		    EmitSljitTypedExpressionTreeFastFloatCompareJump(compiler, float_compare, when, index_mode, data_hoists)};
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op) &&
	    !SljitTypedExpressionTreeFastIsStringCompare(node)) {
		auto operands = EmitSljitTypedExpressionTreeFastBinaryOperands(compiler, node, spill_index, overflows,
		                                                               index_mode, data_hoists);
		const auto condition =
		    EmitSljitTypedExpressionTreeFastCompareFlags(compiler, node, operands.left, operands.right);
		// integer conditions pair with their inverse in the lowest bit
		return {sljit_emit_jump(compiler, when ? condition : condition ^ 1)};
	}
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows, index_mode, data_hoists);
	return {sljit_emit_cmp(compiler, when ? SLJIT_NOT_EQUAL : SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0)};
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeFastJumpsInternal(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists, bool null_rows) {
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION && !node.children.empty()) {
		// the outcome one child decides alone: true for OR, false for AND
		const bool decisive = node.conjunction_op == ExecutionExpressionConjunctionOp::OR;
		vector<sljit_jump *> decided;
		const auto last = node.children.size() - 1;
		for (idx_t child_idx = 0; child_idx < last; child_idx++) {
			auto child = EmitSljitTypedExpressionTreeFastJumpsInternal(compiler, *node.children[child_idx], decisive,
			                                                           spill_index, overflows, index_mode, data_hoists,
			                                                           null_rows);
			decided.insert(decided.end(), child.begin(), child.end());
		}
		// every child before the last left the outcome open: the last child's outcome is the conjunction's
		auto result = EmitSljitTypedExpressionTreeFastJumpsInternal(compiler, *node.children[last], when, spill_index,
		                                                            overflows, index_mode, data_hoists, null_rows);
		if (when == decisive) {
			result.insert(result.end(), decided.begin(), decided.end());
		} else {
			SetSljitJumpLabels(decided, sljit_emit_label(compiler));
		}
		return result;
	}
	if (!null_rows) {
		return EmitSljitTypedExpressionTreeFastLeafJumps(compiler, node, when, spill_index, overflows, index_mode,
		                                                 data_hoists);
	}
	// a null-strict leaf with a NULL source is NULL, which the AND and OR above it decide as FALSE
	vector<idx_t> sources;
	CollectSljitTypedExpressionTreeReferences(node, sources);
	auto emit_null_jumps = [&]() {
		vector<sljit_jump *> null_jumps;
		for (auto source_index : sources) {
			null_jumps.push_back(
			    EmitJumpIfSljitTypedExpressionTreeFastSourceNull(compiler, source_index, index_mode, data_hoists));
		}
		return null_jumps;
	};
	if (SljitTypedExpressionTreeEvaluatesAnyBits(node)) {
		// a NULL row's leftover value decides FALSE rightly, so only a row the leaf accepts tests its sources
		auto rejected = EmitSljitTypedExpressionTreeFastLeafJumps(compiler, node, false, spill_index, overflows,
		                                                          index_mode, data_hoists);
		auto null_jumps = emit_null_jumps();
		rejected.insert(rejected.end(), null_jumps.begin(), null_jumps.end());
		if (!when) {
			return rejected;
		}
		vector<sljit_jump *> accepted {sljit_emit_jump(compiler, SLJIT_JUMP)};
		SetSljitJumpLabels(rejected, sljit_emit_label(compiler));
		return accepted;
	}
	auto null_jumps = emit_null_jumps();
	auto result = EmitSljitTypedExpressionTreeFastLeafJumps(compiler, node, when, spill_index, overflows, index_mode,
	                                                        data_hoists);
	if (when) {
		SetSljitJumpLabels(null_jumps, sljit_emit_label(compiler));
	} else {
		result.insert(result.end(), null_jumps.begin(), null_jumps.end());
	}
	return result;
}

vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastJumps(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when,
                                      idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                      SljitTypedExpressionTreeFastIndexMode index_mode,
                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	return EmitSljitTypedExpressionTreeFastJumpsInternal(compiler, node, when, spill_index, overflows, index_mode,
	                                                     data_hoists, false);
}

vector<sljit_jump *> EmitSljitTypedExpressionTreeNullableFastJumps(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, bool when, idx_t &spill_index,
    vector<SljitArithmeticOverflowSite> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(SljitTypedExpressionTreeNullDecidesFalse(node));
	return EmitSljitTypedExpressionTreeFastJumpsInternal(compiler, node, when, spill_index, overflows, index_mode,
	                                                     data_hoists, true);
}

vector<sljit_jump *>
EmitSljitTypedExpressionTreeFastJumpsIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                            idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                            const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	return EmitSljitTypedExpressionTreeFastJumps(compiler, node, true, spill_index, overflows,
	                                             SljitTypedExpressionTreeFastIndexMode::FLAT, data_hoists);
}

void EmitSljitTypedExpressionTreeFastGuardReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                              SljitTypedExpressionTreeFastIndexMode index_mode,
                                              const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(SljitTypedExpressionTreeIsFastGuardedValue(node));
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[0], spill_index, overflows, index_mode,
	                                                 data_hoists);
}

void EmitSljitTypedExpressionTreeFastGuardedValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                     SljitTypedExpressionTreeFastIndexMode index_mode,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	D_ASSERT(SljitTypedExpressionTreeIsFastGuardedValue(node));
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[1], spill_index, overflows, index_mode,
	                                                 data_hoists);
}

void EmitSljitTypedExpressionTreeSelectedFastValueReg(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &node, idx_t &spill_index,
                                                      vector<SljitArithmeticOverflowSite> &overflows,
                                                      const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                      vector<sljit_jump *> *null_exits) {
	EmitSljitTypedExpressionTreeFastRootValueReg(compiler, node, spill_index, overflows,
	                                             SljitTypedExpressionTreeFastIndexMode::SELECTED, data_hoists,
	                                             null_exits);
}

void EmitSljitTypedExpressionTreeLogicalFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index, vector<SljitArithmeticOverflowSite> &overflows,
                                                     const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists,
                                                     vector<sljit_jump *> *null_exits) {
	EmitSljitTypedExpressionTreeFastRootValueReg(compiler, node, spill_index, overflows,
	                                             SljitTypedExpressionTreeFastIndexMode::LOGICAL, data_hoists,
	                                             null_exits);
}

} // namespace duckdb

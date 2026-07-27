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

namespace duckdb {

static bool SljitTypedExpressionTreeFastIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE;
}

static void EmitSljitTypedExpressionTreeFastValueRegInternal(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists);

static void
EmitSljitTypedExpressionTreeFastLeafReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                        const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::REFERENCE);
	auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
	EmitLoadSljitTypedExpressionTreeFastSourceIndex(compiler, node.ref_index, SLJIT_R1, index_mode);
	sljit_s32 data_reg;
	if (TryGetSljitTypedExpressionTreeDataPointerHoist(data_hoists, node.ref_index, data_reg)) {
		sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), target, 0, SLJIT_MEM2(data_reg, SLJIT_R1),
		               NativeIntegerDataScale(source_kind));
		return;
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	}
	sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1),
	               NativeIntegerDataScale(source_kind));
}

static void EmitSljitTypedExpressionTreeFastBinaryOpReg(struct sljit_compiler *compiler,
                                                        const ExecutionExpressionIR &node, sljit_s32 left_reg,
                                                        sljit_s32 right_reg, sljit_s32 target_reg,
                                                        vector<SljitExpressionTreeOverflowJumps> &overflows) {
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
		                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
		auto compare_true = sljit_emit_cmp(compiler, compare_type, left_reg, 0, right_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 0);
		auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(compare_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 1);
		sljit_set_label(compare_done, sljit_emit_label(compiler));
		return;
	}
	if (node.binary_op == ExecutionExpressionBinaryOp::MODULO ||
	    node.binary_op == ExecutionExpressionBinaryOp::INTEGER_DIVIDE) {
		// left_reg holds the dividend and is never R0/R1 here (the fast path only parks live values in R2-R4 and on
		// the stack), so the op0 magic-multiply reduction can borrow R0/R1 freely.
		const int64_t divisor = SljitTypedExpressionTreeConstantValue(*node.right);
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		if (node.binary_op == ExecutionExpressionBinaryOp::MODULO) {
			EmitSljitTypedExpressionTreeModulo(compiler, divisor, binary_kind, left_reg, target_reg);
		} else {
			EmitSljitTypedExpressionTreeIntegerDivide(compiler, divisor, binary_kind, left_reg, target_reg);
		}
		return;
	}
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
	}
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
	auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
	if (node.arithmetic_overflow_check) {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, target_reg, 0, left_reg, 0, right_reg, 0);
		AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	} else {
		sljit_emit_op2(compiler, binary_op, target_reg, 0, left_reg, 0, right_reg, 0);
	}
	AddSljitTypedExpressionTreeDecimal64RangeJumps(compiler, node, target_reg, overflows, native_op);
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, target_reg, 0, target_reg, 0);
	}
}

static bool SljitTypedExpressionTreeFastIsLeafBinary(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::BINARY && node.left && node.right &&
	       SljitTypedExpressionTreeFastIsLeaf(*node.left) && SljitTypedExpressionTreeFastIsLeaf(*node.right);
}

static void
EmitSljitTypedExpressionTreeFastLeafBinaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              sljit_s32 target, SljitTypedExpressionTreeFastIndexMode index_mode,
                                              vector<SljitExpressionTreeOverflowJumps> &overflows,
                                              const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(SljitTypedExpressionTreeFastIsLeafBinary(node));
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.left, target, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R3, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, target, SLJIT_R3, target, overflows);
}

static void
EmitSljitTypedExpressionTreeFastBinaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          SljitTypedExpressionTreeFastIndexMode index_mode,
                                          const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	{
		idx_t source_index;
		string constant;
		bool compare_equal;
		if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
			EmitSljitTypedExpressionTreeFastStringCompareReg(compiler, node, index_mode, data_hoists);
			return;
		}
	}
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
	                                                 data_hoists);
	if (SljitTypedExpressionTreeFastIsLeafBinary(*node.right)) {
		EmitSljitTypedExpressionTreeFastLeafBinaryReg(compiler, *node.right, SLJIT_R4, index_mode, overflows,
		                                              data_hoists);
		EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, SLJIT_R2, SLJIT_R4, SLJIT_R2, overflows);
		return;
	}
	if (SljitTypedExpressionTreeFastIsLeaf(*node.right)) {
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R4, index_mode, data_hoists);
		EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, SLJIT_R2, SLJIT_R4, SLJIT_R2, overflows);
		return;
	}

	auto left_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), left_offset, SLJIT_R2, 0);
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.right, spill_index, overflows, index_mode,
	                                                 data_hoists);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), left_offset);
	EmitSljitTypedExpressionTreeFastBinaryOpReg(compiler, node, SLJIT_R4, SLJIT_R2, SLJIT_R2, overflows);
}

static void
EmitSljitTypedExpressionTreeFastUnaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                         idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                         SljitTypedExpressionTreeFastIndexMode index_mode,
                                         const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
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
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast unary operator");
	}
}

static void
EmitSljitTypedExpressionTreeFastCastReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                        SljitTypedExpressionTreeFastIndexMode index_mode,
                                        const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(node.left);
	D_ASSERT(SljitTypedExpressionTreeValueCastSupported(node));
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.left, spill_index, overflows, index_mode,
	                                                 data_hoists);
	if (SljitTypedExpressionTreeIsInt32Node(*node.left)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void
EmitSljitTypedExpressionTreeFastConjunctionReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                               idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                               SljitTypedExpressionTreeFastIndexMode index_mode,
                                               const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
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

static void
EmitSljitTypedExpressionTreeFastCoalesceReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                            idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                            SljitTypedExpressionTreeFastIndexMode index_mode,
                                            const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(!node.children.empty());
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[0], spill_index, overflows, index_mode,
	                                                 data_hoists);
}

static void
EmitSljitTypedExpressionTreeFastCaseReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                        SljitTypedExpressionTreeFastIndexMode index_mode,
                                        const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
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

static void EmitSljitTypedExpressionTreeFastIntegralCompressReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	D_ASSERT(node.children.size() == 2);
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, *node.children[0], spill_index, overflows, index_mode,
	                                                 data_hoists);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*node.children[1])));
}

static void EmitSljitTypedExpressionTreeFastValueRegInternal(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
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

void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                              const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows,
	                                                 SljitTypedExpressionTreeFastIndexMode::FLAT, data_hoists);
}

void EmitSljitTypedExpressionTreeSelectedFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows,
	                                                 SljitTypedExpressionTreeFastIndexMode::SELECTED, data_hoists);
}

void EmitSljitTypedExpressionTreeLogicalFastValueReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &spill_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	EmitSljitTypedExpressionTreeFastValueRegInternal(compiler, node, spill_index, overflows,
	                                                 SljitTypedExpressionTreeFastIndexMode::LOGICAL, data_hoists);
}

} // namespace duckdb

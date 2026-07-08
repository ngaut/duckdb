//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

static void EmitSljitTypedExpressionTreeConstant(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, node.constant.IsNull() ? 0 : 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static void EmitSljitTypedExpressionTreeReference(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                  const SljitTypedExpressionTreeSlot &slot,
                                                  const vector<idx_t> *known_valid_sources) {
	EmitLoadSljitExpressionTreeSourceIndex(compiler, node.ref_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	if (!SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.ref_index)) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
		               NumericCast<sljit_sw>(node.ref_index * sizeof(const validity_t *)));
		auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_R0, 0);
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R2), 3);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
		sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R0, 0);
		auto source_valid = sljit_emit_jump(compiler, SLJIT_NOT_EQUAL);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
		auto validity_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(source_valid, sljit_emit_label(compiler));
		sljit_set_label(source_all_valid, sljit_emit_label(compiler));
		sljit_set_label(validity_done, sljit_emit_label(compiler));
	}

	auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1),
	               NativeIntegerDataScale(source_kind));
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static void EmitSljitTypedExpressionTreeCast(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                             const SljitTypedExpressionTreeSlot &slot,
                                             const vector<idx_t> *known_valid_sources) {
	D_ASSERT(SljitTypedExpressionTreeValueCastSupported(node));
	auto &source = *node.left;
	auto source_slot = EmitSljitTypedExpressionTreeValue(compiler, source, slot_index, overflows, known_valid_sources);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source_slot.value_offset);
	if (SljitTypedExpressionTreeIsInt32Node(source)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source_slot.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeDateYear(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                     idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                     const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.children.size() == 1);
	auto source_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.children[0], slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source_slot.valid_offset);
	auto source_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source_slot.value_offset);
	auto positive_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM,
	                                        NumericCast<sljit_sw>(date_t::infinity().days));
	auto negative_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM,
	                                        NumericCast<sljit_sw>(date_t::ninfinity().days));
	EmitSljitDateYearFromDays(compiler, SLJIT_R2, SLJIT_R2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_invalid, invalid_label);
	sljit_set_label(positive_infinity, invalid_label);
	sljit_set_label(negative_infinity, invalid_label);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
	return result_slot;
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeIntegralCompress(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                             const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.children.size() == 2);
	auto source_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.children[0], slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source_slot.valid_offset);
	auto source_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source_slot.value_offset);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(*node.children[1])));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(source_invalid, sljit_emit_label(compiler));
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
	return result_slot;
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeBinary(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                   idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                   const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	{
		idx_t source_index;
		string constant;
		bool compare_equal;
		if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
			return EmitSljitTypedExpressionTreeStringCompare(compiler, node, slot_index);
		}
	}
	auto left_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto right_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.right, slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), left_slot.valid_offset);
	auto invalid_left = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), right_slot.valid_offset);
	auto invalid_right = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), left_slot.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), right_slot.value_offset);
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
		                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
		auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(compare_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_set_label(compare_done, sljit_emit_label(compiler));
	} else {
		SljitNativeIntegerBinaryOp native_op;
		if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
			throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
		}
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
		if (node.arithmetic_overflow_check) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
			AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		} else {
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
		}
		AddSljitTypedExpressionTreeDecimal64RangeJumps(compiler, node, SLJIT_R2, overflows, native_op);
		if (binary_kind == SljitNativeIntegerKind::INT32) {
			sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(invalid_left, invalid_label);
	sljit_set_label(invalid_right, invalid_label);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
	return result_slot;
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeUnary(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                  const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	auto child_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	switch (node.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		if (node.unary_op == ExecutionExpressionUnaryOp::IS_NULL) {
			auto is_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(is_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		} else {
			auto is_not_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(is_not_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		return result_slot;
	case ExecutionExpressionUnaryOp::NOT: {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.value_offset);
		auto child_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_false, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		auto done_from_false = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_invalid, sljit_emit_label(compiler));
		EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
		auto done_label = sljit_emit_label(compiler);
		sljit_set_label(done, done_label);
		sljit_set_label(done_from_false, done_label);
		return result_slot;
	}
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree unary operator");
	}
}

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeValue(struct sljit_compiler *compiler,
                                                               const ExecutionExpressionIR &node, idx_t &slot_index,
                                                               vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                               const vector<idx_t> *known_valid_sources) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeConstant(compiler, node, slot);
		return slot;
	}
	case ExecutionExpressionIRKind::REFERENCE: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeReference(compiler, node, slot, known_valid_sources);
		return slot;
	}
	case ExecutionExpressionIRKind::CAST: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeCast(compiler, node, slot_index, overflows, slot, known_valid_sources);
		return slot;
	}
	case ExecutionExpressionIRKind::UNARY:
		return EmitSljitTypedExpressionTreeUnary(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::BINARY:
		return EmitSljitTypedExpressionTreeBinary(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::CONJUNCTION:
		return EmitSljitTypedExpressionTreeConjunction(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::COALESCE:
		return EmitSljitTypedExpressionTreeCoalesce(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::CASE:
		return EmitSljitTypedExpressionTreeCase(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::INTRINSIC:
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::DATE_YEAR) {
			return EmitSljitTypedExpressionTreeDateYear(compiler, node, slot_index, overflows, known_valid_sources);
		}
		if (node.intrinsic == ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS) {
			return EmitSljitTypedExpressionTreeIntegralCompress(compiler, node, slot_index, overflows,
			                                                   known_valid_sources);
		}
		return EmitSljitTypedExpressionTreeStringPrefix(compiler, node, slot_index);
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree node kind");
	}
}

} // namespace duckdb

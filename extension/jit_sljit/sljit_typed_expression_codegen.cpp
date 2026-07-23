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

struct SljitSignedDivisionMagic {
	int64_t multiplier;
	int shift;
};

// Signed magic numbers for 64-bit division by a constant d >= 2 (Hacker's Delight). Together with a signed high
// multiply, an arithmetic shift, and a sign-bit correction this reproduces the exact truncated-toward-zero quotient
// that `n / d` yields for every 64-bit dividend, without emitting a hardware divide.
static SljitSignedDivisionMagic ComputeSljitSignedDivisionMagic(int64_t d) {
	const uint64_t two63 = 0x8000000000000000ULL;
	const uint64_t ad = static_cast<uint64_t>(d);
	const uint64_t t = two63;
	const uint64_t anc = t - 1 - t % ad;
	int p = 63;
	uint64_t q1 = two63 / anc;
	uint64_t r1 = two63 - q1 * anc;
	uint64_t q2 = two63 / ad;
	uint64_t r2 = two63 - q2 * ad;
	uint64_t delta;
	do {
		p++;
		q1 *= 2;
		r1 *= 2;
		if (r1 >= anc) {
			q1++;
			r1 -= anc;
		}
		q2 *= 2;
		r2 *= 2;
		if (r2 >= ad) {
			q2++;
			r2 -= ad;
		}
		delta = ad - r2;
	} while (q1 < delta || (q1 == delta && r1 == 0));
	SljitSignedDivisionMagic magic;
	magic.multiplier = static_cast<int64_t>(q2 + 1);
	magic.shift = p - 64;
	return magic;
}

// Computes the truncated-toward-zero quotient n_reg / divisor (a compile-time constant >= 2) into SLJIT_R1 with signed
// magic-multiply strength reduction. R0/R1 are op0 scratch and must be dead here; n_reg must not be R0/R1 and is only
// read.
static void EmitSljitSignedConstantDivideQuotient(struct sljit_compiler *compiler, int64_t divisor, sljit_s32 n_reg) {
	auto magic = ComputeSljitSignedDivisionMagic(divisor);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, n_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, NumericCast<sljit_sw>(magic.multiplier));
	sljit_emit_op0(compiler, SLJIT_LMUL_SW); // R1:R0 = signed 128-bit product; R1 = high(n * multiplier)
	if (magic.multiplier < 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, n_reg, 0);
	}
	if (magic.shift > 0) {
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, magic.shift);
	}
	// q += (unsigned)q >> 63 rounds the quotient toward zero for negative dividends. R1 now holds the quotient.
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R0, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R0, 0);
}

// Emits result_reg = n_reg % divisor. n_reg is only read until the final store, so result_reg may alias n_reg.
void EmitSljitTypedExpressionTreeModulo(struct sljit_compiler *compiler, int64_t divisor,
                                        SljitNativeIntegerKind binary_kind, sljit_s32 n_reg, sljit_s32 result_reg) {
	EmitSljitSignedConstantDivideQuotient(compiler, divisor, n_reg);
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R0, 0, SLJIT_R1, 0, SLJIT_IMM, NumericCast<sljit_sw>(divisor));
	sljit_emit_op2(compiler, SLJIT_SUB, result_reg, 0, n_reg, 0, SLJIT_R0, 0);
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, result_reg, 0, result_reg, 0);
	}
}

// Emits result_reg = n_reg / divisor (truncating), matching DuckDB integer division ("//") semantics.
void EmitSljitTypedExpressionTreeIntegerDivide(struct sljit_compiler *compiler, int64_t divisor,
                                               SljitNativeIntegerKind binary_kind, sljit_s32 n_reg,
                                               sljit_s32 result_reg) {
	EmitSljitSignedConstantDivideQuotient(compiler, divisor, n_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, result_reg, 0, SLJIT_R1, 0);
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, result_reg, 0, result_reg, 0);
	}
}

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
		EmitLoadSljitNativeSourceValidity(compiler, node.ref_index, SLJIT_R0);
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
	auto positive_infinity =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::infinity().days));
	auto negative_infinity =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::ninfinity().days));
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
	} else if (node.binary_op == ExecutionExpressionBinaryOp::MODULO ||
	           node.binary_op == ExecutionExpressionBinaryOp::INTEGER_DIVIDE) {
		// The plan admits these only for a constant divisor >= 2. R2 holds the dividend on entry and the result on
		// exit; R0/R1 are dead op0 scratch here and R2-R4 survive SLJIT_LMUL_SW.
		const int64_t divisor = SljitTypedExpressionTreeConstantValue(*node.right);
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		if (node.binary_op == ExecutionExpressionBinaryOp::MODULO) {
			EmitSljitTypedExpressionTreeModulo(compiler, divisor, binary_kind, SLJIT_R2, SLJIT_R2);
		} else {
			EmitSljitTypedExpressionTreeIntegerDivide(compiler, divisor, binary_kind, SLJIT_R2, SLJIT_R2);
		}
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

#include <cstring>

namespace duckdb {

static SljitTypedExpressionTreeSlot AllocateSljitTypedExpressionTreeSlot(idx_t &slot_index) {
	const auto value_offset = NumericCast<sljit_sw>(slot_index++ * sizeof(sljit_sw) * 2);
	return SljitTypedExpressionTreeSlot {value_offset, value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))};
}

static void EmitStoreSljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                  const SljitTypedExpressionTreeSlot &slot, sljit_s32 value_reg,
                                                  sljit_s32 valid_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.value_offset, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.valid_offset, valid_reg, 0);
}

static int64_t SljitTypedExpressionTreeConstantValue(const ExecutionExpressionIR &node) {
	if (node.constant.IsNull()) {
		return 0;
	}
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return node.constant.GetValueUnsafe<bool>() ? 1 : 0;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return node.constant.GetValueUnsafe<int32_t>();
	}
	return node.constant.GetValueUnsafe<int64_t>();
}

static void AddSljitTypedExpressionTreeDecimal64RangeJumps(struct sljit_compiler *compiler,
                                                           const ExecutionExpressionIR &node, sljit_s32 target,
                                                           vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                           SljitNativeIntegerBinaryOp native_op) {
	if (!node.arithmetic_overflow_check || !SljitTypedExpressionTreeIsDecimal64Node(node)) {
		return;
	}
	int64_t result_min;
	int64_t result_max;
	if (!TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		throw InternalException("SLJIT typed expression-tree binary node missing decimal64 result range");
	}
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min)));
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max)));
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

static void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label);
static void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                                      const SljitTypedExpressionTreeSlot &slot);

static idx_t SljitTypedStringCompareChunkSize(idx_t remaining) {
	if (remaining >= sizeof(sljit_sw)) {
		return sizeof(sljit_sw);
	}
	if (remaining >= sizeof(uint32_t)) {
		return sizeof(uint32_t);
	}
	if (remaining >= sizeof(uint16_t)) {
		return sizeof(uint16_t);
	}
	return sizeof(uint8_t);
}

static sljit_s32 SljitTypedStringChunkLoadOp(idx_t chunk_size) {
	if (chunk_size == sizeof(uint8_t)) {
		return SLJIT_MOV_U8;
	}
	if (chunk_size == sizeof(uint16_t)) {
		return SLJIT_MOV_U16;
	}
	if (chunk_size == sizeof(uint32_t)) {
		return SLJIT_MOV_U32;
	}
	if (chunk_size == sizeof(sljit_sw)) {
		return SLJIT_MOV;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree string comparison width");
}

static sljit_sw SljitTypedStringChunkImmediate(const string &constant, idx_t constant_offset, idx_t chunk_size) {
	uint64_t value = 0;
	memcpy(&value, constant.data() + constant_offset, chunk_size);
	if (chunk_size == sizeof(sljit_sw)) {
		return static_cast<sljit_sw>(value);
	}
	return NumericCast<sljit_sw>(value);
}

static void EmitLoadSljitTypedExpressionTreeStringForPrefix(struct sljit_compiler *compiler, idx_t source_index,
                                                            vector<sljit_jump *> &null_jumps) {
	static_assert(sizeof(string_t) == 16, "SLJIT typed expression-tree string prefix expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_R1, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);

	auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitLoadSljitTypedExpressionTreeFastStringForPrefix(struct sljit_compiler *compiler, idx_t source_index) {
	static_assert(sizeof(string_t) == 16, "SLJIT typed expression-tree string prefix expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);

	auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitSljitTypedExpressionTreeStringPrefixFalseBranches(struct sljit_compiler *compiler,
                                                                  const string &prefix,
                                                                  vector<sljit_jump *> &false_jumps) {
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size())));
	if (prefix.empty()) {
		return;
	}
	for (idx_t byte_idx = 0; byte_idx < prefix.size();) {
		const auto chunk_size = SljitTypedStringCompareChunkSize(prefix.size() - byte_idx);
		sljit_emit_mem(compiler, SljitTypedStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(SLJIT_R4), NumericCast<sljit_sw>(byte_idx));
		false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                   SljitTypedStringChunkImmediate(prefix, byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeStringPrefix(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                         idx_t &slot_index) {
	idx_t source_index;
	string prefix;
	if (!TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix)) {
		throw InternalException("Unsupported SLJIT typed expression-tree intrinsic");
	}

	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> null_jumps;
	vector<sljit_jump *> false_jumps;
	EmitLoadSljitTypedExpressionTreeStringForPrefix(compiler, source_index, null_jumps);
	EmitSljitTypedExpressionTreeStringPrefixFalseBranches(compiler, prefix, false_jumps);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done_from_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto false_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(false_jumps, false_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done_from_false = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto null_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_jumps, null_label);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done_from_true, done_label);
	sljit_set_label(done_from_false, done_label);
	return result_slot;
}

static void EmitSljitTypedExpressionTreeFastStringPrefixReg(struct sljit_compiler *compiler,
                                                            const ExecutionExpressionIR &node) {
	idx_t source_index;
	string prefix;
	if (!TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix)) {
		throw InternalException("Unsupported SLJIT typed expression-tree fast string predicate");
	}

	vector<sljit_jump *> false_jumps;
	EmitLoadSljitTypedExpressionTreeFastStringForPrefix(compiler, source_index);
	EmitSljitTypedExpressionTreeStringPrefixFalseBranches(compiler, prefix, false_jumps);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
	auto done_from_true = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto false_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(false_jumps, false_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_set_label(done_from_true, sljit_emit_label(compiler));
}

static void EmitSljitTypedExpressionTreeCast(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                             const SljitTypedExpressionTreeSlot &slot,
                                             const vector<idx_t> *known_valid_sources) {
	D_ASSERT(SljitTypedExpressionTreeInt64CastSupported(node));
	auto &source = *node.left;
	auto source_slot = EmitSljitTypedExpressionTreeValue(compiler, source, slot_index, overflows, known_valid_sources);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source_slot.value_offset);
	if (SljitTypedExpressionTreeIsInt32Node(source)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source_slot.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfTrue(struct sljit_compiler *compiler,
                                                                   const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                   vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                                   const vector<idx_t> *known_valid_sources = nullptr);

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          const vector<idx_t> *known_valid_sources = nullptr);

static void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                 const SljitTypedExpressionTreeSlot &source,
                                                 const SljitTypedExpressionTreeSlot &target);

static void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                                      const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeStringPrefixJumpIfTrue(struct sljit_compiler *compiler,
                                                                               const ExecutionExpressionIR &node) {
	idx_t source_index;
	string prefix;
	if (!TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix)) {
		throw InternalException("Unsupported SLJIT typed expression-tree string predicate");
	}

	vector<sljit_jump *> null_or_false_jumps;
	EmitLoadSljitTypedExpressionTreeStringForPrefix(compiler, source_index, null_or_false_jumps);
	EmitSljitTypedExpressionTreeStringPrefixFalseBranches(compiler, prefix, null_or_false_jumps);
	auto result_true = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_or_false_jumps, not_true_label);
	return vector<sljit_jump *> {result_true};
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeBinary(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                   idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                   const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
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

static vector<sljit_jump *> EmitSljitTypedExpressionTreeSlotJumpIfTrue(struct sljit_compiler *compiler,
                                                                       const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	auto invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	auto result_true = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_set_label(invalid, sljit_emit_label(compiler));
	return vector<sljit_jump *> {result_true};
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(struct sljit_compiler *compiler,
                                                                          const SljitTypedExpressionTreeSlot &slot) {
	vector<sljit_jump *> result;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
	return result;
}

static vector<sljit_jump *> EmitSljitTypedReferenceJumpIfNull(struct sljit_compiler *compiler, idx_t source_index) {
	return vector<sljit_jump *> {EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index)};
}

static vector<sljit_jump *> EmitSljitTypedReferenceJumpIfNotNull(struct sljit_compiler *compiler, idx_t source_index) {
	auto null_jumps = EmitSljitTypedReferenceJumpIfNull(compiler, source_index);
	auto not_null = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto null_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_jumps, null_label);
	return vector<sljit_jump *> {not_null};
}

static bool SljitTypedExpressionTreeIsBoolConstantTrue(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && !node.constant.IsNull() &&
	       SljitTypedExpressionTreeIsBoolNode(node) && node.constant.GetValueUnsafe<bool>();
}

static bool SljitTypedExpressionTreeIsBoolConstantNotTrue(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && SljitTypedExpressionTreeIsBoolNode(node) &&
	       (node.constant.IsNull() || !node.constant.GetValueUnsafe<bool>());
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeComparisonJumpIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                 const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	auto left_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto right_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.right, slot_index, overflows, known_valid_sources);
	vector<sljit_jump *> invalid_jumps;

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), left_slot.valid_offset);
	invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), right_slot.valid_offset);
	invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), left_slot.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), right_slot.value_offset);
	auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
	                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
	auto result_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(invalid_jumps, not_true_label);
	return vector<sljit_jump *> {result_true};
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeComparisonJumpIfNotTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	auto true_jumps =
	    EmitSljitTypedExpressionTreeComparisonJumpIfTrue(compiler, node, slot_index, overflows, known_valid_sources);
	auto not_true = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(true_jumps, true_label);
	return vector<sljit_jump *> {not_true};
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeUnaryJumpIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                            idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                            const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	if (node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::IS_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *>();
			}
			return EmitSljitTypedReferenceJumpIfNull(compiler, node.left->ref_index);
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
			}
			return EmitSljitTypedReferenceJumpIfNotNull(compiler, node.left->ref_index);
		default:
			break;
		}
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfTrue(compiler, slot);
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeUnaryJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                               idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                               const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	if (node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::IS_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
			}
			return EmitSljitTypedReferenceJumpIfNotNull(compiler, node.left->ref_index);
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *>();
			}
			return EmitSljitTypedReferenceJumpIfNull(compiler, node.left->ref_index);
		default:
			break;
		}
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(compiler, slot);
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeConjunctionJumpIfTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	vector<sljit_jump *> result;
	if (node.conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		for (auto &child : node.children) {
			auto child_true =
			    EmitSljitTypedExpressionTreeJumpIfTrue(compiler, *child, slot_index, overflows, known_valid_sources);
			result.insert(result.end(), child_true.begin(), child_true.end());
		}
		return result;
	}

	vector<sljit_jump *> not_true_jumps;
	for (auto &child : node.children) {
		auto child_not_true =
		    EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *child, slot_index, overflows, known_valid_sources);
		not_true_jumps.insert(not_true_jumps.end(), child_not_true.begin(), child_not_true.end());
	}
	result.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(not_true_jumps, not_true_label);
	return result;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeConjunctionJumpIfNotTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	vector<sljit_jump *> result;
	if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			auto child_not_true =
			    EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *child, slot_index, overflows, known_valid_sources);
			result.insert(result.end(), child_not_true.begin(), child_not_true.end());
		}
		return result;
	}

	vector<sljit_jump *> true_jumps;
	for (auto &child : node.children) {
		auto child_true =
		    EmitSljitTypedExpressionTreeJumpIfTrue(compiler, *child, slot_index, overflows, known_valid_sources);
		true_jumps.insert(true_jumps.end(), child_true.begin(), child_true.end());
	}
	result.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(true_jumps, true_label);
	return result;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfTrue(struct sljit_compiler *compiler,
                                                                   const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                   vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                                   const vector<idx_t> *known_valid_sources) {
	if (SljitTypedExpressionTreeIsBoolConstantTrue(node)) {
		return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
	}
	if (SljitTypedExpressionTreeIsBoolConstantNotTrue(node)) {
		return vector<sljit_jump *>();
	}
	if (node.kind == ExecutionExpressionIRKind::INTRINSIC) {
		return EmitSljitTypedExpressionTreeStringPrefixJumpIfTrue(compiler, node);
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		return EmitSljitTypedExpressionTreeUnaryJumpIfTrue(compiler, node, slot_index, overflows, known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		return EmitSljitTypedExpressionTreeComparisonJumpIfTrue(compiler, node, slot_index, overflows,
		                                                        known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return EmitSljitTypedExpressionTreeConjunctionJumpIfTrue(compiler, node, slot_index, overflows,
		                                                         known_valid_sources);
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfTrue(compiler, slot);
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          const vector<idx_t> *known_valid_sources) {
	if (SljitTypedExpressionTreeIsBoolConstantTrue(node)) {
		return vector<sljit_jump *>();
	}
	if (SljitTypedExpressionTreeIsBoolConstantNotTrue(node)) {
		return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		return EmitSljitTypedExpressionTreeUnaryJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                      known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		return EmitSljitTypedExpressionTreeComparisonJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                           known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return EmitSljitTypedExpressionTreeConjunctionJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                            known_valid_sources);
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(compiler, slot);
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

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeConjunction(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                        const vector<idx_t> *known_valid_sources) {
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset, SLJIT_IMM, 0);
	vector<sljit_jump *> decisive_jumps;
	for (auto &child : node.children) {
		auto child_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *child, slot_index, overflows, known_valid_sources);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_valid = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset, SLJIT_IMM, 1);
		auto next_child = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_valid, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.value_offset);
		if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		} else {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		}
		sljit_set_label(next_child, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset);
	auto no_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	auto done_from_null = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 1 : 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done_from_default = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto decisive_label = sljit_emit_label(compiler);
	for (auto jump : decisive_jumps) {
		sljit_set_label(jump, decisive_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 0 : 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done_from_null, done_label);
	sljit_set_label(done_from_default, done_label);
	return result_slot;
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeCoalesce(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                     idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                     const vector<idx_t> *known_valid_sources) {
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> done_jumps;
	for (auto &child : node.children) {
		auto child_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *child, slot_index, overflows, known_valid_sources);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		EmitCopySljitTypedExpressionTreeSlot(compiler, child_slot, result_slot);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(child_invalid, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
	return result_slot;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfAnySourceMayHaveNull(
    struct sljit_compiler *compiler, const vector<idx_t> &source_indices, const vector<idx_t> &known_valid_sources) {
	vector<sljit_jump *> result;
	for (auto source_index : source_indices) {
		if (SljitTypedExpressionTreeSourceKnownValid(&known_valid_sources, source_index)) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
		               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
		result.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	}
	return result;
}

static void EmitSljitTypedExpressionTreeCaseValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &value,
                                                  const SljitTypedExpressionTreeSlot &result_slot, idx_t &slot_index,
                                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                  const vector<idx_t> &known_valid_sources) {
	if (!SljitTypedExpressionTreeFastPathSupported(value)) {
		auto value_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, value, slot_index, overflows, &known_valid_sources);
		EmitCopySljitTypedExpressionTreeSlot(compiler, value_slot, result_slot);
		return;
	}

	vector<idx_t> source_indices;
	CollectSljitTypedExpressionTreeReferences(value, source_indices);
	vector<sljit_jump *> use_generic;
	if (!source_indices.empty()) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		use_generic.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	}
	auto nullable_jumps =
	    EmitSljitTypedExpressionTreeJumpIfAnySourceMayHaveNull(compiler, source_indices, known_valid_sources);
	use_generic.insert(use_generic.end(), nullable_jumps.begin(), nullable_jumps.end());
	idx_t fast_spill_index = slot_index * 2;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, value, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	if (use_generic.empty()) {
		return;
	}

	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto generic_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(use_generic, generic_label);
	auto value_slot = EmitSljitTypedExpressionTreeValue(compiler, value, slot_index, overflows, &known_valid_sources);
	EmitCopySljitTypedExpressionTreeSlot(compiler, value_slot, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeCase(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                 vector<SljitExpressionTreeOverflowJumps> &overflows,
                                 const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> done_jumps;
	vector<idx_t> fallthrough_known_valid;
	if (known_valid_sources) {
		fallthrough_known_valid = *known_valid_sources;
	}

	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = node.children[child_idx];
		auto &value = node.children[child_idx + 1];
		auto branch_known_valid = fallthrough_known_valid;
		CollectSljitTypedExpressionTreeTrueFacts(*condition, branch_known_valid);
		auto condition_not_true = EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *condition, slot_index, overflows,
		                                                                    &fallthrough_known_valid);
		EmitSljitTypedExpressionTreeCaseValue(compiler, *value, result_slot, slot_index, overflows, branch_known_valid);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		auto next_condition = sljit_emit_label(compiler);
		SetSljitJumpLabels(condition_not_true, next_condition);
		CollectSljitTypedExpressionTreeNotTrueFacts(*condition, fallthrough_known_valid);
	}

	EmitSljitTypedExpressionTreeCaseValue(compiler, *node.else_node, result_slot, slot_index, overflows,
	                                      fallthrough_known_valid);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
	return result_slot;
}

static void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                 const SljitTypedExpressionTreeSlot &source,
                                                 const SljitTypedExpressionTreeSlot &target) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, target, SLJIT_R2, SLJIT_R3);
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
		return EmitSljitTypedExpressionTreeStringPrefix(compiler, node, slot_index);
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree node kind");
	}
}

static bool SljitTypedExpressionTreeFastIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE;
}

static void EmitSljitTypedExpressionTreeFastLeafReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    sljit_s32 target) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::REFERENCE);
	auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(source_kind));
}

static void EmitSljitTypedExpressionTreeFastBinaryReg(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &node, idx_t &spill_index,
                                                      vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
	if (SljitTypedExpressionTreeFastIsLeaf(*node.right)) {
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R4);
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
			                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
			auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(compare_true, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(compare_done, sljit_emit_label(compiler));
			return;
		}
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
		return;
	}

	auto left_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), left_offset, SLJIT_R2, 0);
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.right, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), left_offset);
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
		                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
		auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R4, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(compare_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_set_label(compare_done, sljit_emit_label(compiler));
		return;
	}
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
	}
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
	auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
	if (node.arithmetic_overflow_check) {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
		AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	} else {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	}
	AddSljitTypedExpressionTreeDecimal64RangeJumps(compiler, node, SLJIT_R2, overflows, native_op);
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void EmitSljitTypedExpressionTreeFastUnaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index,
                                                     vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	switch (node.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		return;
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		return;
	case ExecutionExpressionUnaryOp::NOT:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
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

static void EmitSljitTypedExpressionTreeFastCastReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index,
                                                    vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	D_ASSERT(SljitTypedExpressionTreeInt64CastSupported(node));
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
	if (SljitTypedExpressionTreeIsInt32Node(*node.left)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void EmitSljitTypedExpressionTreeFastConjunctionReg(struct sljit_compiler *compiler,
                                                           const ExecutionExpressionIR &node, idx_t &spill_index,
                                                           vector<SljitExpressionTreeOverflowJumps> &overflows) {
	vector<sljit_jump *> decisive_jumps;
	for (auto &child : node.children) {
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *child, spill_index, overflows);
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
                                                        vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(!node.children.empty());
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.children[0], spill_index, overflows);
}

static void EmitSljitTypedExpressionTreeFastCaseReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index,
                                                    vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	vector<sljit_jump *> done_jumps;
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = node.children[child_idx];
		auto &value = node.children[child_idx + 1];
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *condition, spill_index, overflows);
		auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *value, spill_index, overflows);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(condition_false, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.else_node, spill_index, overflows);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
}

void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                              idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
	case ExecutionExpressionIRKind::REFERENCE:
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, node, SLJIT_R2);
		return;
	case ExecutionExpressionIRKind::CAST:
		EmitSljitTypedExpressionTreeFastCastReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::UNARY:
		EmitSljitTypedExpressionTreeFastUnaryReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::BINARY:
		EmitSljitTypedExpressionTreeFastBinaryReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::CONJUNCTION:
		EmitSljitTypedExpressionTreeFastConjunctionReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::COALESCE:
		EmitSljitTypedExpressionTreeFastCoalesceReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::CASE:
		EmitSljitTypedExpressionTreeFastCaseReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::INTRINSIC:
		EmitSljitTypedExpressionTreeFastStringPrefixReg(compiler, node);
		return;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast node kind");
	}
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
	auto local_size = NumericCast<sljit_sw>(tree_plan.node_count * sizeof(sljit_sw) * 3);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	struct sljit_jump *use_slow_loop = nullptr;
	if (fast_path.fast_path_supported) {
		use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	} else {
		use_slow_loop = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	idx_t fast_spill_index = 0;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
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
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
		               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
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
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), root_slot.value_offset);
	sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	EmitSetResultRowInvalid(compiler);
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
	sljit_set_label(fast_done, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static void EmitStoreTypedExpressionTreeTrueSelection(struct sljit_compiler *compiler, sljit_s32 index_reg) {
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
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	struct sljit_jump *use_slow_loop = nullptr;
	if (fast_path.fast_path_supported) {
		use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	} else {
		use_slow_loop = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	idx_t fast_spill_index = 0;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
	auto fast_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitStoreTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
	sljit_set_label(fast_false, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
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
		EmitStoreTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
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
	EmitStoreTypedExpressionTreeTrueSelection(compiler, SLJIT_S3);
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
	sljit_set_label(fast_done, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumTypedExpressionTree(
    const ExecutionExpressionIR &root, SljitNativeAggregateUpdateFunction &function, string &error,
    SljitNativeAggregateSumStateKind state_kind, bool emit_flat_nullable_fast_path) {
	const auto tree_plan = BuildSljitTypedExpressionTreePlan(root, emit_flat_nullable_fast_path);
	if (!tree_plan.supported || !tree_plan.result_is_int64) {
		error = "SLJIT aggregate typed expression-tree reducer only supports INT64/DECIMAL64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(tree_plan.node_count * sizeof(sljit_sw) * 3);
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = tree_local_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	vector<SljitExpressionTreeOverflowJumps> overflows;
	const auto &fast_path = tree_plan.fast_path;
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	struct sljit_jump *use_slow_loop = nullptr;
	if (fast_path.fast_path_supported) {
		use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	} else {
		use_slow_loop = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	idx_t fast_spill_index = 0;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	EmitSljitAggregateLoopStep(compiler, fast_loop);

	sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
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
		if (hugeint_state) {
			EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset,
			                                         saw_value_offset, SLJIT_R2);
		} else {
			EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
		}
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
		sljit_set_label(flat_nullable_next, sljit_emit_label(compiler));
		EmitSljitAggregateLoopStep(compiler, flat_nullable_loop);
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
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	sljit_set_label(row_done, sljit_emit_label(compiler));
	EmitSljitAggregateLoopStep(compiler, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(fast_done, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	if (hugeint_state) {
		EmitSljitAggregateCommitHugeint(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset);
	} else {
		EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64TypedExpressionTree(const ExecutionExpressionIR &root,
                                                     SljitNativeAggregateUpdateFunction &function, string &error,
                                                     bool emit_flat_nullable_fast_path) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(
	    root, function, error, SljitNativeAggregateSumStateKind::INT64, emit_flat_nullable_fast_path);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(const ExecutionExpressionIR &root,
                                                       SljitNativeAggregateUpdateFunction &function, string &error,
                                                       bool emit_flat_nullable_fast_path) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(
	    root, function, error, SljitNativeAggregateSumStateKind::HUGEINT, emit_flat_nullable_fast_path);
}

} // namespace duckdb

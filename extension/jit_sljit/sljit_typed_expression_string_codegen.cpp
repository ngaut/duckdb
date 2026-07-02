//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_string_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_string_chunk_codegen.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

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

	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitLoadSljitTypedExpressionTreeFastStringForPrefix(
    struct sljit_compiler *compiler, idx_t source_index, SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	static_assert(sizeof(string_t) == 16, "SLJIT typed expression-tree string prefix expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;

	EmitLoadSljitTypedExpressionTreeFastSourceIndex(compiler, source_index, SLJIT_R1, index_mode);
	sljit_s32 data_reg;
	if (TryGetSljitTypedExpressionTreeDataPointerHoist(data_hoists, source_index, data_reg)) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, data_reg, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
		               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	}
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_R1, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
}

static void EmitSljitTypedExpressionTreeFastStringBytesFalseBranches(struct sljit_compiler *compiler,
                                                                     const string &constant,
                                                                     vector<sljit_jump *> &false_jumps);

static void EmitSljitTypedExpressionTreeFastStringPrefixFalseBranches(struct sljit_compiler *compiler,
                                                                      const string &prefix,
                                                                      vector<sljit_jump *> &false_jumps) {
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size())));
	EmitSljitTypedExpressionTreeFastStringBytesFalseBranches(compiler, prefix, false_jumps);
}

static void EmitSljitTypedExpressionTreeFastStringBytesFalseBranches(struct sljit_compiler *compiler,
                                                                     const string &constant,
                                                                     vector<sljit_jump *> &false_jumps) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	if (constant.empty()) {
		return;
	}

	const auto embedded_prefix_bytes =
	    constant.size() < string_t::PREFIX_BYTES ? constant.size() : idx_t(string_t::PREFIX_BYTES);
	for (idx_t byte_idx = 0; byte_idx < embedded_prefix_bytes;) {
		const auto chunk_size = SljitStringCompareChunkSize(embedded_prefix_bytes - byte_idx);
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(SLJIT_R0), NumericCast<sljit_sw>(STRING_INLINE_PREFIX_OFFSET + byte_idx));
		false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                                     SljitStringChunkImmediate(constant, byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
	if (constant.size() <= string_t::PREFIX_BYTES) {
		return;
	}

	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));

	for (idx_t byte_idx = string_t::PREFIX_BYTES; byte_idx < constant.size();) {
		const auto chunk_size = SljitStringCompareChunkSize(constant.size() - byte_idx);
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(SLJIT_R4), NumericCast<sljit_sw>(byte_idx));
		false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                                     SljitStringChunkImmediate(constant, byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitSljitTypedExpressionTreeFastStringEqualFalseBranches(struct sljit_compiler *compiler,
                                                                     const string &constant,
                                                                     vector<sljit_jump *> &false_jumps) {
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant.size())));
	EmitSljitTypedExpressionTreeFastStringBytesFalseBranches(compiler, constant, false_jumps);
}

static void EmitSljitTypedExpressionTreeStringBytesFalseBranches(struct sljit_compiler *compiler,
                                                                 const string &constant,
                                                                 vector<sljit_jump *> &false_jumps) {
	for (idx_t byte_idx = 0; byte_idx < constant.size();) {
		const auto chunk_size = SljitStringCompareChunkSize(constant.size() - byte_idx);
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(SLJIT_R4), NumericCast<sljit_sw>(byte_idx));
		false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                                     SljitStringChunkImmediate(constant, byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitSljitTypedExpressionTreeStringPrefixFalseBranches(struct sljit_compiler *compiler, const string &prefix,
                                                                  vector<sljit_jump *> &false_jumps) {
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size())));
	EmitSljitTypedExpressionTreeStringBytesFalseBranches(compiler, prefix, false_jumps);
}

static void EmitSljitTypedExpressionTreeStringEqualFalseBranches(struct sljit_compiler *compiler,
                                                                 const string &constant,
                                                                 vector<sljit_jump *> &false_jumps) {
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant.size())));
	EmitSljitTypedExpressionTreeStringBytesFalseBranches(compiler, constant, false_jumps);
}

static void EmitSljitTypedExpressionTreeStringBoolResult(struct sljit_compiler *compiler,
                                                         const SljitTypedExpressionTreeSlot &result_slot,
                                                         vector<sljit_jump *> &null_jumps,
                                                         vector<sljit_jump *> &match_false_jumps, bool match_value) {
	vector<sljit_jump *> done_jumps;
	EmitStoreSljitTypedExpressionTreeBool(compiler, result_slot, match_value);
	done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto mismatch_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(match_false_jumps, mismatch_label);
	EmitStoreSljitTypedExpressionTreeBool(compiler, result_slot, !match_value);
	done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

	auto null_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_jumps, null_label);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	auto done_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(done_jumps, done_label);
}

static void EmitSljitTypedExpressionTreeFastStringBoolReg(struct sljit_compiler *compiler,
                                                          vector<sljit_jump *> &match_false_jumps, bool match_value) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, match_value ? 1 : 0);
	auto done_from_match = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto mismatch_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(match_false_jumps, mismatch_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, match_value ? 0 : 1);
	sljit_set_label(done_from_match, sljit_emit_label(compiler));
}

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeStringPrefix(struct sljit_compiler *compiler,
                                                                      const ExecutionExpressionIR &node,
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
	EmitSljitTypedExpressionTreeStringBoolResult(compiler, result_slot, null_jumps, false_jumps, true);
	return result_slot;
}

void EmitSljitTypedExpressionTreeFastStringPrefixReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
    SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	idx_t source_index;
	string prefix;
	if (!TryReadSljitTypedExpressionTreeStringPrefixConstant(node, source_index, prefix)) {
		throw InternalException("Unsupported SLJIT typed expression-tree fast string predicate");
	}

	vector<sljit_jump *> false_jumps;
	EmitLoadSljitTypedExpressionTreeFastStringForPrefix(compiler, source_index, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastStringPrefixFalseBranches(compiler, prefix, false_jumps);
	EmitSljitTypedExpressionTreeFastStringBoolReg(compiler, false_jumps, true);
}

void EmitSljitTypedExpressionTreeFastStringCompareReg(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
    SljitTypedExpressionTreeFastIndexMode index_mode,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	if (!TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
		throw InternalException("Unsupported SLJIT typed expression-tree fast string comparison");
	}

	vector<sljit_jump *> equal_false_jumps;
	EmitLoadSljitTypedExpressionTreeFastStringForPrefix(compiler, source_index, index_mode, data_hoists);
	EmitSljitTypedExpressionTreeFastStringEqualFalseBranches(compiler, constant, equal_false_jumps);
	EmitSljitTypedExpressionTreeFastStringBoolReg(compiler, equal_false_jumps, compare_equal);
}

vector<sljit_jump *> EmitSljitTypedExpressionTreeStringPrefixJumpIfTrue(struct sljit_compiler *compiler,
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

vector<sljit_jump *> EmitSljitTypedExpressionTreeStringCompareJumpIfTrue(struct sljit_compiler *compiler,
                                                                         const ExecutionExpressionIR &node) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	if (!TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
		throw InternalException("Unsupported SLJIT typed expression-tree string comparison predicate");
	}

	vector<sljit_jump *> null_jumps;
	vector<sljit_jump *> equal_false_jumps;
	EmitLoadSljitTypedExpressionTreeStringForPrefix(compiler, source_index, null_jumps);
	EmitSljitTypedExpressionTreeStringEqualFalseBranches(compiler, constant, equal_false_jumps);
	if (compare_equal) {
		auto result_true = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto not_true_label = sljit_emit_label(compiler);
		SetSljitJumpLabels(null_jumps, not_true_label);
		SetSljitJumpLabels(equal_false_jumps, not_true_label);
		return vector<sljit_jump *> {result_true};
	}

	auto equal_not_true = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_jumps, not_true_label);
	sljit_set_label(equal_not_true, not_true_label);
	return equal_false_jumps;
}

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeStringCompare(struct sljit_compiler *compiler,
                                                                       const ExecutionExpressionIR &node,
                                                                       idx_t &slot_index) {
	idx_t source_index;
	string constant;
	bool compare_equal;
	if (!TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
		throw InternalException("Unsupported SLJIT typed expression-tree string comparison");
	}

	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> null_jumps;
	vector<sljit_jump *> equal_false_jumps;
	EmitLoadSljitTypedExpressionTreeStringForPrefix(compiler, source_index, null_jumps);
	EmitSljitTypedExpressionTreeStringEqualFalseBranches(compiler, constant, equal_false_jumps);
	EmitSljitTypedExpressionTreeStringBoolResult(compiler, result_slot, null_jumps, equal_false_jumps, compare_equal);
	return result_slot;
}

} // namespace duckdb

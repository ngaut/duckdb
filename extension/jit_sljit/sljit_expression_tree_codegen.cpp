#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <algorithm>
#include <exception>

namespace duckdb {

static void SljitNativeTreeOverflow(SljitNativeVectorInput *input, const char *message) {
	try {
		throw OutOfRangeException("%s", message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeTreeAddOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in addition of DECIMAL");
}

static void SLJIT_FUNC SljitNativeTreeSubtractOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in subtract of DECIMAL");
}

static void SLJIT_FUNC SljitNativeTreeMultiplyOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in multiplication of DECIMAL");
}

static bool SljitExpressionTreeIsDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitExpressionTreeBinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

bool TryGetSljitExpressionTreeBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		native_op = SljitNativeIntegerBinaryOp::ADD;
		return true;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		native_op = SljitNativeIntegerBinaryOp::SUBTRACT;
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		native_op = SljitNativeIntegerBinaryOp::MULTIPLY;
		return true;
	default:
		return false;
	}
}

static bool TryGetSljitExpressionTreeDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max) {
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	result_max = NumericHelper::POWERS_OF_TEN[width] - 1;
	result_min = -result_max;
	return true;
}

static bool SljitExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return SljitExpressionTreeIsDecimal64Node(node);
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return !node.constant.IsNull() && SljitExpressionTreeIsDecimal64Node(node);
	}
	SljitNativeIntegerBinaryOp native_op;
	int64_t result_min;
	int64_t result_max;
	if (node.kind != ExecutionExpressionIRKind::BINARY || !node.left || !node.right ||
	    !SljitExpressionTreeIsDecimal64Node(node) || !SljitExpressionTreeIsDecimal64Node(*node.left) ||
	    !SljitExpressionTreeIsDecimal64Node(*node.right) ||
	    !TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op) ||
	    !SljitExpressionTreeBinaryHasRawSemantics(node) ||
	    !TryGetSljitExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		return false;
	}
	return SljitExpressionTreeIsSupported(*node.left) && SljitExpressionTreeIsSupported(*node.right);
}

static idx_t CountSljitExpressionTreeSpills(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY) {
		return 0;
	}
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	return 1 + CountSljitExpressionTreeSpills(*node.left) + CountSljitExpressionTreeSpills(*node.right);
}

static void CollectSljitExpressionTreeSourceRefs(const ExecutionExpressionIR &node, vector<idx_t> &refs) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (std::find(refs.begin(), refs.end(), node.ref_index) == refs.end()) {
			refs.push_back(node.ref_index);
		}
		return;
	}
	if (node.left) {
		CollectSljitExpressionTreeSourceRefs(*node.left, refs);
	}
	if (node.right) {
		CollectSljitExpressionTreeSourceRefs(*node.right, refs);
	}
}

void AddSljitExpressionOverflowJump(vector<SljitExpressionTreeOverflowJumps> &overflows, SljitNativeIntegerBinaryOp op,
                                    sljit_jump *jump) {
	for (auto &entry : overflows) {
		if (entry.op == op) {
			entry.jumps.push_back(jump);
			return;
		}
	}
	SljitExpressionTreeOverflowJumps entry;
	entry.op = op;
	entry.jumps.push_back(jump);
	overflows.push_back(std::move(entry));
}

void EmitLoadSljitExpressionTreeLogicalIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

void EmitLoadSljitExpressionTreeSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target) {
	// SLJIT_S4 holds the loop-invariant source_sel_array base.
	auto no_source_sel_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S4),
	               NumericCast<sljit_sw>(source_index * sizeof(const sel_t *)));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto use_common_source_sel = sljit_emit_label(compiler);
	sljit_set_label(no_source_sel_array, use_common_source_sel);
	sljit_set_label(no_source_sel, use_common_source_sel);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_common_sel));
	auto no_common_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_common_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_common_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(have_source_index, done);
	sljit_set_label(have_common_source_index, done);
}

sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	// SLJIT_S6 holds the loop-invariant source_validity_array base (hoisted by the caller); index
	// straight to this column's validity pointer instead of reloading the base array every row.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
	               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

sljit_jump *EmitJumpIfSljitExpressionTreeFlatSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
	               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitLoadSljitExpressionTreeReference(struct sljit_compiler *compiler, idx_t source_index,
                                                 sljit_s32 target) {
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	// SLJIT_S5 holds the loop-invariant source_data_array base (hoisted out of the row loop by the
	// caller), so the per-reference base load is gone; index directly to this column's data pointer.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, SLJIT_R1), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
}

static void EmitLoadSljitExpressionTreeReferenceFast(struct sljit_compiler *compiler, idx_t source_index,
                                                     sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, SLJIT_S1), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
}

void EmitSljitExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeTreeAddOverflow));
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeSubtractOverflow));
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeMultiplyOverflow));
		return;
	default:
		throw InternalException("Unknown SLJIT expression-tree overflow operator");
	}
}

static bool SljitExpressionTreeIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE;
}

static void EmitSljitExpressionTreeCheckedBinaryOp(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                   SljitNativeIntegerBinaryOp native_op, sljit_s32 target,
                                                   sljit_s32 left_reg, sljit_s32 right_reg,
                                                   vector<SljitExpressionTreeOverflowJumps> &overflows) {
	auto binary_op = NativeIntegerBinaryOp(SljitNativeIntegerKind::DECIMAL64, native_op);
	if (!node.arithmetic_overflow_check) {
		sljit_emit_op2(compiler, binary_op, target, 0, left_reg, 0, right_reg, 0);
		return;
	}
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, target, 0, left_reg, 0, right_reg, 0);
	AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));

	int64_t result_min;
	int64_t result_max;
	if (!TryGetSljitExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		throw InternalException("SLJIT expression-tree binary node missing decimal64 result range");
	}
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min)));
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max)));
}

static void EmitSljitExpressionTreeValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                         sljit_s32 target, idx_t &spill_index,
                                         vector<SljitExpressionTreeOverflowJumps> &overflows) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(node.constant.GetValueUnsafe<int64_t>()));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		EmitLoadSljitExpressionTreeReference(compiler, node.ref_index, target);
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::BINARY);
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT expression-tree binary operator");
	}
	if (SljitExpressionTreeIsLeaf(*node.right)) {
		D_ASSERT(target != SLJIT_R4);
		EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows);
		EmitSljitExpressionTreeValue(compiler, *node.right, SLJIT_R4, spill_index, overflows);
		EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, target, SLJIT_R4, overflows);
		return;
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValue(compiler, *node.right, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, SLJIT_R4, target, overflows);
}

static void EmitSljitExpressionTreeValueFast(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             sljit_s32 target, idx_t &spill_index,
                                             vector<SljitExpressionTreeOverflowJumps> &overflows) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(node.constant.GetValueUnsafe<int64_t>()));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		EmitLoadSljitExpressionTreeReferenceFast(compiler, node.ref_index, target);
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::BINARY);
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT expression-tree binary operator");
	}
	if (SljitExpressionTreeIsLeaf(*node.right)) {
		D_ASSERT(target != SLJIT_R4);
		EmitSljitExpressionTreeValueFast(compiler, *node.left, target, spill_index, overflows);
		EmitSljitExpressionTreeValueFast(compiler, *node.right, SLJIT_R4, spill_index, overflows);
		EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, target, SLJIT_R4, overflows);
		return;
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValueFast(compiler, *node.left, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValueFast(compiler, *node.right, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, SLJIT_R4, target, overflows);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeVectorFunction &function, string &error) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT expression-tree codegen only supports checked DECIMAL64 arithmetic trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto local_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	// Hoist loop-invariant vector-format arrays so source indexing and references do not reload them
	// for every expression node in the fused tree.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValueFast(compiler, root, SLJIT_R2, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
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
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumExpressionTree(const ExecutionExpressionIR &root,
                                           SljitNativeAggregateUpdateFunction &function, string &error,
                                           SljitNativeAggregateSumStateKind state_kind) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT aggregate reducer only supports checked DECIMAL64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto spill_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = spill_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);
	// Hoist loop-invariant vector-format arrays (see projection tree).
	EmitInitSljitNativeVectorSourceArrays(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValueFast(compiler, root, SLJIT_R2, fast_spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	EmitSljitAggregateLoopStep(compiler, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

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
	sljit_set_label(done, done_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	if (!hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_int64_value));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	}
	sljit_set_label(no_value, sljit_emit_label(compiler));

	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64ExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::INT64);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintExpressionTree(const ExecutionExpressionIR &root,
                                                  SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::HUGEINT);
}

} // namespace duckdb

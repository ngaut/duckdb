#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

#include "sljitLir.h"

#include <algorithm>

namespace duckdb {

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

bool SljitExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
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

idx_t CountSljitExpressionTreeSpills(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY) {
		return 0;
	}
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	return 1 + CountSljitExpressionTreeSpills(*node.left) + CountSljitExpressionTreeSpills(*node.right);
}

void CollectSljitExpressionTreeSourceRefs(const ExecutionExpressionIR &node, vector<idx_t> &refs) {
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

static void EmitLoadSljitExpressionTreeReference(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                                 bool flat_index) {
	auto index_reg = SLJIT_R1;
	if (flat_index) {
		index_reg = SLJIT_S1;
	} else {
		EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, index_reg);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, index_reg), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
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

void EmitSljitExpressionTreeValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, sljit_s32 target,
                                  idx_t &spill_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                  bool flat_index) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(node.constant.GetValueUnsafe<int64_t>()));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		EmitLoadSljitExpressionTreeReference(compiler, node.ref_index, target, flat_index);
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
		EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows, flat_index);
		EmitSljitExpressionTreeValue(compiler, *node.right, SLJIT_R4, spill_index, overflows, flat_index);
		EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, target, SLJIT_R4, overflows);
		return;
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows, flat_index);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValue(compiler, *node.right, target, spill_index, overflows, flat_index);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, SLJIT_R4, target, overflows);
}

} // namespace duckdb

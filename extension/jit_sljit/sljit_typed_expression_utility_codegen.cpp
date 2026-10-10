//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_utility_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

SljitTypedExpressionTreeSlot AllocateSljitTypedExpressionTreeSlot(idx_t &slot_index) {
	const auto value_offset = NumericCast<sljit_sw>(slot_index++ * sizeof(sljit_sw) * 2);
	return SljitTypedExpressionTreeSlot {value_offset, value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))};
}

void EmitStoreSljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           sljit_s32 value_reg, sljit_s32 valid_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.value_offset, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.valid_offset, valid_reg, 0);
}

void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &source,
                                          const SljitTypedExpressionTreeSlot &target) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, target, SLJIT_R2, SLJIT_R3);
}

void EmitStoreSljitTypedExpressionTreeBool(struct sljit_compiler *compiler, const SljitTypedExpressionTreeSlot &slot,
                                           bool value) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, value ? 1 : 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

int64_t SljitTypedExpressionTreeConstantValue(const ExecutionExpressionIR &node) {
	if (node.constant.IsNull()) {
		return 0;
	}
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return node.constant.GetValueUnsafe<bool>() ? 1 : 0;
	}
	if (node.return_type.id() == LogicalTypeId::UTINYINT && node.physical_type == PhysicalType::UINT8) {
		return node.constant.GetValueUnsafe<uint8_t>();
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return node.constant.GetValueUnsafe<int32_t>();
	}
	if (SljitTypedExpressionTreeIsNarrowDecimal128Node(node)) {
		// The narrow-DECIMAL128 plan proved |value| <= INT64_MAX, so the hugeint payload narrows exactly.
		int64_t value;
		if (!Hugeint::TryCast<int64_t>(node.constant.GetValueUnsafe<hugeint_t>(), value)) {
			throw InternalException("SLJIT narrow DECIMAL128 constant does not fit INT64");
		}
		return value;
	}
	return node.constant.GetValueUnsafe<int64_t>();
}

SljitIntegerOverflowChecks SljitTypedExpressionTreeOverflowChecks(const ExecutionExpressionIR &node) {
	SljitIntegerOverflowChecks checks;
	if (!node.arithmetic_overflow_check) {
		return checks;
	}
	checks.overflow = true;
	if (SljitTypedExpressionTreeIsDecimal64Node(node)) {
		checks.range = true;
		if (!TryGetSljitTypedExpressionTreeDecimal64Range(node.return_type, checks.result_min, checks.result_max)) {
			throw InternalException("SLJIT typed expression-tree binary node missing decimal64 result range");
		}
	}
	return checks;
}

bool TryGetSljitTypedExpressionTreeDataPointerHoist(const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists,
                                                    idx_t source_index, sljit_s32 &data_reg) {
	if (!data_hoists) {
		return false;
	}
	for (auto &hoist : *data_hoists) {
		if (hoist.source_index == source_index) {
			data_reg = hoist.data_reg;
			return true;
		}
	}
	return false;
}

void EmitLoadSljitTypedExpressionTreeFastSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                                     sljit_s32 target,
                                                     SljitTypedExpressionTreeFastIndexMode index_mode) {
	if (index_mode == SljitTypedExpressionTreeFastIndexMode::FLAT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
		return;
	}
	if (index_mode == SljitTypedExpressionTreeFastIndexMode::LOGICAL) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
		return;
	}
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, target);
}

void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto jump : jumps) {
		sljit_set_label(jump, label);
	}
}

void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                               const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_jump_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfTrue(struct sljit_compiler *compiler,
                                                                   const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                   vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                                   const vector<idx_t> *known_valid_sources = nullptr);

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
	{
		idx_t source_index;
		string constant;
		bool compare_equal;
		if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
			return EmitSljitTypedExpressionTreeStringCompareJumpIfTrue(compiler, node);
		}
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

vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler,
                                                               const ExecutionExpressionIR &node, idx_t &slot_index,
                                                               vector<SljitExpressionTreeOverflowJumps> &overflows,
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
	{
		idx_t source_index;
		string constant;
		bool compare_equal;
		if (TryReadSljitTypedExpressionTreeStringCompareConstant(node, source_index, constant, compare_equal)) {
			auto true_jumps = EmitSljitTypedExpressionTreeStringCompareJumpIfTrue(compiler, node);
			auto not_true = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto true_label = sljit_emit_label(compiler);
			SetSljitJumpLabels(true_jumps, true_label);
			return vector<sljit_jump *> {not_true};
		}
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

} // namespace duckdb

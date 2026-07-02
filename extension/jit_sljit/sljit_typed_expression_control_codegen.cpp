//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_control_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

SljitTypedExpressionTreeSlot
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

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeCoalesce(struct sljit_compiler *compiler,
                                                                  const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
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

SljitTypedExpressionTreeSlot EmitSljitTypedExpressionTreeCase(struct sljit_compiler *compiler,
                                                              const ExecutionExpressionIR &node, idx_t &slot_index,
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

} // namespace duckdb

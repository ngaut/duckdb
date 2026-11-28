#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_kernel_frame.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

//! An integer operand of a comparison the branchless select evaluates: a constant, a column, or a column's remainder by
//! a constant divisor
struct SljitCompareSelectOperand {
	//! the column's source slot, INVALID_INDEX for a constant
	idx_t source = DConstants::INVALID_INDEX;
	int64_t constant = 0;
	//! the constant the column's value is reduced modulo, 0 for none
	int64_t divisor = 0;
};

//! A comparison the branchless select evaluates: two integer operands of one kind, or a DOUBLE/FLOAT column against a
//! constant that is no NaN
struct SljitCompareSelectLeaf {
	bool is_float = false;
	SljitCompareSelectOperand left;
	SljitCompareSelectOperand right;
	//! a float leaf reads `left.source op constant`
	double float_constant = 0;
	//! the jump type of the comparison, SLJIT_32 included for a 32-bit integer comparison
	sljit_s32 compare_type = 0;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
};

//! A column the select reads once per row
struct SljitCompareSelectSource {
	idx_t ref_index = 0;
	bool is_float = false;
	bool single_precision = false;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;

	sljit_sw Scale() const {
		if (is_float) {
			return single_precision ? 2 : 3;
		}
		return NativeIntegerDataScale(integer_kind);
	}
	sljit_sw Width() const {
		return sljit_sw(1) << Scale();
	}
};

//! A predicate whose rows the select decides without a branch: one comparison, or an AND of comparisons
struct SljitCompareSelectPlan {
	static constexpr idx_t MAX_SOURCES = 8;
	static constexpr idx_t MAX_LEAVES = 8;

	vector<SljitCompareSelectSource> sources;
	vector<SljitCompareSelectLeaf> leaves;

	idx_t FloatLeafCount() const {
		idx_t count = 0;
		for (auto &leaf : leaves) {
			count += leaf.is_float;
		}
		return count;
	}
	//! the float register of a float column's value; the float constants follow the columns
	idx_t FloatSourceCount() const {
		idx_t count = 0;
		for (auto &source : sources) {
			count += source.is_float;
		}
		return count;
	}
};

static bool TryAddSljitCompareSelectSource(SljitCompareSelectPlan &plan, const SljitCompareSelectSource &source,
                                           idx_t &slot) {
	for (slot = 0; slot < plan.sources.size(); slot++) {
		auto &existing = plan.sources[slot];
		if (existing.ref_index == source.ref_index) {
			return existing.is_float == source.is_float && existing.single_precision == source.single_precision &&
			       existing.integer_kind == source.integer_kind;
		}
	}
	if (plan.sources.size() == SljitCompareSelectPlan::MAX_SOURCES) {
		return false;
	}
	plan.sources.push_back(source);
	return true;
}

static bool TryPlanSljitCompareSelectOperand(const ExecutionExpressionIR &node, SljitNativeIntegerKind integer_kind,
                                             SljitCompareSelectPlan &plan, SljitCompareSelectOperand &operand) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		operand.constant = SljitTypedExpressionTreeConstantValue(node);
		return true;
	}
	auto column = &node;
	if (node.kind == ExecutionExpressionIRKind::BINARY && node.binary_op == ExecutionExpressionBinaryOp::MODULO &&
	    node.left && node.left->kind == ExecutionExpressionIRKind::REFERENCE &&
	    SljitTypedExpressionTreeConstantDivisorReductionSupported(node) &&
	    (integer_kind == SljitNativeIntegerKind::INT32 || integer_kind == SljitNativeIntegerKind::INT64)) {
		operand.divisor = SljitTypedExpressionTreeConstantValue(*node.right);
		column = node.left.get();
	}
	if (column->kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	SljitCompareSelectSource source;
	source.ref_index = column->ref_index;
	source.integer_kind = integer_kind;
	return TryAddSljitCompareSelectSource(plan, source, operand.source);
}

static bool TryPlanSljitCompareSelectLeaf(const ExecutionExpressionIR &node, SljitCompareSelectPlan &plan) {
	SljitCompareSelectLeaf leaf;
	SljitTypedExpressionTreeFloatCompare float_compare;
	if (TryReadSljitTypedExpressionTreeFloatCompareConstant(node, float_compare)) {
		SljitCompareSelectSource source;
		source.ref_index = float_compare.source_index;
		source.is_float = true;
		source.single_precision = float_compare.single_precision;
		leaf.is_float = true;
		leaf.float_constant = float_compare.constant;
		leaf.compare_type = NativeDoubleCompareAgainstNumberJumpType(float_compare.op);
		if (!TryAddSljitCompareSelectSource(plan, source, leaf.left.source)) {
			return false;
		}
		plan.leaves.push_back(leaf);
		return true;
	}
	if (node.kind != ExecutionExpressionIRKind::BINARY || node.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !node.left || !node.right || !SljitTypedExpressionTreeComparisonSupported(node.binary_op) ||
	    !SljitTypedExpressionTreeSameIntegerKind(*node.left, *node.right)) {
		return false;
	}
	leaf.integer_kind = SljitTypedExpressionTreeIntegerKind(*node.left);
	if (!TryPlanSljitCompareSelectOperand(*node.left, leaf.integer_kind, plan, leaf.left) ||
	    !TryPlanSljitCompareSelectOperand(*node.right, leaf.integer_kind, plan, leaf.right) ||
	    (leaf.left.source == DConstants::INVALID_INDEX && leaf.right.source == DConstants::INVALID_INDEX)) {
		return false;
	}
	leaf.compare_type =
	    NativeIntegerCompareJumpType(leaf.integer_kind, SljitTypedExpressionTreeCompareOp(node.binary_op));
	plan.leaves.push_back(leaf);
	return true;
}

//! The comparisons an AND of comparisons decides, each a leaf of the plan
static bool TryPlanSljitCompareSelect(const ExecutionExpressionIR &root, SljitCompareSelectPlan &plan) {
	vector<reference<const ExecutionExpressionIR>> leaves;
	if (root.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    root.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : root.children) {
			if (!child) {
				return false;
			}
			leaves.push_back(*child);
		}
	} else {
		leaves.push_back(root);
	}
	if (leaves.empty() || leaves.size() > SljitCompareSelectPlan::MAX_LEAVES) {
		return false;
	}
	for (auto &leaf : leaves) {
		if (!TryPlanSljitCompareSelectLeaf(leaf.get(), plan)) {
			return false;
		}
	}
	return true;
}

static bool SljitCompareSelectTakesRemainder(const SljitCompareSelectPlan &plan) {
	for (auto &leaf : plan.leaves) {
		if (leaf.left.divisor || leaf.right.divisor) {
			return true;
		}
	}
	return false;
}

//! ARM64 has no cheap integer movemask, so its packed compare + mask reduction loses to a branchless scalar loop for
//! a single comparison, and floating point comparisons have no packed form here at all. An AND of integer comparisons
//! keeps the packed loop. A remainder has no packed form either; the flat loop decides it on every target. Rows an
//! execute selection names have no packed loop, so the scalar loop decides them on every target
static bool SljitCompareSelectDecidesFlatRows(const SljitCompareSelectPlan &plan) {
	return SljitCompareSelectTakesRemainder(plan) ||
	       (GetSljitTargetCapabilities().IsArm64() && (plan.leaves.size() == 1 || plan.FloatLeafCount() > 0));
}

bool SljitTypedExpressionTreeFlatCompareSelectSupported(const ExecutionExpressionIR &root) {
	SljitCompareSelectPlan plan;
	return TryPlanSljitCompareSelect(root, plan) && SljitCompareSelectDecidesFlatRows(plan);
}

//! The registers the compare select keeps: the AND of a row's comparisons, the execute selection, then each column's
//! data pointer and each integer column's value
struct SljitCompareSelectRegisters {
	sljit_s32 flags = 0;
	sljit_s32 execute_sel = 0;
	vector<sljit_s32> data;
	vector<sljit_s32> values;
};

static bool TryPlanSljitCompareSelectRegisters(SljitKernelFrame &frame, const SljitCompareSelectPlan &plan,
                                               SljitCompareSelectRegisters &registers) {
	registers.flags = frame.AllocateRegister();
	registers.execute_sel = frame.AllocateRegister();
	if (!registers.flags || !registers.execute_sel) {
		return false;
	}
	for (auto &source : plan.sources) {
		registers.data.push_back(frame.AllocateRegister());
		registers.values.push_back(source.is_float ? sljit_s32(0) : frame.AllocateRegister());
		if (!registers.data.back() || (!source.is_float && !registers.values.back())) {
			return false;
		}
	}
	return true;
}

//! Selects rows: each row loads its columns once and decides its comparisons. Without a branch per row it ANDs them
//! into flags, stores its index into the next selection slot and advances the count by the flags; a flat batch whose
//! predicate takes a remainder, whose rows a period of the divisor decides, branches over the store instead, which
//! the predictor learns and which leaves the rejected rows' stores out. A flat batch's rows run eight to an iteration
//! with the column pointers bumped at the group's end; the rows an execute selection names run one at a time, R4
//! holding the row. S3 counts the selected rows, S4 holds the output selection
static void EmitSljitCompareSelect(struct sljit_compiler *compiler, const SljitCompareSelectPlan &plan,
                                   const SljitCompareSelectRegisters &registers, bool execute_selection) {
	const auto branch_per_row = !execute_selection && SljitCompareSelectTakesRemainder(plan);
	const auto first_float_constant = NumericCast<sljit_s32>(plan.FloatSourceCount());
	// each float column's value register, in column order
	vector<sljit_s32> float_values;
	sljit_s32 next_float = 0;
	for (auto &source : plan.sources) {
		float_values.push_back(source.is_float ? SLJIT_FR(next_float++) : 0);
	}
	for (idx_t slot = 0; slot < plan.sources.size(); slot++) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.data[slot], 0, SLJIT_MEM1(SLJIT_S5),
		               SljitPointerArrayOffset(plan.sources[slot].ref_index));
	}
	if (execute_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.execute_sel, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, execute_sel));
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	{
		sljit_s32 float_constant = first_float_constant;
		for (auto &leaf : plan.leaves) {
			if (leaf.is_float) {
				sljit_emit_fset64(compiler, SLJIT_FR(float_constant++), leaf.float_constant);
			}
		}
	}

	// an operand into a register or an immediate; a remainder lands in target (the reduction uses R0 and R1)
	auto emit_operand = [&](const SljitCompareSelectLeaf &leaf, const SljitCompareSelectOperand &operand,
	                        sljit_s32 target, sljit_s32 &reg, sljit_sw &value) {
		if (operand.source == DConstants::INVALID_INDEX) {
			reg = SLJIT_IMM;
			value = NumericCast<sljit_sw>(operand.constant);
			return;
		}
		reg = registers.values[operand.source];
		value = 0;
		if (operand.divisor) {
			EmitSljitTypedExpressionTreeModulo(compiler, operand.divisor, leaf.integer_kind, reg, target);
			reg = target;
		}
	};
	// the row R4 names under an execute selection, else the flat batch's row lane rows past S1
	auto emit_row = [&](sljit_sw lane) {
		for (idx_t slot = 0; slot < plan.sources.size(); slot++) {
			auto &source = plan.sources[slot];
			const auto memory = execute_selection
			                        ? SljitOperand {SLJIT_MEM2(registers.data[slot], SLJIT_R4), source.Scale()}
			                        : SljitOperand {SLJIT_MEM1(registers.data[slot]), lane * source.Width()};
			if (!source.is_float) {
				sljit_emit_op1(compiler, NativeIntegerLoadOp(source.integer_kind), registers.values[slot], 0,
				               memory.operand, memory.operand_w);
			} else if (source.single_precision) {
				sljit_emit_fop1(compiler, SLJIT_MOV_F32, float_values[slot], 0, memory.operand, memory.operand_w);
				sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_F32, float_values[slot], 0, float_values[slot], 0);
			} else {
				sljit_emit_fop1(compiler, SLJIT_MOV_F64, float_values[slot], 0, memory.operand, memory.operand_w);
			}
		}
		sljit_s32 float_constant = first_float_constant;
		vector<sljit_jump *> rejected;
		for (idx_t leaf_idx = 0; leaf_idx < plan.leaves.size(); leaf_idx++) {
			auto &leaf = plan.leaves[leaf_idx];
			const auto condition = leaf.compare_type & ~SLJIT_32;
			if (leaf.is_float) {
				sljit_emit_fop1(compiler, SLJIT_CMP_F64 | SljitCompareConditionSetFlags(condition),
				                float_values[leaf.left.source], 0, SLJIT_FR(float_constant++), 0);
			} else {
				sljit_s32 left, right;
				sljit_sw left_value, right_value;
				emit_operand(leaf, leaf.left, SLJIT_R2, left, left_value);
				emit_operand(leaf, leaf.right, SLJIT_R3, right, right_value);
				sljit_emit_op2u(compiler,
				                SLJIT_SUB | (leaf.compare_type & SLJIT_32) | SljitCompareConditionSetFlags(condition),
				                left, left_value, right, right_value);
			}
			if (branch_per_row) {
				rejected.push_back(sljit_emit_jump(compiler, condition ^ 1));
			} else {
				sljit_emit_op_flags(compiler, leaf_idx == 0 ? SLJIT_MOV : SLJIT_AND, registers.flags, 0, condition);
			}
		}
		auto row_index = execute_selection ? SLJIT_R4 : SLJIT_S1;
		if (lane != 0) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
			row_index = SLJIT_R4;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, row_index, 0);
		if (branch_per_row) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
			SetSljitJumpLabels(rejected, sljit_emit_label(compiler));
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, registers.flags, 0);
		}
	};

	if (execute_selection) {
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R4, 0, SLJIT_MEM2(registers.execute_sel, SLJIT_S1), 2);
		emit_row(0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	} else {
		auto advance_sources = [&](sljit_sw rows) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, rows);
			for (idx_t slot = 0; slot < plan.sources.size(); slot++) {
				sljit_emit_op2(compiler, SLJIT_ADD, registers.data[slot], 0, registers.data[slot], 0, SLJIT_IMM,
				               rows * plan.sources[slot].Width());
			}
		};
		// Match the optimized native loop shape: independent scalar compares per iteration, one loop branch, and
		// pointer bumps only at the group edge
		static constexpr sljit_sw UNROLL = 8;
		auto unrolled_loop = sljit_emit_label(compiler);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
		auto scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);
		for (sljit_sw lane = 0; lane < UNROLL; lane++) {
			emit_row(lane);
		}
		advance_sources(UNROLL);
		sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), unrolled_loop);

		sljit_set_label(scalar_tail, sljit_emit_label(compiler));
		auto tail_loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		emit_row(0);
		advance_sources(1);
		sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_S3, 0);
}

void EmitStoreSljitTypedExpressionTreeTrueSelection(struct sljit_compiler *compiler, sljit_s32 index_reg) {
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

	// A packed-lane loop can only run under the flat all-valid fast path (it has
	// no per-lane null handling). Reserve a 16-byte, 16-aligned scratch slot for
	// the lane mask above the scalar spill area.
	// an AND of comparisons decides rows without a branch: those an execute selection names on every target, and a flat
	// batch's where no packed loop beats it
	SljitKernelFrame frame(5, GetSljitNativeVectorRegisterLayout().saved_register_count);
	SljitCompareSelectPlan compare_plan;
	SljitCompareSelectRegisters compare_registers;
	const auto compare = fast_path.fast_path_supported && TryPlanSljitCompareSelect(root, compare_plan) &&
	                     TryPlanSljitCompareSelectRegisters(frame, compare_plan, compare_registers);
	const auto flat_compare = compare && SljitCompareSelectDecidesFlatRows(compare_plan);
	auto simd_plan = fast_path.fast_path_supported && !flat_compare ? TryPlanSljitTypedExpressionTreeSimd(root)
	                                                                : SljitTypedExpressionTreeSimdPlan();
	sljit_sw simd_mask_offset = 0;
	sljit_s32 simd_scratches = 5;
	if (simd_plan.supported) {
		simd_mask_offset = (local_size + 15) & ~sljit_sw(15);
		local_size = simd_mask_offset + 16;
		// Vector registers used: constants + all-ones + lane bits + validity nibble
		// + peak live temporaries.
		auto vector_regs = simd_plan.constant_count + (simd_plan.needs_all_ones ? 1 : 0) + simd_plan.max_live_temps +
		                   (simd_plan.nullable_capable ? idx_t(2) : idx_t(0));
		// ARM64 selection keeps one horizontal-mask reduction destination live
		// alongside the predicate temporaries.
		if (GetSljitTargetCapabilities().IsArm64()) {
			vector_regs++;
		}
		simd_scratches = 5 | SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_regs));
	}
	// the row's float values and the float constants
	const auto float_registers = compare ? compare_plan.FloatSourceCount() + compare_plan.FloatLeafCount() : 0;
	simd_scratches = (simd_scratches & ~sljit_s32(0xff)) | frame.ScratchCount();
	simd_scratches |= NumericCast<sljit_s32>(float_registers) << 8;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(simd_scratches),
	                 frame.SavedCount(), local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_IMM, 0);

	vector<SljitArithmeticOverflowSite> overflows;
	struct sljit_jump *fast_done = nullptr;
	if (fast_path.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		if (flat_compare) {
			EmitSljitCompareSelect(compiler, compare_plan, compare_registers, false);
			fast_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		} else if (simd_plan.supported) {
			// Process full lane groups with packed SIMD; S1 is left at the tail
			// start and the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdSelectLoop(compiler, root, simd_plan, simd_mask_offset);
		}
		if (!flat_compare) {
			auto fast_loop = sljit_emit_label(compiler);
			fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
			idx_t fast_spill_index = 0;
			auto fast_false =
			    EmitSljitTypedExpressionTreeFastJumps(compiler, root, false, fast_spill_index, overflows,
			                                          SljitTypedExpressionTreeFastIndexMode::FLAT, nullptr);
			EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
			SetSljitJumpLabels(fast_false, sljit_emit_label(compiler));
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(fast_repeat, fast_loop);
		}
		sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	}
	// Flat batches that MAY contain NULLs run the packed loop with a lane-expanded
	// validity mask ANDead in (AND-only predicates: a row with a NULL referenced
	// source cannot pass); the scalar tail pre-checks sources per row.
	struct sljit_jump *nullable_simd_done = nullptr;
	if (fast_path.fast_path_supported && simd_plan.supported && simd_plan.nullable_capable) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto skip_nullable_simd = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		EmitSljitTypedExpressionTreeSimdSelectLoop(compiler, root, simd_plan, simd_mask_offset, &simd_plan.source_refs);
		auto nullable_tail_loop = sljit_emit_label(compiler);
		auto nullable_tail_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> tail_null_jumps;
		for (auto source_index : simd_plan.source_refs) {
			tail_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t nullable_tail_spill_index = 0;
		auto nullable_tail_false =
		    EmitSljitTypedExpressionTreeFastJumps(compiler, root, false, nullable_tail_spill_index, overflows,
		                                          SljitTypedExpressionTreeFastIndexMode::FLAT, nullptr);
		EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
		auto nullable_tail_skip = sljit_emit_label(compiler);
		SetSljitJumpLabels(nullable_tail_false, nullable_tail_skip);
		for (auto null_jump : tail_null_jumps) {
			sljit_set_label(null_jump, nullable_tail_skip);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto nullable_tail_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(nullable_tail_repeat, nullable_tail_loop);
		sljit_set_label(nullable_tail_done, sljit_emit_label(compiler));
		nullable_simd_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(skip_nullable_simd, sljit_emit_label(compiler));
	}
	// Flat batches with NULLs, for a predicate that decides NULL as FALSE: each leaf checks its own sources
	vector<sljit_jump *> flat_nullable_exits;
	if (fast_path.fast_path_supported && emit_flat_nullable_fast_path && !nullable_simd_done &&
	    SljitTypedExpressionTreeNullDecidesFalse(root)) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		flat_nullable_exits.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0));
		auto flat_nullable_loop = sljit_emit_label(compiler);
		auto emit_latch = [&]() {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), flat_nullable_loop);
			flat_nullable_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		};
		if (SljitTypedExpressionTreeEvaluatesAnyBits(root)) {
			// Forcing a NULL leaf FALSE only lowers an AND/OR predicate, so a row the predicate rejects on the
			// leftover values of its NULLs is rejected: it falls through to the back-edge, and only a row it accepts
			// tests its NULLs, out of line
			idx_t raw_spill_index = 0;
			auto raw_true = EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, root, raw_spill_index, overflows);
			emit_latch();
			SetSljitJumpLabels(raw_true, sljit_emit_label(compiler));
		}
		idx_t flat_nullable_spill_index = 0;
		auto flat_nullable_false =
		    EmitSljitTypedExpressionTreeNullableFastJumps(compiler, root, false, flat_nullable_spill_index, overflows,
		                                                  SljitTypedExpressionTreeFastIndexMode::FLAT, nullptr);
		EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S1);
		SetSljitJumpLabels(flat_nullable_false, sljit_emit_label(compiler));
		emit_latch();
		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	}

	struct sljit_jump *selected_done = nullptr;
	if (compare) {
		// rows an execute selection names, over columns without NULLs or selections of their own
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_sources_flat_all_valid));
		auto sources_not_flat = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, execute_sel));
		auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitSljitCompareSelect(compiler, compare_plan, compare_registers, true);
		selected_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto generic = sljit_emit_label(compiler);
		sljit_set_label(sources_not_flat, generic);
		sljit_set_label(no_execute_sel, generic);
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
	EmitStoreSljitTypedExpressionTreeTrueSelection(compiler, SLJIT_S3);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	sljit_set_label(root_false, sljit_emit_label(compiler));
	sljit_set_label(row_done, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto helper_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	SetSljitJumpLabels(flat_nullable_exits, done_label);
	if (nullable_simd_done) {
		sljit_set_label(nullable_simd_done, done_label);
	}
	if (selected_done) {
		sljit_set_label(selected_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

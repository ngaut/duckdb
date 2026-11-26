#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

//! A comparison the branchless flat select evaluates: an integer column against a constant or a column of its kind,
//! or a DOUBLE/FLOAT column against a constant that is no NaN
struct SljitFlatCompareSelectLeaf {
	bool is_float = false;
	//! the leaf's operands: a source slot, or the constant for an integer leaf's other side
	idx_t left_source = DConstants::INVALID_INDEX;
	idx_t right_source = DConstants::INVALID_INDEX;
	int64_t left_constant = 0;
	int64_t right_constant = 0;
	//! a float leaf reads `left_source op constant`
	double float_constant = 0;
	//! the jump type of the comparison, SLJIT_32 included for a 32-bit integer comparison
	sljit_s32 compare_type = 0;
};

//! A column the select reads, kept in its own register per row
struct SljitFlatCompareSelectSource {
	idx_t ref_index = 0;
	bool is_float = false;
	bool single_precision = false;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;

	sljit_sw Width() const {
		if (is_float) {
			return single_precision ? sizeof(float) : sizeof(double);
		}
		return sljit_sw(1) << NativeIntegerDataScale(integer_kind);
	}
};

//! A predicate whose rows the select decides without a branch: one comparison, or an AND of comparisons, over at most
//! two columns
struct SljitFlatCompareSelectPlan {
	static constexpr idx_t MAX_SOURCES = 2;
	static constexpr idx_t MAX_LEAVES = 4;

	vector<SljitFlatCompareSelectSource> sources;
	vector<SljitFlatCompareSelectLeaf> leaves;

	idx_t FloatLeafCount() const {
		idx_t count = 0;
		for (auto &leaf : leaves) {
			count += leaf.is_float;
		}
		return count;
	}
};

static bool TryAddSljitFlatCompareSelectSource(SljitFlatCompareSelectPlan &plan,
                                               const SljitFlatCompareSelectSource &source, idx_t &slot) {
	for (slot = 0; slot < plan.sources.size(); slot++) {
		auto &existing = plan.sources[slot];
		if (existing.ref_index == source.ref_index) {
			return existing.is_float == source.is_float && existing.single_precision == source.single_precision &&
			       existing.integer_kind == source.integer_kind;
		}
	}
	if (plan.sources.size() == SljitFlatCompareSelectPlan::MAX_SOURCES) {
		return false;
	}
	plan.sources.push_back(source);
	return true;
}

static bool TryPlanSljitFlatCompareSelectLeaf(const ExecutionExpressionIR &node, SljitFlatCompareSelectPlan &plan) {
	SljitFlatCompareSelectLeaf leaf;
	SljitTypedExpressionTreeFloatCompare float_compare;
	if (TryReadSljitTypedExpressionTreeFloatCompareConstant(node, float_compare)) {
		SljitFlatCompareSelectSource source;
		source.ref_index = float_compare.source_index;
		source.is_float = true;
		source.single_precision = float_compare.single_precision;
		leaf.is_float = true;
		leaf.float_constant = float_compare.constant;
		leaf.compare_type = NativeDoubleCompareAgainstNumberJumpType(float_compare.op);
		if (!TryAddSljitFlatCompareSelectSource(plan, source, leaf.left_source)) {
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
	const auto integer_kind = SljitTypedExpressionTreeIntegerKind(*node.left);
	auto read_operand = [&](const ExecutionExpressionIR &operand, idx_t &slot, int64_t &constant) {
		if (operand.kind == ExecutionExpressionIRKind::CONSTANT) {
			constant = SljitTypedExpressionTreeConstantValue(operand);
			return true;
		}
		if (operand.kind != ExecutionExpressionIRKind::REFERENCE) {
			return false;
		}
		SljitFlatCompareSelectSource source;
		source.ref_index = operand.ref_index;
		source.integer_kind = integer_kind;
		return TryAddSljitFlatCompareSelectSource(plan, source, slot);
	};
	if (!read_operand(*node.left, leaf.left_source, leaf.left_constant) ||
	    !read_operand(*node.right, leaf.right_source, leaf.right_constant) ||
	    (leaf.left_source == DConstants::INVALID_INDEX && leaf.right_source == DConstants::INVALID_INDEX)) {
		return false;
	}
	leaf.compare_type = NativeIntegerCompareJumpType(integer_kind, SljitTypedExpressionTreeCompareOp(node.binary_op));
	plan.leaves.push_back(leaf);
	return true;
}

//! ARM64 has no cheap integer movemask, so its packed compare + mask reduction loses to a branchless scalar loop for
//! a single integer comparison, and floating point comparisons have no packed form here at all. An AND of integer
//! comparisons keeps the packed loop
static bool TryPlanSljitFlatCompareSelect(const ExecutionExpressionIR &root, SljitFlatCompareSelectPlan &plan) {
	if (!GetSljitTargetCapabilities().IsArm64() || !GetSljitNativeVectorRegisterLayout().HasOptionalInvariant()) {
		return false;
	}
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
	if (leaves.empty() || leaves.size() > SljitFlatCompareSelectPlan::MAX_LEAVES) {
		return false;
	}
	for (auto &leaf : leaves) {
		if (!TryPlanSljitFlatCompareSelectLeaf(leaf.get(), plan)) {
			return false;
		}
	}
	return plan.leaves.size() == 1 || plan.FloatLeafCount() > 0;
}

bool SljitTypedExpressionTreeFlatCompareSelectSupported(const ExecutionExpressionIR &root) {
	SljitFlatCompareSelectPlan plan;
	return TryPlanSljitFlatCompareSelect(root, plan);
}

static bool SljitUseFlatConstantModuloCompareSelect(const ExecutionExpressionIR &root) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !SljitTypedExpressionTreeComparisonSupported(root.binary_op) ||
	    root.right->kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}
	auto &modulo = *root.left;
	if (modulo.kind != ExecutionExpressionIRKind::BINARY || modulo.binary_op != ExecutionExpressionBinaryOp::MODULO ||
	    !modulo.left || modulo.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !SljitTypedExpressionTreeConstantDivisorReductionSupported(modulo) ||
	    !SljitTypedExpressionTreeSameIntegerKind(modulo, *root.right)) {
		return false;
	}
	auto integer_kind = SljitTypedExpressionTreeIntegerKind(modulo);
	return integer_kind == SljitNativeIntegerKind::INT32 || integer_kind == SljitNativeIntegerKind::INT64;
}

static void EmitSljitFlatConstantModuloCompareSelect(struct sljit_compiler *compiler,
                                                     const ExecutionExpressionIR &root) {
	D_ASSERT(SljitUseFlatConstantModuloCompareSelect(root));
	auto &modulo = *root.left;
	auto &source = *modulo.left;
	auto integer_kind = SljitTypedExpressionTreeIntegerKind(modulo);
	auto divisor = SljitTypedExpressionTreeConstantValue(*modulo.right);
	auto compare_value = SljitTypedExpressionTreeConstantValue(*root.right);
	auto load_op = NativeIntegerLoadOp(integer_kind);
	auto data_scale = NativeIntegerDataScale(integer_kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto compare_type = NativeIntegerCompareJumpType(integer_kind, SljitTypedExpressionTreeCompareOp(root.binary_op));

	// Hoist the only source pointer and the selection append cursor. The modulo
	// reduction uses R0/R1 as scratch and leaves the remainder in R2.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	auto emit_lane = [&](sljit_sw lane, sljit_sw data_offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S5), data_offset);
		EmitSljitTypedExpressionTreeModulo(compiler, divisor, integer_kind, SLJIT_R2, SLJIT_R2);
		auto not_selected =
		    sljit_emit_cmp(compiler, compare_type ^ 1, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(compare_value));
		if (lane == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_S1, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, SLJIT_R2, 0);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		sljit_set_label(not_selected, sljit_emit_label(compiler));
	};

	static constexpr sljit_sw UNROLL = 8;
	auto unrolled_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	auto scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);
	for (sljit_sw lane = 0; lane < UNROLL; lane++) {
		emit_lane(lane, lane * data_width);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, UNROLL * data_width);
	auto unrolled_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(unrolled_repeat, unrolled_loop);

	sljit_set_label(scalar_tail, sljit_emit_label(compiler));
	auto tail_loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_lane(0, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
	auto tail_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(tail_repeat, tail_loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_S3, 0);
}

//! Selects the rows of a flat batch without a branch per row: each row decides its comparisons into flags, stores its
//! index into the next selection slot and advances the cursor by the flag
static void EmitSljitFlatCompareSelect(struct sljit_compiler *compiler, const SljitFlatCompareSelectPlan &plan) {
	const sljit_s32 source_data[] = {SLJIT_S5, GetSljitNativeVectorRegisterLayout().optional_invariant};
	const sljit_s32 integer_values[] = {SLJIT_R2, SLJIT_R3};
	const sljit_s32 float_values[] = {SLJIT_FR0, SLJIT_FR1};
	static constexpr sljit_s32 FIRST_FLOAT_CONSTANT = 2;
	// S5 holds the source data array until the first source's pointer replaces it
	for (idx_t slot = plan.sources.size(); slot-- > 0;) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, source_data[slot], 0, SLJIT_MEM1(SLJIT_S5),
		               SljitPointerArrayOffset(plan.sources[slot].ref_index));
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_s32 float_constant = FIRST_FLOAT_CONSTANT;
	for (auto &leaf : plan.leaves) {
		if (leaf.is_float) {
			sljit_emit_fset64(compiler, SLJIT_FR(float_constant++), leaf.float_constant);
		}
	}

	auto emit_lane = [&](sljit_sw lane) {
		for (idx_t slot = 0; slot < plan.sources.size(); slot++) {
			auto &source = plan.sources[slot];
			const auto offset = lane * source.Width();
			if (!source.is_float) {
				sljit_emit_op1(compiler, NativeIntegerLoadOp(source.integer_kind), integer_values[slot], 0,
				               SLJIT_MEM1(source_data[slot]), offset);
			} else if (source.single_precision) {
				sljit_emit_fop1(compiler, SLJIT_MOV_F32, float_values[slot], 0, SLJIT_MEM1(source_data[slot]), offset);
				sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_F32, float_values[slot], 0, float_values[slot], 0);
			} else {
				sljit_emit_fop1(compiler, SLJIT_MOV_F64, float_values[slot], 0, SLJIT_MEM1(source_data[slot]), offset);
			}
		}
		float_constant = FIRST_FLOAT_CONSTANT;
		for (idx_t leaf_idx = 0; leaf_idx < plan.leaves.size(); leaf_idx++) {
			auto &leaf = plan.leaves[leaf_idx];
			const auto condition = leaf.compare_type & ~SLJIT_32;
			if (leaf.is_float) {
				sljit_emit_fop1(compiler, SLJIT_CMP_F64 | SljitCompareConditionSetFlags(condition),
				                float_values[leaf.left_source], 0, SLJIT_FR(float_constant++), 0);
			} else {
				auto operand = [&](idx_t source, int64_t constant, sljit_s32 &reg, sljit_sw &value) {
					reg = source == DConstants::INVALID_INDEX ? SLJIT_IMM : integer_values[source];
					value = source == DConstants::INVALID_INDEX ? NumericCast<sljit_sw>(constant) : 0;
				};
				sljit_s32 left, right;
				sljit_sw left_value, right_value;
				operand(leaf.left_source, leaf.left_constant, left, left_value);
				operand(leaf.right_source, leaf.right_constant, right, right_value);
				sljit_emit_op2u(compiler,
				                SLJIT_SUB | (leaf.compare_type & SLJIT_32) | SljitCompareConditionSetFlags(condition),
				                left, left_value, right, right_value);
			}
			sljit_emit_op_flags(compiler, leaf_idx == 0 ? SLJIT_MOV : SLJIT_AND, SLJIT_R0, 0, condition);
		}
		auto row_index = SLJIT_S1;
		if (lane != 0) {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_S1, 0, SLJIT_IMM, lane);
			row_index = SLJIT_R1;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_S4, SLJIT_S3), 2, row_index, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_R0, 0);
	};
	auto advance_sources = [&](sljit_sw rows) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, rows);
		for (idx_t slot = 0; slot < plan.sources.size(); slot++) {
			sljit_emit_op2(compiler, SLJIT_ADD, source_data[slot], 0, source_data[slot], 0, SLJIT_IMM,
			               rows * plan.sources[slot].Width());
		}
	};

	// Match the optimized native loop shape: independent scalar compares per iteration, one loop branch, and pointer
	// bumps only at the group edge. Typed filter selection always provides true_sel; the outer C++ adapter owns the
	// optional identity-materialization policy.
	static constexpr sljit_sw UNROLL = 8;
	auto unrolled_loop = sljit_emit_label(compiler);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_S1, 0, SLJIT_IMM, UNROLL);
	auto scalar_tail = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R0, 0, SLJIT_S2, 0);
	for (sljit_sw lane = 0; lane < UNROLL; lane++) {
		emit_lane(lane);
	}
	advance_sources(UNROLL);
	sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), unrolled_loop);

	sljit_set_label(scalar_tail, sljit_emit_label(compiler));
	auto tail_loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_lane(0);
	advance_sources(1);
	sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), tail_loop);

	sljit_set_label(done, sljit_emit_label(compiler));
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
	SljitFlatCompareSelectPlan flat_compare_plan;
	auto flat_compare = fast_path.fast_path_supported && TryPlanSljitFlatCompareSelect(root, flat_compare_plan);
	auto flat_constant_modulo_compare = fast_path.fast_path_supported && SljitUseFlatConstantModuloCompareSelect(root);
	auto simd_plan = fast_path.fast_path_supported && !flat_compare && !flat_constant_modulo_compare
	                     ? TryPlanSljitTypedExpressionTreeSimd(root)
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
	if (flat_compare) {
		// the row's two float values and the float constants
		simd_scratches |= NumericCast<sljit_s32>(2 + flat_compare_plan.FloatLeafCount()) << 8;
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(simd_scratches),
	                 GetSljitNativeVectorRegisterLayout().saved_register_count, local_size);
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
			EmitSljitFlatCompareSelect(compiler, flat_compare_plan);
			fast_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		} else if (flat_constant_modulo_compare) {
			EmitSljitFlatConstantModuloCompareSelect(compiler, root);
			fast_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		} else if (simd_plan.supported) {
			// Process full lane groups with packed SIMD; S1 is left at the tail
			// start and the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdSelectLoop(compiler, root, simd_plan, simd_mask_offset);
		}
		if (!flat_compare && !flat_constant_modulo_compare) {
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
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

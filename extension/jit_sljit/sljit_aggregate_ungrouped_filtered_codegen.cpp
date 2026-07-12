#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitFilteredFusedPrimitiveAggregateCodegenPlan {
	SljitTypedExpressionTreePlan predicate;
	vector<SljitTypedExpressionTreePlan> payloads;
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	idx_t tree_node_count = 0;
	bool fast_path_supported = false;
};

static bool BuildSljitFilteredFusedPrimitiveAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &payload,
                                                                 const ExecutionRegionAggregateInput &aggregate,
                                                                 SljitTypedExpressionTreePlan &payload_plan,
                                                                 SljitAggregatePayloadDescriptor &descriptor) {
	if (!SljitTryBindAggregatePayloadDescriptor(payload, aggregate, descriptor)) {
		return false;
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return true;
	}
	if ((descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    !descriptor.IsMachineWord()) {
		return false;
	}
	if (!payload.expression_tree) {
		return false;
	}
	payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
	return SljitAggregateTypedPayloadPlanSupported(payload_plan, descriptor);
}

static bool
BuildSljitFilteredFusedPrimitiveAggregateCodegenPlan(const ExecutionExpressionIR &predicate,
                                                     const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                     const vector<ExecutionRegionAggregateInput> &aggregates,
                                                     SljitFilteredFusedPrimitiveAggregateCodegenPlan &codegen_plan) {
	if (payloads.empty() || payloads.size() != aggregates.size()) {
		return false;
	}
	codegen_plan = SljitFilteredFusedPrimitiveAggregateCodegenPlan();
	codegen_plan.predicate = BuildSljitTypedExpressionTreePlan(predicate, false);
	if (!codegen_plan.predicate.supported || !codegen_plan.predicate.result_is_bool) {
		return false;
	}
	codegen_plan.tree_node_count = codegen_plan.predicate.node_count;
	codegen_plan.fast_path_supported = codegen_plan.predicate.fast_path.fast_path_supported;
	codegen_plan.payloads.resize(payloads.size());
	codegen_plan.payload_descriptors.resize(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!BuildSljitFilteredFusedPrimitiveAggregatePayloadPlan(payloads[payload_idx], aggregates[payload_idx],
		                                                          codegen_plan.payloads[payload_idx],
		                                                          codegen_plan.payload_descriptors[payload_idx])) {
			return false;
		}
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		codegen_plan.tree_node_count += codegen_plan.payloads[payload_idx].node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, SljitNativeAggregateUpdateFunction &function,
    string &error) {
	SljitFilteredFusedPrimitiveAggregateCodegenPlan codegen_plan;
	if (!BuildSljitFilteredFusedPrimitiveAggregateCodegenPlan(predicate, payloads, aggregates, codegen_plan)) {
		error = "unsupported filtered fused aggregate payload shape";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	vector<sljit_sw> local_count_offsets(payloads.size(), -1);
	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> local_sum_upper_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	sljit_sw local_size = tree_local_size;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		local_count_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		local_sum_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			local_sum_upper_offsets[payload_idx] = local_size;
			local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		}
		saw_value_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}

	// Packed-lane COUNT(*) filter: applies when the sole aggregate is COUNT(*),
	// the flat all-valid fast path is available, and the predicate is SIMD-
	// profitable. Counts full lane groups with SIMD; the scalar fast loop below
	// handles the < lanes tail.
	SljitTypedExpressionTreeSimdPlan simd_plan;
	SljitTypedExpressionTreeSimdPlan simd_payload_plan;
	bool simd_is_sum = false;
	bool simd_is_hybrid = false;
	if (payloads.size() == 1 && codegen_plan.fast_path_supported) {
		auto kind = codegen_plan.payload_descriptors[0].primitive_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			simd_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_INT64 && payloads[0].expression_tree) {
			auto predicate_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
			// SADALP widens int32->int64, so the packed SUM path needs a 4-lane int32
			// predicate and a 4-lane int32 value payload; it is ARM64-only.
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
			auto payload_plan = TryPlanSljitTypedExpressionTreeSimdValue(*payloads[0].expression_tree, 2);
			if (predicate_plan.supported && predicate_plan.elem_scale == 2 && payload_plan.supported) {
				simd_plan = predicate_plan;
				simd_payload_plan = payload_plan;
				simd_is_sum = true;
			}
#endif
		}
	}
	// Hybrid: when no fully-packed form fits the payloads, the predicate mask can
	// still vectorize with the payload work staying scalar per matching lane.
	if (!simd_plan.supported && codegen_plan.fast_path_supported) {
		auto predicate_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
		if (predicate_plan.supported) {
			simd_plan = predicate_plan;
			simd_is_hybrid = true;
		}
	}
	// The nullable variant runs the same packed loops with a lane-expanded validity
	// mask ANDed in; the row-skip semantics require an AND-only predicate and cover
	// payload references only when there is a single payload (or none for count(*)).
	vector<idx_t> simd_validity_refs;
	bool simd_nullable_ok = false;
	if (simd_plan.supported && simd_plan.nullable_capable && payloads.size() == 1) {
		simd_validity_refs = simd_plan.source_refs;
		auto kind = codegen_plan.payload_descriptors[0].primitive_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			simd_nullable_ok = true;
		} else if (payloads[0].expression_tree) {
			CollectSljitTypedExpressionTreeReferences(*payloads[0].expression_tree, simd_validity_refs);
			simd_nullable_ok = true;
		}
	}
	sljit_sw simd_mask_offset = 0;
	sljit_s32 simd_scratches = 5;
	if (simd_plan.supported) {
		simd_mask_offset = (local_size + 15) & ~sljit_sw(15);
		local_size = simd_mask_offset + 16;
		// constants (predicate + payload, an over-estimate if they share values) + all-ones
		// + lane bits + accumulator(s) + peak live temporaries (+1 validity nibble).
		auto accumulators = simd_is_sum ? idx_t(2) : idx_t(1);
		auto vector_regs = simd_plan.constant_count + simd_payload_plan.constant_count +
		                   (simd_plan.needs_all_ones ? 1 : 0) + accumulators + simd_plan.max_live_temps +
		                   simd_payload_plan.max_live_temps + (simd_is_sum ? idx_t(1) : idx_t(0)) +
		                   (simd_nullable_ok ? idx_t(2) : idx_t(0));
		simd_scratches = 5 | SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_regs));
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), simd_scratches, 7, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], SLJIT_IMM, 0);
		if (local_sum_offsets[payload_idx] >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
			if (local_sum_upper_offsets[payload_idx] >= 0) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx],
				               SLJIT_IMM, 0);
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
		}
	}
	EmitInitSljitNativeVectorSourceArrays(compiler);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	struct sljit_jump *nullable_simd_done = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		// The scalar per-row payload updates, shared by the scalar fast loop and the
		// hybrid packed-mask loop (which runs them once per matching lane).
		auto emit_payload_updates = [&]() {
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
				if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
					continue;
				}
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeFastValueReg(compiler, *payloads[payload_idx].expression_tree,
				                                         payload_spill_index, overflows);
				EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
				                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
				                                 SLJIT_R2);
				EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
			}
		};

		if (simd_is_sum) {
			// Sum full lane groups with packed SIMD (masked payload widened via SADALP);
			// the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdSumLoop(compiler, predicate, *payloads[0].expression_tree, simd_plan,
			                                        local_sum_offsets[0], local_count_offsets[0], saw_value_offsets[0],
			                                        simd_mask_offset);
		} else if (simd_is_hybrid) {
			// Packed predicate mask; the payload updates stay scalar per matching lane.
			EmitSljitTypedExpressionTreeSimdHybridFilterLoop(compiler, predicate, simd_plan, simd_mask_offset,
			                                                 emit_payload_updates);
		} else if (simd_plan.supported) {
			// Count full lane groups with packed SIMD; S1 is left at the tail start
			// and the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdCountLoop(compiler, predicate, simd_plan, local_count_offsets[0],
			                                          simd_mask_offset);
		}
		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		idx_t predicate_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, predicate, predicate_spill_index, overflows);
		auto predicate_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		emit_payload_updates();
		sljit_set_label(predicate_false, sljit_emit_label(compiler));
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));

		// Flat batches that MAY contain NULLs run the same packed loops with a
		// lane-expanded validity mask ANDed into the predicate mask; the scalar
		// tail pre-checks the referenced sources per row (a null row cannot pass
		// an AND-only predicate, and a single payload's sources are covered too).
		if (simd_nullable_ok) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
			auto skip_nullable_simd = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			if (simd_is_sum) {
				EmitSljitTypedExpressionTreeSimdSumLoop(compiler, predicate, *payloads[0].expression_tree, simd_plan,
				                                        local_sum_offsets[0], local_count_offsets[0],
				                                        saw_value_offsets[0], simd_mask_offset, &simd_validity_refs);
			} else if (simd_is_hybrid) {
				EmitSljitTypedExpressionTreeSimdHybridFilterLoop(compiler, predicate, simd_plan, simd_mask_offset,
				                                                 emit_payload_updates, &simd_validity_refs);
			} else {
				EmitSljitTypedExpressionTreeSimdCountLoop(compiler, predicate, simd_plan, local_count_offsets[0],
				                                          simd_mask_offset, &simd_validity_refs);
			}
			// Scalar tail for the remaining < lanes rows: skip any row with a NULL
			// referenced source, then run the shared predicate + payload blocks.
			auto nullable_tail_loop = sljit_emit_label(compiler);
			auto nullable_tail_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
			vector<sljit_jump *> tail_null_jumps;
			for (auto source_index : simd_validity_refs) {
				tail_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
			}
			idx_t nullable_tail_spill_index = 0;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, predicate, nullable_tail_spill_index, overflows);
			auto nullable_tail_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			emit_payload_updates();
			auto nullable_tail_skip = sljit_emit_label(compiler);
			sljit_set_label(nullable_tail_false, nullable_tail_skip);
			for (auto null_jump : tail_null_jumps) {
				sljit_set_label(null_jump, nullable_tail_skip);
			}
			EmitNextSljitNativeVectorLoop(compiler, nullable_tail_loop);
			sljit_set_label(nullable_tail_done, sljit_emit_label(compiler));
			nullable_simd_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(skip_nullable_simd, sljit_emit_label(compiler));
		}

		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		idx_t selected_predicate_spill_index = 0;
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, predicate, selected_predicate_spill_index,
		                                                 overflows);
		auto selected_predicate_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
			if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
				continue;
			}
			idx_t payload_spill_index = 0;
			EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payloads[payload_idx].expression_tree,
			                                                 payload_spill_index, overflows);
			EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                 SLJIT_R2);
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		}
		sljit_set_label(selected_predicate_false, sljit_emit_label(compiler));
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<sljit_jump *> row_skip_jumps;
	idx_t predicate_slot_index = 0;
	auto predicate_slot = EmitSljitTypedExpressionTreeValue(compiler, predicate, predicate_slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.valid_offset);
	row_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.value_offset);
	row_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
			continue;
		}
		vector<sljit_jump *> payload_skip_jumps;
		idx_t payload_slot_index = 0;
		auto payload_slot = EmitSljitTypedExpressionTreeValue(compiler, *payloads[payload_idx].expression_tree,
		                                                      payload_slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
		payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
		                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                 SLJIT_R2);
		EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_skip_label = sljit_emit_label(compiler);
		for (auto payload_skip : payload_skip_jumps) {
			sljit_set_label(payload_skip, payload_skip_label);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto row_skip_label = sljit_emit_label(compiler);
	for (auto row_skip : row_skip_jumps) {
		sljit_set_label(row_skip, row_skip_label);
	}
	sljit_set_label(row_done, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);

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
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
	}
	if (nullable_simd_done) {
		sljit_set_label(nullable_simd_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx]);
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitUngroupedAggregateCommitCountStar(compiler, payload_idx, SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitUngroupedAggregateCommitSumHugeint(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                       local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                       SLJIT_R2);
		} else {
			EmitUngroupedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                     saw_value_offsets[payload_idx], SLJIT_R2);
		}
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

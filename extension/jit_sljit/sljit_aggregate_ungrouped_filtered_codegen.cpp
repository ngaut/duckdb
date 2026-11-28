#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_aggregate_source_hoist_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_kernel_frame.hpp"
#include "sljit_predicate_row_loops.hpp"
#include "sljit_typed_expression_plan.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

#include "sljitLir.h"

#include <algorithm>

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
	// COUNT reads only whether its payload is NULL: the tree's value is evaluated and dropped
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
		if (!payload.expression_tree) {
			return false;
		}
		payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
		return payload_plan.supported;
	}
	const bool integer_sum = (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	                          descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) &&
	                         descriptor.IsMachineWord();
	if (!integer_sum && descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
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
		// a payload whose CASE yields NULL for some rows takes the fast loops too, skipping its update on them
		codegen_plan.tree_node_count += codegen_plan.payloads[payload_idx].node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported &&
		    codegen_plan.payloads[payload_idx].fast_path.FastPathSupportedWithNullExits();
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, SljitNativeAggregateUpdateFunction &function,
    bool &fast_null_rows, string &error) {
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
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR || kind == AggregatePrimitiveUpdateKind::COUNT) {
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

	// Packed-lane filters, when the flat all-valid fast path is available and the predicate packs: the sole
	// aggregate COUNT(*) counts full lane groups, and one SUM beside any COUNT(*) sums and counts them; the scalar
	// fast loop below handles the < lanes tail.
	SljitTypedExpressionTreeSimdPlan simd_plan;
	SljitTypedExpressionTreeSimdPlan simd_payload_plan;
	bool simd_is_sum = false;
	bool simd_is_hybrid = false;
	idx_t sum_payload_idx = DConstants::INVALID_INDEX;
	idx_t sum_payload_count = 0;
	vector<sljit_sw> simd_count_offsets;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			sum_payload_idx = payload_idx;
			sum_payload_count++;
		}
		simd_count_offsets.push_back(local_count_offsets[payload_idx]);
	}
	if (payloads.size() == 1 && sum_payload_count == 0 && codegen_plan.fast_path_supported) {
		simd_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
	} else if (sum_payload_count == 1 && codegen_plan.fast_path_supported &&
	           codegen_plan.payload_descriptors[sum_payload_idx].primitive_kind ==
	               AggregatePrimitiveUpdateKind::SUM_INT64 &&
	           payloads[sum_payload_idx].expression_tree) {
		auto predicate_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
		// SADALP widens int32->int64, so the packed SUM path needs a 4-lane int32
		// predicate and a 4-lane int32 value payload; it is ARM64-only.
		if (GetSljitTargetCapabilities().IsArm64()) {
			auto payload_plan = TryPlanSljitTypedExpressionTreeSimdValue(*payloads[sum_payload_idx].expression_tree, 2);
			if (predicate_plan.supported && predicate_plan.elem_scale == 2 && payload_plan.supported) {
				simd_plan = predicate_plan;
				simd_payload_plan = payload_plan;
				simd_is_sum = true;
			}
		}
	}
	// Hybrid: when no fully-packed form fits the payloads, the predicate mask can
	// still vectorize with the payload work staying scalar per matching lane.
	// a DOUBLE sum's running value lives in a float register, which the vector registers overlap
	idx_t double_sum_count = 0;
	for (auto &descriptor : codegen_plan.payload_descriptors) {
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			double_sum_count++;
		}
	}
	if (!simd_plan.supported && codegen_plan.fast_path_supported && double_sum_count == 0) {
		auto predicate_plan = TryPlanSljitTypedExpressionTreeSimd(predicate);
		if (SljitTypedExpressionTreeSimdHybridFilterProfitable(predicate_plan)) {
			simd_plan = predicate_plan;
			simd_is_hybrid = true;
		}
	}
	// The nullable variant runs the same packed loops with a lane-expanded validity
	// mask ANDead in, skipping every row with a NULL in a masked source. An AND-only
	// predicate rejects a row with a NULL source, so the skip is exact for the
	// predicate's sources, for the own sources of a single null-strict payload (a NULL
	// source makes its value NULL, which a sum or a count ignores), and for any
	// payload that reads only the predicate's sources.
	vector<idx_t> simd_validity_refs;
	bool simd_nullable_ok = false;
	if (simd_plan.supported && simd_plan.nullable_capable) {
		simd_validity_refs = simd_plan.source_refs;
		simd_nullable_ok = true;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (codegen_plan.payload_descriptors[payload_idx].primitive_kind ==
			    AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			vector<idx_t> payload_refs;
			CollectSljitTypedExpressionTreeReferences(*payloads[payload_idx].expression_tree, payload_refs);
			for (auto source_index : payload_refs) {
				const bool predicate_source = std::find(simd_plan.source_refs.begin(), simd_plan.source_refs.end(),
				                                        source_index) != simd_plan.source_refs.end();
				if (payloads.size() == 1 &&
				    SljitTypedExpressionTreeIsNullStrict(*payloads[payload_idx].expression_tree)) {
					AddSljitTypedKnownValidSource(simd_validity_refs, source_index);
				} else if (!predicate_source) {
					simd_nullable_ok = false;
				}
			}
		}
	}
	sljit_sw simd_mask_offset = 0;
	sljit_s32 simd_scratches = 5;
	if (simd_plan.supported) {
		simd_mask_offset = (local_size + 15) & ~sljit_sw(15);
		local_size = simd_mask_offset + 24;
		// Constants (predicate + payload, an over-estimate if they share values),
		// all-ones, lane bits, peak live temporaries, and loop state. SUM and COUNT
		// keep two accumulator sets (count + sum, or count); the hybrid loop keeps
		// its ARM64 mask reducer.
		auto accumulators = simd_is_sum ? idx_t(4) : simd_is_hybrid ? idx_t(1) : idx_t(2);
		auto vector_regs = simd_plan.constant_count + simd_payload_plan.constant_count +
		                   (simd_plan.needs_all_ones ? 1 : 0) + accumulators + simd_plan.max_live_temps +
		                   simd_payload_plan.max_live_temps + (simd_is_sum ? idx_t(1) : idx_t(0)) +
		                   (simd_nullable_ok ? idx_t(2) : idx_t(0));
		simd_scratches = 5 | SLJIT_ENTER_VECTOR(NumericCast<sljit_s32>(vector_regs));
	}

	// A flat batch with NULLs ANDs the validity of every source the rows read into one mask before its rows
	const sljit_sw combined_validity_offset = local_size;
	local_size += NumericCast<sljit_sw>(STANDARD_VECTOR_SIZE / ValidityMask::BITS_PER_VALUE * sizeof(validity_t));

	// The scalar loops hold the most used sources' data pointers in registers instead of loading them from the
	// source array for every reference of every row
	vector<SljitTypedExpressionTreeLoopHoist> data_hoists;
	// the generic loop calls helpers, so the hoisted pointers take saved registers
	SljitKernelFrame hoist_frame(5, GetSljitNativeVectorRegisterLayout().saved_register_count, true);
	vector<sljit_s32> hoist_registers;
	for (idx_t hoist_idx = 0; hoist_idx < 2; hoist_idx++) {
		if (auto reg = hoist_frame.AllocateRegister()) {
			hoist_registers.push_back(reg);
		}
	}
	if (!hoist_registers.empty()) {
		vector<reference<const ExecutionExpressionIR>> expressions {predicate};
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (codegen_plan.payload_descriptors[payload_idx].primitive_kind !=
			    AggregatePrimitiveUpdateKind::COUNT_STAR) {
				expressions.push_back(*payloads[payload_idx].expression_tree);
			}
		}
		data_hoists = BuildSljitExpressionSourceDataPointerHoists(expressions, hoist_registers, 1);
	}
	const auto hoisted_saved_register_count =
	    data_hoists.empty() ? GetSljitNativeVectorRegisterLayout().saved_register_count : hoist_frame.SavedCount();
	{
		vector<reference<const ExecutionExpressionIR>> expressions {predicate};
		for (auto &payload : payloads) {
			if (payload.expression_tree) {
				expressions.push_back(*payload.expression_tree);
			}
		}
		AppendSljitTypedExpressionTreeConstantHoists(expressions, data_hoists, local_size);
		AppendSljitTypedExpressionTreeStringCompareHoists(expressions, data_hoists, local_size);
		vector<idx_t> sources;
		for (auto &expression : expressions) {
			CollectSljitTypedExpressionTreeReferences(expression.get(), sources);
		}
		AppendSljitTypedExpressionTreeSelectionHoists(sources, data_hoists, local_size);
	}
	const auto logical_selection_offset = local_size;
	local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto hoists = data_hoists.empty() ? nullptr : &data_hoists;
	// The loops that call no helper keep each payload's running sum in a frame register past the five every emitter
	// uses, else a stack slot of its own: a register spares the row the store-to-load chain on the previous row's
	// update. Each loop zeroes them on entry and adds them into the payloads' slots on its exits. A sum's count only
	// tells whether it saw a value: a sum that adds every row the loop accepts reads the loop's row count, which
	// COUNT(*) counts too, and a sum that may skip a row (a NULL value) sets its bit in one presence mask. A COUNT is a
	// sum without a value whose count is its result: one that may skip a row counts in a register of its own. A guarded
	// payload CASE WHEN c THEN v END (SljitTypedExpressionTreeIsFastGuardedValue) takes no branch on rows without
	// NULLs: its count adds c, and its sum adds v masked by c, kept in a free register or else a stack slot. A DOUBLE
	// sum's float register starts from the stack slot's running value and stores back to it, so its rows add in source
	// order
	struct AccumulatorRegisters {
		//! the loop's row count, for COUNT(*) and a sum that adds every accepted row, or a COUNT's own count
		SljitOperand count {};
		bool own_count = false;
		//! the presence mask and the sum's bit in it, for a sum that may skip a row
		SljitOperand presence {};
		sljit_sw presence_bit = 0;
		SljitOperand lower {};
		SljitOperand upper {};
		//! a DOUBLE sum's float register
		sljit_s32 double_lower = 0;
		bool guarded = false;
	};
	// FR0 and FR1 hold the trees' DOUBLE operands
	static constexpr sljit_s32 FIRST_DOUBLE_ACCUMULATOR = 2;
	static constexpr idx_t MAX_DOUBLE_ACCUMULATORS = 4;
	struct AccumulatorLayout {
		vector<AccumulatorRegisters> payloads;
		SljitOperand row_count {};
		//! the mask a guarded sum's value is kept by
		SljitOperand guard {};
		bool fits = false;
		sljit_s32 scratch_register_count = 0;
		sljit_s32 saved_register_count = 0;
		sljit_sw local_size = 0;
	};
	auto plan_accumulator_layout = [&](bool sums_skip_null_rows, bool guard_payloads) {
		AccumulatorLayout layout;
		layout.payloads.resize(payloads.size());
		if (!codegen_plan.fast_path_supported) {
			return layout;
		}
		if (double_sum_count > MAX_DOUBLE_ACCUMULATORS) {
			return layout;
		}
		auto may_skip = [&](idx_t payload_idx) {
			return codegen_plan.payload_descriptors[payload_idx].primitive_kind !=
			           AggregatePrimitiveUpdateKind::COUNT_STAR &&
			       (sums_skip_null_rows || !codegen_plan.payloads[payload_idx].fast_path.fast_path_supported);
		};
		auto is_count = [&](idx_t payload_idx) {
			return codegen_plan.payload_descriptors[payload_idx].primitive_kind == AggregatePrimitiveUpdateKind::COUNT;
		};
		// only a count and a 64-bit sum are cheap enough to update on every row (a DOUBLE sum also keeps its branch:
		// adding a masked +0.0 would turn a -0.0 sum into +0.0). The rows take no branch only when no payload keeps
		// one: a payload that still branches predicts its condition from the others' branch history
		auto guardable = [&](idx_t payload_idx) {
			auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
			auto &tree = payloads[payload_idx].expression_tree;
			return (kind == AggregatePrimitiveUpdateKind::COUNT || kind == AggregatePrimitiveUpdateKind::SUM_INT64) &&
			       tree && SljitTypedExpressionTreeIsFastGuardedValue(*tree);
		};
		bool any_row_counted = false;
		bool any_presence = false;
		bool guard_rows = guard_payloads;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			any_presence = any_presence || (may_skip(payload_idx) && !is_count(payload_idx));
			any_row_counted = any_row_counted || !may_skip(payload_idx);
			guard_rows = guard_rows && (!may_skip(payload_idx) || guardable(payload_idx));
		}
		SljitKernelFrame frame(5, hoisted_saved_register_count);
		frame.ReserveLocal(local_size);
		sljit_s32 next_float_register = FIRST_DOUBLE_ACCUMULATOR;
		layout.row_count = any_row_counted ? frame.AllocateCarried() : SljitOperand {};
		const auto presence_mask = any_presence ? frame.AllocateCarried() : SljitOperand {};
		sljit_sw next_presence_bit = 0;
		bool guarded_presence = false;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
			auto &accumulator = layout.payloads[payload_idx];
			if (!may_skip(payload_idx)) {
				accumulator.count = layout.row_count;
			} else if (is_count(payload_idx)) {
				accumulator.count = frame.AllocateCarried();
				accumulator.own_count = true;
			} else {
				accumulator.presence = presence_mask;
				accumulator.presence_bit = next_presence_bit++;
			}
			if (kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				accumulator.double_lower = SLJIT_FR(next_float_register++);
			} else if (kind != AggregatePrimitiveUpdateKind::COUNT_STAR &&
			           kind != AggregatePrimitiveUpdateKind::COUNT) {
				accumulator.lower = frame.AllocateCarried();
			}
			if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				accumulator.upper = frame.AllocateCarried();
			}
			accumulator.guarded = guard_rows && may_skip(payload_idx);
			guarded_presence = guarded_presence || (accumulator.guarded && accumulator.presence.operand);
		}
		if (guarded_presence) {
			layout.guard = frame.AllocateCarried();
		}
		layout.fits = next_presence_bit < sljit_sw(8 * sizeof(sljit_sw));
		layout.scratch_register_count = frame.ScratchCount();
		layout.saved_register_count = frame.SavedCount();
		layout.local_size = frame.LocalSize();
		return layout;
	};
	const auto row_accumulators = plan_accumulator_layout(false, true);
	const auto null_row_accumulators = plan_accumulator_layout(true, false);
	auto saved_register_count = hoisted_saved_register_count;
	sljit_s32 accumulator_scratch_register_count = 5;
	for (auto layout : {&row_accumulators, &null_row_accumulators}) {
		if (layout->fits) {
			accumulator_scratch_register_count =
			    MaxValue(accumulator_scratch_register_count, layout->scratch_register_count);
			saved_register_count = MaxValue(saved_register_count, layout->saved_register_count);
			local_size = MaxValue(local_size, layout->local_size);
		}
	}
	if (row_accumulators.fits || null_row_accumulators.fits) {
		simd_scratches = (simd_scratches & ~sljit_s32(0xff)) | accumulator_scratch_register_count;
		if (double_sum_count != 0) {
			simd_scratches = (simd_scratches & ~sljit_s32(0xff00)) |
			                 SLJIT_ENTER_FLOAT(FIRST_DOUBLE_ACCUMULATOR + NumericCast<sljit_s32>(double_sum_count));
		}
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(simd_scratches),
	                 saved_register_count, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], SLJIT_IMM, 0);
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			EmitUngroupedAggregateLoadDoubleSum(compiler, payload_idx, local_sum_offsets[payload_idx]);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
		} else if (local_sum_offsets[payload_idx] >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
			if (local_sum_upper_offsets[payload_idx] >= 0) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx],
				               SLJIT_IMM, 0);
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
		}
	}
	EmitInitSljitNativeVectorSourceArrays(compiler);
	for (auto &hoist : data_hoists) {
		if (hoist.source_index != DConstants::INVALID_INDEX) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.location.operand, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	}
	EmitSljitTypedExpressionTreeConstantHoists(compiler, data_hoists);

	vector<SljitArithmeticOverflowSite> overflows;
	// The registers the loop being emitted keeps its accumulators in, or none: it zeroes them on entry and adds them
	// into the stack slots on every exit (end_register_accumulators)
	optional_ptr<const AccumulatorLayout> accumulators;
	auto emit_count = [&](idx_t payload_idx) {
		if (!accumulators) {
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
			return;
		}
		auto &accumulator = accumulators->payloads[payload_idx];
		if (accumulator.presence.operand) {
			auto &presence = accumulator.presence;
			sljit_emit_op2(compiler, SLJIT_OR, presence.operand, presence.operand_w, presence.operand,
			               presence.operand_w, SLJIT_IMM, sljit_sw(1) << accumulator.presence_bit);
		} else if (accumulator.own_count) {
			auto &count = accumulator.count;
			sljit_emit_op2(compiler, SLJIT_ADD, count.operand, count.operand_w, count.operand, count.operand_w,
			               SLJIT_IMM, 1);
		}
	};
	// a row the loop accepts: the row count counts it for COUNT(*) and every sum that adds it
	auto emit_row_accepted = [&]() {
		if (accumulators && accumulators->row_count.operand) {
			auto &count = accumulators->row_count;
			sljit_emit_op2(compiler, SLJIT_ADD, count.operand, count.operand_w, count.operand, count.operand_w,
			               SLJIT_IMM, 1);
		}
	};
	// adds the value in R2 to the payload's sum
	auto emit_add_value = [&](idx_t payload_idx) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		if (!accumulators) {
			EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                 SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			// the carry pair works on registers: a half in a stack slot is loaded into R0 or R1 and stored back
			auto &accumulator = accumulators->payloads[payload_idx];
			const auto lower = SljitKernelFrameRegister(compiler, accumulator.lower, SLJIT_R0);
			const auto upper = SljitKernelFrameRegister(compiler, accumulator.upper, SLJIT_R1);
			EmitUngroupedAggregateAccumulateHugeintInt64Regs(compiler, lower, upper, SLJIT_R2);
			for (auto half : {std::make_pair(accumulator.lower, lower), std::make_pair(accumulator.upper, upper)}) {
				if (half.first.operand & SLJIT_MEM) {
					sljit_emit_op1(compiler, SLJIT_MOV, half.first.operand, half.first.operand_w, half.second, 0);
				}
			}
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			auto &accumulator = accumulators->payloads[payload_idx];
			sljit_emit_fcopy(compiler, SLJIT_COPY_TO_F64, SLJIT_FR0, SLJIT_R2);
			sljit_emit_fop2(compiler, SLJIT_ADD_F64, accumulator.double_lower, 0, accumulator.double_lower, 0,
			                SLJIT_FR0, 0);
		} else {
			auto &lower = accumulators->payloads[payload_idx].lower;
			sljit_emit_op2(compiler, SLJIT_ADD, lower.operand, lower.operand_w, lower.operand, lower.operand_w,
			               SLJIT_R2, 0);
		}
	};
	// a payload's value in R2 updates its aggregate: a sum adds and counts it, a COUNT only counts it
	auto emit_accumulate = [&](idx_t payload_idx) {
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind != AggregatePrimitiveUpdateKind::COUNT) {
			emit_add_value(payload_idx);
		}
		emit_count(payload_idx);
	};
	// a guarded payload's row in registers, without a branch: its count adds the guard, its sum the value the guard
	// keeps
	auto emit_guarded_accumulate = [&](idx_t payload_idx, SljitTypedExpressionTreeFastIndexMode index_mode) {
		auto &accumulator = accumulators->payloads[payload_idx];
		auto &tree = *payloads[payload_idx].expression_tree;
		idx_t spill_index = 0;
		EmitSljitTypedExpressionTreeFastGuardReg(compiler, tree, spill_index, overflows, index_mode, hoists);
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
			auto &count = accumulator.count;
			sljit_emit_op2(compiler, SLJIT_ADD, count.operand, count.operand_w, count.operand, count.operand_w,
			               SLJIT_R2, 0);
			return;
		}
		auto &guard = accumulators->guard;
		auto &presence = accumulator.presence;
		D_ASSERT(guard.operand && presence.operand);
		sljit_emit_op2(compiler, SLJIT_SUB, guard.operand, guard.operand_w, SLJIT_IMM, 0, SLJIT_R2, 0);
		if (accumulator.presence_bit != 0) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, accumulator.presence_bit);
		}
		sljit_emit_op2(compiler, SLJIT_OR, presence.operand, presence.operand_w, presence.operand, presence.operand_w,
		               SLJIT_R2, 0);
		spill_index = 0;
		EmitSljitTypedExpressionTreeFastGuardedValueReg(compiler, tree, spill_index, overflows, index_mode, hoists);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R2, 0, guard.operand, guard.operand_w);
		emit_add_value(payload_idx);
	};
	auto begin_register_accumulators = [&](bool null_rows) {
		auto &layout = null_rows ? null_row_accumulators : row_accumulators;
		if (!layout.fits) {
			return;
		}
		accumulators = &layout;
		auto zero = [&](const SljitOperand &value) {
			if (value.operand) {
				sljit_emit_op1(compiler, SLJIT_MOV, value.operand, value.operand_w, SLJIT_IMM, 0);
			}
		};
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &accumulator = layout.payloads[payload_idx];
			if (accumulator.double_lower) {
				sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, accumulator.double_lower,
				                SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx]);
			}
			zero(accumulator.lower);
			zero(accumulator.upper);
			if (accumulator.own_count) {
				zero(accumulator.count);
			}
			zero(accumulator.presence);
		}
		zero(layout.row_count);
	};
	// the loop's exits add its registers into the stack slots, then leave through the returned jump
	vector<sljit_jump *> register_flush_exits;
	auto end_register_accumulators = [&](const vector<sljit_jump *> &exits) {
		if (!accumulators) {
			register_flush_exits.insert(register_flush_exits.end(), exits.begin(), exits.end());
			return;
		}
		auto &layout = *accumulators;
		accumulators = nullptr;
		SetSljitJumpLabels(exits, sljit_emit_label(compiler));
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &accumulator = layout.payloads[payload_idx];
			if (accumulator.presence.operand) {
				// a sum's count only tells whether it saw a value: its presence bit
				auto &presence = accumulator.presence;
				sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R0, 0, presence.operand, presence.operand_w, SLJIT_IMM,
				               accumulator.presence_bit);
				sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], SLJIT_R0, 0);
			} else {
				auto &count = accumulator.count;
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], count.operand, count.operand_w);
			}
			if (accumulator.double_lower) {
				sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32,
				                accumulator.double_lower, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx]);
				continue;
			}
			auto &lower = accumulator.lower;
			if (!lower.operand) {
				continue;
			}
			if (!accumulator.upper.operand) {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], lower.operand, lower.operand_w);
				continue;
			}
			// the halves in registers, a half in a stack slot loaded into R2 or R3
			const auto lower_reg = SljitKernelFrameRegister(compiler, lower, SLJIT_R2);
			const auto upper_reg = SljitKernelFrameRegister(compiler, accumulator.upper, SLJIT_R3);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx]);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP),
			               local_sum_upper_offsets[payload_idx]);
			EmitSljitAddDoubleWord(compiler, SLJIT_R0, SLJIT_R1, lower_reg, upper_reg);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_R0, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx], SLJIT_R1,
			               0);
		}
		register_flush_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	};
	// One row evaluated with SQL's three-valued logic, each value and its validity in a stack slot: a NULL predicate
	// rejects the row and a NULL payload adds nothing
	auto emit_generic_row = [&]() {
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
			if (kind != AggregatePrimitiveUpdateKind::COUNT) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
				EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
				                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
				                                 SLJIT_R2);
			}
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
	};

	// the sources a row reads: the predicate's and the sums'
	vector<idx_t> row_source_refs;
	CollectSljitTypedExpressionTreeReferences(predicate, row_source_refs);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			CollectSljitTypedExpressionTreeReferences(*payloads[payload_idx].expression_tree, row_source_refs);
		}
	}
	// A predicate that decides NULL as FALSE and sums of null-strict payloads run batches with NULLs on the fast loops:
	// each leaf and each sum checks its own sources
	bool null_checked = codegen_plan.fast_path_supported && SljitTypedExpressionTreeNullDecidesFalse(predicate);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			null_checked = null_checked && SljitTypedExpressionTreeIsNullStrict(*payloads[payload_idx].expression_tree);
		}
	}
	fast_null_rows = null_checked;
	auto emit_payload_null_jumps = [&](idx_t payload_idx, SljitTypedExpressionTreeFastIndexMode index_mode) {
		vector<idx_t> sources;
		CollectSljitTypedExpressionTreeReferences(*payloads[payload_idx].expression_tree, sources);
		vector<sljit_jump *> null_jumps;
		for (auto source_index : sources) {
			null_jumps.push_back(
			    EmitJumpIfSljitTypedExpressionTreeFastSourceNull(compiler, source_index, index_mode, hoists));
		}
		return null_jumps;
	};
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		begin_register_accumulators(false);

		// The scalar per-row payload updates, shared by the scalar fast loop and the
		// hybrid packed-mask loop (which runs them once per matching lane).
		auto emit_row_payload_updates = [&](bool null_rows) {
			emit_row_accepted();
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
				if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					emit_count(payload_idx);
					continue;
				}
				if (!null_rows && accumulators && accumulators->payloads[payload_idx].guarded) {
					emit_guarded_accumulate(payload_idx, SljitTypedExpressionTreeFastIndexMode::FLAT);
					continue;
				}
				vector<sljit_jump *> null_jumps;
				if (null_rows) {
					null_jumps = emit_payload_null_jumps(payload_idx, SljitTypedExpressionTreeFastIndexMode::FLAT);
				}
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeFastValueReg(compiler, *payloads[payload_idx].expression_tree,
				                                         payload_spill_index, overflows, hoists, &null_jumps);
				emit_accumulate(payload_idx);
				SetSljitJumpLabels(null_jumps, sljit_emit_label(compiler));
			}
		};
		auto emit_payload_updates = [&]() {
			emit_row_payload_updates(false);
		};

		if (simd_is_sum) {
			// Sum full lane groups with packed SIMD (masked payload widened via SADALP);
			// the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdSumLoop(compiler, predicate, *payloads[sum_payload_idx].expression_tree,
			                                        simd_plan, local_sum_offsets[sum_payload_idx], simd_count_offsets,
			                                        saw_value_offsets[sum_payload_idx], simd_mask_offset, nullptr,
			                                        hoists);
		} else if (simd_is_hybrid) {
			// Packed predicate mask; the payload updates stay scalar per matching lane.
			EmitSljitTypedExpressionTreeSimdHybridFilterLoop(compiler, predicate, simd_plan, simd_mask_offset,
			                                                 emit_payload_updates, nullptr, hoists);
		} else if (simd_plan.supported) {
			// Count full lane groups with packed SIMD; S1 is left at the tail start
			// and the scalar fast loop below finishes the < lanes remainder.
			EmitSljitTypedExpressionTreeSimdCountLoop(compiler, predicate, simd_plan, local_count_offsets[0],
			                                          simd_mask_offset, nullptr, hoists);
		}
		end_register_accumulators(
		    EmitSljitFlatPredicateLoop(compiler, predicate, overflows, hoists, emit_payload_updates));

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));

		// Flat batches that MAY contain NULLs run the same packed loops with a
		// lane-expanded validity mask ANDead into the predicate mask; the scalar
		// tail pre-checks the referenced sources per row (a null row cannot pass
		// an AND-only predicate, and a single payload's sources are covered too).
		if (simd_nullable_ok) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
			auto skip_nullable_simd = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			begin_register_accumulators(false);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			if (simd_is_sum) {
				EmitSljitTypedExpressionTreeSimdSumLoop(compiler, predicate, *payloads[sum_payload_idx].expression_tree,
				                                        simd_plan, local_sum_offsets[sum_payload_idx],
				                                        simd_count_offsets, saw_value_offsets[sum_payload_idx],
				                                        simd_mask_offset, &simd_validity_refs, hoists);
			} else if (simd_is_hybrid) {
				EmitSljitTypedExpressionTreeSimdHybridFilterLoop(compiler, predicate, simd_plan, simd_mask_offset,
				                                                 emit_payload_updates, &simd_validity_refs, hoists);
			} else {
				EmitSljitTypedExpressionTreeSimdCountLoop(compiler, predicate, simd_plan, local_count_offsets[0],
				                                          simd_mask_offset, &simd_validity_refs, hoists);
			}
			// Scalar tail for the remaining < lanes rows: skip any row with a NULL
			// referenced source, then run the shared predicate + payload blocks.
			auto nullable_tail_done = EmitSljitSourcesCheckedPredicateLoop(compiler, predicate, simd_validity_refs,
			                                                               overflows, hoists, emit_payload_updates);
			end_register_accumulators({nullable_tail_done});
			sljit_set_label(skip_nullable_simd, sljit_emit_label(compiler));
		} else if (null_checked) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
			auto skip_flat_nullable = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			begin_register_accumulators(true);
			auto null_checked_exits = EmitSljitFlatNullablePredicateLoop(
			    compiler, &predicate, overflows, hoists, []() {}, [&]() { emit_row_payload_updates(true); });
			end_register_accumulators(null_checked_exits);
			sljit_set_label(skip_flat_nullable, sljit_emit_label(compiler));
		} else {
			// A flat batch with NULLs runs a row whose sources are all valid on the fast path and only a row with a
			// NULL on the generic path
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
			vector<sljit_jump *> skip_hybrid;
			skip_hybrid.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
			skip_hybrid.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_S2, 0, SLJIT_IMM,
			                                     NumericCast<sljit_sw>(STANDARD_VECTOR_SIZE)));
			// combined[word] = the AND of every source's validity word; a source without a mask is all valid
			sljit_get_local_base(compiler, SLJIT_R3, 0, combined_validity_offset);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
			auto combine_loop = sljit_emit_label(compiler);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_S2, 0, SLJIT_IMM, ValidityMask::BITS_PER_VALUE - 1);
			sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, 6);
			auto combine_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_R1, 0, SLJIT_R4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, -1);
			for (auto source_index : row_source_refs) {
				EmitLoadSljitNativeSourceValidity(compiler, source_index, SLJIT_R0);
				auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
				sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3);
				sljit_set_label(source_all_valid, sljit_emit_label(compiler));
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R3, SLJIT_R1), 3, SLJIT_R2, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
			sljit_set_label(sljit_emit_jump(compiler, SLJIT_JUMP), combine_loop);
			sljit_set_label(combine_done, sljit_emit_label(compiler));

			// the fast rows add to the registers, a row with a NULL adds to the stack slots on the generic path
			begin_register_accumulators(false);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			end_register_accumulators(EmitSljitRowLoop(compiler, [&]() {
				sljit_get_local_base(compiler, SLJIT_R3, 0, combined_validity_offset);
				vector<sljit_jump *> source_null_jumps {
				    EmitJumpIfSljitValidityBitClear(compiler, SLJIT_R3, SLJIT_S1, SLJIT_R0)};
				idx_t hybrid_spill_index = 0;
				auto hybrid_predicate_false =
				    EmitSljitTypedExpressionTreeFastJumps(compiler, predicate, false, hybrid_spill_index, overflows,
				                                          SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
				emit_payload_updates();
				auto hybrid_row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
				SetSljitJumpLabels(source_null_jumps, sljit_emit_label(compiler));
				emit_generic_row();
				auto hybrid_next = sljit_emit_label(compiler);
				SetSljitJumpLabels(hybrid_predicate_false, hybrid_next);
				sljit_set_label(hybrid_row_done, hybrid_next);
			}));
			SetSljitJumpLabels(skip_hybrid, sljit_emit_label(compiler));
		}

		// A row read through the batch's selection: at its logical row when no source has a selection of its own
		// (logical), or through the selection hoisted for each source (selected)
		auto emit_selected_fast_row = [&](SljitTypedExpressionTreeFastIndexMode index_mode, bool null_rows) {
			idx_t selected_predicate_spill_index = 0;
			auto selected_predicate_false =
			    null_rows
			        ? EmitSljitTypedExpressionTreeNullableFastJumps(
			              compiler, predicate, false, selected_predicate_spill_index, overflows, index_mode, hoists)
			        : EmitSljitTypedExpressionTreeFastJumps(compiler, predicate, false, selected_predicate_spill_index,
			                                                overflows, index_mode, hoists);
			emit_row_accepted();
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
				if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					emit_count(payload_idx);
					continue;
				}
				if (!null_rows && accumulators && accumulators->payloads[payload_idx].guarded) {
					emit_guarded_accumulate(payload_idx, index_mode);
					continue;
				}
				vector<sljit_jump *> null_jumps;
				if (null_rows) {
					null_jumps = emit_payload_null_jumps(payload_idx, index_mode);
				}
				idx_t payload_spill_index = 0;
				if (index_mode == SljitTypedExpressionTreeFastIndexMode::LOGICAL) {
					EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, *payloads[payload_idx].expression_tree,
					                                                payload_spill_index, overflows, hoists,
					                                                &null_jumps);
				} else {
					EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payloads[payload_idx].expression_tree,
					                                                 payload_spill_index, overflows, hoists,
					                                                 &null_jumps);
				}
				emit_accumulate(payload_idx);
				SetSljitJumpLabels(null_jumps, sljit_emit_label(compiler));
			}
			SetSljitJumpLabels(selected_predicate_false, sljit_emit_label(compiler));
		};

		EmitSljitLogicalSelectionHoist(compiler, logical_selection_offset);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_logical_all_valid));
		auto not_logical = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		begin_register_accumulators(false);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		end_register_accumulators(EmitSljitRowLoop(compiler, [&]() {
			EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
			emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::LOGICAL, false);
		}));

		sljit_set_label(not_logical, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		begin_register_accumulators(false);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		end_register_accumulators(EmitSljitRowLoop(compiler, [&]() {
			EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
			emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::SELECTED, false);
		}));

		// A selected batch with NULLs runs a row whose sources are all valid on the selected fast path and only a
		// row with a NULL on the generic path
		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		begin_register_accumulators(null_checked);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		end_register_accumulators(EmitSljitRowLoop(compiler, [&]() {
			EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
			if (null_checked) {
				emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::SELECTED, true);
				return;
			}
			vector<sljit_jump *> selected_null_jumps;
			for (auto source_index : row_source_refs) {
				selected_null_jumps.push_back(EmitJumpIfSljitTypedExpressionTreeFastSourceNull(
				    compiler, source_index, SljitTypedExpressionTreeFastIndexMode::SELECTED, hoists));
			}
			emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::SELECTED, false);
			auto selected_hybrid_row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			SetSljitJumpLabels(selected_null_jumps, sljit_emit_label(compiler));
			emit_generic_row();
			sljit_set_label(selected_hybrid_row_done, sljit_emit_label(compiler));
		}));
	}

	auto generic_exits = EmitSljitRowLoop(compiler, emit_generic_row);

	auto helper_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(register_flush_exits, done_label);
	SetSljitJumpLabels(generic_exits, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx]);
		if (saw_value_offsets[payload_idx] >= 0) {
			// a sum saw a value exactly when it counted one; the register loops count without marking
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_R2, 0);
		}
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR || kind == AggregatePrimitiveUpdateKind::COUNT) {
			EmitUngroupedAggregateCommitCount(compiler, payload_idx, SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitUngroupedAggregateCommitSumHugeint(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                       local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                       SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			EmitUngroupedAggregateCommitSumDouble(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                      saw_value_offsets[payload_idx], SLJIT_R2);
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

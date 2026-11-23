#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_aggregate_source_hoist_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
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
	const auto &registers = GetSljitUngroupedAggregateRegisterLayout();
	if (registers.has_source_data_hoists) {
		vector<reference<const ExecutionExpressionIR>> expressions {predicate};
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (codegen_plan.payload_descriptors[payload_idx].primitive_kind !=
			    AggregatePrimitiveUpdateKind::COUNT_STAR) {
				expressions.push_back(*payloads[payload_idx].expression_tree);
			}
		}
		data_hoists = BuildSljitExpressionSourceDataPointerHoists(
		    expressions, {registers.source_data[0], registers.source_data[1]}, 1);
	}
	const bool hoists_source_data = !data_hoists.empty();
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
	// The loops that call no helper keep each payload's running sum in scratch registers past the five every emitter
	// uses: a stack slot per row would chain each row's update on the previous row's store. A sum's count only tells
	// whether it saw a value: a sum that adds every row the loop accepts reads the loop's row count, which COUNT(*)
	// counts too, and a sum that may skip a row (a NULL value) sets its bit in one presence mask.
	// A DOUBLE sum's float register starts from the stack slot's running value and stores back to it, so its rows add
	// in source order
	struct AccumulatorRegisters {
		//! the loop's row count, for COUNT(*) and a sum that adds every accepted row
		sljit_s32 count = 0;
		//! the presence mask and the sum's bit in it, for a sum that may skip a row
		sljit_s32 presence = 0;
		sljit_sw presence_bit = 0;
		sljit_s32 lower = 0;
		sljit_s32 upper = 0;
		bool is_double = false;
	};
	// FR0 and FR1 hold the trees' DOUBLE operands
	static constexpr sljit_s32 FIRST_DOUBLE_ACCUMULATOR = 2;
	static constexpr idx_t MAX_DOUBLE_ACCUMULATORS = 4;
	struct AccumulatorLayout {
		vector<AccumulatorRegisters> payloads;
		sljit_s32 row_count = 0;
		bool fits = false;
	};
	auto plan_accumulator_layout = [&](bool sums_skip_null_rows) {
		AccumulatorLayout layout;
		layout.payloads.resize(payloads.size());
		if (!registers.has_register_accumulators || !codegen_plan.fast_path_supported) {
			return layout;
		}
		if (double_sum_count > MAX_DOUBLE_ACCUMULATORS) {
			return layout;
		}
		auto sum_may_skip = [&](idx_t payload_idx) {
			return sums_skip_null_rows || !codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
		};
		bool any_row_counted = false;
		bool any_presence = false;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			const bool count_star = codegen_plan.payload_descriptors[payload_idx].primitive_kind ==
			                        AggregatePrimitiveUpdateKind::COUNT_STAR;
			const bool presence = !count_star && sum_may_skip(payload_idx);
			any_presence = any_presence || presence;
			any_row_counted = any_row_counted || !presence;
		}
		sljit_s32 next_register = 5;
		sljit_s32 next_float_register = FIRST_DOUBLE_ACCUMULATOR;
		layout.row_count = any_row_counted ? SLJIT_R(next_register++) : 0;
		const sljit_s32 presence_mask = any_presence ? SLJIT_R(next_register++) : 0;
		sljit_sw next_presence_bit = 0;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
			auto &accumulator = layout.payloads[payload_idx];
			if (kind != AggregatePrimitiveUpdateKind::COUNT_STAR && sum_may_skip(payload_idx)) {
				accumulator.presence = presence_mask;
				accumulator.presence_bit = next_presence_bit++;
			} else {
				accumulator.count = layout.row_count;
			}
			if (kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
				accumulator.lower = SLJIT_FR(next_float_register++);
				accumulator.is_double = true;
			} else if (kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
				accumulator.lower = SLJIT_R(next_register++);
			}
			if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				accumulator.upper = SLJIT_R(next_register++);
			}
		}
		layout.fits = next_register <= registers.accumulator_scratch_register_count &&
		              next_presence_bit < sljit_sw(8 * sizeof(sljit_sw));
		return layout;
	};
	const auto row_accumulators = plan_accumulator_layout(false);
	const auto null_row_accumulators = plan_accumulator_layout(true);
	const bool register_accumulators = row_accumulators.fits || null_row_accumulators.fits;
	if (register_accumulators) {
		simd_scratches = (simd_scratches & ~sljit_s32(0xff)) | registers.accumulator_scratch_register_count;
		if (double_sum_count != 0) {
			simd_scratches = (simd_scratches & ~sljit_s32(0xff00)) |
			                 SLJIT_ENTER_FLOAT(FIRST_DOUBLE_ACCUMULATOR + NumericCast<sljit_s32>(double_sum_count));
		}
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(simd_scratches),
	                 hoists_source_data ? registers.source_data_saved_register_count
	                                    : GetSljitNativeVectorRegisterLayout().saved_register_count,
	                 local_size);
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
		if (accumulator.presence) {
			sljit_emit_op2(compiler, SLJIT_OR, accumulator.presence, 0, accumulator.presence, 0, SLJIT_IMM,
			               sljit_sw(1) << accumulator.presence_bit);
		}
	};
	// a row the loop accepts: the row count counts it for COUNT(*) and every sum that adds it
	auto emit_row_accepted = [&]() {
		if (accumulators && accumulators->row_count) {
			auto count = accumulators->row_count;
			sljit_emit_op2(compiler, SLJIT_ADD, count, 0, count, 0, SLJIT_IMM, 1);
		}
	};
	// adds the value in R2 to the payload's sum and counts it
	auto emit_accumulate = [&](idx_t payload_idx) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		if (!accumulators) {
			EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                 SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			auto &accumulator = accumulators->payloads[payload_idx];
			EmitUngroupedAggregateAccumulateHugeintInt64Regs(compiler, accumulator.lower, accumulator.upper, SLJIT_R2);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
			auto &accumulator = accumulators->payloads[payload_idx];
			sljit_emit_fcopy(compiler, SLJIT_COPY_TO_F64, SLJIT_FR0, SLJIT_R2);
			sljit_emit_fop2(compiler, SLJIT_ADD_F64, accumulator.lower, 0, accumulator.lower, 0, SLJIT_FR0, 0);
		} else {
			auto &accumulator = accumulators->payloads[payload_idx];
			sljit_emit_op2(compiler, SLJIT_ADD, accumulator.lower, 0, accumulator.lower, 0, SLJIT_R2, 0);
		}
		emit_count(payload_idx);
	};
	auto begin_register_accumulators = [&](bool null_rows) {
		auto &layout = null_rows ? null_row_accumulators : row_accumulators;
		if (!layout.fits) {
			return;
		}
		accumulators = &layout;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &accumulator = layout.payloads[payload_idx];
			if (accumulator.is_double) {
				sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, accumulator.lower, SLJIT_MEM1(SLJIT_SP),
				                local_sum_offsets[payload_idx]);
			}
			for (auto reg : {accumulator.is_double ? 0 : accumulator.lower, accumulator.upper}) {
				if (reg) {
					sljit_emit_op1(compiler, SLJIT_MOV, reg, 0, SLJIT_IMM, 0);
				}
			}
			if (accumulator.presence) {
				sljit_emit_op1(compiler, SLJIT_MOV, accumulator.presence, 0, SLJIT_IMM, 0);
			}
		}
		if (layout.row_count) {
			sljit_emit_op1(compiler, SLJIT_MOV, layout.row_count, 0, SLJIT_IMM, 0);
		}
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
			if (accumulator.presence) {
				// a sum's count only tells whether it saw a value: its presence bit
				sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R0, 0, accumulator.presence, 0, SLJIT_IMM,
				               accumulator.presence_bit);
				sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], SLJIT_R0, 0);
			} else {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], accumulator.count, 0);
			}
			if (!accumulator.lower) {
				continue;
			}
			if (accumulator.is_double) {
				sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, accumulator.lower,
				                SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx]);
				continue;
			}
			if (!accumulator.upper) {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx],
				               SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], accumulator.lower, 0);
				continue;
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx]);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP),
			               local_sum_upper_offsets[payload_idx]);
			EmitSljitAddDoubleWord(compiler, SLJIT_R0, SLJIT_R1, accumulator.lower, accumulator.upper);
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
	struct sljit_jump *fast_done = nullptr;
	vector<sljit_jump *> fast_loop_exits;
	struct sljit_jump *selected_fast_done = nullptr;
	struct sljit_jump *selected_hybrid_done = nullptr;
	vector<sljit_jump *> null_checked_exits;
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
		// Both outcomes end in their own bottom-tested latch: a rejected row falls through to one and takes only the
		// back-edge branch, a matching row jumps to its payload updates and the other.
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		auto fast_loop = sljit_emit_label(compiler);
		idx_t predicate_spill_index = 0;
		auto predicate_true =
		    EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, predicate, predicate_spill_index, overflows, hoists);
		auto emit_latch = [&]() {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
			sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), fast_loop);
			fast_loop_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		};
		emit_latch();
		SetSljitJumpLabels(predicate_true, sljit_emit_label(compiler));
		emit_payload_updates();
		emit_latch();
		fast_loop_exits.push_back(fast_done);
		end_register_accumulators(fast_loop_exits);

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
			auto nullable_tail_loop = sljit_emit_label(compiler);
			auto nullable_tail_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
			vector<sljit_jump *> tail_null_jumps;
			for (auto source_index : simd_validity_refs) {
				tail_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
			}
			idx_t nullable_tail_spill_index = 0;
			auto nullable_tail_false =
			    EmitSljitTypedExpressionTreeFastJumps(compiler, predicate, false, nullable_tail_spill_index, overflows,
			                                          SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
			emit_payload_updates();
			auto nullable_tail_skip = sljit_emit_label(compiler);
			SetSljitJumpLabels(nullable_tail_false, nullable_tail_skip);
			for (auto null_jump : tail_null_jumps) {
				sljit_set_label(null_jump, nullable_tail_skip);
			}
			EmitNextSljitNativeVectorLoop(compiler, nullable_tail_loop);
			end_register_accumulators({nullable_tail_done});
			sljit_set_label(skip_nullable_simd, sljit_emit_label(compiler));
		} else if (null_checked) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
			auto skip_flat_nullable = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
			begin_register_accumulators(true);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			null_checked_exits.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0));
			auto flat_nullable_loop = sljit_emit_label(compiler);
			auto emit_latch = [&]() {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
				sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), flat_nullable_loop);
				null_checked_exits.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			};
			if (SljitTypedExpressionTreeEvaluatesAnyBits(predicate)) {
				// Forcing a NULL leaf FALSE only lowers an AND/OR predicate, so a row the predicate rejects on the
				// leftover values of its NULLs is rejected: it falls through to the back-edge, and only a row it
				// accepts tests its NULLs, out of line
				idx_t raw_spill_index = 0;
				auto raw_true = EmitSljitTypedExpressionTreeFastJumpsIfTrue(compiler, predicate, raw_spill_index,
				                                                            overflows, hoists);
				emit_latch();
				SetSljitJumpLabels(raw_true, sljit_emit_label(compiler));
			}
			idx_t flat_nullable_spill_index = 0;
			auto flat_nullable_false = EmitSljitTypedExpressionTreeNullableFastJumps(
			    compiler, predicate, false, flat_nullable_spill_index, overflows,
			    SljitTypedExpressionTreeFastIndexMode::FLAT, hoists);
			emit_row_payload_updates(true);
			SetSljitJumpLabels(flat_nullable_false, sljit_emit_label(compiler));
			emit_latch();
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
			auto hybrid_loop = sljit_emit_label(compiler);
			auto hybrid_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
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
			EmitNextSljitNativeVectorLoop(compiler, hybrid_loop);
			end_register_accumulators({hybrid_done});
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
		auto logical_fast_loop = sljit_emit_label(compiler);
		auto logical_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
		emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::LOGICAL, false);
		EmitNextSljitNativeVectorLoop(compiler, logical_fast_loop);
		end_register_accumulators({logical_fast_done});

		sljit_set_label(not_logical, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		begin_register_accumulators(false);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
		emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::SELECTED, false);
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);
		end_register_accumulators({selected_fast_done});

		// A selected batch with NULLs runs a row whose sources are all valid on the selected fast path and only a
		// row with a NULL on the generic path
		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		begin_register_accumulators(null_checked);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto selected_hybrid_loop = sljit_emit_label(compiler);
		selected_hybrid_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitHoistedLogicalIndex(compiler, logical_selection_offset);
		if (null_checked) {
			emit_selected_fast_row(SljitTypedExpressionTreeFastIndexMode::SELECTED, true);
		} else {
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
		}
		EmitNextSljitNativeVectorLoop(compiler, selected_hybrid_loop);
		end_register_accumulators({selected_hybrid_done});
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_generic_row();
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto helper_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(register_flush_exits, done_label);
	sljit_set_label(done, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = codegen_plan.payload_descriptors[payload_idx].primitive_kind;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx]);
		if (kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			// a sum saw a value exactly when it counted one; the register loops count without marking
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_R2, 0);
		}
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitUngroupedAggregateCommitCountStar(compiler, payload_idx, SLJIT_R2);
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

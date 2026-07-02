#include "sljit_native_codegen.hpp"

#include "sljit_arm64_neon_integer_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_flat_integer_projection_codegen.hpp"
#include "sljit_native_flat_loop_codegen.hpp"
#include "sljit_native_flat_projection_codegen.hpp"

#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFlatIntegerProjectionSharedSources(
    const vector<SljitNativeRegionExpressionPlan> &plans, const vector<idx_t> &projection_indices,
    const SljitFlatProjectionSharedSourcePlan &shared_plan, SljitNativeIntegerKind integer_kind,
    SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(integer_kind);
	auto load_op = NativeIntegerLoadOp(integer_kind);
	auto store_op = NativeIntegerStoreOp(integer_kind);
	bool use_simd = true;
	bool needs_overflow_handling = false;
	for (auto projection_index : projection_indices) {
		auto &plan = plans[projection_index];
		auto projection_needs_overflow_handling = plan.check_arithmetic_overflow || plan.check_result_range;
		needs_overflow_handling = needs_overflow_handling || projection_needs_overflow_handling;
		use_simd = use_simd && !projection_needs_overflow_handling &&
		           SljitArm64NeonIntegerBinarySupported(integer_kind, plan.binary_op);
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), use_simd ? 5 | SLJIT_ENTER_VECTOR(3) : 5,
	                 SljitFlatIntegerProjectionSavedRegisterCount(), 0);
	vector<SljitFlatIntegerProjectionOverflowJump> overflow_jumps;

	auto emit_scalar_sources = [&]() {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = SljitFlatProjectionSourcePointerRegister(source_idx);
			auto source_value_reg = SljitFlatIntegerProjectionSourceScalarRegister(source_idx);
			sljit_emit_op1(compiler, load_op, source_value_reg, 0, SLJIT_MEM1(source_pointer_reg), 0);
		}
	};
	auto emit_simd_sources = [&]() {
		auto simd_type = SljitArm64NeonIntegerSimdType(integer_kind);
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = SljitFlatProjectionSourcePointerRegister(source_idx);
			auto source_vector_reg = SljitFlatIntegerProjectionSourceVectorRegister(source_idx);
			sljit_emit_simd_mov(compiler, simd_type, source_vector_reg, SLJIT_MEM1(source_pointer_reg), 0);
		}
	};
	auto emit_scalar_projection = [&](idx_t projection_index, sljit_s32 result_pointer_reg) {
		auto &plan = plans[projection_index];
		auto left_source_idx =
		    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.source_index, "integer");
		auto left_reg = SljitFlatIntegerProjectionSourceScalarRegister(left_source_idx);
		struct sljit_jump *date_is_negative_infinity = nullptr;
		struct sljit_jump *date_is_positive_infinity = nullptr;
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(integer_kind)) {
			date_is_negative_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM,
			                                           SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
			date_is_positive_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM,
			                                           SLJIT_DATE_POSITIVE_INFINITY_DAYS);
		}
		sljit_s32 right_reg;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			auto right_source_idx =
			    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.right_source_index, "integer");
			right_reg = SljitFlatIntegerProjectionSourceScalarRegister(right_source_idx);
		} else {
			auto constant_offset = NumericCast<sljit_sw>(projection_index * sizeof(int64_t));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S2), constant_offset);
			right_reg = SLJIT_R1;
		}
		auto binary_op = NativeIntegerBinaryOp(integer_kind, plan.binary_op);
		auto emit_binary_op = plan.check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT && plan.constant_on_left) {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R4, 0, right_reg, 0, left_reg, 0);
		} else {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R4, 0, left_reg, 0, right_reg, 0);
		}
		if (plan.check_arithmetic_overflow) {
			overflow_jumps.push_back({projection_index, sljit_emit_jump(compiler, SLJIT_OVERFLOW)});
		}
		if (plan.check_result_range) {
			overflow_jumps.push_back({projection_index, sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R4, 0, SLJIT_IMM,
			                                                           NumericCast<sljit_sw>(plan.result_min))});
			overflow_jumps.push_back(
			    {projection_index, sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R4, 0, SLJIT_IMM,
			                                      NumericCast<sljit_sw>(plan.result_max))});
		}
		struct sljit_jump *arithmetic_done = nullptr;
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(integer_kind)) {
			arithmetic_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto date_infinity_label = sljit_emit_label(compiler);
			sljit_set_label(date_is_negative_infinity, date_infinity_label);
			sljit_set_label(date_is_positive_infinity, date_infinity_label);
			sljit_emit_op1(compiler, SLJIT_MOV32, SLJIT_R4, 0, left_reg, 0);
			sljit_set_label(arithmetic_done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(result_pointer_reg), 0, SLJIT_R4, 0);
	};
	auto emit_simd_projection = [&](idx_t projection_index, sljit_s32 result_pointer_reg) {
		auto &plan = plans[projection_index];
		auto simd_type = SljitArm64NeonIntegerSimdType(integer_kind);
		auto left_source_idx =
		    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.source_index, "integer");
		auto left_reg = SljitFlatIntegerProjectionSourceVectorRegister(left_source_idx);
		sljit_s32 right_reg;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			auto right_source_idx =
			    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.right_source_index, "integer");
			right_reg = SljitFlatIntegerProjectionSourceVectorRegister(right_source_idx);
		} else {
			auto constant_offset = NumericCast<sljit_sw>(projection_index * sizeof(int64_t));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S2), constant_offset);
			sljit_emit_simd_replicate(compiler, simd_type, SLJIT_VR2, SLJIT_R1, 0);
			right_reg = SLJIT_VR2;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT && plan.constant_on_left) {
			EmitSljitArm64NeonIntegerBinary(compiler, integer_kind, plan.binary_op, SLJIT_VR2, right_reg, left_reg);
		} else {
			EmitSljitArm64NeonIntegerBinary(compiler, integer_kind, plan.binary_op, SLJIT_VR2, left_reg, right_reg);
		}
		sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(result_pointer_reg), 0);
	};

	auto increment_source_pointers = [&](sljit_sw bytes) {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = SljitFlatProjectionSourcePointerRegister(source_idx);
			sljit_emit_op2(compiler, SLJIT_ADD, source_pointer_reg, 0, source_pointer_reg, 0, SLJIT_IMM, bytes);
		}
	};
	auto increment_result_pointers = [&](idx_t group_begin, idx_t group_end, sljit_sw bytes) {
		for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
			auto result_reg = SljitFlatIntegerProjectionResultPointerRegister(group_idx);
			sljit_emit_op2(compiler, SLJIT_ADD, result_reg, 0, result_reg, 0, SLJIT_IMM, bytes);
		}
	};
	const auto group_size = SljitFlatIntegerProjectionGroupSize();
	auto data_width = sljit_sw(1) << data_scale;
	auto simd_bytes = sljit_sw(16);
	for (idx_t group_begin = 0; group_begin < projection_indices.size(); group_begin += group_size) {
		auto group_end = MinValue<idx_t>(group_begin + group_size, projection_indices.size());
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constants));
		EmitSljitFlatProjectionLoadSharedSourcePointers(compiler, shared_plan, SLJIT_R0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data_array));
		for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
			auto projection_index = projection_indices[group_begin + group_idx];
			auto result_pointer_offset = SljitPointerArrayOffset(projection_index);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SljitFlatIntegerProjectionResultPointerRegister(group_idx), 0,
			               SLJIT_MEM1(SLJIT_R0), result_pointer_offset);
		}
		auto emit_scalar_row = [&]() {
			emit_scalar_sources();
			for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
				emit_scalar_projection(projection_indices[group_begin + group_idx],
				                       SljitFlatIntegerProjectionResultPointerRegister(group_idx));
			}
		};
		auto emit_advance = [&](sljit_sw bytes) {
			increment_source_pointers(bytes);
			increment_result_pointers(group_begin, group_end, bytes);
		};
		if (use_simd) {
			auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(integer_kind));
			auto emit_simd_row = [&]() {
				emit_simd_sources();
				for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
					emit_simd_projection(projection_indices[group_begin + group_idx],
					                     SljitFlatIntegerProjectionResultPointerRegister(group_idx));
				}
			};
			EmitSljitFlatSimdThenScalarTailLoop(compiler, SLJIT_S1, simd_lanes, simd_bytes, data_width, emit_simd_row,
			                                    emit_scalar_row, emit_advance);
		} else {
			EmitSljitFlatRemainingScalarLoop(compiler, SLJIT_S1, data_width, emit_scalar_row, emit_advance);
		}
	}
	auto success = sljit_emit_jump(compiler, SLJIT_JUMP);
	vector<struct sljit_jump *> overflow_returns;
	if (needs_overflow_handling) {
		EmitSljitFlatIntegerProjectionOverflowReturns(compiler, overflow_jumps, overflow_returns);
	}
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(success, done_label);
	for (auto overflow_return : overflow_returns) {
		sljit_set_label(overflow_return, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatIntegerProjection(const vector<SljitNativeRegionExpressionPlan> &plans,
                                      const vector<idx_t> &projection_indices, SljitNativeVectorFunction &function,
                                      string &error) {
	if (projection_indices.empty()) {
		error = "SLJIT flat integer projection has no expressions";
		return nullptr;
	}
	auto first_projection_index = projection_indices[0];
	if (first_projection_index >= plans.size()) {
		error = "SLJIT flat integer projection index is out of range";
		return nullptr;
	}
	auto integer_kind = plans[first_projection_index].integer_kind;
	for (auto projection_index : projection_indices) {
		if (projection_index >= plans.size()) {
			error = "SLJIT flat integer projection index is out of range";
			return nullptr;
		}
		auto &plan = plans[projection_index];
		if (!ValidateNativeFlatIntegerProjectionExpression(plan, integer_kind, error)) {
			return nullptr;
		}
	}

	SljitFlatProjectionSharedSourcePlan shared_source_plan;
	if (!TryPlanSljitFlatProjectionSharedSources(plans, projection_indices,
	                                             SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES, 1,
	                                             DConstants::INVALID_INDEX, shared_source_plan)) {
		error = "SLJIT flat integer projection only supports projections with at most two input sources";
		return nullptr;
	}
	return BuildSljitNativeFlatIntegerProjectionSharedSources(plans, projection_indices, shared_source_plan,
	                                                          integer_kind, function, error);
}

} // namespace duckdb

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_flat_double_codegen_helpers.hpp"
#include "sljit_native_flat_floating_stats_codegen.hpp"
#include "sljit_native_flat_projection_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static bool GetNativeFlatDoubleProjectionPrecision(const SljitNativeRegionExpressionPlan &plan, bool &single_precision,
                                                   string &error) {
	switch (plan.return_type.InternalType()) {
	case PhysicalType::FLOAT:
		single_precision = true;
		return true;
	case PhysicalType::DOUBLE:
		single_precision = false;
		return true;
	default:
		error = "SLJIT flat floating projection only supports FLOAT/DOUBLE result types";
		return false;
	}
}

static bool ValidateNativeFlatDoubleProjectionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                         bool single_precision, string &error) {
	bool plan_single_precision;
	if (!GetNativeFlatDoubleProjectionPrecision(plan, plan_single_precision, error)) {
		return false;
	}
	if (plan_single_precision != single_precision) {
		error = "SLJIT flat floating projection cannot mix FLOAT and DOUBLE outputs";
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		return ValidateNativeFlatDoubleBinarySource(plan.double_source_kind, single_precision, error);
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		return ValidateNativeFlatDoubleBinarySource(plan.double_source_kind, single_precision, error) &&
		       ValidateNativeFlatDoubleBinarySource(plan.double_right_source_kind, single_precision, error);
	default:
		error = "SLJIT flat floating projection only supports floating binary expressions";
		return false;
	}
}

static sljit_s32 SljitFlatDoubleProjectionSourceFloatRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_FR0;
	case 1:
		return SLJIT_FR1;
	default:
		throw InternalException("SLJIT flat floating projection source register is out of range");
	}
}

static unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFlatDoubleProjectionSharedSources(
    const vector<SljitNativeRegionExpressionPlan> &plans, const vector<idx_t> &projection_indices,
    const SljitFlatProjectionSharedSourcePlan &shared_plan, bool single_precision, SljitNativeVectorFunction &function,
    string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_scale = NativeDirectFloatingDataScale(single_precision);
	auto constant_width = NativeDirectFloatingDataWidth(single_precision);

	auto stats_float_register_count = NumericCast<sljit_s32>(4 + projection_indices.size() * 2);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(stats_float_register_count),
	                 SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	EmitSljitFlatProjectionLoadSharedSourcePointers(compiler, shared_plan, SLJIT_R2);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data_array));
	if (SLJIT_NATIVE_VECTOR_HAS_EXTRA_SAVED_REG) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_constants));
	}

	auto emit_sources = [&]() {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = SljitFlatProjectionSourcePointerRegister(source_idx);
			auto source_float_reg = SljitFlatDoubleProjectionSourceFloatRegister(source_idx);
			sljit_emit_fmem(compiler, move_op | fmem_align, source_float_reg, SLJIT_MEM2(source_pointer_reg, SLJIT_S1),
			                data_scale);
		}
	};
	auto emit_projection = [&](idx_t fused_idx, bool initialize_stats, bool collect_stats) {
		auto projection_index = projection_indices[fused_idx];
		auto &plan = plans[projection_index];
		auto left_source_idx =
		    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.source_index, "floating");
		auto left_reg = SljitFlatDoubleProjectionSourceFloatRegister(left_source_idx);
		sljit_s32 right_reg;
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			auto right_source_idx =
			    SljitFlatProjectionSourceRegisterIndex(shared_plan.sources, plan.right_source_index, "floating");
			right_reg = SljitFlatDoubleProjectionSourceFloatRegister(right_source_idx);
		} else {
			right_reg = SLJIT_FR3;
		}
		auto binary_op = NativeDoubleBinaryOp(plan.double_binary_op, single_precision);
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT) {
			auto constant_offset = NumericCast<sljit_sw>(projection_index * constant_width);
			auto constant_base = SLJIT_S6;
			if (!SLJIT_NATIVE_VECTOR_HAS_EXTRA_SAVED_REG) {
				constant_base = SLJIT_R4;
				sljit_emit_op1(compiler, SLJIT_MOV_P, constant_base, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeVectorInput, floating_constants));
			}
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR3, SLJIT_MEM1(constant_base), constant_offset);
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT && plan.constant_on_left) {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR2, 0, SLJIT_FR3, 0, left_reg, 0);
		} else {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR2, 0, left_reg, 0, right_reg, 0);
		}
		auto result_pointer_offset = SljitPointerArrayOffset(projection_index);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S5), result_pointer_offset);
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR2, SLJIT_MEM2(SLJIT_R4, SLJIT_S1),
		                data_scale);
		if (!collect_stats) {
			return;
		}
		auto min_reg = SljitFlatFloatingProjectionStatsMinRegister(fused_idx);
		auto max_reg = SljitFlatFloatingProjectionStatsMaxRegister(fused_idx);
		if (initialize_stats) {
			EmitSljitFlatFloatingStatsInit(compiler, move_op, SLJIT_FR2, min_reg, max_reg);
		} else {
			EmitSljitFlatFloatingStatsUpdate(compiler, move_op, single_precision, SLJIT_FR2, min_reg, max_reg);
		}
	};

	auto emit_increment = [&]() {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	};
	auto emit_projection_row = [&](bool initialize_stats, bool collect_stats) {
		emit_sources();
		for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
			emit_projection(fused_idx, initialize_stats, collect_stats);
		}
	};
	EmitSljitFlatFloatingOptionalStatsLoop(compiler, emit_projection_row, emit_increment, [&]() {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_stats_min));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_stats_max));
		for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
			auto min_reg = SljitFlatFloatingProjectionStatsMinRegister(fused_idx);
			auto max_reg = SljitFlatFloatingProjectionStatsMaxRegister(fused_idx);
			EmitSljitStoreFlatFloatingStats(compiler, projection_indices[fused_idx], single_precision, min_reg, max_reg,
			                                SLJIT_R2, SLJIT_R3);
		}
	});
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleProjection(const vector<SljitNativeRegionExpressionPlan> &plans,
                                     const vector<idx_t> &projection_indices, SljitNativeVectorFunction &function,
                                     string &error) {
	if (projection_indices.empty()) {
		error = "SLJIT flat floating projection has no expressions";
		return nullptr;
	}
	auto first_projection_index = projection_indices[0];
	if (first_projection_index >= plans.size()) {
		error = "SLJIT flat floating projection index is out of range";
		return nullptr;
	}
	bool single_precision;
	if (!GetNativeFlatDoubleProjectionPrecision(plans[first_projection_index], single_precision, error)) {
		return nullptr;
	}
	for (auto projection_index : projection_indices) {
		if (projection_index >= plans.size()) {
			error = "SLJIT flat floating projection index is out of range";
			return nullptr;
		}
		if (!ValidateNativeFlatDoubleProjectionExpression(plans[projection_index], single_precision, error)) {
			return nullptr;
		}
	}

	SljitFlatProjectionSharedSourcePlan shared_source_plan;
	static_assert(SLJIT_NUMBER_OF_FLOAT_REGISTERS >= 4,
	              "flat floating projection requires four scratch floating-point registers");
	const auto max_stats_projections =
	    MinValue<idx_t>(8, NumericCast<idx_t>((SLJIT_NUMBER_OF_FLOAT_REGISTERS - 4) / 2));
	if (TryPlanSljitFlatProjectionSharedSources(plans, projection_indices,
	                                            SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES, 2,
	                                            max_stats_projections, shared_source_plan)) {
		return BuildSljitNativeFlatDoubleProjectionSharedSources(plans, projection_indices, shared_source_plan,
		                                                         single_precision, function, error);
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_scale = NativeDirectFloatingDataScale(single_precision);
	auto constant_width = NativeDirectFloatingDataWidth(single_precision);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(4), SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT, 0);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data_array));
	if (SLJIT_NATIVE_VECTOR_HAS_EXTRA_SAVED_REG) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_constants));
	}

	for (auto projection_index : projection_indices) {
		auto &plan = plans[projection_index];
		auto projection_pointer_offset = SljitPointerArrayOffset(projection_index);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S3), projection_pointer_offset);
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S4), projection_pointer_offset);
		} else {
			auto constant_offset = NumericCast<sljit_sw>(projection_index * constant_width);
			auto constant_base = SLJIT_S6;
			if (!SLJIT_NATIVE_VECTOR_HAS_EXTRA_SAVED_REG) {
				constant_base = SLJIT_R4;
				sljit_emit_op1(compiler, SLJIT_MOV_P, constant_base, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeVectorInput, floating_constants));
			}
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR1, SLJIT_MEM1(constant_base), constant_offset);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S5), projection_pointer_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);

		auto emit_projection_row = [&]() {
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale);
			if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT) {
				auto binary_op = NativeDoubleBinaryOp(plan.double_binary_op, single_precision);
				if (plan.constant_on_left) {
					sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR1, 0, SLJIT_FR0, 0);
				} else {
					sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
				}
			} else {
				sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR1, SLJIT_MEM2(SLJIT_R1, SLJIT_S1), data_scale);
				auto binary_op = NativeDoubleBinaryOp(plan.double_binary_op, single_precision);
				sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
			}
			sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_FR0, SLJIT_MEM2(SLJIT_R2, SLJIT_S1),
			                data_scale);
		};

		auto emit_increment = [&]() {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		};
		auto emit_projection_with_optional_stats = [&](bool initialize_stats, bool collect_stats) {
			emit_projection_row();
			if (!collect_stats) {
				return;
			}
			if (initialize_stats) {
				EmitSljitFlatFloatingStatsInit(compiler, move_op, SLJIT_FR0, SLJIT_FR2, SLJIT_FR3);
			} else {
				EmitSljitFlatFloatingStatsUpdate(compiler, move_op, single_precision, SLJIT_FR0, SLJIT_FR2, SLJIT_FR3);
			}
		};
		EmitSljitFlatFloatingOptionalStatsLoop(compiler, emit_projection_with_optional_stats, emit_increment, [&]() {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, floating_stats_min));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, floating_stats_max));
			EmitSljitStoreFlatFloatingStats(compiler, projection_index, single_precision, SLJIT_FR2, SLJIT_FR3,
			                                SLJIT_R3, SLJIT_R4);
		});
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

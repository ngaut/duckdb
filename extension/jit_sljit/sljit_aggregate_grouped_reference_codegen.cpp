#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "sljitLir.h"

namespace duckdb {

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                    string &error, SljitNativeAggregateSumStateKind state_kind) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitGroupedAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3, load_op,
		                                   data_scale, SLJIT_R2);
		if (hugeint_state) {
			EmitSljitGroupedAggregateAccumulateHugeintInt64(compiler, SLJIT_S4, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateInt64(compiler, SLJIT_S4, SLJIT_R2);
		}
	});

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                         string &error) {
	return BuildSljitNativeGroupedSumReference(kind, function, error, SljitNativeAggregateSumStateKind::INT64);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumHugeintReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error) {
	return BuildSljitNativeGroupedSumReference(kind, function, error, SljitNativeAggregateSumStateKind::HUGEINT);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                       string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitGroupedAggregateLoop(compiler, [&]() {
		EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	});

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedCountReference(SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto source_has_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto all_valid_done = EmitSljitGroupedAggregateLoop(compiler, [&]() {
		EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	});
	auto finish = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_has_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto done = EmitSljitGroupedAggregateSelectedSourceLoop(compiler, [&]() {
		EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	});

	auto finish_label = sljit_emit_label(compiler);
	sljit_set_label(all_valid_done, finish_label);
	sljit_set_label(done, finish_label);
	sljit_set_label(finish, finish_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

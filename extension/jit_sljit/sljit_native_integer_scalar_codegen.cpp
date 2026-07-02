#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <exception>

namespace duckdb {

static void SLJIT_FUNC SljitNativeIntegerCastOverflow(SljitNativeVectorInput *input) {
	try {
		throw ConversionException(input->query_location, input->overflow_message, input->overflow_value);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerCast(SljitNativeSignedIntegerWidth source_width,
                                                                  SljitNativeSignedIntegerWidth target_width,
                                                                  bool try_cast, SljitNativeVectorFunction &function,
                                                                  string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeSignedIntegerDataScale(source_width);
	auto target_data_scale = NativeSignedIntegerDataScale(target_width);
	auto source_load_op = NativeSignedIntegerLoadOp(source_width);
	auto target_store_op = NativeSignedIntegerStoreOp(target_width);
	auto needs_range_check = NativeSignedIntegerCastNeedsRangeCheck(source_width, target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &invalid_jumps) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   source_load_op, source_data_scale, SLJIT_R2);

		if (needs_range_check) {
			range_too_small = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                 NumericCast<sljit_sw>(NativeSignedIntegerMin(target_width)));
			range_too_large = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                 NumericCast<sljit_sw>(NativeSignedIntegerMax(target_width)));
			if (try_cast) {
				invalid_jumps.push_back(range_too_small);
				invalid_jumps.push_back(range_too_large);
			}
		}

		EmitStoreSljitNativeFixedWidthResult(compiler, target_store_op, target_data_scale, SLJIT_R2);
	});

	if (needs_range_check && !try_cast) {
		auto overflow_label = sljit_emit_label(compiler);
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_value),
		               SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeIntegerCastOverflow));
	}

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeSignedToUnsignedIntegerCast(SljitNativeSignedIntegerWidth source_width,
                                            SljitNativeUnsignedIntegerWidth target_width, bool try_cast,
                                            SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeSignedIntegerDataScale(source_width);
	auto target_data_scale = NativeUnsignedIntegerDataScale(target_width);
	auto source_load_op = NativeSignedIntegerLoadOp(source_width);
	auto target_store_op = NativeUnsignedIntegerStoreOp(target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &invalid_jumps) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   source_load_op, source_data_scale, SLJIT_R2);

		range_too_small = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, 0);
		range_too_large = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                  NumericCast<sljit_sw>(NativeUnsignedIntegerMax(target_width)));
		if (try_cast) {
			invalid_jumps.push_back(range_too_small);
			invalid_jumps.push_back(range_too_large);
		}

		EmitStoreSljitNativeFixedWidthResult(compiler, target_store_op, target_data_scale, SLJIT_R2);
	});

	if (!try_cast) {
		auto overflow_label = sljit_emit_label(compiler);
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_value),
		               SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeIntegerCastOverflow));
	}

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerCoalesce(SljitNativeSignedIntegerWidth width, SljitNativeCoalesceRhsKind rhs_kind,
                                bool rhs_constant_is_null, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeSignedIntegerDataScale(width);
	auto load_op = NativeSignedIntegerLoadOp(width);
	auto store_op = NativeSignedIntegerStoreOp(width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitInvalidResultLoop(
	    compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &next_jumps) {
		    EmitLoadLogicalIndex(compiler, SLJIT_R1);
		    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
		    auto source_is_null =
		        EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);

		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                                       load_op, data_scale, SLJIT_R2);
		    EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, SLJIT_R2);
		    next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

		    sljit_set_label(source_is_null, sljit_emit_label(compiler));
		    if (rhs_kind == SljitNativeCoalesceRhsKind::CONSTANT) {
			    if (rhs_constant_is_null) {
				    invalid_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			    } else {
				    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
				                   offsetof(SljitNativeVectorInput, constant));
				    EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, SLJIT_R2);
			    }
		    } else {
			    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1,
			                        SLJIT_S4);
			    invalid_jumps.push_back(
			        EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity),
			                               SLJIT_S4));
			    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data),
			                                       SLJIT_S4, load_op, data_scale, SLJIT_R2);
			    EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, SLJIT_R2);
		    }
	    });

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

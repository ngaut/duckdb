#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/string_vector.hpp"

#include "sljitLir.h"

#include <cstddef>
#include <cstring>
#include <exception>

namespace duckdb {

static constexpr sljit_sw SLJIT_DATE_NEGATIVE_INFINITY_DAYS = -2147483647;
static constexpr sljit_sw SLJIT_DATE_POSITIVE_INFINITY_DAYS = 2147483647;

static bool SljitNativeIntegerKindPreservesSourceDateInfinity(SljitNativeIntegerKind kind) {
	return kind == SljitNativeIntegerKind::DATE;
}

static void SLJIT_FUNC SljitNativeIntegerOverflow(SljitNativeVectorInput *input) {
	try {
		throw OutOfRangeException("%s", input->overflow_message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

double SLJIT_FUNC SljitNativeHugeintToDouble(uint64_t lower, int64_t upper) {
	hugeint_t value;
	value.lower = lower;
	value.upper = upper;
	return Hugeint::Cast<double>(value);
}

static void SLJIT_FUNC SljitNativeIntegerCastOverflow(SljitNativeVectorInput *input) {
	try {
		throw ConversionException(input->query_location, input->overflow_message, input->overflow_value);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

void SLJIT_FUNC SljitNativeRuntimeError(SljitNativeVectorInput *input) {
	try {
		throw InvalidInputException("%s", input->error_message ? input->error_message : "");
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

template <class INPUT_TYPE>
static void SljitNativeReadCompressedStringBytes(const_data_ptr_t source, data_t *bytes) {
	auto value = BSwapIfBE(Load<INPUT_TYPE>(source));
	memcpy(bytes, const_data_ptr_cast(&value), sizeof(INPUT_TYPE));
}

static void SljitNativeReadCompressedStringBytes(const_data_ptr_t source, idx_t source_size, data_t *bytes) {
	switch (source_size) {
	case 1:
		SljitNativeReadCompressedStringBytes<uint8_t>(source, bytes);
		return;
	case 2:
		SljitNativeReadCompressedStringBytes<uint16_t>(source, bytes);
		return;
	case 4:
		SljitNativeReadCompressedStringBytes<uint32_t>(source, bytes);
		return;
	case 8:
		SljitNativeReadCompressedStringBytes<uint64_t>(source, bytes);
		return;
	case 16:
		SljitNativeReadCompressedStringBytes<uhugeint_t>(source, bytes);
		return;
	default:
		throw InternalException("Unsupported SLJIT native string decompression source size");
	}
}

static void SLJIT_FUNC SljitNativeStringDecompress(SljitNativeVectorInput *input) {
	try {
		auto source_size = input->string_decompress_source_size;
		D_ASSERT(input->source_data);
		D_ASSERT(input->result_data);
		D_ASSERT(input->result_vector);
		data_t compressed[sizeof(uhugeint_t)];
		auto source = input->source_data + input->active_source_index * NumericCast<idx_t>(source_size);
		SljitNativeReadCompressedStringBytes(source, source_size, compressed);

		char decoded[sizeof(uhugeint_t)];
		idx_t length;
		if (source_size == 1) {
			if (compressed[0] == 0) {
				length = 0;
			} else {
				length = 1;
				decoded[0] = char(compressed[0] - 1);
			}
		} else {
			length = compressed[0];
			if (length >= source_size) {
				throw InternalException("Invalid compressed string payload for SLJIT native decompression");
			}
			for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
				decoded[byte_idx] = char(compressed[source_size - byte_idx - 1]);
			}
		}

		auto result_data = reinterpret_cast<string_t *>(input->result_data);
		result_data[input->active_result_index] = StringVector::AddString(*input->result_vector, decoded, length);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

unique_ptr<ExecutionRegionCodeHandle> FinishSljitNativeVectorCode(struct sljit_compiler *compiler,
                                                                  SljitNativeVectorFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = GenerateSljitCode(compiler);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeVectorFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

unique_ptr<ExecutionRegionCodeHandle> FinishSljitNativeAggregateUpdateCode(struct sljit_compiler *compiler,
                                                                           SljitNativeAggregateUpdateFunction &function,
                                                                           string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = GenerateSljitCode(compiler);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeAggregateUpdateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

void EmitLoadLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

struct SljitNativeVectorLoopContext {
	sljit_s32 row_index_reg = SLJIT_S1;
	sljit_s32 count_reg = SLJIT_S2;
	sljit_s32 source_sel_array_reg = SLJIT_S4;
	sljit_s32 source_data_array_reg = SLJIT_S5;
	sljit_s32 source_validity_array_reg = SLJIT_S6;
};

static constexpr SljitNativeVectorLoopContext SLJIT_NATIVE_VECTOR_LOOP;

static void EmitInitSljitNativeVectorLoop(struct sljit_compiler *compiler, const SljitNativeVectorLoopContext &loop) {
	sljit_emit_op1(compiler, SLJIT_MOV, loop.row_index_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, loop.count_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, count));
}

static void EmitInitSljitNativeVectorSourceArrays(struct sljit_compiler *compiler,
                                                  const SljitNativeVectorLoopContext &loop) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, loop.source_sel_array_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, loop.source_data_array_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, loop.source_validity_array_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));
}

void EmitInitSljitNativeVectorLoop(struct sljit_compiler *compiler) {
	EmitInitSljitNativeVectorLoop(compiler, SLJIT_NATIVE_VECTOR_LOOP);
}

void EmitInitSljitNativeVectorSourceArrays(struct sljit_compiler *compiler) {
	EmitInitSljitNativeVectorSourceArrays(compiler, SLJIT_NATIVE_VECTOR_LOOP);
}

void EmitInitSljitNativeExpressionVectorLoop(struct sljit_compiler *compiler) {
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);
}

void EmitLoadSourceIndex(struct sljit_compiler *compiler, sljit_sw sel_offset, sljit_s32 logical_index,
                         sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), sel_offset);
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

sljit_jump *EmitJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_offset, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), validity_offset);
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

void EmitLoadSelectedIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 2);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
}

sljit_jump *EmitSkipInvalidSourceRow(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

void EmitSetResultRowInvalid(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_validity));
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_XOR, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, -1);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3, SLJIT_R3, 0);
}

static struct sljit_jump *EmitJumpIfSourceNull(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeNullCheck(SljitNativeNullCheckOp op,
                                                                SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitJumpIfSourceNull(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM,
	               op == SljitNativeNullCheckOp::IS_NULL ? 0 : 1);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM,
	               op == SljitNativeNullCheckOp::IS_NULL ? 1 : 0);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static bool IsSljitNativeStringCompressionStorageSize(idx_t storage_size) {
	switch (storage_size) {
	case 1:
	case 2:
	case 4:
	case 8:
	case 16:
		return true;
	default:
		return false;
	}
}

static bool IsSljitNativeStringCompressTargetSize(idx_t target_size) {
	return IsSljitNativeStringCompressionStorageSize(target_size);
}

static idx_t SljitNativeStringCompressTargetShift(idx_t target_size) {
	switch (target_size) {
	case 1:
		return 0;
	case 2:
		return 1;
	case 4:
		return 2;
	case 8:
		return 3;
	case 16:
		return 4;
	default:
		throw InternalException("Unsupported SLJIT native string compression target size");
	}
}

static void EmitSljitNativeStringCompressResultPointer(struct sljit_compiler *compiler, idx_t target_size) {
	auto target_shift = SljitNativeStringCompressTargetShift(target_size);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	if (target_shift == 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_S1, 0);
	} else {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(target_shift));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R4, 0);
	}
}

static void EmitSljitNativeMiniStringCompress(struct sljit_compiler *compiler) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	auto empty_string = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S5, 0, SLJIT_IMM, 0);
	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_S5, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S3), STRING_INLINE_PREFIX_OFFSET);
	auto have_first_byte = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S3), STRING_POINTER_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R4), 0);
	sljit_set_label(have_first_byte, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_S5, 0, SLJIT_R3, 0);
	auto have_result = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(empty_string, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);

	sljit_set_label(have_result, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), 0, SLJIT_R2, 0);
}

static void EmitSljitNativeInlineStringCompress(struct sljit_compiler *compiler, idx_t target_size) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);

	for (idx_t result_byte = 0; result_byte < target_size; result_byte++) {
		auto source_byte = target_size - result_byte - 1;
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3),
		               STRING_INLINE_PREFIX_OFFSET + NumericCast<sljit_sw>(source_byte));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), NumericCast<sljit_sw>(result_byte), SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), 0, SLJIT_S5, 0);
}

static void EmitSljitNativeHugeStringCompress(struct sljit_compiler *compiler, idx_t target_size) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;
	auto inline_remainder = target_size - string_t::INLINE_LENGTH;

	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_S5, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));

	for (idx_t result_byte = 0; result_byte < inline_remainder; result_byte++) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), NumericCast<sljit_sw>(result_byte), SLJIT_IMM, 0);
	}
	for (idx_t result_byte = 0; result_byte < string_t::INLINE_LENGTH; result_byte++) {
		auto source_byte = string_t::INLINE_LENGTH - result_byte - 1;
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3),
		               STRING_INLINE_PREFIX_OFFSET + NumericCast<sljit_sw>(source_byte));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4),
		               NumericCast<sljit_sw>(inline_remainder + result_byte), SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), 0, SLJIT_S5, 0);
	auto have_result = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S3), STRING_POINTER_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_S5, 0);
	auto copy_count_fits =
	    sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, SLJIT_R4, 0, SLJIT_IMM, NumericCast<sljit_sw>(target_size));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_IMM, NumericCast<sljit_sw>(target_size));
	sljit_set_label(copy_count_fits, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(target_size), SLJIT_R4, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	auto zero_loop = sljit_emit_label(compiler);
	auto zero_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	auto repeat_zero = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_zero, zero_loop);
	sljit_set_label(zero_done, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
	auto copy_loop = sljit_emit_label(compiler);
	auto copy_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_R1, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R4, 0, SLJIT_IMM, 1);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R2, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_S4, SLJIT_R0), 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	auto repeat_copy = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_copy, copy_loop);
	sljit_set_label(copy_done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S4), 0, SLJIT_S5, 0);

	sljit_set_label(have_result, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeStringCompress(idx_t target_size, SljitNativeVectorFunction &function, string &error) {
	static_assert(sizeof(string_t) == 16, "SLJIT string compression expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	static constexpr sljit_sw STRING_T_SHIFT = 4;

	if (!IsSljitNativeStringCompressTargetSize(target_size)) {
		error = "SLJIT native string compression has unsupported target size " + std::to_string(target_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_R1, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S3), STRING_LENGTH_OFFSET);
	EmitSljitNativeStringCompressResultPointer(compiler, target_size);

	if (target_size == 1) {
		EmitSljitNativeMiniStringCompress(compiler);
	} else if (target_size <= string_t::INLINE_LENGTH) {
		EmitSljitNativeInlineStringCompress(compiler, target_size);
	} else {
		EmitSljitNativeHugeStringCompress(compiler, target_size);
	}
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeStringDecompress(idx_t source_size, SljitNativeVectorFunction &function, string &error) {
	static_assert(sizeof(string_t) == 16, "SLJIT string decompression expects DuckDB string_t ABI size");

	if (!IsSljitNativeStringCompressionStorageSize(source_size)) {
		error = "SLJIT native string decompression has unsupported source size " + std::to_string(source_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, active_source_index),
	               SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, active_result_index),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeStringDecompress));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, has_error));
	auto helper_error = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(helper_error, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralCompress(SljitNativeSignedIntegerWidth source_width,
                                                                       SljitNativeUnsignedIntegerWidth target_width,
                                                                       SljitNativeVectorFunction &function,
                                                                       string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeSignedIntegerDataScale(source_width);
	auto source_load_op = NativeSignedIntegerLoadOp(source_width);
	auto target_data_scale = NativeUnsignedIntegerDataScale(target_width);
	auto target_store_op = NativeUnsignedIntegerStoreOp(target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, target_store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), target_data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralDecompress(SljitNativeUnsignedIntegerWidth source_width,
                                                                         SljitNativeSignedIntegerWidth target_width,
                                                                         SljitNativeVectorFunction &function,
                                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeUnsignedIntegerDataScale(source_width);
	auto source_load_op = NativeUnsignedIntegerLoadOp(source_width);
	auto target_data_scale = NativeSignedIntegerDataScale(target_width);
	auto target_store_op = NativeSignedIntegerStoreOp(target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, target_store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), target_data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDateYear(SljitNativeVectorFunction &function, string &error) {
	static_assert(sizeof(date_t) == sizeof(int32_t), "SLJIT date-year expects DuckDB date_t ABI size");

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 2);

	auto positive_infinity =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::infinity().days));
	auto negative_infinity =
	    sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, NumericCast<sljit_sw>(date_t::ninfinity().days));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, Date::EPOCH_YEAR);

	auto normalize_negative_loop = sljit_emit_label(compiler);
	auto not_negative = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, Date::YEAR_INTERVAL);
	auto repeat_negative = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_negative, normalize_negative_loop);
	sljit_set_label(not_negative, sljit_emit_label(compiler));

	auto normalize_positive_loop = sljit_emit_label(compiler);
	auto in_current_interval =
	    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_S3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, Date::YEAR_INTERVAL);
	auto repeat_positive = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_positive, normalize_positive_loop);
	sljit_set_label(in_current_interval, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_S3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 365);
	sljit_emit_op0(compiler, SLJIT_DIV_SW);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_IMM,
	               reinterpret_cast<sljit_sw>(&Date::CUMULATIVE_YEAR_DAYS[0]));

	auto year_offset_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R4, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R2), 2);
	auto have_year_offset = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	auto repeat_year_offset = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_year_offset, year_offset_loop);
	sljit_set_label(have_year_offset, sljit_emit_label(compiler));

	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3, SLJIT_S4, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	sljit_set_label(positive_infinity, invalid_label);
	sljit_set_label(negative_infinity, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static bool IsSljitNativeGuardedReferenceValueSize(idx_t value_size) {
	switch (value_size) {
	case 1:
	case 2:
	case 4:
	case 8:
	case 16:
		return true;
	default:
		return false;
	}
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeErrorGuardedReference(idx_t value_size, SljitNativeIntegerCompareOp guard_compare_op,
                                      bool guard_constant_on_left, SljitNativeVectorFunction &function, string &error) {
	if (!IsSljitNativeGuardedReferenceValueSize(value_size)) {
		error = "SLJIT error-guarded reference has unsupported value size " + std::to_string(value_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto compare_type = NativeIntegerCompareJumpType(SljitNativeIntegerKind::INT64, guard_compare_op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);

	auto guard_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	struct sljit_jump *guard_is_true;
	if (guard_constant_on_left) {
		guard_is_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		guard_is_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}

	auto copy_label = sljit_emit_label(compiler);
	sljit_set_label(guard_is_null, copy_label);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, NumericCast<sljit_sw>(value_size));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(value_size));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	for (idx_t byte_idx = 0; byte_idx < value_size; byte_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), NumericCast<sljit_sw>(byte_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R3), NumericCast<sljit_sw>(byte_idx), SLJIT_R2, 0);
	}
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto error_label = sljit_emit_label(compiler);
	sljit_set_label(guard_is_true, error_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	auto helper_error = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(helper_error, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
                                      SljitNativeVectorFunction &function, string &error,
                                      bool check_arithmetic_overflow, bool check_result_range, int64_t result_min,
                                      int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	struct sljit_jump *date_is_negative_infinity = nullptr;
	struct sljit_jump *date_is_positive_infinity = nullptr;
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		date_is_negative_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
		                   SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
		date_is_positive_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
		                   SLJIT_DATE_POSITIVE_INFINITY_DAYS);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	auto emit_binary_op = check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op;
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (constant_on_left) {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		break;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
	struct sljit_jump *overflow = nullptr;
	if (check_arithmetic_overflow) {
		overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	}
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
	}
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		auto store_date_result = sljit_emit_label(compiler);
		sljit_set_label(date_is_negative_infinity, store_date_result);
		sljit_set_label(date_is_positive_infinity, store_date_result);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
	}
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static bool SljitArm64NeonIntegerBinarySupported(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	if (!sljit_has_cpu_feature(SLJIT_HAS_SIMD)) {
		return false;
	}
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return op == SljitNativeIntegerBinaryOp::ADD || op == SljitNativeIntegerBinaryOp::SUBTRACT ||
		       op == SljitNativeIntegerBinaryOp::MULTIPLY;
	case SljitNativeIntegerKind::INT64:
		return op == SljitNativeIntegerBinaryOp::ADD || op == SljitNativeIntegerBinaryOp::SUBTRACT;
	default:
		return false;
	}
#else
	return false;
#endif
}

static sljit_s32 SljitArm64NeonIntegerSimdType(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_32;
	case SljitNativeIntegerKind::INT64:
		return SLJIT_SIMD_REG_128 | SLJIT_SIMD_ELEM_64;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
}

static idx_t SljitArm64NeonIntegerLaneCount(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		return 4;
	case SljitNativeIntegerKind::INT64:
		return 2;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
}

static uint32_t SljitArm64NeonIntegerBinaryInstruction(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                                       uint32_t dst, uint32_t left, uint32_t right) {
	uint32_t base;
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			base = 0x4ea08400;
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			base = 0x6ea08400;
			break;
		case SljitNativeIntegerBinaryOp::MULTIPLY:
			base = 0x4ea09c00;
			break;
		default:
			throw InternalException("Unsupported ARM64 NEON int32 binary op");
		}
		break;
	case SljitNativeIntegerKind::INT64:
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			base = 0x4ee08400;
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			base = 0x6ee08400;
			break;
		default:
			throw InternalException("Unsupported ARM64 NEON int64 binary op");
		}
		break;
	default:
		throw InternalException("Unsupported ARM64 NEON integer kind");
	}
	return base | (right << 16) | (left << 5) | dst;
}

static void EmitSljitArm64NeonIntegerBinary(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                            SljitNativeIntegerBinaryOp op, sljit_s32 dst_vreg, sljit_s32 left_vreg,
                                            sljit_s32 right_vreg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, dst_vreg);
	auto left = sljit_get_register_index(SLJIT_SIMD_REG_128, left_vreg);
	auto right = sljit_get_register_index(SLJIT_SIMD_REG_128, right_vreg);
	if (dst < 0 || left < 0 || right < 0) {
		throw InternalException("SLJIT ARM64 NEON register mapping is unavailable");
	}
	auto instruction =
	    SljitArm64NeonIntegerBinaryInstruction(kind, op, UnsafeNumericCast<uint32_t>(dst),
	                                           UnsafeNumericCast<uint32_t>(left), UnsafeNumericCast<uint32_t>(right));
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

static bool SljitArm64NeonFloatingBinarySupported(SljitNativeDoubleBinaryOp op) {
#if defined(SLJIT_CONFIG_ARM_64) && SLJIT_CONFIG_ARM_64
	if (!sljit_has_cpu_feature(SLJIT_HAS_SIMD)) {
		return false;
	}
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
	case SljitNativeDoubleBinaryOp::SUBTRACT:
	case SljitNativeDoubleBinaryOp::MULTIPLY:
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return true;
	default:
		return false;
	}
#else
	return false;
#endif
}

static sljit_s32 SljitArm64NeonFloatingSimdType(bool single_precision) {
	return SLJIT_SIMD_REG_128 | SLJIT_SIMD_FLOAT | (single_precision ? SLJIT_SIMD_ELEM_32 : SLJIT_SIMD_ELEM_64);
}

static idx_t SljitArm64NeonFloatingLaneCount(bool single_precision) {
	return single_precision ? 4 : 2;
}

static uint32_t SljitArm64NeonFloatingBinaryInstruction(bool single_precision, SljitNativeDoubleBinaryOp op,
                                                        uint32_t dst, uint32_t left, uint32_t right) {
	uint32_t base;
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		base = single_precision ? 0x4e20d400 : 0x4e60d400;
		break;
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		base = single_precision ? 0x4ea0d400 : 0x4ee0d400;
		break;
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		base = single_precision ? 0x6e20dc00 : 0x6e60dc00;
		break;
	case SljitNativeDoubleBinaryOp::DIVIDE:
		base = single_precision ? 0x6e20fc00 : 0x6e60fc00;
		break;
	default:
		throw InternalException("Unsupported ARM64 NEON floating binary op");
	}
	return base | (right << 16) | (left << 5) | dst;
}

static void EmitSljitArm64NeonFloatingBinary(struct sljit_compiler *compiler, bool single_precision,
                                             SljitNativeDoubleBinaryOp op, sljit_s32 dst_vreg, sljit_s32 left_vreg,
                                             sljit_s32 right_vreg) {
	auto dst = sljit_get_register_index(SLJIT_SIMD_REG_128, dst_vreg);
	auto left = sljit_get_register_index(SLJIT_SIMD_REG_128, left_vreg);
	auto right = sljit_get_register_index(SLJIT_SIMD_REG_128, right_vreg);
	if (dst < 0 || left < 0 || right < 0) {
		throw InternalException("SLJIT ARM64 NEON register mapping is unavailable");
	}
	auto instruction =
	    SljitArm64NeonFloatingBinaryInstruction(single_precision, op, UnsafeNumericCast<uint32_t>(dst),
	                                            UnsafeNumericCast<uint32_t>(left), UnsafeNumericCast<uint32_t>(right));
	sljit_emit_op_custom(compiler, &instruction, sizeof(instruction));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                          bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	auto use_simd = SljitArm64NeonIntegerBinarySupported(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), use_simd ? 5 | SLJIT_ENTER_VECTOR(3) : 5, 6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));

	auto emit_integer_constant_op = [&]() {
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			if (constant_on_left) {
				sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_S5, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			}
			break;
		case SljitNativeIntegerBinaryOp::MULTIPLY:
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_S5, 0);
			break;
		default:
			throw InternalException("Unknown SLJIT native integer binary operator");
		}
	};
	auto emit_integer_constant_row = [&](sljit_sw offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3), offset);
		emit_integer_constant_op();
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(SLJIT_S4), offset, SLJIT_R2, 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonIntegerSimdType(kind);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(kind));
		sljit_emit_simd_replicate(compiler, simd_type, SLJIT_VR1, SLJIT_S5, 0);

		auto vector_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
		if (op == SljitNativeIntegerBinaryOp::SUBTRACT && constant_on_left) {
			EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR1, SLJIT_VR0);
		} else {
			EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
		}
		sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S4), 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, vector_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_integer_constant_row(0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	} else {
		auto unrolled_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_IMM, 4);
		for (idx_t row = 0; row < 4; row++) {
			emit_integer_constant_row(NumericCast<sljit_sw>(row) * data_width);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width * 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width * 4);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, unrolled_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_integer_constant_row(0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeVectorFunction &function, string &error,
    bool check_arithmetic_overflow, bool check_result_range, int64_t result_min, int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), data_scale);
	struct sljit_jump *date_is_negative_infinity = nullptr;
	struct sljit_jump *date_is_positive_infinity = nullptr;
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		date_is_negative_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
		                   SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
		date_is_positive_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
		                   SLJIT_DATE_POSITIVE_INFINITY_DAYS);
	}

	sljit_emit_op2(compiler, check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op, SLJIT_R2, 0,
	               SLJIT_R2, 0, SLJIT_R3, 0);
	struct sljit_jump *overflow = nullptr;
	if (check_arithmetic_overflow) {
		overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	}
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
	}
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		auto store_date_result = sljit_emit_label(compiler);
		sljit_set_label(date_is_negative_infinity, store_date_result);
		sljit_set_label(date_is_positive_infinity, store_date_result);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
	}

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFlatIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                                  SljitNativeIntegerBinaryOp op,
                                                                                  SljitNativeVectorFunction &function,
                                                                                  string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto data_width = sljit_sw(1) << data_scale;
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	auto use_simd = SljitArm64NeonIntegerBinarySupported(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), use_simd ? 5 | SLJIT_ENTER_VECTOR(3) : 5, 6, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));

	auto emit_integer_reference_row = [&](sljit_sw offset) {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S3), offset);
		sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S4), offset);
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		sljit_emit_op1(compiler, store_op, SLJIT_MEM1(SLJIT_S5), offset, SLJIT_R2, 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonIntegerSimdType(kind);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(kind));

		auto vector_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR1, SLJIT_MEM1(SLJIT_S4), 0);
		EmitSljitArm64NeonIntegerBinary(compiler, kind, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
		sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S5), 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, vector_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_integer_reference_row(0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	} else {
		auto unrolled_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_IMM, 4);
		for (idx_t row = 0; row < 4; row++) {
			emit_integer_reference_row(NumericCast<sljit_sw>(row) * data_width);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width * 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width * 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width * 4);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, unrolled_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_integer_reference_row(0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static bool ValidateNativeFlatIntegerProjectionExpression(const SljitNativeRegionExpressionPlan &plan,
                                                          SljitNativeIntegerKind kind, string &error) {
	if (plan.integer_kind != kind) {
		error = "SLJIT flat integer projection cannot mix integer widths";
		return false;
	}
	switch (kind) {
	case SljitNativeIntegerKind::INT32:
		if (plan.check_arithmetic_overflow || plan.check_result_range) {
			error = "SLJIT flat integer projection only supports unchecked INTEGER expressions";
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT32) {
			error = "SLJIT flat integer projection result type does not match integer width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::INT64:
		if (plan.check_arithmetic_overflow || plan.check_result_range) {
			error = "SLJIT flat integer projection only supports unchecked BIGINT expressions";
			return false;
		}
		if (plan.return_type.InternalType() != PhysicalType::INT64) {
			error = "SLJIT flat integer projection result type does not match integer width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::DECIMAL64:
		if (plan.return_type.id() != LogicalTypeId::DECIMAL ||
		    plan.return_type.InternalType() != PhysicalType::INT64) {
			error = "SLJIT flat integer projection DECIMAL64 result type does not match storage width";
			return false;
		}
		break;
	case SljitNativeIntegerKind::DATE:
		if (plan.return_type.id() != LogicalTypeId::DATE || plan.return_type.InternalType() != PhysicalType::INT32) {
			error = "SLJIT flat integer projection DATE result type does not match storage width";
			return false;
		}
		break;
	default:
		error = "SLJIT flat integer projection only supports INTEGER/BIGINT/DECIMAL64/DATE result types";
		return false;
	}
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		error = "SLJIT flat integer projection only supports integer binary expressions";
		return false;
	}
}

struct SljitFlatProjectionSourceRef {
	idx_t input_index = DConstants::INVALID_INDEX;
	idx_t projection_index = DConstants::INVALID_INDEX;
	bool right_source = false;
};

struct SljitFlatProjectionSharedSourcePlan {
	vector<SljitFlatProjectionSourceRef> sources;
};

struct SljitFlatProjectionOverflowJump {
	idx_t projection_index;
	struct sljit_jump *jump;
};

static idx_t SljitFlatProjectionSourceRegisterIndex(const vector<SljitFlatProjectionSourceRef> &sources,
                                                    idx_t input_index, const char *projection_kind) {
	for (idx_t source_idx = 0; source_idx < sources.size(); source_idx++) {
		if (sources[source_idx].input_index == input_index) {
			return source_idx;
		}
	}
	throw InternalException("SLJIT flat %s projection source index is not registered", projection_kind);
}

static bool TryAddSljitFlatProjectionSource(SljitFlatProjectionSharedSourcePlan &shared_plan, idx_t input_index,
                                            idx_t projection_index, bool right_source) {
	for (auto &source : shared_plan.sources) {
		if (source.input_index == input_index) {
			return true;
		}
	}
	if (shared_plan.sources.size() >= 2) {
		return false;
	}
	SljitFlatProjectionSourceRef source;
	source.input_index = input_index;
	source.projection_index = projection_index;
	source.right_source = right_source;
	shared_plan.sources.push_back(source);
	return true;
}

static bool TryPlanSljitFlatProjectionSharedSources(const vector<SljitNativeRegionExpressionPlan> &plans,
                                                    const vector<idx_t> &projection_indices,
                                                    SljitNativeRegionExpressionKind references_kind,
                                                    idx_t min_projection_count, idx_t max_projection_count,
                                                    SljitFlatProjectionSharedSourcePlan &shared_plan) {
	if (projection_indices.size() < min_projection_count ||
	    (max_projection_count != DConstants::INVALID_INDEX && projection_indices.size() > max_projection_count)) {
		return false;
	}
	shared_plan = SljitFlatProjectionSharedSourcePlan();
	for (auto projection_index : projection_indices) {
		auto &plan = plans[projection_index];
		if (!TryAddSljitFlatProjectionSource(shared_plan, plan.source_index, projection_index, false)) {
			return false;
		}
		if (plan.kind == references_kind &&
		    !TryAddSljitFlatProjectionSource(shared_plan, plan.right_source_index, projection_index, true)) {
			return false;
		}
	}
	return !shared_plan.sources.empty();
}

static sljit_s32 SljitFlatIntegerProjectionSourceVectorRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_VR0;
	case 1:
		return SLJIT_VR1;
	default:
		throw InternalException("SLJIT flat integer projection source vector register is out of range");
	}
}

static sljit_s32 SljitFlatIntegerProjectionSourceScalarRegister(idx_t source_idx) {
	switch (source_idx) {
	case 0:
		return SLJIT_R2;
	case 1:
		return SLJIT_R3;
	default:
		throw InternalException("SLJIT flat integer projection source scalar register is out of range");
	}
}

static constexpr idx_t SljitFlatIntegerProjectionGroupSize() {
#if SLJIT_NUMBER_OF_SAVED_REGISTERS >= 9
	return 4;
#else
	return 2;
#endif
}

static constexpr sljit_s32 SljitFlatIntegerProjectionSavedRegisterCount() {
	return static_cast<sljit_s32>(5 + SljitFlatIntegerProjectionGroupSize());
}

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
	vector<SljitFlatProjectionOverflowJump> overflow_jumps;

	auto emit_scalar_sources = [&]() {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
			auto source_value_reg = SljitFlatIntegerProjectionSourceScalarRegister(source_idx);
			sljit_emit_op1(compiler, load_op, source_value_reg, 0, SLJIT_MEM1(source_pointer_reg), 0);
		}
	};
	auto emit_simd_sources = [&]() {
		auto simd_type = SljitArm64NeonIntegerSimdType(integer_kind);
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
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
			date_is_negative_infinity =
			    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM,
			                   SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
			date_is_positive_infinity =
			    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM,
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
			overflow_jumps.push_back(
			    {projection_index,
			     sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R4, 0, SLJIT_IMM,
			                    NumericCast<sljit_sw>(plan.result_min))});
			overflow_jumps.push_back(
			    {projection_index,
			     sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R4, 0, SLJIT_IMM,
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

	auto load_source_pointers = [&]() {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto &source = shared_plan.sources[source_idx];
			auto source_array_offset = source.right_source ? offsetof(SljitNativeVectorInput, right_source_data_array)
			                                               : offsetof(SljitNativeVectorInput, source_data_array);
			auto source_pointer_offset = SljitPointerArrayOffset(source.projection_index);
			auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_array_offset);
			sljit_emit_op1(compiler, SLJIT_MOV_P, source_pointer_reg, 0, SLJIT_MEM1(SLJIT_R0), source_pointer_offset);
		}
	};
	auto increment_source_pointers = [&](sljit_sw bytes) {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
			sljit_emit_op2(compiler, SLJIT_ADD, source_pointer_reg, 0, source_pointer_reg, 0, SLJIT_IMM, bytes);
		}
	};
	auto result_pointer_register = [](idx_t group_idx) {
		switch (group_idx) {
		case 0:
			return SLJIT_S5;
		case 1:
			return SLJIT_S6;
		case 2:
			return SLJIT_S7;
		case 3:
			return SLJIT_S8;
		default:
			throw InternalException("SLJIT flat integer projection result register is out of range");
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
		load_source_pointers();
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data_array));
		for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
			auto projection_index = projection_indices[group_begin + group_idx];
			auto result_pointer_offset = SljitPointerArrayOffset(projection_index);
			sljit_emit_op1(compiler, SLJIT_MOV_P, result_pointer_register(group_idx), 0, SLJIT_MEM1(SLJIT_R0),
			               result_pointer_offset);
		}
		if (use_simd) {
			auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonIntegerLaneCount(integer_kind));
			auto vector_loop = sljit_emit_label(compiler);
			auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
			emit_simd_sources();
			for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
				emit_simd_projection(projection_indices[group_begin + group_idx], result_pointer_register(group_idx));
			}
			increment_source_pointers(simd_bytes);
			for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
				auto result_reg = result_pointer_register(group_idx);
				sljit_emit_op2(compiler, SLJIT_ADD, result_reg, 0, result_reg, 0, SLJIT_IMM, simd_bytes);
			}
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, simd_lanes);
			auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(repeat, vector_loop);
			sljit_set_label(tail, sljit_emit_label(compiler));
		}

		auto tail_loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_scalar_sources();
		for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
			emit_scalar_projection(projection_indices[group_begin + group_idx], result_pointer_register(group_idx));
		}
		increment_source_pointers(data_width);
		for (idx_t group_idx = 0; group_begin + group_idx < group_end; group_idx++) {
			auto result_reg = result_pointer_register(group_idx);
			sljit_emit_op2(compiler, SLJIT_ADD, result_reg, 0, result_reg, 0, SLJIT_IMM, data_width);
		}
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}
	auto success = sljit_emit_jump(compiler, SLJIT_JUMP);
	vector<struct sljit_jump *> overflow_returns;
	if (needs_overflow_handling) {
		for (auto &overflow_jump : overflow_jumps) {
			auto overflow_label = sljit_emit_label(compiler);
			sljit_set_label(overflow_jump.jump, overflow_label);
			auto overflow_message_offset = SljitPointerArrayOffset(overflow_jump.projection_index);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, overflow_messages));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), overflow_message_offset);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, overflow_message), SLJIT_R1, 0);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
			sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
			                 SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
			overflow_returns.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
	}
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(success, done_label);
	for (auto overflow_return : overflow_returns) {
		sljit_set_label(overflow_return, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
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

sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op, bool single_precision) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		return single_precision ? SLJIT_ADD_F32 : SLJIT_ADD_F64;
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		return single_precision ? SLJIT_SUB_F32 : SLJIT_SUB_F64;
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		return single_precision ? SLJIT_MUL_F32 : SLJIT_MUL_F64;
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return single_precision ? SLJIT_DIV_F32 : SLJIT_DIV_F64;
	default:
		throw InternalException("Unknown SLJIT native double binary operator");
	}
}

bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::INT128_TO_DOUBLE ||
	       kind == SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
}

bool NativeDoubleSourceHasDecimalScale(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE ||
	       kind == SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
}

static void EmitScaleNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                         sljit_sw scale_offset, sljit_s32 target) {
	if (!NativeDoubleSourceHasDecimalScale(kind)) {
		return;
	}
	sljit_emit_fop2(compiler, SLJIT_DIV_F64, target, 0, target, 0, SLJIT_MEM1(SLJIT_S0), scale_offset);
}

void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                 sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset, sljit_s32 target,
                                 bool single_precision) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), data_offset);
	switch (kind) {
	case SljitNativeDoubleSourceKind::FLOAT:
		sljit_emit_fmem(compiler, SLJIT_MOV_F32 | SLJIT_MEM_ALIGNED_16, target, SLJIT_MEM2(SLJIT_R0, index_reg), 2);
		return;
	case SljitNativeDoubleSourceKind::DOUBLE:
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		return;
	case SljitNativeDoubleSourceKind::INT64_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		sljit_emit_fop1(compiler, single_precision ? SLJIT_CONV_F32_FROM_SW : SLJIT_CONV_F64_FROM_SW, target, 0,
		                SLJIT_R2, 0);
		EmitScaleNativeDoubleOperand(compiler, kind, scale_offset, target);
		return;
	case SljitNativeDoubleSourceKind::INT128_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE:
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R0, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, lower)));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, upper)));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS2(F64, W, W), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeHugeintToDouble));
		sljit_emit_fop1(compiler, SLJIT_MOV_F64, target, 0, SLJIT_RETURN_FREG, 0);
		EmitScaleNativeDoubleOperand(compiler, kind, scale_offset, target);
		return;
	default:
		throw InternalException("Unknown SLJIT native double source kind");
	}
}

static bool IsDirectNativeFloatingSource(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::FLOAT || kind == SljitNativeDoubleSourceKind::DOUBLE;
}

static sljit_s32 NativeDirectFloatingMoveOp(bool single_precision) {
	return single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
}

static sljit_s32 NativeDirectFloatingMemoryAlignment(bool single_precision) {
	return single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
}

static sljit_sw NativeDirectFloatingDataScale(bool single_precision) {
	return single_precision ? 2 : 3;
}

static sljit_sw NativeDirectFloatingDataWidth(bool single_precision) {
	return single_precision ? NumericCast<sljit_sw>(sizeof(float)) : NumericCast<sljit_sw>(sizeof(double));
}

static bool ValidateNativeFlatDoubleBinarySource(SljitNativeDoubleSourceKind kind, bool single_precision,
                                                 string &error) {
	if (!IsDirectNativeFloatingSource(kind)) {
		error = "SLJIT flat floating binary fast path only supports direct FLOAT/DOUBLE sources";
		return false;
	}
	if (single_precision != (kind == SljitNativeDoubleSourceKind::FLOAT)) {
		error = "SLJIT flat floating binary fast path source type does not match result precision";
		return false;
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                                           SljitNativeDoubleSourceKind source_kind,
                                                                           bool constant_on_left, bool single_precision,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
	auto fmem_align = single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
	auto result_data_scale = single_precision ? 2 : 3;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
	                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0, single_precision);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S0),
	                offsetof(SljitNativeVectorInput, double_constant));
	if (single_precision) {
		sljit_emit_fop1(compiler, SLJIT_CONV_F32_FROM_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0);
	}
	if (constant_on_left) {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR0, 0);
	} else {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	                result_data_scale);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryConstant(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind source_kind,
                                         bool constant_on_left, bool single_precision,
                                         SljitNativeVectorFunction &function, string &error) {
	if (!ValidateNativeFlatDoubleBinarySource(source_kind, single_precision, error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_width = NativeDirectFloatingDataWidth(single_precision);
	auto use_simd = SljitArm64NeonFloatingBinarySupported(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2) | (use_simd ? SLJIT_ENTER_VECTOR(3) : 0), 5,
	                 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S0),
	                offsetof(SljitNativeVectorInput, double_constant));
	if (single_precision) {
		sljit_emit_fop1(compiler, SLJIT_CONV_F32_FROM_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0);
	}

	auto emit_row = [&]() {
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_S3), 0);
		if (constant_on_left) {
			sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR0, 0);
		} else {
			sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
		}
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_S4), 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonFloatingSimdType(single_precision);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonFloatingLaneCount(single_precision));
		sljit_emit_simd_replicate(compiler, simd_type, SLJIT_VR1, SLJIT_TMP_FR1, 0);

		auto vector_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S2, 0, SLJIT_IMM, simd_lanes);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
		if (constant_on_left) {
			EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR1, SLJIT_VR0);
		} else {
			EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
		}
		sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S4), 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S2, 0, SLJIT_S2, 0, SLJIT_IMM, simd_lanes);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, vector_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		emit_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S2, 0, SLJIT_S2, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	} else {
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		emit_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}

	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                       SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                       SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = single_precision ? SLJIT_MOV_F32 : SLJIT_MOV_F64;
	auto fmem_align = single_precision ? SLJIT_MEM_ALIGNED_16 : SLJIT_MEM_ALIGNED_32;
	auto result_data_scale = single_precision ? 2 : 3;
	auto needs_helper_spill = NativeDoubleSourceUsesHelper(left_kind) || NativeDoubleSourceUsesHelper(right_kind);
	auto spill_width = single_precision ? sizeof(float) : sizeof(double);
	sljit_sw left_spill_offset = 0;
	auto right_spill_offset = NumericCast<sljit_sw>(spill_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5,
	                 needs_helper_spill ? NumericCast<sljit_sw>(spill_width * 2) : 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	if (needs_helper_spill) {
		EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0,
		                            single_precision);
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_SP),
		                left_spill_offset);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR0,
		                            single_precision);
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_SP),
		                right_spill_offset);
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_SP), left_spill_offset);
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP), right_spill_offset);
	} else {
		EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0,
		                            single_precision);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR1,
		                            single_precision);
	}
	sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	                result_data_scale);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                           SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                           SljitNativeVectorFunction &function, string &error) {
	if (!ValidateNativeFlatDoubleBinarySource(left_kind, single_precision, error) ||
	    !ValidateNativeFlatDoubleBinarySource(right_kind, single_precision, error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op, single_precision);
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto data_width = NativeDirectFloatingDataWidth(single_precision);
	auto use_simd = SljitArm64NeonFloatingBinarySupported(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2) | (use_simd ? SLJIT_ENTER_VECTOR(3) : 0), 6,
	                 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));

	auto emit_row = [&]() {
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_S3), 0);
		sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S4), 0);
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
		sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_S5), 0);
	};

	if (use_simd) {
		auto simd_type = SljitArm64NeonFloatingSimdType(single_precision);
		auto simd_lanes = NumericCast<sljit_sw>(SljitArm64NeonFloatingLaneCount(single_precision));

		auto vector_loop = sljit_emit_label(compiler);
		auto tail = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S2, 0, SLJIT_IMM, simd_lanes);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR0, SLJIT_MEM1(SLJIT_S3), 0);
		sljit_emit_simd_mov(compiler, simd_type, SLJIT_VR1, SLJIT_MEM1(SLJIT_S4), 0);
		EmitSljitArm64NeonFloatingBinary(compiler, single_precision, op, SLJIT_VR2, SLJIT_VR0, SLJIT_VR1);
		sljit_emit_simd_mov(compiler, simd_type | SLJIT_SIMD_STORE, SLJIT_VR2, SLJIT_MEM1(SLJIT_S5), 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, 16);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S2, 0, SLJIT_S2, 0, SLJIT_IMM, simd_lanes);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, vector_loop);

		auto tail_loop = sljit_emit_label(compiler);
		sljit_set_label(tail, tail_loop);
		auto done = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		emit_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S2, 0, SLJIT_S2, 0, SLJIT_IMM, 1);
		repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, tail_loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	} else {
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		emit_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, data_width);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S5, 0, SLJIT_S5, 0, SLJIT_IMM, data_width);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, loop);
		sljit_set_label(done, sljit_emit_label(compiler));
	}

	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

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

static sljit_s32 SljitFlatDoubleProjectionStatsMinRegister(idx_t projection_idx) {
	return SLJIT_FR(NumericCast<sljit_s32>(4 + projection_idx * 2));
}

static sljit_s32 SljitFlatDoubleProjectionStatsMaxRegister(idx_t projection_idx) {
	return SLJIT_FR(NumericCast<sljit_s32>(5 + projection_idx * 2));
}

static sljit_s32 NativeFloatingCompare(sljit_s32 compare_type, bool single_precision) {
	return single_precision ? compare_type | SLJIT_32 : compare_type;
}

static void EmitSljitFloatingStatsInit(struct sljit_compiler *compiler, sljit_s32 move_op, sljit_s32 value_reg,
                                       sljit_s32 min_reg, sljit_s32 max_reg) {
	sljit_emit_fop1(compiler, move_op, min_reg, 0, value_reg, 0);
	sljit_emit_fop1(compiler, move_op, max_reg, 0, value_reg, 0);
}

static void EmitSljitFloatingStatsUpdate(struct sljit_compiler *compiler, sljit_s32 move_op, bool single_precision,
                                         sljit_s32 value_reg, sljit_s32 min_reg, sljit_s32 max_reg) {
	auto value_is_nan_for_min =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), value_reg, 0, value_reg, 0);
	auto min_is_nan =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), min_reg, 0, min_reg, 0);
	auto value_less_than_min = sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_ORDERED_LESS, single_precision),
	                                           value_reg, 0, min_reg, 0);
	auto min_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto set_min = sljit_emit_label(compiler);
	sljit_set_label(min_is_nan, set_min);
	sljit_set_label(value_less_than_min, set_min);
	sljit_emit_fop1(compiler, move_op, min_reg, 0, value_reg, 0);
	auto after_min = sljit_emit_label(compiler);
	sljit_set_label(value_is_nan_for_min, after_min);
	sljit_set_label(min_done, after_min);

	auto value_is_nan_for_max =
	    sljit_emit_fcmp(compiler, NativeFloatingCompare(SLJIT_UNORDERED, single_precision), value_reg, 0, value_reg, 0);
	auto value_greater_than_max = sljit_emit_fcmp(
	    compiler, NativeFloatingCompare(SLJIT_ORDERED_GREATER, single_precision), value_reg, 0, max_reg, 0);
	auto max_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto set_max = sljit_emit_label(compiler);
	sljit_set_label(value_is_nan_for_max, set_max);
	sljit_set_label(value_greater_than_max, set_max);
	sljit_emit_fop1(compiler, move_op, max_reg, 0, value_reg, 0);
	auto after_max = sljit_emit_label(compiler);
	sljit_set_label(max_done, after_max);
}

static void EmitSljitStoreFloatingStats(struct sljit_compiler *compiler, idx_t projection_index, bool single_precision,
                                        sljit_s32 min_reg, sljit_s32 max_reg, sljit_s32 stats_min_base,
                                        sljit_s32 stats_max_base) {
	auto move_op = NativeDirectFloatingMoveOp(single_precision);
	auto fmem_align = NativeDirectFloatingMemoryAlignment(single_precision);
	auto constant_width = NativeDirectFloatingDataWidth(single_precision);
	auto stats_offset = NumericCast<sljit_sw>(projection_index * constant_width);
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, min_reg, SLJIT_MEM1(stats_min_base),
	                stats_offset);
	sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, max_reg, SLJIT_MEM1(stats_max_base),
	                stats_offset);
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
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(stats_float_register_count), 7, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
		auto &source = shared_plan.sources[source_idx];
		auto source_array_offset = source.right_source ? offsetof(SljitNativeVectorInput, right_source_data_array)
		                                               : offsetof(SljitNativeVectorInput, source_data_array);
		auto source_pointer_offset = SljitPointerArrayOffset(source.projection_index);
		auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0), source_array_offset);
		sljit_emit_op1(compiler, SLJIT_MOV_P, source_pointer_reg, 0, SLJIT_MEM1(SLJIT_R2), source_pointer_offset);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_constants));

	auto emit_sources = [&]() {
		for (idx_t source_idx = 0; source_idx < shared_plan.sources.size(); source_idx++) {
			auto source_pointer_reg = source_idx == 0 ? SLJIT_S3 : SLJIT_S4;
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
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_FR3, SLJIT_MEM1(SLJIT_S6), constant_offset);
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
		auto min_reg = SljitFlatDoubleProjectionStatsMinRegister(fused_idx);
		auto max_reg = SljitFlatDoubleProjectionStatsMaxRegister(fused_idx);
		if (initialize_stats) {
			EmitSljitFloatingStatsInit(compiler, move_op, SLJIT_FR2, min_reg, max_reg);
		} else {
			EmitSljitFloatingStatsUpdate(compiler, move_op, single_precision, SLJIT_FR2, min_reg, max_reg);
		}
	};

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_stats_min));
	auto no_stats = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	auto stats_empty = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
	emit_sources();
	for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
		emit_projection(fused_idx, true, true);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto stats_loop = sljit_emit_label(compiler);
	auto stats_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_sources();
	for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
		emit_projection(fused_idx, false, true);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat_stats = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_stats, stats_loop);
	sljit_set_label(stats_done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_stats_min));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_stats_max));
	for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
		auto min_reg = SljitFlatDoubleProjectionStatsMinRegister(fused_idx);
		auto max_reg = SljitFlatDoubleProjectionStatsMaxRegister(fused_idx);
		EmitSljitStoreFloatingStats(compiler, projection_indices[fused_idx], single_precision, min_reg, max_reg,
		                            SLJIT_R2, SLJIT_R3);
	}
	auto stats_finished = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto old_loop_label = sljit_emit_label(compiler);
	sljit_set_label(no_stats, old_loop_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	emit_sources();
	for (idx_t fused_idx = 0; fused_idx < projection_indices.size(); fused_idx++) {
		emit_projection(fused_idx, false, false);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(stats_empty, done_label);
	sljit_set_label(stats_finished, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
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
	if (TryPlanSljitFlatProjectionSharedSources(plans, projection_indices,
	                                            SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES, 2, 8,
	                                            shared_source_plan)) {
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

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(4), 7, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, floating_constants));

	for (auto projection_index : projection_indices) {
		auto &plan = plans[projection_index];
		auto projection_pointer_offset = SljitPointerArrayOffset(projection_index);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S3), projection_pointer_offset);
		if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S4), projection_pointer_offset);
		} else {
			auto constant_offset = NumericCast<sljit_sw>(projection_index * constant_width);
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S6), constant_offset);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S5), projection_pointer_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);

		auto emit_projection_row = [&]() {
			sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale);
			if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT) {
				auto binary_op = NativeDoubleBinaryOp(plan.double_binary_op, single_precision);
				if (plan.constant_on_left) {
					sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR0, 0);
				} else {
					sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
				}
			} else {
				sljit_emit_fmem(compiler, move_op | fmem_align, SLJIT_TMP_FR1, SLJIT_MEM2(SLJIT_R1, SLJIT_S1),
				                data_scale);
				auto binary_op = NativeDoubleBinaryOp(plan.double_binary_op, single_precision);
				sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
			}
			sljit_emit_fmem(compiler, move_op | SLJIT_MEM_STORE | fmem_align, SLJIT_TMP_FR0,
			                SLJIT_MEM2(SLJIT_R2, SLJIT_S1), data_scale);
		};

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_stats_min));
		auto no_stats = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		auto stats_empty = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		emit_projection_row();
		EmitSljitFloatingStatsInit(compiler, move_op, SLJIT_TMP_FR0, SLJIT_FR2, SLJIT_FR3);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto stats_loop = sljit_emit_label(compiler);
		auto stats_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		emit_projection_row();
		EmitSljitFloatingStatsUpdate(compiler, move_op, single_precision, SLJIT_TMP_FR0, SLJIT_FR2, SLJIT_FR3);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat_stats = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_stats, stats_loop);
		sljit_set_label(stats_done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_stats_min));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, floating_stats_max));
		EmitSljitStoreFloatingStats(compiler, projection_index, single_precision, SLJIT_FR2, SLJIT_FR3, SLJIT_R3,
		                            SLJIT_R4);
		auto stats_finished = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto old_loop_label = sljit_emit_label(compiler);
		sljit_set_label(no_stats, old_loop_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		emit_projection_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat, loop);
		auto done_label = sljit_emit_label(compiler);
		sljit_set_label(done, done_label);
		sljit_set_label(stats_empty, done_label);
		sljit_set_label(stats_finished, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);

	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (needs_range_check) {
		range_too_small = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
		                                 NumericCast<sljit_sw>(NativeSignedIntegerMin(target_width)));
		range_too_large = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                 NumericCast<sljit_sw>(NativeSignedIntegerMax(target_width)));
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, target_store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), target_data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	if (needs_range_check && try_cast) {
		sljit_set_label(range_too_small, invalid_label);
		sljit_set_label(range_too_large, invalid_label);
	}
	EmitSetResultRowInvalid(compiler);
	auto next_after_invalid = sljit_emit_jump(compiler, SLJIT_JUMP);

	if (needs_range_check && !try_cast) {
		auto overflow_label = sljit_emit_label(compiler);
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_value),
		               SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeIntegerCastOverflow));
		auto after_overflow = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(after_overflow, sljit_emit_label(compiler));
		sljit_emit_return_void(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_set_label(next_after_invalid, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);

	auto range_too_small = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, 0);
	auto range_too_large = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
	                                      NumericCast<sljit_sw>(NativeUnsignedIntegerMax(target_width)));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, target_store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), target_data_scale, SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	if (try_cast) {
		sljit_set_label(range_too_small, invalid_label);
		sljit_set_label(range_too_large, invalid_label);
	}
	EmitSetResultRowInvalid(compiler);
	auto next_after_invalid = sljit_emit_jump(compiler, SLJIT_JUMP);

	if (!try_cast) {
		auto overflow_label = sljit_emit_label(compiler);
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, overflow_value),
		               SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeIntegerCastOverflow));
		auto after_overflow = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(after_overflow, sljit_emit_label(compiler));
		sljit_emit_return_void(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_set_label(next_after_invalid, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, SLJIT_R2, 0);
	auto next_after_source = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	struct sljit_jump *next_after_rhs = nullptr;
	if (rhs_kind == SljitNativeCoalesceRhsKind::CONSTANT) {
		if (!rhs_constant_is_null) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, constant));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, result_data));
			sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, SLJIT_R2, 0);
			next_after_rhs = sljit_emit_jump(compiler, SLJIT_JUMP);
		}
	} else {
		EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
		auto right_is_null =
		    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, right_source_data));
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), data_scale);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, SLJIT_R2, 0);
		next_after_rhs = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(right_is_null, sljit_emit_label(compiler));
	}

	EmitSetResultRowInvalid(compiler);
	auto next_after_invalid = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_source, next_label);
	if (next_after_rhs) {
		sljit_set_label(next_after_rhs, next_label);
	}
	sljit_set_label(next_after_invalid, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static void EmitLoadResultAndSourceIndexForSelect(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_result_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_set_label(have_result_index, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_RESULT_INDEX_OFFSET, SLJIT_R1, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 2);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
}

static void EmitStoreTrueSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_RESULT_INDEX_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_true_sel, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R2, 0);
}

static void EmitStoreFalseSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, false_sel));
	auto no_false_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_RESULT_INDEX_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_false_sel, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerCompareConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                       bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	struct sljit_jump *comparison_true;
	if (constant_on_left) {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(comparison_true, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	auto after_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_set_label(after_true, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static void EmitStoreBoolResult(struct sljit_compiler *compiler, bool value);

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerCompareReferences(SljitNativeIntegerKind kind,
                                                                               SljitNativeIntegerCompareOp op,
                                                                               SljitNativeVectorFunction &function,
                                                                               string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), data_scale);

	auto comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	EmitStoreBoolResult(compiler, false);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(comparison_true, sljit_emit_label(compiler));
	EmitStoreBoolResult(compiler, true);
	auto after_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_set_label(after_true, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerSelectConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                      bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);

	vector<sljit_jump *> generic_path;
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, false_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	struct sljit_jump *fast_comparison_true;
	if (constant_on_left) {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	auto fast_next = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_comparison_true, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R1, 0);
	auto fast_next_label = sljit_emit_label(compiler);
	sljit_set_label(fast_next, fast_next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(fast_done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	auto loop = sljit_emit_label(compiler);
	for (auto &jump : generic_path) {
		sljit_set_label(jump, loop);
	}
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadResultAndSourceIndexForSelect(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	struct sljit_jump *comparison_true;
	if (constant_on_left) {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}

	auto comparison_false = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, comparison_false);
	EmitStoreFalseSelection(compiler);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(comparison_true, sljit_emit_label(compiler));
	EmitStoreTrueSelection(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerSelectReferences(SljitNativeIntegerKind kind,
                                                                              SljitNativeIntegerCompareOp op,
                                                                              SljitNativeVectorFunction &function,
                                                                              string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, SLJIT_SELECT_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_result_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_set_label(have_result_index, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_RESULT_INDEX_OFFSET, SLJIT_R1, 0);

	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), data_scale);

	auto comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	auto comparison_false = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, comparison_false);
	sljit_set_label(right_is_null, comparison_false);
	EmitStoreFalseSelection(compiler);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(comparison_true, sljit_emit_label(compiler));
	EmitStoreTrueSelection(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeNullCheckSelect(SljitNativeNullCheckOp op, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadResultAndSourceIndexForSelect(compiler);
	auto source_is_null = EmitJumpIfSourceNull(compiler);

	if (op == SljitNativeNullCheckOp::IS_NULL) {
		EmitStoreFalseSelection(compiler);
	} else {
		EmitStoreTrueSelection(compiler);
	}
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	if (op == SljitNativeNullCheckOp::IS_NULL) {
		EmitStoreTrueSelection(compiler);
	} else {
		EmitStoreFalseSelection(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static void EmitStoreBoolResult(struct sljit_compiler *compiler, bool value) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, value ? 1 : 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerInList(SljitNativeIntegerKind kind, idx_t constant_count,
                                                                    bool list_has_null, bool not_in,
                                                                    SljitNativeVectorFunction &function,
                                                                    string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	auto constants_loop = sljit_emit_label(compiler);
	auto no_match =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, constants));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);
	auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto repeat_constants = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_constants, constants_loop);

	sljit_set_label(no_match, sljit_emit_label(compiler));
	if (list_has_null) {
		EmitSetResultRowInvalid(compiler);
	} else {
		EmitStoreBoolResult(compiler, not_in);
	}
	auto next_after_no_match = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(match, sljit_emit_label(compiler));
	EmitStoreBoolResult(compiler, !not_in);
	auto next_after_match = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_no_match, next_label);
	sljit_set_label(next_after_match, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static void EmitStoreInListSelectionResult(struct sljit_compiler *compiler, bool selected) {
	if (selected) {
		EmitStoreTrueSelection(compiler);
	} else {
		EmitStoreFalseSelection(compiler);
	}
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerInListSelect(SljitNativeIntegerKind kind, idx_t constant_count, bool list_has_null, bool not_in,
                                    SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 4, SLJIT_SELECT_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadResultAndSourceIndexForSelect(compiler);
	auto source_is_null = EmitJumpIfSourceNull(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	auto constants_loop = sljit_emit_label(compiler);
	auto no_match =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant_count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, constants));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);
	auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto repeat_constants = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_constants, constants_loop);

	sljit_set_label(no_match, sljit_emit_label(compiler));
	EmitStoreInListSelectionResult(compiler, !list_has_null && not_in);
	auto next_after_no_match = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(match, sljit_emit_label(compiler));
	EmitStoreInListSelectionResult(compiler, !not_in);
	auto next_after_match = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	EmitStoreFalseSelection(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_no_match, next_label);
	sljit_set_label(next_after_match, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

sljit_s32 NativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_LESS : SLJIT_SIG_LESS_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

sljit_s32 NativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_GREATER : SLJIT_SIG_GREATER_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetween(SljitNativeIntegerKind kind, int64_t lower,
                                                                     int64_t upper, bool lower_inclusive,
                                                                     bool upper_inclusive, bool not_between,
                                                                     SljitNativeVectorFunction &function,
                                                                     string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);
	auto lower_failure = NativeIntegerLowerBoundFailureJump(kind, lower_inclusive);
	auto upper_failure = NativeIntegerUpperBoundFailureJump(kind, upper_inclusive);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	auto lower_failed = sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(lower));
	auto upper_failed = sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(upper));

	EmitStoreBoolResult(compiler, !not_between);
	auto next_after_success = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto failed_label = sljit_emit_label(compiler);
	sljit_set_label(lower_failed, failed_label);
	sljit_set_label(upper_failed, failed_label);
	EmitStoreBoolResult(compiler, not_between);
	auto next_after_failure = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_success, next_label);
	sljit_set_label(next_after_failure, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetweenSelect(SljitNativeIntegerKind kind, int64_t lower,
                                                                           int64_t upper, bool lower_inclusive,
                                                                           bool upper_inclusive, bool not_between,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);
	auto lower_failure = NativeIntegerLowerBoundFailureJump(kind, lower_inclusive);
	auto upper_failure = NativeIntegerUpperBoundFailureJump(kind, upper_inclusive);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadResultAndSourceIndexForSelect(compiler);
	auto source_is_null = EmitJumpIfSourceNull(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, source_load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), source_data_scale);
	auto lower_failed = sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(lower));
	auto upper_failed = sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(upper));

	EmitStoreInListSelectionResult(compiler, !not_between);
	auto next_after_success = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto failed_label = sljit_emit_label(compiler);
	sljit_set_label(lower_failed, failed_label);
	sljit_set_label(upper_failed, failed_label);
	EmitStoreInListSelectionResult(compiler, not_between);
	auto next_after_failure = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	EmitStoreFalseSelection(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_success, next_label);
	sljit_set_label(next_after_failure, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, selected_count),
	               SLJIT_R0, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

} // namespace duckdb

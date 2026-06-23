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
                                      SljitNativeVectorFunction &function, string &error, bool check_result_range,
                                      int64_t result_min, int64_t result_max) {
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (constant_on_left) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		break;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
	auto overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
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

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                              SljitNativeIntegerBinaryOp op,
                                                                              SljitNativeVectorFunction &function,
                                                                              string &error, bool check_result_range,
                                                                              int64_t result_min, int64_t result_max) {
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

	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	auto overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
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

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::ADD:
		return SLJIT_ADD_F64;
	case SljitNativeDoubleBinaryOp::SUBTRACT:
		return SLJIT_SUB_F64;
	case SljitNativeDoubleBinaryOp::MULTIPLY:
		return SLJIT_MUL_F64;
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return SLJIT_DIV_F64;
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
                                 sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), data_offset);
	switch (kind) {
	case SljitNativeDoubleSourceKind::DOUBLE:
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		return;
	case SljitNativeDoubleSourceKind::INT64_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_SW, target, 0, SLJIT_R2, 0);
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

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind source_kind,
                                     bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
	                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S0),
	                offsetof(SljitNativeVectorInput, double_constant));
	if (constant_on_left) {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR0, 0);
	} else {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
	                SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
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

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDoubleBinaryReferences(SljitNativeDoubleBinaryOp op,
                                                                             SljitNativeDoubleSourceKind left_kind,
                                                                             SljitNativeDoubleSourceKind right_kind,
                                                                             SljitNativeVectorFunction &function,
                                                                             string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);
	auto needs_helper_spill = NativeDoubleSourceUsesHelper(left_kind) || NativeDoubleSourceUsesHelper(right_kind);
	constexpr sljit_sw left_spill_offset = 0;
	constexpr sljit_sw right_spill_offset = sizeof(double);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5,
	                 needs_helper_spill ? sizeof(double) * 2 : 0);
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
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
		                SLJIT_MEM1(SLJIT_SP), left_spill_offset);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
		                SLJIT_MEM1(SLJIT_SP), right_spill_offset);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_SP),
		                left_spill_offset);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
		                right_spill_offset);
	} else {
		EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR1);
	}
	sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
	                SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
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

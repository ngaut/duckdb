#include "sljit_native_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/string_vector.hpp"

#include "sljitLir.h"

#include <algorithm>
#include <cstddef>
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

static void SljitNativeTreeOverflow(SljitNativeVectorInput *input, const char *message) {
	try {
		throw OutOfRangeException("%s", message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeTreeAddOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in addition");
}

static void SLJIT_FUNC SljitNativeTreeSubtractOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in subtraction");
}

static void SLJIT_FUNC SljitNativeTreeMultiplyOverflow(SljitNativeVectorInput *input) {
	SljitNativeTreeOverflow(input, "Overflow in multiplication");
}

static void SljitNativeAggregateTreeOverflow(SljitNativeVectorInput *input, const char *message) {
	try {
		throw OutOfRangeException("%s", message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeAggregateTreeAddOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in addition");
}

static void SLJIT_FUNC SljitNativeAggregateTreeSubtractOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in subtraction");
}

static void SLJIT_FUNC SljitNativeAggregateTreeMultiplyOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in multiplication");
}

static void SLJIT_FUNC SljitNativeIntegerCastOverflow(SljitNativeVectorInput *input) {
	try {
		throw ConversionException(input->overflow_message, input->overflow_value);
	} catch (...) {
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeRuntimeError(SljitNativeVectorInput *input) {
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

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitNativeVectorCode(struct sljit_compiler *compiler, SljitNativeVectorFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeVectorFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitNativeAggregateUpdateCode(struct sljit_compiler *compiler, SljitNativeAggregateUpdateFunction &function,
                                     string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeAggregateUpdateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitNativePredicateCode(struct sljit_compiler *compiler, SljitNativePredicateFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativePredicateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static void EmitLoadLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadSourceIndex(struct sljit_compiler *compiler, sljit_sw sel_offset, sljit_s32 logical_index,
                                sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), sel_offset);
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_offset,
                                                 sljit_s32 index_reg) {
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

static void EmitLoadSelectedIndex(struct sljit_compiler *compiler) {
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

static struct sljit_jump *EmitSkipInvalidSourceRow(struct sljit_compiler *compiler) {
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

static void EmitSetResultRowInvalid(struct sljit_compiler *compiler) {
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

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
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

struct SljitExpressionTreeOverflowJumps {
	SljitNativeIntegerBinaryOp op = SljitNativeIntegerBinaryOp::ADD;
	vector<sljit_jump *> jumps;
};

static bool SljitExpressionTreeIsDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitExpressionTreeBinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

static bool TryGetSljitExpressionTreeBinaryOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerBinaryOp &native_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
		native_op = SljitNativeIntegerBinaryOp::ADD;
		return true;
	case ExecutionExpressionBinaryOp::SUBTRACT:
		native_op = SljitNativeIntegerBinaryOp::SUBTRACT;
		return true;
	case ExecutionExpressionBinaryOp::MULTIPLY:
		native_op = SljitNativeIntegerBinaryOp::MULTIPLY;
		return true;
	default:
		return false;
	}
}

static bool TryGetSljitExpressionTreeDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max) {
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	result_max = NumericHelper::POWERS_OF_TEN[width] - 1;
	result_min = -result_max;
	return true;
}

static bool SljitExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		return SljitExpressionTreeIsDecimal64Node(node);
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return !node.constant.IsNull() && SljitExpressionTreeIsDecimal64Node(node);
	}
	SljitNativeIntegerBinaryOp native_op;
	int64_t result_min;
	int64_t result_max;
	if (node.kind != ExecutionExpressionIRKind::BINARY || !node.left || !node.right ||
	    !SljitExpressionTreeIsDecimal64Node(node) || !SljitExpressionTreeIsDecimal64Node(*node.left) ||
	    !SljitExpressionTreeIsDecimal64Node(*node.right) ||
	    !TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op) ||
	    !SljitExpressionTreeBinaryHasRawSemantics(node) ||
	    !TryGetSljitExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		return false;
	}
	return SljitExpressionTreeIsSupported(*node.left) && SljitExpressionTreeIsSupported(*node.right);
}

static idx_t CountSljitExpressionTreeSpills(const ExecutionExpressionIR &node) {
	if (node.kind != ExecutionExpressionIRKind::BINARY) {
		return 0;
	}
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	return 1 + CountSljitExpressionTreeSpills(*node.left) + CountSljitExpressionTreeSpills(*node.right);
}

static void CollectSljitExpressionTreeSourceRefs(const ExecutionExpressionIR &node, vector<idx_t> &refs) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (std::find(refs.begin(), refs.end(), node.ref_index) == refs.end()) {
			refs.push_back(node.ref_index);
		}
		return;
	}
	if (node.left) {
		CollectSljitExpressionTreeSourceRefs(*node.left, refs);
	}
	if (node.right) {
		CollectSljitExpressionTreeSourceRefs(*node.right, refs);
	}
}

static void AddSljitExpressionOverflowJump(vector<SljitExpressionTreeOverflowJumps> &overflows,
                                           SljitNativeIntegerBinaryOp op, sljit_jump *jump) {
	for (auto &entry : overflows) {
		if (entry.op == op) {
			entry.jumps.push_back(jump);
			return;
		}
	}
	SljitExpressionTreeOverflowJumps entry;
	entry.op = op;
	entry.jumps.push_back(jump);
	overflows.push_back(std::move(entry));
}

static void EmitLoadSljitExpressionTreeLogicalIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadSljitExpressionTreeSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                                   sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
	               NumericCast<sljit_sw>(source_index * sizeof(const sel_t *)));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
	               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_R0, 0);
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S4, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitLoadSljitExpressionTreeReference(struct sljit_compiler *compiler, idx_t source_index,
                                                 sljit_s32 target) {
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, SLJIT_R1), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
}

static void EmitSljitExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeTreeAddOverflow));
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeSubtractOverflow));
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeTreeMultiplyOverflow));
		return;
	default:
		throw InternalException("Unknown SLJIT expression-tree overflow operator");
	}
}

static void EmitSljitAggregateExpressionTreeOverflowCall(struct sljit_compiler *compiler,
                                                         SljitNativeIntegerBinaryOp op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeAddOverflow));
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeSubtractOverflow));
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeMultiplyOverflow));
		return;
	default:
		throw InternalException("Unknown SLJIT aggregate expression-tree overflow operator");
	}
}

static void EmitSljitAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                              sljit_sw saw_value_offset, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitSljitAggregateCommitInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                          sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitSljitAggregateLoopStep(struct sljit_compiler *compiler, struct sljit_label *loop) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
}

static void EmitSljitExpressionTreeValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                         sljit_s32 target, idx_t &spill_index,
                                         vector<SljitExpressionTreeOverflowJumps> &overflows) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(node.constant.GetValueUnsafe<int64_t>()));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		EmitLoadSljitExpressionTreeReference(compiler, node.ref_index, target);
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::BINARY);
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT expression-tree binary operator");
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValue(compiler, *node.right, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	auto binary_op = NativeIntegerBinaryOp(SljitNativeIntegerKind::DECIMAL64, native_op);
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, target, 0, SLJIT_R4, 0, target, 0);
	AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));

	int64_t result_min;
	int64_t result_max;
	if (!TryGetSljitExpressionTreeDecimal64Range(node.return_type, result_min, result_max)) {
		throw InternalException("SLJIT expression-tree binary node missing decimal64 result range");
	}
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min)));
	AddSljitExpressionOverflowJump(
	    overflows, native_op,
	    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, target, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max)));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeVectorFunction &function, string &error) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT expression-tree codegen only supports checked DECIMAL64 arithmetic trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto local_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64ExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT aggregate reducer only supports checked DECIMAL64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto spill_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	const auto local_sum_offset = spill_size;
	const auto saw_value_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

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
	sljit_set_label(done, done_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));

	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumInt64Reference(SljitNativeIntegerKind kind, idx_t state_value_offset,
                                         idx_t state_is_set_offset,
                                         SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto value_offset = NumericCast<sljit_sw>(state_value_offset);
	auto is_set_offset = NumericCast<sljit_sw>(state_is_set_offset);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R3), value_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), value_offset, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R3), is_set_offset, SLJIT_IMM, 1);

	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);
	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
    SljitNativeAggregateUpdateFunction &function, string &error, bool check_result_range, int64_t result_min,
    int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	if (constant_on_left) {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
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
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
	auto helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_set_label(helper_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeAggregateUpdateFunction &function,
    string &error, bool check_result_range, int64_t result_min, int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

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
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
	auto helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_set_label(helper_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
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

static sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op) {
	switch (op) {
	case SljitNativeDoubleBinaryOp::DIVIDE:
		return SLJIT_DIV_F64;
	default:
		throw InternalException("Unknown SLJIT native double binary operator");
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                                           bool constant_on_left,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3);
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
                                                                             SljitNativeVectorFunction &function,
                                                                             string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);

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
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 3);
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

static constexpr sljit_sw SLJIT_SELECT_TRUE_COUNT_OFFSET = 0;
static constexpr sljit_sw SLJIT_SELECT_RESULT_INDEX_OFFSET = sizeof(sljit_sw);
static constexpr sljit_sw SLJIT_SELECT_LOCAL_SIZE = 2 * sizeof(sljit_sw);

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

static sljit_s32 NativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_LESS : SLJIT_SIG_LESS_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

static sljit_s32 NativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
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

struct SljitPredicateBranches {
	vector<sljit_jump *> true_jumps;
	vector<sljit_jump *> false_jumps;
	vector<sljit_jump *> null_jumps;
};

static void AppendPredicateJumps(vector<sljit_jump *> &target, vector<sljit_jump *> source) {
	for (auto &jump : source) {
		target.push_back(jump);
	}
}

static void AppendPredicateBranches(SljitPredicateBranches &target, SljitPredicateBranches source) {
	AppendPredicateJumps(target.true_jumps, std::move(source.true_jumps));
	AppendPredicateJumps(target.false_jumps, std::move(source.false_jumps));
	AppendPredicateJumps(target.null_jumps, std::move(source.null_jumps));
}

static void SetPredicateJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto &jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static sljit_sw SljitPointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

static void EmitLoadPredicateLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadPredicateResultAndLogicalIndexForSelect(struct sljit_compiler *compiler) {
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);
}

static void EmitLoadPredicateResultIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_result_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_result_index, sljit_emit_label(compiler));
}

static void EmitLoadPredicateSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 logical_index,
                                         sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfPredicateSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                        sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_validity));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
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

static void EmitLoadPredicateSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                        sljit_s32 index_reg, sljit_sw data_scale, sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, index_reg), data_scale);
}

static void EmitLoadPredicateStringPointer(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                           sljit_s32 index_reg) {
	static_assert(sizeof(string_t) == 16, "SLJIT string prefix expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(target), SljitPointerArrayOffset(source_index));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index_reg, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R4, 0);
}

static void EmitLoadPredicateStringDataPointer(struct sljit_compiler *compiler, sljit_s32 string_pointer_reg,
                                               sljit_s32 string_length_reg, sljit_s32 target_reg) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;
	auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, string_length_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, string_pointer_reg, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(string_pointer_reg), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitLoadPredicateStringForMatch(struct sljit_compiler *compiler, idx_t source_index,
                                            SljitPredicateBranches &result) {
	static_assert(sizeof(string_t) == 16, "SLJIT string match expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
	result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
	EmitLoadPredicateStringPointer(compiler, source_index, SLJIT_R0, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	EmitLoadPredicateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
}

static void EmitStringEqualsAtPosition(struct sljit_compiler *compiler, const string &constant, sljit_s32 data_reg,
                                       sljit_s32 position_reg, vector<sljit_jump *> &mismatch_jumps) {
	for (idx_t byte_idx = 0; byte_idx < constant.size(); byte_idx++) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, position_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(byte_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM2(data_reg, SLJIT_R0), 0);
		mismatch_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(static_cast<uint8_t>(constant[byte_idx]))));
	}
}

static void EmitStringPrefixBranches(struct sljit_compiler *compiler, const string &prefix,
                                     SljitPredicateBranches &result) {
	const auto prefix_length = prefix.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix_length)));
	if (prefix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringEqualsAtPosition(compiler, prefix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringEqualBranches(struct sljit_compiler *compiler, const string &constant,
                                    SljitPredicateBranches &result) {
	const auto constant_length = constant.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant_length)));
	if (constant_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringEqualsAtPosition(compiler, constant, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringInListBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                     SljitPredicateBranches &result) {
	for (auto &constant : predicate.string_constants) {
		vector<sljit_jump *> next_candidate_jumps;
		next_candidate_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant.size())));
		if (!constant.empty()) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
			EmitStringEqualsAtPosition(compiler, constant, SLJIT_R4, SLJIT_S4, next_candidate_jumps);
		}
		if (predicate.not_in) {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		SetPredicateJumpLabels(next_candidate_jumps, sljit_emit_label(compiler));
	}
	if (predicate.list_has_null) {
		result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else if (predicate.not_in) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else {
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}
}

static void EmitStringSuffixBranches(struct sljit_compiler *compiler, const string &suffix,
                                     SljitPredicateBranches &result) {
	const auto suffix_length = suffix.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(suffix_length)));
	if (suffix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S4, 0, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(suffix_length));
	EmitStringEqualsAtPosition(compiler, suffix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringFindFromCurrentPosition(struct sljit_compiler *compiler, const string &needle, sljit_s32 data_reg,
                                              sljit_s32 length_reg, sljit_s32 position_reg,
                                              vector<sljit_jump *> &false_jumps) {
	const auto needle_length = needle.size();
	if (needle_length == 0) {
		return;
	}
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, length_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(needle_length)));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, length_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(needle_length));
	false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER, position_reg, 0, SLJIT_R1, 0));
	auto loop = sljit_emit_label(compiler);
	vector<sljit_jump *> mismatch_jumps;
	EmitStringEqualsAtPosition(compiler, needle, data_reg, position_reg, mismatch_jumps);
	auto found = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto next_position = sljit_emit_label(compiler);
	SetPredicateJumpLabels(mismatch_jumps, next_position);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, position_reg, 0, SLJIT_R1, 0);
	false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto found_label = sljit_emit_label(compiler);
	sljit_set_label(repeat, loop);
	sljit_set_label(found, found_label);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(needle_length));
}

static void EmitStringContainsBranches(struct sljit_compiler *compiler, const string &needle,
                                       SljitPredicateBranches &result) {
	if (needle.empty()) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringFindFromCurrentPosition(compiler, needle, SLJIT_R4, SLJIT_R2, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringLikeBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                   SljitPredicateBranches &result) {
	if (predicate.string_constants.empty()) {
		if (predicate.string_anchor_start && predicate.string_anchor_end) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	idx_t fragment_idx = 0;
	if (predicate.string_anchor_start) {
		auto &prefix = predicate.string_constants[0];
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size())));
		EmitStringEqualsAtPosition(compiler, prefix, SLJIT_R4, SLJIT_S4, result.false_jumps);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size()));
		fragment_idx = 1;
	}
	for (; fragment_idx < predicate.string_constants.size(); fragment_idx++) {
		auto &fragment = predicate.string_constants[fragment_idx];
		const bool is_last = fragment_idx + 1 == predicate.string_constants.size();
		if (is_last && predicate.string_anchor_end) {
			result.false_jumps.push_back(
			    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(fragment.size())));
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(fragment.size()));
			result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R1, 0, SLJIT_S4, 0));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_R1, 0);
			EmitStringEqualsAtPosition(compiler, fragment, SLJIT_R4, SLJIT_S4, result.false_jumps);
		} else {
			EmitStringFindFromCurrentPosition(compiler, fragment, SLJIT_R4, SLJIT_R2, SLJIT_S4, result.false_jumps);
		}
	}
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitPredicateStoreTrueSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	EmitLoadPredicateResultIndex(compiler, SLJIT_R3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, true_sel));
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_true_sel, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R2, 0);
}

static void EmitPredicateStoreFalseSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_R2, 0);
	EmitLoadPredicateResultIndex(compiler, SLJIT_R3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, false_sel));
	auto no_false_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_false_sel, sljit_emit_label(compiler));
}

static void EmitStorePredicateResultInvalid(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, result_validity));
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_XOR, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, -1);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3, SLJIT_R3, 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeConstantOrNull(const vector<idx_t> &guard_source_indices,
                                                                     SljitNativePredicateFunction &function,
                                                                     string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);

	vector<sljit_jump *> invalid_jumps;
	for (auto source_index : guard_source_indices) {
		EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
		invalid_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
	}

	auto next_valid = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(invalid_jumps, invalid_label);
	EmitStorePredicateResultInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_valid, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativePredicateCode(compiler, function, error);
}

static SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                         const SljitNativePredicate &predicate);

static SljitPredicateBranches EmitSljitConjunctionBranches(struct sljit_compiler *compiler,
                                                           const SljitNativePredicate &predicate, idx_t child_index,
                                                           bool null_pending) {
	SljitPredicateBranches result;
	if (child_index >= predicate.children.size()) {
		if (null_pending) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}

	auto child = EmitSljitPredicateBranches(compiler, *predicate.children[child_index]);
	if (predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		auto true_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.true_jumps, true_label);
		auto true_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending);
		AppendPredicateBranches(result, std::move(true_rest));

		auto null_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.null_jumps, null_label);
		auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true);
		AppendPredicateBranches(result, std::move(null_rest));

		AppendPredicateJumps(result.false_jumps, std::move(child.false_jumps));
		return result;
	}

	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.false_jumps, false_label);
	auto false_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending);
	AppendPredicateBranches(result, std::move(false_rest));

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.null_jumps, null_label);
	auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true);
	AppendPredicateBranches(result, std::move(null_rest));

	AppendPredicateJumps(result.true_jumps, std::move(child.true_jumps));
	return result;
}

static SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                         const SljitNativePredicate &predicate) {
	SljitPredicateBranches result;
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		if (predicate.constant_is_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.constant_value) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	case SljitNativePredicateKind::REFERENCE: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, 0, SLJIT_MOV_U8);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::NOT: {
		auto child = EmitSljitPredicateBranches(compiler, *predicate.child);
		result.true_jumps = std::move(child.false_jumps);
		result.false_jumps = std::move(child.true_jumps);
		result.null_jumps = std::move(child.null_jumps);
		return result;
	}
	case SljitNativePredicateKind::CONJUNCTION:
		return EmitSljitConjunctionBranches(compiler, predicate, 0, false);
	case SljitNativePredicateKind::CONSTANT_OR_NULL: {
		if (predicate.guard_has_null_constant) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
		}
		for (auto source_index : predicate.guard_source_indices) {
			EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
			result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
		}
		auto child = EmitSljitPredicateBranches(compiler, *predicate.child);
		AppendPredicateBranches(result, std::move(child));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant), SLJIT_R2, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant)));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		EmitLoadPredicateSourceData(compiler, predicate.right_source_index, SLJIT_R3, SLJIT_S4, data_scale, load_op);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_IN_LIST: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		for (auto constant : predicate.constants) {
			auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant));
			if (predicate.not_in) {
				result.false_jumps.push_back(match);
			} else {
				result.true_jumps.push_back(match);
			}
		}
		if (predicate.list_has_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.not_in) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure = NativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure = NativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		auto lower_failed =
		    sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(predicate.lower));
		auto upper_failed =
		    sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(predicate.upper));
		if (predicate.not_between) {
			result.true_jumps.push_back(lower_failed);
			result.true_jumps.push_back(upper_failed);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(lower_failed);
			result.false_jumps.push_back(upper_failed);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringEqualBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringInListBranches(compiler, predicate, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringPrefixBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringSuffixBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringContainsBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringLikeBranches(compiler, predicate, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT: {
		static_assert(sizeof(string_t) == 16, "SLJIT string substring expects DuckDB string_t ABI size");
		static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
		static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
		const auto substring_length = predicate.substring_length;
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateStringPointer(compiler, predicate.source_index, SLJIT_R0, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(substring_length)));
		if (substring_length == 0) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
		}
		if (substring_length > string_t::PREFIX_LENGTH) {
			EmitLoadPredicateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
		}
		for (auto &constant : predicate.string_constants) {
			vector<sljit_jump *> mismatch_jumps;
			for (idx_t byte_idx = 0; byte_idx < substring_length; byte_idx++) {
				if (byte_idx < string_t::PREFIX_LENGTH) {
					sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0),
					               STRING_INLINE_PREFIX_OFFSET + NumericCast<sljit_sw>(byte_idx));
				} else {
					sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R4),
					               NumericCast<sljit_sw>(byte_idx));
				}
				mismatch_jumps.push_back(
				    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
				                   NumericCast<sljit_sw>(static_cast<uint8_t>(constant[byte_idx]))));
			}
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			SetPredicateJumpLabels(mismatch_jumps, sljit_emit_label(compiler));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::NULL_CHECK: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		auto source_is_null = EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1);
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			result.true_jumps.push_back(source_is_null);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(source_is_null);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePredicate(const SljitNativePredicate &predicate,
                                                                bool materialize_result,
                                                                SljitNativePredicateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto local_size = materialize_result ? 0 : SLJIT_SELECT_LOCAL_SIZE;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	if (materialize_result) {
		EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);
	} else {
		EmitLoadPredicateResultAndLogicalIndexForSelect(compiler);
	}

	auto branches = EmitSljitPredicateBranches(compiler, predicate);
	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.false_jumps, false_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}
	auto next_after_false = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto true_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.true_jumps, true_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	} else {
		EmitPredicateStoreTrueSelection(compiler);
	}
	auto next_after_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.null_jumps, null_label);
	if (materialize_result) {
		EmitStorePredicateResultInvalid(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_false, next_label);
	sljit_set_label(next_after_true, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, selected_count),
		               SLJIT_R0, 0);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativePredicateCode(compiler, function, error);
}

} // namespace duckdb

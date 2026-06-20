#include "sljit_native_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/string_vector.hpp"

#include "sljitLir.h"

#include <algorithm>
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

static double SLJIT_FUNC SljitNativeHugeintToDouble(uint64_t lower, int64_t upper) {
	hugeint_t value;
	value.lower = lower;
	value.upper = upper;
	return Hugeint::Cast<double>(value);
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

static void SLJIT_FUNC SljitNativeAggregateHugeintCommit(SljitNativeVectorInput *input) {
	try {
		if (!input->aggregate_hugeint_value || !input->aggregate_state_is_set) {
			throw InternalException("SLJIT hugeint aggregate primitive state is incomplete");
		}
		*input->aggregate_hugeint_value = Hugeint::Add(*input->aggregate_hugeint_value, input->aggregate_local_hugeint);
		*input->aggregate_state_is_set = true;
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
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
	// SLJIT_S4 holds the loop-invariant source_sel_array base.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S4),
	               NumericCast<sljit_sw>(source_index * sizeof(const sel_t *)));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	// SLJIT_S6 holds the loop-invariant source_validity_array base (hoisted by the caller); index
	// straight to this column's validity pointer instead of reloading the base array every row.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
	               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static sljit_jump *EmitJumpIfSljitExpressionTreeFlatSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
	               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitLoadSljitExpressionTreeReference(struct sljit_compiler *compiler, idx_t source_index,
                                                 sljit_s32 target) {
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	// SLJIT_S5 holds the loop-invariant source_data_array base (hoisted out of the row loop by the
	// caller), so the per-reference base load is gone; index directly to this column's data pointer.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, SLJIT_R1), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
}

static void EmitLoadSljitExpressionTreeReferenceFast(struct sljit_compiler *compiler, idx_t source_index,
                                                     sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(source_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(SljitNativeIntegerKind::DECIMAL64), target, 0,
	               SLJIT_MEM2(SLJIT_R0, SLJIT_S1), NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64));
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

static void EmitSljitAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                                     sljit_sw local_upper_offset, sljit_sw saw_value_offset,
                                                     sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_lower_offset, SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_upper_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitSljitAggregateCommitHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                            sljit_sw local_upper_offset, sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitSljitGroupedAggregateStatePointer(struct sljit_compiler *compiler, sljit_s32 logical_index,
                                                  sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R0, 0);
}

static void EmitSljitGroupedAggregateSetStateIsSet(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

static void EmitSljitGroupedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                     sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

static void EmitSljitGroupedAggregateIncrementInt64(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                            sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper), SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

static void EmitSljitAggregateLoopStep(struct sljit_compiler *compiler, struct sljit_label *loop) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
}

static sljit_sw SljitPointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

static bool SljitExpressionTreeIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE;
}

static void EmitSljitExpressionTreeCheckedBinaryOp(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                   SljitNativeIntegerBinaryOp native_op, sljit_s32 target,
                                                   sljit_s32 left_reg, sljit_s32 right_reg,
                                                   vector<SljitExpressionTreeOverflowJumps> &overflows) {
	auto binary_op = NativeIntegerBinaryOp(SljitNativeIntegerKind::DECIMAL64, native_op);
	if (!node.arithmetic_overflow_check) {
		sljit_emit_op2(compiler, binary_op, target, 0, left_reg, 0, right_reg, 0);
		return;
	}
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, target, 0, left_reg, 0, right_reg, 0);
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
	if (SljitExpressionTreeIsLeaf(*node.right)) {
		D_ASSERT(target != SLJIT_R4);
		EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows);
		EmitSljitExpressionTreeValue(compiler, *node.right, SLJIT_R4, spill_index, overflows);
		EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, target, SLJIT_R4, overflows);
		return;
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValue(compiler, *node.left, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValue(compiler, *node.right, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, SLJIT_R4, target, overflows);
}

static void EmitSljitExpressionTreeValueFast(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             sljit_s32 target, idx_t &spill_index,
                                             vector<SljitExpressionTreeOverflowJumps> &overflows) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(node.constant.GetValueUnsafe<int64_t>()));
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		EmitLoadSljitExpressionTreeReferenceFast(compiler, node.ref_index, target);
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::BINARY);
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT expression-tree binary operator");
	}
	if (SljitExpressionTreeIsLeaf(*node.right)) {
		D_ASSERT(target != SLJIT_R4);
		EmitSljitExpressionTreeValueFast(compiler, *node.left, target, spill_index, overflows);
		EmitSljitExpressionTreeValueFast(compiler, *node.right, SLJIT_R4, spill_index, overflows);
		EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, target, SLJIT_R4, overflows);
		return;
	}
	auto local_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	EmitSljitExpressionTreeValueFast(compiler, *node.left, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_offset, target, 0);
	EmitSljitExpressionTreeValueFast(compiler, *node.right, target, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), local_offset);
	EmitSljitExpressionTreeCheckedBinaryOp(compiler, node, native_op, target, SLJIT_R4, target, overflows);
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
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	// Hoist loop-invariant vector-format arrays so source indexing and references do not reload them
	// for every expression node in the fused tree.
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValueFast(compiler, root, SLJIT_R2, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

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
	sljit_set_label(fast_done, done_label);
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static bool SljitTypedExpressionTreeIsInt64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() != LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

static bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() != LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT32;
}

static bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::BOOLEAN && node.physical_type == PhysicalType::BOOL;
}

static bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node) ||
	       SljitTypedExpressionTreeIsBoolNode(node);
}

static bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node);
}

static bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left,
                                                    const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right));
}

static bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left,
                                                  const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right)) ||
	       (SljitTypedExpressionTreeIsBoolNode(left) && SljitTypedExpressionTreeIsBoolNode(right));
}

static bool SljitTypedExpressionTreeComparisonSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

static bool SljitTypedExpressionTreeIsSupported(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
	case ExecutionExpressionIRKind::CONSTANT:
		return SljitTypedExpressionTreeIsValueNode(node);
	case ExecutionExpressionIRKind::CAST:
		return !node.try_cast && node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR &&
		       node.exception_behavior != ExecutionExpressionExceptionKind::ERROR &&
		       SljitTypedExpressionTreeIsInt64Node(node) && node.left &&
		       SljitTypedExpressionTreeIsIntegerNode(*node.left) && SljitTypedExpressionTreeIsSupported(*node.left);
	case ExecutionExpressionIRKind::UNARY:
		if (!node.left) {
			return false;
		}
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::NOT:
			return SljitTypedExpressionTreeIsBoolNode(node) && SljitTypedExpressionTreeIsBoolNode(*node.left) &&
			       SljitTypedExpressionTreeIsSupported(*node.left);
		case ExecutionExpressionUnaryOp::IS_NULL:
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			return SljitTypedExpressionTreeIsBoolNode(node) && SljitTypedExpressionTreeIsValueNode(*node.left) &&
			       SljitTypedExpressionTreeIsSupported(*node.left);
		default:
			return false;
		}
	case ExecutionExpressionIRKind::BINARY:
		if (!node.left || !node.right) {
			return false;
		}
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			return SljitTypedExpressionTreeIsBoolNode(node) &&
			       SljitTypedExpressionTreeSameIntegerKind(*node.left, *node.right) &&
			       SljitTypedExpressionTreeIsSupported(*node.left) && SljitTypedExpressionTreeIsSupported(*node.right);
		}
		{
			SljitNativeIntegerBinaryOp native_op;
			return SljitTypedExpressionTreeIsIntegerNode(node) &&
			       SljitTypedExpressionTreeSameIntegerKind(node, *node.left) &&
			       SljitTypedExpressionTreeSameIntegerKind(node, *node.right) &&
			       TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op) &&
			       SljitTypedExpressionTreeIsSupported(*node.left) && SljitTypedExpressionTreeIsSupported(*node.right);
		}
	case ExecutionExpressionIRKind::CONJUNCTION:
		if (!SljitTypedExpressionTreeIsBoolNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !SljitTypedExpressionTreeIsBoolNode(*child) || !SljitTypedExpressionTreeIsSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::COALESCE:
		if (!SljitTypedExpressionTreeIsValueNode(node) || node.children.empty()) {
			return false;
		}
		for (auto &child : node.children) {
			if (!child || !SljitTypedExpressionTreeSameValueKind(node, *child) ||
			    !SljitTypedExpressionTreeIsSupported(*child)) {
				return false;
			}
		}
		return true;
	case ExecutionExpressionIRKind::CASE:
		if (!SljitTypedExpressionTreeIsValueNode(node) || !node.else_node || node.children.empty() ||
		    node.children.size() % 2 != 0 || !SljitTypedExpressionTreeSameValueKind(node, *node.else_node) ||
		    !SljitTypedExpressionTreeIsSupported(*node.else_node)) {
			return false;
		}
		for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
			auto &condition = node.children[child_idx];
			auto &value = node.children[child_idx + 1];
			if (!condition || !value || !SljitTypedExpressionTreeIsBoolNode(*condition) ||
			    !SljitTypedExpressionTreeSameValueKind(node, *value) ||
			    !SljitTypedExpressionTreeIsSupported(*condition) || !SljitTypedExpressionTreeIsSupported(*value)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool SljitTypedExpressionTreeInt64CastSupported(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CAST && SljitTypedExpressionTreeIsSupported(node);
}

static bool SljitTypedExpressionTreeFastPathSupported(const ExecutionExpressionIR &node) {
	if (SljitTypedExpressionTreeInt64CastSupported(node)) {
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT && node.constant.IsNull()) {
		return false;
	}
	if (node.left && !SljitTypedExpressionTreeFastPathSupported(*node.left)) {
		return false;
	}
	if (node.right && !SljitTypedExpressionTreeFastPathSupported(*node.right)) {
		return false;
	}
	if (node.else_node && !SljitTypedExpressionTreeFastPathSupported(*node.else_node)) {
		return false;
	}
	for (auto &child : node.children) {
		if (!child || !SljitTypedExpressionTreeFastPathSupported(*child)) {
			return false;
		}
	}
	return SljitTypedExpressionTreeIsSupported(node);
}

static bool SljitTypedExpressionTreeCanPrecheckNulls(const ExecutionExpressionIR &node) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitTypedExpressionTreeIsInt64Node(node);
	case ExecutionExpressionIRKind::CONSTANT:
		return !node.constant.IsNull() && SljitTypedExpressionTreeIsInt64Node(node);
	case ExecutionExpressionIRKind::CAST:
		return SljitTypedExpressionTreeInt64CastSupported(node);
	case ExecutionExpressionIRKind::BINARY: {
		SljitNativeIntegerBinaryOp native_op;
		return SljitTypedExpressionTreeIsInt64Node(node) && node.left && node.right &&
		       TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op) &&
		       SljitTypedExpressionTreeCanPrecheckNulls(*node.left) &&
		       SljitTypedExpressionTreeCanPrecheckNulls(*node.right);
	}
	default:
		return false;
	}
}

static SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node) {
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return SljitNativeIntegerKind::UINT8;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return SljitNativeIntegerKind::INT32;
	}
	if (SljitTypedExpressionTreeIsInt64Node(node)) {
		return SljitNativeIntegerKind::INT64;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree node type");
}

static SljitNativeIntegerKind SljitTypedExpressionTreeCastSourceKind(const ExecutionExpressionIR &node) {
	switch (node.physical_type) {
	case PhysicalType::INT32:
		return SljitNativeIntegerKind::INT32;
	case PhysicalType::INT64:
		return SljitNativeIntegerKind::INT64;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree cast source type");
	}
}

static SljitNativeIntegerCompareOp SljitTypedExpressionTreeCompareOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		return SljitNativeIntegerCompareOp::EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		return SljitNativeIntegerCompareOp::NOT_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree comparison operator");
	}
}

static idx_t CountSljitTypedExpressionTreeNodes(const ExecutionExpressionIR &node) {
	idx_t result = 1;
	if (node.left) {
		result += CountSljitTypedExpressionTreeNodes(*node.left);
	}
	if (node.right) {
		result += CountSljitTypedExpressionTreeNodes(*node.right);
	}
	if (node.else_node) {
		result += CountSljitTypedExpressionTreeNodes(*node.else_node);
	}
	for (auto &child : node.children) {
		result += CountSljitTypedExpressionTreeNodes(*child);
	}
	return result;
}

struct SljitTypedExpressionTreeSlot {
	sljit_sw value_offset;
	sljit_sw valid_offset;
};

static bool SljitTypedExpressionTreeSourceKnownValid(const vector<idx_t> *known_valid_sources, idx_t source_index) {
	if (!known_valid_sources) {
		return false;
	}
	for (auto known_source : *known_valid_sources) {
		if (known_source == source_index) {
			return true;
		}
	}
	return false;
}

static SljitTypedExpressionTreeSlot AllocateSljitTypedExpressionTreeSlot(idx_t &slot_index) {
	const auto value_offset = NumericCast<sljit_sw>(slot_index++ * sizeof(sljit_sw) * 2);
	return SljitTypedExpressionTreeSlot {value_offset, value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))};
}

static void EmitStoreSljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                  const SljitTypedExpressionTreeSlot &slot, sljit_s32 value_reg,
                                                  sljit_s32 valid_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.value_offset, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), slot.valid_offset, valid_reg, 0);
}

static int64_t SljitTypedExpressionTreeConstantValue(const ExecutionExpressionIR &node) {
	if (node.constant.IsNull()) {
		return 0;
	}
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		return node.constant.GetValueUnsafe<bool>() ? 1 : 0;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		return node.constant.GetValueUnsafe<int32_t>();
	}
	return node.constant.GetValueUnsafe<int64_t>();
}

static void EmitSljitTypedExpressionTreeConstant(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, node.constant.IsNull() ? 0 : 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static void EmitSljitTypedExpressionTreeReference(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                  const SljitTypedExpressionTreeSlot &slot,
                                                  const vector<idx_t> *known_valid_sources) {
	EmitLoadSljitExpressionTreeSourceIndex(compiler, node.ref_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	if (!SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.ref_index)) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
		               NumericCast<sljit_sw>(node.ref_index * sizeof(const validity_t *)));
		auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_R0, 0);
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_R2), 3);
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
		sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R0, 0);
		auto source_valid = sljit_emit_jump(compiler, SLJIT_NOT_EQUAL);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
		auto validity_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(source_valid, sljit_emit_label(compiler));
		sljit_set_label(source_all_valid, sljit_emit_label(compiler));
		sljit_set_label(validity_done, sljit_emit_label(compiler));
	}

	auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1),
	               NativeIntegerDataScale(source_kind));
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                  const vector<idx_t> *known_valid_sources = nullptr);

static void EmitSljitTypedExpressionTreeCast(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                             idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                             const SljitTypedExpressionTreeSlot &slot,
                                             const vector<idx_t> *known_valid_sources) {
	D_ASSERT(SljitTypedExpressionTreeInt64CastSupported(node));
	auto &source = *node.left;
	auto source_slot = EmitSljitTypedExpressionTreeValue(compiler, source, slot_index, overflows, known_valid_sources);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source_slot.value_offset);
	if (SljitTypedExpressionTreeIsInt32Node(source)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source_slot.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index,
                                                     vector<SljitExpressionTreeOverflowJumps> &overflows);

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfTrue(struct sljit_compiler *compiler,
                                                                   const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                   vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                                   const vector<idx_t> *known_valid_sources = nullptr);

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          const vector<idx_t> *known_valid_sources = nullptr);

static void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                 const SljitTypedExpressionTreeSlot &source,
                                                 const SljitTypedExpressionTreeSlot &target);

static void SetSljitJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static void EmitSljitTypedExpressionTreeInvalidResult(struct sljit_compiler *compiler,
                                                      const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, slot, SLJIT_R2, SLJIT_R3);
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeBinary(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                   idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                   const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	auto left_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto right_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.right, slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), left_slot.valid_offset);
	auto invalid_left = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), right_slot.valid_offset);
	auto invalid_right = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), left_slot.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), right_slot.value_offset);
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
		                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
		auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(compare_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_set_label(compare_done, sljit_emit_label(compiler));
	} else {
		SljitNativeIntegerBinaryOp native_op;
		if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
			throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
		}
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
		if (node.arithmetic_overflow_check) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
			AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		} else {
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
		}
		if (binary_kind == SljitNativeIntegerKind::INT32) {
			sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(invalid_left, invalid_label);
	sljit_set_label(invalid_right, invalid_label);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
	return result_slot;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeSlotJumpIfTrue(struct sljit_compiler *compiler,
                                                                       const SljitTypedExpressionTreeSlot &slot) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	auto invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	auto result_true = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_set_label(invalid, sljit_emit_label(compiler));
	return vector<sljit_jump *> {result_true};
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(struct sljit_compiler *compiler,
                                                                          const SljitTypedExpressionTreeSlot &slot) {
	vector<sljit_jump *> result;
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), slot.valid_offset);
	result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), slot.value_offset);
	result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
	return result;
}

static vector<sljit_jump *> EmitSljitTypedReferenceJumpIfNull(struct sljit_compiler *compiler, idx_t source_index) {
	return vector<sljit_jump *> {EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index)};
}

static vector<sljit_jump *> EmitSljitTypedReferenceJumpIfNotNull(struct sljit_compiler *compiler, idx_t source_index) {
	auto null_jumps = EmitSljitTypedReferenceJumpIfNull(compiler, source_index);
	auto not_null = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto null_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(null_jumps, null_label);
	return vector<sljit_jump *> {not_null};
}

static bool SljitTypedExpressionTreeIsBoolConstantTrue(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && !node.constant.IsNull() &&
	       SljitTypedExpressionTreeIsBoolNode(node) && node.constant.GetValueUnsafe<bool>();
}

static bool SljitTypedExpressionTreeIsBoolConstantNotTrue(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && SljitTypedExpressionTreeIsBoolNode(node) &&
	       (node.constant.IsNull() || !node.constant.GetValueUnsafe<bool>());
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeComparisonJumpIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                 idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                 const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	auto left_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto right_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.right, slot_index, overflows, known_valid_sources);
	vector<sljit_jump *> invalid_jumps;

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), left_slot.valid_offset);
	invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), right_slot.valid_offset);
	invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), left_slot.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), right_slot.value_offset);
	auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
	                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
	auto result_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(invalid_jumps, not_true_label);
	return vector<sljit_jump *> {result_true};
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeComparisonJumpIfNotTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	auto true_jumps =
	    EmitSljitTypedExpressionTreeComparisonJumpIfTrue(compiler, node, slot_index, overflows, known_valid_sources);
	auto not_true = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(true_jumps, true_label);
	return vector<sljit_jump *> {not_true};
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeUnaryJumpIfTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                            idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                            const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	if (node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::IS_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *>();
			}
			return EmitSljitTypedReferenceJumpIfNull(compiler, node.left->ref_index);
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
			}
			return EmitSljitTypedReferenceJumpIfNotNull(compiler, node.left->ref_index);
		default:
			break;
		}
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfTrue(compiler, slot);
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeUnaryJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                               idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                               const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	if (node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		switch (node.unary_op) {
		case ExecutionExpressionUnaryOp::IS_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
			}
			return EmitSljitTypedReferenceJumpIfNotNull(compiler, node.left->ref_index);
		case ExecutionExpressionUnaryOp::IS_NOT_NULL:
			if (SljitTypedExpressionTreeSourceKnownValid(known_valid_sources, node.left->ref_index)) {
				return vector<sljit_jump *>();
			}
			return EmitSljitTypedReferenceJumpIfNull(compiler, node.left->ref_index);
		default:
			break;
		}
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(compiler, slot);
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeConjunctionJumpIfTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	vector<sljit_jump *> result;
	if (node.conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		for (auto &child : node.children) {
			auto child_true =
			    EmitSljitTypedExpressionTreeJumpIfTrue(compiler, *child, slot_index, overflows, known_valid_sources);
			result.insert(result.end(), child_true.begin(), child_true.end());
		}
		return result;
	}

	vector<sljit_jump *> not_true_jumps;
	for (auto &child : node.children) {
		auto child_not_true =
		    EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *child, slot_index, overflows, known_valid_sources);
		not_true_jumps.insert(not_true_jumps.end(), child_not_true.begin(), child_not_true.end());
	}
	result.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto not_true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(not_true_jumps, not_true_label);
	return result;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeConjunctionJumpIfNotTrue(
    struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
    vector<SljitExpressionTreeOverflowJumps> &overflows, const vector<idx_t> *known_valid_sources) {
	vector<sljit_jump *> result;
	if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			auto child_not_true =
			    EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *child, slot_index, overflows, known_valid_sources);
			result.insert(result.end(), child_not_true.begin(), child_not_true.end());
		}
		return result;
	}

	vector<sljit_jump *> true_jumps;
	for (auto &child : node.children) {
		auto child_true =
		    EmitSljitTypedExpressionTreeJumpIfTrue(compiler, *child, slot_index, overflows, known_valid_sources);
		true_jumps.insert(true_jumps.end(), child_true.begin(), child_true.end());
	}
	result.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto true_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(true_jumps, true_label);
	return result;
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfTrue(struct sljit_compiler *compiler,
                                                                   const ExecutionExpressionIR &node, idx_t &slot_index,
                                                                   vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                                   const vector<idx_t> *known_valid_sources) {
	if (SljitTypedExpressionTreeIsBoolConstantTrue(node)) {
		return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
	}
	if (SljitTypedExpressionTreeIsBoolConstantNotTrue(node)) {
		return vector<sljit_jump *>();
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		return EmitSljitTypedExpressionTreeUnaryJumpIfTrue(compiler, node, slot_index, overflows, known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		return EmitSljitTypedExpressionTreeComparisonJumpIfTrue(compiler, node, slot_index, overflows,
		                                                        known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return EmitSljitTypedExpressionTreeConjunctionJumpIfTrue(compiler, node, slot_index, overflows,
		                                                         known_valid_sources);
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfTrue(compiler, slot);
}

static vector<sljit_jump *>
EmitSljitTypedExpressionTreeJumpIfNotTrue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                          idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                          const vector<idx_t> *known_valid_sources) {
	if (SljitTypedExpressionTreeIsBoolConstantTrue(node)) {
		return vector<sljit_jump *>();
	}
	if (SljitTypedExpressionTreeIsBoolConstantNotTrue(node)) {
		return vector<sljit_jump *> {sljit_emit_jump(compiler, SLJIT_JUMP)};
	}
	if (node.kind == ExecutionExpressionIRKind::UNARY) {
		return EmitSljitTypedExpressionTreeUnaryJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                      known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		return EmitSljitTypedExpressionTreeComparisonJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                           known_valid_sources);
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return EmitSljitTypedExpressionTreeConjunctionJumpIfNotTrue(compiler, node, slot_index, overflows,
		                                                            known_valid_sources);
	}
	auto slot = EmitSljitTypedExpressionTreeValue(compiler, node, slot_index, overflows, known_valid_sources);
	return EmitSljitTypedExpressionTreeSlotJumpIfNotTrue(compiler, slot);
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeUnary(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                  const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.left);
	auto child_slot =
	    EmitSljitTypedExpressionTreeValue(compiler, *node.left, slot_index, overflows, known_valid_sources);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);

	switch (node.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		if (node.unary_op == ExecutionExpressionUnaryOp::IS_NULL) {
			auto is_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(is_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		} else {
			auto is_not_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(is_not_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		return result_slot;
	case ExecutionExpressionUnaryOp::NOT: {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.value_offset);
		auto child_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_false, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
		EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
		auto done_from_false = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_invalid, sljit_emit_label(compiler));
		EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
		auto done_label = sljit_emit_label(compiler);
		sljit_set_label(done, done_label);
		sljit_set_label(done_from_false, done_label);
		return result_slot;
	}
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree unary operator");
	}
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeConjunction(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                        idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                        const vector<idx_t> *known_valid_sources) {
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset, SLJIT_IMM, 0);
	vector<sljit_jump *> decisive_jumps;
	for (auto &child : node.children) {
		auto child_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *child, slot_index, overflows, known_valid_sources);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_valid = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset, SLJIT_IMM, 1);
		auto next_child = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(child_valid, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), child_slot.value_offset);
		if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		} else {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		}
		sljit_set_label(next_child, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), result_slot.value_offset);
	auto no_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	auto done_from_null = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 1 : 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	auto done_from_default = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto decisive_label = sljit_emit_label(compiler);
	for (auto jump : decisive_jumps) {
		sljit_set_label(jump, decisive_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 0 : 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done_from_null, done_label);
	sljit_set_label(done_from_default, done_label);
	return result_slot;
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeCoalesce(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                     idx_t &slot_index, vector<SljitExpressionTreeOverflowJumps> &overflows,
                                     const vector<idx_t> *known_valid_sources) {
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> done_jumps;
	for (auto &child : node.children) {
		auto child_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *child, slot_index, overflows, known_valid_sources);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), child_slot.valid_offset);
		auto child_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		EmitCopySljitTypedExpressionTreeSlot(compiler, child_slot, result_slot);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(child_invalid, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeInvalidResult(compiler, result_slot);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
	return result_slot;
}

static void AddSljitTypedKnownValidSource(vector<idx_t> &known_valid_sources, idx_t source_index) {
	for (auto known_source : known_valid_sources) {
		if (known_source == source_index) {
			return;
		}
	}
	known_valid_sources.push_back(source_index);
}

static void CollectSljitTypedExpressionTreeReferences(const ExecutionExpressionIR &node,
                                                      vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.ref_index);
		return;
	}
	if (node.left) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
	}
	if (node.right) {
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
	}
	if (node.else_node) {
		CollectSljitTypedExpressionTreeReferences(*node.else_node, known_valid_sources);
	}
	for (auto &child : node.children) {
		CollectSljitTypedExpressionTreeReferences(*child, known_valid_sources);
	}
}

static void CollectSljitTypedExpressionTreeTrueFacts(const ExecutionExpressionIR &node,
                                                     vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NOT_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::BINARY && node.left && node.right &&
	    SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		CollectSljitTypedExpressionTreeReferences(*node.left, known_valid_sources);
		CollectSljitTypedExpressionTreeReferences(*node.right, known_valid_sources);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeTrueFacts(*child, known_valid_sources);
		}
	}
}

static void CollectSljitTypedExpressionTreeNotTrueFacts(const ExecutionExpressionIR &node,
                                                        vector<idx_t> &known_valid_sources) {
	if (node.kind == ExecutionExpressionIRKind::UNARY && node.left &&
	    node.unary_op == ExecutionExpressionUnaryOp::IS_NULL &&
	    node.left->kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitTypedKnownValidSource(known_valid_sources, node.left->ref_index);
		return;
	}
	if (node.kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    node.conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		for (auto &child : node.children) {
			CollectSljitTypedExpressionTreeNotTrueFacts(*child, known_valid_sources);
		}
	}
}

static vector<sljit_jump *> EmitSljitTypedExpressionTreeJumpIfAnySourceMayHaveNull(
    struct sljit_compiler *compiler, const vector<idx_t> &source_indices, const vector<idx_t> &known_valid_sources) {
	vector<sljit_jump *> result;
	for (auto source_index : source_indices) {
		if (SljitTypedExpressionTreeSourceKnownValid(&known_valid_sources, source_index)) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S6),
		               NumericCast<sljit_sw>(source_index * sizeof(const validity_t *)));
		result.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	}
	return result;
}

static void EmitSljitTypedExpressionTreeCaseValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &value,
                                                  const SljitTypedExpressionTreeSlot &result_slot, idx_t &slot_index,
                                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                                  const vector<idx_t> &known_valid_sources) {
	if (!SljitTypedExpressionTreeFastPathSupported(value)) {
		auto value_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, value, slot_index, overflows, &known_valid_sources);
		EmitCopySljitTypedExpressionTreeSlot(compiler, value_slot, result_slot);
		return;
	}

	vector<idx_t> source_indices;
	CollectSljitTypedExpressionTreeReferences(value, source_indices);
	auto use_generic =
	    EmitSljitTypedExpressionTreeJumpIfAnySourceMayHaveNull(compiler, source_indices, known_valid_sources);
	idx_t fast_spill_index = slot_index * 2;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, value, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, result_slot, SLJIT_R2, SLJIT_R3);
	if (use_generic.empty()) {
		return;
	}

	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto generic_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(use_generic, generic_label);
	auto value_slot = EmitSljitTypedExpressionTreeValue(compiler, value, slot_index, overflows, &known_valid_sources);
	EmitCopySljitTypedExpressionTreeSlot(compiler, value_slot, result_slot);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeCase(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                 vector<SljitExpressionTreeOverflowJumps> &overflows,
                                 const vector<idx_t> *known_valid_sources) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	auto result_slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
	vector<sljit_jump *> done_jumps;
	vector<idx_t> fallthrough_known_valid;
	if (known_valid_sources) {
		fallthrough_known_valid = *known_valid_sources;
	}

	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = node.children[child_idx];
		auto &value = node.children[child_idx + 1];
		auto branch_known_valid = fallthrough_known_valid;
		CollectSljitTypedExpressionTreeTrueFacts(*condition, branch_known_valid);
		auto condition_not_true = EmitSljitTypedExpressionTreeJumpIfNotTrue(compiler, *condition, slot_index, overflows,
		                                                                    &fallthrough_known_valid);
		EmitSljitTypedExpressionTreeCaseValue(compiler, *value, result_slot, slot_index, overflows, branch_known_valid);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		auto next_condition = sljit_emit_label(compiler);
		SetSljitJumpLabels(condition_not_true, next_condition);
		CollectSljitTypedExpressionTreeNotTrueFacts(*condition, fallthrough_known_valid);
	}

	EmitSljitTypedExpressionTreeCaseValue(compiler, *node.else_node, result_slot, slot_index, overflows,
	                                      fallthrough_known_valid);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
	return result_slot;
}

static void EmitCopySljitTypedExpressionTreeSlot(struct sljit_compiler *compiler,
                                                 const SljitTypedExpressionTreeSlot &source,
                                                 const SljitTypedExpressionTreeSlot &target) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), source.value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), source.valid_offset);
	EmitStoreSljitTypedExpressionTreeSlot(compiler, target, SLJIT_R2, SLJIT_R3);
}

static SljitTypedExpressionTreeSlot
EmitSljitTypedExpressionTreeValue(struct sljit_compiler *compiler, const ExecutionExpressionIR &node, idx_t &slot_index,
                                  vector<SljitExpressionTreeOverflowJumps> &overflows,
                                  const vector<idx_t> *known_valid_sources) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeConstant(compiler, node, slot);
		return slot;
	}
	case ExecutionExpressionIRKind::REFERENCE: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeReference(compiler, node, slot, known_valid_sources);
		return slot;
	}
	case ExecutionExpressionIRKind::CAST: {
		auto slot = AllocateSljitTypedExpressionTreeSlot(slot_index);
		EmitSljitTypedExpressionTreeCast(compiler, node, slot_index, overflows, slot, known_valid_sources);
		return slot;
	}
	case ExecutionExpressionIRKind::UNARY:
		return EmitSljitTypedExpressionTreeUnary(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::BINARY:
		return EmitSljitTypedExpressionTreeBinary(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::CONJUNCTION:
		return EmitSljitTypedExpressionTreeConjunction(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::COALESCE:
		return EmitSljitTypedExpressionTreeCoalesce(compiler, node, slot_index, overflows, known_valid_sources);
	case ExecutionExpressionIRKind::CASE:
		return EmitSljitTypedExpressionTreeCase(compiler, node, slot_index, overflows, known_valid_sources);
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree node kind");
	}
}

static bool SljitTypedExpressionTreeFastIsLeaf(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT || node.kind == ExecutionExpressionIRKind::REFERENCE;
}

static void EmitSljitTypedExpressionTreeFastLeafReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    sljit_s32 target) {
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(SljitTypedExpressionTreeConstantValue(node)));
		return;
	}
	D_ASSERT(node.kind == ExecutionExpressionIRKind::REFERENCE);
	auto source_kind = SljitTypedExpressionTreeIntegerKind(node);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
	               NumericCast<sljit_sw>(node.ref_index * sizeof(const_data_ptr_t)));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(source_kind), target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(source_kind));
}

static void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index,
                                                     vector<SljitExpressionTreeOverflowJumps> &overflows);

static void EmitSljitTypedExpressionTreeFastBinaryReg(struct sljit_compiler *compiler,
                                                      const ExecutionExpressionIR &node, idx_t &spill_index,
                                                      vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	D_ASSERT(node.right);
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
	if (SljitTypedExpressionTreeFastIsLeaf(*node.right)) {
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, *node.right, SLJIT_R4);
		if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
			auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
			                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
			auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(compare_true, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(compare_done, sljit_emit_label(compiler));
			return;
		}
		SljitNativeIntegerBinaryOp native_op;
		if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
			throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
		}
		auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
		auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
		if (node.arithmetic_overflow_check) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
			AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		} else {
			sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
		}
		if (binary_kind == SljitNativeIntegerKind::INT32) {
			sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
		}
		return;
	}

	auto left_offset = NumericCast<sljit_sw>(spill_index++ * sizeof(sljit_sw));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), left_offset, SLJIT_R2, 0);
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.right, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), left_offset);
	if (SljitTypedExpressionTreeComparisonSupported(node.binary_op)) {
		auto compare_type = NativeIntegerCompareJumpType(SljitTypedExpressionTreeIntegerKind(*node.left),
		                                                 SljitTypedExpressionTreeCompareOp(node.binary_op));
		auto compare_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R4, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto compare_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(compare_true, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		sljit_set_label(compare_done, sljit_emit_label(compiler));
		return;
	}
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(node.binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT typed expression-tree binary operator");
	}
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(node);
	auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
	if (node.arithmetic_overflow_check) {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
		AddSljitExpressionOverflowJump(overflows, native_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	} else {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	}
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void EmitSljitTypedExpressionTreeFastUnaryReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index,
                                                     vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	switch (node.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		return;
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		return;
	case ExecutionExpressionUnaryOp::NOT:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
		{
			auto child_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(child_false, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		return;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast unary operator");
	}
}

static void EmitSljitTypedExpressionTreeFastCastReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index,
                                                    vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.left);
	D_ASSERT(SljitTypedExpressionTreeInt64CastSupported(node));
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.left, spill_index, overflows);
	if (SljitTypedExpressionTreeIsInt32Node(*node.left)) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void EmitSljitTypedExpressionTreeFastConjunctionReg(struct sljit_compiler *compiler,
                                                           const ExecutionExpressionIR &node, idx_t &spill_index,
                                                           vector<SljitExpressionTreeOverflowJumps> &overflows) {
	vector<sljit_jump *> decisive_jumps;
	for (auto &child : node.children) {
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *child, spill_index, overflows);
		if (node.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		} else {
			decisive_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 1 : 0);
	auto done_from_default = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto decisive_label = sljit_emit_label(compiler);
	for (auto jump : decisive_jumps) {
		sljit_set_label(jump, decisive_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM,
	               node.conjunction_op == ExecutionExpressionConjunctionOp::AND ? 0 : 1);
	sljit_set_label(done_from_default, sljit_emit_label(compiler));
}

static void EmitSljitTypedExpressionTreeFastCoalesceReg(struct sljit_compiler *compiler,
                                                        const ExecutionExpressionIR &node, idx_t &spill_index,
                                                        vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(!node.children.empty());
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.children[0], spill_index, overflows);
}

static void EmitSljitTypedExpressionTreeFastCaseReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                    idx_t &spill_index,
                                                    vector<SljitExpressionTreeOverflowJumps> &overflows) {
	D_ASSERT(node.else_node);
	D_ASSERT(node.children.size() % 2 == 0);
	vector<sljit_jump *> done_jumps;
	for (idx_t child_idx = 0; child_idx < node.children.size(); child_idx += 2) {
		auto &condition = node.children[child_idx];
		auto &value = node.children[child_idx + 1];
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *condition, spill_index, overflows);
		auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *value, spill_index, overflows);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(condition_false, sljit_emit_label(compiler));
	}
	EmitSljitTypedExpressionTreeFastValueReg(compiler, *node.else_node, spill_index, overflows);
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}
}

static void EmitSljitTypedExpressionTreeFastValueReg(struct sljit_compiler *compiler, const ExecutionExpressionIR &node,
                                                     idx_t &spill_index,
                                                     vector<SljitExpressionTreeOverflowJumps> &overflows) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
	case ExecutionExpressionIRKind::REFERENCE:
		EmitSljitTypedExpressionTreeFastLeafReg(compiler, node, SLJIT_R2);
		return;
	case ExecutionExpressionIRKind::CAST:
		EmitSljitTypedExpressionTreeFastCastReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::UNARY:
		EmitSljitTypedExpressionTreeFastUnaryReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::BINARY:
		EmitSljitTypedExpressionTreeFastBinaryReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::CONJUNCTION:
		EmitSljitTypedExpressionTreeFastConjunctionReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::COALESCE:
		EmitSljitTypedExpressionTreeFastCoalesceReg(compiler, node, spill_index, overflows);
		return;
	case ExecutionExpressionIRKind::CASE:
		EmitSljitTypedExpressionTreeFastCaseReg(compiler, node, spill_index, overflows);
		return;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree fast node kind");
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeTypedExpressionTree(const ExecutionExpressionIR &root,
                                                                          SljitNativeIntegerKind result_kind,
                                                                          SljitNativeVectorFunction &function,
                                                                          string &error) {
	if (!SljitTypedExpressionTreeIsSupported(root)) {
		error =
		    "SLJIT typed expression-tree codegen only supports INT64/BOOLEAN arithmetic, comparisons, conjunctions, "
		    "null checks, coalesce, and case";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto fast_path_supported = SljitTypedExpressionTreeFastPathSupported(root);
	auto local_size = NumericCast<sljit_sw>(CountSljitTypedExpressionTreeNodes(root) * sizeof(sljit_sw) * 3);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	vector<SljitExpressionTreeOverflowJumps> overflows;
	vector<idx_t> source_refs;
	CollectSljitTypedExpressionTreeReferences(root, source_refs);
	const auto precheck_nulls_supported =
	    fast_path_supported && SljitTypedExpressionTreeCanPrecheckNulls(root) && !source_refs.empty();
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	struct sljit_jump *use_slow_loop = nullptr;
	if (fast_path_supported) {
		use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	} else {
		use_slow_loop = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	idx_t fast_spill_index = 0;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	struct sljit_jump *flat_nullable_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	if (precheck_nulls_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto flat_nullable_loop = sljit_emit_label(compiler);
		flat_nullable_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> source_null_jumps;
		for (auto source_index : source_refs) {
			source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t flat_nullable_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, flat_nullable_spill_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
		               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
		EmitSetResultRowInvalid(compiler);
		sljit_set_label(flat_nullable_next, sljit_emit_label(compiler));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto flat_nullable_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(flat_nullable_repeat, flat_nullable_loop);
		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	idx_t slot_index = 0;
	auto root_slot = EmitSljitTypedExpressionTreeValue(compiler, root, slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), root_slot.valid_offset);
	auto root_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), root_slot.value_offset);
	sljit_emit_op1(compiler, NativeIntegerStoreOp(result_kind), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(result_kind), SLJIT_R2, 0);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	EmitSetResultRowInvalid(compiler);
	sljit_set_label(row_done, sljit_emit_label(compiler));
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
	sljit_set_label(fast_done, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeVectorCode(compiler, function, error);
}

static sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op);
static bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind);
static void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                        sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset,
                                        sljit_s32 target);

enum class SljitNativeAggregateSumStateKind : uint8_t { INT64, HUGEINT };

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumExpressionTree(const ExecutionExpressionIR &root,
                                           SljitNativeAggregateUpdateFunction &function, string &error,
                                           SljitNativeAggregateSumStateKind state_kind) {
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
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = spill_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);
	// Hoist loop-invariant vector-format arrays (see projection tree).
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValueFast(compiler, root, SLJIT_R2, fast_spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	EmitSljitAggregateLoopStep(compiler, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
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
	sljit_set_label(fast_done, done_label);
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
	if (!hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_int64_value));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2,
		               0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	}
	sljit_set_label(no_value, sljit_emit_label(compiler));

	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64ExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::INT64);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumTypedExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error,
                                                SljitNativeAggregateSumStateKind state_kind) {
	if (!SljitTypedExpressionTreeIsSupported(root) || !SljitTypedExpressionTreeIsInt64Node(root)) {
		error = "SLJIT aggregate typed expression-tree reducer only supports INT64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(CountSljitTypedExpressionTreeNodes(root) * sizeof(sljit_sw) * 3);
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = tree_local_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));

	vector<SljitExpressionTreeOverflowJumps> overflows;
	vector<idx_t> source_refs;
	CollectSljitTypedExpressionTreeReferences(root, source_refs);
	const auto precheck_nulls_supported = SljitTypedExpressionTreeFastPathSupported(root) &&
	                                      SljitTypedExpressionTreeCanPrecheckNulls(root) && !source_refs.empty();
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	idx_t fast_spill_index = 0;
	EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	EmitSljitAggregateLoopStep(compiler, fast_loop);

	sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	struct sljit_jump *flat_nullable_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	if (precheck_nulls_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto flat_nullable_loop = sljit_emit_label(compiler);
		flat_nullable_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> source_null_jumps;
		for (auto source_index : source_refs) {
			source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t flat_nullable_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, flat_nullable_spill_index, overflows);
		if (hugeint_state) {
			EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset,
			                                         saw_value_offset, SLJIT_R2);
		} else {
			EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
		}
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
		sljit_set_label(flat_nullable_next, sljit_emit_label(compiler));
		EmitSljitAggregateLoopStep(compiler, flat_nullable_loop);
		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	idx_t slot_index = 0;
	auto root_slot = EmitSljitTypedExpressionTreeValue(compiler, root, slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), root_slot.valid_offset);
	auto root_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), root_slot.value_offset);
	if (hugeint_state) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset,
		                                         SLJIT_R2);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	}
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	sljit_set_label(row_done, sljit_emit_label(compiler));
	EmitSljitAggregateLoopStep(compiler, loop);

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
	sljit_set_label(fast_done, done_label);
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	if (hugeint_state) {
		EmitSljitAggregateCommitHugeint(compiler, local_sum_offset, local_sum_upper_offset, saw_value_offset);
	} else {
		EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64TypedExpressionTree(const ExecutionExpressionIR &root,
                                                     SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(root, function, error,
	                                                       SljitNativeAggregateSumStateKind::INT64);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(const ExecutionExpressionIR &root,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(root, function, error,
	                                                       SljitNativeAggregateSumStateKind::HUGEINT);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintExpressionTree(const ExecutionExpressionIR &root,
                                                  SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::HUGEINT);
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

static void EmitSljitStoreZeroDoubleLocal(struct sljit_compiler *compiler, sljit_sw local_sum_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (sizeof(double) > sizeof(sljit_sw)) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset + sizeof(sljit_sw), SLJIT_IMM, 0);
	}
}

static void EmitSljitAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                               sljit_sw saw_value_offset, sljit_s32 value_freg) {
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
	                local_sum_offset);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0, value_freg, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1,
	                SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitSljitAggregateCommitDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
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
	               offsetof(SljitNativeVectorInput, aggregate_double_value));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
	                local_sum_offset);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
	                SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleReference(SljitNativeDoubleSourceKind kind,
                                            SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	EmitLoadNativeDoubleOperand(compiler, kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
	                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 3, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	if (hugeint_state) {
		EmitSljitGroupedAggregateAccumulateHugeintInt64(compiler, SLJIT_S4, SLJIT_R2);
	} else {
		EmitSljitGroupedAggregateAccumulateInt64(compiler, SLJIT_S4, SLJIT_R2);
	}
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
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

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(
    SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind source_kind, bool constant_on_left,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

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
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
    SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind, SljitNativeDoubleSourceKind right_kind,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);
	auto needs_helper_spill = NativeDoubleSourceUsesHelper(left_kind) || NativeDoubleSourceUsesHelper(right_kind);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto left_spill_offset = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto right_spill_offset = left_spill_offset + NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = right_spill_offset + (needs_helper_spill ? NumericCast<sljit_sw>(sizeof(double)) : 0);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

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
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static bool SljitFusedUngroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                                  const ExecutionRegionAggregateInput &aggregate) {
	if (!aggregate.primitive_update_ready) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		return false;
	}
}

static void EmitLoadFusedAggregateExecuteIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, sljit_sw source_sel_array_offset,
                                              idx_t lane_idx, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_sel_array_offset);
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_source_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitFusedAggregateJumpIfValidityNull(struct sljit_compiler *compiler,
                                                               sljit_sw validity_array_offset, idx_t lane_idx,
                                                               sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), validity_array_offset);
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static void EmitLoadFusedAggregateIntegerData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                              idx_t lane_idx, SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                              sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

static void EmitLoadFusedAggregatePointer(struct sljit_compiler *compiler, sljit_sw pointer_array_offset,
                                          idx_t lane_idx, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0), pointer_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(target_reg), SljitPointerArrayOffset(lane_idx));
}

static void EmitFusedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                              sljit_sw saw_value_offset, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitFusedAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));

	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitFusedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                             sljit_sw saw_value_offset) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                       const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedUngroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx])) {
			error = "unsupported fused aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	bool has_sum_lane = false;
	sljit_sw local_size = 0;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
			continue;
		}
		has_sum_lane = true;
		local_sum_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		saw_value_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	if (!has_sum_lane) {
		sljit_free_compiler(compiler);
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (local_sum_offsets[payload_idx] < 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> invalid_jumps;
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1));
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel_array),
			                                  payload_idx, SLJIT_S4);
			invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
			    compiler, offsetof(SljitNativeVectorInput, right_source_validity_array), payload_idx, SLJIT_S4));
		}
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, constants));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0),
			               NumericCast<sljit_sw>(payload_idx * sizeof(int64_t)));
			if (payload.constant_on_left) {
				sljit_emit_op2(compiler,
				               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
				               SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler,
				               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
				               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			}
			AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		} else if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, right_source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_S4, SLJIT_R3);
			sljit_emit_op2(compiler,
			               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
			               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		}
		if (payload.check_result_range) {
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_min)));
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_max)));
		}
		EmitFusedAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                  SLJIT_R2);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		for (auto invalid_jump : invalid_jumps) {
			sljit_set_label(invalid_jump, payload_invalid);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitSljitAggregateLoopStep(compiler, loop);

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
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitFusedAggregateCommitCountStar(compiler, payload_idx);
		} else {
			EmitFusedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                 saw_value_offsets[payload_idx]);
		}
	}
	auto return_label = sljit_emit_label(compiler);
	for (auto jump : helper_done) {
		sljit_set_label(jump, return_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static bool SljitFusedGroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                                const ExecutionRegionAggregateInput &aggregate,
                                                                const ExecutionRegionAggregateContract &contract) {
	if (!aggregate.primitive_update_ready || aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if ((aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
}

static void EmitSljitGroupedAggregateValuePointerImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                           idx_t state_offset, idx_t value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
}

static void EmitSljitGroupedAggregateSetStateIsSetImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                            idx_t state_offset, idx_t state_is_set_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + state_is_set_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

static void EmitSljitGroupedAggregateAccumulateInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                              idx_t state_offset, idx_t value_offset,
                                                              idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

static void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                             idx_t state_offset, idx_t value_offset) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateAccumulateHugeintImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                idx_t state_offset, idx_t value_offset,
                                                                idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper), SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.size() < 2) {
		error = "unsupported fused grouped aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                         contract)) {
			error = "unsupported fused grouped aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

struct SljitPerfectHashGroupPlan {
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	int64_t minimum = 0;
	idx_t shift = 0;
};

static bool TryGetSljitPerfectHashGroupIntegerKind(const LogicalType &type, SljitNativeIntegerKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		kind = SljitNativeIntegerKind::INT8;
		return true;
	case PhysicalType::UINT8:
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool TryGetSljitPerfectHashGroupMinimum(const LogicalType &type, const Value &minimum, int64_t &result) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::UINT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::INT32:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = minimum.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static bool TryBuildSljitPerfectHashGroupPlans(const vector<ExecutionRegionGroupInput> &groups,
                                               const ExecutionRegionAggregateContract &contract,
                                               vector<SljitPerfectHashGroupPlan> &result) {
	if (contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH ||
	    contract.perfect_required_bits.size() != groups.size() ||
	    contract.perfect_group_minima.size() != groups.size()) {
		return false;
	}
	result.reserve(groups.size());
	idx_t shift = contract.perfect_required_bits_total;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		if (!group.supported_reference) {
			return false;
		}
		if (shift < contract.perfect_required_bits[group_idx]) {
			return false;
		}
		shift -= contract.perfect_required_bits[group_idx];
		SljitPerfectHashGroupPlan plan;
		if (!TryGetSljitPerfectHashGroupIntegerKind(group.type, plan.integer_kind) ||
		    !TryGetSljitPerfectHashGroupMinimum(group.type, contract.perfect_group_minima[group_idx], plan.minimum)) {
			return false;
		}
		plan.shift = shift;
		result.push_back(plan);
	}
	return true;
}

static void EmitLoadFusedAggregateGroupSourceIndex(struct sljit_compiler *compiler, idx_t group_idx,
                                                   sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_sel_array));
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto no_group_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_group_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_group_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_group_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitFusedAggregateJumpIfGroupValidityNull(struct sljit_compiler *compiler, idx_t group_idx,
                                                                    sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_validity_array));
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static void EmitLoadFusedAggregateGroupIntegerData(struct sljit_compiler *compiler, idx_t group_idx,
                                                   SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                                   sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const ExecutionRegionAggregateContract &contract,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	vector<SljitPerfectHashGroupPlan> group_plans;
	if (!TryBuildSljitPerfectHashGroupPlans(groups, contract, group_plans) || group_plans.empty() ||
	    !contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
		error = "unsupported fused perfect-hash aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                         contract)) {
			error = "unsupported fused perfect-hash aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	vector<sljit_jump *> group_out_of_range;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
		auto &group = group_plans[group_idx];
		EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1);
		auto group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
		EmitLoadFusedAggregateGroupIntegerData(compiler, group_idx, group.integer_kind, SLJIT_R1, SLJIT_R2);
		if (group.minimum != 0) {
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group.minimum));
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
		if (group.shift != 0) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group.shift));
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
		auto group_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto group_null_label = sljit_emit_label(compiler);
		sljit_set_label(group_is_null, group_null_label);
		sljit_set_label(group_done, sljit_emit_label(compiler));
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_total_groups));
	group_out_of_range.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S4, 0, SLJIT_R0, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_tuple_size));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_state_data));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R1, 0);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			continue;
		}
		auto &payload = payloads[payload_idx];
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}
	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	auto range_error_label = sljit_emit_label(compiler);
	for (auto jump : group_out_of_range) {
		sljit_set_label(jump, range_error_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
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

static sljit_s32 NativeDoubleBinaryOp(SljitNativeDoubleBinaryOp op) {
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

static bool NativeDoubleSourceUsesHelper(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::INT128_TO_DOUBLE ||
	       kind == SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE;
}

static bool NativeDoubleSourceHasDecimalScale(SljitNativeDoubleSourceKind kind) {
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

static void EmitLoadNativeDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                        sljit_sw data_offset, sljit_s32 index_reg, sljit_sw scale_offset,
                                        sljit_s32 target) {
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

static void EmitScalePredicateDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                            double scale, sljit_s32 target) {
	if (!NativeDoubleSourceHasDecimalScale(kind)) {
		return;
	}
	sljit_emit_fset64(compiler, SLJIT_FR2, scale);
	sljit_emit_fop2(compiler, SLJIT_DIV_F64, target, 0, target, 0, SLJIT_FR2, 0);
}

static void EmitLoadPredicateDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                           idx_t source_index, sljit_s32 index_reg, double scale, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	switch (kind) {
	case SljitNativeDoubleSourceKind::DOUBLE:
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		return;
	case SljitNativeDoubleSourceKind::INT64_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_SW, target, 0, SLJIT_R2, 0);
		EmitScalePredicateDoubleOperand(compiler, kind, scale, target);
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
		EmitScalePredicateDoubleOperand(compiler, kind, scale, target);
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate double source kind");
	}
}

static sljit_sw SljitUInt64Immediate(uint64_t value) {
	return static_cast<sljit_sw>(value);
}

static SljitNativeIntegerCompareOp FlipNativeIntegerCompareOp(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	default:
		return op;
	}
}

static sljit_s32 NativeDoubleCompareJumpType(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		return SLJIT_ORDERED_EQUAL;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		return SLJIT_ORDERED_NOT_EQUAL;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SLJIT_ORDERED_LESS;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SLJIT_ORDERED_GREATER;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SLJIT_ORDERED_LESS_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SLJIT_ORDERED_GREATER_EQUAL;
	default:
		throw InternalException("Unknown SLJIT native double predicate comparison operator");
	}
}

static void EmitLoadPredicateInt128SourceWord(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                              sljit_s32 index_reg, sljit_sw word_offset, sljit_s32 scratch) {
	static_assert(sizeof(hugeint_t) == 16, "SLJIT INT128 predicate lowering expects DuckDB hugeint_t ABI size");
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index_reg, 0, SLJIT_IMM, 4);
	if (word_offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, word_offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(SLJIT_R0, scratch), 0);
}

static void EmitSljitInt128CompareBranches(struct sljit_compiler *compiler, SljitNativeIntegerCompareOp op,
                                           sljit_s32 left_lower, sljit_sw left_lowerw, sljit_s32 left_upper,
                                           sljit_sw left_upperw, sljit_s32 right_lower, sljit_sw right_lowerw,
                                           sljit_s32 right_upper, sljit_sw right_upperw,
                                           SljitPredicateBranches &result) {
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_GREATER, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	default:
		throw InternalException("Unknown SLJIT INT128 predicate comparison operator");
	}
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

static idx_t SljitStringCompareChunkSize(idx_t remaining) {
	if (remaining >= sizeof(sljit_sw)) {
		return sizeof(sljit_sw);
	}
	if (remaining >= sizeof(uint32_t)) {
		return sizeof(uint32_t);
	}
	if (remaining >= sizeof(uint16_t)) {
		return sizeof(uint16_t);
	}
	return sizeof(uint8_t);
}

static sljit_s32 SljitStringChunkLoadOp(idx_t chunk_size) {
	if (chunk_size == sizeof(uint8_t)) {
		return SLJIT_MOV_U8;
	}
	if (chunk_size == sizeof(uint16_t)) {
		return SLJIT_MOV_U16;
	}
	if (chunk_size == sizeof(uint32_t)) {
		return SLJIT_MOV_U32;
	}
	if (chunk_size == sizeof(sljit_sw)) {
		return SLJIT_MOV;
	}
	throw InternalException("Unsupported SLJIT packed string comparison width");
}

static sljit_sw SljitStringChunkImmediate(const string &constant, idx_t constant_offset, idx_t chunk_size) {
	uint64_t value = 0;
	memcpy(&value, constant.data() + constant_offset, chunk_size);
	if (chunk_size == sizeof(sljit_sw)) {
		return SljitUInt64Immediate(value);
	}
	return NumericCast<sljit_sw>(value);
}

static void EmitStringBytesEqualAtPosition(struct sljit_compiler *compiler, const string &constant,
                                           idx_t constant_offset, idx_t compare_length, sljit_s32 data_reg,
                                           sljit_sw data_offset, sljit_s32 position_reg,
                                           vector<sljit_jump *> &mismatch_jumps) {
	if (data_reg == SLJIT_R0) {
		throw InternalException("SLJIT variable-position string comparison cannot use R0 as its data base");
	}
	for (idx_t byte_idx = 0; byte_idx < compare_length;) {
		const auto chunk_size = SljitStringCompareChunkSize(compare_length - byte_idx);
		const auto source_offset = data_offset + NumericCast<sljit_sw>(byte_idx);
		if (source_offset == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, position_reg, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, position_reg, 0, SLJIT_IMM, source_offset);
		}
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM2(data_reg, SLJIT_R0), 0);
		mismatch_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                   SljitStringChunkImmediate(constant, constant_offset + byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitStringBytesEqualAtFixedOffset(struct sljit_compiler *compiler, const string &constant,
                                              idx_t constant_offset, idx_t compare_length, sljit_s32 data_reg,
                                              sljit_sw data_offset, vector<sljit_jump *> &mismatch_jumps) {
	for (idx_t byte_idx = 0; byte_idx < compare_length;) {
		const auto chunk_size = SljitStringCompareChunkSize(compare_length - byte_idx);
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(data_reg), data_offset + NumericCast<sljit_sw>(byte_idx));
		mismatch_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                   SljitStringChunkImmediate(constant, constant_offset + byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitStringEqualsAtPosition(struct sljit_compiler *compiler, const string &constant, sljit_s32 data_reg,
                                       sljit_s32 position_reg, vector<sljit_jump *> &mismatch_jumps) {
	EmitStringBytesEqualAtPosition(compiler, constant, 0, constant.size(), data_reg, 0, position_reg, mismatch_jumps);
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

static bool PredicateDoubleCompareUsesHelper(const SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind);
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		       NativeDoubleSourceUsesHelper(predicate.double_right_source_kind);
	case SljitNativePredicateKind::NOT:
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		return predicate.child && PredicateDoubleCompareUsesHelper(*predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child && PredicateDoubleCompareUsesHelper(*child)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

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
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT: {
		auto compare_type = NativeDoubleCompareJumpType(predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
		                               predicate.double_source_scale, SLJIT_FR0);
		sljit_emit_fset64(compiler, SLJIT_FR1, predicate.double_constant);
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR1, 0, SLJIT_FR0, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR0, 0, SLJIT_FR1, 0));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES: {
		static constexpr sljit_sw LEFT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE;
		static constexpr sljit_sw RIGHT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE + sizeof(double);
		auto compare_type = NativeDoubleCompareJumpType(predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		if (NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		    NativeDoubleSourceUsesHelper(predicate.double_right_source_kind)) {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), LEFT_SPILL_OFFSET);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), RIGHT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                LEFT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
			                RIGHT_SPILL_OFFSET);
		} else {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_scale, SLJIT_FR0);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_scale, SLJIT_FR1);
		}
		result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR0, 0, SLJIT_FR1, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT: {
		auto compare_op =
		    predicate.constant_on_left ? FlipNativeIntegerCompareOp(predicate.compare_op) : predicate.compare_op;
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R3, SLJIT_R1,
		                                  offsetof(hugeint_t, upper), SLJIT_R4);
		EmitSljitInt128CompareBranches(compiler, compare_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_IMM,
		                               SljitUInt64Immediate(predicate.int128_constant_lower), SLJIT_IMM,
		                               NumericCast<sljit_sw>(predicate.int128_constant_upper), result);
		return result;
	}
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R3, SLJIT_R1,
		                                  offsetof(hugeint_t, upper), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.right_source_index, SLJIT_R4, SLJIT_S4,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.right_source_index, SLJIT_S4, SLJIT_S4,
		                                  offsetof(hugeint_t, upper), SLJIT_R1);
		EmitSljitInt128CompareBranches(compiler, predicate.compare_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R4, 0, SLJIT_S4,
		                               0, result);
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
		const auto inline_length = std::min<idx_t>(substring_length, string_t::PREFIX_LENGTH);
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
			if (inline_length > 0) {
				EmitStringBytesEqualAtFixedOffset(compiler, constant, 0, inline_length, SLJIT_R0,
				                                  STRING_INLINE_PREFIX_OFFSET, mismatch_jumps);
			}
			if (substring_length > inline_length) {
				EmitStringBytesEqualAtFixedOffset(compiler, constant, inline_length, substring_length - inline_length,
				                                  SLJIT_R4, NumericCast<sljit_sw>(inline_length), mismatch_jumps);
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
	if (PredicateDoubleCompareUsesHelper(predicate)) {
		local_size = SLJIT_SELECT_LOCAL_SIZE + sizeof(double) * 2;
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(3), 5, local_size);
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

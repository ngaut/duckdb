#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/string_vector.hpp"

#include "sljitLir.h"

#include <cstring>
#include <exception>

namespace duckdb {

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

	if (!IsSljitNativeFixedByteStorageSize(target_size)) {
		error = "SLJIT native string compression has unsupported target size " + std::to_string(target_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 6, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done =
	    EmitSljitSelectedSourceInvalidResultBranchLoop(compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &) {
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
	    });
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeStringDecompress(idx_t source_size, SljitNativeVectorFunction &function, string &error) {
	static_assert(sizeof(string_t) == 16, "SLJIT string decompression expects DuckDB string_t ABI size");

	if (!IsSljitNativeFixedByteStorageSize(source_size)) {
		error = "SLJIT native string decompression has unsupported source size " + std::to_string(source_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	sljit_jump *helper_error = nullptr;
	auto done =
	    EmitSljitSelectedSourceInvalidResultBranchLoop(compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &) {
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, active_source_index), SLJIT_R1, 0);
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, active_result_index), SLJIT_S1, 0);
		    sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		    sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                     SLJIT_FUNC_ADDR(SljitNativeStringDecompress));
		    sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, has_error));
		    helper_error = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	    });
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(helper_error, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb

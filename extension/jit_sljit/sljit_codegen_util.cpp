#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/execution_region_backend.hpp"

#include <chrono>

namespace duckdb {

static thread_local ExecutionRegionCompileTimings *active_sljit_codegen_timings = nullptr;

static int64_t SljitCodegenElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

SljitCodegenTimingScope::SljitCodegenTimingScope(ExecutionRegionCompileTimings *timings)
    : previous_timings(active_sljit_codegen_timings) {
	active_sljit_codegen_timings = timings;
}

SljitCodegenTimingScope::~SljitCodegenTimingScope() {
	active_sljit_codegen_timings = previous_timings;
}

void *GenerateSljitCode(struct sljit_compiler *compiler) {
	auto start = std::chrono::steady_clock::now();
	auto code = sljit_generate_code(compiler, 0, nullptr);
	if (active_sljit_codegen_timings) {
		active_sljit_codegen_timings->machine_codegen_time_us += SljitCodegenElapsedMicros(start);
	}
	return code;
}

class SljitCodeHandle : public ExecutionRegionCodeHandle {
public:
	SljitCodeHandle(void *code_p, idx_t code_size_p, vector<shared_ptr<void>> owned_data_p)
	    : code(code_p), code_size(code_size_p), owned_data(std::move(owned_data_p)) {
	}

	~SljitCodeHandle() override {
		if (code) {
			sljit_free_code(code, nullptr);
		}
	}

	idx_t CodeSize() const override {
		return code_size;
	}

private:
	void *code;
	idx_t code_size;
	vector<shared_ptr<void>> owned_data;
};

unique_ptr<ExecutionRegionCodeHandle> MakeSljitCodeHandle(void *code, idx_t code_size,
                                                          vector<shared_ptr<void>> owned_data) {
	return make_uniq<SljitCodeHandle>(code, code_size, std::move(owned_data));
}

unique_ptr<ExecutionRegionCodeHandle> FinishSljitCode(struct sljit_compiler *compiler, void *&code, string &error,
                                                      vector<shared_ptr<void>> owned_data) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	code = GenerateSljitCode(compiler);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	return MakeSljitCodeHandle(code, code_size, std::move(owned_data));
}

sljit_sw NativeIntegerDataScale(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
	case SljitNativeIntegerKind::UINT8:
		return 0;
	case SljitNativeIntegerKind::UINT16:
		return 1;
	case SljitNativeIntegerKind::UINT32:
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return 2;
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return 3;
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

sljit_s32 NativeIntegerLoadOp(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
		return SLJIT_MOV_S8;
	case SljitNativeIntegerKind::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeIntegerKind::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeIntegerKind::UINT32:
		return SLJIT_MOV_U32;
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SLJIT_MOV_S32;
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SLJIT_MOV;
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

sljit_s32 NativeIntegerStoreOp(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
	case SljitNativeIntegerKind::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeIntegerKind::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeIntegerKind::UINT32:
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SLJIT_MOV32;
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SLJIT_MOV;
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

sljit_s32 NativeIntegerBinaryOp(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	sljit_s32 result;
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		result = SLJIT_ADD;
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		result = SLJIT_SUB;
		break;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		result = SLJIT_MUL;
		break;
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
	if (kind == SljitNativeIntegerKind::INT32 || kind == SljitNativeIntegerKind::DATE) {
		result |= SLJIT_32;
	}
	return result;
}

sljit_s32 NativeIntegerCompareJumpType(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op) {
	sljit_s32 result;
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		result = SLJIT_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		result = SLJIT_NOT_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		result = SLJIT_SIG_LESS;
		break;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		result = SLJIT_SIG_GREATER;
		break;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		result = SLJIT_SIG_LESS_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		result = SLJIT_SIG_GREATER_EQUAL;
		break;
	default:
		throw InternalException("Unknown SLJIT native integer comparison operator");
	}
	if (kind == SljitNativeIntegerKind::INT32 || kind == SljitNativeIntegerKind::DATE) {
		result |= SLJIT_32;
	}
	return result;
}

sljit_s32 NativeDoubleOrderedCompareJumpType(SljitNativeIntegerCompareOp op) {
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
		throw InternalException("Unknown SLJIT native double comparison operator");
	}
}

sljit_s32 NativeDoubleCompareAgainstNumberJumpType(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		return SLJIT_UNORDERED_OR_NOT_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SLJIT_UNORDERED_OR_GREATER;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SLJIT_UNORDERED_OR_GREATER_EQUAL;
	default:
		return NativeDoubleOrderedCompareJumpType(op);
	}
}

sljit_s32 SljitCompareConditionSetFlags(sljit_s32 condition) {
	// each ordering condition shares its flag set with its negation, which sljit numbers next to it
	return condition <= SLJIT_NOT_EQUAL ? SLJIT_SET_Z : SLJIT_SET(condition & ~1);
}

void EmitLoadSljitValidityWord(struct sljit_compiler *compiler, sljit_s32 validity_reg, sljit_s32 index_reg,
                               sljit_s32 word_reg) {
	D_ASSERT(word_reg != validity_reg && word_reg != index_reg && validity_reg != index_reg);
	sljit_emit_op2(compiler, SLJIT_LSHR, word_reg, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, word_reg, 0, SLJIT_MEM2(validity_reg, word_reg), 3);
}

struct sljit_jump *EmitJumpIfSljitValidityWordBitClear(struct sljit_compiler *compiler, sljit_s32 word_reg,
                                                       sljit_s32 index_reg) {
	// the shift takes the row's bit within its word
	sljit_emit_op2(compiler, SLJIT_MLSHR, word_reg, 0, word_reg, 0, index_reg, 0);
	sljit_emit_op2u(compiler, SLJIT_AND | SLJIT_SET_Z, word_reg, 0, SLJIT_IMM, 1);
	return sljit_emit_jump(compiler, SLJIT_ZERO);
}

struct sljit_jump *EmitJumpIfSljitValidityBitClear(struct sljit_compiler *compiler, sljit_s32 validity_reg,
                                                   sljit_s32 index_reg, sljit_s32 word_reg) {
	EmitLoadSljitValidityWord(compiler, validity_reg, index_reg, word_reg);
	return EmitJumpIfSljitValidityWordBitClear(compiler, word_reg, index_reg);
}

void EmitLoadSljitValidityBit(struct sljit_compiler *compiler, sljit_s32 validity_reg, sljit_s32 index_reg,
                              sljit_s32 target_reg) {
	EmitLoadSljitValidityWord(compiler, validity_reg, index_reg, target_reg);
	sljit_emit_op2(compiler, SLJIT_MLSHR, target_reg, 0, target_reg, 0, index_reg, 0);
	sljit_emit_op2(compiler, SLJIT_AND, target_reg, 0, target_reg, 0, SLJIT_IMM, 1);
}

sljit_sw NativeSignedIntegerDataScale(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return 0;
	case SljitNativeSignedIntegerWidth::INT16:
		return 1;
	case SljitNativeSignedIntegerWidth::INT32:
		return 2;
	case SljitNativeSignedIntegerWidth::INT64:
		return 3;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

sljit_s32 NativeSignedIntegerLoadOp(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return SLJIT_MOV_S8;
	case SljitNativeSignedIntegerWidth::INT16:
		return SLJIT_MOV_S16;
	case SljitNativeSignedIntegerWidth::INT32:
		return SLJIT_MOV_S32;
	case SljitNativeSignedIntegerWidth::INT64:
		return SLJIT_MOV;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

sljit_s32 NativeSignedIntegerStoreOp(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return SLJIT_MOV_U8;
	case SljitNativeSignedIntegerWidth::INT16:
		return SLJIT_MOV_U16;
	case SljitNativeSignedIntegerWidth::INT32:
		return SLJIT_MOV32;
	case SljitNativeSignedIntegerWidth::INT64:
		return SLJIT_MOV;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

int64_t NativeSignedIntegerMin(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return NumericLimits<int8_t>::Minimum();
	case SljitNativeSignedIntegerWidth::INT16:
		return NumericLimits<int16_t>::Minimum();
	case SljitNativeSignedIntegerWidth::INT32:
		return NumericLimits<int32_t>::Minimum();
	case SljitNativeSignedIntegerWidth::INT64:
		return NumericLimits<int64_t>::Minimum();
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

int64_t NativeSignedIntegerMax(SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return NumericLimits<int8_t>::Maximum();
	case SljitNativeSignedIntegerWidth::INT16:
		return NumericLimits<int16_t>::Maximum();
	case SljitNativeSignedIntegerWidth::INT32:
		return NumericLimits<int32_t>::Maximum();
	case SljitNativeSignedIntegerWidth::INT64:
		return NumericLimits<int64_t>::Maximum();
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

bool NativeSignedIntegerCastNeedsRangeCheck(SljitNativeSignedIntegerWidth source_width,
                                            SljitNativeSignedIntegerWidth target_width) {
	return NativeSignedIntegerMin(target_width) > NativeSignedIntegerMin(source_width) ||
	       NativeSignedIntegerMax(target_width) < NativeSignedIntegerMax(source_width);
}

sljit_sw NativeUnsignedIntegerDataScale(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return 0;
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return 1;
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return 2;
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

sljit_s32 NativeUnsignedIntegerLoadOp(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return SLJIT_MOV_U32;
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

sljit_s32 NativeUnsignedIntegerStoreOp(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return SLJIT_MOV32;
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

int64_t NativeUnsignedIntegerMax(SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return NumericLimits<uint8_t>::Maximum();
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return NumericLimits<uint16_t>::Maximum();
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return NumericLimits<uint32_t>::Maximum();
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

void EmitSljitAddDoubleWord(struct sljit_compiler *compiler, sljit_s32 lower_reg, sljit_s32 upper_reg,
                            sljit_s32 value_lower_reg, sljit_s32 value_upper_reg) {
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, lower_reg, 0, lower_reg, 0, value_lower_reg, 0);
	sljit_emit_op2(compiler, SLJIT_ADDC, upper_reg, 0, upper_reg, 0, value_upper_reg, 0);
}

} // namespace duckdb

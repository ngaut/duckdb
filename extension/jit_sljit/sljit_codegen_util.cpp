#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

class SljitCodeHandle : public ExecutionRegionCodeHandle {
public:
	SljitCodeHandle(void *code_p, idx_t code_size_p) : code(code_p), code_size(code_size_p) {
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
};

unique_ptr<ExecutionRegionCodeHandle> MakeSljitCodeHandle(void *code, idx_t code_size) {
	return make_uniq<SljitCodeHandle>(code, code_size);
}

sljit_sw NativeIntegerDataScale(SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
	case SljitNativeIntegerKind::UINT8:
		return 0;
	case SljitNativeIntegerKind::INT32:
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
	case SljitNativeIntegerKind::INT32:
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
	case SljitNativeIntegerKind::INT32:
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
	if (kind == SljitNativeIntegerKind::INT32) {
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
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
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

} // namespace duckdb

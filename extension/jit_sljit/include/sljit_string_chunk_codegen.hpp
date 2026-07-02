#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstring>

namespace duckdb {

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
		return static_cast<sljit_sw>(value);
	}
	return NumericCast<sljit_sw>(value);
}

} // namespace duckdb

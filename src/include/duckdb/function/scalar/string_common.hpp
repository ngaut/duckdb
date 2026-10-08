#pragma once

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "utf8proc_wrapper.hpp"

#include <cstring>
#include <type_traits>

namespace duckdb {

bool IsAscii(const char *input, idx_t n);
//! Returns the index of the first byte with the high bit set, or n if all bytes are ASCII
idx_t FirstNonAscii(const char *input, idx_t n);
idx_t LowerLength(const char *input_data, idx_t input_length);
void LowerCase(const char *input_data, idx_t input_length, char *result_data);
idx_t FindStrInStr(const string_t &haystack_s, const string_t &needle_s);
idx_t FindStrInStr(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle, idx_t needle_size);
string_t SubstringASCII(Vector &result, string_t input, int64_t offset, int64_t length);
string_t SubstringUnicode(Vector &result, string_t input, int64_t offset, int64_t length);
string_t SubstringGrapheme(Vector &result, string_t input, int64_t offset, int64_t length);
//! Whether the offset and length are within the range supported by substring - it throws for values outside of it
bool SubstringInSupportedRange(int64_t offset, int64_t length);
unique_ptr<BaseStatistics> PropagateStringSliceStats(FunctionStatisticsInput &input, idx_t start_character_index,
                                                     optional_idx character_count);
//! Common util zonemap pruning for `prefix(s, constant)`.
FilterPropagateResult PrefixFilterPrune(const FunctionStatisticsPruneInput &input);

ScalarFunction GetStringContains();
DUCKDB_API bool Glob(const char *s, idx_t slen, const char *pattern, idx_t plen, bool allow_question_mark = true);

static inline bool IsCharacter(char c) {
	return (c & 0xc0) != 0x80;
}

static inline string_t SubstringPrefixUnicode(string_t input, idx_t length) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();
	if (length == 0 || input_size == 0) {
		return string_t(input_data, 0);
	}

	idx_t characters = 0;
	idx_t end_pos = input_size;
	for (idx_t byte_idx = 0; byte_idx < input_size; byte_idx++) {
		if (!IsCharacter(input_data[byte_idx])) {
			continue;
		}
		if (characters == length) {
			end_pos = byte_idx;
			break;
		}
		characters++;
	}
	return string_t(input_data, UnsafeNumericCast<uint32_t>(end_pos));
}

template <class TA, class TR>
static inline TR Length(TA input) {
	auto input_data = input.GetData();
	auto input_length = input.GetSize();
	// ASCII bytes are one code point each, so only the remainder needs to be counted
	const auto ascii_end = FirstNonAscii(input_data, input_length);
	if (ascii_end == input_length) {
		return UnsafeNumericCast<TR>(input_length);
	}
	TR length = UnsafeNumericCast<TR>(ascii_end);
	for (idx_t i = ascii_end; i < input_length; i++) {
		length += IsCharacter(input_data[i]);
	}
	return length;
}

template <class TA, class TR>
static inline TR GraphemeCount(TA input) {
	auto input_data = input.GetData();
	auto input_length = input.GetSize();
	if (FirstNonAscii(input_data, input_length) == input_length) {
		// ASCII: every byte is a grapheme cluster
		return UnsafeNumericCast<TR>(input_length);
	}
	return UnsafeNumericCast<TR>(Utf8Proc::GraphemeCount(input_data, input_length));
}

template <idx_t LENGTH>
static inline void StringCompressionReverseMemCpy(const data_ptr_t &__restrict dest,
                                                  const const_data_ptr_t &__restrict src) {
	for (idx_t i = 0; i < LENGTH; i++) {
		dest[i] = src[LENGTH - 1 - i];
	}
}

static inline void StringCompressionReverseMemCpy(const data_ptr_t &__restrict dest,
                                                  const const_data_ptr_t &__restrict src, const idx_t &length) {
	for (idx_t i = 0; i < length; i++) {
		dest[i] = src[length - 1 - i];
	}
}

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompressWideValue(const string_t &input) {
	D_ASSERT(input.GetSize() < sizeof(RESULT_TYPE));
	RESULT_TYPE result;
	const auto result_ptr = data_ptr_cast(&result);
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		StringCompressionReverseMemCpy<sizeof(RESULT_TYPE)>(result_ptr, const_data_ptr_cast(input.GetPrefix()));
	} else if (input.IsInlined()) {
		static constexpr auto REMAINDER = sizeof(RESULT_TYPE) - string_t::INLINE_LENGTH;
		StringCompressionReverseMemCpy<string_t::INLINE_LENGTH>(result_ptr + REMAINDER,
		                                                        const_data_ptr_cast(input.GetPrefix()));
		memset(result_ptr, '\0', REMAINDER);
	} else {
		const auto size = MinValue<idx_t>(sizeof(RESULT_TYPE), input.GetSize());
		const auto remainder = sizeof(RESULT_TYPE) - size;
		StringCompressionReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), size);
		memset(result_ptr, '\0', remainder);
	}
	result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());
	return BSwapIfBE(result);
}

static inline uint8_t StringCompressUInt8Value(const string_t &input) {
	D_ASSERT(input.GetSize() <= sizeof(uint8_t));
	uint8_t result = input.GetSize() == 0
	                     ? 0
	                     : UnsafeNumericCast<uint8_t>(input.GetSize() + *const_data_ptr_cast(input.GetPrefix()));
	return BSwapIfBE(result);
}

template <class RESULT_TYPE>
static inline RESULT_TYPE StringCompressValue(const string_t &input) {
	if constexpr (std::is_same<RESULT_TYPE, uint8_t>::value) {
		return StringCompressUInt8Value(input);
	} else {
		return StringCompressWideValue<RESULT_TYPE>(input);
	}
}

template <class RESULT_TYPE>
static inline bool TryStringCompressValue(const string_t &input, RESULT_TYPE &result) {
	if constexpr (std::is_same<RESULT_TYPE, uint8_t>::value) {
		if (input.GetSize() > sizeof(uint8_t)) {
			return false;
		}
	} else if (input.GetSize() >= sizeof(RESULT_TYPE)) {
		return false;
	}
	result = StringCompressValue<RESULT_TYPE>(input);
	return true;
}

} // namespace duckdb

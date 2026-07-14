//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_selected_input_vector_group_key.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_date_year_runtime.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#include <array>
#include <cstring>
#include <type_traits>
#include <utility>

namespace duckdb {

template <class T>
struct SljitSelectedInputGroupIdentity {
	static constexpr bool CAN_FAIL = false;
	static constexpr bool STAGE_TRANSFORMED_KEYS = false;

	bool operator()(const T &source, T &target) const {
		target = source;
		return true;
	}
	bool ConvertPreflighted(const T &source, T &target) const {
		return (*this)(source, target);
	}
};

template <class SRC, class DST>
struct SljitSelectedInputGroupNarrowingCast {
	static constexpr bool CAN_FAIL = false;
	static constexpr bool STAGE_TRANSFORMED_KEYS = false;

	bool unchecked;

	bool operator()(SRC source, DST &target) const {
		if (unchecked) {
			target = static_cast<DST>(source);
			return true;
		}
		if (!TryCast::Operation<SRC, DST>(source, target, false)) {
			throw InvalidInputException(CastExceptionText<SRC, DST>(source));
		}
		return true;
	}
	bool ConvertPreflighted(SRC source, DST &target) const {
		return (*this)(source, target);
	}
};

template <class SRC, class DST>
struct SljitSelectedInputGroupIntegralCompress {
	static constexpr bool CAN_FAIL = true;
	static constexpr bool STAGE_TRANSFORMED_KEYS = true;

	int64_t minimum;

	bool CanConvert(SRC source) const {
		DST target;
		return Convert(source, target);
	}
	bool operator()(SRC source, DST &target) const {
		return Convert(source, target);
	}
	bool ConvertPreflighted(SRC source, DST &target) const {
		return Convert(source, target);
	}

private:
	bool Convert(SRC source, DST &target) const {
		SRC typed_minimum;
		SRC compressed;
		return TryCast::Operation<int64_t, SRC>(minimum, typed_minimum, false) &&
		       TrySubtractOperator::Operation<SRC, SRC, SRC>(source, typed_minimum, compressed) &&
		       TryCast::Operation<SRC, DST>(compressed, target, false);
	}
};

template <class DST>
struct SljitSelectedInputGroupDateYearCompress {
	static constexpr bool CAN_FAIL = false;
	static constexpr bool STAGE_TRANSFORMED_KEYS = false;

	int64_t minimum;

	bool operator()(int32_t source, DST &target) const {
		target = SljitDateYearCompressedGroupKeyOrThrow<DST>(source, minimum);
		return true;
	}
	bool ConvertPreflighted(int32_t source, DST &target) const {
		return (*this)(source, target);
	}
};

template <class DST>
static DST SljitCompressSelectedStringGroup(const string_t &source) {
	return StringCompressValue<DST>(source);
}

template <>
inline uhugeint_t SljitCompressSelectedStringGroup<uhugeint_t>(const string_t &source) {
#if DUCKDB_IS_BIG_ENDIAN
	return StringCompressValue<uhugeint_t>(source);
#else
	const auto length = source.GetSize();
	D_ASSERT(length < sizeof(uhugeint_t));
	const auto data = source.GetDataUnsafe();
	uint64_t lower = length;
	uint64_t upper = 0;
	if (length >= sizeof(uint64_t)) {
		upper = BSWAP64(Load<uint64_t>(const_data_ptr_cast(data)));
		uint64_t tail = 0;
		std::memcpy(&tail, data + sizeof(uint64_t), length - sizeof(uint64_t));
		lower |= BSWAP64(tail);
	} else if (length > 0) {
		uint64_t head = 0;
		std::memcpy(&head, data, length);
		upper = BSWAP64(head);
	}
	return uhugeint_t(upper, lower);
#endif
}

//! Only the unsigned physical types used by the string-compression transform
//! can be reconstructed into the canonical inlined string_t representation.
//! Keep the admission as a type trait: generic target dispatch instantiates
//! every physical width, while this specialization has a precise ABI contract.
template <class T>
struct SljitInlineStringStorageSignatureSupported : std::false_type {};

template <>
struct SljitInlineStringStorageSignatureSupported<uint16_t> : std::true_type {};

template <>
struct SljitInlineStringStorageSignatureSupported<uint32_t> : std::true_type {};

template <>
struct SljitInlineStringStorageSignatureSupported<uint64_t> : std::true_type {};

template <>
struct SljitInlineStringStorageSignatureSupported<uhugeint_t> : std::true_type {};

struct SljitInlineStringStorageSignature {
	uint64_t header;
	uint64_t tail;
	bool requires_tail;

	bool MatchesInlined(const string_t &value, uint64_t value_header) const {
		D_ASSERT(value.IsInlined());
		return value_header == header &&
		       (!requires_tail || Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t)) == tail);
	}

	bool Matches(const string_t &value) const {
		if (!value.IsInlined()) {
			return false;
		}
		return MatchesInlined(value, Load<uint64_t>(const_data_ptr_cast(&value)));
	}
};

//! Classify a small established string group domain after one canonical string
//! header load. The tail comparison stays attached to the candidate signature:
//! a matching four-byte prefix is not sufficient to establish equality.
static bool SljitTryClassifyOneOrTwoInlineStringStorageSignatures(
    const string_t &value, bool value_is_valid, const std::array<bool, 2> &known_group_validity,
    const std::array<SljitInlineStringStorageSignature, 2> &known_group_signatures, idx_t group_count,
    idx_t &group_idx) {
	D_ASSERT(group_count > 0 && group_count <= 2);
	if (!value_is_valid) {
		if (!known_group_validity[0]) {
			group_idx = 0;
			return true;
		}
		if (group_count == 2 && !known_group_validity[1]) {
			group_idx = 1;
			return true;
		}
		return false;
	}
	if (!value.IsInlined()) {
		return false;
	}
	const auto value_header = Load<uint64_t>(const_data_ptr_cast(&value));
	if (known_group_validity[0] && known_group_signatures[0].MatchesInlined(value, value_header)) {
		group_idx = 0;
		return true;
	}
	if (group_count == 2 && known_group_validity[1] && known_group_signatures[1].MatchesInlined(value, value_header)) {
		group_idx = 1;
		return true;
	}
	return false;
}

//! Reconstruct the canonical inlined string_t storage from a compressed key.
//! This lets a known small group domain compare an input string directly
//! against its already-compressed group keys without recompressing every row.
template <class T>
static bool SljitTryInlineStringStorageSignature(T compressed, SljitInlineStringStorageSignature &signature) {
	static_assert(SljitInlineStringStorageSignatureSupported<T>::value,
	              "only unsigned string-compression targets have an inline storage signature");
	auto le_compressed = BSwapIfBE(compressed);
	auto compressed_data = const_data_ptr_cast(&le_compressed);
	const auto length = compressed_data[0];
	if (length > string_t::INLINE_LENGTH) {
		return false;
	}
	string_t value(UnsafeNumericCast<uint32_t>(length));
	auto value_data = data_ptr_cast(value.GetPrefixWriteable());
	if constexpr (sizeof(T) <= string_t::INLINE_LENGTH) {
		StringCompressionReverseMemCpy<sizeof(T)>(value_data, compressed_data);
		memset(value_data + sizeof(T) - 1, 0, string_t::INLINE_LENGTH - sizeof(T) + 1);
	} else {
		static constexpr auto REMAINDER = sizeof(T) - string_t::INLINE_LENGTH;
		StringCompressionReverseMemCpy<string_t::INLINE_LENGTH>(value_data, compressed_data + REMAINDER);
	}
	D_ASSERT(value.IsInlined());
	signature.header = Load<uint64_t>(const_data_ptr_cast(&value));
	signature.tail = Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t));
	signature.requires_tail = length > string_t::PREFIX_LENGTH;
	return true;
}

template <class DST>
struct SljitSelectedInputGroupStringCompress {
	static constexpr bool CAN_FAIL = true;
	static constexpr bool STAGE_TRANSFORMED_KEYS = true;

	bool CanConvert(const string_t &source) const {
		if constexpr (std::is_same<DST, uint8_t>::value) {
			return source.GetSize() <= sizeof(DST);
		}
		return source.GetSize() < sizeof(DST);
	}
	bool operator()(const string_t &source, DST &target) const {
		if (!CanConvert(source)) {
			return false;
		}
		target = SljitCompressSelectedStringGroup<DST>(source);
		return true;
	}
	bool ConvertPreflighted(const string_t &source, DST &target) const {
		target = SljitCompressSelectedStringGroup<DST>(source);
		return true;
	}
};

template <class SRC, class DST, class CONVERT, class CONSUMER>
static bool SljitWithSelectedInputVectorGroupKey(Vector &input, const SelectionVector &selection, CONVERT convert,
                                                 CONSUMER &&consumer) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto data = UnifiedVectorFormat::GetData<SRC>(format);
	auto source_index = [&](idx_t selected_row) {
		const auto input_row = selection.get_index(selected_row);
		return format.sel->get_index(input_row);
	};
	auto preflight = [&](idx_t count) {
		if constexpr (!CONVERT::CAN_FAIL) {
			return true;
		} else {
			for (idx_t selected_row = 0; selected_row < count; selected_row++) {
				const auto source_row = source_index(selected_row);
				if (format.validity.RowIsValid(source_row) && !convert.CanConvert(data[source_row])) {
					return false;
				}
			}
			return true;
		}
	};
	auto load = [&](idx_t selected_row, DST &key, bool &valid) {
		const auto source_row = source_index(selected_row);
		valid = format.validity.RowIsValid(source_row);
		if (!valid) {
			key = DST {};
			return true;
		}
		return convert(data[source_row], key);
	};
	auto preflighted_load = [&](idx_t selected_row, DST &key, bool &valid) {
		const auto source_row = source_index(selected_row);
		valid = format.validity.RowIsValid(source_row);
		if (!valid) {
			key = DST {};
			return true;
		}
		return convert.ConvertPreflighted(data[source_row], key);
	};
	auto prepare = [&](idx_t count, DST *prepared_keys, uint8_t *prepared_validity) {
		if (count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		for (idx_t selected_row = 0; selected_row < count; selected_row++) {
			const auto source_row = source_index(selected_row);
			const bool valid = format.validity.RowIsValid(source_row);
			prepared_validity[selected_row] = valid;
			if (!valid) {
				prepared_keys[selected_row] = DST {};
				continue;
			}
			if (!convert(data[source_row], prepared_keys[selected_row])) {
				return false;
			}
		}
		return true;
	};
	// A fallible transform must prove every selected value before it mutates a
	// local accumulator. The converter owns whether a staged key repays its
	// transform work; destination width alone does not determine that tradeoff.
	return consumer(load, preflight, preflighted_load, prepare, std::integral_constant < bool,
	                CONVERT::CAN_FAIL &&CONVERT::STAGE_TRANSFORMED_KEYS > {});
}

template <class SRC, class DST, class CONSUMER>
static bool SljitWithSelectedIntegralCompressedInputVectorGroupKey(Vector &input, const SelectionVector &selection,
                                                                   const ExecutionRowPointerGroupKeySource &source,
                                                                   CONSUMER &&consumer) {
	return SljitWithSelectedInputVectorGroupKey<SRC, DST>(
	    input, selection, SljitSelectedInputGroupIntegralCompress<SRC, DST> {source.cast_constant},
	    std::forward<CONSUMER>(consumer));
}

template <class DST, class CONSUMER>
static bool SljitDispatchSelectedInputVectorGroupKey(Vector &input, const SelectionVector &selection,
                                                     const ExecutionRowPointerGroupKeySource &source,
                                                     bool source_key0_int64_to_int32_unchecked, CONSUMER &&consumer) {
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    input.GetType() != source.source_type || source.target_physical_type != GetTypeId<DST>()) {
		return false;
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != GetTypeId<DST>()) {
			return false;
		}
		return SljitWithSelectedInputVectorGroupKey<DST, DST>(input, selection, SljitSelectedInputGroupIdentity<DST> {},
		                                                      std::forward<CONSUMER>(consumer));
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if constexpr (std::is_same<DST, int32_t>::value) {
			const bool unchecked = source.unchecked_integral_cast ||
			                       (source_key0_int64_to_int32_unchecked && source.hash_join_condition_idx == 0);
			return SljitWithSelectedInputVectorGroupKey<int64_t, DST>(
			    input, selection, SljitSelectedInputGroupNarrowingCast<int64_t, DST> {unchecked},
			    std::forward<CONSUMER>(consumer));
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if constexpr (std::is_same<DST, int16_t>::value) {
			return SljitWithSelectedInputVectorGroupKey<int64_t, DST>(
			    input, selection, SljitSelectedInputGroupNarrowingCast<int64_t, DST> {source.unchecked_integral_cast},
			    std::forward<CONSUMER>(consumer));
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if constexpr (std::is_same<DST, int8_t>::value) {
			return SljitWithSelectedInputVectorGroupKey<int32_t, DST>(
			    input, selection, SljitSelectedInputGroupNarrowingCast<int32_t, DST> {source.unchecked_integral_cast},
			    std::forward<CONSUMER>(consumer));
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		switch (source.source_physical_type) {
		case PhysicalType::INT8:
			return SljitWithSelectedIntegralCompressedInputVectorGroupKey<int8_t, DST>(
			    input, selection, source, std::forward<CONSUMER>(consumer));
		case PhysicalType::INT16:
			return SljitWithSelectedIntegralCompressedInputVectorGroupKey<int16_t, DST>(
			    input, selection, source, std::forward<CONSUMER>(consumer));
		case PhysicalType::INT32:
			return SljitWithSelectedIntegralCompressedInputVectorGroupKey<int32_t, DST>(
			    input, selection, source, std::forward<CONSUMER>(consumer));
		case PhysicalType::INT64:
			return SljitWithSelectedIntegralCompressedInputVectorGroupKey<int64_t, DST>(
			    input, selection, source, std::forward<CONSUMER>(consumer));
		default:
			return false;
		}
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
			return false;
		}
		return SljitWithSelectedInputVectorGroupKey<int32_t, DST>(
		    input, selection, SljitSelectedInputGroupDateYearCompress<DST> {source.cast_constant},
		    std::forward<CONSUMER>(consumer));
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		return SljitWithSelectedInputVectorGroupKey<string_t, DST>(
		    input, selection, SljitSelectedInputGroupStringCompress<DST> {}, std::forward<CONSUMER>(consumer));
	default:
		return false;
	}
}

} // namespace duckdb

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

#include <cstring>
#include <type_traits>
#include <utility>

namespace duckdb {

template <class T>
struct SljitSelectedInputGroupIdentity {
	static constexpr bool CAN_FAIL = false;

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

template <class DST>
struct SljitSelectedInputGroupStringCompress {
	static constexpr bool CAN_FAIL = true;

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
	return consumer(load, preflight, preflighted_load, prepare, std::integral_constant < bool,
	                CONVERT::CAN_FAIL && (sizeof(DST) > sizeof(uint64_t)) > {});
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

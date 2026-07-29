//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_single_key_reader_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"

#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

template <bool UNCHECKED>
static inline int32_t SljitCastHashJoinKeyInt64ToInt32(int64_t value) {
	if constexpr (UNCHECKED) {
		return UnsafeNumericCast<int32_t>(value);
	} else {
		return NumericCast<int32_t>(value);
	}
}

template <class T>
struct SljitHashJoinDirectKeyReader {
	using Key = T;

	explicit SljitHashJoinDirectKeyReader(const SljitNativeRegularHashJoinProbeInput &input)
	    : data(reinterpret_cast<const T *__restrict>(input.source_data[0])) {
	}

	inline T Load(const sel_t source_idx) const {
		return data[source_idx];
	}

	const T *__restrict data;
};

template <bool UNCHECKED>
struct SljitHashJoinInt64ToInt32KeyReader {
	using Key = int32_t;

	explicit SljitHashJoinInt64ToInt32KeyReader(const SljitNativeRegularHashJoinProbeInput &input)
	    : data(reinterpret_cast<const int64_t *__restrict>(input.source_data[0])) {
	}

	inline int32_t Load(const sel_t source_idx) const {
		return SljitCastHashJoinKeyInt64ToInt32<UNCHECKED>(data[source_idx]);
	}

	const int64_t *__restrict data;
};

template <class DISPATCH>
static bool SljitDispatchHashJoinSingleKeyReader(const SljitNativeHashJoinProbePlan &plan,
                                                 const SljitNativeRegularHashJoinProbeInput &input,
                                                 DISPATCH &dispatch) {
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		if (input.source_key0_int64_to_int32_unchecked) {
			dispatch.template Execute<SljitHashJoinInt64ToInt32KeyReader<true>>();
		} else {
			dispatch.template Execute<SljitHashJoinInt64ToInt32KeyReader<false>>();
		}
		return true;
	}

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<int8_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<uint8_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<int16_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<uint16_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<int32_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<uint32_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<int64_t>>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		dispatch.template Execute<SljitHashJoinDirectKeyReader<uint64_t>>();
		return true;
	default:
		return false;
	}
}

} // namespace duckdb

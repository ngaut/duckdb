//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_primitive_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

enum class AggregatePrimitiveUpdateKind : uint8_t { NONE, SUM_INT64, SUM_HUGEINT, COUNT_STAR };

static inline bool AggregatePrimitiveUpdateKindIsSupported(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
		return true;
	default:
		return false;
	}
}

static inline bool AggregatePrimitiveUpdateRequiresPayload(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return true;
	default:
		return false;
	}
}

static inline bool AggregatePrimitiveUpdateHasStateIsSet(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return true;
	default:
		return false;
	}
}

static inline bool AggregatePrimitiveUpdateUsesInt64State(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
		return true;
	default:
		return false;
	}
}

static inline bool AggregatePrimitiveUpdateUsesHugeintState(AggregatePrimitiveUpdateKind kind) {
	return kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
}

static inline idx_t AggregatePrimitiveUpdateStateValueSize(AggregatePrimitiveUpdateKind kind) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
		return sizeof(int64_t);
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return sizeof(hugeint_t);
	default:
		return 0;
	}
}

struct AggregatePrimitiveUpdateABI {
	AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
	PhysicalType input_type = PhysicalType::INVALID;
	idx_t state_size = 0;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;

	bool IsReady() const {
		if (kind == AggregatePrimitiveUpdateKind::NONE || state_size == 0) {
			return false;
		}
		if (AggregatePrimitiveUpdateRequiresPayload(kind) && input_type == PhysicalType::INVALID) {
			return false;
		}
		return AggregatePrimitiveUpdateStateValueSize(kind) > 0;
	}
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_consumer_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class SljitHashJoinAggregateConsumerStatus : uint8_t { MATERIALIZED, EXECUTED, EMPTY, DEFERRED };

enum class SljitHashJoinAggregateConsumerDispatch : uint8_t { UNBOUND, DIRECT, HYBRID, MATERIALIZED };

struct SljitHashJoinAggregateConsumerResult {
	SljitHashJoinAggregateConsumerStatus status = SljitHashJoinAggregateConsumerStatus::MATERIALIZED;
	idx_t matched_count = 0;
	string deferred_reason;
};

} // namespace duckdb

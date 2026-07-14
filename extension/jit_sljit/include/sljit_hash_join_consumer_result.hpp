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

enum class SljitHashJoinAggregateConsumerStatus : uint8_t { NOT_APPLICABLE, EXECUTED, DEFERRED };

struct SljitHashJoinAggregateConsumerResult {
	SljitHashJoinAggregateConsumerStatus status = SljitHashJoinAggregateConsumerStatus::NOT_APPLICABLE;
	idx_t matched_count = 0;
	const char *blocker = nullptr;
	string deferred_reason;
};

} // namespace duckdb

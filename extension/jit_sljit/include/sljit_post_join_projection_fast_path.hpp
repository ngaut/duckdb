//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_fast_path.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/constants.hpp"

#include <array>

namespace duckdb {

struct SljitStringSetCaseGroupedPayloadProjection {
	static constexpr idx_t CONSTANT_COUNT = 2;

	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	idx_t compressed_group_source_idx = DConstants::INVALID_INDEX;
	std::array<string, CONSTANT_COUNT> constants;
};

enum class SljitPostJoinProjectionFastPath : uint8_t { NONE, STRING_SET_CASE_GROUPED_PAYLOAD };

} // namespace duckdb

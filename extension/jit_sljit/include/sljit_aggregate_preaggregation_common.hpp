//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_preaggregation_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

static constexpr idx_t SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT = 64;
static constexpr idx_t SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT = STANDARD_VECTOR_SIZE;
static constexpr idx_t SLJIT_LOCAL_DENSE_PREAGGREGATION_MIN_COMPRESSION = 8;

template <class T>
static T *PrepareFlatPreaggregatedGroupTarget(DataChunk &compact_groups) {
	compact_groups.Reset();
	auto &target = compact_groups.data[0];
	target.SetVectorType(VectorType::FLAT_VECTOR);
	return FlatVector::GetDataMutable<T>(target);
}

static void FinishFlatPreaggregatedGroupTarget(DataChunk &compact_groups, idx_t group_count) {
	auto &target = compact_groups.data[0];
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.Reset(group_count);
	target_validity.SetAllValid(group_count);
	FlatVector::SetSize(target, count_t(group_count));
	compact_groups.SetChildCardinality(group_count);
}

} // namespace duckdb

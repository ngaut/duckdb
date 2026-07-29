//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class ExecutionRegionRuntime;
struct SljitDirectJoinOutputAggregateStrategy;
struct SljitExecutableRegionOp;
struct SljitPostJoinProjectionStrategy;
struct SljitRegionExecutionScratch;

//! Binds the projection-to-aggregate descriptor once at the batch boundary.
//! Both materialized output and direct hash-probe consumers share this owner.
bool SljitTryPrepareDirectJoinOutputAggregateDescriptor(ExecutionRegionRuntime &runtime,
                                                        vector<SljitExecutableRegionOp> &ops,
                                                        SljitRegionExecutionScratch &scratch,
                                                        optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy,
                                                        SljitPostJoinProjectionStrategy &post_join_projection,
                                                        idx_t row_count,
                                                        optional_ptr<const vector<idx_t>> output_column_map = nullptr,
                                                        idx_t output_projection_idx = DConstants::INVALID_INDEX);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_dense_group_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_region_adapter_scratch.hpp"

namespace duckdb {

bool TryPreaggregateDensePrimitiveGroups(SljitExecutableRegionOp &op, DataChunk &input,
                                         const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                         DataChunk &compact_groups,
                                         SljitPreaggregatedPrimitiveAggregateScratch &scratch);

bool TryPreaggregateDenseFusedPrimitiveGroups(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch);

} // namespace duckdb

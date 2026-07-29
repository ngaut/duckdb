//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_preaggregation_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class DataChunk;
class ExecutionRegionRuntime;
struct ExecutionGroupedAggregateStateAddressBinding;
struct ExecutionPrimitiveAggregateUpdateLane;
struct ExecutionRowPointerGroupKeySource;
struct SljitAggregatePayloadAdapterScratch;
enum class SljitAggregatePayloadSourceLayout : uint8_t;
struct SljitExecutableRegionOp;
struct SljitGroupedReductionLaneBinding;
struct SljitPendingPreaggregatedPrimitiveGroupBatch;
struct SljitPreaggregatedPrimitiveAggregateScratch;
struct SljitRegionExecutionScratch;

//! Batch-level entry points for typed grouped-preaggregation runtime families.
//! Their implementation owns physical-type template dispatch; callers do not
//! instantiate that graph, and the typed row loops remain directly bound.
bool SljitTryPreaggregateInputVectorPrimitiveGroupRunsBest(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count, bool &fused_payloads);

bool SljitTryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish);

bool SljitTryPreaggregateInputVectorPrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish);

} // namespace duckdb

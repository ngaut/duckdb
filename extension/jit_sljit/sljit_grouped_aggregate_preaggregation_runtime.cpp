//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_preaggregation_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_grouped_aggregate_preaggregation_api.hpp"
#include "sljit_grouped_aggregate_pending_preaggregation_runtime.hpp"
#include "sljit_grouped_aggregate_run_preaggregation_runtime.hpp"

namespace duckdb {

bool SljitTryPreaggregateInputVectorPrimitiveGroupRunsBest(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    optional_ptr<DataChunk> run_group_keys, idx_t &group_count, bool &fused_payloads) {
	return TryPreaggregateInputVectorPrimitiveGroupRunsBest(
	    op, input, group_sources, payload_source_indices, payload_source_layout, payload_lanes, reduction_lanes,
	    scratch, payload_scratch, run_group_keys, group_count, fused_payloads);
}

bool SljitTryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish) {
	return TryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPending(
	    runtime, scratch, op_idx, op, input, group_sources, payload_source_indices, payload_lanes, grouped_state,
	    pending, finish);
}

bool SljitTryPreaggregateInputVectorPrimitiveGroupsIntoPending(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitPendingPreaggregatedPrimitiveGroupBatch &pending,
    bool finish) {
	return TryPreaggregateInputVectorPrimitiveGroupsIntoPending(runtime, scratch, op_idx, op, input, group_sources,
	                                                            payload_source_indices, payload_lanes, grouped_state,
	                                                            pending, finish);
}

} // namespace duckdb

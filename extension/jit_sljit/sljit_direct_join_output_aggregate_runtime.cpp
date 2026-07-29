//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_join_output_aggregate_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_direct_join_output_aggregate_api.hpp"

#include "sljit_direct_join_output_aggregate_state.hpp"
#include "sljit_direct_join_output_aggregate_trace.hpp"
#include "sljit_hash_join_projection_aggregate_input_runtime.hpp"
#include "sljit_post_join_projection_strategy.hpp"
#include "sljit_projection_aggregate_descriptor.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

bool SljitTryPrepareDirectJoinOutputAggregateDescriptor(
    ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
    optional_ptr<SljitDirectJoinOutputAggregateStrategy> strategy_ptr,
    SljitPostJoinProjectionStrategy &post_join_projection, idx_t row_count,
    optional_ptr<const vector<idx_t>> output_column_map, idx_t output_projection_idx) {
	if (!strategy_ptr || strategy_ptr->disabled) {
		return false;
	}
	auto &strategy = *strategy_ptr;
	if (strategy.aggregate_idx >= ops.size()) {
		throw InternalException("SLJIT direct join-output aggregate index is out of range");
	}
	const bool has_projection_chain = post_join_projection.HasProjectionChain();
	const bool descriptor_ready =
	    has_projection_chain
	        ? SljitTryBuildPostJoinProjectionAggregateDescriptor(ops, scratch, post_join_projection,
	                                                             strategy.aggregate_idx, strategy.descriptor,
	                                                             output_column_map, output_projection_idx)
	        : SljitTryBuildSelectedJoinAggregateInputDescriptor(ops, scratch, post_join_projection.hash_join_idx,
	                                                            strategy.aggregate_idx, strategy.descriptor,
	                                                            output_column_map, output_projection_idx);
	if (descriptor_ready) {
		strategy.descriptor.EnsureInput(runtime.GetAllocator());
		return true;
	}
	SljitRecordDirectJoinOutputAggregateProjectionUnsupported(runtime, ops, post_join_projection,
	                                                          strategy.descriptor.Blocker(), row_count);
	strategy.last_failure = strategy.descriptor.Blocker();
	strategy.disabled = true;
	return false;
}

} // namespace duckdb

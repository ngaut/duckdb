//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_state_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static void FinishGroupedAggregateStateUpdates(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                               ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                               const char *phase) {
	auto finish_stage_start = SljitRegionStageStart(runtime);
	grouped_state.state->FinishStateUpdates();
	RecordSljitRegionStageRuntime(runtime, op_idx, SljitNativeRegionOpKind::AGGREGATE_UPDATE, phase,
	                              finish_stage_start);
}

static void SljitFinishDeferredGroupedAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                      SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
                                                      bool &deferred_grouped_finish) {
	if (!deferred_grouped_finish) {
		return;
	}
	auto &binding = scratch.SinkBinding(aggregate_idx);
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
	    !binding.aggregate_update.grouped_state.state) {
		throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
	}
	FinishGroupedAggregateStateUpdates(runtime, aggregate_idx, binding.aggregate_update.grouped_state,
	                                   "finish_deferred_grouped_state_updates");
	deferred_grouped_finish = false;
}

static bool NeedsGroupedAggregateStateAddressPlan(const SljitExecutableAggregateUpdate &aggregate_update) {
	return aggregate_update.plan.use_grouped_state_addresses &&
	       !aggregate_update.fused_payload_update_owns_group_lookup;
}

static void MarkDeferredGroupedFinish(bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (defer_grouped_finish && deferred_grouped_finish) {
		*deferred_grouped_finish = true;
	}
}

} // namespace duckdb

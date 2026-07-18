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

static bool NeedsGroupedAggregateStateAddressPlan(const SljitExecutableAggregateUpdate &aggregate_update) {
	return aggregate_update.plan.use_grouped_state_addresses &&
	       !aggregate_update.fused_payload_update_owns_group_lookup;
}

static void
RecordPreaggregatedGroupedAggregateRepresentedRows(ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                                   idx_t represented_row_count, idx_t compact_group_count) {
	if (represented_row_count > compact_group_count) {
		grouped_state.state->RecordDirectStateAddressUpdates(represented_row_count - compact_group_count);
	}
}

} // namespace duckdb

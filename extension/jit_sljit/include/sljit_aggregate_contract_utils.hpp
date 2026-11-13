//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_contract_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_region_ir.hpp"

namespace duckdb {

static bool SljitAggregateSinkHasDistinctState(const ExecutionRegionAggregateSinkInfo &sink_info) {
	auto &contract = sink_info.contract;
	if (contract.distinct_aggregate_count != 0 || contract.distinct_table_count != 0 ||
	    contract.distinct_child_count != 0 || contract.distinct_filter_count != 0) {
		return true;
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (aggregate.distinct) {
			return true;
		}
	}
	return false;
}

//! An aggregate the region updates in DuckDB's own states, without distinct or sorted inputs: an ungrouped aggregate,
//! or a perfect-hash one whose table the region writes. Chunks DuckDB's operators sink and chunks the region updates
//! may alternate; the region sinks every row it holds before DuckDB's operators take a chunk, so rows reach the
//! states in source order, as an order-dependent aggregate needs
static bool SljitAggregateSinkAdmitsNativeChunks(const ExecutionRegionAggregateSinkInfo &sink_info) {
	if ((sink_info.Kind() != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	     sink_info.Kind() != ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) ||
	    SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (aggregate.has_order_bys) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

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

//! An ungrouped aggregate whose states add up regardless of which rows each update saw, in any order: chunks DuckDB's
//! operators sink and chunks the region updates may alternate
static bool SljitUngroupedAggregateSinkAdmitsNativeChunks(const ExecutionRegionAggregateSinkInfo &sink_info) {
	if (sink_info.Kind() != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (aggregate.order_dependent || aggregate.has_order_bys) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

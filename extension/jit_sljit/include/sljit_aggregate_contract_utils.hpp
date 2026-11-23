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

//! An aggregate without distinct or sorted inputs: chunks DuckDB's operators sink and chunks the region runs may
//! alternate into it; the region sinks every row it holds before DuckDB's operators take a chunk
static bool SljitAggregateSinkTakesUnsortedChunks(const ExecutionRegionAggregateSinkInfo &sink_info) {
	if (SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (aggregate.has_order_bys) {
			return false;
		}
	}
	return true;
}

//! An ungrouped aggregate, or a perfect-hash one whose table the region writes, without distinct or sorted inputs, in
//! DuckDB's own states: the region's chunks and DuckDB's may alternate into it
static bool SljitAggregateSinkAdmitsNativeChunks(const ExecutionRegionAggregateSinkInfo &sink_info) {
	return (sink_info.Kind() == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	        sink_info.Kind() == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE) &&
	       SljitAggregateSinkTakesUnsortedChunks(sink_info);
}

} // namespace duckdb

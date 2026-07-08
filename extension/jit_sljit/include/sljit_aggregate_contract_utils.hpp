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

static bool SljitAggregateSinkHasDistinctState(const ExecutionRegionSinkInfo &sink_info) {
	auto &contract = sink_info.aggregate_contract;
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

static bool SljitDistinctKeySinkSetInputType(vector<LogicalType> &input_types, idx_t input_idx,
                                             const LogicalType &type) {
	if (input_idx == DConstants::INVALID_INDEX) {
		return false;
	}
	if (input_idx >= input_types.size()) {
		input_types.resize(input_idx + 1);
	}
	if (input_types[input_idx].id() != LogicalTypeId::INVALID && input_types[input_idx] != type) {
		return false;
	}
	input_types[input_idx] = type;
	return true;
}

static bool SljitAggregateSinkCanUseDistinctKeySink(const ExecutionRegionSinkInfo &sink_info) {
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE) {
		return false;
	}
	auto &contract = sink_info.aggregate_contract;
	if (!SljitAggregateSinkHasDistinctState(sink_info)) {
		return false;
	}
	return !sink_info.groups.empty() && contract.aggregate_count != 0 &&
	       contract.aggregate_count == contract.distinct_aggregate_count && contract.distinct_table_count != 0 &&
	       contract.distinct_child_count != 0 && contract.aggregate_filter_count == 0 &&
	       contract.non_distinct_filter_count == 0 && contract.aggregate_order_count == 0;
}

static bool SljitTryBuildDistinctKeySinkInputTypes(const ExecutionRegionSinkInfo &sink_info,
                                                   vector<LogicalType> &input_types) {
	input_types.clear();
	if (!SljitAggregateSinkCanUseDistinctKeySink(sink_info)) {
		return false;
	}
	for (auto &group : sink_info.groups) {
		if (!SljitDistinctKeySinkSetInputType(input_types, group.input_index, group.type)) {
			input_types.clear();
			return false;
		}
	}
	for (auto &aggregate : sink_info.aggregates) {
		if (!aggregate.distinct) {
			input_types.clear();
			return false;
		}
		if (aggregate.child_count == 0 || aggregate.child_types.empty()) {
			input_types.clear();
			return false;
		}
		if (aggregate.child_indices.size() == aggregate.child_types.size()) {
			for (idx_t child_idx = 0; child_idx < aggregate.child_indices.size(); child_idx++) {
				if (!SljitDistinctKeySinkSetInputType(input_types, aggregate.child_indices[child_idx],
				                                      aggregate.child_types[child_idx])) {
					input_types.clear();
					return false;
				}
			}
			continue;
		}
		if (aggregate.payload_index == DConstants::INVALID_INDEX ||
		    aggregate.payload_index + aggregate.child_types.size() < aggregate.payload_index) {
			input_types.clear();
			return false;
		}
		for (idx_t child_idx = 0; child_idx < aggregate.child_types.size(); child_idx++) {
			if (!SljitDistinctKeySinkSetInputType(input_types, aggregate.payload_index + child_idx,
			                                      aggregate.child_types[child_idx])) {
				input_types.clear();
				return false;
			}
		}
	}
	if (input_types.empty()) {
		return false;
	}
	for (auto &type : input_types) {
		if (type.id() == LogicalTypeId::INVALID) {
			input_types.clear();
			return false;
		}
	}
	return true;
}

} // namespace duckdb

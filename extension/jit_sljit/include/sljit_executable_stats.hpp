//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

namespace duckdb {

void SljitUpdateExecutableCurrentNotNull(const SljitNativeRegionOpPlan &op, vector<bool> &current_not_null);
vector<bool> SljitBuildExecutableOutputNotNull(const SljitNativeRegionOpPlan &op, const vector<bool> &input_not_null);
void SljitUpdateExecutableCurrentDistinctCounts(const SljitNativeRegionOpPlan &op,
                                                vector<idx_t> &current_distinct_counts,
                                                const vector<Value> &current_min_values,
                                                const vector<Value> &current_max_values);
void SljitUpdateExecutableCurrentDistinctReserveCounts(const SljitNativeRegionOpPlan &op,
                                                       vector<idx_t> &current_distinct_reserve_counts,
                                                       const vector<idx_t> &current_distinct_counts,
                                                       const vector<Value> &current_min_values,
                                                       const vector<Value> &current_max_values);
bool SljitTryGetHashJoinRHSOutputConditionIndex(const ExecutionRegionHashJoinContract &contract, idx_t rhs_output_idx,
                                                idx_t &condition_idx);
bool SljitTryGetHashJoinProbeKeyInputIndex(const SljitNativeRegionOpPlan &op, idx_t condition_idx, idx_t &input_idx);
void SljitUpdateExecutableCurrentRanges(const SljitNativeRegionOpPlan &op, vector<Value> &current_min_values,
                                        vector<Value> &current_max_values);
void SljitSpecializeAggregatePayloadRanges(SljitNativeAggregateUpdatePlan &aggregate_update,
                                           const vector<Value> &input_min_values,
                                           const vector<Value> &input_max_values);

} // namespace duckdb

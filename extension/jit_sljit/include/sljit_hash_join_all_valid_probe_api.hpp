//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"

namespace duckdb {

struct SljitHashJoinDirectUngroupedAggregateProbeConsumer;
struct SljitHashJoinMatchedRowBatchConsumer;

//! Consumer-specialized probe templates are owned by the runtime translation
//! unit. These vector-level boundaries keep the hot match loop specialized
//! without exposing the heavy dispatch templates to pipeline orchestration.
bool SljitTryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbe(bool selected,
                                                                       const SljitNativeHashJoinProbePlan &plan,
                                                                       SljitNativeRegularHashJoinProbeInput &input);
bool SljitTryExecuteAllValidSingleKeyNoChainProbeWithBatchConsumer(bool selected,
                                                                   const SljitNativeHashJoinProbePlan &plan,
                                                                   SljitNativeRegularHashJoinProbeInput &input,
                                                                   SljitHashJoinMatchedRowBatchConsumer &consumer);
bool SljitTryExecuteAllValidSingleKeyNoChainDirectUngroupedAggregateProbe(
    bool selected, const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitHashJoinDirectUngroupedAggregateProbeConsumer &consumer);

} // namespace duckdb

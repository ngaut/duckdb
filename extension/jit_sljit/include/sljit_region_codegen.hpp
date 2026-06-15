//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

unique_ptr<JitCodeHandle> BuildSljitUngroupedCountStarUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                             string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedCountUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                         string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedSumInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                            string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedSumHugeintInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                                   string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedCountStarUpdate(SljitNativeGroupedAggregateFunction &function,
                                                           string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedCountUpdate(SljitNativeGroupedAggregateFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedSumInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                          string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedSumHugeintInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                                 string &error);
unique_ptr<JitCodeHandle> BuildSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                                   idx_t equality_key_count, bool mark_build_match,
                                                   idx_t found_match_offset, idx_t pointer_offset,
                                                   JitRegionHashJoinProbeOutputMode output_mode,
                                                   SljitNativeHashJoinProbeFunction &function, string &error);

} // namespace duckdb

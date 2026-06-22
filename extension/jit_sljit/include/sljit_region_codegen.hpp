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

#include "duckdb/execution/execution_region_telemetry.hpp"

namespace duckdb {

bool ValidateSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys, idx_t equality_key_count,
                                ExecutionHashJoinProbeOutputMode output_mode, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                                              idx_t equality_key_count, bool mark_build_match,
                                                              idx_t found_match_offset, idx_t pointer_offset,
                                                              ExecutionHashJoinProbeOutputMode output_mode,
                                                              SljitNativeHashJoinProbeFunction &function,
                                                              string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNestedLoopJoinProbe(const SljitNativeNestedLoopJoinProbePlan &plan,
                                                                    SljitNativeNestedLoopJoinProbeFunction &function,
                                                                    string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitPerfectHashJoinProbe(const SljitNativeHashJoinProbeKeyPlan &key,
                                                                     ExecutionHashJoinProbeOutputMode output_mode,
                                                                     SljitNativeHashJoinProbeFunction &function,
                                                                     string &error);

} // namespace duckdb

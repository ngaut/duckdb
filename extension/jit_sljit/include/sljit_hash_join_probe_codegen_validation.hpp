//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_codegen_validation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

namespace duckdb {

bool SljitValidatePerfectHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error);
bool SljitValidateRegularHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error);
bool SljitValidateHashJoinProbePlan(const SljitNativeHashJoinProbePlan &plan, string &error);

} // namespace duckdb

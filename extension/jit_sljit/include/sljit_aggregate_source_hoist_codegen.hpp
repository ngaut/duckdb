//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_source_hoist_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"
#include "sljit_region_plan.hpp"

namespace duckdb {

vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitAggregateSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                           const vector<sljit_s32> &data_regs, idx_t min_use_count);

} // namespace duckdb

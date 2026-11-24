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

//! The sources the payloads and the loop's other expressions (group keys, the predicate) reference most often, one per
//! data register
vector<SljitTypedExpressionTreeLoopHoist>
BuildSljitAggregateSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                           const vector<reference<const ExecutionExpressionIR>> &expressions,
                                           const vector<sljit_s32> &data_regs, idx_t min_use_count);
//! The sources the expressions reference most often, one per data register, whose data pointers a loop over flat
//! inputs holds in those registers
vector<SljitTypedExpressionTreeLoopHoist>
BuildSljitExpressionSourceDataPointerHoists(const vector<reference<const ExecutionExpressionIR>> &expressions,
                                            const vector<sljit_s32> &data_regs, idx_t min_use_count);

} // namespace duckdb

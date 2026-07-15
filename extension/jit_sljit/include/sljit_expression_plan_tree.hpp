//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_expression_plan_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

namespace duckdb {

unique_ptr<ExecutionExpressionIR> CopySljitExpressionPlanAsInputTree(const SljitNativeRegionExpressionPlan &expr);

} // namespace duckdb

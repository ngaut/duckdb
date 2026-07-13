//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_predicate_simd_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"
#include "sljit_typed_expression_simd_codegen.hpp"

namespace duckdb {

// A semantics-preserving split of a native AND predicate. The leading typed
// children execute as a packed mask; rows that survive continue through the
// specialized native residual. Keeping a prefix (rather than collecting
// arbitrary supported children) preserves the original evaluation order.
struct SljitNativePredicatePartialSimdPlan {
	bool supported = false;
	unique_ptr<ExecutionExpressionIR> packed_prefix;
	unique_ptr<SljitNativePredicate> residual;
	SljitTypedExpressionTreeSimdPlan simd;
};

SljitNativePredicatePartialSimdPlan TryPlanSljitNativePredicatePartialSimd(const ExecutionExpressionIR &root,
                                                                           const vector<bool> &source_not_null);

} // namespace duckdb

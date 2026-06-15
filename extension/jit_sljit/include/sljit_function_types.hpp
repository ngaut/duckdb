//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_function_types.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "sljitLir.h"

namespace duckdb {

using SljitNativeVectorFunction = void(SLJIT_FUNC *)(SljitNativeVectorInput *);
using SljitNativePredicateFunction = void(SLJIT_FUNC *)(SljitNativePredicateInput *);
using SljitFusedFilterProjectionFunction = void(SLJIT_FUNC *)(SljitFusedFilterProjectionInput *);
using SljitNativeUngroupedAggregateFunction = void(SLJIT_FUNC *)(SljitNativeUngroupedAggregateInput *);
using SljitNativeGroupedAggregateFunction = void(SLJIT_FUNC *)(SljitNativeGroupedAggregateInput *);
using SljitFusedUngroupedAggregateFunction = void(SLJIT_FUNC *)(SljitFusedUngroupedAggregateInput *);
using SljitFusedPerfectHashAggregateFunction = void(SLJIT_FUNC *)(SljitFusedPerfectHashAggregateInput *);
using SljitNativeHashJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativeHashJoinProbeInput *);

} // namespace duckdb

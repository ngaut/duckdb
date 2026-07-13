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
using SljitNativeAggregateUpdateFunction = void(SLJIT_FUNC *)(SljitNativeVectorInput *);
using SljitNativePrimitiveRunFunction = void(SLJIT_FUNC *)(SljitNativePrimitiveRunInput *);
using SljitNativePredicateFunction = void(SLJIT_FUNC *)(SljitNativePredicateInput *);
using SljitNativeRegularHashJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativeRegularHashJoinProbeInput *);
using SljitNativePerfectHashJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativePerfectHashJoinProbeInput *);
using SljitNativeNestedLoopJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativeNestedLoopJoinProbeInput *);

} // namespace duckdb

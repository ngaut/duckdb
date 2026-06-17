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
using SljitNativePredicateFunction = void(SLJIT_FUNC *)(SljitNativePredicateInput *);
using SljitNativeHashJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativeHashJoinProbeInput *);
using SljitNativeNestedLoopJoinProbeFunction = void(SLJIT_FUNC *)(SljitNativeNestedLoopJoinProbeInput *);

} // namespace duckdb

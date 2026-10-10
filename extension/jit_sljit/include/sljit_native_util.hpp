//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_primitive_update.hpp"

#include "sljit_native_types.hpp"

namespace duckdb {

string NativeSignedIntegerTypeName(SljitNativeSignedIntegerWidth width);
string NativeUnsignedIntegerTypeName(SljitNativeUnsignedIntegerWidth width);
bool SljitFilteredAggregateUsesPayloadExpression(AggregatePrimitiveUpdateKind kind);
bool SljitFilteredAggregateKindCanGenerate(AggregatePrimitiveUpdateKind kind);

} // namespace duckdb

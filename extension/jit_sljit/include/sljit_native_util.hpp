//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

namespace duckdb {

class ClientContext;

string MaybeDumpIr(ClientContext &context, string ir);
string NativeSignedIntegerTypeName(SljitNativeSignedIntegerWidth width);
string NativeUnsignedIntegerTypeName(SljitNativeUnsignedIntegerWidth width);
string NativeIntegerBinaryOverflowMessage(SljitNativeIntegerBinaryOp op);
string NativeIntegerCastOverflowMessage(SljitNativeSignedIntegerWidth source_width,
                                        SljitNativeSignedIntegerWidth target_width);
string NativeSignedToUnsignedIntegerCastOverflowMessage(SljitNativeSignedIntegerWidth source_width,
                                                        SljitNativeUnsignedIntegerWidth target_width);

} // namespace duckdb

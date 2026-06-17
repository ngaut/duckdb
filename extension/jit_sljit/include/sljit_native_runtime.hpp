//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

const_data_ptr_t NativeIntegerSourceData(UnifiedVectorFormat &format, SljitNativeIntegerKind kind);
data_ptr_t NativeIntegerResultData(Vector &result, SljitNativeIntegerKind kind);
const_data_ptr_t NativeSignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeSignedIntegerWidth width);
data_ptr_t NativeSignedIntegerResultData(Vector &result, SljitNativeSignedIntegerWidth width);
const_data_ptr_t NativeUnsignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeUnsignedIntegerWidth width);
data_ptr_t NativeUnsignedIntegerResultData(Vector &result, SljitNativeUnsignedIntegerWidth width);
bool NativePredicateRequiresInput(const SljitNativePredicate &predicate);

void PrepareSljitPredicateSources(DataChunk *input, bool requires_input, vector<UnifiedVectorFormat> &formats,
                                  vector<const_data_ptr_t> &source_data, vector<const sel_t *> &source_sel,
                                  vector<const validity_t *> &source_validity);

} // namespace duckdb

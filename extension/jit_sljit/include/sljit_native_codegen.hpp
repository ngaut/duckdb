//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

unique_ptr<JitCodeHandle> BuildSljitNativeNullCheck(SljitNativeNullCheckOp op, SljitNativeVectorFunction &function,
                                                    string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerBinaryConstant(SljitNativeIntegerKind kind,
                                                                SljitNativeIntegerBinaryOp op, bool constant_on_left,
                                                                SljitNativeVectorFunction &function, string &error,
                                                                bool check_result_range = false,
                                                                int64_t result_min = 0, int64_t result_max = 0);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                  SljitNativeIntegerBinaryOp op,
                                                                  SljitNativeVectorFunction &function, string &error,
                                                                  bool check_result_range = false,
                                                                  int64_t result_min = 0, int64_t result_max = 0);
unique_ptr<JitCodeHandle> BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op, bool constant_on_left,
                                                               SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeDoubleBinaryReferences(SljitNativeDoubleBinaryOp op,
                                                                 SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerCast(SljitNativeSignedIntegerWidth source_width,
                                                      SljitNativeSignedIntegerWidth target_width, bool try_cast,
                                                      SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeSignedToUnsignedIntegerCast(
    SljitNativeSignedIntegerWidth source_width, SljitNativeUnsignedIntegerWidth target_width, bool try_cast,
    SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerCoalesce(SljitNativeSignedIntegerWidth width,
                                                          SljitNativeCoalesceRhsKind rhs_kind,
                                                          bool constant_is_null,
                                                          SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerCompareConstant(SljitNativeIntegerKind kind,
                                                                 SljitNativeIntegerCompareOp op,
                                                                 bool constant_on_left,
                                                                 SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerCompareReferences(SljitNativeIntegerKind kind,
                                                                   SljitNativeIntegerCompareOp op,
                                                                   SljitNativeVectorFunction &function,
                                                                   string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerSelectConstant(SljitNativeIntegerKind kind,
                                                                SljitNativeIntegerCompareOp op, bool constant_on_left,
                                                                SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerSelectReferences(SljitNativeIntegerKind kind,
                                                                  SljitNativeIntegerCompareOp op,
                                                                  SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeNullCheckSelect(SljitNativeNullCheckOp op,
                                                          SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerInList(SljitNativeIntegerKind kind, idx_t constant_count,
                                                        bool list_has_null, bool not_in,
                                                        SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerInListSelect(SljitNativeIntegerKind kind, idx_t constant_count,
                                                              bool list_has_null, bool not_in,
                                                              SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerBetween(SljitNativeIntegerKind kind, int64_t lower, int64_t upper,
                                                         bool lower_inclusive, bool upper_inclusive, bool not_between,
                                                         SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegerBetweenSelect(SljitNativeIntegerKind kind, int64_t lower,
                                                               int64_t upper, bool lower_inclusive,
                                                               bool upper_inclusive, bool not_between,
                                                               SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeStringCompressUInt8(SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegralCompress(SljitNativeSignedIntegerWidth source_width,
                                                           SljitNativeUnsignedIntegerWidth target_width,
                                                           SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeIntegralDecompress(SljitNativeUnsignedIntegerWidth source_width,
                                                             SljitNativeSignedIntegerWidth target_width,
                                                             SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeDateYear(SljitNativeVectorFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativeConstantOrNull(const vector<idx_t> &guard_source_indices,
                                                         SljitNativePredicateFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitNativePredicate(const SljitNativePredicate &predicate, bool generate_result,
                                                    SljitNativePredicateFunction &function, string &error);

} // namespace duckdb

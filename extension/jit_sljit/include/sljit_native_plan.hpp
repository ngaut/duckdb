//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

namespace duckdb {

bool TryGetNativeSignedIntegerWidth(const ExecutionExpressionIR &node, SljitNativeSignedIntegerWidth &width);
string NativeIntegerBinaryReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op);
string NativeIntegerBinaryReferenceReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op);
string NativeDoubleBinaryReason(SljitNativeDoubleBinaryOp op);
string NativeDoubleBinaryReferenceReason(SljitNativeDoubleBinaryOp op);
string NativeIntegerCompareReason(SljitNativeIntegerKind kind);
string NativeIntegerCompareReferenceReason(SljitNativeIntegerKind kind);
string NativeIntegerCastReason(SljitNativeSignedIntegerWidth source_width, SljitNativeSignedIntegerWidth target_width,
                               bool try_cast);
string NativeIntegerCoalesceReason(SljitNativeSignedIntegerWidth width);
string NativeIntegerInListReason(SljitNativeIntegerKind kind, bool not_in);
string NativeIntegerBetweenReason(SljitNativeIntegerKind kind, bool not_between);
string NativeNullCheckReason(SljitNativeNullCheckOp op);

bool TryGetNativeIntegerCompareOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerCompareOp &compare_op);
bool TryReadNativeIntegerBinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                        SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                        bool &constant_on_left);
bool TryReadNativeIntegerBinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          SljitNativeIntegerKind &kind, idx_t &left_source_index,
                                          idx_t &right_source_index);
bool TryReadNativeDoubleBinaryConstant(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                       SljitNativeDoubleSourceKind &source_kind, idx_t &source_index,
                                       double &source_scale, double &constant_value, bool &constant_on_left);
bool TryReadNativeDoubleBinaryReferences(const ExecutionExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                         SljitNativeDoubleSourceKind &left_kind, idx_t &left_source_index,
                                         double &left_scale, SljitNativeDoubleSourceKind &right_kind,
                                         idx_t &right_source_index, double &right_scale);
bool TryReadNativeDecimal64BinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                            idx_t &left_source_index, idx_t &right_source_index, int64_t &result_min,
                                            int64_t &result_max);
bool TryReadNativeDecimal64BinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          idx_t &source_index, int64_t &constant_value, bool &constant_on_left,
                                          int64_t &result_min, int64_t &result_max);
bool TryReadNativeDateBinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                       idx_t &date_source_index, idx_t &integer_source_index);
bool TryReadNativeDateBinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                     idx_t &date_source_index, int64_t &constant_value, bool &constant_on_left);
bool TryReadNativeIntegerCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                         SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                         bool &constant_on_left);
bool TryReadNativeIntegerCompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                           SljitNativeIntegerKind &kind, idx_t &left_source_index,
                                           idx_t &right_source_index);
bool TryReadNativeIntegerCast(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                              SljitNativeSignedIntegerWidth &target_width, idx_t &source_index, bool &try_cast);
bool TryReadNativeSignedToUnsignedIntegerCast(const ExecutionExpressionIR &root,
                                              SljitNativeSignedIntegerWidth &source_width,
                                              SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                              bool &try_cast);
bool TryReadNativeIntegralCompress(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                                   SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                   int64_t &minimum);
bool TryReadNativeIntegralDecompress(const ExecutionExpressionIR &root, SljitNativeUnsignedIntegerWidth &source_width,
                                     SljitNativeSignedIntegerWidth &target_width, idx_t &source_index,
                                     int64_t &minimum);
bool TryReadNativeIntegerCoalesce(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &width,
                                  idx_t &source_index, SljitNativeCoalesceRhsKind &rhs_kind, idx_t &right_source_index,
                                  int64_t &constant_value, bool &constant_is_null);
bool TryReadNativeIntegerInList(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                vector<int64_t> &constants, bool &list_has_null, bool &not_in);
bool TryReadNativeIntegerBetween(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                 int64_t &lower, int64_t &upper, bool &lower_inclusive, bool &upper_inclusive,
                                 bool &not_between);
bool TryReadNativeStringPrefixConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &prefix);
bool TryReadNativeStringSubstringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, vector<string> &constants);
bool TryReadNativeNullCheck(const ExecutionExpressionIR &root, SljitNativeNullCheckOp &op, idx_t &source_index);
bool TryReadNativeConstantOrNull(const ExecutionExpressionIR &root, SljitNativeConstantOrNull &expr);
bool ShouldTryNativePredicateRoot(const ExecutionExpressionIR &root);
bool TryBuildNativeBasePredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate);
bool TryBuildNativeConstantOrNullPredicate(const ExecutionExpressionIR &root,
                                           unique_ptr<SljitNativePredicate> &predicate);
bool TryBuildNativeConjunctionPredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate);
bool TryBuildNativeNullCheckPredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate);
void FinalizeSljitNativePredicateSourceIndices(SljitNativePredicate &predicate);
bool TryBuildNativePredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate);

} // namespace duckdb

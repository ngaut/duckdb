//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeNullCheck(SljitNativeNullCheckOp op,
                                                                SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
                                      SljitNativeVectorFunction &function, string &error,
                                      bool check_arithmetic_overflow = true, bool check_result_range = false,
                                      int64_t result_min = 0, int64_t result_max = 0);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                          bool constant_on_left, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerBinaryReferences(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                        SljitNativeVectorFunction &function, string &error,
                                        bool check_arithmetic_overflow = true, bool check_result_range = false,
                                        int64_t result_min = 0, int64_t result_max = 0);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFlatIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                                  SljitNativeIntegerBinaryOp op,
                                                                                  SljitNativeVectorFunction &function,
                                                                                  string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatIntegerProjection(const vector<SljitNativeRegionExpressionPlan> &plans,
                                      const vector<idx_t> &projection_indices, SljitNativeVectorFunction &function,
                                      string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                                           SljitNativeDoubleSourceKind source_kind,
                                                                           bool constant_on_left, bool single_precision,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryConstant(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind source_kind,
                                         bool constant_on_left, bool single_precision,
                                         SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                       SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                       SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleBinaryReferences(SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind,
                                           SljitNativeDoubleSourceKind right_kind, bool single_precision,
                                           SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeFlatDoubleProjection(const vector<SljitNativeRegionExpressionPlan> &plans,
                                     const vector<idx_t> &projection_indices, SljitNativeVectorFunction &function,
                                     string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerCast(SljitNativeSignedIntegerWidth source_width,
                                                                  SljitNativeSignedIntegerWidth target_width,
                                                                  bool try_cast, SljitNativeVectorFunction &function,
                                                                  string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeSignedToUnsignedIntegerCast(SljitNativeSignedIntegerWidth source_width,
                                            SljitNativeUnsignedIntegerWidth target_width, bool try_cast,
                                            SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerCoalesce(SljitNativeSignedIntegerWidth width, SljitNativeCoalesceRhsKind rhs_kind,
                                bool constant_is_null, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerCompareConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                       bool constant_on_left, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerCompareReferences(SljitNativeIntegerKind kind,
                                                                               SljitNativeIntegerCompareOp op,
                                                                               SljitNativeVectorFunction &function,
                                                                               string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerSelectConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                      bool constant_on_left, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerSelectReferences(SljitNativeIntegerKind kind,
                                                                              SljitNativeIntegerCompareOp op,
                                                                              SljitNativeVectorFunction &function,
                                                                              string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeNullCheckSelect(SljitNativeNullCheckOp op, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerInList(SljitNativeIntegerKind kind, idx_t constant_count,
                                                                    bool list_has_null, bool not_in,
                                                                    SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerInListSelect(SljitNativeIntegerKind kind, idx_t constant_count, bool list_has_null, bool not_in,
                                    SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetween(SljitNativeIntegerKind kind, int64_t lower,
                                                                     int64_t upper, bool lower_inclusive,
                                                                     bool upper_inclusive, bool not_between,
                                                                     SljitNativeVectorFunction &function,
                                                                     string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetweenSelect(SljitNativeIntegerKind kind, int64_t lower,
                                                                           int64_t upper, bool lower_inclusive,
                                                                           bool upper_inclusive, bool not_between,
                                                                           SljitNativeVectorFunction &function,
                                                                           string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeStringCompress(idx_t target_size, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeStringDecompress(idx_t source_size, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeTypedExpressionTree(const ExecutionExpressionIR &root,
                                                                          SljitNativeIntegerKind result_kind,
                                                                          SljitNativeVectorFunction &function,
                                                                          string &error,
                                                                          bool emit_flat_nullable_fast_path = true);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeTypedExpressionTreeSelect(const ExecutionExpressionIR &root, SljitNativeVectorFunction &function,
                                          string &error, bool emit_flat_nullable_fast_path = true);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64ExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64TypedExpressionTree(const ExecutionExpressionIR &root,
                                                     SljitNativeAggregateUpdateFunction &function, string &error,
                                                     bool emit_flat_nullable_fast_path = true);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(const ExecutionExpressionIR &root,
                                                       SljitNativeAggregateUpdateFunction &function, string &error,
                                                       bool emit_flat_nullable_fast_path = true);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, SljitNativeAggregateUpdateFunction &function,
    string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintExpressionTree(const ExecutionExpressionIR &root,
                                                  SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleReference(SljitNativeDoubleSourceKind kind,
                                            SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                         string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                         string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumHugeintReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                       string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
    SljitNativeAggregateUpdateFunction &function, string &error, bool check_arithmetic_overflow = true,
    bool check_result_range = false, int64_t result_min = 0, int64_t result_max = 0);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeAggregateUpdateFunction &function,
    string &error, bool check_arithmetic_overflow = true, bool check_result_range = false, int64_t result_min = 0,
    int64_t result_max = 0);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                       SljitNativeDoubleSourceKind source_kind, bool constant_on_left,
                                                       SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
    SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind, SljitNativeDoubleSourceKind right_kind,
    SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                       const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const ExecutionRegionAggregateContract &contract, const vector<bool> &source_not_null,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralCompress(SljitNativeSignedIntegerWidth source_width,
                                                                       SljitNativeUnsignedIntegerWidth target_width,
                                                                       SljitNativeVectorFunction &function,
                                                                       string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralDecompress(SljitNativeUnsignedIntegerWidth source_width,
                                                                         SljitNativeSignedIntegerWidth target_width,
                                                                         SljitNativeVectorFunction &function,
                                                                         string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDateYear(SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeErrorGuardedReference(idx_t value_size, SljitNativeIntegerCompareOp guard_compare_op,
                                      bool guard_constant_on_left, SljitNativeVectorFunction &function, string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeConstantOrNull(const vector<idx_t> &guard_source_indices,
                                                                     SljitNativePredicateFunction &function,
                                                                     string &error);
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePredicate(const SljitNativePredicate &predicate,
                                                                bool generate_result,
                                                                SljitNativePredicateFunction &function, string &error);

} // namespace duckdb

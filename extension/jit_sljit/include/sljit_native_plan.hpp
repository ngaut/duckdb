//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

enum class SljitNativeExpressionPlanKind : uint8_t {
	NULL_CHECK,
	INTEGER_IN_LIST,
	INTEGER_BETWEEN,
	PREDICATE,
	INTEGER_BINARY_REFERENCES,
	DOUBLE_BINARY_REFERENCES,
	INTEGER_CAST,
	INTEGER_COALESCE,
	CONSTANT_OR_NULL,
	INTEGER_COMPARE_REFERENCES,
	INTEGER_COMPARE_CONSTANT,
	INTEGER_BINARY_CONSTANT,
	DOUBLE_BINARY_CONSTANT
};

struct SljitNativeExpressionPlan {
	SljitNativeExpressionPlanKind kind = SljitNativeExpressionPlanKind::NULL_CHECK;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	SljitNativeIntegerBinaryOp binary_op = SljitNativeIntegerBinaryOp::ADD;
	SljitNativeDoubleBinaryOp double_binary_op = SljitNativeDoubleBinaryOp::DIVIDE;
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeSignedIntegerWidth cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth coalesce_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeCoalesceRhsKind coalesce_rhs_kind = SljitNativeCoalesceRhsKind::CONSTANT;
	SljitNativeNullCheckOp null_check_op = SljitNativeNullCheckOp::IS_NULL;
	idx_t source_index = 0;
	idx_t right_source_index = 0;
	int64_t constant = 0;
	int64_t result_min = 0;
	int64_t result_max = 0;
	int64_t lower = 0;
	int64_t upper = 0;
	int64_t coalesce_constant = 0;
	double double_constant = 0;
	bool constant_on_left = false;
	bool check_result_range = false;
	bool list_has_null = false;
	bool not_in = false;
	bool lower_inclusive = true;
	bool upper_inclusive = true;
	bool not_between = false;
	bool try_cast = false;
	bool coalesce_constant_is_null = false;
	vector<int64_t> constants;
	unique_ptr<SljitNativePredicate> predicate;
	SljitNativeConstantOrNull constant_or_null;
};

struct SljitNativeExpressionBackendPlan : public JitBackendPlan {
	SljitNativeExpressionPlan plan;
};

bool TryGetNativeSignedIntegerWidth(const JitExpressionIR &node, SljitNativeSignedIntegerWidth &width);
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

bool TryReadNativeIntegerBinaryConstant(const JitExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                        SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                        bool &constant_on_left);
bool TryReadNativeIntegerBinaryReferences(const JitExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          SljitNativeIntegerKind &kind, idx_t &left_source_index,
                                          idx_t &right_source_index);
bool TryReadNativeDoubleBinaryConstant(const JitExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                       idx_t &source_index, double &constant_value, bool &constant_on_left);
bool TryReadNativeDoubleBinaryReferences(const JitExpressionIR &root, SljitNativeDoubleBinaryOp &native_op,
                                         idx_t &left_source_index, idx_t &right_source_index);
bool TryReadNativeDecimal64BinaryReferences(const JitExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                            idx_t &left_source_index, idx_t &right_source_index,
                                            int64_t &result_min, int64_t &result_max);
bool TryReadNativeDecimal64BinaryConstant(const JitExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          idx_t &source_index, int64_t &constant_value, bool &constant_on_left,
                                          int64_t &result_min, int64_t &result_max);
bool TryReadNativeIntegerCompareConstant(const JitExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                         SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                         bool &constant_on_left);
bool TryReadNativeIntegerCompareReferences(const JitExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                           SljitNativeIntegerKind &kind, idx_t &left_source_index,
                                           idx_t &right_source_index);
bool TryReadNativeIntegerCast(const JitExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                              SljitNativeSignedIntegerWidth &target_width, idx_t &source_index, bool &try_cast);
bool TryReadNativeSignedToUnsignedIntegerCast(const JitExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                                              SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                              bool &try_cast);
bool TryReadNativeIntegralCompress(const JitExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                                   SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                   int64_t &minimum);
bool TryReadNativeIntegralDecompress(const JitExpressionIR &root, SljitNativeUnsignedIntegerWidth &source_width,
                                     SljitNativeSignedIntegerWidth &target_width, idx_t &source_index,
                                     int64_t &minimum);
bool TryReadNativeIntegerCoalesce(const JitExpressionIR &root, SljitNativeSignedIntegerWidth &width,
                                  idx_t &source_index, SljitNativeCoalesceRhsKind &rhs_kind,
                                  idx_t &right_source_index, int64_t &constant_value, bool &constant_is_null);
bool TryReadNativeIntegerInList(const JitExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                vector<int64_t> &constants, bool &list_has_null, bool &not_in);
bool TryReadNativeIntegerBetween(const JitExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                 int64_t &lower, int64_t &upper, bool &lower_inclusive, bool &upper_inclusive,
                                 bool &not_between);
bool TryReadNativeStringPrefixConstant(const JitExpressionIR &root, idx_t &source_index, string &prefix);
bool TryReadNativeStringSubstringInListConstant(const JitExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, vector<string> &constants);
bool TryReadNativeNullCheck(const JitExpressionIR &root, SljitNativeNullCheckOp &op, idx_t &source_index);
bool TryReadNativeConstantOrNull(const JitExpressionIR &root, SljitNativeConstantOrNull &expr);
bool ShouldTryNativePredicateRoot(const JitExpressionIR &root);
bool TryBuildNativePredicate(const JitExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate);

string SljitNativeExpressionPlanReason(const SljitNativeExpressionPlan &plan);
string SljitNativeExpressionPlanIrPrefix(const SljitNativeExpressionPlan &plan);
bool TryPlanSljitNativeExpression(const JitExpressionFragment &fragment, SljitNativeExpressionPlan &plan);

} // namespace duckdb

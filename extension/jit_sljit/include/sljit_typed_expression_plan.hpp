//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/execution_expression_ir.hpp"

namespace duckdb {

struct SljitTypedExpressionTreeFastPathPlan {
	bool fast_path_supported = false;
	bool precheck_nulls_supported = false;
	bool hybrid_nulls_supported = false;
	vector<idx_t> source_refs;
};

struct SljitTypedExpressionTreePlan {
	bool supported = false;
	SljitNativeIntegerKind result_kind = SljitNativeIntegerKind::INT64;
	bool result_is_bool = false;
	bool result_is_int64 = false;
	idx_t node_count = 0;
	SljitTypedExpressionTreeFastPathPlan fast_path;
};

bool SljitExpressionTreeBinaryOpSupported(ExecutionExpressionBinaryOp op);
bool SljitTypedExpressionTreeComparisonSupported(ExecutionExpressionBinaryOp op);
bool SljitTypedExpressionTreeIsInt64Node(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsDecimal64Node(const ExecutionExpressionIR &node);
bool TryGetSljitTypedExpressionTreeDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max);
bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(const ExecutionExpressionIR &node);
//! A narrow DECIMAL128 node is a DECIMAL node with physical INT128 whose magnitude is structurally bounded by
//! INT64_MAX, so it is computed exactly in one 64-bit register (INT64 register semantics) and only sign-extended where
//! the value leaves the tree. On success, `bound` is a conservative bound on |value|.
bool TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(const ExecutionExpressionIR &node, uint64_t &bound);
bool SljitTypedExpressionTreeIsNarrowDecimal128Node(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node);
//! A signed integer column narrower than the tree's arithmetic: read only through a cast that widens it
bool SljitTypedExpressionTreeIsNarrowSignedReference(const ExecutionExpressionIR &node);
//! A cast widening a narrow signed integer column to INT32 or INT64: a sign-extending load of the column
bool SljitTypedExpressionTreeIsNarrowSourceWideningCast(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right);
bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right);
bool TryReadSljitTypedExpressionTreeStringPrefixConstant(const ExecutionExpressionIR &node, idx_t &source_index,
                                                         string &prefix);
bool TryReadSljitTypedExpressionTreeStringCompareConstant(const ExecutionExpressionIR &node, idx_t &source_index,
                                                          string &constant, bool &compare_equal);
bool SljitTypedExpressionTreeIsSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeInt64CastSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeValueCastSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeConstantDivisorReductionSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeFastPathSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeCanPrecheckNulls(const ExecutionExpressionIR &node);
//! A NULL in any source the node reads makes it NULL
bool SljitTypedExpressionTreeIsNullStrict(const ExecutionExpressionIR &node);
//! Whether the expression can run on whatever bits its sources hold, such as the leftover value of a NULL row: it reads
//! no string and neither divides nor checks for overflow, so it never faults or raises an error
bool SljitTypedExpressionTreeEvaluatesAnyBits(const ExecutionExpressionIR &node);
//! The predicate's leaves are null-strict and meet only at AND and OR, which are monotone: a leaf with a NULL source
//! can be decided as FALSE without changing which rows the predicate accepts (a NULL that a TRUE row reads is outvoted
//! for every value it could take)
bool SljitTypedExpressionTreeNullDecidesFalse(const ExecutionExpressionIR &predicate);
SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node);
SljitNativeIntegerCompareOp SljitTypedExpressionTreeCompareOp(ExecutionExpressionBinaryOp op);
idx_t CountSljitTypedExpressionTreeNodes(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeSourceKnownValid(const vector<idx_t> *known_valid_sources, idx_t source_index);
void AddSljitTypedKnownValidSource(vector<idx_t> &known_valid_sources, idx_t source_index);
void CollectSljitTypedExpressionTreeReferences(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources);
SljitTypedExpressionTreeFastPathPlan BuildSljitTypedExpressionTreeFastPathPlan(const ExecutionExpressionIR &root,
                                                                               bool emit_flat_nullable_fast_path);
void CollectSljitTypedExpressionTreeTrueFacts(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources);
void CollectSljitTypedExpressionTreeNotTrueFacts(const ExecutionExpressionIR &node, vector<idx_t> &known_valid_sources);
bool TryGetSljitTypedExpressionTreeResultKind(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind);
SljitTypedExpressionTreePlan BuildSljitTypedExpressionTreePlan(const ExecutionExpressionIR &root,
                                                               bool emit_flat_nullable_fast_path);

} // namespace duckdb

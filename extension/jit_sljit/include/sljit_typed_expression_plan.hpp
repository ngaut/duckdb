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
bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right);
bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right);
bool SljitTypedExpressionTreeIsSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeInt64CastSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeFastPathSupported(const ExecutionExpressionIR &node);
bool SljitTypedExpressionTreeCanPrecheckNulls(const ExecutionExpressionIR &node);
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

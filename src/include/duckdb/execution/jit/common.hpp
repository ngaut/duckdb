//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/common.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class JitCompileTarget : uint8_t { REGION };
enum class JitCompileStatus : uint8_t { COMPILED, SKIPPED, UNSUPPORTED, UNAVAILABLE, DISABLED, ERROR };
enum class JitExecutionMode : uint8_t { NONE, NATIVE, EXECUTOR_FALLBACK, UNSUPPORTED };
enum class JitRegionExecutionForm : uint8_t { NONE, FUSED };
enum class JitLoweringKind : uint8_t { NATIVE, FALLBACK };
enum class JitRegionIRNodeKind : uint8_t { SOURCE, FILTER, PROJECTION, OPERATOR, SINK };
enum class JitRegionABI : uint8_t { NONE, CHUNK_TRANSFORM, SOURCE_PREFIX, SINK_SUFFIX, FULL_PIPELINE, STATE_SCAN };
enum class JitRegionCandidateScope : uint8_t {
	POST_SOURCE_OPERATOR_INTERVAL,
	SOURCE_PREFIX,
	SINK_PIPELINE,
	FULL_PIPELINE
};
enum class JitRegionSourceKind : uint8_t {
	NONE,
	DUCKDB_TABLE_SCAN,
	TABLE_FUNCTION_SCAN,
	GENERIC_SCAN,
	STATEFUL_OPERATOR
};
enum class JitRegionSourceExecutionKind : uint8_t { NONE, DUCKDB_SOURCE_BOUNDARY, EXECUTOR_FALLBACK, NATIVE_SOURCE };
enum class JitRegionNativeSourceStatus : uint8_t { NONE, READY, BLOCKED };
enum class JitRegionStateContractStatus : uint8_t { NONE, READY, MISSING, BLOCKED };
enum class JitRegionOperatorKind : uint8_t { NONE, HASH_JOIN_PROBE, OPERATOR };
enum class JitRegionSinkKind : uint8_t {
	NONE,
	HASH_JOIN_BUILD,
	HASH_AGGREGATE_UPDATE,
	PERFECT_HASH_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	SORT,
	MATERIALIZATION,
	OPERATOR
};
enum class JitRegionAggregateOperatorKind : uint8_t { NONE, HASH, PERFECT_HASH, UNGROUPED };
enum class JitAggregateUpdateKind : uint8_t { NONE, COUNT_STAR, COUNT, SUM };
enum class JitRegionVectorFormatKind : uint8_t {
	NONE,
	DATA_CHUNK,
	UNIFIED_VECTOR,
	FLAT_VECTOR,
	SELECTION_VECTOR,
	EXECUTOR_BOUNDARY
};
enum class JitRegionVectorSourceKind : uint8_t { NONE, REGION_INPUT, OPERATOR_OUTPUT, EXECUTOR_BOUNDARY };
enum class JitRegionSelectionSourceKind : uint8_t { NONE, INPUT_SELECTION, FILTER_SELECTION, EXECUTOR_BOUNDARY };
enum class JitRegionBoundaryKind : uint8_t {
	NONE,
	SCAN,
	SOURCE_NATIVE,
	SINK,
	SINK_NATIVE,
	OPERATOR_NATIVE,
	OPERATOR_PROTOCOL_BOUNDARY,
	OPERATOR_FALLBACK,
	EXPRESSION_FALLBACK
};
enum class JitRegionOwnershipKind : uint8_t {
	NONE,
	GENERATED_IR,
	NATIVE_PROTOCOL,
	SOURCE_BOUNDARY,
	EXECUTOR_BOUNDARY,
	MISSING_PROTOCOL
};
enum class JitRegionStageKind : uint8_t {
	SOURCE,
	SOURCE_FILTER,
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
	HASH_JOIN_BUILD,
	HASH_AGGREGATE_UPDATE,
	PERFECT_HASH_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	SINK_BOUNDARY,
	OPERATOR_BOUNDARY
};
enum class JitRegionStageExecutionKind : uint8_t {
	NONE,
	GENERATED_IR,
	NATIVE_PROTOCOL,
	SOURCE_BOUNDARY,
	EXECUTOR_FALLBACK,
	MISSING_PROTOCOL
};
enum class JitPolicyMode : uint8_t { AUTO, FORCE, OFF };
enum class JitFullPipelineResult : uint8_t { NOT_FINISHED, FINISHED, INTERRUPTED };
enum class JitExpressionValidityKind : uint8_t {
	UNKNOWN,
	CONSTANT_NULL,
	CONSTANT_VALID,
	SOURCE,
	NOT_NULL,
	CHILD,
	CHILDREN_NULL_PROPAGATING,
	CHILD_OR_CAST_FAILURE,
	THREE_VALUED_BOOLEAN,
	FIRST_VALID_CHILD,
	CONSTANT_PLUS_NULL_GUARDS,
	SQL_IN_LIST,
	SQL_BETWEEN,
	SELECTED_BRANCH
};
enum class JitExpressionSourceKind : uint8_t { UNKNOWN, CONSTANT, VECTOR, DERIVED };
enum class JitExpressionExceptionKind : uint8_t { UNKNOWN, NONE, CAST, NULL_ON_CAST_ERROR, ARITHMETIC };
enum class JitExpressionIRKind : uint8_t {
	CONSTANT,
	REFERENCE,
	UNARY,
	BINARY,
	CAST,
	CONJUNCTION,
	COALESCE,
	CONSTANT_OR_NULL,
	IN_LIST,
	BETWEEN,
	CASE,
	INTRINSIC
};
enum class JitExpressionIntrinsicKind : uint8_t {
	NONE,
	STRING_COMPRESS,
	STRING_PREFIX,
	STRING_SUFFIX,
	STRING_CONTAINS,
	STRING_LIKE,
	STRING_SUBSTRING,
	INTEGRAL_COMPRESS,
	INTEGRAL_DECOMPRESS,
	DATE_YEAR
};
enum class JitExpressionUnaryOp : uint8_t { NOT, IS_NULL, IS_NOT_NULL, NEGATE };
enum class JitExpressionBinaryOp : uint8_t {
	ADD,
	SUBTRACT,
	MULTIPLY,
	DIVIDE,
	INTEGER_DIVIDE,
	MODULO,
	COMPARE_EQUAL,
	COMPARE_NOTEQUAL,
	COMPARE_LESSTHAN,
	COMPARE_GREATERTHAN,
	COMPARE_LESSTHANOREQUALTO,
	COMPARE_GREATERTHANOREQUALTO,
	COMPARE_DISTINCT_FROM,
	COMPARE_NOT_DISTINCT_FROM
};
enum class JitExpressionConjunctionOp : uint8_t { AND, OR };

DUCKDB_API const char *JitCompileTargetToString(JitCompileTarget target);
DUCKDB_API const char *JitCompileStatusToString(JitCompileStatus status);
DUCKDB_API const char *JitExecutionModeToString(JitExecutionMode mode);
DUCKDB_API const char *JitRegionExecutionFormToString(JitRegionExecutionForm form);
DUCKDB_API const char *JitLoweringKindToString(JitLoweringKind kind);
DUCKDB_API const char *JitRegionIRNodeKindToString(JitRegionIRNodeKind kind);
DUCKDB_API const char *JitRegionABIToString(JitRegionABI abi);
DUCKDB_API bool JitRegionABIIsChunkTransform(JitRegionABI abi);
DUCKDB_API bool JitRegionABIIsSourcePrefix(JitRegionABI abi);
DUCKDB_API bool JitRegionABIIsSinkPipeline(JitRegionABI abi);
DUCKDB_API bool JitRegionABIIsFullPipeline(JitRegionABI abi);
DUCKDB_API bool JitRegionABIOwnsSource(JitRegionABI abi);
DUCKDB_API bool JitRegionABIOwnsSink(JitRegionABI abi);
DUCKDB_API const char *JitRegionCandidateScopeToString(JitRegionCandidateScope scope);
DUCKDB_API const char *JitRegionSourceKindToString(JitRegionSourceKind kind);
DUCKDB_API const char *JitRegionSourceExecutionKindToString(JitRegionSourceExecutionKind kind);
DUCKDB_API const char *JitRegionNativeSourceStatusToString(JitRegionNativeSourceStatus status);
DUCKDB_API const char *JitRegionStateContractStatusToString(JitRegionStateContractStatus status);
DUCKDB_API const char *JitRegionOperatorKindToString(JitRegionOperatorKind kind);
DUCKDB_API const char *JitRegionSinkKindToString(JitRegionSinkKind kind);
DUCKDB_API const char *JitRegionAggregateOperatorKindToString(JitRegionAggregateOperatorKind kind);
DUCKDB_API const char *JitAggregateUpdateKindToString(JitAggregateUpdateKind kind);
DUCKDB_API const char *JitRegionVectorFormatKindToString(JitRegionVectorFormatKind kind);
DUCKDB_API const char *JitRegionVectorSourceKindToString(JitRegionVectorSourceKind kind);
DUCKDB_API const char *JitRegionSelectionSourceKindToString(JitRegionSelectionSourceKind kind);
DUCKDB_API const char *JitRegionBoundaryKindToString(JitRegionBoundaryKind kind);
DUCKDB_API const char *JitRegionOwnershipKindToString(JitRegionOwnershipKind kind);
DUCKDB_API const char *JitRegionStageKindToString(JitRegionStageKind kind);
DUCKDB_API const char *JitRegionStageExecutionKindToString(JitRegionStageExecutionKind kind);
DUCKDB_API const char *JitPolicyModeToString(JitPolicyMode mode);
DUCKDB_API const char *JitExpressionValidityKindToString(JitExpressionValidityKind kind);
DUCKDB_API const char *JitExpressionSourceKindToString(JitExpressionSourceKind kind);
DUCKDB_API const char *JitExpressionExceptionKindToString(JitExpressionExceptionKind kind);
DUCKDB_API const char *JitExpressionIRKindToString(JitExpressionIRKind kind);
DUCKDB_API const char *JitExpressionIntrinsicKindToString(JitExpressionIntrinsicKind kind);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_common.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class ExecutionRegionCompileStatus : uint8_t { COMPILED, SKIPPED, UNSUPPORTED, UNAVAILABLE, DISABLED, ERROR };
enum class ExecutionRegionExecutionMode : uint8_t { NONE, NATIVE, VECTORIZED, UNSUPPORTED };
enum class ExecutionRegionLoweringKind : uint8_t { NATIVE, BOUNDARY };
enum class ExecutionRegionOperatorKind : uint8_t {
	GENERIC,
	TABLE_SCAN,
	SCAN_SOURCE,
	FILTER,
	PROJECTION,
	HASH_JOIN,
	NESTED_LOOP_JOIN,
	UNGROUPED_AGGREGATE,
	HASH_GROUP_BY,
	PERFECT_HASH_GROUP_BY,
	ORDER_BY,
	TOP_N,
	CTE,
	RESULT_COLLECTOR,
	EXPLAIN_ANALYZE,
	CREATE_TABLE_AS
};
enum class ExecutionRegionNodeKind : uint8_t { SOURCE, FILTER, PROJECTION, OPERATOR, SINK };
enum class ExecutionRegionABI : uint8_t { NONE, FULL_PIPELINE };
enum class ExecutionRegionSourceKind : uint8_t {
	NONE,
	DUCKDB_TABLE_SCAN,
	TABLE_FUNCTION_SCAN,
	GENERIC_SCAN,
	STATEFUL_OPERATOR
};
enum class ExecutionRegionSourceExecutionKind : uint8_t { NONE, DUCKDB_SOURCE_BOUNDARY, SOURCE_CONTRACT };
enum class ExecutionRegionSourceContractStatus : uint8_t { NONE, READY, BLOCKED };
enum class ExecutionRegionStateContractStatus : uint8_t { NONE, READY, MISSING, BLOCKED };
enum class ExecutionRegionOperatorContractKind : uint8_t { NONE, HASH_JOIN_PROBE, NESTED_LOOP_JOIN_PROBE };
enum class ExecutionRegionJoinType : uint8_t {
	INVALID,
	LEFT,
	RIGHT,
	INNER,
	OUTER,
	SEMI,
	ANTI,
	MARK,
	RIGHT_SEMI,
	RIGHT_ANTI
};
enum class ExecutionRegionComparisonType : uint8_t {
	INVALID,
	EQUAL,
	NOT_EQUAL,
	LESS_THAN,
	GREATER_THAN,
	LESS_THAN_OR_EQUAL,
	GREATER_THAN_OR_EQUAL,
	DISTINCT_FROM,
	NOT_DISTINCT_FROM
};
enum class ExecutionRegionSinkKind : uint8_t {
	NONE,
	HASH_JOIN_BUILD,
	NESTED_LOOP_JOIN_BUILD,
	HASH_AGGREGATE_UPDATE,
	HASH_AGGREGATE_DISTINCT_SINK,
	PERFECT_HASH_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	RESULT_COLLECTOR_SINK,
	SORT,
	MATERIALIZATION,
	DELIM_JOIN_SINK
};
enum class ExecutionRegionAggregateOperatorKind : uint8_t { NONE, HASH, PERFECT_HASH, UNGROUPED };
enum class ExecutionRegionVectorFormatKind : uint8_t {
	NONE,
	DATA_CHUNK,
	UNIFIED_VECTOR,
	FLAT_VECTOR,
	SELECTION_VECTOR,
	BOUNDARY
};
enum class ExecutionRegionVectorSourceKind : uint8_t { NONE, REGION_INPUT, OPERATOR_OUTPUT, BOUNDARY };
enum class ExecutionRegionSelectionSourceKind : uint8_t { NONE, INPUT_SELECTION, FILTER_SELECTION, BOUNDARY };
enum class ExecutionRegionBoundaryKind : uint8_t {
	NONE,
	SCAN,
	SOURCE_CONTRACT,
	SINK,
	SINK_NATIVE,
	OPERATOR_NATIVE,
	OPERATOR_CONTRACT_BOUNDARY,
	OPERATOR_MISSING,
	EXPRESSION_MISSING
};
enum class ExecutionRegionOwnershipKind : uint8_t {
	NONE,
	GENERATED_IR,
	NATIVE_CONTRACT,
	SOURCE_BOUNDARY,
	MISSING_CONTRACT
};
enum class ExecutionRegionStageKind : uint8_t {
	SOURCE,
	SOURCE_FILTER,
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
	HASH_JOIN_BUILD,
	NESTED_LOOP_JOIN_PROBE,
	NESTED_LOOP_JOIN_BUILD,
	HASH_AGGREGATE_UPDATE,
	HASH_AGGREGATE_DISTINCT_SINK,
	PERFECT_HASH_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	APPEND_SINK,
	SORT_SINK,
	DELIM_JOIN_SINK,
	SINK_BOUNDARY,
	OPERATOR_BOUNDARY
};
enum class ExecutionRegionStageExecutionKind : uint8_t {
	NONE,
	GENERATED_IR,
	NATIVE_CONTRACT,
	SOURCE_BOUNDARY,
	MISSING_CONTRACT
};
enum class ExecutionRunnerKind : uint8_t { VECTORIZED, COMPILED_VECTORIZED };
enum class ExecutionRegionPolicyMode : uint8_t { AUTO, OFF };
enum class ExecutionRegionResult : uint8_t { NOT_FINISHED, FINISHED, INTERRUPTED, DEFERRED };
enum class ExecutionExpressionValidityKind : uint8_t {
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
enum class ExecutionExpressionSourceKind : uint8_t { UNKNOWN, CONSTANT, VECTOR, DERIVED };
enum class ExecutionExpressionExceptionKind : uint8_t { UNKNOWN, NONE, CAST, NULL_ON_CAST_ERROR, ARITHMETIC, ERROR };
enum class ExecutionExpressionIRKind : uint8_t {
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
enum class ExecutionExpressionIntrinsicKind : uint8_t {
	NONE,
	STRING_COMPRESS,
	STRING_DECOMPRESS,
	STRING_PREFIX,
	STRING_SUFFIX,
	STRING_CONTAINS,
	STRING_LIKE,
	STRING_SUBSTRING,
	INTEGRAL_COMPRESS,
	INTEGRAL_DECOMPRESS,
	DATE_YEAR,
	ERROR
};
enum class ExecutionExpressionUnaryOp : uint8_t { NOT, IS_NULL, IS_NOT_NULL, NEGATE };
enum class ExecutionExpressionBinaryOp : uint8_t {
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
enum class ExecutionExpressionConjunctionOp : uint8_t { AND, OR };

struct ExecutionRegionStageId {
	idx_t key = 0;
	string name;

	ExecutionRegionStageId();
	ExecutionRegionStageId(const char *name_p);
	ExecutionRegionStageId(string name_p);

	bool IsValid() const;
};

struct ExecutionRegionRecordedStageRuntime {
	ExecutionRegionStageId stage;
	int64_t runtime_time_us = 0;
	idx_t count = 0;
};

struct ExecutionRegionOpenRequest {
	bool present = false;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	bool uses_scan_filters = false;

	bool UsesSourceContract() const {
		return source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}

	bool UsesScanFilters() const {
		return uses_scan_filters;
	}
};

DUCKDB_API const char *ExecutionRegionCompileStatusToString(ExecutionRegionCompileStatus status);
DUCKDB_API const char *ExecutionRegionExecutionModeToString(ExecutionRegionExecutionMode mode);
DUCKDB_API bool ExecutionRegionExecutionModeIsCompiled(ExecutionRegionExecutionMode mode);
DUCKDB_API const char *ExecutionRegionLoweringKindToString(ExecutionRegionLoweringKind kind);
DUCKDB_API const char *ExecutionRegionOperatorKindToString(ExecutionRegionOperatorKind kind);
DUCKDB_API const char *ExecutionRegionOperatorKindToTraceLabel(ExecutionRegionOperatorKind kind);
DUCKDB_API const char *ExecutionRegionNodeKindToString(ExecutionRegionNodeKind kind);
DUCKDB_API const char *ExecutionRegionABIToString(ExecutionRegionABI abi);
DUCKDB_API bool ExecutionRegionABIIsFullPipeline(ExecutionRegionABI abi);
DUCKDB_API bool ExecutionRegionABIOwnsSource(ExecutionRegionABI abi);
DUCKDB_API bool ExecutionRegionABIOwnsSink(ExecutionRegionABI abi);
DUCKDB_API const char *ExecutionRegionSourceKindToString(ExecutionRegionSourceKind kind);
DUCKDB_API const char *ExecutionRegionSourceExecutionKindToString(ExecutionRegionSourceExecutionKind kind);
DUCKDB_API const char *ExecutionRegionSourceContractStatusToString(ExecutionRegionSourceContractStatus status);
DUCKDB_API const char *ExecutionRegionStateContractStatusToString(ExecutionRegionStateContractStatus status);
DUCKDB_API const char *ExecutionRegionOperatorContractKindToString(ExecutionRegionOperatorContractKind kind);
DUCKDB_API const char *ExecutionRegionJoinTypeToString(ExecutionRegionJoinType type);
DUCKDB_API bool ExecutionRegionJoinTypePropagatesBuildSide(ExecutionRegionJoinType type);
DUCKDB_API const char *ExecutionRegionComparisonTypeToString(ExecutionRegionComparisonType type);
DUCKDB_API const char *ExecutionRegionSinkKindToString(ExecutionRegionSinkKind kind);
DUCKDB_API const char *ExecutionRegionAggregateOperatorKindToString(ExecutionRegionAggregateOperatorKind kind);
DUCKDB_API const char *ExecutionRegionVectorFormatKindToString(ExecutionRegionVectorFormatKind kind);
DUCKDB_API const char *ExecutionRegionVectorSourceKindToString(ExecutionRegionVectorSourceKind kind);
DUCKDB_API const char *ExecutionRegionSelectionSourceKindToString(ExecutionRegionSelectionSourceKind kind);
DUCKDB_API const char *ExecutionRegionBoundaryKindToString(ExecutionRegionBoundaryKind kind);
DUCKDB_API const char *ExecutionRegionOwnershipKindToString(ExecutionRegionOwnershipKind kind);
DUCKDB_API const char *ExecutionRegionStageKindToString(ExecutionRegionStageKind kind);
DUCKDB_API const char *ExecutionRegionStageExecutionKindToString(ExecutionRegionStageExecutionKind kind);
DUCKDB_API const char *ExecutionRunnerKindToString(ExecutionRunnerKind kind);
DUCKDB_API const char *ExecutionRegionPolicyModeToString(ExecutionRegionPolicyMode mode);
DUCKDB_API const char *ExecutionExpressionValidityKindToString(ExecutionExpressionValidityKind kind);
DUCKDB_API const char *ExecutionExpressionSourceKindToString(ExecutionExpressionSourceKind kind);
DUCKDB_API const char *ExecutionExpressionExceptionKindToString(ExecutionExpressionExceptionKind kind);
DUCKDB_API const char *ExecutionExpressionIRKindToString(ExecutionExpressionIRKind kind);
DUCKDB_API const char *ExecutionExpressionIntrinsicKindToString(ExecutionExpressionIntrinsicKind kind);

} // namespace duckdb

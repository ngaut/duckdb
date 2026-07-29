//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_common.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

class TableFilterKernelProvider;

//! Stable query-local identity shared by a runtime filter and the operator state that owns it.
//! Identity equality is the proof that both sides describe the same finalized membership set.
struct ExecutionRuntimeFilterIdentity {};

enum class ExecutionRegionCompileStatus : uint8_t { COMPILED, SKIPPED, UNSUPPORTED, UNAVAILABLE, DISABLED, ERROR };
enum class ExecutionRegionExecutionMode : uint8_t { NONE, NATIVE, GPU, VECTORIZED, UNSUPPORTED };
enum class ExecutionRegionLoweringKind : uint8_t { NATIVE, BOUNDARY };

static constexpr const char *EXECUTION_REGION_BLOCKER_DUCKDB_SELECTED_VECTORIZED = "duckdb_selected_vectorized";
static constexpr const char *EXECUTION_REGION_BLOCKER_NOT_FULLY_FUSED = "region_not_fully_fused";
static constexpr const char *EXECUTION_REGION_BLOCKER_NO_EXECUTION_REGION_CANDIDATES = "no_execution_region_candidates";
static constexpr const char *EXECUTION_REGION_BLOCKER_NO_EXECUTABLE_REGION_WORK = "no_executable_region_work";
static constexpr const char *EXECUTION_REGION_BLOCKER_UNSUPPORTED_REGION_EXECUTION = "unsupported_region_execution";
static constexpr const char *EXECUTION_REGION_BLOCKER_REGION_CONTAINS_NO_NATIVE_NODES =
    "region_contains_no_native_nodes";
static constexpr idx_t EXECUTION_REGION_LOW_CARDINALITY_STRING_SEARCH_LIMIT = 64;

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
	CREATE_TABLE_AS,
	COUNT
};

static constexpr idx_t EXECUTION_REGION_OPERATOR_KIND_COUNT = static_cast<idx_t>(ExecutionRegionOperatorKind::COUNT);
using ExecutionRegionOperatorKindCounts = array<idx_t, EXECUTION_REGION_OPERATOR_KIND_COUNT>;
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
enum class ExecutionRegionScanFilterMode : uint8_t {
	NONE,
	ALL,
	STATIC_PRUNING_ONLY,
	DYNAMIC_FILTERS_WITH_STATIC_PRUNING
};
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
	BOUNDARY,
	COUNT
};
static constexpr idx_t EXECUTION_REGION_VECTOR_FORMAT_KIND_COUNT =
    static_cast<idx_t>(ExecutionRegionVectorFormatKind::COUNT);
using ExecutionRegionVectorFormatKindCounts = array<idx_t, EXECUTION_REGION_VECTOR_FORMAT_KIND_COUNT>;
enum class ExecutionRegionVectorSourceKind : uint8_t { NONE, REGION_INPUT, OPERATOR_OUTPUT, BOUNDARY, COUNT };
static constexpr idx_t EXECUTION_REGION_VECTOR_SOURCE_KIND_COUNT =
    static_cast<idx_t>(ExecutionRegionVectorSourceKind::COUNT);
using ExecutionRegionVectorSourceKindCounts = array<idx_t, EXECUTION_REGION_VECTOR_SOURCE_KIND_COUNT>;
enum class ExecutionRegionSelectionSourceKind : uint8_t { NONE, INPUT_SELECTION, FILTER_SELECTION, BOUNDARY, COUNT };
static constexpr idx_t EXECUTION_REGION_SELECTION_SOURCE_KIND_COUNT =
    static_cast<idx_t>(ExecutionRegionSelectionSourceKind::COUNT);
using ExecutionRegionSelectionSourceKindCounts = array<idx_t, EXECUTION_REGION_SELECTION_SOURCE_KIND_COUNT>;
enum class ExecutionRegionCapabilityTypeKind : uint8_t {
	UNKNOWN,
	BOOL,
	INT8,
	INT16,
	INT32,
	INT64,
	INT128,
	UINT8,
	UINT16,
	UINT32,
	UINT64,
	UINT128,
	FLOAT,
	DOUBLE,
	DECIMAL16,
	DECIMAL32,
	DECIMAL64,
	DECIMAL128,
	DATE,
	VARCHAR,
	POINTER,
	OTHER,
	COUNT
};
static constexpr idx_t EXECUTION_REGION_CAPABILITY_TYPE_KIND_COUNT =
    static_cast<idx_t>(ExecutionRegionCapabilityTypeKind::COUNT);
using ExecutionRegionCapabilityTypeKindCounts = array<idx_t, EXECUTION_REGION_CAPABILITY_TYPE_KIND_COUNT>;
enum class ExecutionRegionCapabilityValidityKind : uint8_t { UNKNOWN, NOT_NULL, MAY_HAVE_NULL, COUNT };
static constexpr idx_t EXECUTION_REGION_CAPABILITY_VALIDITY_KIND_COUNT =
    static_cast<idx_t>(ExecutionRegionCapabilityValidityKind::COUNT);
using ExecutionRegionCapabilityValidityKindCounts = array<idx_t, EXECUTION_REGION_CAPABILITY_VALIDITY_KIND_COUNT>;
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
enum class ExecutionRunnerKind : uint8_t { VECTORIZED, COMPILED_VECTORIZED, COMPILED_GPU };
enum class ExecutionRegionPolicyMode : uint8_t { AUTO, OFF };
enum class ExecutionRegionResult : uint8_t { NOT_FINISHED, FINISHED, INTERRUPTED, DEFERRED };
enum class ExecutionRegionJitRuntimeProof : uint8_t {
	GENERATED_STAGE_WORK,
	GENERATED_BACKEND_WORK,
	MATERIALIZATION_ELISION,
	FULL_PIPELINE_OWNERSHIP,
	DELEGATED_RUNTIME_WORK,
	NO_WORK,
	COUNT
};
using ExecutionRegionJitRuntimeProofMask = uint32_t;

static constexpr ExecutionRegionJitRuntimeProofMask
ExecutionRegionJitRuntimeProofBit(ExecutionRegionJitRuntimeProof proof) {
	return ExecutionRegionJitRuntimeProofMask(1) << static_cast<uint8_t>(proof);
}

static constexpr bool ExecutionRegionJitRuntimeProofRequired(ExecutionRegionJitRuntimeProofMask requirements,
                                                             ExecutionRegionJitRuntimeProof proof) {
	return (requirements & ExecutionRegionJitRuntimeProofBit(proof)) != 0;
}
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

struct ExecutionRegionRecordedCounter {
	ExecutionRegionStageId counter;
	idx_t count = 0;
};

struct ExecutionRegionOpenRequest {
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionScanFilterMode scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
	vector<LogicalType> source_contract_input_types;
	optional_ptr<const TableFilterKernelProvider> table_filter_kernel_provider;

	bool UsesSourceContract() const {
		return source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}

	bool UsesScanFilters() const {
		return scan_filter_mode != ExecutionRegionScanFilterMode::NONE;
	}

	bool UsesStaticPruningOnly() const {
		return scan_filter_mode == ExecutionRegionScanFilterMode::STATIC_PRUNING_ONLY;
	}

	bool UsesDynamicFiltersWithStaticPruning() const {
		return scan_filter_mode == ExecutionRegionScanFilterMode::DYNAMIC_FILTERS_WITH_STATIC_PRUNING;
	}

	bool UsesSourceContractInputLayout() const {
		return UsesSourceContract() && !source_contract_input_types.empty();
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
DUCKDB_API const char *ExecutionRegionCapabilityTypeKindToString(ExecutionRegionCapabilityTypeKind kind);
DUCKDB_API const char *ExecutionRegionCapabilityValidityKindToString(ExecutionRegionCapabilityValidityKind kind);
DUCKDB_API const char *ExecutionRegionBoundaryKindToString(ExecutionRegionBoundaryKind kind);
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

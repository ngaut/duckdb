#include "duckdb/execution/execution_region_telemetry.hpp"

#include "duckdb/common/types.hpp"

namespace duckdb {

const char *ExecutionRegionCompileStatusToString(ExecutionRegionCompileStatus status) {
	switch (status) {
	case ExecutionRegionCompileStatus::COMPILED:
		return "compiled";
	case ExecutionRegionCompileStatus::SKIPPED:
		return "skipped";
	case ExecutionRegionCompileStatus::UNSUPPORTED:
		return "unsupported";
	case ExecutionRegionCompileStatus::UNAVAILABLE:
		return "unavailable";
	case ExecutionRegionCompileStatus::DISABLED:
		return "disabled";
	case ExecutionRegionCompileStatus::ERROR:
		return "error";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionExecutionModeToString(ExecutionRegionExecutionMode mode) {
	switch (mode) {
	case ExecutionRegionExecutionMode::NONE:
		return "none";
	case ExecutionRegionExecutionMode::NATIVE:
		return "native";
	case ExecutionRegionExecutionMode::GPU:
		return "gpu";
	case ExecutionRegionExecutionMode::VECTORIZED:
		return "vectorized";
	case ExecutionRegionExecutionMode::UNSUPPORTED:
		return "unsupported";
	default:
		return "unknown";
	}
}

const char *ExecutionRunnerKindToString(ExecutionRunnerKind kind) {
	switch (kind) {
	case ExecutionRunnerKind::VECTORIZED:
		return "vectorized";
	case ExecutionRunnerKind::COMPILED_VECTORIZED:
		return "compiled_vectorized";
	case ExecutionRunnerKind::COMPILED_GPU:
		return "compiled_gpu";
	default:
		return "unknown";
	}
}

bool ExecutionRegionExecutionModeIsCompiled(ExecutionRegionExecutionMode mode) {
	return mode == ExecutionRegionExecutionMode::NATIVE || mode == ExecutionRegionExecutionMode::GPU;
}

const char *ExecutionRegionLoweringKindToString(ExecutionRegionLoweringKind kind) {
	switch (kind) {
	case ExecutionRegionLoweringKind::NATIVE:
		return "native";
	case ExecutionRegionLoweringKind::BOUNDARY:
		return "boundary";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionOperatorKindToString(ExecutionRegionOperatorKind kind) {
	switch (kind) {
	case ExecutionRegionOperatorKind::GENERIC:
		return "generic";
	case ExecutionRegionOperatorKind::TABLE_SCAN:
		return "table-scan";
	case ExecutionRegionOperatorKind::SCAN_SOURCE:
		return "scan-source";
	case ExecutionRegionOperatorKind::FILTER:
		return "filter";
	case ExecutionRegionOperatorKind::PROJECTION:
		return "projection";
	case ExecutionRegionOperatorKind::HASH_JOIN:
		return "hash-join";
	case ExecutionRegionOperatorKind::NESTED_LOOP_JOIN:
		return "nested-loop-join";
	case ExecutionRegionOperatorKind::UNGROUPED_AGGREGATE:
		return "ungrouped-aggregate";
	case ExecutionRegionOperatorKind::HASH_GROUP_BY:
		return "hash-group-by";
	case ExecutionRegionOperatorKind::PERFECT_HASH_GROUP_BY:
		return "perfect-hash-group-by";
	case ExecutionRegionOperatorKind::ORDER_BY:
		return "order-by";
	case ExecutionRegionOperatorKind::TOP_N:
		return "top-n";
	case ExecutionRegionOperatorKind::CTE:
		return "cte";
	case ExecutionRegionOperatorKind::RESULT_COLLECTOR:
		return "result-collector";
	case ExecutionRegionOperatorKind::EXPLAIN_ANALYZE:
		return "explain-analyze";
	case ExecutionRegionOperatorKind::CREATE_TABLE_AS:
		return "create-table-as";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionOperatorKindToTraceLabel(ExecutionRegionOperatorKind kind) {
	switch (kind) {
	case ExecutionRegionOperatorKind::GENERIC:
		return "GENERIC";
	case ExecutionRegionOperatorKind::TABLE_SCAN:
		return "TABLE_SCAN";
	case ExecutionRegionOperatorKind::SCAN_SOURCE:
		return "SCAN_SOURCE";
	case ExecutionRegionOperatorKind::FILTER:
		return "FILTER";
	case ExecutionRegionOperatorKind::PROJECTION:
		return "PROJECTION";
	case ExecutionRegionOperatorKind::HASH_JOIN:
		return "HASH_JOIN";
	case ExecutionRegionOperatorKind::NESTED_LOOP_JOIN:
		return "NESTED_LOOP_JOIN";
	case ExecutionRegionOperatorKind::UNGROUPED_AGGREGATE:
		return "UNGROUPED_AGGREGATE";
	case ExecutionRegionOperatorKind::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case ExecutionRegionOperatorKind::PERFECT_HASH_GROUP_BY:
		return "PERFECT_HASH_GROUP_BY";
	case ExecutionRegionOperatorKind::ORDER_BY:
		return "ORDER_BY";
	case ExecutionRegionOperatorKind::TOP_N:
		return "TOP_N";
	case ExecutionRegionOperatorKind::CTE:
		return "CTE";
	case ExecutionRegionOperatorKind::RESULT_COLLECTOR:
		return "RESULT_COLLECTOR";
	case ExecutionRegionOperatorKind::EXPLAIN_ANALYZE:
		return "EXPLAIN_ANALYZE";
	case ExecutionRegionOperatorKind::CREATE_TABLE_AS:
		return "CREATE_TABLE_AS";
	default:
		return "UNKNOWN";
	}
}

const char *ExecutionRegionNodeKindToString(ExecutionRegionNodeKind kind) {
	switch (kind) {
	case ExecutionRegionNodeKind::SOURCE:
		return "source";
	case ExecutionRegionNodeKind::FILTER:
		return "filter";
	case ExecutionRegionNodeKind::PROJECTION:
		return "projection";
	case ExecutionRegionNodeKind::OPERATOR:
		return "operator";
	case ExecutionRegionNodeKind::SINK:
		return "sink";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionABIToString(ExecutionRegionABI abi) {
	switch (abi) {
	case ExecutionRegionABI::NONE:
		return "none";
	case ExecutionRegionABI::FULL_PIPELINE:
		return "full_pipeline";
	default:
		return "unknown";
	}
}

bool ExecutionRegionABIOwnsSource(ExecutionRegionABI abi) {
	return abi == ExecutionRegionABI::FULL_PIPELINE;
}

bool ExecutionRegionABIOwnsSink(ExecutionRegionABI abi) {
	return abi == ExecutionRegionABI::FULL_PIPELINE;
}

bool ExecutionRegionABIIsFullPipeline(ExecutionRegionABI abi) {
	return abi == ExecutionRegionABI::FULL_PIPELINE;
}

const char *ExecutionRegionSourceKindToString(ExecutionRegionSourceKind kind) {
	switch (kind) {
	case ExecutionRegionSourceKind::NONE:
		return "none";
	case ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN:
		return "duckdb-table-scan";
	case ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN:
		return "table-function-scan";
	case ExecutionRegionSourceKind::GENERIC_SCAN:
		return "generic-scan";
	case ExecutionRegionSourceKind::STATEFUL_OPERATOR:
		return "stateful-operator";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionSourceExecutionKindToString(ExecutionRegionSourceExecutionKind kind) {
	switch (kind) {
	case ExecutionRegionSourceExecutionKind::NONE:
		return "none";
	case ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY:
		return "duckdb-source-boundary";
	case ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT:
		return "source-contract";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionSourceContractStatusToString(ExecutionRegionSourceContractStatus status) {
	switch (status) {
	case ExecutionRegionSourceContractStatus::NONE:
		return "none";
	case ExecutionRegionSourceContractStatus::READY:
		return "ready";
	case ExecutionRegionSourceContractStatus::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionStateContractStatusToString(ExecutionRegionStateContractStatus status) {
	switch (status) {
	case ExecutionRegionStateContractStatus::NONE:
		return "none";
	case ExecutionRegionStateContractStatus::READY:
		return "ready";
	case ExecutionRegionStateContractStatus::MISSING:
		return "missing";
	case ExecutionRegionStateContractStatus::BLOCKED:
		return "blocked";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionOperatorContractKindToString(ExecutionRegionOperatorContractKind kind) {
	switch (kind) {
	case ExecutionRegionOperatorContractKind::NONE:
		return "none";
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE:
		return "nested-loop-join-probe";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionJoinTypeToString(ExecutionRegionJoinType type) {
	switch (type) {
	case ExecutionRegionJoinType::LEFT:
		return "left";
	case ExecutionRegionJoinType::RIGHT:
		return "right";
	case ExecutionRegionJoinType::INNER:
		return "inner";
	case ExecutionRegionJoinType::OUTER:
		return "full";
	case ExecutionRegionJoinType::SEMI:
		return "semi";
	case ExecutionRegionJoinType::ANTI:
		return "anti";
	case ExecutionRegionJoinType::MARK:
		return "mark";
	case ExecutionRegionJoinType::RIGHT_SEMI:
		return "right_semi";
	case ExecutionRegionJoinType::RIGHT_ANTI:
		return "right_anti";
	default:
		return "invalid";
	}
}

bool ExecutionRegionJoinTypePropagatesBuildSide(ExecutionRegionJoinType type) {
	switch (type) {
	case ExecutionRegionJoinType::RIGHT:
	case ExecutionRegionJoinType::OUTER:
	case ExecutionRegionJoinType::RIGHT_SEMI:
	case ExecutionRegionJoinType::RIGHT_ANTI:
		return true;
	default:
		return false;
	}
}

const char *ExecutionRegionComparisonTypeToString(ExecutionRegionComparisonType type) {
	switch (type) {
	case ExecutionRegionComparisonType::EQUAL:
		return "compare_equal";
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return "compare_notequal";
	case ExecutionRegionComparisonType::LESS_THAN:
		return "compare_lessthan";
	case ExecutionRegionComparisonType::GREATER_THAN:
		return "compare_greaterthan";
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return "compare_lessthanorequalto";
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return "compare_greaterthanorequalto";
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return "compare_distinct_from";
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return "compare_not_distinct_from";
	default:
		return "invalid";
	}
}

const char *ExecutionRegionSinkKindToString(ExecutionRegionSinkKind kind) {
	switch (kind) {
	case ExecutionRegionSinkKind::NONE:
		return "none";
	case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
		return "nested-loop-join-build";
	case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
		return "result-collector-sink";
	case ExecutionRegionSinkKind::SORT:
		return "sort";
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return "materialization";
	case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
		return "delim-join-sink";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionAggregateOperatorKindToString(ExecutionRegionAggregateOperatorKind kind) {
	switch (kind) {
	case ExecutionRegionAggregateOperatorKind::NONE:
		return "none";
	case ExecutionRegionAggregateOperatorKind::HASH:
		return "hash";
	case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
		return "perfect-hash";
	case ExecutionRegionAggregateOperatorKind::UNGROUPED:
		return "ungrouped";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionVectorFormatKindToString(ExecutionRegionVectorFormatKind kind) {
	switch (kind) {
	case ExecutionRegionVectorFormatKind::NONE:
		return "none";
	case ExecutionRegionVectorFormatKind::DATA_CHUNK:
		return "data-chunk";
	case ExecutionRegionVectorFormatKind::UNIFIED_VECTOR:
		return "unified-vector";
	case ExecutionRegionVectorFormatKind::FLAT_VECTOR:
		return "flat-vector";
	case ExecutionRegionVectorFormatKind::SELECTION_VECTOR:
		return "selection-vector";
	case ExecutionRegionVectorFormatKind::BOUNDARY:
		return "boundary";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionVectorSourceKindToString(ExecutionRegionVectorSourceKind kind) {
	switch (kind) {
	case ExecutionRegionVectorSourceKind::NONE:
		return "none";
	case ExecutionRegionVectorSourceKind::REGION_INPUT:
		return "region-input";
	case ExecutionRegionVectorSourceKind::OPERATOR_OUTPUT:
		return "operator-output";
	case ExecutionRegionVectorSourceKind::BOUNDARY:
		return "boundary";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionSelectionSourceKindToString(ExecutionRegionSelectionSourceKind kind) {
	switch (kind) {
	case ExecutionRegionSelectionSourceKind::NONE:
		return "none";
	case ExecutionRegionSelectionSourceKind::INPUT_SELECTION:
		return "input-selection";
	case ExecutionRegionSelectionSourceKind::FILTER_SELECTION:
		return "filter-selection";
	case ExecutionRegionSelectionSourceKind::BOUNDARY:
		return "boundary";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionCapabilityTypeKindToString(ExecutionRegionCapabilityTypeKind kind) {
	switch (kind) {
	case ExecutionRegionCapabilityTypeKind::UNKNOWN:
		return "unknown";
	case ExecutionRegionCapabilityTypeKind::BOOL:
		return "bool";
	case ExecutionRegionCapabilityTypeKind::INT8:
		return "int8";
	case ExecutionRegionCapabilityTypeKind::INT16:
		return "int16";
	case ExecutionRegionCapabilityTypeKind::INT32:
		return "int32";
	case ExecutionRegionCapabilityTypeKind::INT64:
		return "int64";
	case ExecutionRegionCapabilityTypeKind::INT128:
		return "int128";
	case ExecutionRegionCapabilityTypeKind::UINT8:
		return "uint8";
	case ExecutionRegionCapabilityTypeKind::UINT16:
		return "uint16";
	case ExecutionRegionCapabilityTypeKind::UINT32:
		return "uint32";
	case ExecutionRegionCapabilityTypeKind::UINT64:
		return "uint64";
	case ExecutionRegionCapabilityTypeKind::UINT128:
		return "uint128";
	case ExecutionRegionCapabilityTypeKind::FLOAT:
		return "float";
	case ExecutionRegionCapabilityTypeKind::DOUBLE:
		return "double";
	case ExecutionRegionCapabilityTypeKind::DECIMAL16:
		return "decimal16";
	case ExecutionRegionCapabilityTypeKind::DECIMAL32:
		return "decimal32";
	case ExecutionRegionCapabilityTypeKind::DECIMAL64:
		return "decimal64";
	case ExecutionRegionCapabilityTypeKind::DECIMAL128:
		return "decimal128";
	case ExecutionRegionCapabilityTypeKind::DATE:
		return "date";
	case ExecutionRegionCapabilityTypeKind::VARCHAR:
		return "varchar";
	case ExecutionRegionCapabilityTypeKind::POINTER:
		return "pointer";
	case ExecutionRegionCapabilityTypeKind::OTHER:
		return "other";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionCapabilityValidityKindToString(ExecutionRegionCapabilityValidityKind kind) {
	switch (kind) {
	case ExecutionRegionCapabilityValidityKind::UNKNOWN:
		return "unknown";
	case ExecutionRegionCapabilityValidityKind::NOT_NULL:
		return "not-null";
	case ExecutionRegionCapabilityValidityKind::MAY_HAVE_NULL:
		return "may-have-null";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionBoundaryKindToString(ExecutionRegionBoundaryKind kind) {
	switch (kind) {
	case ExecutionRegionBoundaryKind::NONE:
		return "none";
	case ExecutionRegionBoundaryKind::SCAN:
		return "scan";
	case ExecutionRegionBoundaryKind::SOURCE_CONTRACT:
		return "source-contract";
	case ExecutionRegionBoundaryKind::SINK:
		return "sink";
	case ExecutionRegionBoundaryKind::SINK_NATIVE:
		return "sink-native";
	case ExecutionRegionBoundaryKind::OPERATOR_NATIVE:
		return "operator-native";
	case ExecutionRegionBoundaryKind::OPERATOR_CONTRACT_BOUNDARY:
		return "operator-contract-boundary";
	case ExecutionRegionBoundaryKind::OPERATOR_MISSING:
		return "operator-missing";
	case ExecutionRegionBoundaryKind::EXPRESSION_MISSING:
		return "expression-missing";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionOwnershipKindToString(ExecutionRegionOwnershipKind kind) {
	switch (kind) {
	case ExecutionRegionOwnershipKind::NONE:
		return "none";
	case ExecutionRegionOwnershipKind::GENERATED_IR:
		return "generated-ir";
	case ExecutionRegionOwnershipKind::NATIVE_CONTRACT:
		return "native-contract";
	case ExecutionRegionOwnershipKind::SOURCE_BOUNDARY:
		return "source-boundary";
	case ExecutionRegionOwnershipKind::MISSING_CONTRACT:
		return "missing-contract";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionStageKindToString(ExecutionRegionStageKind kind) {
	switch (kind) {
	case ExecutionRegionStageKind::SOURCE:
		return "source";
	case ExecutionRegionStageKind::SOURCE_FILTER:
		return "source-filter";
	case ExecutionRegionStageKind::FILTER:
		return "filter";
	case ExecutionRegionStageKind::PROJECTION:
		return "projection";
	case ExecutionRegionStageKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case ExecutionRegionStageKind::HASH_JOIN_BUILD:
		return "hash-join-build";
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE:
		return "nested-loop-join-probe";
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD:
		return "nested-loop-join-build";
	case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
		return "hash-aggregate-update";
	case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
		return "perfect-hash-aggregate-update";
	case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
		return "ungrouped-aggregate-update";
	case ExecutionRegionStageKind::APPEND_SINK:
		return "append-sink";
	case ExecutionRegionStageKind::SORT_SINK:
		return "sort-sink";
	case ExecutionRegionStageKind::DELIM_JOIN_SINK:
		return "delim-join-sink";
	case ExecutionRegionStageKind::SINK_BOUNDARY:
		return "sink-boundary";
	case ExecutionRegionStageKind::OPERATOR_BOUNDARY:
		return "operator-boundary";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionStageExecutionKindToString(ExecutionRegionStageExecutionKind kind) {
	switch (kind) {
	case ExecutionRegionStageExecutionKind::NONE:
		return "none";
	case ExecutionRegionStageExecutionKind::GENERATED_IR:
		return "generated-ir";
	case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
		return "native-contract";
	case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
		return "source-boundary";
	case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
		return "missing-contract";
	default:
		return "unknown";
	}
}

const char *ExecutionCompiledContractKindToString(ExecutionCompiledContractKind kind) {
	switch (kind) {
	case ExecutionCompiledContractKind::SCAN_CURSOR:
		return "scan_cursor";
	case ExecutionCompiledContractKind::FILTER_STAGE:
		return "filter_stage";
	case ExecutionCompiledContractKind::PROJECTION_STAGE:
		return "projection_stage";
	case ExecutionCompiledContractKind::HASH_JOIN_BUILD:
		return "hash_join_build";
	case ExecutionCompiledContractKind::HASH_JOIN_PROBE_CURSOR:
		return "hash_join_probe_cursor";
	case ExecutionCompiledContractKind::NESTED_LOOP_JOIN_BUILD:
		return "nested_loop_join_build";
	case ExecutionCompiledContractKind::NESTED_LOOP_JOIN_PROBE_CURSOR:
		return "nested_loop_join_probe_cursor";
	case ExecutionCompiledContractKind::AGGREGATE_LOOKUP:
		return "aggregate_lookup";
	case ExecutionCompiledContractKind::AGGREGATE_UPDATE:
		return "aggregate_update";
	case ExecutionCompiledContractKind::SINK_CURSOR:
		return "sink_cursor";
	case ExecutionCompiledContractKind::STATE_SCAN_CURSOR:
		return "state_scan_cursor";
	default:
		return "none";
	}
}

const char *ExecutionCompiledDrainKindToString(ExecutionCompiledDrainKind kind) {
	switch (kind) {
	case ExecutionCompiledDrainKind::ONE_INPUT_ONE_OUTPUT:
		return "one_input_one_output";
	case ExecutionCompiledDrainKind::ZERO_OR_ONE_OUTPUT:
		return "zero_or_one_output";
	case ExecutionCompiledDrainKind::ZERO_OR_MANY_OUTPUT:
		return "zero_or_many_output";
	case ExecutionCompiledDrainKind::STATE_DRAIN:
		return "state_drain";
	default:
		return "none";
	}
}

const char *ExecutionRegionPolicyModeToString(ExecutionRegionPolicyMode mode) {
	switch (mode) {
	case ExecutionRegionPolicyMode::AUTO:
		return "auto";
	case ExecutionRegionPolicyMode::OFF:
		return "off";
	default:
		return "unknown";
	}
}

const char *ExecutionExpressionValidityKindToString(ExecutionExpressionValidityKind kind) {
	switch (kind) {
	case ExecutionExpressionValidityKind::CONSTANT_NULL:
		return "constant-null";
	case ExecutionExpressionValidityKind::CONSTANT_VALID:
		return "constant-valid";
	case ExecutionExpressionValidityKind::SOURCE:
		return "source";
	case ExecutionExpressionValidityKind::NOT_NULL:
		return "not-null";
	case ExecutionExpressionValidityKind::CHILD:
		return "child";
	case ExecutionExpressionValidityKind::CHILDREN_NULL_PROPAGATING:
		return "children-null-propagating";
	case ExecutionExpressionValidityKind::CHILD_OR_CAST_FAILURE:
		return "child-or-cast-failure";
	case ExecutionExpressionValidityKind::THREE_VALUED_BOOLEAN:
		return "three-valued-boolean";
	case ExecutionExpressionValidityKind::FIRST_VALID_CHILD:
		return "first-valid-child";
	case ExecutionExpressionValidityKind::CONSTANT_PLUS_NULL_GUARDS:
		return "constant-plus-null-guards";
	case ExecutionExpressionValidityKind::SQL_IN_LIST:
		return "sql-in-list";
	case ExecutionExpressionValidityKind::SQL_BETWEEN:
		return "sql-between";
	case ExecutionExpressionValidityKind::SELECTED_BRANCH:
		return "selected-branch";
	case ExecutionExpressionValidityKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *ExecutionExpressionSourceKindToString(ExecutionExpressionSourceKind kind) {
	switch (kind) {
	case ExecutionExpressionSourceKind::CONSTANT:
		return "constant";
	case ExecutionExpressionSourceKind::VECTOR:
		return "vector";
	case ExecutionExpressionSourceKind::DERIVED:
		return "derived";
	case ExecutionExpressionSourceKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *ExecutionExpressionExceptionKindToString(ExecutionExpressionExceptionKind kind) {
	switch (kind) {
	case ExecutionExpressionExceptionKind::NONE:
		return "none";
	case ExecutionExpressionExceptionKind::CAST:
		return "cast";
	case ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR:
		return "null-on-cast-error";
	case ExecutionExpressionExceptionKind::ARITHMETIC:
		return "arithmetic";
	case ExecutionExpressionExceptionKind::ERROR:
		return "error";
	case ExecutionExpressionExceptionKind::UNKNOWN:
	default:
		return "unknown";
	}
}

const char *ExecutionExpressionIRKindToString(ExecutionExpressionIRKind kind) {
	switch (kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		return "constant";
	case ExecutionExpressionIRKind::REFERENCE:
		return "reference";
	case ExecutionExpressionIRKind::UNARY:
		return "unary";
	case ExecutionExpressionIRKind::BINARY:
		return "binary";
	case ExecutionExpressionIRKind::CAST:
		return "cast";
	case ExecutionExpressionIRKind::CONJUNCTION:
		return "conjunction";
	case ExecutionExpressionIRKind::COALESCE:
		return "coalesce";
	case ExecutionExpressionIRKind::CONSTANT_OR_NULL:
		return "constant_or_null";
	case ExecutionExpressionIRKind::IN_LIST:
		return "in_list";
	case ExecutionExpressionIRKind::BETWEEN:
		return "between";
	case ExecutionExpressionIRKind::CASE:
		return "case";
	case ExecutionExpressionIRKind::INTRINSIC:
		return "intrinsic";
	default:
		return "unknown";
	}
}

const char *ExecutionExpressionIntrinsicKindToString(ExecutionExpressionIntrinsicKind kind) {
	switch (kind) {
	case ExecutionExpressionIntrinsicKind::NONE:
		return "none";
	case ExecutionExpressionIntrinsicKind::STRING_COMPRESS:
		return "string_compress";
	case ExecutionExpressionIntrinsicKind::STRING_DECOMPRESS:
		return "string_decompress";
	case ExecutionExpressionIntrinsicKind::STRING_PREFIX:
		return "string_prefix";
	case ExecutionExpressionIntrinsicKind::STRING_SUFFIX:
		return "string_suffix";
	case ExecutionExpressionIntrinsicKind::STRING_CONTAINS:
		return "string_contains";
	case ExecutionExpressionIntrinsicKind::STRING_LIKE:
		return "string_like";
	case ExecutionExpressionIntrinsicKind::STRING_SUBSTRING:
		return "string_substring";
	case ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS:
		return "integral_compress";
	case ExecutionExpressionIntrinsicKind::INTEGRAL_DECOMPRESS:
		return "integral_decompress";
	case ExecutionExpressionIntrinsicKind::DATE_YEAR:
		return "date_year";
	case ExecutionExpressionIntrinsicKind::ERROR:
		return "error";
	default:
		return "unknown";
	}
}

void ExecutionRegionLoweringPlan::AddNode(string label, string operator_name, ExecutionRegionLoweringKind kind,
                                          string reason) {
	AddNode(std::move(label), std::move(operator_name), ExecutionRegionOperatorKind::GENERIC, kind, std::move(reason));
}

static void AccumulateExecutionRegionLoweringFact(ExecutionRegionLoweringPlan &plan,
                                                  ExecutionRegionOperatorKind operator_kind,
                                                  ExecutionRegionLoweringKind kind, const string &reason) {
	plan.node_count++;
	auto operator_kind_index = static_cast<idx_t>(operator_kind);
	if (kind == ExecutionRegionLoweringKind::NATIVE) {
		plan.native_count++;
		if (operator_kind_index < EXECUTION_REGION_OPERATOR_KIND_COUNT) {
			plan.capability_facts.native_operator_kind_counts[operator_kind_index]++;
		}
	} else if (kind == ExecutionRegionLoweringKind::BOUNDARY) {
		plan.boundary_count++;
		if (operator_kind_index < EXECUTION_REGION_OPERATOR_KIND_COUNT) {
			plan.capability_facts.boundary_operator_kind_counts[operator_kind_index]++;
		}
		if (plan.first_boundary_reason.empty() && !reason.empty()) {
			plan.first_boundary_reason = reason;
		}
	}
}

void ExecutionRegionLoweringPlan::AddNode(string label, string operator_name, ExecutionRegionOperatorKind operator_kind,
                                          ExecutionRegionLoweringKind kind, string reason) {
	AccumulateExecutionRegionLoweringFact(*this, operator_kind, kind, reason);
	ExecutionRegionNodeLowering node;
	node.label = std::move(label);
	node.operator_name = std::move(operator_name);
	node.operator_kind = operator_kind;
	node.kind = kind;
	node.reason = std::move(reason);
	if (record_detailed_nodes) {
		nodes.push_back(std::move(node));
	}
}

void ExecutionRegionLoweringPlan::AddCompactNode(ExecutionRegionOperatorKind operator_kind,
                                                 ExecutionRegionLoweringKind kind, const string &reason) {
	AccumulateExecutionRegionLoweringFact(*this, operator_kind, kind, reason);
}

void ExecutionRegionLoweringPlan::AddBackendDataShapeCapability(ExecutionRegionVectorFormatKind input_format,
                                                                ExecutionRegionVectorFormatKind output_format,
                                                                ExecutionRegionVectorSourceKind vector_source,
                                                                ExecutionRegionSelectionSourceKind selection_source) {
	auto input_format_index = static_cast<idx_t>(input_format);
	if (input_format_index < EXECUTION_REGION_VECTOR_FORMAT_KIND_COUNT) {
		capability_facts.native_input_format_counts[input_format_index]++;
	}
	auto output_format_index = static_cast<idx_t>(output_format);
	if (output_format_index < EXECUTION_REGION_VECTOR_FORMAT_KIND_COUNT) {
		capability_facts.native_output_format_counts[output_format_index]++;
	}
	auto vector_source_index = static_cast<idx_t>(vector_source);
	if (vector_source_index < EXECUTION_REGION_VECTOR_SOURCE_KIND_COUNT) {
		capability_facts.native_vector_source_counts[vector_source_index]++;
	}
	auto selection_source_index = static_cast<idx_t>(selection_source);
	if (selection_source_index < EXECUTION_REGION_SELECTION_SOURCE_KIND_COUNT) {
		capability_facts.native_selection_source_counts[selection_source_index]++;
	}
}

static ExecutionRegionCapabilityTypeKind ExecutionRegionCapabilityTypeKindFromLogicalType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return ExecutionRegionCapabilityTypeKind::BOOL;
	case LogicalTypeId::TINYINT:
		return ExecutionRegionCapabilityTypeKind::INT8;
	case LogicalTypeId::SMALLINT:
		return ExecutionRegionCapabilityTypeKind::INT16;
	case LogicalTypeId::INTEGER:
		return ExecutionRegionCapabilityTypeKind::INT32;
	case LogicalTypeId::BIGINT:
		return ExecutionRegionCapabilityTypeKind::INT64;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return ExecutionRegionCapabilityTypeKind::INT128;
	case LogicalTypeId::UTINYINT:
		return ExecutionRegionCapabilityTypeKind::UINT8;
	case LogicalTypeId::USMALLINT:
		return ExecutionRegionCapabilityTypeKind::UINT16;
	case LogicalTypeId::UINTEGER:
		return ExecutionRegionCapabilityTypeKind::UINT32;
	case LogicalTypeId::UBIGINT:
		return ExecutionRegionCapabilityTypeKind::UINT64;
	case LogicalTypeId::UHUGEINT:
		return ExecutionRegionCapabilityTypeKind::UINT128;
	case LogicalTypeId::FLOAT:
		return ExecutionRegionCapabilityTypeKind::FLOAT;
	case LogicalTypeId::DOUBLE:
		return ExecutionRegionCapabilityTypeKind::DOUBLE;
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return ExecutionRegionCapabilityTypeKind::DECIMAL16;
		case PhysicalType::INT32:
			return ExecutionRegionCapabilityTypeKind::DECIMAL32;
		case PhysicalType::INT64:
			return ExecutionRegionCapabilityTypeKind::DECIMAL64;
		case PhysicalType::INT128:
			return ExecutionRegionCapabilityTypeKind::DECIMAL128;
		default:
			return ExecutionRegionCapabilityTypeKind::OTHER;
		}
	case LogicalTypeId::DATE:
		return ExecutionRegionCapabilityTypeKind::DATE;
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR:
		return ExecutionRegionCapabilityTypeKind::VARCHAR;
	case LogicalTypeId::POINTER:
		return ExecutionRegionCapabilityTypeKind::POINTER;
	case LogicalTypeId::INVALID:
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::ANY:
	case LogicalTypeId::UNBOUND:
	case LogicalTypeId::TEMPLATE:
		return ExecutionRegionCapabilityTypeKind::UNKNOWN;
	default:
		return ExecutionRegionCapabilityTypeKind::OTHER;
	}
}

static void AddExecutionRegionCapabilityTypeFact(ExecutionRegionCapabilityTypeKindCounts &counts,
                                                 const LogicalType &type) {
	auto type_index = static_cast<idx_t>(ExecutionRegionCapabilityTypeKindFromLogicalType(type));
	if (type_index < EXECUTION_REGION_CAPABILITY_TYPE_KIND_COUNT) {
		counts[type_index]++;
	}
}

void ExecutionRegionLoweringPlan::AddBackendSourceValidityCapability(bool not_null) {
	auto validity = not_null ? ExecutionRegionCapabilityValidityKind::NOT_NULL
	                         : ExecutionRegionCapabilityValidityKind::MAY_HAVE_NULL;
	auto validity_index = static_cast<idx_t>(validity);
	if (validity_index < EXECUTION_REGION_CAPABILITY_VALIDITY_KIND_COUNT) {
		capability_facts.backend_source_validity_counts[validity_index]++;
	}
}

void ExecutionRegionLoweringPlan::AddBackendJoinKeyTypeCapability(const LogicalType &type) {
	AddExecutionRegionCapabilityTypeFact(capability_facts.backend_join_key_type_counts, type);
}

void ExecutionRegionLoweringPlan::AddBackendGroupKeyTypeCapability(const LogicalType &type) {
	AddExecutionRegionCapabilityTypeFact(capability_facts.backend_group_key_type_counts, type);
}

void ExecutionRegionLoweringPlan::AddBackendPayloadTypeCapability(const LogicalType &type) {
	AddExecutionRegionCapabilityTypeFact(capability_facts.backend_payload_type_counts, type);
}

void ExecutionRegionLoweringPlan::AddBackendHashJoinProbeCapability(bool perfect_hash_probe, bool residual_predicate,
                                                                    idx_t equality_key_count, idx_t key_count) {
	capability_facts.backend_hash_join_probe_count++;
	if (perfect_hash_probe) {
		capability_facts.backend_perfect_hash_join_probe_count++;
	} else {
		capability_facts.backend_regular_hash_join_probe_count++;
	}
	if (residual_predicate) {
		capability_facts.backend_residual_hash_join_probe_count++;
	}
	capability_facts.backend_hash_join_equality_key_count += equality_key_count;
	if (key_count > equality_key_count) {
		capability_facts.backend_hash_join_non_equality_key_count += key_count - equality_key_count;
	}
}

void ExecutionRegionLoweringPlan::AddBackendHashJoinBuildCapability() {
	capability_facts.backend_hash_join_build_count++;
}

void ExecutionRegionLoweringPlan::AddBackendNestedLoopJoinProbeCapability() {
	capability_facts.backend_nested_loop_join_probe_count++;
}

void ExecutionRegionLoweringPlan::AddBackendNestedLoopJoinBuildCapability() {
	capability_facts.backend_nested_loop_join_build_count++;
}

void ExecutionRegionLoweringPlan::AddBackendAggregateUpdateCapability(ExecutionRegionAggregateOperatorKind kind,
                                                                      bool primitive_payloads,
                                                                      bool grouped_state_addresses,
                                                                      bool perfect_hash_group_lookup) {
	switch (kind) {
	case ExecutionRegionAggregateOperatorKind::HASH:
		capability_facts.backend_hash_aggregate_update_count++;
		break;
	case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
		capability_facts.backend_perfect_hash_aggregate_update_count++;
		break;
	case ExecutionRegionAggregateOperatorKind::UNGROUPED:
		capability_facts.backend_ungrouped_aggregate_update_count++;
		break;
	default:
		break;
	}
	if (primitive_payloads) {
		capability_facts.backend_primitive_aggregate_payload_update_count++;
	}
	if (grouped_state_addresses) {
		capability_facts.backend_grouped_state_address_lookup_count++;
		if (perfect_hash_group_lookup) {
			capability_facts.backend_generated_perfect_hash_lookup_count++;
		} else {
			capability_facts.backend_native_state_address_lookup_count++;
		}
	}
}

void ExecutionRegionLoweringPlan::AddFusionBlocker(string reason) {
	if (first_fusion_blocker.empty()) {
		first_fusion_blocker = reason;
	}
	if (record_detailed_nodes) {
		fusion_blockers.push_back(std::move(reason));
	}
}

void ExecutionRegionLoweringPlan::SetRecordDetailedNodes(bool record_detailed_nodes_p) {
	record_detailed_nodes = record_detailed_nodes_p;
}

void ExecutionRegionLoweringPlan::SetCompiledExecutionMode(ExecutionRegionExecutionMode execution_mode) {
	compiled_execution_mode = execution_mode;
}

void ExecutionRegionLoweringPlan::SetFullyFused(bool fully_fused_p) {
	fully_fused = fully_fused_p;
}

void ExecutionRegionLoweringPlan::SetUsesScanFilters(bool uses_scan_filters_p) {
	uses_scan_filters = uses_scan_filters_p;
}

void ExecutionRegionLoweringPlan::SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void ExecutionRegionLoweringPlan::SetOperatorStageIR(string stage_ir) {
	operator_stage_ir = std::move(stage_ir);
}

idx_t ExecutionRegionLoweringPlan::NativeCount() const {
	return native_count;
}

idx_t ExecutionRegionLoweringPlan::BoundaryCount() const {
	return boundary_count;
}

bool ExecutionRegionLoweringPlan::HasNodes() const {
	return node_count > 0;
}

bool ExecutionRegionLoweringPlan::RecordDetailedNodes() const {
	return record_detailed_nodes;
}

ExecutionRegionExecutionMode ExecutionRegionLoweringPlan::ExpectedCompiledExecutionMode() const {
	return compiled_execution_mode;
}

bool ExecutionRegionLoweringPlan::IsFullyFused() const {
	return fully_fused;
}

bool ExecutionRegionLoweringPlan::UsesScanFilters() const {
	return uses_scan_filters;
}

ExecutionRegionSourceExecutionKind ExecutionRegionLoweringPlan::SelectedSourceExecution() const {
	return selected_source_execution;
}

static void AppendFirstExecutionRegionLoweringReasonToken(string &result, const string &reason) {
	auto separator = reason.find(';');
	if (separator == string::npos) {
		result += reason;
		return;
	}
	result.append(reason, 0, separator);
}

static void AppendExecutionRegionOperatorKindCounts(string &result, const char *label,
                                                    const ExecutionRegionOperatorKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_OPERATOR_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result += ExecutionRegionOperatorKindToString(static_cast<ExecutionRegionOperatorKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void AppendExecutionRegionVectorFormatKindCounts(string &result, const char *label,
                                                        const ExecutionRegionVectorFormatKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_VECTOR_FORMAT_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result += ExecutionRegionVectorFormatKindToString(static_cast<ExecutionRegionVectorFormatKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void AppendExecutionRegionVectorSourceKindCounts(string &result, const char *label,
                                                        const ExecutionRegionVectorSourceKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_VECTOR_SOURCE_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result += ExecutionRegionVectorSourceKindToString(static_cast<ExecutionRegionVectorSourceKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void AppendExecutionRegionSelectionSourceKindCounts(string &result, const char *label,
                                                           const ExecutionRegionSelectionSourceKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_SELECTION_SOURCE_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result += ExecutionRegionSelectionSourceKindToString(static_cast<ExecutionRegionSelectionSourceKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void AppendExecutionRegionCapabilityTypeKindCounts(string &result, const char *label,
                                                          const ExecutionRegionCapabilityTypeKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_CAPABILITY_TYPE_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result += ExecutionRegionCapabilityTypeKindToString(static_cast<ExecutionRegionCapabilityTypeKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void
AppendExecutionRegionCapabilityValidityKindCounts(string &result, const char *label,
                                                  const ExecutionRegionCapabilityValidityKindCounts &counts) {
	bool first = true;
	for (idx_t index = 0; index < EXECUTION_REGION_CAPABILITY_VALIDITY_KIND_COUNT; index++) {
		auto count = counts[index];
		if (count == 0) {
			continue;
		}
		if (first) {
			result += ";";
			result += label;
			result += "=";
			first = false;
		} else {
			result += "|";
		}
		result +=
		    ExecutionRegionCapabilityValidityKindToString(static_cast<ExecutionRegionCapabilityValidityKind>(index));
		result += ":";
		result += std::to_string(count);
	}
}

static void AppendExecutionRegionCapabilityCount(string &result, const char *label, const char *capability, idx_t count,
                                                 bool &first) {
	if (count == 0) {
		return;
	}
	if (first) {
		result += ";";
		result += label;
		result += "=";
		first = false;
	} else {
		result += "|";
	}
	result += capability;
	result += ":";
	result += std::to_string(count);
}

static void AppendExecutionRegionBackendCapabilityFacts(string &result, const ExecutionRegionLoweringPlan &plan) {
	auto &facts = plan.capability_facts;
	bool first_join = true;
	AppendExecutionRegionCapabilityCount(result, "backend_join", "hash_probe", facts.backend_hash_join_probe_count,
	                                     first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "regular_hash_probe",
	                                     facts.backend_regular_hash_join_probe_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "perfect_hash_probe",
	                                     facts.backend_perfect_hash_join_probe_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "residual_hash_probe",
	                                     facts.backend_residual_hash_join_probe_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "equality_key",
	                                     facts.backend_hash_join_equality_key_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "non_equality_key",
	                                     facts.backend_hash_join_non_equality_key_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "hash_build", facts.backend_hash_join_build_count,
	                                     first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "nested_loop_probe",
	                                     facts.backend_nested_loop_join_probe_count, first_join);
	AppendExecutionRegionCapabilityCount(result, "backend_join", "nested_loop_build",
	                                     facts.backend_nested_loop_join_build_count, first_join);

	bool first_aggregate = true;
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "hash_update",
	                                     facts.backend_hash_aggregate_update_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "perfect_hash_update",
	                                     facts.backend_perfect_hash_aggregate_update_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "ungrouped_update",
	                                     facts.backend_ungrouped_aggregate_update_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "primitive_payload_update",
	                                     facts.backend_primitive_aggregate_payload_update_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "grouped_state_address_lookup",
	                                     facts.backend_grouped_state_address_lookup_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "generated_perfect_hash_lookup",
	                                     facts.backend_generated_perfect_hash_lookup_count, first_aggregate);
	AppendExecutionRegionCapabilityCount(result, "backend_aggregate", "native_state_address_lookup",
	                                     facts.backend_native_state_address_lookup_count, first_aggregate);
}

string ExecutionRegionLoweringPlan::CompactEventReason() const {
	string result;
	if (!first_fusion_blocker.empty()) {
		result = "region-lowering-blocked:";
		AppendFirstExecutionRegionLoweringReasonToken(result, first_fusion_blocker);
		result += ";";
	} else if (!first_boundary_reason.empty()) {
		result = "region-lowering-blocked:";
		AppendFirstExecutionRegionLoweringReasonToken(result, first_boundary_reason);
		result += ";";
	}
	result += "region-lowering:native=" + std::to_string(native_count) + ",boundary=" + std::to_string(boundary_count) +
	          ",fully-fused=" + (fully_fused ? string("true") : "false");
	AppendExecutionRegionOperatorKindCounts(result, "backend_native", capability_facts.native_operator_kind_counts);
	AppendExecutionRegionOperatorKindCounts(result, "backend_boundary", capability_facts.boundary_operator_kind_counts);
	AppendExecutionRegionVectorFormatKindCounts(result, "backend_input_format",
	                                            capability_facts.native_input_format_counts);
	AppendExecutionRegionVectorFormatKindCounts(result, "backend_output_format",
	                                            capability_facts.native_output_format_counts);
	AppendExecutionRegionVectorSourceKindCounts(result, "backend_vector_source",
	                                            capability_facts.native_vector_source_counts);
	AppendExecutionRegionSelectionSourceKindCounts(result, "backend_selection_source",
	                                               capability_facts.native_selection_source_counts);
	AppendExecutionRegionCapabilityValidityKindCounts(result, "backend_source_validity",
	                                                  capability_facts.backend_source_validity_counts);
	AppendExecutionRegionCapabilityTypeKindCounts(result, "backend_join_key_type",
	                                              capability_facts.backend_join_key_type_counts);
	AppendExecutionRegionCapabilityTypeKindCounts(result, "backend_group_key_type",
	                                              capability_facts.backend_group_key_type_counts);
	AppendExecutionRegionCapabilityTypeKindCounts(result, "backend_payload_type",
	                                              capability_facts.backend_payload_type_counts);
	AppendExecutionRegionBackendCapabilityFacts(result, *this);
	if (selected_source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		result += ";selected-source-execution=";
		result += ExecutionRegionSourceExecutionKindToString(selected_source_execution);
	}
	if (uses_scan_filters) {
		result += ";uses-scan-filters=true";
	}
	return result;
}

string ExecutionRegionLoweringPlan::EventReason() const {
	auto result = CompactEventReason();
	if (!operator_stage_ir.empty()) {
		result += ";";
		result += operator_stage_ir;
	}
	for (auto &blocker : fusion_blockers) {
		result += ";fusion-blocker:";
		result += blocker;
	}
	for (auto &node : nodes) {
		result += ";";
		result += node.label;
		result += ":";
		result += node.operator_kind == ExecutionRegionOperatorKind::GENERIC
		              ? node.operator_name
		              : string(ExecutionRegionOperatorKindToTraceLabel(node.operator_kind));
		result += ":";
		result += ExecutionRegionLoweringKindToString(node.kind);
		if (!node.reason.empty()) {
			result += ":";
			result += node.reason;
		}
	}
	return result;
}

unique_ptr<ExecutionExpressionIR> ExecutionExpressionIR::Copy() const {
	auto result = make_uniq<ExecutionExpressionIR>();
	result->kind = kind;
	result->return_type = return_type;
	result->physical_type = physical_type;
	result->validity = validity;
	result->source = source;
	result->exception_behavior = exception_behavior;
	result->query_location = query_location;
	result->constant = constant;
	result->ref_index = ref_index;
	result->unary_op = unary_op;
	result->binary_op = binary_op;
	result->conjunction_op = conjunction_op;
	result->intrinsic = intrinsic;
	result->arithmetic_overflow_check = arithmetic_overflow_check;
	result->try_cast = try_cast;
	result->not_in = not_in;
	result->not_between = not_between;
	result->lower_inclusive = lower_inclusive;
	result->upper_inclusive = upper_inclusive;
	if (left) {
		result->left = left->Copy();
	}
	if (right) {
		result->right = right->Copy();
	}
	if (else_node) {
		result->else_node = else_node->Copy();
	}
	result->children.reserve(children.size());
	for (auto &child : children) {
		result->children.push_back(child->Copy());
	}
	return result;
}

ExecutionExpressionFragment::ExecutionExpressionFragment(const ExecutionExpressionFragment &other) {
	*this = other;
}

ExecutionExpressionFragment &ExecutionExpressionFragment::operator=(const ExecutionExpressionFragment &other) {
	if (this == &other) {
		return *this;
	}
	expression_index = other.expression_index;
	return_type = other.return_type;
	traits = other.traits;
	root = other.root ? other.root->Copy() : nullptr;
	reason = other.reason;
	ir = other.ir;
	return *this;
}

} // namespace duckdb

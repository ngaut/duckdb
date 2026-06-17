#include "duckdb/execution/execution_region_telemetry.hpp"

namespace duckdb {

const char *ExecutionRegionCompileTargetToString(ExecutionRegionCompileTarget target) {
	switch (target) {
	case ExecutionRegionCompileTarget::REGION:
		return "region";
	default:
		return "unknown";
	}
}

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
	case ExecutionRegionExecutionMode::UNSUPPORTED:
		return "unsupported";
	default:
		return "unknown";
	}
}

bool ExecutionRegionExecutionModeIsCompiled(ExecutionRegionExecutionMode mode) {
	return mode == ExecutionRegionExecutionMode::NATIVE;
}

const char *ExecutionRegionFormToString(ExecutionRegionForm form) {
	switch (form) {
	case ExecutionRegionForm::NONE:
		return "none";
	case ExecutionRegionForm::FUSED:
		return "fused";
	default:
		return "unknown";
	}
}

const char *ExecutionRegionExecutionBodyToString(ExecutionRegionExecutionBody body) {
	switch (body) {
	case ExecutionRegionExecutionBody::NONE:
		return "none";
	case ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE:
		return "generated-machine-code";
	case ExecutionRegionExecutionBody::NATIVE_OPERATOR_PROTOCOL:
		return "native-operator-protocol";
	default:
		return "unknown";
	}
}

ExecutionRegionExecutionBody ExecutionRegionExecutionBodyForCompileEvent(ExecutionRegionCompileStatus status,
                                                                         ExecutionRegionExecutionMode mode,
                                                                         idx_t code_size) {
	if (status != ExecutionRegionCompileStatus::COMPILED || !ExecutionRegionExecutionModeIsCompiled(mode)) {
		return ExecutionRegionExecutionBody::NONE;
	}
	if (code_size > 0) {
		return ExecutionRegionExecutionBody::GENERATED_MACHINE_CODE;
	}
	return ExecutionRegionExecutionBody::NATIVE_OPERATOR_PROTOCOL;
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

const char *ExecutionRegionSourceFilterOwnershipKindToString(ExecutionRegionSourceFilterOwnershipKind kind) {
	switch (kind) {
	case ExecutionRegionSourceFilterOwnershipKind::NONE:
		return "none";
	case ExecutionRegionSourceFilterOwnershipKind::GENERATED:
		return "generated";
	case ExecutionRegionSourceFilterOwnershipKind::DUCKDB_SCAN:
		return "duckdb-scan";
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
	case ExecutionRegionOperatorContractKind::PROJECTION:
		return "projection";
	case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE:
		return "hash-join-probe";
	case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE:
		return "nested-loop-join-probe";
	case ExecutionRegionOperatorContractKind::OPERATOR:
		return "operator";
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
	case ExecutionRegionSinkKind::HASH_AGGREGATE_DISTINCT_SINK:
		return "hash-aggregate-distinct-sink";
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
	case ExecutionRegionSinkKind::OPERATOR:
		return "operator";
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
	case ExecutionRegionStageKind::HASH_AGGREGATE_DISTINCT_SINK:
		return "hash-aggregate-distinct-sink";
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
	case ExecutionCompiledContractKind::AGGREGATE_DISTINCT_SINK:
		return "aggregate_distinct_sink";
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
	case ExecutionRegionPolicyMode::FORCE:
		return "force";
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

void ExecutionRegionLoweringPlan::AddNode(string label, string operator_name, ExecutionRegionOperatorKind operator_kind,
                                          ExecutionRegionLoweringKind kind, string reason) {
	ExecutionRegionNodeLowering node;
	node.label = std::move(label);
	node.operator_name = std::move(operator_name);
	node.operator_kind = operator_kind;
	node.kind = kind;
	node.reason = std::move(reason);
	nodes.push_back(std::move(node));
}

void ExecutionRegionLoweringPlan::AddFusionBlocker(string reason) {
	fusion_blockers.push_back(std::move(reason));
}

void ExecutionRegionLoweringPlan::SetCompiledExecutionMode(ExecutionRegionExecutionMode execution_mode) {
	compiled_execution_mode = execution_mode;
}

void ExecutionRegionLoweringPlan::SetRegionExecutionForm(ExecutionRegionForm execution_form) {
	region_execution_form = execution_form;
}

void ExecutionRegionLoweringPlan::SetSourceFilterOwnership(
    ExecutionRegionSourceFilterOwnershipKind source_filter_ownership_p) {
	source_filter_ownership = source_filter_ownership_p;
}

void ExecutionRegionLoweringPlan::SetSelectedSourceExecution(ExecutionRegionSourceExecutionKind source_execution) {
	selected_source_execution = source_execution;
}

void ExecutionRegionLoweringPlan::SetOperatorStageIR(string stage_ir) {
	operator_stage_ir = std::move(stage_ir);
}

idx_t ExecutionRegionLoweringPlan::NativeCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == ExecutionRegionLoweringKind::NATIVE) {
			result++;
		}
	}
	return result;
}

idx_t ExecutionRegionLoweringPlan::BoundaryCount() const {
	idx_t result = 0;
	for (auto &node : nodes) {
		if (node.kind == ExecutionRegionLoweringKind::BOUNDARY) {
			result++;
		}
	}
	return result;
}

ExecutionRegionExecutionMode ExecutionRegionLoweringPlan::ExpectedCompiledExecutionMode() const {
	return compiled_execution_mode;
}

ExecutionRegionForm ExecutionRegionLoweringPlan::ExpectedRegionExecutionForm() const {
	return region_execution_form;
}

ExecutionRegionSourceFilterOwnershipKind ExecutionRegionLoweringPlan::SourceFilterOwnership() const {
	return source_filter_ownership;
}

ExecutionRegionSourceExecutionKind ExecutionRegionLoweringPlan::SelectedSourceExecution() const {
	return selected_source_execution;
}

string ExecutionRegionLoweringPlan::EventReason() const {
	string result = "region-lowering:native=" + std::to_string(NativeCount()) +
	                ",boundary=" + std::to_string(BoundaryCount()) +
	                ",execution-form=" + ExecutionRegionFormToString(region_execution_form);
	if (selected_source_execution != ExecutionRegionSourceExecutionKind::NONE) {
		result += ";selected-source-execution=";
		result += ExecutionRegionSourceExecutionKindToString(selected_source_execution);
	}
	if (source_filter_ownership != ExecutionRegionSourceFilterOwnershipKind::NONE) {
		result += ";source-filter-ownership=";
		result += ExecutionRegionSourceFilterOwnershipKindToString(source_filter_ownership);
	}
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
	result->constant = constant;
	result->ref_index = ref_index;
	result->unary_op = unary_op;
	result->binary_op = binary_op;
	result->conjunction_op = conjunction_op;
	result->intrinsic = intrinsic;
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
	root = other.root ? other.root->Copy() : nullptr;
	reason = other.reason;
	ir = other.ir;
	return *this;
}

} // namespace duckdb

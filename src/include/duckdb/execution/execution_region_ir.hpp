//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_ir.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/execution_compiled_contract.hpp"
#include "duckdb/execution/execution_expression_ir.hpp"
#include "duckdb/function/aggregate_primitive_update.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExecutionRegionSourceFilter {
	idx_t filter_index = 0;
	idx_t scan_column_index = 0;
	idx_t table_column_index = 0;
	unique_ptr<ExecutionExpressionFragment> expression;
	string reason;
};

struct ExecutionRegionContractField {
	string name;
	string value;
};

enum class ExecutionHashJoinProbeOutputMode : uint8_t {
	NONE,
	MATCHED_PROBE_AND_BUILD,
	LEFT_PROBE_AND_BUILD,
	MATCHED_PROBE_ONLY,
	MARK_PROBE,
	MARK_BUILD_ONLY
};

enum class ExecutionHashJoinResidualSourceKind : uint8_t { PROBE, BUILD };

struct ExecutionHashJoinResidualSource {
	ExecutionHashJoinResidualSourceKind kind = ExecutionHashJoinResidualSourceKind::PROBE;
	idx_t source_index = 0;
	idx_t input_index = 0;
	LogicalType type;
};

struct ExecutionRegionNativeGroupedStateContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
	string ir;
};

struct ExecutionRegionNativeStateScanContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
	string ir;
};

struct ExecutionRegionNativeOperatorContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
	string ir;
};

struct ExecutionRegionTableScanContract {
	bool present = false;
	string function_name;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	idx_t column_id_count = 0;
	idx_t projected_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	idx_t source_contract_input_column_count = 0;
	vector<LogicalType> source_contract_input_types;
	vector<idx_t> source_contract_output_projection_map;
	vector<idx_t> source_contract_filter_column_map;
	bool source_contract_requires_unfiltered_input = false;
	bool source_contract_filter_prune_required = false;
	bool source_contract_filter_takeover_supported = false;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	idx_t filter_count = 0;
	string ir;
};

struct ExecutionRegionHashJoinContract {
	bool present = false;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	idx_t condition_count = 0;
	idx_t equality_condition_count = 0;
	idx_t non_equality_condition_count = 0;
	idx_t null_equal_condition_count = 0;
	vector<LogicalType> condition_types;
	vector<ExecutionRegionComparisonType> comparison_types;
	idx_t payload_column_count = 0;
	vector<idx_t> payload_column_indices;
	vector<LogicalType> payload_types;
	idx_t lhs_output_column_count = 0;
	vector<idx_t> lhs_output_column_indices;
	vector<LogicalType> lhs_output_types;
	idx_t rhs_output_column_count = 0;
	vector<LogicalType> rhs_output_types;
	idx_t lhs_probe_column_count = 0;
	vector<idx_t> lhs_probe_column_indices;
	vector<LogicalType> lhs_probe_types;
	idx_t lhs_output_in_probe_count = 0;
	idx_t delim_type_count = 0;
	bool correlated_mark_counts_required = false;
	bool residual_predicate = false;
	bool residual_info = false;
	bool residual_expression_ready = false;
	string residual_expression_blocker;
	ExecutionExpressionFragment residual_expression;
	vector<ExecutionHashJoinResidualSource> residual_sources;
	bool filter_pushdown = false;
	idx_t filter_pushdown_condition_count = 0;
	idx_t filter_pushdown_probe_count = 0;
	bool build_side_has_filter = false;
	bool source_produces_rows = false;
	bool regular_hash_table_layout_ready = false;
	bool perfect_hash_probe_shape_ready = false;
	bool found_match_column_present = false;
	bool native_probe_shape_ready = false;
	bool build_sink_shape_ready = false;
	ExecutionHashJoinProbeOutputMode native_probe_output_mode = ExecutionHashJoinProbeOutputMode::NONE;
	idx_t layout_column_count = 0;
	vector<idx_t> layout_offsets;
	idx_t tuple_size = 0;
	idx_t entry_size = 0;
	idx_t pointer_offset = 0;
	idx_t hash_column_index = 0;
	idx_t found_match_column_index = 0;
	string native_contract_blocker;
	string native_probe_shape_blocker;
	string perfect_hash_probe_shape_blocker;
	string build_sink_shape_blocker;
	ExecutionRegionNativeOperatorContract native_probe_contract;
	ExecutionRegionNativeOperatorContract native_build_contract;
	string ir;
};

struct ExecutionRegionNestedLoopJoinConditionInput {
	idx_t condition_index = 0;
	LogicalType type;
	ExecutionRegionComparisonType comparison_type = ExecutionRegionComparisonType::INVALID;
	bool lhs_expression_ready = false;
	ExecutionExpressionFragment lhs_expression;
	string lhs_expression_blocker;
	bool rhs_expression_ready = false;
	ExecutionExpressionFragment rhs_expression;
	string rhs_expression_blocker;
	string ir;
};

struct ExecutionRegionNestedLoopJoinContract {
	bool present = false;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	idx_t condition_count = 0;
	idx_t comparison_condition_count = 0;
	vector<LogicalType> condition_types;
	vector<ExecutionRegionComparisonType> comparison_types;
	vector<LogicalType> lhs_input_types;
	vector<LogicalType> rhs_input_types;
	vector<LogicalType> output_types;
	bool simple_join = false;
	bool complex_join = false;
	bool source_produces_rows = false;
	bool residual_predicate = false;
	bool filter_pushdown = false;
	bool conditions_ready = false;
	string condition_blocker;
	bool native_probe_shape_ready = false;
	bool build_sink_shape_ready = false;
	string native_probe_shape_blocker;
	string build_sink_shape_blocker;
	ExecutionRegionNativeOperatorContract native_probe_contract;
	ExecutionRegionNativeOperatorContract native_build_contract;
	vector<ExecutionRegionNestedLoopJoinConditionInput> conditions;
	string ir;
};

struct ExecutionRegionAggregateContract {
	bool present = false;
	ExecutionRegionAggregateOperatorKind kind = ExecutionRegionAggregateOperatorKind::NONE;
	idx_t group_count = 0;
	vector<LogicalType> group_types;
	idx_t aggregate_count = 0;
	vector<string> aggregate_functions;
	vector<LogicalType> aggregate_return_types;
	vector<idx_t> aggregate_child_counts;
	vector<string> aggregate_types;
	idx_t aggregate_filter_count = 0;
	idx_t aggregate_order_count = 0;
	idx_t payload_type_count = 0;
	vector<LogicalType> payload_types;
	idx_t grouping_set_count = 0;
	idx_t grouping_function_count = 0;
	idx_t radix_table_count = 0;
	idx_t distinct_aggregate_count = 0;
	idx_t distinct_table_count = 0;
	idx_t distinct_child_count = 0;
	idx_t input_group_type_count = 0;
	vector<LogicalType> input_group_types;
	idx_t non_distinct_filter_count = 0;
	idx_t distinct_filter_count = 0;
	idx_t perfect_required_bits_count = 0;
	idx_t perfect_required_bits_total = 0;
	vector<idx_t> perfect_required_bits;
	idx_t perfect_group_minima_count = 0;
	vector<Value> perfect_group_minima;
	bool grouped_state_layout_ready = false;
	vector<idx_t> grouped_state_offsets;
	vector<idx_t> grouped_state_payload_sizes;
	ExecutionRegionNativeGroupedStateContract native_grouped_state_contract;
	ExecutionRegionNativeOperatorContract native_hash_lookup_contract;
	ExecutionRegionNativeOperatorContract native_state_update_contract;
	ExecutionRegionNativeOperatorContract native_distinct_state_update_contract;
	string ir;
};

struct ExecutionRegionOrderKeyInput {
	idx_t key_index = 0;
	LogicalType type;
	PhysicalType physical_type = PhysicalType::INVALID;
	OrderType order_type = OrderType::INVALID;
	OrderByNullType null_order = OrderByNullType::INVALID;
	bool expression_ready = false;
	ExecutionExpressionFragment expression;
	string expression_blocker;
	string reason;
	string ir;
};

struct ExecutionRegionOrderContract {
	bool present = false;
	ExecutionRegionOperatorKind kind = ExecutionRegionOperatorKind::GENERIC;
	idx_t order_count = 0;
	idx_t payload_type_count = 0;
	vector<LogicalType> payload_types;
	idx_t projection_count = 0;
	vector<idx_t> projection_ids;
	bool has_limit = false;
	idx_t limit = 0;
	idx_t offset = 0;
	bool dynamic_filter = false;
	bool is_index_sort = false;
	bool all_order_keys_ready = false;
	string order_key_blocker;
	vector<ExecutionRegionOrderKeyInput> order_keys;
	string ir;
};

struct ExecutionRegionAggregateInput {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
	idx_t child_count = 0;
	string function_name;
	LogicalType return_type;
	vector<LogicalType> child_types;
	vector<idx_t> child_indices;
	vector<ExecutionExpressionFragment> child_expressions;
	bool distinct = false;
	bool has_filter = false;
	bool has_order_bys = false;
	bool order_dependent = false;
	bool has_state_update = false;
	bool primitive_update_ready = false;
	AggregatePrimitiveUpdateKind primitive_update_kind = AggregatePrimitiveUpdateKind::NONE;
	PhysicalType primitive_update_input_type = PhysicalType::INVALID;
	idx_t primitive_update_state_size = 0;
	idx_t primitive_update_state_value_offset = 0;
	idx_t primitive_update_state_is_set_offset = 0;
	string primitive_update_blocker;
	bool supported_payload_references = false;
	bool payload_expressions_ready = false;
	string payload_expression_blocker;
	string reason;
	string ir;
};

struct ExecutionRegionHashJoinKeyInput {
	idx_t key_index = 0;
	idx_t input_index = 0;
	LogicalType type;
	bool supported_reference = false;
	string reason;
	string ir;
};

struct ExecutionRegionGroupInput {
	idx_t group_index = 0;
	idx_t input_index = 0;
	LogicalType type;
	bool supported_reference = false;
	bool expression_ready = false;
	ExecutionExpressionFragment expression;
	string expression_blocker;
	string reason;
	string ir;
};

struct ExecutionSourceProtocolContract {
	ExecutionRegionSourceContractStatus status = ExecutionRegionSourceContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
	string ir;
};

struct ExecutionRegionSourceInfo {
	ExecutionRegionSourceKind kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE;
	string function_name;
	vector<ExecutionRegionContractField> fields;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	vector<ExecutionRegionSourceFilter> filters;
	ExecutionRegionTableScanContract table_scan_contract;
	ExecutionRegionHashJoinContract hash_join_contract;
	ExecutionRegionNestedLoopJoinContract nested_loop_join_contract;
	ExecutionRegionAggregateContract aggregate_contract;
	ExecutionRegionOrderContract order_contract;
	vector<ExecutionRegionAggregateInput> aggregates;
	vector<ExecutionRegionHashJoinKeyInput> hash_join_keys;
	vector<ExecutionRegionGroupInput> groups;
	ExecutionSourceProtocolContract source_contract;
	ExecutionRegionNativeStateScanContract native_state_scan_contract;
	string reason;
	string ir;
};

struct ExecutionRegionOperatorInfo {
	ExecutionRegionOperatorContractKind kind = ExecutionRegionOperatorContractKind::NONE;
	vector<ExecutionRegionContractField> fields;
	ExecutionRegionHashJoinContract hash_join_contract;
	vector<ExecutionRegionHashJoinKeyInput> hash_join_keys;
	ExecutionRegionNestedLoopJoinContract nested_loop_join_contract;
	string reason;
	string ir;
};

struct ExecutionRegionSinkInfo {
	ExecutionRegionSinkKind kind = ExecutionRegionSinkKind::NONE;
	vector<ExecutionRegionContractField> fields;
	ExecutionRegionNativeOperatorContract native_sink_contract;
	ExecutionRegionHashJoinContract hash_join_contract;
	ExecutionRegionNestedLoopJoinContract nested_loop_join_contract;
	ExecutionRegionAggregateContract aggregate_contract;
	ExecutionRegionOrderContract order_contract;
	vector<ExecutionRegionAggregateInput> aggregates;
	vector<ExecutionRegionHashJoinKeyInput> hash_join_keys;
	vector<ExecutionRegionGroupInput> groups;
	string reason;
	string ir;
};

struct ExecutionRegionContract {
	ExecutionRegionABI abi = ExecutionRegionABI::NONE;
	ExecutionRegionOwnershipKind source_ownership = ExecutionRegionOwnershipKind::NONE;
	ExecutionRegionOwnershipKind state_scan_ownership = ExecutionRegionOwnershipKind::NONE;
	ExecutionRegionOwnershipKind transform_ownership = ExecutionRegionOwnershipKind::NONE;
	ExecutionRegionOwnershipKind sink_ownership = ExecutionRegionOwnershipKind::NONE;
	idx_t generated_operator_count = 0;
	idx_t source_boundary_count = 0;
	idx_t missing_contract_count = 0;
	vector<string> required_capabilities;
	vector<string> blockers;
	string ir;

	bool OwnsSource() const {
		return source_ownership != ExecutionRegionOwnershipKind::NONE;
	}

	bool OwnsTransform() const {
		return transform_ownership != ExecutionRegionOwnershipKind::NONE;
	}

	bool OwnsSink() const {
		return sink_ownership != ExecutionRegionOwnershipKind::NONE;
	}

	bool OwnsStateScan() const {
		return state_scan_ownership != ExecutionRegionOwnershipKind::NONE;
	}
};

struct ExecutionRegionCandidateTraits {
	bool expression_traits_known = false;
	ExecutionRegionSourceKind source_kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSinkKind sink_kind = ExecutionRegionSinkKind::NONE;
	idx_t source_filter_count = 0;
	idx_t source_filter_expression_count = 0;
	idx_t source_filter_missing_count = 0;
	idx_t source_comparison_filter_count = 0;
	idx_t source_integer_comparison_filter_count = 0;
	idx_t source_non_integer_comparison_filter_count = 0;
	idx_t source_conjunction_filter_count = 0;
	idx_t source_projected_column_count = 0;
	idx_t source_returned_column_count = 0;
	idx_t filter_count = 0;
	idx_t projection_count = 0;
	idx_t operator_count = 0;
	idx_t hash_join_operator_count = 0;
	idx_t right_hash_join_operator_count = 0;
	idx_t inner_hash_join_operator_count = 0;
	idx_t aggregate_count = 0;
	idx_t aggregate_count_function_count = 0;
	idx_t aggregate_sum_function_count = 0;
	idx_t aggregate_other_function_count = 0;
	idx_t core_expression_operator_count = 0;
	idx_t arithmetic_projection_count = 0;
	idx_t integer_arithmetic_projection_count = 0;
	idx_t non_integer_arithmetic_projection_count = 0;
	idx_t reference_projection_count = 0;
	idx_t comparison_filter_count = 0;
	idx_t integer_comparison_filter_count = 0;
	idx_t non_integer_comparison_filter_count = 0;
	idx_t conjunction_filter_count = 0;
	idx_t expression_missing_count = 0;
	idx_t operator_missing_count = 0;
	string ir;

	bool HasSource() const {
		return source_kind != ExecutionRegionSourceKind::NONE;
	}

	bool HasSink() const {
		return sink_kind != ExecutionRegionSinkKind::NONE;
	}
};

struct ExecutionRegionNode {
	string label;
	string operator_name;
	ExecutionRegionOperatorKind operator_kind = ExecutionRegionOperatorKind::GENERIC;
	idx_t operator_index = DConstants::INVALID_INDEX;
	ExecutionCompiledOperatorContract compiled_contract;
	ExecutionRegionNodeKind kind = ExecutionRegionNodeKind::OPERATOR;
	vector<LogicalType> output_types;
	idx_t estimated_cardinality = 0;
	ExecutionRegionVectorFormatKind input_format = ExecutionRegionVectorFormatKind::NONE;
	ExecutionRegionVectorFormatKind output_format = ExecutionRegionVectorFormatKind::NONE;
	ExecutionRegionVectorSourceKind vector_source = ExecutionRegionVectorSourceKind::NONE;
	ExecutionRegionSelectionSourceKind selection_source = ExecutionRegionSelectionSourceKind::NONE;
	ExecutionRegionBoundaryKind boundary = ExecutionRegionBoundaryKind::NONE;
	unique_ptr<ExecutionRegionSourceInfo> source;
	unique_ptr<ExecutionRegionOperatorInfo> operator_info;
	unique_ptr<ExecutionRegionSinkInfo> sink;
	unique_ptr<ExecutionExpressionFragment> filter;
	vector<unique_ptr<ExecutionExpressionFragment>> projections;
	string blocker_reason;
};

struct ExecutionRegionContractShapePart {
	ExecutionRegionNodeKind node_kind = ExecutionRegionNodeKind::OPERATOR;
	string shape;
};

struct ExecutionRegionSignature {
	string context;
	string shape;
	string feature_shape;
	string context_feature_shape;
	vector<ExecutionRegionContractShapePart> contract_shape_parts;
	string contract_shape;
	string ir;
};

struct ExecutionRegionStage {
	ExecutionRegionStageKind kind = ExecutionRegionStageKind::OPERATOR_BOUNDARY;
	ExecutionRegionStageExecutionKind execution = ExecutionRegionStageExecutionKind::NONE;
	ExecutionRegionOwnershipKind ownership = ExecutionRegionOwnershipKind::NONE;
	ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE;
	ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE;
	idx_t node_index = DConstants::INVALID_INDEX;
	idx_t operator_index = DConstants::INVALID_INDEX;
	idx_t filter_index = DConstants::INVALID_INDEX;
	string operator_name;
	string required_capability;
	string reason;
};

struct ExecutionRegionStagePlan {
	vector<ExecutionRegionStage> stages;
	string shape;
	string ir;

	bool HasStages() const {
		return !stages.empty();
	}
};

struct ExecutionRegionCandidate {
	idx_t candidate_id = 0;
	idx_t first_node = 0;
	idx_t node_count = 0;
	idx_t start_operator_index = 0;
	idx_t end_operator_index = 0;
	idx_t estimated_cardinality = 0;
	vector<LogicalType> input_types;
	vector<LogicalType> output_types;
	string shape;
	string pipeline_shape;
	string context_pipeline_shape;
	ExecutionRegionCandidateTraits traits;
	ExecutionRegionContract contract;
	ExecutionRegionCandidateTraits upstream_traits;
	ExecutionRegionCandidateTraits context_traits;
	ExecutionRegionCandidateTraits continuation_traits;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSourceFilterOwnershipKind source_filter_ownership = ExecutionRegionSourceFilterOwnershipKind::NONE;
	ExecutionRegionSignature signature;
	ExecutionRegionStagePlan stage_plan;
	string ir;

	idx_t EndNode() const {
		return first_node + node_count;
	}
};

struct ExecutionRegionPipelineInventory {
	bool source_produces_rows = true;
	ExecutionRegionSourceKind source_kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSinkKind sink_kind = ExecutionRegionSinkKind::NONE;
	ExecutionRegionOperatorKind source_operator_kind = ExecutionRegionOperatorKind::GENERIC;
	ExecutionRegionOperatorKind sink_operator_kind = ExecutionRegionOperatorKind::GENERIC;
	string source_operator_name;
	string sink_operator_name;
	vector<string> operator_names;
	vector<ExecutionRegionOperatorKind> operator_kinds;
	vector<ExecutionRegionBoundaryKind> operator_boundaries;
	idx_t estimated_cardinality = 0;
	idx_t source_filter_count = 0;
	idx_t source_projected_column_count = 0;
	idx_t source_returned_column_count = 0;
	idx_t operator_count = 0;
	idx_t hash_join_operator_count = 0;
	idx_t right_hash_join_operator_count = 0;
	idx_t inner_hash_join_operator_count = 0;
	idx_t filter_operator_count = 0;
	idx_t projection_operator_count = 0;
	idx_t aggregate_count = 0;
	idx_t aggregate_count_function_count = 0;
	idx_t aggregate_sum_function_count = 0;
	idx_t aggregate_other_function_count = 0;
	bool workload_relevant = false;
	string workload_relevance_reason;
	string feature_shape;
	string candidate_shape;
	vector<ExecutionRegionContractShapePart> contract_shape_parts;
	string contract_shape;
	string pipeline_shape;
	string ir;

	bool HasSource() const {
		return source_kind != ExecutionRegionSourceKind::NONE;
	}

	bool HasSink() const {
		return sink_kind != ExecutionRegionSinkKind::NONE;
	}

	bool HasFilterOperator() const {
		return filter_operator_count > 0;
	}

	bool HasProjectionOperator() const {
		return projection_operator_count > 0;
	}

	bool HasTableScanSource() const {
		return source_kind == ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN ||
		       source_operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN;
	}

	bool HasOperatorKind(ExecutionRegionOperatorKind kind) const {
		for (auto operator_kind : operator_kinds) {
			if (operator_kind == kind) {
				return true;
			}
		}
		return false;
	}

	bool HasHashJoinOperator() const {
		return HasOperatorKind(ExecutionRegionOperatorKind::HASH_JOIN);
	}

	bool HasNestedLoopJoinOperator() const {
		return HasOperatorKind(ExecutionRegionOperatorKind::NESTED_LOOP_JOIN);
	}

	bool HasWrapperOnlySource() const {
		return source_operator_kind == ExecutionRegionOperatorKind::CREATE_TABLE_AS ||
		       source_operator_kind == ExecutionRegionOperatorKind::RESULT_COLLECTOR ||
		       source_operator_kind == ExecutionRegionOperatorKind::EXPLAIN_ANALYZE;
	}

	bool HasHashJoinSink() const {
		return sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD;
	}

	bool HasNestedLoopJoinSink() const {
		return sink_kind == ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD;
	}

	bool HasHashAggregateSink() const {
		return sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	}

	bool HasHashAggregateDistinctSink() const {
		return sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_DISTINCT_SINK;
	}

	bool HasPerfectHashAggregateSink() const {
		return sink_kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
	}

	bool HasUngroupedAggregateSink() const {
		return sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	}
};

DUCKDB_API bool ExecutionRegionAggregateFunctionIsCount(const string &function_name);
DUCKDB_API bool ExecutionRegionAggregateFunctionIsSum(const string &function_name);
DUCKDB_API void AccumulateExecutionRegionHashJoinKind(ExecutionRegionJoinType join_type, idx_t &hash_join_count,
                                                      idx_t &right_hash_join_count, idx_t &inner_hash_join_count);
DUCKDB_API void AccumulateExecutionRegionAggregateFunctionKinds(const ExecutionRegionAggregateContract &contract,
                                                                idx_t &aggregate_count, idx_t &count_function_count,
                                                                idx_t &sum_function_count, idx_t &other_function_count);

struct ExecutionRegionIR {
	vector<ExecutionRegionNode> nodes;
	vector<ExecutionRegionCandidate> candidates;
	string pipeline_shape;
	string ir;
};

DUCKDB_API ExecutionSourceProtocolContract
BuildExecutionSourceProtocolContract(ExecutionRegionSourceKind kind, ExecutionRegionSourceExecutionKind execution);
DUCKDB_API string DescribeExecutionRegionSourceInfo(
    const ExecutionRegionSourceInfo &source,
    ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE);

} // namespace duckdb

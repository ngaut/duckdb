//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/region.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/jit/compiled_contract.hpp"
#include "duckdb/execution/jit/ir.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct JitRegionSourceFilter {
	idx_t filter_index = 0;
	idx_t scan_column_index = 0;
	idx_t table_column_index = 0;
	unique_ptr<JitExpressionFragment> expression;
	string reason;
};

struct JitRegionProtocolField {
	string name;
	string value;
};

enum class JitRegionHashJoinProbeOutputMode : uint8_t {
	NONE,
	MATCHED_PROBE_AND_BUILD,
	MATCHED_PROBE_ONLY,
	MARK_PROBE,
	MARK_BUILD_ONLY
};

struct JitRegionNativeGroupedStateContract {
	JitRegionStateContractStatus status = JitRegionStateContractStatus::NONE;
	string required_capability;
	string protocol_version;
	string blocker;
	string ir;
};

struct JitRegionNativeStateScanContract {
	JitRegionStateContractStatus status = JitRegionStateContractStatus::NONE;
	string required_capability;
	string protocol_version;
	string blocker;
	string ir;
};

struct JitRegionNativeOperatorContract {
	JitRegionStateContractStatus status = JitRegionStateContractStatus::NONE;
	string required_capability;
	string protocol_version;
	string blocker;
	string ir;
};

struct JitRegionTableScanProtocol {
	bool present = false;
	string function_name;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	idx_t column_id_count = 0;
	idx_t projected_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	idx_t native_source_input_column_count = 0;
	vector<LogicalType> native_source_input_types;
	vector<idx_t> native_source_output_projection_map;
	vector<idx_t> native_source_filter_column_map;
	bool native_source_requires_unfiltered_input = false;
	bool native_source_filter_prune_required = false;
	bool native_source_filter_takeover_supported = false;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	idx_t filter_count = 0;
	string ir;
};

struct JitRegionHashJoinProtocol {
	bool present = false;
	JoinType join_type = JoinType::INVALID;
	idx_t condition_count = 0;
	idx_t equality_condition_count = 0;
	idx_t non_equality_condition_count = 0;
	idx_t null_equal_condition_count = 0;
	vector<LogicalType> condition_types;
	vector<ExpressionType> comparison_types;
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
	bool filter_pushdown = false;
	idx_t filter_pushdown_condition_count = 0;
	idx_t filter_pushdown_probe_count = 0;
	bool build_side_has_filter = false;
	bool source_produces_rows = false;
	bool regular_hash_table_layout_ready = false;
	bool found_match_column_present = false;
	bool native_probe_shape_ready = false;
	bool build_append_shape_ready = false;
	JitRegionHashJoinProbeOutputMode native_probe_output_mode = JitRegionHashJoinProbeOutputMode::NONE;
	idx_t layout_column_count = 0;
	vector<idx_t> layout_offsets;
	idx_t tuple_size = 0;
	idx_t entry_size = 0;
	idx_t pointer_offset = 0;
	idx_t hash_column_index = 0;
	idx_t found_match_column_index = 0;
	string native_protocol_blocker;
	string native_probe_shape_blocker;
	string build_append_shape_blocker;
	JitRegionNativeOperatorContract native_probe_contract;
	JitRegionNativeOperatorContract native_build_contract;
	string ir;
};

struct JitRegionAggregateProtocol {
	bool present = false;
	JitRegionAggregateOperatorKind kind = JitRegionAggregateOperatorKind::NONE;
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
	JitRegionNativeGroupedStateContract native_grouped_state_contract;
	JitRegionNativeOperatorContract native_hash_lookup_contract;
	string ir;
};

struct JitRegionAggregateInput {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
	idx_t child_count = 0;
	string function_name;
	LogicalType return_type;
	vector<LogicalType> child_types;
	vector<idx_t> child_indices;
	bool distinct = false;
	bool has_filter = false;
	bool has_order_bys = false;
	bool order_dependent = false;
	bool has_state_update = false;
	bool supported_payload_references = false;
	JitAggregateUpdateKind native_update = JitAggregateUpdateKind::NONE;
	LogicalType state_type;
	idx_t state_size = 0;
	bool state_is_optional = false;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;
	string reason;
	string ir;
};

struct JitRegionHashJoinKeyInput {
	idx_t key_index = 0;
	idx_t input_index = 0;
	LogicalType type;
	bool supported_reference = false;
	string reason;
	string ir;
};

struct JitRegionGroupInput {
	idx_t group_index = 0;
	idx_t input_index = 0;
	LogicalType type;
	bool supported_reference = false;
	string reason;
	string ir;
};

struct JitRegionNativeSourceContract {
	JitRegionNativeSourceStatus status = JitRegionNativeSourceStatus::NONE;
	string required_capability;
	string protocol_version;
	string blocker;
	string ir;
};

struct JitRegionSourceInfo {
	JitRegionSourceKind kind = JitRegionSourceKind::NONE;
	JitRegionSourceExecutionKind execution = JitRegionSourceExecutionKind::NONE;
	string function_name;
	vector<JitRegionProtocolField> fields;
	idx_t output_column_count = 0;
	idx_t returned_column_count = 0;
	vector<idx_t> column_ids;
	vector<idx_t> projection_ids;
	bool projection_pushdown = false;
	bool filter_pushdown = false;
	bool filter_prune = false;
	bool dynamic_filters = false;
	bool in_out_function = false;
	vector<JitRegionSourceFilter> filters;
	JitRegionTableScanProtocol table_scan_protocol;
	JitRegionHashJoinProtocol hash_join_protocol;
	JitRegionAggregateProtocol aggregate_protocol;
	vector<JitRegionAggregateInput> aggregates;
	vector<JitRegionHashJoinKeyInput> hash_join_keys;
	vector<JitRegionGroupInput> groups;
	JitRegionNativeSourceContract native_source_contract;
	JitRegionNativeStateScanContract native_state_scan_contract;
	string reason;
	string ir;
};

struct JitRegionOperatorInfo {
	JitRegionOperatorKind kind = JitRegionOperatorKind::NONE;
	vector<JitRegionProtocolField> fields;
	JitRegionHashJoinProtocol hash_join_protocol;
	vector<JitRegionHashJoinKeyInput> hash_join_keys;
	string reason;
	string ir;
};

struct JitRegionSinkInfo {
	JitRegionSinkKind kind = JitRegionSinkKind::NONE;
	vector<JitRegionProtocolField> fields;
	JitRegionHashJoinProtocol hash_join_protocol;
	JitRegionAggregateProtocol aggregate_protocol;
	vector<JitRegionAggregateInput> aggregates;
	vector<JitRegionHashJoinKeyInput> hash_join_keys;
	vector<JitRegionGroupInput> groups;
	string reason;
	string ir;
};

struct JitRegionContract {
	JitRegionABI abi = JitRegionABI::NONE;
	idx_t first_node = 0;
	idx_t node_count = 0;
	idx_t start_operator_index = 0;
	idx_t end_operator_index = 0;
	bool owns_source = false;
	bool owns_transform = false;
	bool owns_sink = false;
	bool owns_state_scan = false;
	JitRegionOwnershipKind source_ownership = JitRegionOwnershipKind::NONE;
	JitRegionOwnershipKind state_scan_ownership = JitRegionOwnershipKind::NONE;
	JitRegionOwnershipKind transform_ownership = JitRegionOwnershipKind::NONE;
	JitRegionOwnershipKind sink_ownership = JitRegionOwnershipKind::NONE;
	bool executor_boundary_free = false;
	bool native_fusion_ready = false;
	idx_t generated_operator_count = 0;
	idx_t source_boundary_count = 0;
	idx_t executor_boundary_count = 0;
	idx_t missing_protocol_count = 0;
	vector<string> required_capabilities;
	vector<string> blockers;
	string ir;
};

struct JitRegionCandidateTraits {
	bool has_source = false;
	bool has_sink = false;
	bool has_table_scan_source = false;
	bool has_stateful_source = false;
	bool expression_traits_known = false;
	JitRegionSourceKind source_kind = JitRegionSourceKind::NONE;
	JitRegionSourceExecutionKind source_execution = JitRegionSourceExecutionKind::NONE;
	JitRegionSinkKind sink_kind = JitRegionSinkKind::NONE;
	idx_t source_filter_count = 0;
	idx_t source_filter_expression_count = 0;
	idx_t source_filter_fallback_count = 0;
	idx_t source_comparison_filter_count = 0;
	idx_t source_integer_comparison_filter_count = 0;
	idx_t source_non_integer_comparison_filter_count = 0;
	idx_t source_conjunction_filter_count = 0;
	idx_t source_projected_column_count = 0;
	idx_t source_returned_column_count = 0;
	idx_t filter_count = 0;
	idx_t projection_count = 0;
	idx_t operator_count = 0;
	idx_t core_expression_operator_count = 0;
	idx_t arithmetic_projection_count = 0;
	idx_t integer_arithmetic_projection_count = 0;
	idx_t non_integer_arithmetic_projection_count = 0;
	idx_t reference_projection_count = 0;
	idx_t comparison_filter_count = 0;
	idx_t integer_comparison_filter_count = 0;
	idx_t non_integer_comparison_filter_count = 0;
	idx_t conjunction_filter_count = 0;
	idx_t expression_fallback_count = 0;
	idx_t operator_fallback_count = 0;
	idx_t operator_protocol_boundary_count = 0;
	idx_t resumable_operator_count = 0;
	idx_t scan_boundary_count = 0;
	idx_t sink_boundary_count = 0;
	string ir;
};

struct JitRegionIRNode {
	string role;
	string operator_name;
	idx_t operator_index = DConstants::INVALID_INDEX;
	JitCompiledOperatorContract compiled_contract;
	JitRegionIRNodeKind kind = JitRegionIRNodeKind::OPERATOR;
	vector<LogicalType> output_types;
	idx_t estimated_cardinality = 0;
	JitRegionVectorFormatKind input_format = JitRegionVectorFormatKind::NONE;
	JitRegionVectorFormatKind output_format = JitRegionVectorFormatKind::NONE;
	JitRegionVectorSourceKind vector_source = JitRegionVectorSourceKind::NONE;
	JitRegionSelectionSourceKind selection_source = JitRegionSelectionSourceKind::NONE;
	JitRegionBoundaryKind boundary = JitRegionBoundaryKind::NONE;
	unique_ptr<JitRegionSourceInfo> source;
	unique_ptr<JitRegionOperatorInfo> operator_info;
	unique_ptr<JitRegionSinkInfo> sink;
	unique_ptr<JitExpressionFragment> filter;
	vector<unique_ptr<JitExpressionFragment>> projections;
	string fallback_reason;
};

struct JitRegionSignature {
	string context;
	string shape;
	string feature_shape;
	string context_feature_shape;
	string ir;
};

struct JitRegionStage {
	JitRegionStageKind kind = JitRegionStageKind::OPERATOR_BOUNDARY;
	JitRegionStageExecutionKind execution = JitRegionStageExecutionKind::NONE;
	JitRegionOwnershipKind ownership = JitRegionOwnershipKind::NONE;
	JitCompiledProtocolKind protocol = JitCompiledProtocolKind::NONE;
	JitCompiledDrainKind drain = JitCompiledDrainKind::NONE;
	idx_t node_index = DConstants::INVALID_INDEX;
	idx_t operator_index = DConstants::INVALID_INDEX;
	idx_t filter_index = DConstants::INVALID_INDEX;
	string operator_name;
	string required_capability;
	string reason;
};

struct JitRegionStagePlan {
	vector<JitRegionStage> stages;
	string shape;
	string ir;

	bool HasStages() const {
		return !stages.empty();
	}
};

struct JitRegionCandidate {
	idx_t candidate_id = 0;
	idx_t first_node = 0;
	idx_t node_count = 0;
	idx_t start_operator_index = 0;
	idx_t end_operator_index = 0;
	idx_t estimated_cardinality = 0;
	JitRegionCandidateScope scope = JitRegionCandidateScope::FULL_PIPELINE;
	vector<LogicalType> input_types;
	vector<LogicalType> output_types;
	string shape;
	string pipeline_shape;
	string context_pipeline_shape;
	JitRegionCandidateTraits traits;
	JitRegionContract contract;
	JitRegionCandidateTraits upstream_traits;
	JitRegionCandidateTraits context_traits;
	JitRegionCandidateTraits continuation_traits;
	JitRegionSourceExecutionKind source_execution = JitRegionSourceExecutionKind::NONE;
	JitRegionSignature signature;
	JitRegionStagePlan stage_plan;
	string ir;

	idx_t EndNode() const {
		return first_node + node_count;
	}
};

struct JitRegionPipelineInventory {
	bool has_source = false;
	bool has_sink = false;
	bool has_scan_source = false;
	bool has_table_scan_source = false;
	bool has_stateful_source = false;
	bool source_produces_rows = true;
	JitRegionSourceKind source_kind = JitRegionSourceKind::NONE;
	JitRegionSourceExecutionKind source_execution = JitRegionSourceExecutionKind::NONE;
	JitRegionSinkKind sink_kind = JitRegionSinkKind::NONE;
	string source_operator_name;
	string sink_operator_name;
	vector<string> operator_names;
	vector<JitRegionBoundaryKind> operator_boundaries;
	idx_t estimated_cardinality = 0;
	idx_t source_filter_count = 0;
	idx_t source_projected_column_count = 0;
	idx_t source_returned_column_count = 0;
	idx_t operator_count = 0;
	idx_t filter_operator_count = 0;
	idx_t projection_operator_count = 0;
	bool has_filter_operator = false;
	bool has_projection_operator = false;
	bool has_hash_join_operator = false;
	bool has_hash_join_sink = false;
	bool has_hash_aggregate_sink = false;
	bool has_perfect_hash_aggregate_sink = false;
	bool has_ungrouped_aggregate_sink = false;
	string feature_shape;
	string pipeline_shape;
	string ir;
};

struct JitRegionIR {
	vector<JitRegionIRNode> nodes;
	vector<JitRegionCandidate> candidates;
	string pipeline_shape;
	string ir;
};

DUCKDB_API JitRegionNativeSourceContract BuildJitRegionNativeSourceContract(JitRegionSourceKind kind,
                                                                            JitRegionSourceExecutionKind execution);
DUCKDB_API string DescribeJitRegionSourceInfo(
    const JitRegionSourceInfo &source,
    JitRegionSourceExecutionKind execution = JitRegionSourceExecutionKind::NONE);

} // namespace duckdb

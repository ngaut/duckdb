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

#include <variant>

namespace duckdb {

static constexpr idx_t HIGH_COST_GENERATED_PROJECTION_EXPRESSION_COST = 1000;

struct ExecutionRegionExactFilterProof {
	idx_t source_input_index = DConstants::INVALID_INDEX;
	shared_ptr<ExecutionRuntimeFilterIdentity> identity;
};

struct ExecutionRegionSourceFilter {
	idx_t filter_index = 0;
	idx_t scan_column_index = 0;
	idx_t table_column_index = 0;
	unique_ptr<ExecutionExpressionFragment> expression;
	bool generated_source_stage_candidate = false;
	string reason;
};

struct ExecutionRegionGeneratedSourceFilterCapability {
	bool can_generate = false;
	string blocker;
};

DUCKDB_API ExecutionRegionGeneratedSourceFilterCapability GetExecutionRegionGeneratedSourceFilterCapability(
    const ExecutionExpressionFragment &expression, const LogicalType &source_type);

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
	bool not_null = false;
	LogicalType type;
};

struct ExecutionRegionNativeGroupedStateContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
};

//! Semantic description of one aggregate state exposed by a native state-scan
//! batch. Physical state offsets and hash-table layout remain core-owned.
struct ExecutionRegionPrimitiveAggregateStateLane {
	idx_t aggregate_index = DConstants::INVALID_INDEX;
	idx_t source_output_index = DConstants::INVALID_INDEX;
	LogicalType return_type;
	AggregatePrimitiveUpdateKind primitive_update_kind = AggregatePrimitiveUpdateKind::NONE;
};

struct ExecutionRegionNativeStateScanContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
	bool primitive_aggregate_batch_ready = false;
	vector<ExecutionRegionPrimitiveAggregateStateLane> primitive_aggregate_lanes;
	string primitive_aggregate_batch_blocker;
};

struct ExecutionRegionNativeOperatorContract {
	ExecutionRegionStateContractStatus status = ExecutionRegionStateContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
};

struct ExecutionRegionTableScanContract {
	bool present = false;
	string function_name;
	idx_t estimated_source_cardinality = 0;
	vector<LogicalType> source_contract_input_types;
	vector<bool> source_contract_input_not_null;
	vector<idx_t> source_contract_input_distinct_counts;
	vector<idx_t> source_contract_input_distinct_reserve_counts;
	vector<Value> source_contract_input_min_values;
	vector<Value> source_contract_input_max_values;
	vector<idx_t> source_contract_output_projection_map;
	bool filter_pushdown = false;
	bool dynamic_filters = false;
	bool finalized_dynamic_filter_cardinality_estimate = false;
	//! The table's checkpointed data is compressed (dictionary strings, bit-packed integers), so a storage scan
	//! filter can evaluate against the compressed form
	bool compressed_storage = false;
};

struct ExecutionRegionHashJoinContract {
	bool present = false;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	idx_t condition_count = 0;
	idx_t equality_condition_count = 0;
	idx_t non_equality_condition_count = 0;
	idx_t null_equal_condition_count = 0;
	vector<LogicalType> condition_types;
	vector<LogicalType> rhs_condition_types;
	vector<ExecutionRegionComparisonType> comparison_types;
	idx_t payload_column_count = 0;
	vector<idx_t> payload_column_indices;
	vector<LogicalType> payload_types;
	idx_t lhs_output_column_count = 0;
	vector<idx_t> lhs_output_column_indices;
	vector<LogicalType> lhs_output_types;
	idx_t rhs_output_column_count = 0;
	vector<idx_t> rhs_output_column_indices;
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
	bool native_probe_shape_ready = false;
	bool build_sink_shape_ready = false;
	ExecutionHashJoinProbeOutputMode native_probe_output_mode = ExecutionHashJoinProbeOutputMode::NONE;
	//! The probe's output can reach a sink that narrows global-dictionary payloads (a hash-join build, directly or via
	//! a delim join). Only then must compiled probes emit global-dictionary RHS columns exactly as native does.
	bool output_reaches_dictionary_narrowing_sink = false;
	//! Row layout of the build side as the sink publishes it. Per payload column, a non-zero dict-index width means
	//! the slot holds a dictionary index of that byte width (dict-surviving join) instead of the value.
	vector<uint8_t> payload_dict_index_widths;
	vector<idx_t> layout_offsets;
	idx_t tuple_size = 0;
	idx_t pointer_offset = 0;
	string native_contract_blocker;
	string native_probe_shape_blocker;
	string perfect_hash_probe_shape_blocker;
	string build_sink_shape_blocker;
	ExecutionRegionNativeOperatorContract native_probe_contract;
	ExecutionRegionNativeOperatorContract native_build_contract;
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
	ExecutionRegionNativeGroupedStateContract native_grouped_state_contract;
	ExecutionRegionNativeOperatorContract native_state_update_contract;
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
	//! FILTER (WHERE ...). An aggregate the contract admits updates its state as the same primitive update of its one
	//! child expression CASE WHEN filter THEN child END (count(*): COUNT of CASE WHEN filter THEN TRUE END), so no
	//! plain payload column backs it
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
};

struct ExecutionRegionHashJoinKeyInput {
	idx_t key_index = 0;
	idx_t input_index = 0;
	LogicalType type;
	bool supported_reference = false;
	//! A probe key computed from the input: backends evaluate it ahead of the probe
	bool expression_ready = false;
	ExecutionExpressionFragment expression;
	string reason;
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
};

struct ExecutionSourceProtocolContract {
	ExecutionRegionSourceContractStatus status = ExecutionRegionSourceContractStatus::NONE;
	string required_capability;
	string contract_version;
	string blocker;
};

struct ExecutionRegionSourceInfo {
	ExecutionRegionSourceKind kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE;
	string function_name;
	vector<ExecutionRegionContractField> fields;
	idx_t estimated_source_cardinality = 0;
	bool estimated_source_cardinality_exact = false;
	bool finalized_source_cardinality_required = false;
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
	vector<ExecutionRegionExactFilterProof> exact_filter_proofs;
	ExecutionRegionTableScanContract table_scan_contract;
	ExecutionRegionHashJoinContract hash_join_contract;
	ExecutionRegionNestedLoopJoinContract nested_loop_join_contract;
	ExecutionRegionOrderContract order_contract;
	vector<ExecutionRegionHashJoinKeyInput> hash_join_keys;
	ExecutionSourceProtocolContract source_contract;
	ExecutionRegionNativeStateScanContract native_state_scan_contract;
	string reason;
	string ir;
};

struct ExecutionRegionHashJoinInfo {
	ExecutionRegionHashJoinContract contract;
	vector<ExecutionRegionHashJoinKeyInput> keys;
};

class ExecutionRegionOperatorInfo {
public:
	explicit ExecutionRegionOperatorInfo(
	    ExecutionRegionOperatorContractKind kind = ExecutionRegionOperatorContractKind::NONE) {
		SetKind(kind);
	}

	ExecutionRegionOperatorContractKind Kind() const {
		return static_cast<ExecutionRegionOperatorContractKind>(payload.index());
	}

	void SetKind(ExecutionRegionOperatorContractKind kind) {
		switch (kind) {
		case ExecutionRegionOperatorContractKind::NONE:
			payload.emplace<std::monostate>();
			return;
		case ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE:
			payload.emplace<ExecutionRegionHashJoinInfo>();
			return;
		case ExecutionRegionOperatorContractKind::NESTED_LOOP_JOIN_PROBE:
			payload.emplace<ExecutionRegionNestedLoopJoinContract>();
			return;
		}
		throw InternalException("Unknown execution-region operator contract kind");
	}

	ExecutionRegionHashJoinInfo &HashJoin() {
		return std::get<ExecutionRegionHashJoinInfo>(payload);
	}
	const ExecutionRegionHashJoinInfo &HashJoin() const {
		return std::get<ExecutionRegionHashJoinInfo>(payload);
	}
	ExecutionRegionNestedLoopJoinContract &NestedLoopJoin() {
		return std::get<ExecutionRegionNestedLoopJoinContract>(payload);
	}
	const ExecutionRegionNestedLoopJoinContract &NestedLoopJoin() const {
		return std::get<ExecutionRegionNestedLoopJoinContract>(payload);
	}

	vector<ExecutionRegionContractField> fields;
	string reason;
	string ir;

private:
	std::variant<std::monostate, ExecutionRegionHashJoinInfo, ExecutionRegionNestedLoopJoinContract> payload;
};

struct ExecutionRegionAggregateSinkInfo {
	explicit ExecutionRegionAggregateSinkInfo(
	    ExecutionRegionAggregateOperatorKind kind = ExecutionRegionAggregateOperatorKind::NONE) {
		contract.kind = kind;
	}

	ExecutionRegionSinkKind Kind() const {
		switch (contract.kind) {
		case ExecutionRegionAggregateOperatorKind::HASH:
			return ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
		case ExecutionRegionAggregateOperatorKind::PERFECT_HASH:
			return ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
		case ExecutionRegionAggregateOperatorKind::UNGROUPED:
			return ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
		case ExecutionRegionAggregateOperatorKind::NONE:
			return ExecutionRegionSinkKind::NONE;
		}
		throw InternalException("Unknown execution-region aggregate contract kind");
	}

	ExecutionRegionAggregateContract contract;
	vector<ExecutionRegionAggregateInput> aggregates;
	vector<ExecutionRegionGroupInput> groups;
};

struct ExecutionRegionOrderSinkInfo {
	ExecutionRegionNativeOperatorContract native_contract;
	ExecutionRegionOrderContract contract;
};

struct ExecutionRegionDelimJoinSinkInfo {
	ExecutionRegionNativeOperatorContract native_contract;
	ExecutionRegionAggregateSinkInfo distinct;
	ExecutionRegionHashJoinInfo build;
};

class ExecutionRegionSinkInfo {
public:
	explicit ExecutionRegionSinkInfo(ExecutionRegionSinkKind kind = ExecutionRegionSinkKind::NONE) {
		SetKind(kind);
	}

	ExecutionRegionSinkKind Kind() const {
		return static_cast<ExecutionRegionSinkKind>(payload.index());
	}

	void SetKind(ExecutionRegionSinkKind kind) {
		switch (kind) {
		case ExecutionRegionSinkKind::NONE:
			payload.emplace<0>();
			return;
		case ExecutionRegionSinkKind::HASH_JOIN_BUILD:
			payload.emplace<1>();
			return;
		case ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD:
			payload.emplace<2>();
			return;
		case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
			payload.emplace<3>(ExecutionRegionAggregateOperatorKind::HASH);
			return;
		case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
			payload.emplace<4>(ExecutionRegionAggregateOperatorKind::PERFECT_HASH);
			return;
		case ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE:
			payload.emplace<5>(ExecutionRegionAggregateOperatorKind::UNGROUPED);
			return;
		case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
			payload.emplace<6>();
			return;
		case ExecutionRegionSinkKind::SORT:
			payload.emplace<7>();
			return;
		case ExecutionRegionSinkKind::MATERIALIZATION:
			payload.emplace<8>();
			return;
		case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
			payload.emplace<9>();
			return;
		}
		throw InternalException("Unknown execution-region sink contract kind");
	}

	ExecutionRegionHashJoinInfo &HashJoin() {
		return std::get<1>(payload);
	}
	const ExecutionRegionHashJoinInfo &HashJoin() const {
		return std::get<1>(payload);
	}
	ExecutionRegionNestedLoopJoinContract &NestedLoopJoin() {
		return std::get<2>(payload);
	}
	const ExecutionRegionNestedLoopJoinContract &NestedLoopJoin() const {
		return std::get<2>(payload);
	}
	ExecutionRegionAggregateSinkInfo &Aggregate() {
		switch (Kind()) {
		case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
			return std::get<3>(payload);
		case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
			return std::get<4>(payload);
		default:
			return std::get<5>(payload);
		}
	}
	const ExecutionRegionAggregateSinkInfo &Aggregate() const {
		switch (Kind()) {
		case ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE:
			return std::get<3>(payload);
		case ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE:
			return std::get<4>(payload);
		default:
			return std::get<5>(payload);
		}
	}
	ExecutionRegionOrderSinkInfo &Order() {
		return std::get<7>(payload);
	}
	const ExecutionRegionOrderSinkInfo &Order() const {
		return std::get<7>(payload);
	}
	ExecutionRegionDelimJoinSinkInfo &DelimJoin() {
		return std::get<9>(payload);
	}
	const ExecutionRegionDelimJoinSinkInfo &DelimJoin() const {
		return std::get<9>(payload);
	}
	ExecutionRegionNativeOperatorContract &Native() {
		switch (Kind()) {
		case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
			return std::get<6>(payload);
		case ExecutionRegionSinkKind::SORT:
			return Order().native_contract;
		case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
			return DelimJoin().native_contract;
		default:
			return std::get<8>(payload);
		}
	}
	const ExecutionRegionNativeOperatorContract &Native() const {
		switch (Kind()) {
		case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
			return std::get<6>(payload);
		case ExecutionRegionSinkKind::SORT:
			return Order().native_contract;
		case ExecutionRegionSinkKind::DELIM_JOIN_SINK:
			return DelimJoin().native_contract;
		default:
			return std::get<8>(payload);
		}
	}

	vector<ExecutionRegionContractField> fields;
	string reason;
	string ir;

private:
	std::variant<std::monostate, ExecutionRegionHashJoinInfo, ExecutionRegionNestedLoopJoinContract,
	             ExecutionRegionAggregateSinkInfo, ExecutionRegionAggregateSinkInfo, ExecutionRegionAggregateSinkInfo,
	             ExecutionRegionNativeOperatorContract, ExecutionRegionOrderSinkInfo,
	             ExecutionRegionNativeOperatorContract, ExecutionRegionDelimJoinSinkInfo>
	    payload;
};

struct ExecutionRegionCandidateTraits {
	ExecutionRegionSourceKind source_kind = ExecutionRegionSourceKind::NONE;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSinkKind sink_kind = ExecutionRegionSinkKind::NONE;
	bool sink_present = false;
	idx_t source_contract_input_cardinality = 0;
	bool source_contract_output_cardinality_unknown = false;
	bool finalized_dynamic_filter_cardinality_estimate = false;
	idx_t source_filter_count = 0;
	idx_t source_filter_expression_count = 0;
	idx_t filter_count = 0;
	idx_t projection_count = 0;
	idx_t operator_count = 0;
	idx_t hash_join_operator_count = 0;
	idx_t mark_probe_filter_count = 0;
	idx_t selected_hash_join_filter_materialization_count = 0;
	idx_t selected_hash_join_view_materialization_count = 0;
	//! Hash join probes whose output is materialized (a LEFT probe must emit its unmatched rows)
	idx_t materialized_hash_join_output_count = 0;
	//! Aggregate sinks whose update the generated body performs. A grouped one replaces native group lookup work; an
	//! ungrouped one does the native update's work and only gains from the filters, projections and probes it fuses
	//! with. A sink whose update stays native is native work.
	idx_t generated_aggregate_update_count = 0;
	idx_t generated_aggregate_lookup_count = 0;
	//! The aggregate FILTERs those generated updates evaluate, counted in filter_count
	idx_t generated_aggregate_filter_count = 0;
	idx_t grouped_aggregate_estimated_cardinality = 0;
	idx_t grouped_aggregate_key_count = 0;
	idx_t arithmetic_projection_count = 0;
	idx_t high_cost_projection_count = 0;
	idx_t reference_projection_count = 0;
	idx_t predicate_expression_count = 0;
	idx_t control_expression_count = 0;
	idx_t expression_cost = 0;

	bool HasSource() const {
		return source_kind != ExecutionRegionSourceKind::NONE;
	}

	bool HasSink() const {
		return sink_present;
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
	bool estimated_cardinality_exact = false;
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
	//! Why the node is a boundary, for diagnostics only; the boundary kind is the status
	string blocker_reason;

	//! Whether a filter or projection node's expressions lowered to region IR
	bool ExpressionsLowered() const {
		return boundary != ExecutionRegionBoundaryKind::EXPRESSION_MISSING;
	}
};

struct ExecutionRegionSignature {
	string feature_shape;
	string contract_shape;
};

struct ExecutionRegionStage {
	ExecutionRegionStageKind kind = ExecutionRegionStageKind::OPERATOR_BOUNDARY;
	ExecutionRegionStageExecutionKind execution = ExecutionRegionStageExecutionKind::NONE;
	ExecutionCompiledContractKind operation = ExecutionCompiledContractKind::NONE;
	ExecutionCompiledDrainKind drain = ExecutionCompiledDrainKind::NONE;
	bool executable_work = false;
	idx_t expression_cost = 0;
	idx_t node_index = DConstants::INVALID_INDEX;
	idx_t operator_index = DConstants::INVALID_INDEX;
	idx_t filter_index = DConstants::INVALID_INDEX;
	string operator_name;
	string required_capability;
	string reason;
};

struct ExecutionRegionStagePlan {
	vector<ExecutionRegionStage> stages;
	string ir;

	bool HasStages() const {
		return !stages.empty();
	}

	bool HasExecutableWork() const {
		for (auto &stage : stages) {
			if (stage.executable_work && stage.execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
			    stage.execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY) {
				return true;
			}
		}
		return false;
	}

	idx_t CountExecution(ExecutionRegionStageExecutionKind execution) const {
		idx_t count = 0;
		for (auto &stage : stages) {
			count += stage.execution == execution;
		}
		return count;
	}

	idx_t SourceBoundaryCount() const {
		return CountExecution(ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY);
	}

	idx_t MissingContractCount() const {
		return CountExecution(ExecutionRegionStageExecutionKind::MISSING_CONTRACT);
	}
};

struct ExecutionRegionCandidate {
	idx_t estimated_cardinality = 0;
	string shape;
	ExecutionRegionCandidateTraits traits;
	ExecutionRegionSignature signature;
	ExecutionRegionStagePlan stage_plan;
};

DUCKDB_API string ExecutionRegionAggregateNativeStateUpdateBlocker(
    const ExecutionRegionAggregateContract &contract, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups);
DUCKDB_API bool ExecutionRegionAggregateUpdateGeneratesBody(const ExecutionRegionSinkInfo &sink);
DUCKDB_API bool ExecutionRegionAggregateLookupGeneratesBody(const ExecutionRegionSinkInfo &sink);
//! The aggregate FILTERs of an aggregate update sink: a generated update evaluates each per row, the work of a filter
DUCKDB_API idx_t ExecutionRegionAggregateFilterCount(const ExecutionRegionSinkInfo &sink);

struct ExecutionRegionIR {
	vector<ExecutionRegionNode> nodes;
	unique_ptr<ExecutionRegionCandidate> candidate;
	string candidate_blocker;
	string pipeline_shape;
	string ir;
};

DUCKDB_API ExecutionSourceProtocolContract
BuildExecutionSourceProtocolContract(ExecutionRegionSourceKind kind, ExecutionRegionSourceExecutionKind execution);
DUCKDB_API string DescribeExecutionRegionSourceInfo(
    const ExecutionRegionSourceInfo &source,
    ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE);
//! Every contract fact of an operator or sink, without diagnostic fields. A backend that shares compiled artifacts
//! across queries keys them on these, so an artifact never runs against a contract it was not compiled for.
DUCKDB_API string DescribeExecutionRegionOperatorContract(const ExecutionRegionOperatorInfo &operator_info);
DUCKDB_API string DescribeExecutionRegionSinkContract(const ExecutionRegionSinkInfo &sink);
DUCKDB_API string DescribeExecutionRegionStateScanContract(const ExecutionRegionNativeStateScanContract &contract);

} // namespace duckdb

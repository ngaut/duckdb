//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/execution_region_backend.hpp"

namespace duckdb {

enum class SljitNativeRegionOpKind : uint8_t {
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
	HASH_JOIN_BUILD,
	NESTED_LOOP_JOIN_PROBE,
	NESTED_LOOP_JOIN_BUILD,
	ORDER_SINK,
	APPEND_SINK,
	DELIM_JOIN_SINK,
	AGGREGATE_UPDATE
};
enum class SljitNativeRegionExpressionKind : uint8_t {
	REFERENCE,
	CONSTANT,
	INTEGER_BINARY_CONSTANT,
	INTEGER_BINARY_REFERENCES,
	DOUBLE_BINARY_CONSTANT,
	DOUBLE_BINARY_REFERENCES,
	INTEGER_COMPARE_CONSTANT,
	INTEGER_COMPARE_REFERENCES,
	INTEGER_CAST,
	SIGNED_TO_UNSIGNED_INTEGER_CAST,
	INTEGER_COALESCE,
	INTEGER_IN_LIST,
	INTEGER_BETWEEN,
	DECIMAL64_TO_DOUBLE,
	DECIMAL128_SCALE_UP,
	CONSTANT_OR_NULL,
	INTEGRAL_COMPRESS,
	INTEGRAL_DECOMPRESS,
	STRING_COMPRESS,
	STRING_DECOMPRESS,
	DATE_YEAR,
	ERROR_GUARDED_REFERENCE,
	NULL_CHECK,
	PREDICATE,
	EXPRESSION_TREE,
	TYPED_EXPRESSION_TREE
};

struct SljitNativeRegionExpressionPlan;

struct SljitNativeHashJoinProbeKeyPlan {
	idx_t key_input_index = DConstants::INVALID_INDEX;
	idx_t key_layout_offset = 0;
	LogicalType key_type;
	SljitNativeHashJoinKeyKind key_kind = SljitNativeHashJoinKeyKind::INT64;
	ExecutionRegionComparisonType comparison_type = ExecutionRegionComparisonType::EQUAL;
	bool equality_key = true;
	bool null_equal = false;
};

struct SljitNativeRegionExpressionPlan {
	SljitNativeRegionExpressionKind kind = SljitNativeRegionExpressionKind::REFERENCE;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	LogicalType return_type;
	Value constant_value;
	idx_t source_index = 0;
	idx_t right_source_index = 0;
	int64_t constant = 0;
	int64_t result_min = 0;
	int64_t result_max = 0;
	bool constant_on_left = false;
	bool check_result_range = false;
	SljitNativeIntegerBinaryOp binary_op = SljitNativeIntegerBinaryOp::ADD;
	SljitNativeDoubleBinaryOp double_binary_op = SljitNativeDoubleBinaryOp::DIVIDE;
	SljitNativeDoubleSourceKind double_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
	SljitNativeDoubleSourceKind double_right_source_kind = SljitNativeDoubleSourceKind::DOUBLE;
	double double_source_scale = 1;
	double double_right_source_scale = 1;
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeSignedIntegerWidth cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeUnsignedIntegerWidth unsigned_source_width = SljitNativeUnsignedIntegerWidth::UINT8;
	SljitNativeUnsignedIntegerWidth unsigned_cast_target_width = SljitNativeUnsignedIntegerWidth::UINT16;
	idx_t string_compress_target_size = 0;
	idx_t string_decompress_source_size = 0;
	idx_t guard_source_index = 0;
	SljitNativeIntegerCompareOp guard_compare_op = SljitNativeIntegerCompareOp::GREATER_THAN;
	int64_t guard_constant = 0;
	bool guard_constant_on_left = false;
	idx_t guarded_value_size = 0;
	string error_message;
	bool try_cast = false;
	double double_constant = 0;
	SljitNativeSignedIntegerWidth signed_integer_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeCoalesceRhsKind coalesce_rhs_kind = SljitNativeCoalesceRhsKind::CONSTANT;
	bool coalesce_constant_is_null = false;
	SljitNativeNullCheckOp null_check_op = SljitNativeNullCheckOp::IS_NULL;
	vector<int64_t> constants;
	int64_t lower = 0;
	int64_t upper = 0;
	bool list_has_null = false;
	bool not_in = false;
	bool not_between = false;
	bool lower_inclusive = true;
	bool upper_inclusive = true;
	unique_ptr<SljitNativePredicate> predicate;
	unique_ptr<ExecutionExpressionIR> expression_tree;
	vector<idx_t> expression_tree_source_indices;
	SljitNativeConstantOrNull constant_or_null;
	string ir;
};

struct SljitNativeHashJoinProbePlan {
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	idx_t equality_key_count = 0;
	bool mark_build_match = false;
	bool mark_build_match_after_residual = false;
	bool residual_predicate = false;
	bool perfect_hash_probe = false;
	idx_t found_match_offset = 0;
	idx_t pointer_offset = 0;
	ExecutionHashJoinProbeOutputMode output_mode = ExecutionHashJoinProbeOutputMode::NONE;
	vector<LogicalType> input_types;
	vector<LogicalType> residual_source_types;
	SljitNativeRegionExpressionPlan residual_filter;
	ExecutionRegionOperatorInfo operator_info;
	string ir;
};

struct SljitNativeHashJoinBuildPlan {
	ExecutionRegionSinkInfo sink_info;
	vector<LogicalType> input_types;
	string ir;
};

struct SljitNativeNestedLoopJoinProbeConditionPlan {
	SljitNativeRegionExpressionPlan lhs_condition;
	LogicalType type;
	ExecutionRegionComparisonType comparison_type = ExecutionRegionComparisonType::INVALID;
	SljitNativeNestedLoopJoinValueKind value_kind = SljitNativeNestedLoopJoinValueKind::INT64;
	string ir;
};

struct SljitNativeNestedLoopJoinProbePlan {
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<SljitNativeNestedLoopJoinProbeConditionPlan> conditions;
	vector<LogicalType> input_types;
	vector<LogicalType> condition_types;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	ExecutionRegionOperatorInfo operator_info;
	string ir;
};

struct SljitNativeNestedLoopJoinBuildPlan {
	ExecutionRegionSinkInfo sink_info;
	vector<SljitNativeRegionExpressionPlan> rhs_conditions;
	vector<LogicalType> input_types;
	vector<LogicalType> condition_types;
	string ir;
};

struct SljitNativeAppendSinkPlan {
	ExecutionRegionSinkInfo sink_info;
	vector<LogicalType> input_types;
	string ir;
};

struct SljitNativeDelimJoinSinkPlan {
	ExecutionRegionSinkInfo sink_info;
	vector<LogicalType> input_types;
	string ir;
};

struct SljitNativeOrderSinkPlan {
	ExecutionRegionSinkInfo sink_info;
	vector<SljitNativeRegionExpressionPlan> order_keys;
	vector<LogicalType> input_types;
	vector<LogicalType> key_types;
	string ir;
};

struct SljitNativeAggregateUpdatePlan {
	ExecutionRegionSinkInfo sink_info;
	vector<LogicalType> input_types;
	vector<SljitNativeRegionExpressionPlan> payloads;
	bool use_primitive_payloads = false;
	bool use_grouped_state_addresses = false;
	bool use_perfect_hash_group_lookup = false;
	string ir;
};

struct SljitNativeRegionOpPlan {
	SljitNativeRegionOpKind kind;
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<LogicalType> output_types;
	SljitNativeRegionExpressionPlan filter;
	SljitNativeHashJoinProbePlan hash_join_probe;
	SljitNativeHashJoinBuildPlan hash_join_build;
	SljitNativeNestedLoopJoinProbePlan nested_loop_join_probe;
	SljitNativeNestedLoopJoinBuildPlan nested_loop_join_build;
	SljitNativeAppendSinkPlan append_sink;
	SljitNativeDelimJoinSinkPlan delim_join_sink;
	SljitNativeOrderSinkPlan order_sink;
	SljitNativeAggregateUpdatePlan aggregate_update;
	vector<SljitNativeRegionExpressionPlan> projections;
	bool use_vectorized_projection = false;
};

struct SljitNativeExpressionIntensity {
	idx_t max_nodes = 0;
	idx_t total_nodes = 0;
	idx_t total_arithmetic_nodes = 0;
	idx_t total_predicate_nodes = 0;
	idx_t total_control_nodes = 0;
	idx_t total_null_control_nodes = 0;
	idx_t total_helper_nodes = 0;
	idx_t max_straight_line_arithmetic_nodes = 0;
	idx_t max_branchy_arithmetic_nodes = 0;
	idx_t straight_line_arithmetic_expression_count = 0;
	idx_t branchy_arithmetic_expression_count = 0;
	idx_t expression_count = 0;
};

struct SljitNativeStringPredicateIntensity {
	idx_t total_predicate_nodes = 0;
	idx_t string_predicate_nodes = 0;
	idx_t string_like_nodes = 0;
	idx_t string_contains_nodes = 0;
	idx_t string_prefix_nodes = 0;
	idx_t string_suffix_nodes = 0;
	idx_t string_equality_nodes = 0;
	idx_t string_in_list_nodes = 0;
	idx_t string_substring_in_list_nodes = 0;
	idx_t conjunction_nodes = 0;
	idx_t not_nodes = 0;

	bool HasStringPredicate() const {
		return string_predicate_nodes > 0;
	}

	bool HasHighCostPatternPredicate() const {
		return string_like_nodes > 0 || string_contains_nodes > 0 || string_substring_in_list_nodes > 0;
	}
};

struct SljitNativeRegionSummary {
	bool finalized = false;
	string native_shape;
	bool generates_code = false;
	bool generates_machine_code = false;
	bool has_whole_operator_boundary_stage = false;
	string whole_operator_boundary_blocker;
	bool has_operator_contract_loop = false;
	bool has_native_protocol_body = false;
	ExecutionRegionExecutionBody execution_body = ExecutionRegionExecutionBody::NONE;
	SljitNativeExpressionIntensity expression_intensity;
	SljitNativeStringPredicateIntensity string_predicate_intensity;
};

struct SljitNativeRegionPlan {
	vector<SljitNativeRegionOpPlan> ops;
	idx_t elided_identity_projections = 0;
	idx_t fused_projection_chains = 0;
	idx_t fused_arithmetic_projection_chains = 0;
	idx_t fused_primitive_aggregate_updates = 0;
	idx_t runtime_combined_filter_projections = 0;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	SljitNativeRegionSummary summary;

	const vector<LogicalType> &OutputTypes() const {
		D_ASSERT(!ops.empty());
		return ops.back().output_types;
	}

	bool UsesSourceContract() const {
		return source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}
};

struct SljitOperatorStageRegionPlan {
	vector<ExecutionRegionStage> stages;
	string shape_key;
	string stage_ir;
	bool stage_plan_valid = false;
	string kernel_kind;
	string kernel_blocker;

	bool IsValid() const {
		return stage_plan_valid;
	}

	bool UsesSourceContract() const {
		for (auto &stage : stages) {
			if (stage.kind == ExecutionRegionStageKind::SOURCE &&
			    stage.execution == ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
				return true;
			}
		}
		return false;
	}

	bool OwnsSource() const {
		for (auto &stage : stages) {
			if (stage.kind == ExecutionRegionStageKind::SOURCE) {
				return true;
			}
		}
		return false;
	}

	bool OwnsSink() const {
		for (auto &stage : stages) {
			switch (stage.kind) {
			case ExecutionRegionStageKind::HASH_JOIN_BUILD:
			case ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD:
			case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::HASH_AGGREGATE_DISTINCT_SINK:
			case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
			case ExecutionRegionStageKind::APPEND_SINK:
			case ExecutionRegionStageKind::SORT_SINK:
			case ExecutionRegionStageKind::DELIM_JOIN_SINK:
			case ExecutionRegionStageKind::SINK_BOUNDARY:
				return true;
			default:
				break;
			}
		}
		return false;
	}
};

struct SljitRegionBackendPlan : public ExecutionRegionBackendPlan {
	unique_ptr<SljitNativeRegionPlan> native_region;
	string error;
};

struct SljitRegionPlan {
	ExecutionRegionLoweringPlan lowering_plan;
	shared_ptr<SljitRegionBackendPlan> backend_plan;
};

SljitNativeRegionExpressionPlan CopySljitNativeRegionExpression(const SljitNativeRegionExpressionPlan &input);
SljitNativeHashJoinProbePlan CopySljitNativeHashJoinProbePlan(const SljitNativeHashJoinProbePlan &input);
unique_ptr<SljitNativeRegionPlan> CopySljitNativeRegion(const SljitNativeRegionPlan &input);

SljitOperatorStageRegionPlan BuildSljitOperatorStageRegionPlan(const SljitNativeRegionPlan &region,
                                                               const ExecutionRegionContract &contract,
                                                               const ExecutionRegionStagePlan &core_stage_plan);
ExecutionRegionExecutionBody SljitNativeRegionExecutionBody(const SljitNativeRegionPlan &region,
                                                            const ExecutionRegionContract &contract);
string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode);
string DescribeNativeRegionShape(const SljitNativeRegionPlan &region);
SljitRegionPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir, const ExecutionRegionCandidate &candidate);

} // namespace duckdb

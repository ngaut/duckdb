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
	bool check_arithmetic_overflow = true;
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
	optional_idx query_location;
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
	bool references_region_input = true;
	bool emit_flat_nullable_fast_path = true;
	string ir;

	SljitNativeRegionExpressionPlan Copy(bool copy_auxiliary_expression_tree = true, bool copy_ir = true) const;
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
	vector<bool> residual_source_not_null;
	SljitNativeRegionExpressionPlan residual_filter;
	ExecutionRegionOperatorInfo operator_info;
	string ir;

	SljitNativeHashJoinProbePlan Copy(bool copy_ir = true) const;
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
	vector<SljitNativeRegionExpressionPlan> group_expressions;
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
};

struct SljitNativeRegionPlan {
	vector<SljitNativeRegionOpPlan> ops;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	vector<bool> source_not_null;

	const vector<LogicalType> &OutputTypes() const {
		D_ASSERT(!ops.empty());
		return ops.back().output_types;
	}

	bool UsesSourceContract() const {
		return source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}

	unique_ptr<SljitNativeRegionPlan> Copy() const;
};

struct SljitRegionBackendPlan : public ExecutionRegionBackendPlan {
	unique_ptr<SljitNativeRegionPlan> native_region;
	string error;
};

string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode);
string DescribeNativeRegionShape(const SljitNativeRegionPlan &region);
ExecutionRegionLoweringPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate,
                                                 bool render_diagnostics = false);

} // namespace duckdb

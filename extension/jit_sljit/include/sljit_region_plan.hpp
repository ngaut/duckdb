//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_plan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

#include "duckdb/execution/jit/aggregate_runtime.hpp"
#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

enum class SljitNativeRegionOpKind : uint8_t {
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
	HASH_JOIN_BUILD,
	HASH_AGGREGATE_UPDATE,
	PERFECT_HASH_AGGREGATE_UPDATE,
	UNGROUPED_AGGREGATE_UPDATE,
	RESULT_COLLECTOR_APPEND
};
enum class SljitSourceFilterExecutionKind : uint8_t {
	NONE,
	DUCKDB_SCAN,
	GENERATED_REGION
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
	CONSTANT_OR_NULL,
	INTEGRAL_COMPRESS,
	INTEGRAL_DECOMPRESS,
	STRING_COMPRESS_UINT8,
	DATE_YEAR,
	NULL_CHECK,
	PREDICATE
};

struct SljitNativeUngroupedAggregateUpdatePlan {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
	JitAggregateUpdateKind update_kind = JitAggregateUpdateKind::NONE;
	LogicalType payload_type;
	LogicalType state_type;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;
	string ir;
};

struct SljitNativeGroupedAggregateUpdatePlan {
	idx_t aggregate_index = 0;
	idx_t payload_index = 0;
	idx_t aggregate_state_offset = 0;
	JitAggregateUpdateKind update_kind = JitAggregateUpdateKind::NONE;
	LogicalType payload_type;
	LogicalType state_type;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;
	string ir;
};

struct SljitNativeHashJoinProbeKeyPlan {
	idx_t key_input_index = DConstants::INVALID_INDEX;
	idx_t key_layout_offset = 0;
	LogicalType key_type;
	SljitNativeHashJoinKeyKind key_kind = SljitNativeHashJoinKeyKind::INT64;
	ExpressionType comparison_type = ExpressionType::COMPARE_EQUAL;
	bool equality_key = true;
	bool null_equal = false;
};

struct SljitNativeHashJoinProbePlan {
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	idx_t equality_key_count = 0;
	bool mark_build_match = false;
	idx_t found_match_offset = 0;
	idx_t pointer_offset = 0;
	JitRegionHashJoinProbeOutputMode output_mode = JitRegionHashJoinProbeOutputMode::NONE;
	vector<LogicalType> input_types;
	JitRegionOperatorInfo operator_info;
	string ir;
};

struct SljitNativeHashJoinBuildPlan {
	JitRegionSinkInfo sink_info;
	vector<LogicalType> input_types;
	string ir;
};

struct SljitNativeRegionExpressionPlan {
	SljitNativeRegionExpressionKind kind;
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
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeSignedIntegerWidth cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeUnsignedIntegerWidth unsigned_source_width = SljitNativeUnsignedIntegerWidth::UINT8;
	SljitNativeUnsignedIntegerWidth unsigned_cast_target_width = SljitNativeUnsignedIntegerWidth::UINT16;
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
	SljitNativeConstantOrNull constant_or_null;
	string ir;
};

struct SljitNativeRegionOpPlan {
	SljitNativeRegionOpKind kind;
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<LogicalType> output_types;
	SljitNativeRegionExpressionPlan filter;
	SljitNativeHashJoinProbePlan hash_join_probe;
	SljitNativeHashJoinBuildPlan hash_join_build;
	vector<SljitNativeRegionExpressionPlan> projections;
	vector<JitUngroupedAggregatePayloadBinding> aggregate_payloads;
	vector<SljitNativeUngroupedAggregateUpdatePlan> native_ungrouped_aggregate_updates;
	vector<SljitNativeGroupedAggregateUpdatePlan> native_grouped_aggregate_updates;
	vector<JitGroupedAggregatePayloadBinding> grouped_aggregate_payloads;
	vector<JitGroupedAggregateGroupBinding> grouped_aggregate_groups;
	vector<idx_t> perfect_hash_required_bits;
	vector<Value> perfect_hash_group_minima;
};

struct SljitNativeRegionPlan {
	vector<SljitNativeRegionOpPlan> ops;
	idx_t elided_identity_projections = 0;
	idx_t fused_projection_chains = 0;
	idx_t fused_arithmetic_projection_chains = 0;
	idx_t runtime_combined_filter_projections = 0;
	idx_t source_filter_count = 0;
	SljitSourceFilterExecutionKind source_filter_execution = SljitSourceFilterExecutionKind::NONE;
	bool native_source = false;

	const vector<LogicalType> &OutputTypes() const {
		D_ASSERT(!ops.empty());
		return ops.back().output_types;
	}
};

struct SljitOperatorStageRegionPlan {
	vector<JitRegionStage> stages;
	string shape_key;
	string execution_reason;
	string stage_ir;
	bool native_source = false;
	bool owns_source = false;
	bool owns_transform = false;
	bool owns_sink = false;
	bool stage_plan_valid = false;
	bool native_operator_loop = false;
	string kernel_blocker;

	bool IsValid() const {
		return stage_plan_valid;
	}

	bool HasExecutableBody() const {
		return native_operator_loop;
	}
};

struct SljitRegionBackendPlan : public JitBackendPlan {
	unique_ptr<SljitNativeRegionPlan> native_region;
	string error;
};

struct SljitRegionPlan {
	JitRegionLoweringPlan lowering_plan;
	shared_ptr<SljitRegionBackendPlan> backend_plan;
};

SljitNativeRegionExpressionPlan
CopySljitNativeRegionExpression(const SljitNativeRegionExpressionPlan &input);
unique_ptr<SljitNativeRegionPlan> CopySljitNativeRegion(const SljitNativeRegionPlan &input);

SljitOperatorStageRegionPlan BuildSljitOperatorStageRegionPlan(const SljitNativeRegionPlan &region,
                                                               const JitRegionContract &contract,
                                                               const JitRegionStagePlan &core_stage_plan);
string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode);
string DescribeNativeRegionShape(const SljitNativeRegionPlan &region);
string BuildSljitRegionCandidateShapeKey(const JitRegionCandidate &candidate);
string BuildSljitRegionCandidateContextShapeKey(const JitRegionCandidate &candidate, const string &shape_key);
SljitRegionPlan BuildSljitRegionPlan(const JitRegionIR &region_ir, const JitRegionCandidate &candidate);

} // namespace duckdb

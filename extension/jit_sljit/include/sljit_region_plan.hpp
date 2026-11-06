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

#include <variant>

namespace duckdb {

enum class SljitNativeRegionOpKind : uint8_t {
	FILTER,
	PROJECTION,
	HASH_JOIN_PROBE,
	HASH_JOIN_BUILD,
	ORDER_SINK,
	APPEND_SINK,
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
	DECIMAL128_WIDENING_MULTIPLY,
	INTEGRAL_COMPRESS,
	INTEGRAL_DECOMPRESS,
	STRING_COMPRESS,
	STRING_DECOMPRESS,
	STRING_SUBSTRING,
	DATE_YEAR,
	ERROR_GUARDED_REFERENCE,
	NULL_CHECK,
	PREDICATE,
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
	bool source_known_not_null = false;
	//! A key read from the BIGINT source of an elided INTEGER cast: that cast, which raises a failed narrowing's error
	SljitIntegerCastChain source_cast;
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
	//! the scale of a decimal double source, 0 for any other source kind
	uint32_t double_source_decimal_scale = 0;
	uint32_t double_right_source_decimal_scale = 0;
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeSignedIntegerWidth cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth right_cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeUnsignedIntegerWidth unsigned_source_width = SljitNativeUnsignedIntegerWidth::UINT8;
	SljitNativeUnsignedIntegerWidth unsigned_cast_target_width = SljitNativeUnsignedIntegerWidth::UINT16;
	//! INTEGER_CAST and SIGNED_TO_UNSIGNED_INTEGER_CAST: the bound casts, which raise the cast's error
	SljitIntegerCastChain cast_chain;
	idx_t string_compress_target_size = 0;
	idx_t string_decompress_source_size = 0;
	idx_t string_substring_length = 0;
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
	bool references_region_input = true;
	bool emit_flat_nullable_fast_path = true;
	string ir;

	SljitNativeRegionExpressionPlan Copy(bool copy_auxiliary_expression_tree = true, bool copy_ir = true) const;
	SljitIntegerOverflowChecks OverflowChecks() const {
		return {check_arithmetic_overflow, check_result_range, result_min, result_max};
	}
};

//! A static single-column table filter compiled for execution inside DuckDB's
//! canonical storage scan. The filter index is the immutable source-contract
//! identity; the expression is normalized to a one-column input layout.
struct SljitNativeScanFilterPlan {
	idx_t filter_index = DConstants::INVALID_INDEX;
	LogicalType input_type;
	bool input_not_null = false;
	SljitNativeRegionExpressionPlan filter;
};

struct SljitNativeHashJoinProbePlan {
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<SljitNativeHashJoinProbeKeyPlan> keys;
	idx_t equality_key_count = 0;
	bool mark_build_match = false;
	bool mark_build_match_after_residual = false;
	bool residual_predicate = false;
	bool perfect_hash_probe = false;
	//! Query-local proof identity binds through this stable executable-op slot.
	//! The shared executable artifact never owns the identity itself.
	idx_t exact_source_filter_binding = DConstants::INVALID_INDEX;
	shared_ptr<ExecutionRuntimeFilterIdentity> exact_source_filter_identity;
	idx_t found_match_offset = 0;
	idx_t pointer_offset = 0;
	ExecutionHashJoinProbeOutputMode output_mode = ExecutionHashJoinProbeOutputMode::NONE;
	vector<LogicalType> input_types;
	vector<LogicalType> residual_source_types;
	vector<bool> residual_source_not_null;
	SljitNativeRegionExpressionPlan residual_filter;
	ExecutionRegionOperatorInfo operator_info {ExecutionRegionOperatorContractKind::HASH_JOIN_PROBE};

	//! A probe-only (SEMI) row is decided by its first key match unless the residual predicate can still reject that
	//! match: then the probe enumerates every key match and keeps each row's first match that passes the residual
	bool ResidualDecidesProbeRows() const {
		return output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY && residual_predicate;
	}

	SljitNativeHashJoinProbePlan Copy(bool copy_ir = true) const;
};

struct SljitNativeHashJoinBuildPlan {
	ExecutionRegionHashJoinInfo sink_info;
	vector<LogicalType> input_types;
};

struct SljitNativeAppendSinkPlan {
	ExecutionRegionSinkInfo sink_info {ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK};
	vector<LogicalType> input_types;
};

struct SljitNativeOrderSinkPlan {
	ExecutionRegionOrderSinkInfo sink_info;
	vector<SljitNativeRegionExpressionPlan> order_keys;
	vector<LogicalType> input_types;
	vector<LogicalType> key_types;
};

struct SljitAggregateInstanceParameters {
	idx_t estimated_input_count = 0;
	idx_t group_reserve_count = 0;
};

enum class SljitAggregatePayloadBindingState : uint8_t {
	UNBOUND,
	DIRECT,
	PROJECTION_COMPOSED,
};

struct SljitNativeAggregateUpdatePlan {
	ExecutionRegionAggregateSinkInfo sink_info {ExecutionRegionAggregateOperatorKind::UNGROUPED};
	//! The layout the region graph FEEDS this aggregate (compiled updates bind
	//! against it). NOT the delegated physical sink's expectation: delegation
	//! dereferences sink_info's reference declarations, which can differ when
	//! input transforms (payload compute, perfect-hash group compression) are
	//! fused into the compiled representation. DeclaredInputConstraints() is the
	//! single authority for what execution actually touches.
	vector<LogicalType> input_types;
	vector<SljitNativeRegionExpressionPlan> payloads;
	vector<SljitNativeRegionExpressionPlan> group_expressions;
	SljitAggregatePayloadBindingState payload_binding_state = SljitAggregatePayloadBindingState::UNBOUND;
	bool use_grouped_state_addresses = false;
	bool use_perfect_hash_group_lookup = false;
	string ir;

	bool UsesPrimitivePayloads() const {
		return payload_binding_state != SljitAggregatePayloadBindingState::UNBOUND;
	}

	bool CanInitializePrimitivePayloads() const {
		return payload_binding_state == SljitAggregatePayloadBindingState::UNBOUND;
	}

	bool PayloadsWereComposedThroughProjection() const {
		return payload_binding_state == SljitAggregatePayloadBindingState::PROJECTION_COMPOSED;
	}

	void InitializeDirectPrimitivePayloads() {
		D_ASSERT(CanInitializePrimitivePayloads());
		payload_binding_state = SljitAggregatePayloadBindingState::DIRECT;
	}

	void InitializeProjectionComposedPrimitivePayloads() {
		D_ASSERT(CanInitializePrimitivePayloads());
		payload_binding_state = SljitAggregatePayloadBindingState::PROJECTION_COMPOSED;
	}

	void MarkPrimitivePayloadsProjectionComposed() {
		D_ASSERT(UsesPrimitivePayloads());
		payload_binding_state = SljitAggregatePayloadBindingState::PROJECTION_COMPOSED;
	}
};

class SljitNativeRegionOpPlan {
public:
	explicit SljitNativeRegionOpPlan(SljitNativeRegionOpKind kind = SljitNativeRegionOpKind::FILTER) {
		SetKind(kind);
	}

	SljitNativeRegionOpKind Kind() const {
		return static_cast<SljitNativeRegionOpKind>(payload.index());
	}
	void SetKind(SljitNativeRegionOpKind kind) {
		switch (kind) {
		case SljitNativeRegionOpKind::FILTER:
			payload.emplace<SljitNativeRegionExpressionPlan>();
			return;
		case SljitNativeRegionOpKind::PROJECTION:
			payload.emplace<vector<SljitNativeRegionExpressionPlan>>();
			return;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			payload.emplace<SljitNativeHashJoinProbePlan>();
			return;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			payload.emplace<SljitNativeHashJoinBuildPlan>();
			return;
		case SljitNativeRegionOpKind::ORDER_SINK:
			payload.emplace<SljitNativeOrderSinkPlan>();
			return;
		case SljitNativeRegionOpKind::APPEND_SINK:
			payload.emplace<SljitNativeAppendSinkPlan>();
			return;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			payload.emplace<SljitNativeAggregateUpdatePlan>();
			return;
		}
		throw InternalException("Unknown SLJIT native operator plan kind");
	}

	SljitNativeRegionExpressionPlan &Filter() {
		return std::get<SljitNativeRegionExpressionPlan>(payload);
	}
	const SljitNativeRegionExpressionPlan &Filter() const {
		return std::get<SljitNativeRegionExpressionPlan>(payload);
	}
	vector<SljitNativeRegionExpressionPlan> &Projections() {
		return std::get<vector<SljitNativeRegionExpressionPlan>>(payload);
	}
	const vector<SljitNativeRegionExpressionPlan> &Projections() const {
		return std::get<vector<SljitNativeRegionExpressionPlan>>(payload);
	}
	SljitNativeHashJoinProbePlan &HashJoinProbe() {
		return std::get<SljitNativeHashJoinProbePlan>(payload);
	}
	const SljitNativeHashJoinProbePlan &HashJoinProbe() const {
		return std::get<SljitNativeHashJoinProbePlan>(payload);
	}
	SljitNativeHashJoinBuildPlan &HashJoinBuild() {
		return std::get<SljitNativeHashJoinBuildPlan>(payload);
	}
	const SljitNativeHashJoinBuildPlan &HashJoinBuild() const {
		return std::get<SljitNativeHashJoinBuildPlan>(payload);
	}
	SljitNativeOrderSinkPlan &OrderSink() {
		return std::get<SljitNativeOrderSinkPlan>(payload);
	}
	const SljitNativeOrderSinkPlan &OrderSink() const {
		return std::get<SljitNativeOrderSinkPlan>(payload);
	}
	SljitNativeAppendSinkPlan &AppendSink() {
		return std::get<SljitNativeAppendSinkPlan>(payload);
	}
	const SljitNativeAppendSinkPlan &AppendSink() const {
		return std::get<SljitNativeAppendSinkPlan>(payload);
	}
	SljitNativeAggregateUpdatePlan &AggregateUpdate() {
		return std::get<SljitNativeAggregateUpdatePlan>(payload);
	}
	const SljitNativeAggregateUpdatePlan &AggregateUpdate() const {
		return std::get<SljitNativeAggregateUpdatePlan>(payload);
	}

	idx_t operator_index = DConstants::INVALID_INDEX;
	//! Lowering-only estimate; executable plans never retain it.
	idx_t estimated_input_count = 0;
	vector<LogicalType> input_types;
	vector<LogicalType> output_types;

private:
	std::variant<SljitNativeRegionExpressionPlan, vector<SljitNativeRegionExpressionPlan>, SljitNativeHashJoinProbePlan,
	             SljitNativeHashJoinBuildPlan, SljitNativeOrderSinkPlan, SljitNativeAppendSinkPlan,
	             SljitNativeAggregateUpdatePlan>
	    payload;
};

struct SljitNativeAggregateStateSourcePlan {
	string function_name;
	ExecutionRegionNativeStateScanContract state_scan_contract;
};

struct SljitNativeRegionPlan {
	vector<SljitCastErrorIdentity> cast_errors;
	vector<SljitNativeRegionOpPlan> ops;
	vector<SljitNativeScanFilterPlan> scan_filters;
	SljitNativeAggregateStateSourcePlan aggregate_state_source;
	ExecutionRegionScanFilterMode scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
	vector<LogicalType> source_output_types;
	vector<idx_t> source_distinct_counts;
	vector<idx_t> source_distinct_reserve_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	vector<bool> source_not_null;

	bool UsesScanFilters() const {
		return scan_filter_mode != ExecutionRegionScanFilterMode::NONE;
	}
};

string DescribeNativeRegion(const SljitNativeRegionPlan &region, const string &mode);
void BindSljitRegionInstanceSlots(SljitNativeRegionPlan &region);
void BindSljitCastErrorSlots(SljitIntegerCastChain &chain, vector<SljitCastErrorIdentity> &errors);
vector<SljitAggregateInstanceParameters> BuildSljitAggregateInstanceParameters(const SljitNativeRegionPlan &region);
string DescribeNativeRegionShape(const SljitNativeRegionPlan &region);
string BuildSljitRegionArtifactSemanticKey(const SljitNativeRegionPlan &region);
string BuildSljitRegionArtifactBindingKey(const SljitNativeRegionPlan &region);
string ComposeSljitRegionArtifactCacheKey(const string &semantic_key, const string &binding_key);

struct SljitRegionBackendPlan : public ExecutionRegionBackendPlan {
	//! The planner lowers every candidate but compiles few, so the key is described only when one compiles
	string ArtifactCacheKey(const ExecutionRegionCandidate &candidate) const override {
		if (!native_region) {
			throw InternalException("SLJIT backend plan has no native region to key its artifact on");
		}
		return ComposeSljitRegionArtifactCacheKey(BuildSljitRegionArtifactSemanticKey(*native_region),
		                                          BuildSljitRegionArtifactBindingKey(*native_region));
	}

	unique_ptr<SljitNativeRegionPlan> native_region;
};
ExecutionRegionLoweringPlan BuildSljitRegionPlan(const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate,
                                                 bool render_diagnostics = false);

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_executable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_hash_join_probe_specialization.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

#include <array>

namespace duckdb {

struct SljitExecutableRegionExpression {
	SljitNativeRegionExpressionPlan plan;
	vector<idx_t> input_source_indices;
	vector<bool> input_source_not_null;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeVectorFunction function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> flat_code;
	SljitNativeVectorFunction flat_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> select_code;
	SljitNativeVectorFunction select_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> predicate_code;
	SljitNativePredicateFunction predicate_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> predicate_select_code;
	SljitNativePredicateFunction predicate_select_function = nullptr;
	string overflow_message;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (code) {
			result += code->CodeSize();
		}
		if (flat_code) {
			result += flat_code->CodeSize();
		}
		if (select_code) {
			result += select_code->CodeSize();
		}
		if (predicate_code) {
			result += predicate_code->CodeSize();
		}
		if (predicate_select_code) {
			result += predicate_select_code->CodeSize();
		}
		return result;
	}
};

static inline bool SljitInputSourceKnownNotNull(const vector<bool> &input_source_not_null, idx_t source_idx) {
	return source_idx < input_source_not_null.size() && input_source_not_null[source_idx];
}

static inline bool SljitSourceKnownNotNull(const vector<bool> *source_not_null, idx_t input_index) {
	return source_not_null && input_index < source_not_null->size() && (*source_not_null)[input_index];
}

struct SljitExecutableRegularHashJoinProbeCode {
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeRegularHashJoinProbeFunction function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> bloom_code;
	SljitNativeRegularHashJoinProbeFunction bloom_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> mark_match_selection_code;
	SljitNativeRegularHashJoinProbeFunction mark_match_selection_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> mark_match_selection_bloom_code;
	SljitNativeRegularHashJoinProbeFunction mark_match_selection_bloom_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> mark_nonmatch_selection_code;
	SljitNativeRegularHashJoinProbeFunction mark_nonmatch_selection_function = nullptr;
	unique_ptr<ExecutionRegionCodeHandle> mark_nonmatch_selection_bloom_code;
	SljitNativeRegularHashJoinProbeFunction mark_nonmatch_selection_bloom_function = nullptr;

	struct AllValidSpecialization {
		unique_ptr<ExecutionRegionCodeHandle> code;
		SljitNativeRegularHashJoinProbeFunction function = nullptr;

		idx_t CodeSize() const {
			return code ? code->CodeSize() : 0;
		}
	};
	static constexpr idx_t ALL_VALID_SPECIALIZATION_COUNT =
	    SljitHashJoinProbeAllValidSpecializationKey::SPECIALIZATION_COUNT;
	AllValidSpecialization all_valid_specializations[ALL_VALID_SPECIALIZATION_COUNT];

	AllValidSpecialization &AllValidSpecializationFor(const SljitHashJoinProbeAllValidSpecializationKey &key) {
		return all_valid_specializations[key.CacheIndex()];
	}

	AllValidSpecialization mark_match_all_valid_specializations[ALL_VALID_SPECIALIZATION_COUNT];

	AllValidSpecialization &MarkMatchAllValidSpecializationFor(const SljitHashJoinProbeAllValidSpecializationKey &key) {
		return mark_match_all_valid_specializations[key.CacheIndex()];
	}

	AllValidSpecialization mark_nonmatch_all_valid_specializations[ALL_VALID_SPECIALIZATION_COUNT];

	AllValidSpecialization &
	MarkNonMatchAllValidSpecializationFor(const SljitHashJoinProbeAllValidSpecializationKey &key) {
		return mark_nonmatch_all_valid_specializations[key.CacheIndex()];
	}

	idx_t CodeSize() const {
		idx_t result = code ? code->CodeSize() : 0;
		result += bloom_code ? bloom_code->CodeSize() : 0;
		result += mark_match_selection_code ? mark_match_selection_code->CodeSize() : 0;
		result += mark_match_selection_bloom_code ? mark_match_selection_bloom_code->CodeSize() : 0;
		result += mark_nonmatch_selection_code ? mark_nonmatch_selection_code->CodeSize() : 0;
		result += mark_nonmatch_selection_bloom_code ? mark_nonmatch_selection_bloom_code->CodeSize() : 0;
		for (auto &specialization : all_valid_specializations) {
			result += specialization.CodeSize();
		}
		for (auto &specialization : mark_match_all_valid_specializations) {
			result += specialization.CodeSize();
		}
		for (auto &specialization : mark_nonmatch_all_valid_specializations) {
			result += specialization.CodeSize();
		}
		return result;
	}
};

struct SljitExecutablePerfectHashJoinProbeCode {
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativePerfectHashJoinProbeFunction function = nullptr;

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}
};

struct SljitExecutableHashJoinProbe {
	SljitNativeHashJoinProbePlan plan;
	SljitExecutableRegularHashJoinProbeCode regular;
	SljitExecutablePerfectHashJoinProbeCode perfect;
	SljitExecutableRegionExpression residual_filter;

	bool ValidateDeferredCodegen(string &error) const;

	bool HasDeferredCodegen() const;

	idx_t CodeSize() const {
		idx_t result = regular.CodeSize();
		result += perfect.CodeSize();
		result += residual_filter.CodeSize();
		return result;
	}
};

struct SljitExecutableHashJoinBuild {
	SljitNativeHashJoinBuildPlan plan;
};

struct SljitExecutableNestedLoopJoinProbe {
	SljitNativeNestedLoopJoinProbePlan plan;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeNestedLoopJoinProbeFunction function = nullptr;
	vector<SljitExecutableRegionExpression> lhs_conditions;

	idx_t CodeSize() const {
		idx_t result = code ? code->CodeSize() : 0;
		for (auto &condition : lhs_conditions) {
			result += condition.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableNestedLoopJoinBuild {
	SljitNativeNestedLoopJoinBuildPlan plan;
	vector<SljitExecutableRegionExpression> rhs_conditions;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &condition : rhs_conditions) {
			result += condition.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableAppendSink {
	SljitNativeAppendSinkPlan plan;
};

struct SljitExecutableDelimJoinSink {
	SljitNativeDelimJoinSinkPlan plan;
};

struct SljitExecutableOrderSink {
	SljitNativeOrderSinkPlan plan;
	vector<SljitExecutableRegionExpression> order_keys;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &order_key : order_keys) {
			result += order_key.CodeSize();
		}
		return result;
	}
};

struct SljitExecutableFilteredAggregateUpdate {
	SljitExecutableRegionExpression filter;
	vector<SljitExecutableRegionExpression> payloads;
	vector<idx_t> input_source_indices;
	vector<bool> input_source_not_null;
	unique_ptr<ExecutionRegionCodeHandle> code;
	SljitNativeAggregateUpdateFunction function = nullptr;
	bool owns_perfect_hash_group_lookup = false;

	bool IsExecutable() const {
		return code && function;
	}

	idx_t CodeSize() const {
		return code ? code->CodeSize() : 0;
	}
};

enum class SljitGroupedAggregateDirectUpdatePlanKind : uint8_t {
	NONE,
	ADAPTIVE_GROUPED_STATE_ADDRESS,
	DIRECT_STATE_ADDRESS_PAYLOAD_ONLY
};

struct SljitGroupedAggregateDirectUpdatePlan {
	SljitGroupedAggregateDirectUpdatePlanKind kind = SljitGroupedAggregateDirectUpdatePlanKind::NONE;

	void Clear() {
		kind = SljitGroupedAggregateDirectUpdatePlanKind::NONE;
	}

	bool DirectStateAddressPayloadOnly() const {
		return kind == SljitGroupedAggregateDirectUpdatePlanKind::DIRECT_STATE_ADDRESS_PAYLOAD_ONLY;
	}
};

struct SljitExecutableAggregateUpdate {
	SljitNativeAggregateUpdatePlan plan;
	vector<SljitExecutableRegionExpression> payloads;
	SljitExecutableFilteredAggregateUpdate filtered_update;
	ExecutionDenseGroupDomain dense_group_domain;
	SljitGroupedAggregateDirectUpdatePlan grouped_direct_update;
	unique_ptr<ExecutionRegionCodeHandle> fused_payload_update_code;
	SljitNativeAggregateUpdateFunction fused_payload_update_function = nullptr;
	bool fused_payload_update_owns_group_lookup = false;
	vector<unique_ptr<ExecutionRegionCodeHandle>> payload_update_code;
	vector<SljitNativeAggregateUpdateFunction> payload_update_functions;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &payload : payloads) {
			result += payload.CodeSize();
		}
		result += filtered_update.CodeSize();
		result += fused_payload_update_code ? fused_payload_update_code->CodeSize() : 0;
		for (auto &code : payload_update_code) {
			result += code ? code->CodeSize() : 0;
		}
		return result;
	}
};

enum class SljitDirectProjectionKind : uint8_t { NONE, FLOAT, DOUBLE, INT32, INT64, DECIMAL64, DATE };

enum class SljitDirectProjectionStatsMode : uint8_t { NONE, GENERATED_FLOATING_MIN_MAX, POSTPASS_FIXED_STATS };

struct SljitDirectProjectionSourceRef {
	idx_t input_index = DConstants::INVALID_INDEX;
	idx_t projection_index = DConstants::INVALID_INDEX;
	bool right_source = false;
};

struct SljitDirectProjectionPlan {
	SljitDirectProjectionKind kind = SljitDirectProjectionKind::NONE;
	SljitDirectProjectionStatsMode stats_mode = SljitDirectProjectionStatsMode::NONE;
	vector<idx_t> projection_indices;
	vector<SljitDirectProjectionSourceRef> sources;
	bool covers_all_projections = false;

	bool SinglePrecision() const {
		return kind == SljitDirectProjectionKind::FLOAT;
	}
};

struct SljitExecutableRegionOp {
	SljitNativeRegionOpKind kind;
	idx_t operator_index = DConstants::INVALID_INDEX;
	vector<LogicalType> input_types;
	vector<LogicalType> output_types;
	vector<bool> output_not_null;
	SljitExecutableRegionExpression filter;
	SljitExecutableHashJoinProbe hash_join_probe;
	SljitExecutableHashJoinBuild hash_join_build;
	SljitExecutableNestedLoopJoinProbe nested_loop_join_probe;
	SljitExecutableNestedLoopJoinBuild nested_loop_join_build;
	SljitExecutableAppendSink append_sink;
	SljitExecutableDelimJoinSink delim_join_sink;
	SljitExecutableOrderSink order_sink;
	SljitExecutableAggregateUpdate aggregate_update;
	vector<SljitExecutableRegionExpression> projections;
	SljitDirectProjectionPlan flat_fused_floating_projection_plan;
	unique_ptr<ExecutionRegionCodeHandle> flat_fused_floating_projection_code;
	SljitNativeVectorFunction flat_fused_floating_projection_function = nullptr;
	vector<SljitDirectProjectionPlan> flat_fused_fixed_projection_plans;
	vector<unique_ptr<ExecutionRegionCodeHandle>> flat_fused_fixed_projection_codes;
	vector<SljitNativeVectorFunction> flat_fused_fixed_projection_functions;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (kind == SljitNativeRegionOpKind::FILTER) {
			result += filter.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			result += hash_join_probe.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
			result += nested_loop_join_probe.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
			result += nested_loop_join_build.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::ORDER_SINK) {
			result += order_sink.CodeSize();
		}
		if (kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			result += aggregate_update.CodeSize();
		}
		if (flat_fused_floating_projection_code) {
			result += flat_fused_floating_projection_code->CodeSize();
		}
		for (auto &code : flat_fused_fixed_projection_codes) {
			result += code ? code->CodeSize() : 0;
		}
		for (auto &projection : projections) {
			result += projection.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const {
		if (CodeSize() > 0) {
			return true;
		}
		return kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE && hash_join_probe.HasDeferredCodegen();
	}
};

struct SljitExecutableRegion {
	vector<SljitExecutableRegionOp> ops;
	bool uses_scan_filters = false;
	vector<LogicalType> source_output_types;
	vector<idx_t> source_distinct_counts;
	vector<idx_t> source_distinct_reserve_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &op : ops) {
			result += op.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const {
		for (auto &op : ops) {
			if (op.HasExecutableBody()) {
				return true;
			}
		}
		return false;
	}
};

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error);

} // namespace duckdb

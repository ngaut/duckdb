//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_executable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_compiled_function.hpp"
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
	SljitCompiledFunction<SljitNativeVectorFunction> vector;
	SljitCompiledFunction<SljitNativeVectorFunction> flat;
	SljitCompiledFunction<SljitNativeVectorFunction> select;
	SljitCompiledFunction<SljitNativePredicateFunction> predicate;
	SljitCompiledFunction<SljitNativePredicateFunction> predicate_select;
	string overflow_message;

	idx_t CodeSize() const {
		return vector.CodeSize() + flat.CodeSize() + select.CodeSize() + predicate.CodeSize() +
		       predicate_select.CodeSize();
	}
};

static inline bool SljitInputSourceKnownNotNull(const vector<bool> &input_source_not_null, idx_t source_idx) {
	return source_idx < input_source_not_null.size() && input_source_not_null[source_idx];
}

static inline bool SljitSourceKnownNotNull(const vector<bool> *source_not_null, idx_t input_index) {
	return source_not_null && input_index < source_not_null->size() && (*source_not_null)[input_index];
}

struct SljitExecutableRegularHashJoinProbeCode {
	SljitLazyCompiledFunction<SljitNativeRegularHashJoinProbeFunction> &
	Specialization(const SljitHashJoinProbeSpecializationKey &key) {
		return specializations[key.CacheIndex()];
	}

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &specialization : specializations) {
			result += specialization.CodeSize();
		}
		return result;
	}

private:
	array<SljitLazyCompiledFunction<SljitNativeRegularHashJoinProbeFunction>,
	      SljitHashJoinProbeSpecializationKey::SPECIALIZATION_COUNT>
	    specializations;
};

struct SljitExecutablePerfectHashJoinProbeCode {
	SljitLazyCompiledFunction<SljitNativePerfectHashJoinProbeFunction> compiled;
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
		result += perfect.compiled.CodeSize();
		result += residual_filter.CodeSize();
		return result;
	}
};

struct SljitExecutableHashJoinBuild {
	SljitNativeHashJoinBuildPlan plan;
};

struct SljitExecutableNestedLoopJoinProbe {
	SljitNativeNestedLoopJoinProbePlan plan;
	SljitCompiledFunction<SljitNativeNestedLoopJoinProbeFunction> compiled;
	vector<SljitExecutableRegionExpression> lhs_conditions;

	idx_t CodeSize() const {
		idx_t result = compiled.CodeSize();
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
	SljitCompiledFunction<SljitNativeAggregateUpdateFunction> compiled;
	bool owns_perfect_hash_group_lookup = false;

	bool IsExecutable() const {
		return compiled.IsExecutable();
	}

	idx_t CodeSize() const {
		return compiled.CodeSize();
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
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	vector<bool> group_source_not_null;
	SljitExecutableFilteredAggregateUpdate filtered_update;
	ExecutionDenseGroupDomain dense_group_domain;
	SljitGroupedAggregateDirectUpdatePlan grouped_direct_update;
	SljitCompiledFunction<SljitNativeAggregateUpdateFunction> fused_payload_update;
	bool fused_payload_update_owns_group_lookup = false;
	vector<SljitCompiledFunction<SljitNativeAggregateUpdateFunction>> payload_updates;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &payload : payloads) {
			result += payload.CodeSize();
		}
		result += filtered_update.CodeSize();
		result += fused_payload_update.CodeSize();
		for (auto &payload_update : payload_updates) {
			result += payload_update.CodeSize();
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
	SljitCompiledFunction<SljitNativeVectorFunction> flat_fused_floating_projection;
	vector<SljitDirectProjectionPlan> flat_fused_fixed_projection_plans;
	vector<SljitCompiledFunction<SljitNativeVectorFunction>> flat_fused_fixed_projections;

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
		result += flat_fused_floating_projection.CodeSize();
		for (auto &projection : flat_fused_fixed_projections) {
			result += projection.CodeSize();
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

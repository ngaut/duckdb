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
#include "sljit_perfect_hash_predicate_classification.hpp"
#include "sljit_predicate_string_runtime.hpp"

#include "sljit_region_plan.hpp"

#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

#include <array>

namespace duckdb {

struct SljitFullPipelineRecipePlan;

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

	bool HasSelectionKernel() const {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::PREDICATE:
			return predicate_select.Function() != nullptr;
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return select.Function() != nullptr;
		default:
			return false;
		}
	}

	idx_t CodeSize() const {
		return vector.CodeSize() + flat.CodeSize() + select.CodeSize() + predicate.CodeSize() +
		       predicate_select.CodeSize();
	}
};

struct SljitExecutableFilter {
	SljitExecutableRegionExpression expression;
	unique_ptr<SljitNativeStringLikeBatchPlan> batch_select;

	idx_t CodeSize() const {
		return expression.CodeSize();
	}
};

struct SljitExecutableScanFilter {
	idx_t filter_index = DConstants::INVALID_INDEX;
	LogicalType input_type;
	unique_ptr<SljitExecutableFilter> filter;

	idx_t CodeSize() const {
		return filter ? filter->CodeSize() : 0;
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
	SljitLazyCompiledFunction<SljitNativePerfectHashJoinProbeFunction> compact_selection;
	SljitLazyCompiledFunction<SljitNativePerfectHashJoinProbeFunction> identity_preferred_selection;
	SljitLazyCompiledFunction<SljitNativePerfectHashJoinProbeFunction> identity_direct_consumer;

	SljitLazyCompiledFunction<SljitNativePerfectHashJoinProbeFunction> &SelectionKernel(bool prefer_identity_selection,
	                                                                                    bool direct_consumer) {
		D_ASSERT(!direct_consumer || prefer_identity_selection);
		if (direct_consumer) {
			return identity_direct_consumer;
		}
		return prefer_identity_selection ? identity_preferred_selection : compact_selection;
	}

	idx_t CodeSize() const {
		return compact_selection.CodeSize() + identity_preferred_selection.CodeSize() +
		       identity_direct_consumer.CodeSize();
	}
};

struct SljitExecutableHashJoinProbe {
	SljitNativeHashJoinProbePlan plan;
	SljitExecutableRegularHashJoinProbeCode regular;
	SljitExecutablePerfectHashJoinProbeCode perfect;
	SljitSharedPerfectHashPredicateClassificationCache shared_predicate_classification;
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

enum class SljitAggregatePayloadSourceLayout : uint8_t { DIRECT_PER_LANE, FUSED_COMBINED };

enum class SljitFilteredAggregateKernelKind : uint8_t { NONE, UNGROUPED_PAYLOAD, PERFECT_HASH_GROUPED };

struct SljitExecutableFilteredAggregateUpdate {
	SljitExecutableRegionExpression filter;
	vector<SljitExecutableRegionExpression> payloads;
	vector<idx_t> input_source_indices;
	vector<bool> input_source_not_null;
	SljitCompiledFunction<SljitNativeAggregateUpdateFunction> compiled;
	SljitFilteredAggregateKernelKind kind = SljitFilteredAggregateKernelKind::NONE;
	SljitAggregatePayloadSourceLayout payload_source_layout = SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;

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

struct SljitExecutablePrimitiveRunSpecialization {
	PhysicalType group_source_type = PhysicalType::INVALID;
	PhysicalType group_output_type = PhysicalType::INVALID;
	ExecutionRowPointerGroupKeyCastKind group_cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> nullable_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> multi_lane_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> affine_int64_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> affine_int64_nullable_compiled;
};

struct SljitExecutablePrimitiveRunUpdate {
	PhysicalType group_type = PhysicalType::INVALID;
	vector<PhysicalType> payload_types;
	vector<AggregatePrimitiveUpdateKind> primitive_kinds;
	vector<SljitExecutablePrimitiveRunSpecialization> flat_specializations;

	optional_ptr<SljitExecutablePrimitiveRunSpecialization>
	Specialization(PhysicalType group_source_type, PhysicalType group_output_type,
	               ExecutionRowPointerGroupKeyCastKind group_cast_kind) {
		for (auto &specialization : flat_specializations) {
			if (specialization.group_source_type == group_source_type &&
			    specialization.group_output_type == group_output_type &&
			    specialization.group_cast_kind == group_cast_kind) {
				return specialization;
			}
		}
		return nullptr;
	}

	bool HasDeferredCodegen() const {
		return !primitive_kinds.empty() && primitive_kinds.size() == payload_types.size() &&
		       !flat_specializations.empty();
	}

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &specialization : flat_specializations) {
			result += specialization.compiled.CodeSize() + specialization.nullable_compiled.CodeSize() +
			          specialization.multi_lane_compiled.CodeSize() + specialization.affine_int64_compiled.CodeSize() +
			          specialization.affine_int64_nullable_compiled.CodeSize();
		}
		return result;
	}
};

struct SljitFusedAffineRunLane {
	int64_t scale = 1;
	int64_t offset = 0;
};

struct SljitExecutableFusedAffineRunUpdate {
	idx_t source_position = DConstants::INVALID_INDEX;
	PhysicalType source_type = PhysicalType::INVALID;
	AggregatePrimitiveUpdateKind primitive_kind = AggregatePrimitiveUpdateKind::NONE;
	vector<SljitFusedAffineRunLane> lanes;
	bool lanes_form_arithmetic_progression = false;
	SljitFusedAffineRunLane lane_step {0, 0};

	bool Ready() const {
		return source_position != DConstants::INVALID_INDEX && source_type != PhysicalType::INVALID &&
		       primitive_kind != AggregatePrimitiveUpdateKind::NONE && lanes.size() >= 2;
	}

	void Clear() {
		source_position = DConstants::INVALID_INDEX;
		source_type = PhysicalType::INVALID;
		primitive_kind = AggregatePrimitiveUpdateKind::NONE;
		lanes.clear();
		lanes_form_arithmetic_progression = false;
		lane_step = {0, 0};
	}
};

struct SljitExecutableIntegralGroupKeyRange {
	bool ready = false;
	PhysicalType source_physical_type = PhysicalType::INVALID;
	int64_t min_value = 0;
	int64_t max_value = 0;

	bool ProvesNarrowingCast(const ExecutionRowPointerGroupKeySource &source) const {
		if (!ready || source.source_physical_type != source_physical_type) {
			return false;
		}
		switch (source.cast_kind) {
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
			return source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT32 &&
			       min_value >= NumericLimits<int32_t>::Minimum() && max_value <= NumericLimits<int32_t>::Maximum();
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
			return source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT16 &&
			       min_value >= NumericLimits<int16_t>::Minimum() && max_value <= NumericLimits<int16_t>::Maximum();
		case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
			return source_physical_type == PhysicalType::INT32 && source.target_physical_type == PhysicalType::INT8 &&
			       min_value >= NumericLimits<int8_t>::Minimum() && max_value <= NumericLimits<int8_t>::Maximum();
		default:
			return false;
		}
	}
};

struct SljitExecutableAggregateUpdate {
	SljitNativeAggregateUpdatePlan plan;
	vector<SljitExecutableRegionExpression> payloads;
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	SljitAggregatePayloadSourceLayout payload_source_layout = SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;
	vector<idx_t> combined_payload_source_indices;
	vector<bool> combined_payload_source_not_null;
	vector<bool> group_source_not_null;
	vector<SljitExecutableIntegralGroupKeyRange> integral_group_key_ranges;
	SljitExecutableFilteredAggregateUpdate filtered_update;
	ExecutionDenseGroupDomain dense_group_domain;
	SljitGroupedAggregateDirectUpdatePlan grouped_direct_update;
	SljitExecutablePrimitiveRunUpdate primitive_run_update;
	SljitExecutableFusedAffineRunUpdate fused_affine_run_update;
	SljitCompiledFunction<SljitNativeAggregateUpdateFunction> fused_payload_update;
	bool fused_payload_update_owns_group_lookup = false;
	vector<SljitCompiledFunction<SljitNativeAggregateUpdateFunction>> payload_updates;

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &payload : payloads) {
			result += payload.CodeSize();
		}
		result += filtered_update.CodeSize();
		result += primitive_run_update.CodeSize();
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
	vector<bool> input_not_null;
	vector<Value> input_min_values;
	vector<Value> input_max_values;
	vector<LogicalType> output_types;
	vector<bool> output_not_null;
	unique_ptr<SljitExecutableFilter> filter;
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
			D_ASSERT(filter);
			result += filter->CodeSize();
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
		if (kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			return hash_join_probe.HasDeferredCodegen();
		}
		return kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       aggregate_update.primitive_run_update.HasDeferredCodegen();
	}
};

struct SljitExecutableRegion {
	vector<SljitExecutableRegionOp> ops;
	vector<SljitExecutableScanFilter> scan_filters;
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
		for (auto &scan_filter : scan_filters) {
			result += scan_filter.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const {
		for (auto &op : ops) {
			if (op.HasExecutableBody()) {
				return true;
			}
		}
		return !scan_filters.empty();
	}
};

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable,
                                SljitFullPipelineRecipePlan &recipe_plan, string &error);

} // namespace duckdb

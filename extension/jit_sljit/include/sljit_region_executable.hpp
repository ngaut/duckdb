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
	//! The selection predicate emitted a packed-SIMD prefix (scalar residual
	//! remains); recorded so traces report rows-weighted SIMD coverage.
	bool predicate_partial_simd = false;
	//! The partial-SIMD residual that reads one source, run over that source's dictionary: its result per entry lets
	//! the selection predicate look rows up instead of evaluating them
	SljitCompiledFunction<SljitNativePredicateFunction> predicate_residual_dictionary;
	idx_t predicate_residual_dictionary_source = DConstants::INVALID_INDEX;

	//! Resolve a compiled input slot. Aggregate payloads, predicates and typed trees use this map;
	//! ordinary scalar projections still name input columns directly in their semantic plan.
	idx_t InputColumn(idx_t source_slot) const {
		D_ASSERT(source_slot < input_source_indices.size());
		return input_source_indices[source_slot];
	}

	bool HasSelectionKernel() const {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::PREDICATE:
			return predicate_select.Function() != nullptr;
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return select.Function() != nullptr;
		default:
			return false;
		}
	}

	idx_t CodeSize() const {
		return vector.CodeSize() + flat.CodeSize() + select.CodeSize() + predicate.CodeSize() +
		       predicate_select.CodeSize() + predicate_residual_dictionary.CodeSize();
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

struct SljitExecutableAppendSink {
	SljitNativeAppendSinkPlan plan;
};

struct SljitExecutableOrderSink {
	SljitNativeOrderSinkPlan plan;
};

//! Kernel input-array layout, not an expression index convention: every executable payload reads input slots.
enum class SljitAggregatePayloadSourceLayout : uint8_t { DIRECT_PER_LANE, FUSED_COMBINED };

enum class SljitFilteredAggregateKernelKind : uint8_t { NONE, UNGROUPED_PAYLOAD, PERFECT_HASH_GROUPED };

//! A comparison of a string source with a constant, whose result a batch computes once per dictionary entry
struct SljitStringCompareMatches {
	idx_t source = DConstants::INVALID_INDEX;
	string constant;
	bool compare_equal = true;
};

//! The string comparisons an ungrouped kernel over combined input sources computes once per dictionary entry
struct SljitStringCompareSources {
	//! numbered as the compiled kernel numbers SljitNativeVectorInput::string_matches
	vector<SljitStringCompareMatches> compares;
	//! the input sources the kernel reads only through compares whose NULL result decides as FALSE would: a batch with
	//! their dictionary results reads no other row of them on its fast path
	vector<bool> compare_only_sources;
	//! The kernel runs rows with NULLs on its fast loops, which check each source a leaf reads: a compare_only source's
	//! match results decide its NULL rows, so it keeps no validity
	bool fast_null_rows = false;
};

struct SljitExecutableFilteredAggregateUpdate {
	SljitExecutableRegionExpression filter;
	vector<SljitExecutableRegionExpression> payloads;
	vector<idx_t> input_source_indices;
	vector<bool> input_source_not_null;
	SljitStringCompareSources string_compares;
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
};

struct SljitExecutablePrimitiveRunSpecialization {
	PhysicalType group_source_type = PhysicalType::INVALID;
	PhysicalType group_output_type = PhysicalType::INVALID;
	ExecutionRowPointerGroupKeyCastKind group_cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> nullable_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> multi_lane_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> shared_validity_multi_lane_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> affine_int64_compiled;
	SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> affine_int64_nullable_compiled;
};

struct SljitExecutablePrimitiveRunUpdate {
	static constexpr idx_t PREAGGREGATED_SELECTOR_SHAPE_COUNT = 4;

	PhysicalType group_type = PhysicalType::INVALID;
	vector<PhysicalType> payload_types;
	vector<AggregatePrimitiveUpdateKind> primitive_kinds;
	vector<idx_t> state_offsets;
	vector<SljitExecutablePrimitiveRunSpecialization> flat_specializations;
	std::array<SljitLazyCompiledFunction<SljitNativePreaggregatedPrimitiveUpdateFunction>,
	           PREAGGREGATED_SELECTOR_SHAPE_COUNT>
	    preaggregated_initialize;
	std::array<SljitLazyCompiledFunction<SljitNativePreaggregatedPrimitiveUpdateFunction>,
	           PREAGGREGATED_SELECTOR_SHAPE_COUNT>
	    preaggregated_update;

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

	SljitLazyCompiledFunction<SljitNativePreaggregatedPrimitiveUpdateFunction> &
	PreaggregatedArtifact(bool initialize_states, bool address_selected, bool row_selected) {
		const auto selector_shape = (address_selected ? idx_t(1) : idx_t(0)) | (row_selected ? idx_t(2) : idx_t(0));
		D_ASSERT(selector_shape < PREAGGREGATED_SELECTOR_SHAPE_COUNT);
		return initialize_states ? preaggregated_initialize[selector_shape] : preaggregated_update[selector_shape];
	}

	bool HasDeferredCodegen() const {
		return !primitive_kinds.empty() && primitive_kinds.size() == payload_types.size() &&
		       primitive_kinds.size() == state_offsets.size() && !flat_specializations.empty();
	}

	idx_t CodeSize() const {
		idx_t result = 0;
		for (auto &specialization : flat_specializations) {
			result += specialization.compiled.CodeSize() + specialization.nullable_compiled.CodeSize() +
			          specialization.multi_lane_compiled.CodeSize() +
			          specialization.shared_validity_multi_lane_compiled.CodeSize() +
			          specialization.affine_int64_compiled.CodeSize() +
			          specialization.affine_int64_nullable_compiled.CodeSize();
		}
		for (idx_t selector_shape = 0; selector_shape < PREAGGREGATED_SELECTOR_SHAPE_COUNT; selector_shape++) {
			result +=
			    preaggregated_initialize[selector_shape].CodeSize() + preaggregated_update[selector_shape].CodeSize();
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
	//! an ungrouped update's combined-source kernel: the string comparisons it reads per dictionary entry
	SljitStringCompareSources combined_payload_string_compares;
	vector<bool> group_source_not_null;
	vector<SljitExecutableIntegralGroupKeyRange> integral_group_key_ranges;
	SljitExecutableFilteredAggregateUpdate filtered_update;
	ExecutionDenseGroupDomain dense_group_domain;
	//! For a hash aggregate without aggregates, the number of key combinations its bounded integer group keys
	//! span (integral_group_key_ranges), when small enough for a per-thread bitmap of the groups already handed to
	//! the sink; 0 when there is none
	idx_t distinct_group_domain_size = 0;
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

struct SljitDirectProjectionSourceRef {
	idx_t input_index = DConstants::INVALID_INDEX;
	idx_t projection_index = DConstants::INVALID_INDEX;
	bool right_source = false;
};

struct SljitDirectProjectionPlan {
	vector<idx_t> projection_indices;
	vector<SljitDirectProjectionSourceRef> sources;
	bool covers_all_projections = false;
};

struct SljitExecutableProjection {
	vector<SljitExecutableRegionExpression> expressions;
	vector<SljitDirectProjectionPlan> flat_plans;
	vector<SljitCompiledFunction<SljitNativeVectorFunction>> flat_functions;
};

struct SljitExecutableRegionOp {
	explicit SljitExecutableRegionOp(SljitNativeRegionOpKind kind = SljitNativeRegionOpKind::FILTER) {
		SetKind(kind);
	}
	SljitNativeRegionOpKind Kind() const {
		return static_cast<SljitNativeRegionOpKind>(payload.index());
	}
	void SetKind(SljitNativeRegionOpKind kind) {
		switch (kind) {
		case SljitNativeRegionOpKind::FILTER:
			payload.emplace<unique_ptr<SljitExecutableFilter>>();
			return;
		case SljitNativeRegionOpKind::PROJECTION:
			payload.emplace<SljitExecutableProjection>();
			return;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			payload.emplace<SljitExecutableHashJoinProbe>();
			return;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			payload.emplace<SljitExecutableHashJoinBuild>();
			return;
		case SljitNativeRegionOpKind::ORDER_SINK:
			payload.emplace<SljitExecutableOrderSink>();
			return;
		case SljitNativeRegionOpKind::APPEND_SINK:
			payload.emplace<SljitExecutableAppendSink>();
			return;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			payload.emplace<SljitExecutableAggregateUpdate>();
			return;
		}
		throw InternalException("Unknown SLJIT executable operator kind");
	}
	unique_ptr<SljitExecutableFilter> &Filter() {
		return std::get<unique_ptr<SljitExecutableFilter>>(payload);
	}
	const unique_ptr<SljitExecutableFilter> &Filter() const {
		return std::get<unique_ptr<SljitExecutableFilter>>(payload);
	}
	SljitExecutableProjection &Projection() {
		return std::get<SljitExecutableProjection>(payload);
	}
	const SljitExecutableProjection &Projection() const {
		return std::get<SljitExecutableProjection>(payload);
	}
	SljitExecutableHashJoinProbe &HashJoinProbe() {
		return std::get<SljitExecutableHashJoinProbe>(payload);
	}
	const SljitExecutableHashJoinProbe &HashJoinProbe() const {
		return std::get<SljitExecutableHashJoinProbe>(payload);
	}
	SljitExecutableHashJoinBuild &HashJoinBuild() {
		return std::get<SljitExecutableHashJoinBuild>(payload);
	}
	const SljitExecutableHashJoinBuild &HashJoinBuild() const {
		return std::get<SljitExecutableHashJoinBuild>(payload);
	}
	SljitExecutableOrderSink &OrderSink() {
		return std::get<SljitExecutableOrderSink>(payload);
	}
	const SljitExecutableOrderSink &OrderSink() const {
		return std::get<SljitExecutableOrderSink>(payload);
	}
	SljitExecutableAppendSink &AppendSink() {
		return std::get<SljitExecutableAppendSink>(payload);
	}
	const SljitExecutableAppendSink &AppendSink() const {
		return std::get<SljitExecutableAppendSink>(payload);
	}
	SljitExecutableAggregateUpdate &AggregateUpdate() {
		return std::get<SljitExecutableAggregateUpdate>(payload);
	}
	const SljitExecutableAggregateUpdate &AggregateUpdate() const {
		return std::get<SljitExecutableAggregateUpdate>(payload);
	}
	vector<SljitExecutableRegionExpression> &Projections() {
		return Projection().expressions;
	}
	const vector<SljitExecutableRegionExpression> &Projections() const {
		return Projection().expressions;
	}
	vector<SljitDirectProjectionPlan> &FlatFusedProjectionPlans() {
		return Projection().flat_plans;
	}
	const vector<SljitDirectProjectionPlan> &FlatFusedProjectionPlans() const {
		return Projection().flat_plans;
	}
	vector<SljitCompiledFunction<SljitNativeVectorFunction>> &FlatFusedProjections() {
		return Projection().flat_functions;
	}
	const vector<SljitCompiledFunction<SljitNativeVectorFunction>> &FlatFusedProjections() const {
		return Projection().flat_functions;
	}

	idx_t operator_index = DConstants::INVALID_INDEX;
	//! The first op lowered from its operator, fed exactly what DuckDB's operator takes: DuckDB's operators can take
	//! the region's rows over here
	bool input_is_operator_input = false;
	vector<LogicalType> input_types;
	vector<bool> input_not_null;
	vector<Value> input_min_values;
	vector<Value> input_max_values;
	vector<LogicalType> output_types;
	vector<bool> output_not_null;

	idx_t CodeSize() const {
		idx_t result = 0;
		if (Kind() == SljitNativeRegionOpKind::FILTER) {
			D_ASSERT(Filter());
			result += Filter()->CodeSize();
		}
		if (Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			result += HashJoinProbe().CodeSize();
		}
		if (Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			result += AggregateUpdate().CodeSize();
		}
		if (Kind() == SljitNativeRegionOpKind::PROJECTION) {
			for (auto &projection : FlatFusedProjections()) {
				result += projection.CodeSize();
			}
			for (auto &projection : Projections()) {
				result += projection.CodeSize();
			}
		}
		return result;
	}

	//! The single authority for what this op's execution path dereferences from
	//! its input chunk. Dense layouts (plan input types) become exact-arity
	//! constraints; delegated aggregates constrain only the sink-contract
	//! references they actually touch. provable=false means the references
	//! cannot be derived and callers must not guess.
	struct InputConstraints {
		bool provable = false;
		bool exact_arity = false;
		idx_t arity = 0;
		vector<std::pair<idx_t, LogicalType>> references;

		bool SatisfiedBy(const vector<LogicalType> &fed) const {
			if (!provable) {
				return true;
			}
			if (exact_arity && fed.size() != arity) {
				return false;
			}
			for (auto &reference : references) {
				if (reference.first >= fed.size() || fed[reference.first] != reference.second) {
					return false;
				}
			}
			return true;
		}
	};

	InputConstraints DeclaredInputConstraints() const {
		InputConstraints constraints;
		auto dense = [&](const vector<LogicalType> &types) {
			if (types.empty()) {
				return;
			}
			constraints.provable = true;
			constraints.exact_arity = true;
			constraints.arity = types.size();
			for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
				constraints.references.emplace_back(col_idx, types[col_idx]);
			}
		};
		switch (Kind()) {
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE: {
			// The delegated sink dereferences exactly the contract's declared
			// references; fused input transforms are invisible to delegation, so
			// these are the truth regardless of any declared dense layout.
			auto &sink_info = AggregateUpdate().plan.sink_info;
			for (auto &group : sink_info.groups) {
				if (!group.supported_reference) {
					continue;
				}
				constraints.provable = true;
				constraints.references.emplace_back(group.input_index, group.type);
			}
			for (auto &aggregate : sink_info.aggregates) {
				for (idx_t child_idx = 0; child_idx < aggregate.child_indices.size(); child_idx++) {
					if (child_idx >= aggregate.child_types.size()) {
						continue;
					}
					constraints.provable = true;
					constraints.references.emplace_back(aggregate.child_indices[child_idx],
					                                    aggregate.child_types[child_idx]);
				}
			}
			return constraints;
		}
		case SljitNativeRegionOpKind::APPEND_SINK:
			dense(AppendSink().plan.input_types);
			return constraints;
		case SljitNativeRegionOpKind::ORDER_SINK:
			dense(OrderSink().plan.input_types);
			return constraints;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			dense(HashJoinBuild().plan.input_types);
			return constraints;
		default:
			dense(input_types);
			return constraints;
		}
	}

	bool HasExecutableBody() const {
		if (CodeSize() > 0) {
			return true;
		}
		if (Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			return HashJoinProbe().HasDeferredCodegen();
		}
		return Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       AggregateUpdate().primitive_run_update.HasDeferredCodegen();
	}

private:
	std::variant<unique_ptr<SljitExecutableFilter>, SljitExecutableProjection, SljitExecutableHashJoinProbe,
	             SljitExecutableHashJoinBuild, SljitExecutableOrderSink, SljitExecutableAppendSink,
	             SljitExecutableAggregateUpdate>
	    payload;
};

//! What a region that may hand its pipeline to DuckDB's operators needs to decide and to continue natively
struct SljitExecutableNativeContinuation {
	//! Turn a fetched source chunk into the native scan's output: the source's own filters and layout repair
	vector<SljitExecutableRegionOp> source_ops;
	//! The fetched chunk's column the hash aggregate's single group key derives from, sampled for group runs; invalid
	//! when the key does not derive from one source column
	idx_t group_key_source_column = DConstants::INVALID_INDEX;
	//! The hash aggregate only counts rows: a single COUNT or COUNT(*)
	bool counts_rows_only = false;
	//! The region's chunks and DuckDB's may alternate into the sink: which is faster is measured, not decided
	bool measured = false;
};

struct SljitExecutableRegion {
	vector<SljitExecutableRegionOp> ops;
	//! Present for a region that may hand its pipeline to DuckDB's operators
	unique_ptr<SljitExecutableNativeContinuation> native_continuation;
	vector<SljitExecutableScanFilter> scan_filters;
	SljitNativeAggregateStateSourcePlan aggregate_state_source;
	ExecutionRegionScanFilterMode scan_filter_mode = ExecutionRegionScanFilterMode::NONE;
	vector<LogicalType> source_output_types;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
};

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable,
                                SljitFullPipelineRecipePlan &recipe_plan, string &error);

} // namespace duckdb

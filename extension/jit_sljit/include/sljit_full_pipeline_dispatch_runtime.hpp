//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_hash_join_projected_aggregate_runtime.hpp"
#include "sljit_mark_join_aggregate_runtime.hpp"
#include "sljit_native_pipeline_runtime.hpp"
#include "sljit_projection_count_star_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_routes.hpp"
#include "sljit_source_pipeline_runtime.hpp"
#include "sljit_two_join_drain_runtime.hpp"

#include <array>

namespace duckdb {

template <class KERNEL>
class SljitFullPipelineRuntimeDispatcher {
public:
	SljitFullPipelineRuntimeDispatcher(KERNEL &kernel_p, ExecutionRegionRuntime &runtime_p,
	                                   ExecutionRegionResult &result_p, vector<SljitExecutableRegionOp> &ops_p,
	                                   const vector<idx_t> &source_distinct_counts_p,
	                                   const vector<Value> &source_min_values_p,
	                                   const vector<Value> &source_max_values_p, bool uses_scan_filters_p)
	    : kernel(kernel_p), runtime(runtime_p), result(result_p), ops(ops_p),
	      source_distinct_counts(source_distinct_counts_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p), uses_scan_filters(uses_scan_filters_p) {
	}

	bool TryExecute() {
		SljitFullPipelineRouteSelector route_selector(ops, source_min_values, source_max_values, uses_scan_filters);
		auto execute = SelectRuntimeRoute(route_selector);
		if (execute) {
			return (this->*execute)(route_selector);
		}
		return TryExecuteUnbatched(route_selector);
	}

private:
	using RuntimeRouteExecutor = bool (SljitFullPipelineRuntimeDispatcher::*)(const SljitFullPipelineRouteSelector &);

	struct RuntimeRoute {
		constexpr RuntimeRoute(SljitFullPipelineRouteKind kind, RuntimeRouteExecutor execute)
		    : kind(kind), execute(execute) {
		}

		SljitFullPipelineRouteKind kind;
		RuntimeRouteExecutor execute;
	};

	static const std::array<RuntimeRoute, 18> &RuntimeRoutes() {
		static const std::array<RuntimeRoute, 18> routes = {
		    {{SljitFullPipelineRouteKind::FILTERED_SOURCE_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteBatched},
		     {SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteGeneratedFilterBatched},
		     {SljitFullPipelineRouteKind::PROJECTION_COUNT_STAR_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteProjectionCountStarGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteHashJoinProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_DELIM_JOIN_SINK,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteSourceInputBatched},
		     {SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteGeneratedFilterProjectionHashJoinBuildSinkBatched},
		     {SljitFullPipelineRouteKind::GENERATED_PROJECTION_FILTER_PROJECTION_HASH_JOIN_BUILD_SINK,
		      &SljitFullPipelineRuntimeDispatcher::
		          TryExecuteGeneratedProjectionFilterProjectionHashJoinBuildSinkBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_BUILD_SINK,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteSourceInputBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_APPEND_SINK,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteSourceInputBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteHashJoinProjectionProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::MARK_HASH_JOIN_FILTER_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteMarkHashJoinFilterProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::PROJECTION_HASH_JOIN_PROJECTION_CHAIN_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteProjectionHashJoinProjectionChainGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTIONS_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::
		          TryExecuteHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::
		          PROJECTION_HASH_JOIN_PROJECTION_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::
		          TryExecuteProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteHashJoinHashJoinProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_PROJECTION_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::
		          TryExecuteHashJoinHashJoinProjectionProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::GENERATED_FILTER_PROJECTION_HASH_JOIN_PROJECTION_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::TryExecuteGeneratedFilterHashJoinProjectionGroupedAggregateBatched},
		     {SljitFullPipelineRouteKind::HASH_JOIN_HASH_JOIN_FILTER_PROJECTION_CHAIN_GROUPED_AGGREGATE,
		      &SljitFullPipelineRuntimeDispatcher::
		          TryExecuteHashJoinHashJoinFilterProjectionChainGroupedAggregateBatched}}};
		return routes;
	}

	RuntimeRouteExecutor SelectRuntimeRoute(const SljitFullPipelineRouteSelector &route_selector) const {
		auto route_kind = route_selector.SelectFullPipelineRouteKind();
		if (route_kind == SljitFullPipelineRouteKind::NONE) {
			return nullptr;
		}
		for (auto &route : RuntimeRoutes()) {
			if (route.kind == route_kind) {
				return route.execute;
			}
		}
		return nullptr;
	}

	auto NativePipelineExecutor() {
		return SljitMakeNativePipelineExecutor(kernel, runtime, ops, source_distinct_counts);
	}

	bool TryExecuteProjectionCountStarGroupedAggregateBatched(const SljitFullPipelineRouteSelector &route_selector) {
		auto execute_unbatched = [&]() {
			return TryExecuteUnbatched(route_selector);
		};
		auto execute_native_aggregate_update = [&](SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
		                                           SljitExecutableRegionOp &aggregate_op, DataChunk &input) {
			return SljitExecuteNativeAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch, aggregate_idx,
			                                         aggregate_op, input);
		};
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatched(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	}

	bool TryExecuteSourceInputBatched(const SljitFullPipelineRouteSelector &) {
		auto execute_native_full_pipeline = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineSourceInputBatched(runtime, result, ops, execute_native_full_pipeline);
	}

	bool TryExecuteGeneratedFilterProjectionHashJoinBuildSinkBatched(const SljitFullPipelineRouteSelector &) {
		auto execute_native_full_pipeline_from = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineGeneratedFilterProjectionHashJoinBuildSinkBatched(
		    runtime, result, ops, execute_native_full_pipeline_from);
	}

	bool TryExecuteGeneratedProjectionFilterProjectionHashJoinBuildSinkBatched(const SljitFullPipelineRouteSelector &) {
		auto execute_native_full_pipeline_from = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineGeneratedProjectionFilterProjectionHashJoinBuildSinkBatched(
		    runtime, result, ops, execute_native_full_pipeline_from);
	}

	bool TryExecuteUnbatched(const SljitFullPipelineRouteSelector &route_selector) {
		auto execute_native_full_pipeline = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineUnbatched(
		    runtime, result, ops, route_selector.UsesExtendedSourceFetchBudget(), execute_native_full_pipeline);
	}

	bool TryExecuteBatched(const SljitFullPipelineRouteSelector &) {
		auto execute_native_full_pipeline = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineBatched(runtime, result, ops, execute_native_full_pipeline);
	}

	bool TryExecuteGeneratedFilterBatched(const SljitFullPipelineRouteSelector &) {
		auto execute_native_full_pipeline_from = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineGeneratedFilterBatched(runtime, result, ops,
		                                                         execute_native_full_pipeline_from);
	}

	bool TryExecuteHashJoinProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			return SljitPrepareSourceChunkAsJoinInput(runtime, result, source_chunk, source_result, join_input);
		};
		return TryExecuteHashJoinProjectionGroupedAggregateBatched(0, 1, 2, prepare_join_input);
	}

	bool TryExecuteHashJoinProjectionProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			return SljitPrepareSourceChunkAsJoinInput(runtime, result, source_chunk, source_result, join_input);
		};
		return TryExecuteHashJoinProjectedGroupedAggregateBatched(0, 2, 3, prepare_join_input);
	}

	bool TryExecuteMarkHashJoinFilterProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto &native_runtime = runtime.ExecutionOperators();
		auto execute_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		auto execute_native_full_pipeline_from = NativePipelineExecutor();
		return SljitTryExecuteFullPipelineMarkHashJoinFilterProjectionGroupedAggregateBatched(
		    runtime, result, ops, execute_hash_join_probe, execute_native_full_pipeline_from);
	}

	bool TryExecuteProjectionHashJoinProjectionChainGroupedAggregateBatched(
	    const SljitFullPipelineRouteSelector &route_selector) {
		static constexpr idx_t PRE_JOIN_PROJECTION_IDX = 0;
		static constexpr idx_t HASH_JOIN_IDX = 1;
		static constexpr idx_t FIRST_POST_JOIN_PROJECTION_IDX = 2;
		const auto aggregate_idx = ops.size() - 1;
		const auto final_projection_idx = aggregate_idx - 1;
		const bool bypass_pre_join_projection = route_selector.CanBypassInt64ToInt32PreJoinProjection();
		const bool first_join_unchecked_key_cast =
		    bypass_pre_join_projection &&
		    route_selector.CanUseUncheckedInt64ToInt32PreJoinProjection(PRE_JOIN_PROJECTION_IDX, HASH_JOIN_IDX);
		SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
		const bool fast_string_set_case_projection =
		    route_selector.TryBuildStringSetCaseGroupedPayloadProjection(string_set_case_projection);
		SljitPostJoinProjectionStrategy post_join_projection;
		post_join_projection.Initialize(HASH_JOIN_IDX, FIRST_POST_JOIN_PROJECTION_IDX, final_projection_idx);
		if (fast_string_set_case_projection) {
			post_join_projection.EnableStringSetCaseGroupedPayload(string_set_case_projection);
		}
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk,
			                                 source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
			}
			auto &pre_join_projection_op = ops[PRE_JOIN_PROJECTION_IDX];
			SljitPrepareOptionalPreJoinProjectionInput(runtime, scratch, PRE_JOIN_PROJECTION_IDX,
			                                           pre_join_projection_op, source_chunk, bypass_pre_join_projection,
			                                           join_input);
			return false;
		};
		SljitDirectJoinOutputAggregateStrategy direct_join_output_aggregate_strategy(
		    SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN, aggregate_idx);
		SljitDirectJoinOutputAggregatePolicy direct_join_output_aggregate(direct_join_output_aggregate_strategy);
		return TryExecuteHashJoinProjectedGroupedAggregateBatched(aggregate_idx, prepare_join_input,
		                                                          post_join_projection, direct_join_output_aggregate,
		                                                          first_join_unchecked_key_cast);
	}

	bool
	TryExecuteHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		return SljitTryExecuteHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched(kernel, runtime, result,
		                                                                                   ops);
	}

	bool TryExecuteProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateBatched(
	    const SljitFullPipelineRouteSelector &route_selector) {
		const idx_t pre_join_projection_idx = 0;
		const idx_t first_hash_join_idx = 1;
		auto &native_runtime = runtime.ExecutionOperators();
		SljitProjectionTwoJoinProjectionChainRouteConfig config;
		config.bypass_pre_join_projection =
		    route_selector.CanBypassInt64ToInt32PreJoinProjection(pre_join_projection_idx, first_hash_join_idx);
		config.first_join_unchecked_key_cast =
		    config.bypass_pre_join_projection &&
		    route_selector.CanUseUncheckedInt64ToInt32PreJoinProjection(pre_join_projection_idx, first_hash_join_idx);
		config.direct_first_join_to_second_join =
		    route_selector.TryBuildDirectSecondJoinInputProjection(config.direct_second_join_projection);
		auto execute_recorded_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		return SljitTryExecuteProjectionTwoJoinProjectionChainGroupedAggregateBatched(runtime, result, ops, config,
		                                                                              execute_recorded_hash_join_probe);
	}

	bool TryExecuteHashJoinHashJoinProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto &native_runtime = runtime.ExecutionOperators();
		auto execute_recorded_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		return SljitTryExecuteHashJoinHashJoinProjectionGroupedAggregateBatched(runtime, result, ops,
		                                                                        execute_recorded_hash_join_probe);
	}

	bool TryExecuteHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto &native_runtime = runtime.ExecutionOperators();
		auto execute_recorded_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		return SljitTryExecuteHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(
		    runtime, result, ops, execute_recorded_hash_join_probe);
	}

	bool
	TryExecuteHashJoinHashJoinFilterProjectionChainGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto &native_runtime = runtime.ExecutionOperators();
		auto execute_recorded_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		auto execute_native_full_pipeline_from = NativePipelineExecutor();
		return SljitTryExecuteHashJoinHashJoinFilterProjectionChainGroupedAggregateBatched(
		    runtime, result, ops, execute_recorded_hash_join_probe, execute_native_full_pipeline_from);
	}

	bool TryExecuteGeneratedFilterHashJoinProjectionGroupedAggregateBatched(const SljitFullPipelineRouteSelector &) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk,
			                                 source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
			}

			auto &filtered = scratch.TemporaryChunk(1);
			filtered.Reset();
			auto stage_start = SljitRegionStageStart(runtime);
			SljitExecuteFilterProjection(scratch, ops[0], ops[1], 1, source_chunk, filtered,
			                             scratch.FilterSelection(0));
			RecordSljitRegionStageRuntimeWithSuffix(runtime, 0, ops[0].kind, "+projection", stage_start);
			join_input = filtered.size() == 0 ? nullptr : &filtered;
			return false;
		};
		return TryExecuteHashJoinProjectionGroupedAggregateBatched(2, 3, 4, prepare_join_input);
	}

	template <class PREPARE_JOIN_INPUT>
	bool TryExecuteHashJoinProjectionGroupedAggregateBatched(idx_t hash_join_idx, idx_t projection_idx,
	                                                         idx_t aggregate_idx,
	                                                         PREPARE_JOIN_INPUT prepare_join_input) {
		SljitPostJoinProjectionStrategy post_join_projection;
		post_join_projection.Initialize(hash_join_idx, projection_idx, projection_idx);
		SljitDirectJoinOutputAggregateStrategy direct_join_output_aggregate_strategy(
		    SljitDirectJoinOutputAggregateMode::SINGLE_PROJECTION, aggregate_idx);
		SljitDirectJoinOutputAggregatePolicy direct_join_output_aggregate(direct_join_output_aggregate_strategy);
		return TryExecuteHashJoinProjectedGroupedAggregateBatched(aggregate_idx, prepare_join_input,
		                                                          post_join_projection, direct_join_output_aggregate);
	}

	template <class PREPARE_JOIN_INPUT>
	bool TryExecuteHashJoinProjectedGroupedAggregateBatched(idx_t hash_join_idx, idx_t final_projection_idx,
	                                                        idx_t aggregate_idx, PREPARE_JOIN_INPUT prepare_join_input,
	                                                        bool source_key0_int64_to_int32_unchecked = false) {
		SljitPostJoinProjectionStrategy post_join_projection;
		post_join_projection.Initialize(hash_join_idx, hash_join_idx + 1, final_projection_idx);
		SljitDirectJoinOutputAggregateStrategy direct_join_output_aggregate_strategy(
		    SljitDirectJoinOutputAggregateMode::PROJECTION_CHAIN, aggregate_idx);
		SljitDirectJoinOutputAggregatePolicy direct_join_output_aggregate(direct_join_output_aggregate_strategy);
		return TryExecuteHashJoinProjectedGroupedAggregateBatched(aggregate_idx, prepare_join_input,
		                                                          post_join_projection, direct_join_output_aggregate,
		                                                          source_key0_int64_to_int32_unchecked);
	}

	template <class PREPARE_JOIN_INPUT>
	bool TryExecuteHashJoinProjectedGroupedAggregateBatched(
	    idx_t aggregate_idx, PREPARE_JOIN_INPUT prepare_join_input,
	    SljitPostJoinProjectionStrategy &post_join_projection,
	    SljitDirectJoinOutputAggregatePolicy &direct_join_output_aggregate,
	    bool source_key0_int64_to_int32_unchecked = false) {
		auto &native_runtime = runtime.ExecutionOperators();
		auto execute_hash_join_probe = SljitMakeRecordedHashJoinProbeCallback(kernel, runtime, native_runtime);
		return SljitTryExecuteHashJoinProjectedGroupedAggregateBatchedRoute(
		    runtime, result, ops, aggregate_idx, prepare_join_input, post_join_projection, direct_join_output_aggregate,
		    source_key0_int64_to_int32_unchecked, execute_hash_join_probe);
	}

private:
	KERNEL &kernel;
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	const vector<idx_t> &source_distinct_counts;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_scan_filters;
};

template <class KERNEL>
static bool SljitTryExecuteFullPipelineRuntimeRoute(KERNEL &kernel, ExecutionRegionRuntime &runtime,
                                                    ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
                                                    const vector<idx_t> &source_distinct_counts,
                                                    const vector<Value> &source_min_values,
                                                    const vector<Value> &source_max_values, bool uses_scan_filters) {
	SljitFullPipelineRuntimeDispatcher<KERNEL> dispatcher(kernel, runtime, result, ops, source_distinct_counts,
	                                                      source_min_values, source_max_values, uses_scan_filters);
	return dispatcher.TryExecute();
}

} // namespace duckdb

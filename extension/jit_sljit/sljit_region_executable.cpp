#include "sljit_region_executable.hpp"

#include "sljit_executable_aggregate_codegen.hpp"
#include "sljit_dense_group_domain.hpp"
#include "sljit_executable_expression_codegen.hpp"
#include "sljit_executable_stats.hpp"
#include "sljit_full_pipeline_recipe.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

namespace duckdb {

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error, const vector<bool> &input_not_null,
                                    const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                    bool defer_filter_codegen) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.input_types = op.input_types;
	executable.input_not_null = input_not_null;
	executable.input_min_values = input_min_values;
	executable.input_max_values = input_max_values;
	executable.output_types = op.output_types;
	executable.output_not_null = SljitBuildExecutableOutputNotNull(op, input_not_null);
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER: {
		SljitPrepareExecutableFilter(op.filter, executable, &input_not_null, true);
		return defer_filter_codegen || SljitCompilePreparedExecutableFilter(executable, error);
	}
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.hash_join_probe.plan = op.hash_join_probe.Copy(false);
		if (op.hash_join_probe.residual_predicate &&
		    !SljitPrepareAndCompileExecutableRegionExpression(op.hash_join_probe.residual_filter, true,
		                                                      executable.hash_join_probe.residual_filter, error,
		                                                      &op.hash_join_probe.residual_source_not_null)) {
			return false;
		}
		return executable.hash_join_probe.ValidateDeferredCodegen(error);
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.hash_join_build.plan.sink_info = op.hash_join_build.sink_info;
		executable.hash_join_build.plan.input_types = op.hash_join_build.input_types;
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		executable.order_sink.plan.sink_info = op.order_sink.sink_info;
		executable.order_sink.plan.input_types = op.order_sink.input_types;
		executable.order_sink.plan.key_types = op.order_sink.key_types;
		executable.order_sink.order_keys.reserve(op.order_sink.order_keys.size());
		for (auto &order_key : op.order_sink.order_keys) {
			SljitExecutableRegionExpression executable_order_key;
			if (!SljitPrepareAndCompileExecutableRegionExpression(order_key, false, executable_order_key, error,
			                                                      &input_not_null)) {
				return false;
			}
			executable.order_sink.order_keys.push_back(std::move(executable_order_key));
		}
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK:
		executable.append_sink.plan.sink_info = op.append_sink.sink_info;
		executable.append_sink.plan.input_types = op.append_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		SljitBuildExecutableAggregateUpdateMetadata(op.aggregate_update, executable.aggregate_update, input_not_null);
		if (!SljitBuildExecutableAggregateUpdatePayloadCode(op.aggregate_update, executable.aggregate_update, error,
		                                                    input_not_null, input_min_values, input_max_values)) {
			return false;
		}
		SljitPlanExecutablePrimitiveRunUpdate(op.aggregate_update, executable.aggregate_update);
		SljitSelectExecutableAggregateDirectUpdatePlan(executable.aggregate_update);
		return true;
	case SljitNativeRegionOpKind::PROJECTION:
		executable.projections.reserve(op.projections.size());
		for (auto &projection : op.projections) {
			SljitExecutableRegionExpression executable_projection;
			if (!SljitPrepareAndCompileExecutableRegionExpression(projection, false, executable_projection, error,
			                                                      &input_not_null)) {
				return false;
			}
			executable.projections.push_back(std::move(executable_projection));
		}
		return SljitTryBuildFlatFusedProjections(executable, error);
	default:
		throw InternalException("Unknown SLJIT native region operator kind");
	}
}

static void SljitTryBuildExecutableAggregateDenseGroupDomain(const SljitNativeAggregateUpdatePlan &plan,
                                                             const vector<idx_t> &current_distinct_counts,
                                                             const vector<Value> &current_min_values,
                                                             const vector<Value> &current_max_values,
                                                             ExecutionDenseGroupDomain &domain) {
	domain = ExecutionDenseGroupDomain();
	auto &sink_info = plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.size() != 1) {
		return;
	}
	auto &group = sink_info.groups[0];
	if (!group.supported_reference || group.input_index >= current_distinct_counts.size() ||
	    group.input_index >= current_min_values.size() || group.input_index >= current_max_values.size()) {
		return;
	}
	SljitTryBuildDenseGroupDomainFromStats(group.type.InternalType(), current_distinct_counts[group.input_index],
	                                       current_min_values[group.input_index], current_max_values[group.input_index],
	                                       domain);
}

static bool SljitTryReadSignedIntegralGroupKey(const Value &value, int64_t &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		result = value.GetValueUnsafe<bool>() ? 1 : 0;
		return true;
	case PhysicalType::INT8:
		result = value.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		result = value.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		result = value.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static void SljitBuildExecutableIntegralGroupKeyRanges(const SljitNativeAggregateUpdatePlan &plan,
                                                       const vector<Value> &current_min_values,
                                                       const vector<Value> &current_max_values,
                                                       vector<SljitExecutableIntegralGroupKeyRange> &ranges) {
	auto &groups = plan.sink_info.groups;
	ranges.assign(groups.size(), SljitExecutableIntegralGroupKeyRange());
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		if (!group.supported_reference || group.input_index >= current_min_values.size() ||
		    group.input_index >= current_max_values.size()) {
			continue;
		}
		auto &min_value = current_min_values[group.input_index];
		auto &max_value = current_max_values[group.input_index];
		if (min_value.type().InternalType() != max_value.type().InternalType()) {
			continue;
		}
		auto &range = ranges[group_idx];
		range.source_physical_type = min_value.type().InternalType();
		range.ready = SljitTryReadSignedIntegralGroupKey(min_value, range.min_value) &&
		              SljitTryReadSignedIntegralGroupKey(max_value, range.max_value) &&
		              range.min_value <= range.max_value;
	}
}

//! A per-thread bitmap of this many key combinations costs at most 512 KiB
static constexpr idx_t SLJIT_DISTINCT_GROUP_DOMAIN_LIMIT = idx_t(1) << 22;

//! A hash aggregate without aggregates only needs each group once: rows repeating a group this thread already handed
//! to the sink can be dropped when the group keys span a small bounded domain
static idx_t SljitBuildExecutableDistinctGroupDomainSize(const SljitExecutableAggregateUpdate &aggregate_update) {
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || !sink_info.aggregates.empty() ||
	    sink_info.groups.empty() || aggregate_update.integral_group_key_ranges.size() != sink_info.groups.size()) {
		return 0;
	}
	idx_t domain_size = 1;
	for (auto &range : aggregate_update.integral_group_key_ranges) {
		if (!range.ready) {
			return 0;
		}
		const auto span = static_cast<uint64_t>(range.max_value) - static_cast<uint64_t>(range.min_value) + 1;
		if (span == 0 || span > SLJIT_DISTINCT_GROUP_DOMAIN_LIMIT ||
		    domain_size > SLJIT_DISTINCT_GROUP_DOMAIN_LIMIT / span) {
			return 0;
		}
		domain_size *= span;
	}
	return domain_size;
}

static bool SljitTryMultiplyGroupReserveCount(idx_t left, idx_t right, idx_t &result) {
	if (left == 0 || right == 0 || left > NumericLimits<idx_t>::Maximum() / right) {
		return false;
	}
	result = left * right;
	return true;
}

static void SljitTryBuildExecutableAggregateGroupReservePlan(const SljitNativeAggregateUpdatePlan &plan,
                                                             const vector<idx_t> &current_distinct_counts,
                                                             SljitAggregateGroupReservePlan &reserve) {
	reserve = SljitAggregateGroupReservePlan();
	auto &sink_info = plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty()) {
		return;
	}
	idx_t reserve_count = 1;
	for (auto &group : sink_info.groups) {
		if (!group.supported_reference || group.input_index >= current_distinct_counts.size()) {
			return;
		}
		const auto group_distinct_count = current_distinct_counts[group.input_index];
		if (!SljitTryMultiplyGroupReserveCount(reserve_count, group_distinct_count, reserve_count)) {
			return;
		}
	}
	if (plan.estimated_input_count > 0 && reserve_count >= plan.estimated_input_count) {
		return;
	}
	reserve.has_group_count = true;
	reserve.group_count = reserve_count;
}

static bool SljitCanBuildFilteredAggregateUpdate(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx == 0 || op_idx + 1 != ops.size() || ops[op_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[op_idx - 1].kind == SljitNativeRegionOpKind::FILTER;
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable,
                                SljitFullPipelineRecipePlan &recipe_plan, string &error) {
	executable.scan_filter_mode = region.scan_filter_mode;
	executable.aggregate_state_source = region.aggregate_state_source;
	executable.source_output_types = region.source_output_types;
	executable.source_distinct_counts = region.source_distinct_counts;
	executable.source_min_values = region.source_min_values;
	executable.source_max_values = region.source_max_values;
	executable.scan_filters.reserve(region.scan_filters.size());
	for (auto &scan_filter : region.scan_filters) {
		SljitExecutableRegionOp filter_op;
		filter_op.kind = SljitNativeRegionOpKind::FILTER;
		filter_op.input_types.push_back(scan_filter.input_type);
		filter_op.output_types = filter_op.input_types;
		vector<bool> input_not_null {scan_filter.input_not_null};
		string scan_filter_error;
		if (!SljitPrepareAndCompileExecutableFilter(scan_filter.filter, filter_op, scan_filter_error, &input_not_null,
		                                            true)) {
			continue;
		}
		SljitExecutableScanFilter executable_filter;
		executable_filter.filter_index = scan_filter.filter_index;
		executable_filter.input_type = scan_filter.input_type;
		executable_filter.filter = std::move(filter_op.filter);
		executable.scan_filters.push_back(std::move(executable_filter));
	}
	executable.ops.reserve(region.ops.size());
	auto current_not_null = region.source_not_null;
	auto current_distinct_counts = region.source_distinct_counts;
	auto current_distinct_reserve_counts = region.source_distinct_reserve_counts;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		SljitExecutableRegionOp executable_op;
		const bool defer_filter_codegen = op.kind == SljitNativeRegionOpKind::FILTER &&
		                                  op_idx + 2 == region.ops.size() &&
		                                  region.ops[op_idx + 1].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE;
		if (!BuildExecutableRegionOp(op, executable_op, error, current_not_null, current_min_values, current_max_values,
		                             defer_filter_codegen)) {
			return false;
		}
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			SljitTryBuildExecutableAggregateDenseGroupDomain(op.aggregate_update, current_distinct_counts,
			                                                 current_min_values, current_max_values,
			                                                 executable_op.aggregate_update.dense_group_domain);
			SljitBuildExecutableIntegralGroupKeyRanges(op.aggregate_update, current_min_values, current_max_values,
			                                           executable_op.aggregate_update.integral_group_key_ranges);
			executable_op.aggregate_update.distinct_group_domain_size =
			    SljitBuildExecutableDistinctGroupDomainSize(executable_op.aggregate_update);
			SljitTryBuildExecutableAggregateGroupReservePlan(op.aggregate_update, current_distinct_reserve_counts,
			                                                 executable_op.aggregate_update.plan.group_reserve);
		}
		executable.ops.push_back(std::move(executable_op));
		if (SljitCanBuildFilteredAggregateUpdate(region.ops, op_idx)) {
			auto &filter_op = executable.ops[op_idx - 1];
			auto &aggregate_update_op = executable.ops[op_idx];
			if (!SljitTryBuildFilteredAggregateUpdate(filter_op, aggregate_update_op, error, current_not_null,
			                                          current_min_values, current_max_values)) {
				return false;
			}
		}
		SljitUpdateExecutableCurrentNotNull(op, current_not_null);
		SljitUpdateExecutableCurrentDistinctReserveCounts(op, current_distinct_reserve_counts, current_distinct_counts,
		                                                  current_min_values, current_max_values);
		SljitUpdateExecutableCurrentDistinctCounts(op, current_distinct_counts, current_min_values, current_max_values);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}

	// Recipe binding is the executable ownership boundary. A fused aggregate step
	// owns the predicate named by that step; every other recipe (including native tail) and
	// native-only execution needs the standalone selector. Bind once with prepared
	// filters, publish only the selectors that recipe cannot own, then move the
	// same final plan into the kernel.
	recipe_plan = BuildSljitFullPipelineRecipePlan(executable.ops, executable.aggregate_state_source,
	                                               executable.source_output_types, executable.source_min_values,
	                                               executable.source_max_values);
	for (idx_t op_idx = 0; op_idx < executable.ops.size(); op_idx++) {
		auto &op = executable.ops[op_idx];
		if (op.kind != SljitNativeRegionOpKind::FILTER || !op.filter || op.filter->expression.HasSelectionKernel()) {
			continue;
		}
		if (recipe_plan.HasRecipe() && recipe_plan.Recipe().OwnsFusedFilter(op_idx)) {
			continue;
		}
		if (!SljitCompilePreparedExecutableFilter(op, error)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

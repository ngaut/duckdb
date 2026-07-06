#include "sljit_region_executable.hpp"

#include "sljit_executable_aggregate_codegen.hpp"
#include "sljit_dense_group_domain.hpp"
#include "sljit_executable_expression_codegen.hpp"
#include "sljit_executable_stats.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

namespace duckdb {

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error, const vector<bool> &input_not_null,
                                    const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                    bool build_filter_code = true, bool build_aggregate_update_payload_code = true) {
	executable.kind = op.kind;
	executable.operator_index = op.operator_index;
	executable.input_types = op.input_types;
	executable.output_types = op.output_types;
	executable.output_not_null = SljitBuildExecutableOutputNotNull(op, input_not_null);
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		SljitPrepareExecutableRegionExpression(op.filter, executable.filter, &input_not_null, !build_filter_code);
		if (!build_filter_code) {
			return true;
		}
		return SljitCompilePreparedExecutableRegionExpression(executable.filter, true, error);
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
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		executable.nested_loop_join_probe.plan.operator_index = op.nested_loop_join_probe.operator_index;
		executable.nested_loop_join_probe.plan.input_types = op.nested_loop_join_probe.input_types;
		executable.nested_loop_join_probe.plan.condition_types = op.nested_loop_join_probe.condition_types;
		executable.nested_loop_join_probe.plan.join_type = op.nested_loop_join_probe.join_type;
		executable.nested_loop_join_probe.plan.operator_info = op.nested_loop_join_probe.operator_info;
		executable.nested_loop_join_probe.plan.conditions.reserve(op.nested_loop_join_probe.conditions.size());
		executable.nested_loop_join_probe.lhs_conditions.reserve(op.nested_loop_join_probe.conditions.size());
		for (auto &condition : op.nested_loop_join_probe.conditions) {
			SljitNativeNestedLoopJoinProbeConditionPlan condition_plan;
			condition_plan.type = condition.type;
			condition_plan.comparison_type = condition.comparison_type;
			condition_plan.value_kind = condition.value_kind;
			condition_plan.lhs_condition = condition.lhs_condition.Copy(false, false);
			executable.nested_loop_join_probe.plan.conditions.push_back(std::move(condition_plan));

			SljitExecutableRegionExpression executable_condition;
			if (!SljitPrepareAndCompileExecutableRegionExpression(condition.lhs_condition, false, executable_condition,
			                                                      error, &input_not_null)) {
				return false;
			}
			executable.nested_loop_join_probe.lhs_conditions.push_back(std::move(executable_condition));
		}
		executable.nested_loop_join_probe.code = BuildSljitNestedLoopJoinProbe(
		    executable.nested_loop_join_probe.plan, executable.nested_loop_join_probe.function, error);
		return executable.nested_loop_join_probe.code != nullptr &&
		       executable.nested_loop_join_probe.function != nullptr;
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		executable.nested_loop_join_build.plan.sink_info = op.nested_loop_join_build.sink_info;
		executable.nested_loop_join_build.plan.input_types = op.nested_loop_join_build.input_types;
		executable.nested_loop_join_build.plan.condition_types = op.nested_loop_join_build.condition_types;
		executable.nested_loop_join_build.rhs_conditions.reserve(op.nested_loop_join_build.rhs_conditions.size());
		for (auto &condition : op.nested_loop_join_build.rhs_conditions) {
			SljitExecutableRegionExpression executable_condition;
			if (!SljitPrepareAndCompileExecutableRegionExpression(condition, false, executable_condition, error,
			                                                      &input_not_null)) {
				return false;
			}
			executable.nested_loop_join_build.rhs_conditions.push_back(std::move(executable_condition));
		}
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
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		if (op.delim_join_sink.sink_info.kind != ExecutionRegionSinkKind::DELIM_JOIN_SINK) {
			error = "SLJIT delimiter join sink executable is missing delimiter sink info";
			return false;
		}
		executable.delim_join_sink.plan.sink_info = op.delim_join_sink.sink_info;
		executable.delim_join_sink.plan.input_types = op.delim_join_sink.input_types;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		SljitBuildExecutableAggregateUpdateMetadata(op.aggregate_update, executable.aggregate_update, input_not_null);
		if (!build_aggregate_update_payload_code) {
			return true;
		}
		if (!SljitBuildExecutableAggregateUpdatePayloadCode(op.aggregate_update, executable.aggregate_update, error,
		                                                    input_not_null, input_min_values, input_max_values)) {
			return false;
		}
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

static bool SljitCanDeferAggregateUpdatePayloadCode(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx == 0 || op_idx + 1 != ops.size() || ops[op_idx].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[op_idx - 1].kind == SljitNativeRegionOpKind::FILTER;
}

bool BuildSljitExecutableRegion(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable, string &error) {
	executable.source_output_types = region.source_output_types;
	executable.source_distinct_counts = region.source_distinct_counts;
	executable.source_distinct_reserve_counts = region.source_distinct_reserve_counts;
	executable.source_min_values = region.source_min_values;
	executable.source_max_values = region.source_max_values;
	executable.ops.reserve(region.ops.size());
	auto current_not_null = region.source_not_null;
	auto current_distinct_counts = region.source_distinct_counts;
	auto current_distinct_reserve_counts = region.source_distinct_reserve_counts;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		SljitExecutableRegionOp executable_op;
		auto defer_aggregate_payload_code = SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx);
		const auto defer_filter_code = op.kind == SljitNativeRegionOpKind::FILTER && op_idx + 1 < region.ops.size() &&
		                               SljitCanDeferAggregateUpdatePayloadCode(region.ops, op_idx + 1);
		if (!BuildExecutableRegionOp(op, executable_op, error, current_not_null, current_min_values, current_max_values,
		                             !defer_filter_code, !defer_aggregate_payload_code)) {
			return false;
		}
		if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			SljitTryBuildExecutableAggregateDenseGroupDomain(op.aggregate_update, current_distinct_counts,
			                                                 current_min_values, current_max_values,
			                                                 executable_op.aggregate_update.dense_group_domain);
			SljitTryBuildExecutableAggregateGroupReservePlan(op.aggregate_update, current_distinct_reserve_counts,
			                                                 executable_op.aggregate_update.plan.group_reserve);
		}
		executable.ops.push_back(std::move(executable_op));
		if (defer_aggregate_payload_code) {
			auto &aggregate_update_op = executable.ops[op_idx];
			if (!SljitTryBuildFilteredAggregateUpdate(executable.ops[op_idx - 1], aggregate_update_op, error,
			                                          current_not_null, current_min_values, current_max_values)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable() &&
			    !SljitCompilePreparedExecutableRegionExpression(executable.ops[op_idx - 1].filter, true, error)) {
				return false;
			}
			if (!aggregate_update_op.aggregate_update.filtered_update.IsExecutable()) {
				if (!SljitBuildExecutableAggregateUpdatePayloadCode(
				        op.aggregate_update, aggregate_update_op.aggregate_update, error, current_not_null,
				        current_min_values, current_max_values)) {
					return false;
				}
				SljitSelectExecutableAggregateDirectUpdatePlan(aggregate_update_op.aggregate_update);
			}
		}
		SljitUpdateExecutableCurrentNotNull(op, current_not_null);
		SljitUpdateExecutableCurrentDistinctReserveCounts(op, current_distinct_reserve_counts, current_distinct_counts,
		                                                  current_min_values, current_max_values);
		SljitUpdateExecutableCurrentDistinctCounts(op, current_distinct_counts, current_min_values, current_max_values);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}
	return true;
}

} // namespace duckdb

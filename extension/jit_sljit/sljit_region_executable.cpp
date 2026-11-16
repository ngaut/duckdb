#include "sljit_region_executable.hpp"

#include "sljit_executable_aggregate_codegen.hpp"
#include "sljit_dense_group_domain.hpp"
#include "sljit_executable_expression_codegen.hpp"
#include "sljit_executable_stats.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe.hpp"
#include "sljit_join_probe_codegen.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

namespace duckdb {

static bool BuildExecutableRegionOp(const SljitNativeRegionOpPlan &op, SljitExecutableRegionOp &executable,
                                    string &error, const vector<bool> &input_not_null,
                                    const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                    bool defer_filter_codegen) {
	executable.SetKind(op.Kind());
	executable.operator_index = op.operator_index;
	executable.input_is_operator_input = op.input_is_operator_input;
	executable.input_types = op.input_types;
	executable.input_not_null = input_not_null;
	executable.input_min_values = input_min_values;
	executable.input_max_values = input_max_values;
	executable.output_types = op.output_types;
	executable.output_not_null = SljitBuildExecutableOutputNotNull(op, input_not_null);
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::FILTER: {
		SljitPrepareExecutableFilter(op.Filter(), executable, &input_not_null, true);
		return defer_filter_codegen || SljitCompilePreparedExecutableFilter(executable, error);
	}
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		executable.HashJoinProbe().plan = op.HashJoinProbe().Copy(false);
		if (op.HashJoinProbe().residual_predicate &&
		    !SljitPrepareAndCompileExecutableRegionExpression(op.HashJoinProbe().residual_filter, true,
		                                                      executable.HashJoinProbe().residual_filter, error,
		                                                      &op.HashJoinProbe().residual_source_not_null)) {
			return false;
		}
		return executable.HashJoinProbe().ValidateDeferredCodegen(error);
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		executable.HashJoinBuild().plan.sink_info = op.HashJoinBuild().sink_info;
		executable.HashJoinBuild().plan.input_types = op.HashJoinBuild().input_types;
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		executable.OrderSink().plan.sink_info = op.OrderSink().sink_info;
		executable.OrderSink().plan.input_types = op.OrderSink().input_types;
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK:
		executable.AppendSink().plan.sink_info = op.AppendSink().sink_info;
		executable.AppendSink().plan.input_types = op.AppendSink().input_types;
		return true;
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		SljitBuildExecutableAggregateUpdateMetadata(op.AggregateUpdate(), executable.AggregateUpdate(), input_not_null);
		if (!SljitBuildExecutableAggregateUpdatePayloadCode(op.AggregateUpdate(), executable.AggregateUpdate(), error,
		                                                    input_not_null, input_min_values, input_max_values)) {
			return false;
		}
		SljitPlanExecutablePrimitiveRunUpdate(op.AggregateUpdate(), executable.AggregateUpdate());
		SljitSelectExecutableAggregateDirectUpdatePlan(executable.AggregateUpdate());
		return true;
	case SljitNativeRegionOpKind::PROJECTION:
		executable.Projections().reserve(op.Projections().size());
		for (auto &projection : op.Projections()) {
			SljitExecutableRegionExpression executable_projection;
			if (!SljitPrepareAndCompileExecutableRegionExpression(projection, false, executable_projection, error,
			                                                      &input_not_null)) {
				return false;
			}
			executable.Projections().push_back(std::move(executable_projection));
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
	if (sink_info.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.size() != 1) {
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
	if (sink_info.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || !sink_info.aggregates.empty() ||
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

static bool SljitCanBuildFilteredAggregateUpdate(const vector<SljitNativeRegionOpPlan> &ops, idx_t op_idx) {
	if (op_idx == 0 || op_idx + 1 != ops.size() || ops[op_idx].Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	return ops[op_idx - 1].Kind() == SljitNativeRegionOpKind::FILTER;
}

//! A hash aggregate the region updates hands its pipeline to DuckDB's operators unless generated group runs or an
//! index over a bounded key domain update it
static bool SljitRegionMayContinueNatively(const SljitNativeRegionPlan &region) {
	if (region.ops.empty() || region.ops.back().Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	auto &sink_info = region.ops.back().AggregateUpdate().sink_info;
	return sink_info.Kind() == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	       SljitAggregateSinkAdmitsNativeChunks(sink_info);
}

//! A sum over a string set beside the sum over its complement accumulates the few known groups of its key in a local
//! table
static bool SljitAggregateUpdateIsStringSetComplementarySum(const SljitExecutableAggregateUpdate &aggregate_update) {
	auto &payloads = aggregate_update.payloads;
	if (payloads.size() != 2) {
		return false;
	}
	const auto source_count = payloads[0].plan.expression_tree_source_indices.size();
	for (idx_t reference_index = 0; reference_index < source_count; reference_index++) {
		for (idx_t matching = 0; matching < 2; matching++) {
			std::array<string, 2> matching_constants;
			std::array<string, 2> non_matching_constants;
			if (SljitTryReadStringSetCaseExpression(payloads[matching], reference_index, true, matching_constants) &&
			    SljitTryReadStringSetCaseExpression(payloads[1 - matching], reference_index, false,
			                                        non_matching_constants) &&
			    SljitSameStringConstantSet(matching_constants, non_matching_constants)) {
				return true;
			}
		}
	}
	return false;
}

//! Marking the groups of a small domain already sunk indexes a bitmap, and a string-set complementary sum indexes its
//! known groups, which no hash table lookup matches: such a region keeps its pipeline
static bool SljitAggregateUpdateIndexesBoundedDomain(const SljitExecutableRegionOp &op) {
	return op.AggregateUpdate().distinct_group_domain_size != 0 ||
	       SljitAggregateUpdateIsStringSetComplementarySum(op.AggregateUpdate());
}

//! Whether a projection keeps distinct inputs distinct, so its input has the group runs its output has
static bool SljitProjectionPreservesDistinctness(const SljitNativeRegionExpressionPlan &plan) {
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		return true;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		return plan.binary_op == SljitNativeIntegerBinaryOp::ADD ||
		       plan.binary_op == SljitNativeIntegerBinaryOp::SUBTRACT;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return plan.expression_tree && plan.expression_tree->kind == ExecutionExpressionIRKind::REFERENCE;
	default:
		return false;
	}
}

//! Follows a column back through filters, projections that keep one input column's values distinct and a join
//! probe's probe-side output
static bool SljitTraceColumnToOpInput(const SljitExecutableRegionOp &op, idx_t &column) {
	switch (op.Kind()) {
	case SljitNativeRegionOpKind::FILTER:
		return true;
	case SljitNativeRegionOpKind::PROJECTION: {
		vector<idx_t> sources;
		if (column >= op.Projections().size() || !SljitProjectionPreservesDistinctness(op.Projections()[column].plan) ||
		    !SljitCollectExpressionSourceIndices(op.Projections()[column], sources) || sources.size() != 1) {
			return false;
		}
		column = sources[0];
		return true;
	}
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
		auto &probe = op.HashJoinProbe().plan;
		auto &contract = probe.operator_info.HashJoin().contract;
		if (!contract.present) {
			return false;
		}
		if (column < contract.lhs_output_column_indices.size()) {
			column = contract.lhs_output_column_indices[column];
			return true;
		}
		// a build-side column is a function of the single equality key that found its row: its runs follow the
		// probe key's
		if (probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
		    contract.equality_condition_count != 1 || contract.condition_count != 1 || probe.keys.size() != 1) {
			return false;
		}
		column = probe.keys[0].key_input_index;
		return true;
	}
	default:
		return false;
	}
}

//! The fetched source chunk's column the hash aggregate's single group key derives from
static idx_t SljitTraceGroupKeySourceColumn(const vector<SljitExecutableRegionOp> &ops,
                                            const vector<SljitExecutableRegionOp> &source_ops) {
	auto &groups = ops.back().AggregateUpdate().plan.sink_info.groups;
	if (groups.size() != 1) {
		return DConstants::INVALID_INDEX;
	}
	auto column = groups[0].input_index;
	for (idx_t op_idx = ops.size() - 1; op_idx > 0; op_idx--) {
		if (!SljitTraceColumnToOpInput(ops[op_idx - 1], column)) {
			return DConstants::INVALID_INDEX;
		}
	}
	for (idx_t op_idx = source_ops.size(); op_idx > 0; op_idx--) {
		if (!SljitTraceColumnToOpInput(source_ops[op_idx - 1], column)) {
			return DConstants::INVALID_INDEX;
		}
	}
	return column;
}

static bool BuildSljitNativeContinuation(const SljitNativeRegionPlan &region, SljitExecutableRegion &executable,
                                         string &error) {
	auto continuation = make_uniq<SljitExecutableNativeContinuation>();
	auto current_not_null = region.source_not_null;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (auto &op : region.native_continuation_source_ops) {
		SljitExecutableRegionOp executable_op;
		if (!BuildExecutableRegionOp(op, executable_op, error, current_not_null, current_min_values, current_max_values,
		                             false)) {
			return false;
		}
		if (op.Kind() == SljitNativeRegionOpKind::FILTER && !executable_op.Filter()->expression.HasSelectionKernel() &&
		    !SljitCompilePreparedExecutableFilter(executable_op, error)) {
			return false;
		}
		continuation->source_ops.push_back(std::move(executable_op));
		SljitUpdateExecutableCurrentNotNull(op, current_not_null);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}
	continuation->group_key_source_column = SljitTraceGroupKeySourceColumn(executable.ops, continuation->source_ops);
	continuation->measured =
	    SljitAggregateSinkAdmitsNativeChunks(executable.ops.back().AggregateUpdate().plan.sink_info);
	auto &aggregates = executable.ops.back().AggregateUpdate().plan.sink_info.aggregates;
	continuation->counts_rows_only =
	    aggregates.size() == 1 && (aggregates[0].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT ||
	                               aggregates[0].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR);
	executable.native_continuation = std::move(continuation);
	return true;
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
		filter_op.SetKind(SljitNativeRegionOpKind::FILTER);
		filter_op.input_types.push_back(scan_filter.input_type);
		filter_op.output_types = filter_op.input_types;
		vector<bool> input_not_null {scan_filter.input_not_null};
		if (!SljitPrepareAndCompileExecutableFilter(scan_filter.filter, filter_op, error, &input_not_null, true)) {
			return false;
		}
		SljitExecutableScanFilter executable_filter;
		executable_filter.filter_index = scan_filter.filter_index;
		executable_filter.input_type = scan_filter.input_type;
		executable_filter.filter = std::move(filter_op.Filter());
		executable.scan_filters.push_back(std::move(executable_filter));
	}
	executable.ops.reserve(region.ops.size());
	auto current_not_null = region.source_not_null;
	auto current_distinct_counts = region.source_distinct_counts;
	auto current_min_values = region.source_min_values;
	auto current_max_values = region.source_max_values;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		SljitExecutableRegionOp executable_op;
		const bool defer_filter_codegen = op.Kind() == SljitNativeRegionOpKind::FILTER &&
		                                  op_idx + 2 == region.ops.size() &&
		                                  region.ops[op_idx + 1].Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE;
		if (!BuildExecutableRegionOp(op, executable_op, error, current_not_null, current_min_values, current_max_values,
		                             defer_filter_codegen)) {
			return false;
		}
		if (op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			SljitTryBuildExecutableAggregateDenseGroupDomain(op.AggregateUpdate(), current_distinct_counts,
			                                                 current_min_values, current_max_values,
			                                                 executable_op.AggregateUpdate().dense_group_domain);
			SljitBuildExecutableIntegralGroupKeyRanges(op.AggregateUpdate(), current_min_values, current_max_values,
			                                           executable_op.AggregateUpdate().integral_group_key_ranges);
			executable_op.AggregateUpdate().distinct_group_domain_size =
			    SljitBuildExecutableDistinctGroupDomainSize(executable_op.AggregateUpdate());
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
		SljitUpdateExecutableCurrentDistinctCounts(op, current_distinct_counts, current_min_values, current_max_values);
		SljitUpdateExecutableCurrentRanges(op, current_min_values, current_max_values);
	}

	if (SljitRegionMayContinueNatively(region) && !SljitAggregateUpdateIndexesBoundedDomain(executable.ops.back())) {
		if (!BuildSljitNativeContinuation(region, executable, error)) {
			return false;
		}
	}

	// Recipe binding is the executable ownership boundary. A fused aggregate step
	// owns the predicate named by that step; every other recipe needs the standalone
	// selector. Bind once with prepared filters, publish only the selectors that recipe
	// cannot own, then move the same final plan into the kernel.
	recipe_plan = BuildSljitFullPipelineRecipePlan(executable.ops, executable.aggregate_state_source,
	                                               executable.source_output_types, executable.source_min_values,
	                                               executable.source_max_values);
	if (!recipe_plan.HasRecipe()) {
		error = "SLJIT region binds no recipe: no op hands DuckDB's operators what they take";
		return false;
	}
	for (idx_t op_idx = 0; op_idx < executable.ops.size(); op_idx++) {
		auto &op = executable.ops[op_idx];
		if (op.Kind() != SljitNativeRegionOpKind::FILTER || !op.Filter() ||
		    op.Filter()->expression.HasSelectionKernel()) {
			continue;
		}
		if (recipe_plan.HasRecipe() && recipe_plan.Recipe().OwnsFusedFilter(op_idx)) {
			continue;
		}
		if (!SljitCompilePreparedExecutableFilter(op, error)) {
			return false;
		}
	}
	if (!SljitRegionRunsGeneratedCode(executable.ops, recipe_plan.Recipe(), executable.scan_filter_mode,
	                                  executable.scan_filters)) {
		error = "SLJIT region runs no generated code: DuckDB's pipeline runs it";
		return false;
	}
	return true;
}

} // namespace duckdb

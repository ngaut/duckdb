#include "sljit_region_plan.hpp"
#include "sljit_executable_stats.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

static idx_t AggregateGroupReserveCount(const ExecutionRegionAggregateSinkInfo &sink, idx_t estimated_input_count,
                                        const vector<idx_t> &distinct_counts) {
	if (sink.Kind() != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink.groups.empty()) {
		return 0;
	}
	idx_t count = 1;
	for (auto &group : sink.groups) {
		if (!group.supported_reference || group.input_index >= distinct_counts.size()) {
			return 0;
		}
		const auto distinct_count = distinct_counts[group.input_index];
		if (distinct_count == 0 || count > NumericLimits<idx_t>::Maximum() / distinct_count) {
			return 0;
		}
		count *= distinct_count;
	}
	return estimated_input_count > 0 && count >= estimated_input_count ? 0 : count;
}

vector<SljitAggregateInstanceParameters> BuildSljitAggregateInstanceParameters(const SljitNativeRegionPlan &region) {
	vector<SljitAggregateInstanceParameters> parameters(region.ops.size());
	idx_t aggregate_end = 0;
	for (idx_t op_idx = 0; op_idx < region.ops.size(); op_idx++) {
		if (region.ops[op_idx].Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			aggregate_end = op_idx + 1;
		}
	}
	if (aggregate_end == 0) {
		return parameters;
	}
	auto distinct_counts = region.source_distinct_counts;
	auto reserve_counts = region.source_distinct_reserve_counts;
	auto min_values = region.source_min_values;
	auto max_values = region.source_max_values;
	for (idx_t op_idx = 0; op_idx < aggregate_end; op_idx++) {
		auto &op = region.ops[op_idx];
		if (op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			auto &aggregate = parameters[op_idx];
			aggregate.estimated_input_count = op.estimated_input_count;
			aggregate.group_reserve_count = AggregateGroupReserveCount(op.AggregateUpdate().sink_info,
			                                                           aggregate.estimated_input_count, reserve_counts);
		}
		if (op_idx + 1 == aggregate_end) {
			break;
		}
		SljitUpdateExecutableCurrentDistinctReserveCounts(op, reserve_counts, distinct_counts, min_values, max_values);
		SljitUpdateExecutableCurrentDistinctCounts(op, distinct_counts, min_values, max_values);
		SljitUpdateExecutableCurrentRanges(op, min_values, max_values);
	}
	return parameters;
}

static void BindCastIdentity(QueryLocation &location, string &alias, idx_t &binding,
                             vector<SljitCastErrorIdentity> &errors) {
	if (binding == DConstants::INVALID_INDEX) {
		binding = errors.size();
		errors.push_back({location, std::move(alias)});
	} else if (binding >= errors.size()) {
		throw InternalException("SLJIT cast error slot is outside its instance bindings");
	}
	location = QueryLocation();
	alias.clear();
}

void BindSljitCastErrorSlots(SljitIntegerCastChain &chain, vector<SljitCastErrorIdentity> &errors) {
	for (auto &step : chain.steps) {
		BindCastIdentity(step.query_location, step.source_alias, step.error_binding, errors);
	}
}

static void BindTree(ExecutionExpressionIR &node, vector<SljitCastErrorIdentity> &errors) {
	if (node.kind == ExecutionExpressionIRKind::CAST) {
		BindCastIdentity(node.query_location, node.cast_source_alias, node.cast_error_binding, errors);
	} else {
		node.query_location = QueryLocation();
		node.cast_source_alias.clear();
		node.cast_error_binding = DConstants::INVALID_INDEX;
	}
	for (optional_ptr<ExecutionExpressionIR> child : {node.left.get(), node.right.get(), node.else_node.get()}) {
		if (child) {
			BindTree(*child, errors);
		}
	}
	for (auto &child : node.children) {
		if (child) {
			BindTree(*child, errors);
		}
	}
}

static void BindFragment(ExecutionExpressionFragment &fragment, vector<SljitCastErrorIdentity> &errors) {
	if (fragment.root) {
		BindTree(*fragment.root, errors);
	}
	fragment.reason.clear();
	fragment.ir.clear();
}

static void BindHashJoin(ExecutionRegionHashJoinContract &contract, vector<ExecutionRegionHashJoinKeyInput> &keys,
                         vector<SljitCastErrorIdentity> &errors) {
	BindFragment(contract.residual_expression, errors);
	for (auto &key : keys) {
		BindFragment(key.expression, errors);
		key.reason.clear();
	}
}

static void BindHashJoinInfo(ExecutionRegionOperatorInfo &info, vector<SljitCastErrorIdentity> &errors) {
	info.fields.clear();
	info.reason.clear();
	info.ir.clear();
	BindHashJoin(info.HashJoin().contract, info.HashJoin().keys, errors);
}

static void BindSink(ExecutionRegionAggregateSinkInfo &sink, vector<SljitCastErrorIdentity> &errors) {
	for (auto &aggregate : sink.aggregates) {
		aggregate.reason.clear();
		for (auto &child : aggregate.child_expressions) {
			BindFragment(child, errors);
		}
	}
	for (auto &group : sink.groups) {
		group.reason.clear();
		BindFragment(group.expression, errors);
	}
}

static void BindSink(ExecutionRegionHashJoinInfo &sink, vector<SljitCastErrorIdentity> &errors) {
	BindHashJoin(sink.contract, sink.keys, errors);
}

static void BindSink(ExecutionRegionOrderSinkInfo &sink, vector<SljitCastErrorIdentity> &errors) {
	for (auto &key : sink.contract.order_keys) {
		key.reason.clear();
		BindFragment(key.expression, errors);
	}
}

static void BindSink(ExecutionRegionSinkInfo &sink, vector<SljitCastErrorIdentity> &errors) {
	sink.fields.clear();
	sink.reason.clear();
	sink.ir.clear();
	switch (sink.Kind()) {
	case ExecutionRegionSinkKind::RESULT_COLLECTOR_SINK:
	case ExecutionRegionSinkKind::MATERIALIZATION:
		return;
	default:
		throw InternalException("SLJIT instance contains an unsupported sink contract");
	}
}

static void BindExpression(SljitNativeRegionExpressionPlan &expression, vector<SljitCastErrorIdentity> &errors) {
	BindSljitCastErrorSlots(expression.cast_chain, errors);
	if (expression.expression_tree) {
		BindTree(*expression.expression_tree, errors);
	}
	expression.ir.clear();
}

static void BindExpressions(vector<SljitNativeRegionExpressionPlan> &expressions,
                            vector<SljitCastErrorIdentity> &errors) {
	for (auto &expression : expressions) {
		BindExpression(expression, errors);
	}
}

void BindSljitRegionInstanceSlots(SljitNativeRegionPlan &region) {
	auto &errors = region.cast_errors;
	for (auto &filter : region.scan_filters) {
		BindExpression(filter.filter, errors);
	}
	for (auto &op : region.ops) {
		switch (op.Kind()) {
		case SljitNativeRegionOpKind::FILTER:
			BindExpression(op.Filter(), errors);
			break;
		case SljitNativeRegionOpKind::PROJECTION:
			BindExpressions(op.Projections(), errors);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
			for (auto &key : op.HashJoinProbe().keys) {
				BindSljitCastErrorSlots(key.source_cast, errors);
			}
			BindExpression(op.HashJoinProbe().residual_filter, errors);
			BindHashJoinInfo(op.HashJoinProbe().operator_info, errors);
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			BindSink(op.HashJoinBuild().sink_info, errors);
			break;
		case SljitNativeRegionOpKind::APPEND_SINK:
			BindSink(op.AppendSink().sink_info, errors);
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			BindSink(op.OrderSink().sink_info, errors);
			break;
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			BindSink(op.AggregateUpdate().sink_info, errors);
			BindExpressions(op.AggregateUpdate().payloads, errors);
			BindExpressions(op.AggregateUpdate().group_expressions, errors);
			break;
		}
	}
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_graph.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/execution_region_graph.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

#include <cmath>

namespace duckdb {

static bool ExecutionRegionCompiledStageIsSourceSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::SOURCE;
}

static bool ExecutionRegionCompiledStageIsOperatorSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
	       stage.stage == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
	       stage.stage == ExecutionRegionStageKind::OPERATOR_BOUNDARY;
}

static bool ExecutionRegionCompiledStageIsSinkSlot(const ExecutionCompiledStageContract &stage) {
	return stage.stage == ExecutionRegionStageKind::HASH_JOIN_BUILD ||
	       stage.stage == ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD ||
	       stage.stage == ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE ||
	       stage.stage == ExecutionRegionStageKind::APPEND_SINK || stage.stage == ExecutionRegionStageKind::SORT_SINK ||
	       stage.stage == ExecutionRegionStageKind::DELIM_JOIN_SINK ||
	       stage.stage == ExecutionRegionStageKind::SINK_BOUNDARY;
}

static bool ExecutionRegionKeepCompiledStageForSlot(const ExecutionCompiledStageContract &stage,
                                                    ExecutionRegionOperatorSlot slot) {
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		return ExecutionRegionCompiledStageIsSourceSlot(stage);
	case ExecutionRegionOperatorSlot::OPERATOR:
		return ExecutionRegionCompiledStageIsOperatorSlot(stage);
	case ExecutionRegionOperatorSlot::SINK:
		return ExecutionRegionCompiledStageIsSinkSlot(stage);
	default:
		return false;
	}
}

static string DescribeExecutionRegionCompiledContractSlice(const ExecutionCompiledOperatorContract &contract,
                                                           const string &slot) {
	string result = "compiled_contract<slot=" + slot;
	result += ",stages=" + std::to_string(contract.stages.size());
	result += ",source=" + string(contract.HasSource() ? "true" : "false");
	result += ",operator=" + string(contract.HasOperator() ? "true" : "false");
	result += ",sink=" + string(contract.HasSink() ? "true" : "false");
	result += ",state_scan=" + string(contract.HasStateScan() ? "true" : "false");
	result += ",zero_or_many_output=" + string(contract.HasZeroOrManyOutput() ? "true" : "false");
	result += ">";
	return result;
}

static string ExecutionRegionOperatorSlotName(ExecutionRegionOperatorSlot slot) {
	switch (slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		return "source";
	case ExecutionRegionOperatorSlot::OPERATOR:
		return "operator";
	case ExecutionRegionOperatorSlot::SINK:
		return "sink";
	default:
		return "none";
	}
}

static string ExecutionRegionCompiledStageIR(const ExecutionContract &contract,
                                             const ExecutionCompiledStageContract &stage) {
	if (ExecutionRegionCompiledStageIsSourceSlot(stage)) {
		return contract.source.reason;
	}
	if (ExecutionRegionCompiledStageIsOperatorSlot(stage)) {
		return contract.operator_info.reason;
	}
	if (ExecutionRegionCompiledStageIsSinkSlot(stage)) {
		return contract.sink.reason;
	}
	return string();
}

static ExecutionCompiledOperatorContract SliceExecutionRegionCompiledContract(const ExecutionContract &contract,
                                                                              ExecutionRegionOperatorSlot slot,
                                                                              bool render_diagnostics) {
	ExecutionCompiledOperatorContract result;
	for (auto &stage : contract.compiled_contract.stages) {
		if (!ExecutionRegionKeepCompiledStageForSlot(stage, slot)) {
			continue;
		}
		ExecutionCompiledStageContract sliced_stage;
		sliced_stage.stage = stage.stage;
		sliced_stage.operation = stage.operation;
		sliced_stage.execution = stage.execution;
		sliced_stage.drain = stage.drain;
		sliced_stage.executable_work = stage.executable_work;
		sliced_stage.required_capability = stage.required_capability;
		sliced_stage.blocker = stage.blocker;
		if (render_diagnostics) {
			sliced_stage.ir = ExecutionRegionCompiledStageIR(contract, stage);
		}
		result.stages.push_back(std::move(sliced_stage));
	}
	if (render_diagnostics) {
		result.ir = DescribeExecutionRegionCompiledContractSlice(result, ExecutionRegionOperatorSlotName(slot));
	}
	return result;
}

static void SetExecutionRegionOperatorExpressions(ExecutionRegionOperatorEntry &entry,
                                                  const ExecutionTransformContract &transform) {
	entry.filter_expression = transform.filter_expression;
	entry.projection_expressions = transform.projection_expressions;
}

static void ApplyExecutionRegionExactSourceCardinality(ExecutionRegionOperatorEntry &entry,
                                                       const ExecutionContract &descriptor) {
	if (entry.slot != ExecutionRegionOperatorSlot::SOURCE || !descriptor.source.estimated_source_cardinality_exact) {
		return;
	}
	entry.estimated_cardinality = descriptor.source.estimated_source_cardinality;
}

static bool ExecutionRegionAggregateInputsHaveGeneratedExpression(const vector<ExecutionRegionAggregateInput> &inputs) {
	for (auto &input : inputs) {
		if (input.has_filter || !input.child_expressions.empty()) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionGroupsHaveGeneratedExpression(const vector<ExecutionRegionGroupInput> &groups) {
	for (auto &group : groups) {
		if (group.expression_ready) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionOrderKeysHaveGeneratedExpression(const vector<ExecutionRegionOrderKeyInput> &order_keys) {
	for (auto &order_key : order_keys) {
		if (order_key.expression_ready) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionSourceFiltersHaveGeneratedExpression(const vector<ExecutionSourceFilterContract> &filters) {
	for (auto &filter : filters) {
		if (filter.expression) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionProjectionListHasGeneratedExpression(
    const vector<optional_ptr<const Expression>> &projection_expressions) {
	for (auto &expression : projection_expressions) {
		if (expression) {
			return true;
		}
	}
	return false;
}

static bool ExecutionRegionGraphEntryHasGeneratedExpression(const ExecutionRegionOperatorEntry &entry) {
	return entry.filter_expression ||
	       ExecutionRegionProjectionListHasGeneratedExpression(entry.projection_expressions) ||
	       ExecutionRegionSourceFiltersHaveGeneratedExpression(entry.source_payload.filters) ||
	       ExecutionRegionAggregateInputsHaveGeneratedExpression(entry.source_payload.aggregates) ||
	       ExecutionRegionGroupsHaveGeneratedExpression(entry.source_payload.groups) ||
	       ExecutionRegionOrderKeysHaveGeneratedExpression(entry.source_payload.order_contract.order_keys) ||
	       ExecutionRegionAggregateInputsHaveGeneratedExpression(entry.sink_payload.aggregates) ||
	       ExecutionRegionGroupsHaveGeneratedExpression(entry.sink_payload.groups) ||
	       ExecutionRegionOrderKeysHaveGeneratedExpression(entry.sink_payload.order_contract.order_keys);
}

static void SetExecutionRegionOperatorFacts(ExecutionRegionOperatorEntry &entry) {
	entry.has_generated_expression = ExecutionRegionGraphEntryHasGeneratedExpression(entry);
	entry.has_native_operator_work = entry.HasNativeOperator() || entry.HasNativeSink();
}

static unique_ptr<BaseStatistics> TryGetExecutionRegionScanColumnStatistics(const PhysicalTableScan &scan,
                                                                            ClientContext &context,
                                                                            const ColumnIndex &column_id) {
	if (!scan.bind_data || (!scan.function.statistics && !scan.function.statistics_extended)) {
		return nullptr;
	}
	if (column_id.IsRowIdColumn() || column_id.IsRowNumberColumn()) {
		return nullptr;
	}
	if (scan.function.statistics_extended) {
		TableFunctionGetStatisticsInput input(scan.bind_data.get(), column_id);
		return scan.function.statistics_extended(context, input);
	}
	if (!column_id.HasPrimaryIndex()) {
		return nullptr;
	}
	return scan.function.statistics(context, scan.bind_data.get(), column_id.GetPrimaryIndex());
}

static idx_t BuildExecutionRegionDistinctReserveCount(BaseStatistics &stats, idx_t source_cardinality) {
	if (source_cardinality == 0) {
		return 0;
	}
	const auto distinct_count = MinValue(stats.GetDistinctCount(), source_cardinality);
	if (distinct_count == 0 || distinct_count >= source_cardinality) {
		return 0;
	}
	return distinct_count;
}

static idx_t BuildExecutionRegionDistinctCount(BaseStatistics &stats, idx_t source_cardinality) {
	if (source_cardinality == 0) {
		return 0;
	}
	return MinValue(stats.GetDistinctCount(), source_cardinality);
}

static optional_ptr<const Expression> TryUnwrapExecutionRegionOptionalFilterExpression(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return optional_ptr<const Expression>(expr);
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	if (function.Function().GetName() == OptionalFilterScalarFun::NAME && function.BindInfo()) {
		auto &data = function.BindInfo()->Cast<OptionalFilterFunctionData>();
		return data.child_filter_expr ? optional_ptr<const Expression>(*data.child_filter_expr) : nullptr;
	}
	if (function.Function().GetName() == SelectivityOptionalFilterScalarFun::NAME && function.BindInfo()) {
		auto &data = function.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
		return data.child_filter_expr ? optional_ptr<const Expression>(*data.child_filter_expr) : nullptr;
	}
	return optional_ptr<const Expression>(expr);
}

static bool TryGetExecutionRegionSignedNumericFilterRange(const ExpressionFilter &filter, const LogicalType &type,
                                                          SignedNumericRangeFilterData &range) {
	auto current = optional_ptr<const Expression>(*filter.expr);
	while (current) {
		auto unwrapped = TryUnwrapExecutionRegionOptionalFilterExpression(*current);
		if (!unwrapped || unwrapped.get() == current.get()) {
			return TryGetSignedNumericRange(*current, type, range);
		}
		current = unwrapped;
	}
	return false;
}

static idx_t EstimateExecutionRegionEqualityFilterRows(idx_t source_cardinality, idx_t distinct_count) {
	if (source_cardinality == 0 || distinct_count == 0) {
		return 0;
	}
	return MaxValue<idx_t>((source_cardinality + distinct_count - 1) / distinct_count, 1);
}

static bool TryGetExecutionRegionRuntimeMembershipFilterUniqueCount(const Expression &expr, idx_t &unique_count) {
	auto current = optional_ptr<const Expression>(expr);
	while (current) {
		auto unwrapped = TryUnwrapExecutionRegionOptionalFilterExpression(*current);
		if (!unwrapped) {
			return false;
		}
		if (unwrapped.get() == current.get()) {
			break;
		}
		current = unwrapped;
	}
	if (!current) {
		return false;
	}
	if (current->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function = current->Cast<BoundFunctionExpression>();
		if (function.Function().GetName() == PerfectHashJoinScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<PerfectHashJoinFunctionData>();
			if (!data.executor) {
				return false;
			}
			ExecutionPerfectHashJoinTableLayout layout;
			if (!data.executor->GetExecutionPerfectHashJoinTableLayout(layout) || !layout.ready) {
				return false;
			}
			unique_count = layout.build_unique_count;
			return true;
		}
		if (function.Function().GetName() == PrefixRangeScalarFun::NAME && function.BindInfo()) {
			auto &data = function.BindInfo()->Cast<PrefixRangeFunctionData>();
			if (!data.filter || !data.filter->IsInitialized()) {
				return false;
			}
			unique_count = data.filter->DistinctCountUpperBound();
			return unique_count > 0;
		}
	}

	bool found = false;
	idx_t narrowest_count = 0;
	ExpressionIterator::EnumerateChildren(*current, [&](const Expression &child) {
		idx_t child_count;
		if (!TryGetExecutionRegionRuntimeMembershipFilterUniqueCount(child, child_count)) {
			return;
		}
		if (!found || child_count < narrowest_count) {
			narrowest_count = child_count;
		}
		found = true;
	});
	if (found) {
		unique_count = narrowest_count;
	}
	return found;
}

static idx_t EstimateExecutionRegionDistinctSubsetRows(idx_t input_cardinality, idx_t allowed_distinct_count,
                                                       idx_t source_distinct_count) {
	if (input_cardinality == 0 || source_distinct_count == 0) {
		return 0;
	}
	if (allowed_distinct_count == 0) {
		return 0;
	}
	allowed_distinct_count = MinValue(allowed_distinct_count, source_distinct_count);
	const auto scaled = std::ceil(static_cast<double>(input_cardinality) * static_cast<double>(allowed_distinct_count) /
	                              static_cast<double>(source_distinct_count));
	return MaxValue<idx_t>(static_cast<idx_t>(scaled), 1);
}

static void AddExecutionRegionExactMembershipFilterProofs(const Expression &expr, const LogicalType &source_type,
                                                          idx_t source_input_index,
                                                          vector<ExecutionRegionExactFilterProof> &proofs) {
	auto unwrapped = TryUnwrapExecutionRegionOptionalFilterExpression(expr);
	if (!unwrapped) {
		return;
	}
	if (unwrapped.get() != &expr) {
		AddExecutionRegionExactMembershipFilterProofs(*unwrapped, source_type, source_input_index, proofs);
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			AddExecutionRegionExactMembershipFilterProofs(*child, source_type, source_input_index, proofs);
		}
		return;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return;
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	if (!function.BindInfo() || function.GetChildren().size() != 1) {
		return;
	}
	shared_ptr<ExecutionRuntimeFilterIdentity> identity;
	LogicalType filter_key_type;
	if (function.Function().GetName() == PerfectHashJoinScalarFun::NAME) {
		auto &data = function.BindInfo()->Cast<PerfectHashJoinFunctionData>();
		if (!data.executor) {
			return;
		}
		identity = data.executor->GetRuntimeFilterIdentity();
		filter_key_type = data.executor->GetKeyType();
	} else if (function.Function().GetName() == PrefixRangeScalarFun::NAME) {
		auto &data = function.BindInfo()->Cast<PrefixRangeFunctionData>();
		PrefixRangeLookupData lookup;
		if (!data.filter || !data.runtime_filter_identity || !TypeIsInteger(data.key_type.InternalType()) ||
		    !data.filter->GetSignedLookupData(lookup) || lookup.shift != 0) {
			return;
		}
		identity = data.runtime_filter_identity;
		filter_key_type = data.key_type;
	} else {
		return;
	}
	auto &input = *function.GetChildren()[0];
	const bool direct_reference = input.GetExpressionClass() == ExpressionClass::BOUND_REF &&
	                              input.GetReturnType() == source_type && filter_key_type == source_type;
	bool checked_integral_cast = false;
	if (input.GetExpressionClass() == ExpressionClass::BOUND_CAST && input.GetReturnType() == filter_key_type &&
	    TypeIsInteger(source_type.InternalType()) && TypeIsInteger(input.GetReturnType().InternalType())) {
		auto &cast = input.Cast<BoundCastExpression>();
		checked_integral_cast = cast.Child().GetExpressionClass() == ExpressionClass::BOUND_REF &&
		                        cast.Child().GetReturnType() == source_type;
	}
	if (!direct_reference && !checked_integral_cast) {
		return;
	}
	for (auto &proof : proofs) {
		if (proof.source_input_index == source_input_index && proof.identity == identity) {
			return;
		}
	}
	proofs.push_back({source_input_index, identity});
}

static void ApplyExecutionRegionFinalizedDynamicFilterEstimate(const PhysicalTableScan &scan,
                                                               ExecutionContract &descriptor, ClientContext &context) {
	auto &contract = descriptor.source.table_scan_contract;
	if (!contract.dynamic_filters || !scan.dynamic_filters || !scan.dynamic_filters->HasFilters()) {
		return;
	}
	auto filters = scan.dynamic_filters->GetFinalTableFilters(scan, scan.table_filters.get());
	if (!filters || !filters->HasFilters()) {
		return;
	}

	auto source_cardinality = contract.estimated_source_cardinality;
	auto estimated_cardinality = source_cardinality;
	bool used_finalized_filter_estimate = false;
	for (auto &filter_entry : *filters) {
		auto filter_idx = filter_entry.GetIndex().GetIndex();
		if (filter_idx >= scan.column_ids.size() || filter_idx >= contract.source_contract_input_types.size()) {
			continue;
		}
		auto &expr_filter =
		    ExpressionFilter::GetExpressionFilter(filter_entry.Filter(), "execution-region dynamic filter estimate");
		AddExecutionRegionExactMembershipFilterProofs(*expr_filter.expr,
		                                              contract.source_contract_input_types[filter_idx], filter_idx,
		                                              descriptor.source.exact_filter_proofs);
		auto stats = TryGetExecutionRegionScanColumnStatistics(scan, context, scan.column_ids[filter_idx]);
		if (!stats) {
			continue;
		}
		idx_t allowed_distinct_count;
		if (TryGetExecutionRegionRuntimeMembershipFilterUniqueCount(*expr_filter.expr, allowed_distinct_count)) {
			auto filter_estimate = EstimateExecutionRegionDistinctSubsetRows(
			    estimated_cardinality, allowed_distinct_count, stats->GetDistinctCount());
			used_finalized_filter_estimate = used_finalized_filter_estimate || filter_estimate < estimated_cardinality;
			estimated_cardinality = filter_estimate;
			if (estimated_cardinality == 0) {
				break;
			}
			continue;
		}
		SignedNumericRangeFilterData range;
		if (!TryGetExecutionRegionSignedNumericFilterRange(expr_filter,
		                                                   contract.source_contract_input_types[filter_idx], range)) {
			continue;
		}
		if (range.empty) {
			estimated_cardinality = 0;
			used_finalized_filter_estimate = true;
			break;
		}
		if (!range.has_lower || !range.has_upper || range.lower != range.upper) {
			continue;
		}
		auto equality_estimate =
		    EstimateExecutionRegionEqualityFilterRows(source_cardinality, stats->GetDistinctCount());
		if (equality_estimate == 0) {
			continue;
		}
		if (equality_estimate < estimated_cardinality) {
			estimated_cardinality = equality_estimate;
			used_finalized_filter_estimate = true;
		}
	}
	if (used_finalized_filter_estimate && estimated_cardinality < source_cardinality) {
		// Runtime membership filters are much stronger evidence than optimizer-time estimates, but correlated
		// filter columns can make independence estimates too selective. Keep one bit of uncertainty so profitable
		// multi-stage regions are not rejected merely because two finalized filters overlap.
		estimated_cardinality =
		    estimated_cardinality <= source_cardinality / 2 ? estimated_cardinality * 2 : source_cardinality;
		descriptor.source.estimated_source_cardinality = estimated_cardinality;
		contract.finalized_dynamic_filter_cardinality_estimate = true;
	}
}

static void ApplyExecutionRegionSourceCardinalityEstimate(ExecutionRegionOperatorEntry &entry,
                                                          const ExecutionContract &descriptor) {
	if (entry.slot != ExecutionRegionOperatorSlot::SOURCE) {
		return;
	}
	const bool finalized_dynamic_filter_estimate =
	    descriptor.source.table_scan_contract.finalized_dynamic_filter_cardinality_estimate;
	if (descriptor.source.estimated_source_cardinality == 0 && !finalized_dynamic_filter_estimate) {
		return;
	}
	entry.estimated_cardinality = MinValue(entry.estimated_cardinality, descriptor.source.estimated_source_cardinality);
}

static void AddExecutionRegionTableScanColumnStats(const PhysicalOperator &op, ExecutionContract &descriptor,
                                                   ClientContext &context) {
	if (op.type != PhysicalOperatorType::TABLE_SCAN || !descriptor.source.table_scan_contract.present) {
		return;
	}
	auto &scan = op.Cast<PhysicalTableScan>();
	auto &contract = descriptor.source.table_scan_contract;
	if (contract.function_name != "seq_scan") {
		return;
	}
	contract.source_contract_input_distinct_reserve_counts.assign(scan.column_ids.size(), 0);
	contract.source_contract_input_distinct_counts.assign(scan.column_ids.size(), 0);
	contract.source_contract_input_min_values.assign(scan.column_ids.size(), Value());
	contract.source_contract_input_max_values.assign(scan.column_ids.size(), Value());
	for (idx_t column_idx = 0; column_idx < scan.column_ids.size(); column_idx++) {
		auto stats = TryGetExecutionRegionScanColumnStatistics(scan, context, scan.column_ids[column_idx]);
		if (!stats) {
			continue;
		}
		contract.source_contract_input_distinct_counts[column_idx] =
		    BuildExecutionRegionDistinctCount(*stats, contract.estimated_source_cardinality);
		contract.source_contract_input_distinct_reserve_counts[column_idx] =
		    BuildExecutionRegionDistinctReserveCount(*stats, contract.estimated_source_cardinality);
		if (stats->GetStatsType() == StatisticsType::NUMERIC_STATS && NumericStats::HasMinMax(*stats)) {
			contract.source_contract_input_min_values[column_idx] = NumericStats::Min(*stats);
			contract.source_contract_input_max_values[column_idx] = NumericStats::Max(*stats);
		}
	}
	ApplyExecutionRegionFinalizedDynamicFilterEstimate(scan, descriptor, context);
}

static ExecutionRegionOperatorEntry BuildExecutionRegionOperatorEntry(const PhysicalOperator &op,
                                                                      ExecutionRegionOperatorSlot slot,
                                                                      bool render_diagnostics,
                                                                      idx_t operator_index = DConstants::INVALID_INDEX,
                                                                      optional_ptr<ClientContext> context = nullptr) {
	ExecutionRegionOperatorEntry entry;
	entry.present = true;
	entry.slot = slot;
	entry.operator_index = operator_index;
	entry.output_types = op.GetTypes();
	entry.estimated_cardinality = op.estimated_cardinality;
	entry.operator_kind = op.GetExecutionRegionOperatorKind();
	entry.operator_name = entry.operator_kind == ExecutionRegionOperatorKind::GENERIC ||
	                              entry.operator_kind == ExecutionRegionOperatorKind::SCAN_SOURCE
	                          ? op.GetName()
	                          : ExecutionRegionOperatorKindToTraceLabel(entry.operator_kind);
	auto descriptor = op.GetExecutionContract();
	if (context) {
		AddExecutionRegionTableScanColumnStats(op, descriptor, *context);
	}
	ApplyExecutionRegionSourceCardinalityEstimate(entry, descriptor);
	ApplyExecutionRegionExactSourceCardinality(entry, descriptor);
	SetExecutionRegionOperatorExpressions(entry, descriptor.transform);
	entry.source_contract =
	    SliceExecutionRegionCompiledContract(descriptor, ExecutionRegionOperatorSlot::SOURCE, render_diagnostics);
	entry.operator_contract =
	    SliceExecutionRegionCompiledContract(descriptor, ExecutionRegionOperatorSlot::OPERATOR, render_diagnostics);
	entry.sink_contract =
	    SliceExecutionRegionCompiledContract(descriptor, ExecutionRegionOperatorSlot::SINK, render_diagnostics);
	entry.source_boundary_reason = std::move(descriptor.source_boundary_reason);
	entry.source_payload = std::move(descriptor.source);
	entry.operator_payload = std::move(descriptor.operator_info);
	entry.sink_payload = std::move(descriptor.sink);
	SetExecutionRegionOperatorFacts(entry);
	return entry;
}

static void AccumulateExecutionRegionGraphFacts(ExecutionRegionGraph &graph,
                                                const ExecutionRegionOperatorEntry &entry) {
	graph.has_generated_expression = graph.has_generated_expression || entry.has_generated_expression;
	graph.has_native_operator_work = graph.has_native_operator_work || entry.has_native_operator_work;
}

unique_ptr<ExecutionRegionGraph> BuildExecutionRegionGraph(Pipeline &pipeline, bool render_diagnostics) {
	auto result = make_uniq<ExecutionRegionGraph>();
	auto &context = pipeline.GetClientContext();
	if (pipeline.GetSource()) {
		result->source = BuildExecutionRegionOperatorEntry(*pipeline.GetSource(), ExecutionRegionOperatorSlot::SOURCE,
		                                                   render_diagnostics, DConstants::INVALID_INDEX, context);
		AccumulateExecutionRegionGraphFacts(*result, result->source);
	}
	auto &operators = pipeline.GetIntermediateOperators();
	for (idx_t op_idx = 0; op_idx < operators.size(); op_idx++) {
		result->operators.push_back(BuildExecutionRegionOperatorEntry(
		    operators[op_idx].get(), ExecutionRegionOperatorSlot::OPERATOR, render_diagnostics, op_idx, context));
		AccumulateExecutionRegionGraphFacts(*result, result->operators.back());
	}
	if (pipeline.GetSink()) {
		result->sink = BuildExecutionRegionOperatorEntry(*pipeline.GetSink(), ExecutionRegionOperatorSlot::SINK,
		                                                 render_diagnostics, DConstants::INVALID_INDEX, context);
		AccumulateExecutionRegionGraphFacts(*result, result->sink);
	}
	if (result->Empty()) {
		return nullptr;
	}
	return result;
}

static string DescribeExecutionRegionGraphEntry(const char *label, const ExecutionRegionOperatorEntry &entry) {
	string result = string(label) + ":";
	result += ExecutionRegionOperatorKindToTraceLabel(entry.operator_kind);
	result += ":name=" + entry.operator_name;
	result += ":estimated_cardinality=" + std::to_string(entry.estimated_cardinality);
	switch (entry.slot) {
	case ExecutionRegionOperatorSlot::SOURCE:
		result += ":source_contract=" + string(entry.HasSourceContract() ? "present" : "missing");
		result += ":source_native=" + string(entry.UsesSourceContract() ? "true" : "false");
		break;
	case ExecutionRegionOperatorSlot::OPERATOR:
		result += ":operator_contract=" + string(entry.HasOperatorContract() ? "present" : "missing");
		result += ":operator_native=" + string(entry.HasNativeOperator() ? "true" : "false");
		break;
	case ExecutionRegionOperatorSlot::SINK:
		result += ":sink_contract=" + string(entry.HasSinkContract() ? "present" : "missing");
		result += ":sink_native=" + string(entry.HasNativeSink() ? "true" : "false");
		break;
	default:
		break;
	}
	return result;
}

string DescribeExecutionRegionGraphShape(const ExecutionRegionGraph &graph) {
	string result = "graph";
	if (graph.HasSource()) {
		result += ";";
		result += DescribeExecutionRegionGraphEntry("source", graph.source);
	}
	for (idx_t op_idx = 0; op_idx < graph.operators.size(); op_idx++) {
		result += ";op" + std::to_string(op_idx) + ":";
		result += DescribeExecutionRegionGraphEntry("operator", graph.operators[op_idx]);
	}
	if (graph.HasSink()) {
		result += ";";
		result += DescribeExecutionRegionGraphEntry("sink", graph.sink);
	}
	return result;
}

} // namespace duckdb

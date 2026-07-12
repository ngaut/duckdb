//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_cost_input.cpp
//
//===----------------------------------------------------------------------===//

#include "execution_region_cost_input.hpp"

#include "duckdb/execution/execution_region_ir.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/vector_size.hpp"

#include "execution_region_duckdb_type_adapter.hpp"

namespace duckdb {

static void AddExecutionRegionGeneratedExpressionWork(PhysicalRunnerCostInput &input, idx_t expression_cost) {
	if (expression_cost == 0) {
		return;
	}
	input.expression_cost += expression_cost;
	input.generated_stage_count++;
}

static void AddExecutionRegionGeneratedBodyStage(PhysicalRunnerCostInput &input) {
	input.generated_stage_count++;
	input.generated_backend_stage_count++;
}

static void AddExecutionRegionNativeAggregateStage(PhysicalRunnerCostInput &input, bool grouped) {
	input.native_aggregate_stage_count++;
	if (grouped) {
		input.native_grouped_aggregate_stage_count++;
	}
}

static bool ExecutionRegionStageIsExecutable(const ExecutionRegionStage &stage) {
	return stage.executable_work && stage.execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
	       stage.execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
}

enum class ExecutionRegionStageCostWorkKind : uint8_t {
	NONE,
	GENERATED,
	NATIVE_JOIN,
	NATIVE_GROUPED_AGGREGATE,
	NATIVE_UNGROUPED_AGGREGATE
};

struct ExecutionRegionStageCostFact {
	ExecutionRegionStageCostWorkKind work_kind = ExecutionRegionStageCostWorkKind::NONE;
	bool is_filter = false;
	bool is_projection = false;
	bool may_anchor_compiled_body = false;
};

struct ExecutionRegionCostFacts {
	idx_t generated_stage_count = 0;
	idx_t generated_backend_stage_count = 0;
	idx_t generated_grouped_aggregate_stage_count = 0;
	idx_t native_grouped_state_address_lookup_count = 0;
	idx_t materialization_elision_count = 0;
	idx_t native_join_stage_count = 0;
	idx_t native_hash_join_build_sink_count = 0;
	bool native_delim_join_sink = false;
	idx_t native_aggregate_stage_count = 0;
	idx_t native_grouped_aggregate_stage_count = 0;
	bool may_anchor_compiled_body = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
};

static bool ExecutionRegionSinkKindIsGroupedAggregateUpdate(ExecutionRegionSinkKind kind) {
	return kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	       kind == ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
}

static bool ExecutionRegionSinkKindIsAggregateUpdate(ExecutionRegionSinkKind kind) {
	return ExecutionRegionSinkKindIsGroupedAggregateUpdate(kind) ||
	       kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
}

static void RemoveExecutionRegionGeneratedAggregateUpdateNativeCost(ExecutionRegionCostFacts &facts,
                                                                    const ExecutionRegionCandidateTraits &traits) {
	if (traits.generated_aggregate_update_count == 0 || !ExecutionRegionSinkKindIsAggregateUpdate(traits.sink_kind)) {
		return;
	}
	const auto generated_update_count = traits.generated_aggregate_update_count;
	const auto aggregate_decrement = MinValue(facts.native_aggregate_stage_count, generated_update_count);
	facts.native_aggregate_stage_count -= aggregate_decrement;
	if (!ExecutionRegionSinkKindIsGroupedAggregateUpdate(traits.sink_kind)) {
		return;
	}
	facts.generated_grouped_aggregate_stage_count += generated_update_count;
	const auto grouped_decrement = MinValue(facts.native_grouped_aggregate_stage_count, generated_update_count);
	facts.native_grouped_aggregate_stage_count -= grouped_decrement;
}

static ExecutionRegionStageCostFact GetExecutionRegionStageCostFact(const ExecutionRegionStage &stage) {
	ExecutionRegionStageCostFact result;
	if (!ExecutionRegionStageIsExecutable(stage)) {
		return result;
	}
	if (stage.kind == ExecutionRegionStageKind::FILTER || stage.kind == ExecutionRegionStageKind::SOURCE_FILTER) {
		result.is_filter = true;
	}
	if (stage.kind == ExecutionRegionStageKind::PROJECTION) {
		result.is_projection = true;
	}
	if (stage.execution == ExecutionRegionStageExecutionKind::GENERATED_IR) {
		result.work_kind = ExecutionRegionStageCostWorkKind::GENERATED;
		result.may_anchor_compiled_body = true;
		return result;
	}
	if (stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
		return result;
	}
	switch (stage.kind) {
	case ExecutionRegionStageKind::HASH_JOIN_BUILD:
		break;
	case ExecutionRegionStageKind::HASH_JOIN_PROBE:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_JOIN;
		result.may_anchor_compiled_body = true;
		break;
	case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_GROUPED_AGGREGATE;
		result.may_anchor_compiled_body = true;
		break;
	case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_UNGROUPED_AGGREGATE;
		result.may_anchor_compiled_body = true;
		break;
	default:
		break;
	}
	return result;
}

static bool ExecutionRegionCandidateHasGeneratedFilterOrOperatorWork(const ExecutionRegionCandidateTraits &traits) {
	return traits.source_filter_expression_count > 0 || traits.filter_count > 0 ||
	       traits.hash_join_operator_count > 0 || traits.aggregate_count > 0;
}

static bool ExecutionRegionCandidateHasGeneratedProjectionWork(const ExecutionRegionCandidateTraits &traits) {
	return traits.arithmetic_projection_count > 0 || traits.high_cost_projection_count > 0 ||
	       traits.projection_count > traits.reference_projection_count ||
	       (traits.projection_count > 0 &&
	        (traits.predicate_expression_count > 0 || traits.control_expression_count > 0));
}

static PhysicalRunnerGeneratedWorkClass
ClassifyExecutionRegionGeneratedWork(const ExecutionRegionCandidateTraits &traits) {
	if (ExecutionRegionCandidateHasGeneratedFilterOrOperatorWork(traits)) {
		return PhysicalRunnerGeneratedWorkClass::COMPUTE;
	}
	if (traits.projection_count == 0) {
		return PhysicalRunnerGeneratedWorkClass::NONE;
	}
	const bool projection_only = traits.filter_count == 0 && traits.operator_count == 0;
	if (projection_only && traits.high_cost_projection_count > 0) {
		return PhysicalRunnerGeneratedWorkClass::HIGH_COST_PROJECTION;
	}
	if (ExecutionRegionCandidateHasGeneratedProjectionWork(traits)) {
		return PhysicalRunnerGeneratedWorkClass::COMPUTE;
	}
	if (projection_only) {
		return PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE;
	}
	return PhysicalRunnerGeneratedWorkClass::NONE;
}

static PhysicalRunnerNativeProtocolClass
ClassifyExecutionRegionNativeProtocol(const ExecutionRegionCandidateTraits &traits) {
	if (traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
	    traits.source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT && traits.HasSink()) {
		return PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL;
	}
	return PhysicalRunnerNativeProtocolClass::NONE;
}

static void AccumulateExecutionRegionCostFact(const ExecutionRegionStageCostFact &fact,
                                              ExecutionRegionCostFacts &result, bool &has_filter,
                                              bool &has_projection) {
	result.may_anchor_compiled_body = result.may_anchor_compiled_body || fact.may_anchor_compiled_body;
	has_filter = has_filter || fact.is_filter;
	has_projection = has_projection || fact.is_projection;
	switch (fact.work_kind) {
	case ExecutionRegionStageCostWorkKind::GENERATED:
		result.generated_stage_count++;
		return;
	case ExecutionRegionStageCostWorkKind::NATIVE_JOIN:
		result.native_join_stage_count++;
		return;
	case ExecutionRegionStageCostWorkKind::NATIVE_GROUPED_AGGREGATE:
		result.native_aggregate_stage_count++;
		result.native_grouped_aggregate_stage_count++;
		return;
	case ExecutionRegionStageCostWorkKind::NATIVE_UNGROUPED_AGGREGATE:
		result.native_aggregate_stage_count++;
		return;
	case ExecutionRegionStageCostWorkKind::NONE:
		return;
	}
}

static ExecutionRegionCostFacts BuildExecutionRegionCostFacts(const ExecutionRegionCandidate &candidate) {
	ExecutionRegionCostFacts result;
	bool has_filter = false;
	bool has_projection = false;
	for (auto &stage : candidate.stage_plan.stages) {
		AccumulateExecutionRegionCostFact(GetExecutionRegionStageCostFact(stage), result, has_filter, has_projection);
	}
	RemoveExecutionRegionGeneratedAggregateUpdateNativeCost(result, candidate.traits);
	const bool has_selected_view_materialization =
	    candidate.traits.selected_hash_join_filter_materialization_count > 0 ||
	    candidate.traits.selected_hash_join_view_materialization_count > 0;
	if ((has_filter || has_projection) && candidate.traits.generated_aggregate_update_count > 0 &&
	    !has_selected_view_materialization) {
		result.materialization_elision_count = 1;
	}
	result.generated_stage_count += result.native_join_stage_count;
	result.generated_backend_stage_count += result.native_join_stage_count;
	result.generated_backend_stage_count += candidate.traits.mark_probe_filter_count;
	result.generated_stage_count += candidate.traits.generated_aggregate_update_count;
	result.generated_stage_count += candidate.traits.generated_aggregate_lookup_count;
	result.generated_backend_stage_count += candidate.traits.generated_aggregate_update_count;
	result.generated_backend_stage_count += candidate.traits.generated_aggregate_lookup_count;
	if (ExecutionRegionSinkKindIsGroupedAggregateUpdate(candidate.traits.sink_kind)) {
		result.generated_grouped_aggregate_stage_count += candidate.traits.generated_aggregate_lookup_count;
	}
	if (candidate.traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    candidate.traits.generated_aggregate_update_count > candidate.traits.generated_aggregate_lookup_count) {
		result.native_grouped_state_address_lookup_count +=
		    candidate.traits.generated_aggregate_update_count - candidate.traits.generated_aggregate_lookup_count;
	}
	result.may_anchor_compiled_body = result.may_anchor_compiled_body ||
	                                  candidate.traits.generated_aggregate_update_count > 0 ||
	                                  candidate.traits.generated_aggregate_lookup_count > 0;
	result.generated_work_class = ClassifyExecutionRegionGeneratedWork(candidate.traits);
	result.native_protocol_class = ClassifyExecutionRegionNativeProtocol(candidate.traits);
	if (candidate.traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD) {
		result.native_hash_join_build_sink_count++;
	}
	result.native_delim_join_sink = candidate.traits.sink_kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK;
	return result;
}

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate) {
	auto cost_facts = BuildExecutionRegionCostFacts(candidate);
	PhysicalRunnerCostInput input;
	input.input_scope = PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE;
	input.estimated_cardinality = candidate.estimated_cardinality;
	input.expression_cost = candidate.traits.expression_cost;
	input.source_contract_input_cardinality = candidate.traits.source_contract_input_cardinality;
	input.source_contract_output_cardinality_unknown = candidate.traits.source_contract_output_cardinality_unknown;
	input.finalized_dynamic_filter_cardinality_estimate =
	    candidate.traits.finalized_dynamic_filter_cardinality_estimate;
	input.generated_stage_count = cost_facts.generated_stage_count;
	input.generated_backend_stage_count = cost_facts.generated_backend_stage_count;
	input.generated_grouped_aggregate_stage_count = cost_facts.generated_grouped_aggregate_stage_count;
	input.native_grouped_state_address_lookup_count = cost_facts.native_grouped_state_address_lookup_count;
	input.grouped_aggregate_estimated_cardinality = candidate.traits.grouped_aggregate_estimated_cardinality;
	input.materialization_elision_count = cost_facts.materialization_elision_count;
	input.full_pipeline =
	    ExecutionRegionABIIsFullPipeline(candidate.contract.abi) && cost_facts.may_anchor_compiled_body;
	input.selected_hash_join_filter_materialization_count =
	    candidate.traits.selected_hash_join_filter_materialization_count;
	input.native_join_stage_count = cost_facts.native_join_stage_count;
	input.native_hash_join_build_sink_count = cost_facts.native_hash_join_build_sink_count;
	input.native_delim_join_sink = cost_facts.native_delim_join_sink;
	input.native_aggregate_stage_count = cost_facts.native_aggregate_stage_count;
	input.native_grouped_aggregate_stage_count = cost_facts.native_grouped_aggregate_stage_count;
	input.source_filter_count = candidate.traits.source_filter_count;
	input.uses_scan_filters = candidate.uses_scan_filters;
	input.generated_work_class = cost_facts.generated_work_class;
	input.native_protocol_class = cost_facts.native_protocol_class;
	input.has_accelerated_work = cost_facts.may_anchor_compiled_body;
	return input;
}

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate,
                                                               const ExecutionRegionLoweringPlan &lowering_plan) {
	auto input = BuildExecutionRegionCandidateCostInput(candidate);
	input.uses_scan_filters = lowering_plan.UsesScanFilters();
	auto &backend_facts = lowering_plan.capability_facts;
	input.native_grouped_state_address_lookup_count = backend_facts.backend_native_state_address_lookup_count;
	input.vectorized_execution_preferred = backend_facts.backend_low_cardinality_string_predicate_count > 0;
	const auto direct_hash_join_build_count =
	    MinValue(input.native_hash_join_build_sink_count, backend_facts.backend_direct_hash_join_build_count);
	if (direct_hash_join_build_count > 0) {
		input.native_hash_join_build_sink_count -= direct_hash_join_build_count;
		input.generated_stage_count += direct_hash_join_build_count;
		input.generated_backend_stage_count += direct_hash_join_build_count;
		input.has_accelerated_work = true;
	}
	const auto distinct_key_fast_insert_count =
	    MinValue(input.native_aggregate_stage_count, backend_facts.backend_distinct_key_fast_insert_count);
	if (distinct_key_fast_insert_count > 0) {
		input.native_aggregate_stage_count -= distinct_key_fast_insert_count;
		const auto grouped_decrement =
		    MinValue(input.native_grouped_aggregate_stage_count, distinct_key_fast_insert_count);
		input.native_grouped_aggregate_stage_count -= grouped_decrement;
		input.generated_stage_count += distinct_key_fast_insert_count;
		input.generated_backend_stage_count += distinct_key_fast_insert_count;
		input.generated_grouped_aggregate_stage_count += distinct_key_fast_insert_count;
		input.has_accelerated_work = true;
	}
	const bool weak_stateful_grouped_update =
	    candidate.traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
	    candidate.traits.source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    candidate.traits.sink_kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
	    input.materialization_elision_count == 0 && backend_facts.backend_hash_aggregate_update_count > 0 &&
	    backend_facts.backend_native_state_address_lookup_count > 0;
	if (backend_facts.backend_weak_accelerated_work_count == 0) {
		return input;
	}
	if (weak_stateful_grouped_update) {
		return input;
	}
	input.generated_stage_count = 0;
	input.generated_backend_stage_count = 0;
	input.generated_grouped_aggregate_stage_count = 0;
	input.native_grouped_state_address_lookup_count = 0;
	input.materialization_elision_count = 0;
	input.selected_hash_join_filter_materialization_count = 0;
	input.generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	input.has_accelerated_work = false;
	return input;
}

PhysicalRunnerCostInput
BuildExecutionRegionPipelineCandidateUpperBoundCostInput(const PhysicalRunnerCostInput &pipeline_input) {
	auto input = pipeline_input;
	input.input_scope = PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE;
	const auto hash_join_build_backend_stage_count =
	    input.native_join_stage_count == 0 ? input.native_hash_join_build_sink_count : 0;
	const auto native_backend_stage_count =
	    input.native_join_stage_count + input.native_aggregate_stage_count + hash_join_build_backend_stage_count;
	input.generated_stage_count += native_backend_stage_count;
	input.generated_backend_stage_count += native_backend_stage_count;
	input.generated_grouped_aggregate_stage_count += input.native_grouped_aggregate_stage_count;
	input.native_grouped_state_address_lookup_count += input.native_grouped_aggregate_stage_count;
	if (hash_join_build_backend_stage_count > 0) {
		input.native_hash_join_build_sink_count = 0;
	}
	input.native_sort_stage_count = 0;
	input.has_accelerated_work = input.has_accelerated_work || input.generated_stage_count > 0 ||
	                             input.generated_backend_stage_count > 0 || input.materialization_elision_count > 0 ||
	                             input.full_pipeline;
	return input;
}

static idx_t ExecutionRegionPhysicalExpressionListCost(const vector<unique_ptr<Expression>> &expressions) {
	idx_t result = 0;
	for (auto &expression : expressions) {
		if (expression) {
			result += DuckDBCostModel::ExpressionCost(*expression);
		}
	}
	return result;
}

static idx_t ExecutionRegionPhysicalAggregateExpressionCost(const Expression &expression) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return DuckDBCostModel::ExpressionCost(expression);
	}
	auto &aggregate = expression.Cast<BoundAggregateExpression>();
	idx_t result = ExecutionRegionPhysicalExpressionListCost(aggregate.GetChildren());
	if (aggregate.GetFilter()) {
		result += DuckDBCostModel::ExpressionCost(*aggregate.GetFilter());
	}
	if (aggregate.GetOrderBys()) {
		for (auto &order : aggregate.GetOrderBys()->orders) {
			if (order.expression) {
				result += DuckDBCostModel::ExpressionCost(*order.expression);
			}
		}
	}
	return result;
}

static idx_t ExecutionRegionPhysicalAggregateListCost(const vector<unique_ptr<Expression>> &expressions) {
	idx_t result = 0;
	for (auto &expression : expressions) {
		if (expression) {
			result += ExecutionRegionPhysicalAggregateExpressionCost(*expression);
		}
	}
	return result;
}

enum class ExecutionRegionPhysicalPipelineSlot : uint8_t { SOURCE, OPERATOR, SINK };

struct ExecutionRegionPhysicalPipelineCostFacts {
	PhysicalRunnerCostInput cost_input;
	ExecutionRegionCandidateTraits traits;
	bool exact_source_cardinality_bounds_pipeline = true;
	bool native_sink_boundary = false;
	idx_t generated_aggregate_update_count = 0;
};

static bool ExecutionRegionPhysicalExpressionIsReference(const Expression &expression) {
	auto expression_class = expression.GetExpressionClass();
	return expression_class == ExpressionClass::BOUND_REF || expression_class == ExpressionClass::BOUND_COLUMN_REF;
}

static bool ExecutionRegionPhysicalExpressionTypeIsComparison(ExpressionType expression_type) {
	return expression_type >= ExpressionType::COMPARE_BOUNDARY_START &&
	       expression_type <= ExpressionType::COMPARE_BOUNDARY_END;
}

static bool ExecutionRegionPhysicalExpressionTypeIsConjunction(ExpressionType expression_type) {
	return expression_type == ExpressionType::CONJUNCTION_AND || expression_type == ExpressionType::CONJUNCTION_OR;
}

static bool ExecutionRegionPhysicalExpressionIsIntegralType(const LogicalType &type) {
	return type.IsIntegral();
}

static bool ExecutionRegionPhysicalExpressionIsFloatingType(const LogicalType &type) {
	return type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE;
}

static bool ExecutionRegionPhysicalExpressionIsArithmeticType(const LogicalType &type) {
	return ExecutionRegionPhysicalExpressionIsIntegralType(type) ||
	       ExecutionRegionPhysicalExpressionIsFloatingType(type) || type.id() == LogicalTypeId::DECIMAL;
}

static bool ExecutionRegionPhysicalFunctionIsArithmetic(const BoundFunctionExpression &expression) {
	if (expression.GetChildren().size() != 2) {
		return false;
	}
	auto name = expression.Function().GetName().GetIdentifierName();
	if (name == "+" || name == "-" || name == "*") {
		return ExecutionRegionPhysicalExpressionIsArithmeticType(expression.GetReturnType());
	}
	if (name == "/") {
		return ExecutionRegionPhysicalExpressionIsFloatingType(expression.GetReturnType());
	}
	if (name == "//" || name == "%") {
		return ExecutionRegionPhysicalExpressionIsIntegralType(expression.GetReturnType());
	}
	return false;
}

static void AccumulateExecutionRegionPhysicalExpressionShapeTraits(const Expression &expression,
                                                                   ExecutionRegionCandidateTraits &traits) {
	auto expression_type = expression.GetExpressionType();
	if (ExecutionRegionPhysicalExpressionTypeIsComparison(expression_type) ||
	    expression_type == ExpressionType::OPERATOR_IS_NULL ||
	    expression_type == ExpressionType::OPERATOR_IS_NOT_NULL || expression_type == ExpressionType::OPERATOR_NOT) {
		traits.predicate_expression_count++;
	}
	if (ExecutionRegionPhysicalExpressionTypeIsConjunction(expression_type)) {
		traits.predicate_expression_count++;
		traits.control_expression_count++;
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_CASE:
		traits.control_expression_count++;
		break;
	case ExpressionClass::BOUND_FUNCTION:
		if (ExecutionRegionPhysicalFunctionIsArithmetic(expression.Cast<BoundFunctionExpression>())) {
			traits.arithmetic_projection_count++;
		}
		break;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) {
		AccumulateExecutionRegionPhysicalExpressionShapeTraits(child, traits);
	});
}

static void AccumulateExecutionRegionPhysicalProjectionTraits(const Expression &expression,
                                                              ExecutionRegionCandidateTraits &traits) {
	auto expression_cost = DuckDBCostModel::ExpressionCost(expression);
	traits.expression_cost += expression_cost;
	if (ExecutionRegionPhysicalExpressionIsReference(expression)) {
		traits.reference_projection_count++;
		return;
	}
	if (expression_cost == 0) {
		return;
	}
	if (expression_cost >= HIGH_COST_GENERATED_PROJECTION_EXPRESSION_COST) {
		traits.high_cost_projection_count++;
		return;
	}
	AccumulateExecutionRegionPhysicalExpressionShapeTraits(expression, traits);
}

static void AccumulateExecutionRegionPhysicalProjectionListTraits(const vector<unique_ptr<Expression>> &expressions,
                                                                  ExecutionRegionCandidateTraits &traits) {
	for (auto &expression : expressions) {
		if (expression) {
			AccumulateExecutionRegionPhysicalProjectionTraits(*expression, traits);
		}
	}
}

static bool ExecutionRegionPhysicalTableFilterCanUseGeneratedSourceStage(const TableFilter &table_filter,
                                                                         const LogicalType &source_type,
                                                                         idx_t filter_index) {
	if (table_filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &expression_filter = table_filter.Cast<ExpressionFilter>();
	auto expression =
	    TryLowerExecutionExpression(*expression_filter.expr, filter_index, ExecutionExpressionIRMode::COMPACT);
	if (!expression) {
		return false;
	}
	return GetExecutionRegionGeneratedSourceFilterCapability(*expression, source_type).can_generate;
}

static bool TryAccumulateExecutionRegionPhysicalScanCost(const PhysicalOperator &op,
                                                         ExecutionRegionPhysicalPipelineCostFacts &facts) {
	if (op.type != PhysicalOperatorType::TABLE_SCAN) {
		return true;
	}
	auto &scan = op.Cast<PhysicalTableScan>();
	const bool has_pushed_dynamic_filters =
	    scan.function.filter_pushdown && scan.dynamic_filters && scan.dynamic_filters->HasFilters();
	if (has_pushed_dynamic_filters) {
		facts.cost_input.uses_scan_filters = true;
		facts.cost_input.source_contract_output_cardinality_unknown = true;
	}
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
	}
	auto contract = scan.GetExecutionContract();
	auto &source_input_types = contract.source.table_scan_contract.source_contract_input_types;
	idx_t filter_cost = 0;
	idx_t filter_count = 0;
	idx_t generated_filter_count = 0;
	for (auto &filter : *scan.table_filters) {
		auto filter_idx = filter.GetIndex().GetIndex();
		const bool can_generate = filter_idx < source_input_types.size() &&
		                          ExecutionRegionPhysicalTableFilterCanUseGeneratedSourceStage(
		                              filter.Filter(), source_input_types[filter_idx], filter_count);
		if (can_generate) {
			filter_cost += DuckDBCostModel::FilterCost(filter.Filter());
			generated_filter_count++;
		}
		filter_count++;
	}
	facts.traits.source_filter_count += filter_count;
	facts.traits.source_filter_expression_count += generated_filter_count;
	const bool generated_source_filters = generated_filter_count == filter_count;
	if (scan.function.filter_pushdown && !generated_source_filters) {
		facts.cost_input.uses_scan_filters = true;
	}
	if (generated_source_filters) {
		AddExecutionRegionGeneratedExpressionWork(facts.cost_input, filter_cost);
	}
	return true;
}

static ExecutionRegionSourceKind ExecutionRegionPhysicalSourceKind(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN;
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		return ExecutionRegionSourceKind::GENERIC_SCAN;
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::ASOF_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::POSITIONAL_JOIN:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TOP_N:
		return ExecutionRegionSourceKind::STATEFUL_OPERATOR;
	default:
		return ExecutionRegionSourceKind::NONE;
	}
}

static void AccumulateExecutionRegionPhysicalSourceTraits(const PhysicalOperator &source,
                                                          ExecutionRegionCandidateTraits &traits) {
	traits.source_kind = ExecutionRegionPhysicalSourceKind(source.type);
}

static bool ExecutionRegionPhysicalOperatorIsStatefulSource(const PhysicalOperator &op) {
	return ExecutionRegionPhysicalSourceKind(op.type) == ExecutionRegionSourceKind::STATEFUL_OPERATOR;
}

static bool ExecutionRegionPhysicalSourceUsesReadySourceContract(const ExecutionContract &contract) {
	return contract.source.execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	       contract.source.source_contract.status == ExecutionRegionSourceContractStatus::READY;
}

static idx_t ExecutionRegionPhysicalSourceContractInputCardinality(const PhysicalOperator &source,
                                                                   const ExecutionContract &contract) {
	if (contract.source.estimated_source_cardinality > 0) {
		return contract.source.estimated_source_cardinality;
	}
	return source.estimated_cardinality;
}

static void AccumulateExecutionRegionPhysicalSourceContractCost(const PhysicalOperator &source,
                                                                ExecutionRegionPhysicalPipelineCostFacts &facts) {
	auto contract = source.GetExecutionContract();
	if (!ExecutionRegionPhysicalSourceUsesReadySourceContract(contract)) {
		return;
	}
	auto &cost_input = facts.cost_input;
	auto source_cardinality = ExecutionRegionPhysicalSourceContractInputCardinality(source, contract);
	if (source_cardinality > 0) {
		cost_input.source_contract_input_cardinality = source_cardinality;
	}
	if (contract.source.dynamic_filters) {
		cost_input.source_contract_output_cardinality_unknown = true;
	}
	if (contract.source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR &&
	    !contract.source.estimated_source_cardinality_exact) {
		cost_input.source_contract_output_cardinality_unknown = true;
	}
}

static void AccumulateExecutionRegionPhysicalSinkTraits(const PhysicalOperator &sink,
                                                        ExecutionRegionCandidateTraits &traits) {
	traits.sink_present = true;
	switch (sink.type) {
	case PhysicalOperatorType::CREATE_TABLE_AS:
	case PhysicalOperatorType::BATCH_CREATE_TABLE_AS:
	case PhysicalOperatorType::INSERT:
	case PhysicalOperatorType::BATCH_INSERT:
		traits.sink_kind = ExecutionRegionSinkKind::MATERIALIZATION;
		return;
	case PhysicalOperatorType::HASH_GROUP_BY:
		traits.sink_kind = ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
		return;
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		traits.sink_kind = ExecutionRegionSinkKind::PERFECT_HASH_AGGREGATE_UPDATE;
		return;
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
		traits.sink_kind = ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
		return;
	case PhysicalOperatorType::HASH_JOIN:
		traits.sink_kind = ExecutionRegionSinkKind::HASH_JOIN_BUILD;
		return;
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		traits.sink_kind = ExecutionRegionSinkKind::NESTED_LOOP_JOIN_BUILD;
		return;
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
		traits.sink_kind = ExecutionRegionSinkKind::DELIM_JOIN_SINK;
		return;
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TOP_N:
		traits.sink_kind = ExecutionRegionSinkKind::SORT;
		return;
	default:
		traits.sink_kind = ExecutionRegionSinkKind::NONE;
		return;
	}
}

static bool TryAccumulateExecutionRegionPhysicalOperatorCost(const PhysicalOperator &op,
                                                             ExecutionRegionPhysicalPipelineCostFacts &facts,
                                                             ExecutionRegionPhysicalPipelineSlot slot) {
	auto &input = facts.cost_input;
	auto &traits = facts.traits;
	input.estimated_cardinality = MaxValue(input.estimated_cardinality, op.estimated_cardinality);
	if (slot == ExecutionRegionPhysicalPipelineSlot::SOURCE) {
		AccumulateExecutionRegionPhysicalSourceTraits(op, traits);
		AccumulateExecutionRegionPhysicalSourceContractCost(op, facts);
	}
	if (slot == ExecutionRegionPhysicalPipelineSlot::SINK) {
		AccumulateExecutionRegionPhysicalSinkTraits(op, traits);
	}
	if (slot == ExecutionRegionPhysicalPipelineSlot::SOURCE && ExecutionRegionPhysicalOperatorIsStatefulSource(op)) {
		return true;
	}
	switch (op.type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return TryAccumulateExecutionRegionPhysicalScanCost(op, facts);
	case PhysicalOperatorType::FILTER: {
		auto &filter = op.Cast<PhysicalFilter>();
		if (!filter.expression) {
			return false;
		}
		traits.filter_count++;
		AddExecutionRegionGeneratedExpressionWork(input, DuckDBCostModel::ExpressionCost(*filter.expression));
		return true;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = op.Cast<PhysicalProjection>();
		traits.projection_count++;
		AccumulateExecutionRegionPhysicalProjectionListTraits(projection.select_list, traits);
		AddExecutionRegionGeneratedExpressionWork(input,
		                                          ExecutionRegionPhysicalExpressionListCost(projection.select_list));
		return true;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggregate = op.Cast<PhysicalUngroupedAggregate>();
		AddExecutionRegionGeneratedExpressionWork(input,
		                                          ExecutionRegionPhysicalAggregateListCost(aggregate.aggregates));
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot != ExecutionRegionPhysicalPipelineSlot::SINK) {
			AddExecutionRegionNativeAggregateStage(input, false);
			return true;
		}
		auto contract = op.GetExecutionContract();
		auto &sink = contract.sink;
		if (!ExecutionRegionAggregateUpdateGeneratesBody(sink)) {
			// A ready native-state-update sink without a generated body is still a
			// native contract stage inside the region (the generated prefix feeds it
			// through the native sink binding), matching the candidate stage model.
			// Only an unready contract forms a boundary.
			if (sink.aggregate_contract.native_state_update_contract.status ==
			    ExecutionRegionStateContractStatus::READY) {
				AddExecutionRegionNativeAggregateStage(input, false);
				return true;
			}
			facts.native_sink_boundary = true;
			return true;
		}
		AddExecutionRegionGeneratedBodyStage(input);
		facts.generated_aggregate_update_count++;
		return true;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalHashAggregate>();
		if (slot == ExecutionRegionPhysicalPipelineSlot::SINK) {
			input.grouped_aggregate_estimated_cardinality = aggregate.estimated_cardinality;
		}
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.grouped_aggregate_data.groups);
		expression_cost += ExecutionRegionPhysicalAggregateListCost(aggregate.grouped_aggregate_data.aggregates);
		AddExecutionRegionGeneratedExpressionWork(input, expression_cost);
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot != ExecutionRegionPhysicalPipelineSlot::SINK) {
			AddExecutionRegionNativeAggregateStage(input, true);
			return true;
		}
		auto contract = op.GetExecutionContract();
		auto &sink = contract.sink;
		if (!ExecutionRegionAggregateUpdateGeneratesBody(sink)) {
			// Same shape as the ungrouped case: a READY native-state-update sink is a
			// native contract stage the generated prefix can feed, not a boundary.
			if (sink.aggregate_contract.native_state_update_contract.status ==
			    ExecutionRegionStateContractStatus::READY) {
				AddExecutionRegionNativeAggregateStage(input, true);
				return true;
			}
			facts.native_sink_boundary = true;
			return true;
		}
		AddExecutionRegionGeneratedBodyStage(input);
		input.generated_grouped_aggregate_stage_count++;
		if (ExecutionRegionAggregateLookupGeneratesBody(sink)) {
			AddExecutionRegionGeneratedBodyStage(input);
			input.generated_grouped_aggregate_stage_count++;
		} else {
			input.native_grouped_state_address_lookup_count++;
		}
		facts.generated_aggregate_update_count++;
		return true;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalPerfectHashAggregate>();
		if (slot == ExecutionRegionPhysicalPipelineSlot::SINK) {
			input.grouped_aggregate_estimated_cardinality = aggregate.estimated_cardinality;
		}
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.groups);
		expression_cost += ExecutionRegionPhysicalAggregateListCost(aggregate.aggregates);
		AddExecutionRegionGeneratedExpressionWork(input, expression_cost);
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot != ExecutionRegionPhysicalPipelineSlot::SINK) {
			AddExecutionRegionNativeAggregateStage(input, true);
			return true;
		}
		auto contract = op.GetExecutionContract();
		auto &sink = contract.sink;
		if (!ExecutionRegionAggregateUpdateGeneratesBody(sink)) {
			// Same shape as the ungrouped case: a READY native-state-update sink is a
			// native contract stage the generated prefix can feed, not a boundary.
			if (sink.aggregate_contract.native_state_update_contract.status ==
			    ExecutionRegionStateContractStatus::READY) {
				AddExecutionRegionNativeAggregateStage(input, true);
				return true;
			}
			facts.native_sink_boundary = true;
			return true;
		}
		AddExecutionRegionGeneratedBodyStage(input);
		input.generated_grouped_aggregate_stage_count++;
		if (ExecutionRegionAggregateLookupGeneratesBody(sink)) {
			AddExecutionRegionGeneratedBodyStage(input);
			input.generated_grouped_aggregate_stage_count++;
		} else {
			input.native_grouped_state_address_lookup_count++;
		}
		facts.generated_aggregate_update_count++;
		return true;
	}
	case PhysicalOperatorType::HASH_JOIN:
		if (slot == ExecutionRegionPhysicalPipelineSlot::OPERATOR) {
			facts.exact_source_cardinality_bounds_pipeline = false;
			traits.operator_count++;
			traits.hash_join_operator_count++;
			input.native_join_stage_count++;
		}
		return true;
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		if (slot == ExecutionRegionPhysicalPipelineSlot::OPERATOR) {
			facts.exact_source_cardinality_bounds_pipeline = false;
			traits.operator_count++;
		}
		return true;
	case PhysicalOperatorType::LEFT_DELIM_JOIN:
	case PhysicalOperatorType::RIGHT_DELIM_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::ASOF_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::POSITIONAL_JOIN:
		if (slot == ExecutionRegionPhysicalPipelineSlot::OPERATOR) {
			facts.exact_source_cardinality_bounds_pipeline = false;
		}
		return true;
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TOP_N:
		input.native_sort_stage_count++;
		return true;
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
	case PhysicalOperatorType::CTE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::EMPTY_RESULT:
	case PhysicalOperatorType::RESULT_COLLECTOR:
	case PhysicalOperatorType::EXPLAIN_ANALYZE:
	case PhysicalOperatorType::CREATE_TABLE_AS:
	case PhysicalOperatorType::BATCH_CREATE_TABLE_AS:
	case PhysicalOperatorType::INSERT:
	case PhysicalOperatorType::BATCH_INSERT:
		return true;
	default:
		return false;
	}
}

static void FinalizeExecutionRegionPhysicalPipelineCostInput(Pipeline &pipeline,
                                                             ExecutionRegionPhysicalPipelineCostFacts &facts) {
	auto &cost_input = facts.cost_input;
	auto source = pipeline.GetSource();
	if (source && facts.exact_source_cardinality_bounds_pipeline) {
		auto contract = source->GetExecutionContract();
		if (contract.source.estimated_source_cardinality_exact) {
			cost_input.estimated_cardinality = contract.source.estimated_source_cardinality;
		}
	}
	cost_input.full_pipeline = pipeline.GetSource() && pipeline.GetSink();
	facts.traits.source_execution = ExecutionRegionSourceExecutionKind::NONE;
	cost_input.generated_work_class = ClassifyExecutionRegionGeneratedWork(facts.traits);
	if (cost_input.full_pipeline &&
	    cost_input.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	    facts.traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
		D_ASSERT(source);
		auto contract = source->GetExecutionContract();
		if (ExecutionRegionPhysicalSourceUsesReadySourceContract(contract)) {
			facts.traits.source_execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
			cost_input.native_protocol_class = ClassifyExecutionRegionNativeProtocol(facts.traits);
		}
	}
	if (cost_input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	    cost_input.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	    facts.traits.sink_kind == ExecutionRegionSinkKind::SORT) {
		cost_input.native_sort_stage_count = 0;
	}
	const bool generated_filtered_reduction =
	    facts.traits.sink_kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	    cost_input.generated_backend_stage_count > 0 && facts.traits.source_filter_count > 0 &&
	    facts.traits.projection_count > 0;
	const bool generated_projection_aggregate =
	    facts.generated_aggregate_update_count > 0 && facts.traits.projection_count > 0;
	if ((facts.generated_aggregate_update_count > 0 &&
	     (facts.traits.filter_count > 0 || facts.traits.source_filter_expression_count > 0)) ||
	    generated_filtered_reduction || generated_projection_aggregate) {
		cost_input.materialization_elision_count = 1;
	}
	cost_input.source_filter_count = facts.traits.source_filter_count;
	cost_input.native_hash_join_build_sink_count =
	    facts.traits.sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD ? 1 : 0;
	cost_input.native_delim_join_sink = facts.traits.sink_kind == ExecutionRegionSinkKind::DELIM_JOIN_SINK;
	cost_input.has_accelerated_work =
	    !facts.native_sink_boundary &&
	    (cost_input.generated_stage_count > 0 || cost_input.native_join_stage_count > 0 ||
	     cost_input.native_hash_join_build_sink_count > 0 || cost_input.native_aggregate_stage_count > 0 ||
	     cost_input.native_sort_stage_count > 0 || cost_input.full_pipeline);
}

bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input) {
	if (!pipeline.GetSource()) {
		return false;
	}
	ExecutionRegionPhysicalPipelineCostFacts facts;
	facts.cost_input.input_scope = PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE;
	if (!TryAccumulateExecutionRegionPhysicalOperatorCost(*pipeline.GetSource(), facts,
	                                                      ExecutionRegionPhysicalPipelineSlot::SOURCE)) {
		return false;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		if (!TryAccumulateExecutionRegionPhysicalOperatorCost(op.get(), facts,
		                                                      ExecutionRegionPhysicalPipelineSlot::OPERATOR)) {
			return false;
		}
	}
	if (pipeline.GetSink() && !TryAccumulateExecutionRegionPhysicalOperatorCost(
	                              *pipeline.GetSink(), facts, ExecutionRegionPhysicalPipelineSlot::SINK)) {
		return false;
	}
	FinalizeExecutionRegionPhysicalPipelineCostInput(pipeline, facts);
	cost_input = facts.cost_input;
	return true;
}

static bool ExecutionRegionStatefulSourceProducesRows(const PhysicalOperator &source) {
	switch (source.type) {
	case PhysicalOperatorType::HASH_JOIN: {
		auto &join = source.Cast<PhysicalHashJoin>();
		return ExecutionRegionJoinTypePropagatesBuildSide(ExecutionRegionJoinTypeFromDuckDB(join.join_type));
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		auto &join = source.Cast<PhysicalNestedLoopJoin>();
		return ExecutionRegionJoinTypePropagatesBuildSide(ExecutionRegionJoinTypeFromDuckDB(join.join_type));
	}
	default:
		return true;
	}
}

bool ExecutionRegionPipelineHasNonProducingSource(Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	return source && !ExecutionRegionStatefulSourceProducesRows(*source);
}

} // namespace duckdb

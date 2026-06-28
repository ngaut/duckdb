//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution_region_cost_input.cpp
//
//===----------------------------------------------------------------------===//

#include "execution_region_cost_input.hpp"

#include "duckdb/execution/execution_region_ir.hpp"
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
#include "duckdb/planner/expression_iterator.hpp"

#include "execution_region_duckdb_type_adapter.hpp"

namespace duckdb {

class ExecutionRegionRunnerCostInputBuilder {
public:
	void SetEstimatedCardinality(idx_t estimated_cardinality) {
		input.estimated_cardinality = estimated_cardinality;
	}

	void SetInputScope(PhysicalRunnerCostInputScope input_scope) {
		input.input_scope = input_scope;
	}

	void MaxEstimatedCardinality(idx_t estimated_cardinality) {
		input.estimated_cardinality = MaxValue(input.estimated_cardinality, estimated_cardinality);
	}

	void SetExpressionCost(idx_t expression_cost) {
		input.expression_cost = expression_cost;
	}

	void AddGeneratedExpressionWork(idx_t expression_cost) {
		if (expression_cost == 0) {
			return;
		}
		input.expression_cost += expression_cost;
		input.generated_stage_count++;
	}

	void SetGeneratedStageCount(idx_t generated_stage_count) {
		input.generated_stage_count = generated_stage_count;
	}

	void SetMaterializationElisionCount(idx_t materialization_elision_count) {
		input.materialization_elision_count = materialization_elision_count;
	}

	void SetMaterializationSourceAppendCount(idx_t materialization_source_append_count) {
		input.materialization_source_append_count = materialization_source_append_count;
	}

	void SetNativeJoinStageCount(idx_t native_join_stage_count) {
		input.native_join_stage_count = native_join_stage_count;
	}

	void AddNativeJoinStage() {
		input.native_join_stage_count++;
	}

	void SetNativeAggregateStageCount(idx_t native_aggregate_stage_count) {
		input.native_aggregate_stage_count = native_aggregate_stage_count;
	}

	void SetNativeGroupedAggregateStageCount(idx_t native_grouped_aggregate_stage_count) {
		input.native_grouped_aggregate_stage_count = native_grouped_aggregate_stage_count;
	}

	void SetBlockedHashAggregateLookupCount(idx_t blocked_hash_aggregate_lookup_count) {
		input.blocked_hash_aggregate_lookup_count = blocked_hash_aggregate_lookup_count;
	}

	void AddBlockedHashAggregateLookup() {
		input.blocked_hash_aggregate_lookup_count++;
	}

	void AddNativeAggregateStage(bool grouped) {
		input.native_aggregate_stage_count++;
		if (grouped) {
			input.native_grouped_aggregate_stage_count++;
		}
	}

	void SetNativeSortStageCount(idx_t native_sort_stage_count) {
		input.native_sort_stage_count = native_sort_stage_count;
	}

	void AddNativeSortStage() {
		input.native_sort_stage_count++;
	}

	void ClearNativeSortStages() {
		input.native_sort_stage_count = 0;
	}

	void SetFullPipeline(bool full_pipeline) {
		input.full_pipeline = full_pipeline;
	}

	void SetUsesScanFilters(bool uses_scan_filters) {
		input.uses_scan_filters = uses_scan_filters;
	}

	void SetSourceFilterCount(idx_t source_filter_count) {
		input.source_filter_count = source_filter_count;
	}

	void SetNodeCount(idx_t node_count) {
		input.node_count = node_count;
	}

	void AddOperatorNode() {
		input.node_count++;
		input.stage_count++;
		input.operator_count++;
	}

	void SetStageCount(idx_t stage_count) {
		input.stage_count = stage_count;
	}

	void SetExpressionNodeCount(idx_t expression_node_count) {
		input.expression_node_count = expression_node_count;
	}

	void SetOperatorCount(idx_t operator_count) {
		input.operator_count = operator_count;
	}

	void SetGeneratedWorkClass(PhysicalRunnerGeneratedWorkClass generated_work_class) {
		input.generated_work_class = generated_work_class;
	}

	void SetNativeProtocolClass(PhysicalRunnerNativeProtocolClass native_protocol_class) {
		input.native_protocol_class = native_protocol_class;
	}

	void SetHasAcceleratedWork(bool has_accelerated_work) {
		input.has_accelerated_work = has_accelerated_work;
	}

	PhysicalRunnerCostInput &MutableInput() {
		return input;
	}

	const PhysicalRunnerCostInput &Input() const {
		return input;
	}

	PhysicalRunnerCostInput Build() const {
		return input;
	}

private:
	PhysicalRunnerCostInput input;
};

static bool ExecutionRegionStageIsExecutable(const ExecutionRegionStage &stage) {
	return stage.executable_work && stage.execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
	       stage.execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
}

enum class ExecutionRegionStageCostWorkKind : uint8_t {
	NONE,
	GENERATED,
	NATIVE_JOIN,
	NATIVE_GROUPED_AGGREGATE,
	NATIVE_UNGROUPED_AGGREGATE,
	NATIVE_SORT
};

enum class ExecutionRegionStageCostRole : uint8_t { NONE, FILTER };

struct ExecutionRegionStageCostFact {
	ExecutionRegionStageCostWorkKind work_kind = ExecutionRegionStageCostWorkKind::NONE;
	ExecutionRegionStageCostRole role = ExecutionRegionStageCostRole::NONE;
	bool may_anchor_compiled_body = false;
};

struct ExecutionRegionCostFacts {
	idx_t generated_stage_count = 0;
	idx_t materialization_elision_count = 0;
	idx_t native_join_stage_count = 0;
	idx_t native_aggregate_stage_count = 0;
	idx_t native_grouped_aggregate_stage_count = 0;
	idx_t native_sort_stage_count = 0;
	bool may_anchor_compiled_body = false;
	PhysicalRunnerGeneratedWorkClass generated_work_class = PhysicalRunnerGeneratedWorkClass::NONE;
	PhysicalRunnerNativeProtocolClass native_protocol_class = PhysicalRunnerNativeProtocolClass::NONE;
};

static ExecutionRegionStageCostFact GetExecutionRegionStageCostFact(const ExecutionRegionStage &stage) {
	ExecutionRegionStageCostFact result;
	if (!ExecutionRegionStageIsExecutable(stage)) {
		return result;
	}
	if (stage.kind == ExecutionRegionStageKind::FILTER || stage.kind == ExecutionRegionStageKind::SOURCE_FILTER) {
		result.role = ExecutionRegionStageCostRole::FILTER;
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
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE:
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_JOIN;
		result.may_anchor_compiled_body = true;
		break;
	case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionStageKind::HASH_AGGREGATE_DISTINCT_SINK:
	case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_GROUPED_AGGREGATE;
		result.may_anchor_compiled_body = true;
		break;
	case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_UNGROUPED_AGGREGATE;
		result.may_anchor_compiled_body = true;
		break;
	case ExecutionRegionStageKind::SORT_SINK:
		result.work_kind = ExecutionRegionStageCostWorkKind::NATIVE_SORT;
		result.may_anchor_compiled_body = true;
		break;
	default:
		break;
	}
	return result;
}

static bool ExecutionRegionCandidateHasGeneratedFilterOrOperatorWork(const ExecutionRegionCandidateTraits &traits) {
	return traits.source_filter_expression_count > 0 || traits.filter_count > 0 || traits.comparison_filter_count > 0 ||
	       traits.conjunction_filter_count > 0 || traits.hash_join_operator_count > 0 || traits.aggregate_count > 0;
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

static bool ExecutionRegionStageCostWorkIsNativeAggregate(ExecutionRegionStageCostWorkKind work_kind) {
	return work_kind == ExecutionRegionStageCostWorkKind::NATIVE_GROUPED_AGGREGATE ||
	       work_kind == ExecutionRegionStageCostWorkKind::NATIVE_UNGROUPED_AGGREGATE;
}

static void AccumulateExecutionRegionCostFact(const ExecutionRegionStageCostFact &fact,
                                              ExecutionRegionCostFacts &result, bool &has_filter) {
	result.may_anchor_compiled_body = result.may_anchor_compiled_body || fact.may_anchor_compiled_body;
	has_filter = has_filter || fact.role == ExecutionRegionStageCostRole::FILTER;
	if (has_filter && result.materialization_elision_count == 0 &&
	    ExecutionRegionStageCostWorkIsNativeAggregate(fact.work_kind)) {
		result.materialization_elision_count = 1;
	}
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
	case ExecutionRegionStageCostWorkKind::NATIVE_SORT:
		result.native_sort_stage_count++;
		return;
	case ExecutionRegionStageCostWorkKind::NONE:
		return;
	}
}

static ExecutionRegionCostFacts BuildExecutionRegionCostFacts(const ExecutionRegionCandidate &candidate) {
	ExecutionRegionCostFacts result;
	bool has_filter = false;
	for (auto &stage : candidate.stage_plan.stages) {
		AccumulateExecutionRegionCostFact(GetExecutionRegionStageCostFact(stage), result, has_filter);
	}
	result.generated_work_class = ClassifyExecutionRegionGeneratedWork(candidate.traits);
	result.native_protocol_class = ClassifyExecutionRegionNativeProtocol(candidate.traits);
	return result;
}

static bool ExecutionRegionHashAggregateLookupIsBlocked(const ExecutionRegionAggregateContract &contract) {
	return contract.hash_lookup_layout_present &&
	       contract.native_hash_lookup_contract.status != ExecutionRegionStateContractStatus::READY;
}

PhysicalRunnerCostInput BuildExecutionRegionCandidateCostInput(const ExecutionRegionCandidate &candidate) {
	auto cost_facts = BuildExecutionRegionCostFacts(candidate);
	ExecutionRegionRunnerCostInputBuilder builder;
	builder.SetInputScope(PhysicalRunnerCostInputScope::EXECUTION_REGION_CANDIDATE);
	builder.SetEstimatedCardinality(candidate.estimated_cardinality);
	builder.SetExpressionCost(candidate.traits.expression_cost);
	builder.SetGeneratedStageCount(cost_facts.generated_stage_count);
	builder.SetMaterializationElisionCount(cost_facts.materialization_elision_count);
	builder.SetMaterializationSourceAppendCount(candidate.traits.sink_kind == ExecutionRegionSinkKind::MATERIALIZATION
	                                                ? candidate.traits.reference_varchar_projection_count
	                                                : 0);
	builder.SetNativeJoinStageCount(cost_facts.native_join_stage_count);
	builder.SetNativeAggregateStageCount(cost_facts.native_aggregate_stage_count);
	builder.SetNativeGroupedAggregateStageCount(cost_facts.native_grouped_aggregate_stage_count);
	if (candidate.contract.hash_aggregate_lookup_present &&
	    candidate.contract.hash_aggregate_lookup_mode == "blocked") {
		builder.SetBlockedHashAggregateLookupCount(cost_facts.native_grouped_aggregate_stage_count);
	}
	builder.SetNativeSortStageCount(cost_facts.native_sort_stage_count);
	builder.SetSourceFilterCount(candidate.traits.source_filter_count);
	builder.SetFullPipeline(ExecutionRegionABIIsFullPipeline(candidate.contract.abi) &&
	                        cost_facts.may_anchor_compiled_body);
	builder.SetUsesScanFilters(candidate.uses_scan_filters);
	builder.SetNodeCount(candidate.node_count);
	builder.SetStageCount(candidate.stage_plan.stages.size());
	builder.SetExpressionNodeCount(candidate.traits.expression_node_count);
	builder.SetOperatorCount(candidate.traits.operator_count);
	builder.SetGeneratedWorkClass(cost_facts.generated_work_class);
	builder.SetNativeProtocolClass(cost_facts.native_protocol_class);
	builder.SetHasAcceleratedWork(cost_facts.may_anchor_compiled_body);
	return builder.Build();
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

static bool ExecutionRegionPhysicalAggregateSinkUsesPrimitivePayloads(const ExecutionRegionSinkInfo &sink) {
	if (sink.aggregate_contract.native_state_update_contract.status != ExecutionRegionStateContractStatus::READY) {
		return false;
	}
	if (!ExecutionRegionAggregateNativeStateUpdateBlocker(sink.aggregate_contract, sink.aggregates, sink.groups)
	         .empty()) {
		return false;
	}
	for (auto &aggregate : sink.aggregates) {
		if (!aggregate.primitive_update_ready) {
			return false;
		}
	}
	return true;
}

static bool ExecutionRegionPhysicalAggregateSinkCanAnchorNativePipeline(const PhysicalOperator &op) {
	return ExecutionRegionPhysicalAggregateSinkUsesPrimitivePayloads(op.GetExecutionContract().sink);
}

enum class ExecutionRegionPhysicalPipelineSlot : uint8_t { SOURCE, OPERATOR, SINK };

struct ExecutionRegionPhysicalPipelineCostFacts {
	ExecutionRegionRunnerCostInputBuilder builder;
	ExecutionRegionCandidateTraits traits;
	bool native_sink_boundary = false;
};

static bool ExecutionRegionPhysicalExpressionIsReference(const Expression &expression) {
	auto expression_class = expression.GetExpressionClass();
	return expression_class == ExpressionClass::BOUND_REF || expression_class == ExpressionClass::BOUND_COLUMN_REF;
}

static bool ExecutionRegionPhysicalExpressionIsReferenceVarchar(const Expression &expression) {
	return ExecutionRegionPhysicalExpressionIsReference(expression) &&
	       expression.GetReturnType().id() == LogicalTypeId::VARCHAR;
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
		if (ExecutionRegionPhysicalExpressionIsReferenceVarchar(expression)) {
			traits.reference_varchar_projection_count++;
		}
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

static bool TryAccumulateExecutionRegionPhysicalScanCost(const PhysicalOperator &op,
                                                         ExecutionRegionPhysicalPipelineCostFacts &facts) {
	if (op.type != PhysicalOperatorType::TABLE_SCAN) {
		return true;
	}
	auto &scan = op.Cast<PhysicalTableScan>();
	const bool has_pushed_dynamic_filters =
	    scan.function.filter_pushdown && scan.dynamic_filters && scan.dynamic_filters->HasFilters();
	if (has_pushed_dynamic_filters) {
		facts.builder.SetUsesScanFilters(true);
	}
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
	}
	idx_t filter_cost = 0;
	idx_t filter_count = 0;
	for (auto &filter : *scan.table_filters) {
		filter_cost += DuckDBCostModel::FilterCost(filter.Filter());
		filter_count++;
	}
	facts.traits.source_filter_count += filter_count;
	facts.traits.source_filter_expression_count += filter_count;
	if (scan.function.filter_pushdown) {
		facts.builder.SetUsesScanFilters(true);
	}
	facts.builder.AddGeneratedExpressionWork(filter_cost);
	return true;
}

static void AccumulateExecutionRegionPhysicalSourceTraits(const PhysicalOperator &source,
                                                          ExecutionRegionCandidateTraits &traits) {
	switch (source.type) {
	case PhysicalOperatorType::TABLE_SCAN:
		traits.source_kind = ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN;
		return;
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		traits.source_kind = ExecutionRegionSourceKind::GENERIC_SCAN;
		return;
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
		traits.source_kind = ExecutionRegionSourceKind::STATEFUL_OPERATOR;
		return;
	default:
		traits.source_kind = ExecutionRegionSourceKind::NONE;
		return;
	}
}

static bool ExecutionRegionPhysicalOperatorIsStatefulSource(const PhysicalOperator &op) {
	switch (op.type) {
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
		return true;
	default:
		return false;
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
	auto &builder = facts.builder;
	auto &traits = facts.traits;
	builder.MaxEstimatedCardinality(op.estimated_cardinality);
	builder.AddOperatorNode();
	if (slot == ExecutionRegionPhysicalPipelineSlot::SOURCE) {
		AccumulateExecutionRegionPhysicalSourceTraits(op, traits);
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
		builder.AddGeneratedExpressionWork(DuckDBCostModel::ExpressionCost(*filter.expression));
		return true;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = op.Cast<PhysicalProjection>();
		traits.projection_count++;
		AccumulateExecutionRegionPhysicalProjectionListTraits(projection.select_list, traits);
		builder.AddGeneratedExpressionWork(ExecutionRegionPhysicalExpressionListCost(projection.select_list));
		return true;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggregate = op.Cast<PhysicalUngroupedAggregate>();
		builder.AddGeneratedExpressionWork(ExecutionRegionPhysicalAggregateListCost(aggregate.aggregates));
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot == ExecutionRegionPhysicalPipelineSlot::SINK &&
		    !ExecutionRegionPhysicalAggregateSinkCanAnchorNativePipeline(op)) {
			facts.native_sink_boundary = true;
			return true;
		}
		builder.AddNativeAggregateStage(false);
		return true;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalHashAggregate>();
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.grouped_aggregate_data.groups);
		expression_cost += ExecutionRegionPhysicalAggregateListCost(aggregate.grouped_aggregate_data.aggregates);
		builder.AddGeneratedExpressionWork(expression_cost);
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot == ExecutionRegionPhysicalPipelineSlot::SINK &&
		    !ExecutionRegionPhysicalAggregateSinkCanAnchorNativePipeline(op)) {
			facts.native_sink_boundary = true;
			return true;
		}
		builder.AddNativeAggregateStage(true);
		if (ExecutionRegionHashAggregateLookupIsBlocked(aggregate.GetExecutionContract().sink.aggregate_contract)) {
			builder.AddBlockedHashAggregateLookup();
		}
		return true;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalPerfectHashAggregate>();
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.groups);
		expression_cost += ExecutionRegionPhysicalAggregateListCost(aggregate.aggregates);
		builder.AddGeneratedExpressionWork(expression_cost);
		if (slot != ExecutionRegionPhysicalPipelineSlot::SOURCE) {
			traits.aggregate_count++;
		}
		if (slot == ExecutionRegionPhysicalPipelineSlot::SINK &&
		    !ExecutionRegionPhysicalAggregateSinkCanAnchorNativePipeline(op)) {
			facts.native_sink_boundary = true;
			return true;
		}
		builder.AddNativeAggregateStage(true);
		return true;
	}
	case PhysicalOperatorType::HASH_JOIN:
		if (slot == ExecutionRegionPhysicalPipelineSlot::OPERATOR) {
			traits.operator_count++;
			traits.hash_join_operator_count++;
			builder.AddNativeJoinStage();
		}
		return true;
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		if (slot == ExecutionRegionPhysicalPipelineSlot::OPERATOR) {
			traits.operator_count++;
			builder.AddNativeJoinStage();
		} else if (slot == ExecutionRegionPhysicalPipelineSlot::SINK) {
			builder.AddNativeJoinStage();
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
		return true;
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TOP_N:
		builder.AddNativeSortStage();
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

static bool ExecutionRegionPhysicalSourceUsesReadySourceContract(const PhysicalOperator &source) {
	auto contract = source.GetExecutionContract();
	return contract.source.execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	       contract.source.source_contract.status == ExecutionRegionSourceContractStatus::READY;
}

static void FinalizeExecutionRegionPhysicalPipelineCostInput(Pipeline &pipeline,
                                                             ExecutionRegionPhysicalPipelineCostFacts &facts) {
	auto &builder = facts.builder;
	auto &cost_input = builder.MutableInput();
	builder.SetFullPipeline(pipeline.GetSource() && pipeline.GetSink());
	facts.traits.source_execution = ExecutionRegionSourceExecutionKind::NONE;
	builder.SetGeneratedWorkClass(ClassifyExecutionRegionGeneratedWork(facts.traits));
	if (cost_input.full_pipeline &&
	    cost_input.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	    facts.traits.source_kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
		auto source = pipeline.GetSource();
		D_ASSERT(source);
		if (ExecutionRegionPhysicalSourceUsesReadySourceContract(*source)) {
			facts.traits.source_execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
			builder.SetNativeProtocolClass(ClassifyExecutionRegionNativeProtocol(facts.traits));
		}
	}
	if (cost_input.native_protocol_class == PhysicalRunnerNativeProtocolClass::STATEFUL_SOURCE_SINK_PROTOCOL &&
	    cost_input.generated_work_class == PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE &&
	    facts.traits.sink_kind == ExecutionRegionSinkKind::SORT) {
		builder.ClearNativeSortStages();
	}
	if (cost_input.native_aggregate_stage_count > 0 &&
	    (facts.traits.filter_count > 0 || facts.traits.source_filter_expression_count > 0)) {
		builder.SetMaterializationElisionCount(1);
	}
	builder.SetSourceFilterCount(facts.traits.source_filter_count);
	builder.SetMaterializationSourceAppendCount(facts.traits.sink_kind == ExecutionRegionSinkKind::MATERIALIZATION
	                                                ? facts.traits.reference_varchar_projection_count
	                                                : 0);
	builder.SetHasAcceleratedWork(!facts.native_sink_boundary &&
	                              (cost_input.generated_stage_count > 0 || cost_input.native_join_stage_count > 0 ||
	                               cost_input.native_aggregate_stage_count > 0 ||
	                               cost_input.native_sort_stage_count > 0 || cost_input.full_pipeline));
}

bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input) {
	if (!pipeline.GetSource()) {
		return false;
	}
	ExecutionRegionPhysicalPipelineCostFacts facts;
	facts.builder.SetInputScope(PhysicalRunnerCostInputScope::PHYSICAL_PIPELINE);
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
	cost_input = facts.builder.Build();
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

PhysicalRunnerCostInput BuildExecutionRegionNonProducingSourceCostInput(const PhysicalOperator &source) {
	ExecutionRegionRunnerCostInputBuilder builder;
	builder.SetEstimatedCardinality(source.estimated_cardinality);
	builder.SetNodeCount(1);
	builder.SetStageCount(1);
	builder.SetOperatorCount(1);
	return builder.Build();
}

} // namespace duckdb

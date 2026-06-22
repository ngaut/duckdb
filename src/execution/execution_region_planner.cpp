#include "duckdb/execution/execution_region_planner.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/execution_region_lowering.hpp"
#include "duckdb/execution/execution_region_manager.hpp"
#include "duckdb/execution/execution_region_settings.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/cost_model.hpp"

#include "execution_region_duckdb_type_adapter.hpp"

#include <chrono>

namespace duckdb {

struct ExecutionRegionPhysicalRunnerSelection {
	PhysicalRunnerCostProfile runner_cost;
	bool use_compiled_runner = false;
	string reason;
	string blocker;

	bool UsesCompiledRunner() const {
		return use_compiled_runner;
	}

	ExecutionRunnerKind SelectedRunner() const {
		return UsesCompiledRunner() ? ExecutionRunnerKind::COMPILED_VECTORIZED : ExecutionRunnerKind::VECTORIZED;
	}
};

static string ComposeExecutionRegionCompileEventReason(const ExecutionRegionPhysicalRunnerSelection &selection,
                                                       const string &compile_reason) {
	if (selection.reason.empty()) {
		return compile_reason;
	}
	if (compile_reason.empty()) {
		return selection.reason;
	}
	return selection.reason + ";" + compile_reason;
}

static string AttachExecutionRegionCandidateReason(const ExecutionRegionCandidate &candidate, string reason,
                                                   bool record_detailed_telemetry) {
	if (!record_detailed_telemetry) {
		return reason;
	}
	if (!reason.empty()) {
		reason += ";";
	}
	reason += "candidate_id=" + std::to_string(candidate.candidate_id);
	reason += ";candidate_shape=" + candidate.shape;
	return reason;
}

static string FirstExecutionRegionReasonToken(const string &reason) {
	auto separator = reason.find(';');
	return separator == string::npos ? reason : reason.substr(0, separator);
}

static string ExecutionRegionCandidateBlockerCode(const ExecutionRegionIR &region_ir) {
	if (region_ir.candidate_blockers.empty()) {
		return "no_execution_region_candidates";
	}
	if (region_ir.candidate_blockers[0].find("candidate-builder-blocked:no-executable-work") != string::npos) {
		return "no_executable_region_work";
	}
	return "no_execution_region_candidates";
}

static string ExecutionRegionUnsupportedBlockerCode(const ExecutionRegionLoweringPlan &lowering_plan) {
	return lowering_plan.NativeCount() > 0 ? "unsupported_region_execution" : "region_contains_no_native_nodes";
}

static string ExecutionRegionCompileResultBlockerCode(ExecutionRegionCompileStatus status) {
	if (status == ExecutionRegionCompileStatus::COMPILED) {
		return string();
	}
	string result = "backend_compile_";
	result += ExecutionRegionCompileStatusToString(status);
	return result;
}

static string ExecutionRegionLoweringEventReason(const ExecutionRegionLoweringPlan &lowering_plan,
                                                 bool record_detailed_telemetry) {
	return record_detailed_telemetry ? lowering_plan.EventReason() : lowering_plan.CompactEventReason();
}

static int64_t ExecutionRegionPlannerElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

static bool ExecutionRegionStageIsExecutable(const ExecutionRegionStage &stage) {
	return stage.executable_work && stage.execution != ExecutionRegionStageExecutionKind::MISSING_CONTRACT &&
	       stage.execution != ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY;
}

static bool ExecutionRegionStageIsJoinCostWork(const ExecutionRegionStage &stage) {
	if (!ExecutionRegionStageIsExecutable(stage) ||
	    stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
		return false;
	}
	return stage.kind == ExecutionRegionStageKind::HASH_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::HASH_JOIN_BUILD ||
	       stage.kind == ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE ||
	       stage.kind == ExecutionRegionStageKind::NESTED_LOOP_JOIN_BUILD;
}

static bool ExecutionRegionStageIsAggregateCostWork(const ExecutionRegionStage &stage) {
	if (!ExecutionRegionStageIsExecutable(stage) ||
	    stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
		return false;
	}
	switch (stage.kind) {
	case ExecutionRegionStageKind::HASH_AGGREGATE_UPDATE:
	case ExecutionRegionStageKind::PERFECT_HASH_AGGREGATE_UPDATE:
	case ExecutionRegionStageKind::UNGROUPED_AGGREGATE_UPDATE:
		return true;
	default:
		return false;
	}
}

static bool ExecutionRegionStageIsSortCostWork(const ExecutionRegionStage &stage) {
	return ExecutionRegionStageIsExecutable(stage) &&
	       stage.execution == ExecutionRegionStageExecutionKind::NATIVE_CONTRACT &&
	       stage.kind == ExecutionRegionStageKind::SORT_SINK;
}

static bool ExecutionRegionStageIsGeneratedCostWork(const ExecutionRegionStage &stage) {
	if (!ExecutionRegionStageIsExecutable(stage)) {
		return false;
	}
	return stage.execution == ExecutionRegionStageExecutionKind::GENERATED_IR;
}

static void AccumulateExecutionRegionPhysicalRunnerStageCost(const ExecutionRegionStage &stage,
                                                             PhysicalRunnerCostInput &result) {
	if (ExecutionRegionStageIsGeneratedCostWork(stage)) {
		result.generated_stage_count++;
	}
	if (ExecutionRegionStageIsJoinCostWork(stage)) {
		result.native_join_stage_count++;
	}
	if (ExecutionRegionStageIsAggregateCostWork(stage)) {
		result.native_aggregate_stage_count++;
	}
	if (ExecutionRegionStageIsSortCostWork(stage)) {
		result.native_sort_stage_count++;
	}
}

static bool ExecutionRegionStageIsExecutableFilter(const ExecutionRegionStage &stage) {
	return ExecutionRegionStageIsExecutable(stage) &&
	       (stage.kind == ExecutionRegionStageKind::FILTER || stage.kind == ExecutionRegionStageKind::SOURCE_FILTER);
}

static bool ExecutionRegionStageIsExecutableAggregateUpdate(const ExecutionRegionStage &stage) {
	return ExecutionRegionStageIsAggregateCostWork(stage);
}

static idx_t CountExecutionRegionMaterializationElisions(const ExecutionRegionStagePlan &stage_plan) {
	bool has_filter = false;
	for (auto &stage : stage_plan.stages) {
		has_filter = has_filter || ExecutionRegionStageIsExecutableFilter(stage);
		if (has_filter && ExecutionRegionStageIsExecutableAggregateUpdate(stage)) {
			return 1;
		}
	}
	return 0;
}

static PhysicalRunnerCostInput BuildPhysicalRunnerCostInput(const ExecutionRegionCandidate &candidate) {
	PhysicalRunnerCostInput result;
	result.estimated_cardinality = candidate.estimated_cardinality;
	result.expression_cost = candidate.traits.expression_cost;
	for (auto &stage : candidate.stage_plan.stages) {
		AccumulateExecutionRegionPhysicalRunnerStageCost(stage, result);
	}
	result.materialization_elision_count = CountExecutionRegionMaterializationElisions(candidate.stage_plan);
	result.full_pipeline =
	    ExecutionRegionABIIsFullPipeline(candidate.contract.abi) && candidate.stage_plan.HasExecutableWork();
	result.node_count = candidate.node_count;
	result.stage_count = candidate.stage_plan.stages.size();
	result.expression_node_count = candidate.traits.expression_node_count;
	result.operator_count = candidate.traits.operator_count;
	result.has_accelerated_work = candidate.stage_plan.HasExecutableWork();
	return result;
}

static PhysicalRunnerCostParameters BuildPhysicalRunnerCostParameters(ClientContext &context) {
	PhysicalRunnerCostParameters result;
	result.generated_stage_benefit = Settings::Get<JitCboGeneratedStageBenefitSetting>(context);
	result.native_operator_stage_benefit = Settings::Get<JitCboNativeOperatorStageBenefitSetting>(context);
	result.materialization_elision_benefit = Settings::Get<JitCboMaterializationElisionBenefitSetting>(context);
	result.full_pipeline_benefit = Settings::Get<JitCboFullPipelineBenefitSetting>(context);
	result.startup_base_cost = Settings::Get<JitCboStartupBaseCostSetting>(context);
	result.startup_margin_basis_points = Settings::Get<JitCboStartupMarginBasisPointsSetting>(context);
	return result;
}

static bool PhysicalRunnerCostingHasEnabledBenefit(const PhysicalRunnerCostParameters &parameters) {
	return parameters.generated_stage_benefit > 0 || parameters.native_operator_stage_benefit > 0 ||
	       parameters.materialization_elision_benefit > 0 || parameters.full_pipeline_benefit > 0;
}

static bool ExecutionRegionProductionEligibilityAllowsPlanning(ClientContext &context,
                                                               const PhysicalRunnerCostParameters &parameters) {
	if (ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context)) {
		return true;
	}
	return PhysicalRunnerCostingHasEnabledBenefit(parameters);
}

static bool ExecutionRegionPlanningNeedsBackendDiagnostics(ClientContext &context) {
	return ExecutionRegionSettings::DumpIR(context);
}

static bool ExecutionRegionPlanningNeedsCandidateDiagnostics(ClientContext &context) {
	return ExecutionRegionSettings::TraceDecisions(context) || ExecutionRegionSettings::DumpIR(context);
}

static bool ExecutionRegionGraphMayHaveCostedAcceleration(const ExecutionRegionGraph &graph,
                                                          const PhysicalRunnerCostParameters &parameters) {
	if (parameters.generated_stage_benefit > 0 && graph.HasGeneratedExpression()) {
		return true;
	}
	if (parameters.native_operator_stage_benefit > 0 && graph.HasNativeOperatorWork()) {
		return true;
	}
	if (parameters.materialization_elision_benefit > 0 && graph.HasGeneratedExpression() && graph.HasSink()) {
		return true;
	}
	if (parameters.full_pipeline_benefit > 0 && graph.HasSource() && graph.HasSink()) {
		return true;
	}
	return false;
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

static void ExecutionRegionAccumulateGeneratedPhysicalExpressions(idx_t expression_cost,
                                                                  PhysicalRunnerCostInput &cost_input) {
	if (expression_cost == 0) {
		return;
	}
	cost_input.expression_cost += expression_cost;
	cost_input.generated_stage_count++;
}

static bool TryAccumulateExecutionRegionPhysicalScanCost(const PhysicalOperator &op,
                                                         PhysicalRunnerCostInput &cost_input) {
	if (op.type != PhysicalOperatorType::TABLE_SCAN) {
		return true;
	}
	auto &scan = op.Cast<PhysicalTableScan>();
	if (scan.dynamic_filters && scan.dynamic_filters->HasFilters()) {
		return false;
	}
	if (!scan.table_filters || !scan.table_filters->HasFilters()) {
		return true;
	}
	idx_t filter_cost = 0;
	for (auto &filter : *scan.table_filters) {
		filter_cost += DuckDBCostModel::FilterCost(filter.Filter());
	}
	ExecutionRegionAccumulateGeneratedPhysicalExpressions(filter_cost, cost_input);
	return true;
}

static bool TryAccumulateExecutionRegionPhysicalOperatorCost(const PhysicalOperator &op,
                                                             PhysicalRunnerCostInput &cost_input) {
	cost_input.estimated_cardinality = MaxValue(cost_input.estimated_cardinality, op.estimated_cardinality);
	cost_input.node_count++;
	cost_input.stage_count++;
	cost_input.operator_count++;
	switch (op.type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return TryAccumulateExecutionRegionPhysicalScanCost(op, cost_input);
	case PhysicalOperatorType::FILTER: {
		auto &filter = op.Cast<PhysicalFilter>();
		if (!filter.expression) {
			return false;
		}
		ExecutionRegionAccumulateGeneratedPhysicalExpressions(DuckDBCostModel::ExpressionCost(*filter.expression),
		                                                      cost_input);
		return true;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto &projection = op.Cast<PhysicalProjection>();
		ExecutionRegionAccumulateGeneratedPhysicalExpressions(
		    ExecutionRegionPhysicalExpressionListCost(projection.select_list), cost_input);
		return true;
	}
	case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
		auto &aggregate = op.Cast<PhysicalUngroupedAggregate>();
		ExecutionRegionAccumulateGeneratedPhysicalExpressions(
		    ExecutionRegionPhysicalExpressionListCost(aggregate.aggregates), cost_input);
		cost_input.native_aggregate_stage_count++;
		return true;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalHashAggregate>();
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.grouped_aggregate_data.groups);
		expression_cost += ExecutionRegionPhysicalExpressionListCost(aggregate.grouped_aggregate_data.aggregates);
		ExecutionRegionAccumulateGeneratedPhysicalExpressions(expression_cost, cost_input);
		cost_input.native_aggregate_stage_count++;
		return true;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		auto &aggregate = op.Cast<PhysicalPerfectHashAggregate>();
		auto expression_cost = ExecutionRegionPhysicalExpressionListCost(aggregate.groups);
		expression_cost += ExecutionRegionPhysicalExpressionListCost(aggregate.aggregates);
		ExecutionRegionAccumulateGeneratedPhysicalExpressions(expression_cost, cost_input);
		cost_input.native_aggregate_stage_count++;
		return true;
	}
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		cost_input.native_join_stage_count++;
		return true;
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TOP_N:
		cost_input.native_sort_stage_count++;
		return true;
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::CTE_SCAN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
	case PhysicalOperatorType::CTE:
	case PhysicalOperatorType::RESULT_COLLECTOR:
	case PhysicalOperatorType::EXPLAIN_ANALYZE:
	case PhysicalOperatorType::CREATE_TABLE_AS:
	case PhysicalOperatorType::INSERT:
	case PhysicalOperatorType::BATCH_INSERT:
		return true;
	default:
		return false;
	}
}

static bool TryBuildExecutionRegionPipelineCostInput(Pipeline &pipeline, PhysicalRunnerCostInput &cost_input) {
	if (!pipeline.GetSource()) {
		return false;
	}
	cost_input = PhysicalRunnerCostInput();
	if (!TryAccumulateExecutionRegionPhysicalOperatorCost(*pipeline.GetSource(), cost_input)) {
		return false;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		if (!TryAccumulateExecutionRegionPhysicalOperatorCost(op.get(), cost_input)) {
			return false;
		}
	}
	if (pipeline.GetSink() && !TryAccumulateExecutionRegionPhysicalOperatorCost(*pipeline.GetSink(), cost_input)) {
		return false;
	}
	if (cost_input.generated_stage_count > 0 && cost_input.native_aggregate_stage_count > 0) {
		cost_input.materialization_elision_count = 1;
	}
	cost_input.full_pipeline = pipeline.GetSource() && pipeline.GetSink();
	cost_input.has_accelerated_work = cost_input.generated_stage_count > 0 || cost_input.native_join_stage_count > 0 ||
	                                  cost_input.native_aggregate_stage_count > 0 ||
	                                  cost_input.native_sort_stage_count > 0 || cost_input.full_pipeline;
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

static bool ExecutionRegionPipelineHasNonProducingSource(Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	return source && !ExecutionRegionStatefulSourceProducesRows(*source);
}

static PhysicalRunnerCostInput BuildExecutionRegionNonProducingSourceCostInput(const PhysicalOperator &source) {
	PhysicalRunnerCostInput result;
	result.estimated_cardinality = source.estimated_cardinality;
	result.node_count = 1;
	result.stage_count = 1;
	result.operator_count = 1;
	return result;
}

static ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPipelinePhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters, Pipeline &pipeline) {
	ExecutionRegionPhysicalRunnerSelection selection;
	if (ExecutionRegionPipelineHasNonProducingSource(pipeline)) {
		auto source = pipeline.GetSource();
		D_ASSERT(source);
		auto cost_input = BuildExecutionRegionNonProducingSourceCostInput(*source);
		selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
		selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
		selection.reason += ";region_graph=skipped;source_produces_rows=false";
		selection.blocker = "duckdb_selected_vectorized";
		return selection;
	}
	PhysicalRunnerCostInput cost_input;
	if (!TryBuildExecutionRegionPipelineCostInput(pipeline, cost_input)) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo requires execution-region graph for physical runner decision";
		return selection;
	}
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo physical pipeline cost admits region graph analysis";
		return selection;
	}
	selection.reason = "duckdb_cbo selects vectorized physical runner before region graph";
	selection.reason += ";region_graph=skipped";
	selection.blocker = "duckdb_selected_vectorized";
	return selection;
}

static string DescribeExecutionRegionLoweringRejection(const ExecutionRegionGraph &graph) {
	string reason = "core region lowering did not produce typed region IR";
	reason += ";graph_shape=" + DescribeExecutionRegionGraphShape(graph);
	return reason;
}

static ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionCostOnlyPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                            const ExecutionRegionCandidate &candidate) {
	ExecutionRegionPhysicalRunnerSelection selection;
	auto cost_input = BuildPhysicalRunnerCostInput(candidate);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo cost model admits backend capability analysis";
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner before backend analysis";
	selection.reason += ";backend_analysis=skipped";
	selection.blocker = "duckdb_selected_vectorized";
	return selection;
}

static ExecutionRegionPhysicalRunnerSelection
SelectExecutionRegionPhysicalRunner(const PhysicalRunnerCostParameters &cost_parameters,
                                    const ExecutionRegionCandidate &candidate,
                                    const ExecutionRegionLoweringPlan &lowering_plan, bool record_detailed_telemetry) {
	ExecutionRegionPhysicalRunnerSelection selection;
	if (!lowering_plan.IsFullyFused()) {
		selection.reason = "duckdb_cbo skips compiled-vectorized runner because region is not fully fused";
		selection.reason += ";requires=fused";
		selection.reason += ";" + ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
		selection.blocker = "region_not_fully_fused";
		return selection;
	}
	auto cost_input = BuildPhysicalRunnerCostInput(candidate);
	selection.runner_cost = DuckDBCostModel::SelectPhysicalRunner(cost_input, cost_parameters);
	if (selection.runner_cost.selected_accelerated_runner) {
		selection.use_compiled_runner = true;
		selection.reason = "duckdb_cbo selects compiled-vectorized physical runner";
		return selection;
	}

	selection.reason = "duckdb_cbo selects vectorized physical runner;";
	selection.reason += ExecutionRegionLoweringEventReason(lowering_plan, record_detailed_telemetry);
	selection.blocker = "duckdb_selected_vectorized";
	return selection;
}

struct ExecutionRegionPlanner::SelectedCandidate {
	idx_t candidate_index = 0;
	ExecutionRegionLoweringPlan lowering_plan;
	ExecutionRegionPhysicalRunnerSelection physical_runner;
	ExecutionRegionStageTimings stage_timings;
	int64_t decision_time_us = 0;
};

static bool ExecutionRegionStageExecutionIsFused(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::GENERATED_IR:
	case ExecutionRegionStageExecutionKind::NATIVE_CONTRACT:
		return true;
	default:
		return false;
	}
}

static bool ExecutionRegionStageExecutionIsFusionBlocker(ExecutionRegionStageExecutionKind execution) {
	switch (execution) {
	case ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY:
	case ExecutionRegionStageExecutionKind::MISSING_CONTRACT:
		return true;
	default:
		return false;
	}
}

static string DescribeExecutionRegionStageFusionBlocker(const ExecutionRegionStage &stage) {
	string result = "stage=";
	result += ExecutionRegionStageKindToString(stage.kind);
	result += ";stage_execution=";
	result += ExecutionRegionStageExecutionKindToString(stage.execution);
	result += ";stage_ownership=";
	result += ExecutionRegionOwnershipKindToString(stage.ownership);
	if (stage.node_index != DConstants::INVALID_INDEX) {
		result += ";stage_node=" + std::to_string(stage.node_index);
	}
	if (stage.operator_index != DConstants::INVALID_INDEX) {
		result += ";stage_operator=" + std::to_string(stage.operator_index);
	}
	if (!stage.operator_name.empty()) {
		result += ";stage_operator_name=" + stage.operator_name;
	}
	if (!stage.reason.empty()) {
		result += ";stage_reason=" + stage.reason;
	}
	return result;
}

static bool ExecutionRegionContractHasOnlyNativeStages(const ExecutionRegionContract &contract) {
	return contract.source_boundary_count == 0 && contract.missing_contract_count == 0;
}

struct ExecutionRegionFusedStageContractDecision {
	bool valid = true;
	string reason;
	string blocker;
};

static ExecutionRegionFusedStageContractDecision
BuildExecutionRegionFusedContractBoundaryDecision(const ExecutionRegionContract &contract) {
	ExecutionRegionFusedStageContractDecision decision;
	decision.valid = false;
	decision.blocker = "fused_region_contract_has_boundaries";
	decision.reason = "backend advertised fused region but core contract still has boundaries";
	decision.reason += ";source_boundaries=" + std::to_string(contract.source_boundary_count);
	decision.reason += ";missing_contracts=" + std::to_string(contract.missing_contract_count);
	if (!contract.ir.empty()) {
		decision.reason += ";" + contract.ir;
	}
	return decision;
}

static ExecutionRegionFusedStageContractDecision
ValidateExecutionRegionFusedStageContract(const ExecutionRegionCandidate &candidate,
                                          const ExecutionRegionLoweringPlan &lowering_plan) {
	ExecutionRegionFusedStageContractDecision decision;
	if (!lowering_plan.IsFullyFused()) {
		return decision;
	}
	if (!ExecutionRegionContractHasOnlyNativeStages(candidate.contract)) {
		return BuildExecutionRegionFusedContractBoundaryDecision(candidate.contract);
	}
	if (!candidate.stage_plan.HasStages()) {
		decision.valid = false;
		decision.blocker = "fused_region_stage_plan_empty";
		decision.reason = "backend advertised fused region but core operator-stage plan is empty";
		return decision;
	}
	bool has_executable_stage = false;
	for (auto &stage : candidate.stage_plan.stages) {
		if (ExecutionRegionStageExecutionIsFusionBlocker(stage.execution)) {
			decision.valid = false;
			decision.blocker = stage.execution == ExecutionRegionStageExecutionKind::SOURCE_BOUNDARY
			                       ? "fused_region_source_boundary"
			                       : "fused_region_missing_contract";
			decision.reason = "backend advertised fused region across core boundary stage;";
			decision.reason += DescribeExecutionRegionStageFusionBlocker(stage);
			return decision;
		}
		has_executable_stage = has_executable_stage || ExecutionRegionStageExecutionIsFused(stage.execution);
	}
	if (!has_executable_stage) {
		decision.valid = false;
		decision.blocker = "fused_region_no_executable_stage";
		decision.reason =
		    "backend advertised fused region but core operator-stage plan has no generated or native stage";
		return decision;
	}
	return decision;
}

static bool ExecutionRegionStageNeedsOperatorReadiness(const ExecutionRegionStage &stage) {
	if (stage.execution != ExecutionRegionStageExecutionKind::NATIVE_CONTRACT) {
		return false;
	}
	switch (stage.kind) {
	case ExecutionRegionStageKind::HASH_JOIN_PROBE:
	case ExecutionRegionStageKind::NESTED_LOOP_JOIN_PROBE:
	case ExecutionRegionStageKind::OPERATOR_BOUNDARY:
		return true;
	default:
		return false;
	}
}

static const char *ExecutionOperatorReadinessStatusToString(ExecutionOperatorReadinessStatus status) {
	switch (status) {
	case ExecutionOperatorReadinessStatus::READY:
		return "ready";
	case ExecutionOperatorReadinessStatus::NOT_READY:
		return "not_ready";
	case ExecutionOperatorReadinessStatus::INVALID:
		return "invalid";
	default:
		return "unknown";
	}
}

struct ExecutionRegionOperatorReadinessDecision {
	bool ready = true;
	ExecutionRegionCompileStatus status = ExecutionRegionCompileStatus::SKIPPED;
	string reason;
	string blocker;
};

static ExecutionRegionOperatorReadinessDecision
ValidateExecutionRegionNativeOperatorReadiness(ClientContext &context, Pipeline &pipeline,
                                               const ExecutionRegionIR &region_ir,
                                               const ExecutionRegionCandidate &candidate) {
	ExecutionRegionOperatorReadinessDecision decision;
	auto &operators = pipeline.GetIntermediateOperators();
	for (auto &stage : candidate.stage_plan.stages) {
		if (!ExecutionRegionStageNeedsOperatorReadiness(stage)) {
			continue;
		}
		if (stage.node_index >= region_ir.nodes.size()) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage points outside region IR";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_invalid_stage";
			return decision;
		}
		auto &node = region_ir.nodes[stage.node_index];
		if (!node.operator_info) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage has no operator contract";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_missing_contract";
			return decision;
		}
		if (stage.operator_index >= operators.size()) {
			decision.ready = false;
			decision.status = ExecutionRegionCompileStatus::UNSUPPORTED;
			decision.reason = "native operator readiness stage points outside pipeline operators";
			decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
			decision.blocker = "native_operator_readiness_invalid_operator";
			return decision;
		}
		auto &op = operators[stage.operator_index].get();
		auto readiness = op.GetExecutionOperatorReadiness(context, *node.operator_info);
		if (readiness.Ready()) {
			continue;
		}
		decision.ready = false;
		decision.status = readiness.status == ExecutionOperatorReadinessStatus::NOT_READY
		                      ? ExecutionRegionCompileStatus::SKIPPED
		                      : ExecutionRegionCompileStatus::UNSUPPORTED;
		decision.reason = "native operator state is not executable during execution-region inspection";
		decision.reason += ";readiness_status=";
		decision.reason += ExecutionOperatorReadinessStatusToString(readiness.status);
		decision.reason += ";readiness_kind=";
		decision.reason += ExecutionRegionOperatorContractKindToString(readiness.kind);
		decision.reason += ";readiness_blocker=";
		decision.reason += readiness.blocker.empty() ? "unknown" : readiness.blocker;
		decision.reason += ";" + DescribeExecutionRegionStageFusionBlocker(stage);
		decision.blocker = readiness.blocker.empty() ? (decision.status == ExecutionRegionCompileStatus::SKIPPED
		                                                    ? "native_operator_state_not_ready"
		                                                    : "native_operator_state_invalid")
		                                             : readiness.blocker;
		return decision;
	}
	return decision;
}

static bool ExecutionRegionRuntimeCanEnter(Pipeline &pipeline, string &reason) {
	if (!pipeline.GetSource() || !pipeline.GetSink()) {
		reason = "full pipeline runtime requires both source and sink";
		return false;
	}
	return true;
}

static void AccumulateExecutionRegionOpenRequest(ExecutionRegionPlan &plan, const ExecutionRegionIR &region_ir,
                                                 const ExecutionRegionCandidate &candidate,
                                                 const ExecutionRegionLoweringPlan &lowering_plan) {
	if (!ExecutionRegionABIOwnsSource(candidate.contract.abi)) {
		return;
	}
	for (idx_t node_idx = candidate.first_node; node_idx < candidate.EndNode(); node_idx++) {
		auto &node = region_ir.nodes[node_idx];
		if (!node.source) {
			continue;
		}
		auto &source = *node.source;
		auto selected_source_execution =
		    lowering_plan.SelectedSourceExecution() != ExecutionRegionSourceExecutionKind::NONE
		        ? lowering_plan.SelectedSourceExecution()
		        : source.execution;
		auto native_fused_source_owner =
		    ExecutionRegionExecutionModeIsCompiled(lowering_plan.ExpectedCompiledExecutionMode()) &&
		    lowering_plan.IsFullyFused();
		if (source.kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			auto &contract = plan.source_open_request;
			contract.present = true;
			contract.source_execution =
			    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
			            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
			            native_fused_source_owner
			        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
			        : ExecutionRegionSourceExecutionKind::NONE;
			contract.uses_scan_filters = false;
			return;
		}
		if (!source.table_scan_contract.present) {
			continue;
		}
		auto &table_scan_contract = source.table_scan_contract;
		auto &plan_contract = plan.source_open_request;
		plan_contract.present = true;
		plan_contract.source_execution =
		    selected_source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
		            source.source_contract.status == ExecutionRegionSourceContractStatus::READY &&
		            native_fused_source_owner
		        ? ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT
		        : ExecutionRegionSourceExecutionKind::NONE;
		plan_contract.uses_scan_filters =
		    !source.filters.empty() && native_fused_source_owner && lowering_plan.UsesScanFilters();
		return;
	}
}

static unique_ptr<ExecutionRegionPlan> KeepExecutableExecutionRegionPlan(unique_ptr<ExecutionRegionPlan> plan) {
	if (!plan) {
		return nullptr;
	}
	if (plan->HasExecutableRegions() || plan->RequiresOperatorReadinessRefresh()) {
		return plan;
	}
	return nullptr;
}

unique_ptr<ExecutionRegionPlan> ExecutionRegionPlanner::Build(ClientContext &context, Pipeline &pipeline) {
	auto plan = make_uniq<ExecutionRegionPlan>();
	if (context.IsCompiledExecutionSuppressed() || pipeline.executor.IsCompiledExecutionSuppressed() ||
	    !ExecutionRegionSettings::Enabled(context)) {
		return nullptr;
	}
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	auto policy = ExecutionRegionSettings::Policy(context);
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	auto should_record_decision_telemetry = ExecutionRegionSettings::ShouldRecordDecisionTelemetry(context);
	auto needs_backend_diagnostics = ExecutionRegionPlanningNeedsBackendDiagnostics(context);
	auto needs_candidate_diagnostics = ExecutionRegionPlanningNeedsCandidateDiagnostics(context);
	string backend_name;
	optional_ptr<ExecutionRegionBackend> backend;
	auto select_backend = [&]() -> bool {
		if (backend) {
			return true;
		}
		backend = execution_region_manager.SelectBackend(context, backend_name);
		plan->backend_name = backend_name;
		if (!backend) {
			if (should_record_decision_telemetry) {
				execution_region_manager.RecordEvent(
				    context, std::move(backend_name), ExecutionRegionCompileStatus::UNAVAILABLE,
				    ExecutionRegionExecutionMode::NONE, "no available execution region backend", "backend_unavailable",
				    nullptr, 0, 0, 0);
			}
			return false;
		}
		if (!backend->SupportsRegions()) {
			if (should_record_decision_telemetry) {
				execution_region_manager.RecordEvent(context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				                                     ExecutionRegionExecutionMode::UNSUPPORTED,
				                                     "backend does not compile regions",
				                                     "backend_does_not_compile_regions", nullptr, 0, 0, 0);
			}
			return false;
		}
		return true;
	};
	if (policy == ExecutionRegionPolicyMode::OFF) {
		if (!should_record_decision_telemetry) {
			return nullptr;
		}
		execution_region_manager.RecordEvent(context, "policy", ExecutionRegionCompileStatus::DISABLED,
		                                     ExecutionRegionExecutionMode::NONE, "execution_region_policy=off",
		                                     "policy_off", nullptr, 0, 0, 0);
		return nullptr;
	}
	auto cost_parameters = BuildPhysicalRunnerCostParameters(context);
	if (!ExecutionRegionProductionEligibilityAllowsPlanning(context, cost_parameters)) {
		return nullptr;
	}
	ExecutionRegionStageTimings shared_stage_timings;
	if (!needs_candidate_diagnostics) {
		auto pipeline_decision_start = std::chrono::steady_clock::now();
		auto physical_runner = SelectExecutionRegionPipelinePhysicalRunner(cost_parameters, pipeline);
		auto pipeline_cbo_time_us = ExecutionRegionPlannerElapsedMicros(pipeline_decision_start);
		shared_stage_timings.pipeline_cbo_time_us = pipeline_cbo_time_us;
		if (!physical_runner.UsesCompiledRunner()) {
			if (should_record_decision_telemetry) {
				if (!select_backend()) {
					return nullptr;
				}
				auto decision_time_us = ExecutionRegionPlannerElapsedMicros(pipeline_decision_start);
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::SKIPPED,
				    ExecutionRegionExecutionMode::UNSUPPORTED, physical_runner.reason, physical_runner.blocker, nullptr,
				    decision_time_us, 0, 0, nullptr, physical_runner.SelectedRunner(), &shared_stage_timings,
				    ExecutionRegionSourceExecutionKind::NONE, false, &physical_runner.runner_cost);
			}
			return nullptr;
		}
	}
	if (!select_backend()) {
		return nullptr;
	}
	ExecutionExpressionAnalysisCache expression_analysis_cache;
	auto region_ir_mode =
	    ExecutionRegionSettings::DumpIR(context) ? ExecutionRegionIRMode::TRACE : ExecutionRegionIRMode::COMPACT;
	auto graph_build_start = std::chrono::steady_clock::now();
	auto pipeline_descriptor = BuildExecutionRegionGraph(pipeline, region_ir_mode == ExecutionRegionIRMode::TRACE);
	auto graph_build_time_us = ExecutionRegionPlannerElapsedMicros(graph_build_start);
	shared_stage_timings.graph_build_time_us = graph_build_time_us;
	if (!pipeline_descriptor) {
		if (should_record_decision_telemetry) {
			execution_region_manager.RecordEvent(context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
			                                     ExecutionRegionExecutionMode::UNSUPPORTED,
			                                     "core region graph builder produced no execution-region graph",
			                                     "no_execution_region_graph", nullptr,
			                                     shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us, 0, 0,
			                                     nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings);
		}
		return nullptr;
	}
	if (!needs_candidate_diagnostics &&
	    !ExecutionRegionGraphMayHaveCostedAcceleration(*pipeline_descriptor, cost_parameters)) {
		if (should_record_decision_telemetry) {
			string reason = "duckdb_cbo skips region lowering because pipeline has no costed acceleration";
			reason += ";region_lowering=skipped";
			execution_region_manager.RecordEvent(context, backend_name, ExecutionRegionCompileStatus::SKIPPED,
			                                     ExecutionRegionExecutionMode::UNSUPPORTED, std::move(reason),
			                                     "duckdb_selected_vectorized", nullptr,
			                                     shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us, 0, 0,
			                                     nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings);
		}
		return nullptr;
	}
	auto region_decision_start = std::chrono::steady_clock::now();
	auto region_ir = TryLowerExecutionRegion(*pipeline_descriptor, region_ir_mode, &expression_analysis_cache);
	auto region_lowering_time_us = ExecutionRegionPlannerElapsedMicros(region_decision_start);
	shared_stage_timings.ir_lowering_time_us = region_lowering_time_us;
	if (!region_ir) {
		if (should_record_decision_telemetry) {
			auto rejected_reason = DescribeExecutionRegionLoweringRejection(*pipeline_descriptor);
			execution_region_manager.RecordEvent(
			    context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, std::move(rejected_reason), "no_typed_region_ir", nullptr,
			    shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us + region_lowering_time_us, 0, 0,
			    nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings,
			    ExecutionRegionSourceExecutionKind::NONE, false);
		}
		return nullptr;
	}
	auto &lowered_region = *region_ir;
	if (lowered_region.candidates.empty()) {
		if (should_record_decision_telemetry) {
			string reason = "core region lowering produced no candidates";
			if (!lowered_region.candidate_blockers.empty()) {
				reason = "core-region-candidate-blocked:" +
				         FirstExecutionRegionReasonToken(lowered_region.candidate_blockers[0]) + ";" + reason + ";" +
				         lowered_region.candidate_blockers[0];
			}
			execution_region_manager.RecordEvent(
			    context, std::move(backend_name), ExecutionRegionCompileStatus::UNSUPPORTED,
			    ExecutionRegionExecutionMode::UNSUPPORTED, reason, ExecutionRegionCandidateBlockerCode(lowered_region),
			    &lowered_region.ir,
			    shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us + region_lowering_time_us, 0, 0,
			    nullptr, ExecutionRunnerKind::VECTORIZED, &shared_stage_timings);
		}
		return nullptr;
	}

	vector<SelectedCandidate> selected_regions;
	bool shared_decision_time_recorded = false;
	for (idx_t candidate_index = 0; candidate_index < lowered_region.candidates.size(); candidate_index++) {
		auto &candidate = lowered_region.candidates[candidate_index];
		auto candidate_decision_start = std::chrono::steady_clock::now();
		ExecutionRegionStageTimings stage_timings;
		ExecutionRegionPhysicalRunnerSelection cost_only_physical_runner;
		bool has_cost_only_physical_runner = false;
		auto candidate_decision_time_us = [&]() -> int64_t {
			auto decision_time_us = ExecutionRegionPlannerElapsedMicros(candidate_decision_start);
			if (!shared_decision_time_recorded) {
				decision_time_us +=
				    shared_stage_timings.pipeline_cbo_time_us + graph_build_time_us + region_lowering_time_us;
				stage_timings.pipeline_cbo_time_us = shared_stage_timings.pipeline_cbo_time_us;
				stage_timings.graph_build_time_us = graph_build_time_us;
				stage_timings.ir_lowering_time_us = region_lowering_time_us;
				shared_decision_time_recorded = true;
			}
			return decision_time_us;
		};
		if (ExecutionRegionABIIsFullPipeline(candidate.contract.abi)) {
			string full_pipeline_entry_reason;
			if (!ExecutionRegionRuntimeCanEnter(pipeline, full_pipeline_entry_reason)) {
				if (should_record_decision_telemetry) {
					auto decision_time_us = candidate_decision_time_us();
					execution_region_manager.RecordEvent(
					    context, backend_name, ExecutionRegionCompileStatus::SKIPPED,
					    ExecutionRegionExecutionMode::UNSUPPORTED,
					    AttachExecutionRegionCandidateReason(candidate, std::move(full_pipeline_entry_reason),
					                                         should_record_detailed_telemetry),
					    "full_pipeline_runtime_missing_source_or_sink", &lowered_region.ir, decision_time_us, 0, 0,
					    &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings);
				}
				continue;
			}
		}
		if (!needs_backend_diagnostics) {
			auto candidate_cbo_start = std::chrono::steady_clock::now();
			auto physical_runner = SelectExecutionRegionCostOnlyPhysicalRunner(cost_parameters, candidate);
			stage_timings.candidate_cbo_time_us += ExecutionRegionPlannerElapsedMicros(candidate_cbo_start);
			if (!physical_runner.UsesCompiledRunner()) {
				if (should_record_decision_telemetry) {
					auto decision_time_us = candidate_decision_time_us();
					execution_region_manager.RecordEvent(
					    context, backend_name, ExecutionRegionCompileStatus::SKIPPED,
					    ExecutionRegionExecutionMode::UNSUPPORTED,
					    AttachExecutionRegionCandidateReason(candidate, physical_runner.reason,
					                                         should_record_detailed_telemetry),
					    physical_runner.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
					    physical_runner.SelectedRunner(), &stage_timings, ExecutionRegionSourceExecutionKind::NONE,
					    false, &physical_runner.runner_cost);
				}
				continue;
			}
			cost_only_physical_runner = std::move(physical_runner);
			has_cost_only_physical_runner = true;
		}
		if (!needs_backend_diagnostics && !ExecutionRegionContractHasOnlyNativeStages(candidate.contract)) {
			if (should_record_decision_telemetry) {
				auto contract_decision = BuildExecutionRegionFusedContractBoundaryDecision(candidate.contract);
				auto decision_time_us = candidate_decision_time_us();
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				    ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(contract_decision.reason),
				                                         should_record_detailed_telemetry),
				    contract_decision.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
				    ExecutionRunnerKind::VECTORIZED, &stage_timings);
			}
			continue;
		}
		ExecutionRegionCompilationInput input(context, lowered_region, candidate);
		auto analysis_start = std::chrono::steady_clock::now();
		auto lowering_plan = backend->AnalyzeRegion(input);
		stage_timings.backend_analysis_time_us = ExecutionRegionPlannerElapsedMicros(analysis_start);
		input.lowering_plan = &lowering_plan;
		if (!lowering_plan.HasNodes()) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = candidate_decision_time_us();
				string empty_analysis_reason = "backend produced an empty execution-region capability analysis";
				auto lowering_reason =
				    ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
				if (!lowering_reason.empty()) {
					empty_analysis_reason += ";";
					empty_analysis_reason += lowering_reason;
				}
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				    ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(empty_analysis_reason),
				                                         should_record_detailed_telemetry),
				    "backend_empty_analysis", &lowered_region.ir, decision_time_us, 0, 0, &candidate,
				    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.SelectedSourceExecution(),
				    lowering_plan.UsesScanFilters());
			}
			continue;
		}
		if (lowering_plan.ExpectedCompiledExecutionMode() == ExecutionRegionExecutionMode::UNSUPPORTED) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = candidate_decision_time_us();
				auto unsupported_reason =
				    ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry);
				if (lowering_plan.NativeCount() > 0) {
					unsupported_reason +=
					    ";execution:unsupported;backend cannot generate executable code for this whole region";
				} else {
					unsupported_reason = "region lowering contains no native executable nodes: " + unsupported_reason +
					                     ";execution:unsupported";
				}
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				    ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(unsupported_reason),
				                                         should_record_detailed_telemetry),
				    ExecutionRegionUnsupportedBlockerCode(lowering_plan), &lowered_region.ir, decision_time_us, 0, 0,
				    &candidate, ExecutionRunnerKind::VECTORIZED, &stage_timings,
				    lowering_plan.SelectedSourceExecution(), lowering_plan.UsesScanFilters());
			}
			continue;
		}
		auto fused_contract_decision = ValidateExecutionRegionFusedStageContract(candidate, lowering_plan);
		if (!fused_contract_decision.valid) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = candidate_decision_time_us();
				auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) +
				              ";execution:unsupported;" + std::move(fused_contract_decision.reason);
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::UNSUPPORTED,
				    ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(reason),
				                                         should_record_detailed_telemetry),
				    fused_contract_decision.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
				    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.SelectedSourceExecution(),
				    lowering_plan.UsesScanFilters());
			}
			continue;
		}
		auto readiness_decision =
		    ValidateExecutionRegionNativeOperatorReadiness(context, pipeline, lowered_region, candidate);
		if (!readiness_decision.ready) {
			if (readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED) {
				plan->operator_readiness_refresh = true;
			}
			if (should_record_decision_telemetry) {
				auto decision_time_us = candidate_decision_time_us();
				auto reason = ExecutionRegionLoweringEventReason(lowering_plan, should_record_detailed_telemetry) + ";";
				reason += readiness_decision.status == ExecutionRegionCompileStatus::SKIPPED
				              ? "execution:state-not-ready;"
				              : "execution:unsupported;";
				reason += std::move(readiness_decision.reason);
				execution_region_manager.RecordEvent(
				    context, backend_name, readiness_decision.status, ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, std::move(reason),
				                                         should_record_detailed_telemetry),
				    readiness_decision.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
				    ExecutionRunnerKind::VECTORIZED, &stage_timings, lowering_plan.SelectedSourceExecution(),
				    lowering_plan.UsesScanFilters());
			}
			continue;
		}
		ExecutionRegionPhysicalRunnerSelection physical_runner;
		if (has_cost_only_physical_runner && lowering_plan.IsFullyFused()) {
			physical_runner = std::move(cost_only_physical_runner);
			physical_runner.reason = "duckdb_cbo selects compiled-vectorized physical runner";
		} else {
			auto candidate_cbo_start = std::chrono::steady_clock::now();
			physical_runner = SelectExecutionRegionPhysicalRunner(cost_parameters, candidate, lowering_plan,
			                                                      should_record_detailed_telemetry);
			stage_timings.candidate_cbo_time_us += ExecutionRegionPlannerElapsedMicros(candidate_cbo_start);
		}
		if (!physical_runner.UsesCompiledRunner()) {
			if (should_record_decision_telemetry) {
				auto decision_time_us = candidate_decision_time_us();
				execution_region_manager.RecordEvent(
				    context, backend_name, ExecutionRegionCompileStatus::SKIPPED,
				    ExecutionRegionExecutionMode::UNSUPPORTED,
				    AttachExecutionRegionCandidateReason(candidate, physical_runner.reason,
				                                         should_record_detailed_telemetry),
				    physical_runner.blocker, &lowered_region.ir, decision_time_us, 0, 0, &candidate,
				    physical_runner.SelectedRunner(), &stage_timings, lowering_plan.SelectedSourceExecution(),
				    lowering_plan.UsesScanFilters(), &physical_runner.runner_cost);
			}
			continue;
		}

		SelectedCandidate selected_region;
		selected_region.candidate_index = candidate_index;
		selected_region.lowering_plan = std::move(lowering_plan);
		selected_region.physical_runner = std::move(physical_runner);
		selected_region.decision_time_us = candidate_decision_time_us();
		selected_region.stage_timings = stage_timings;
		AccumulateExecutionRegionOpenRequest(*plan, lowered_region, candidate, selected_region.lowering_plan);
		selected_regions.push_back(std::move(selected_region));
	}
	Compile(context, *backend, backend_name, *plan, lowered_region, selected_regions);
	if (plan->HasExecutableFullPipeline() && !ExecutionRegionSettings::TraceVectorizedBaseline(context)) {
		plan->SelectRunner(ExecutionRunnerKind::COMPILED_VECTORIZED);
	}
	return KeepExecutableExecutionRegionPlan(std::move(plan));
}

void ExecutionRegionPlanner::Compile(ClientContext &context, ExecutionRegionBackend &backend,
                                     const string &backend_name, ExecutionRegionPlan &plan,
                                     ExecutionRegionIR &region_ir,
                                     vector<ExecutionRegionPlanner::SelectedCandidate> &selected_regions) {
	plan.kernels.clear();
	if (context.IsCompiledExecutionSuppressed() || !ExecutionRegionSettings::Enabled(context) ||
	    selected_regions.empty()) {
		return;
	}
	auto &execution_region_manager = ExecutionRegionManager::Get(context);
	auto should_record_detailed_telemetry = ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);
	for (auto &compiled_region : selected_regions) {
		auto &candidate = region_ir.candidates[compiled_region.candidate_index];
		auto stage_timings = compiled_region.stage_timings;
		ExecutionRegionCompilationInput input(context, region_ir, candidate);
		input.lowering_plan = &compiled_region.lowering_plan;
		auto start = std::chrono::steady_clock::now();
		auto result = backend.CompileRegion(input);
		auto compile_time_us = ExecutionRegionPlannerElapsedMicros(start);
		stage_timings.codegen_time_us = compile_time_us;
		stage_timings.executable_build_time_us = result.timings.executable_build_time_us;
		stage_timings.machine_codegen_time_us = result.timings.machine_codegen_time_us;
		stage_timings.kernel_build_time_us = result.timings.kernel_build_time_us;

		idx_t code_size = result.kernel ? result.kernel->CodeSize() : 0;
		auto status = result.status;
		auto reason = AttachExecutionRegionCandidateReason(
		    candidate, ComposeExecutionRegionCompileEventReason(compiled_region.physical_runner, result.reason),
		    should_record_detailed_telemetry);
		auto execution_mode = result.execution_mode;
		if (status != ExecutionRegionCompileStatus::COMPILED && result.kernel) {
			throw InternalException("execution region backend \"%s\" returned kernel for non-compiled region status %s",
			                        backend_name, ExecutionRegionCompileStatusToString(status));
		}
		if (status == ExecutionRegionCompileStatus::COMPILED) {
			auto expected_mode = compiled_region.lowering_plan.ExpectedCompiledExecutionMode();
			if (expected_mode == ExecutionRegionExecutionMode::UNSUPPORTED) {
				throw InternalException(
				    "execution region backend \"%s\" compiled region without native executable nodes", backend_name);
			}
			if (!compiled_region.lowering_plan.IsFullyFused()) {
				throw InternalException("execution region backend \"%s\" compiled a region that is not fully fused",
				                        backend_name);
			}
			if (execution_mode != expected_mode) {
				throw InternalException(
				    "execution region backend \"%s\" compiled region with execution mode %s after advertising %s",
				    backend_name, ExecutionRegionExecutionModeToString(execution_mode),
				    ExecutionRegionExecutionModeToString(expected_mode));
			}
			if (!result.kernel || !result.kernel->HasExecutableBody()) {
				throw InternalException("execution region backend \"%s\" compiled region without executable code",
				                        backend_name);
			}
			if (ExecutionRegionABIIsFullPipeline(candidate.contract.abi) && !result.kernel->CanExecuteFullPipeline()) {
				throw InternalException(
				    "execution region backend \"%s\" compiled full pipeline without full-pipeline executable ABI",
				    backend_name);
			}
		}
		auto trace_id = execution_region_manager.RecordEvent(
		    context, backend_name, status, execution_mode, reason, ExecutionRegionCompileResultBlockerCode(status),
		    &result.ir, compiled_region.decision_time_us, compile_time_us, code_size, &candidate,
		    compiled_region.physical_runner.SelectedRunner(), &stage_timings,
		    compiled_region.lowering_plan.SelectedSourceExecution(), compiled_region.lowering_plan.UsesScanFilters(),
		    &compiled_region.physical_runner.runner_cost);
		if (status == ExecutionRegionCompileStatus::COMPILED && result.kernel) {
			result.kernel->SetTraceInfo(trace_id, execution_mode, reason, compile_time_us, code_size);
			result.kernel->SetExecutionABI(candidate.contract.abi);
			result.kernel->SetTraceSelectedSourceExecution(compiled_region.lowering_plan.SelectedSourceExecution());
			result.kernel->SetTraceUsesScanFilters(compiled_region.lowering_plan.UsesScanFilters());
			if (ExecutionRegionSettings::TraceRuntime(context)) {
				result.kernel->SetTracePipeline(candidate);
			}
		}
		if (status == ExecutionRegionCompileStatus::ERROR) {
			throw InvalidInputException("Execution region compilation failed: %s", reason);
		}
		if (result.kernel) {
			plan.kernels.push_back(std::move(result.kernel));
		}
	}
}

} // namespace duckdb

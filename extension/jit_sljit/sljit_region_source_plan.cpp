//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_source_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static SljitRegionNodePlan SljitUnsupportedExpressionBoundaryNode(const string &error) {
	string reason = "region IR node is unsupported by SLJIT native contract lowering";
	if (!error.empty()) {
		reason += ";";
		reason += error;
	}
	return SljitRegionBoundaryNode(std::move(reason));
}

SljitRegionNodePlan PlanSljitFilterNode(const ExecutionRegionNode &node, string &error, bool render_diagnostics) {
	if (!node.blocker_reason.empty() || !node.filter) {
		return SljitNodeBlockerBoundary(node, "filter expression unsupported by SLJIT IR lowering");
	}

	SljitNativeRegionOpPlan native_op;
	native_op.kind = SljitNativeRegionOpKind::FILTER;
	native_op.output_types = node.output_types;
	if (!TryLowerNativeRegionExpression(*node.filter, true, native_op.filter, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}

	return SljitNativeNode(std::move(native_op), "generated typed predicate filter");
}

static bool TryPlanDirectSljitProjection(const ExecutionRegionNode &node, SljitNativeRegionOpPlan &native_op,
                                         string &error, bool render_diagnostics) {
	native_op = SljitNativeRegionOpPlan();
	native_op.kind = SljitNativeRegionOpKind::PROJECTION;
	native_op.output_types = node.output_types;
	for (auto &expression : node.projections) {
		SljitNativeRegionExpressionPlan native_expression;
		if (!TryLowerNativeRegionExpression(*expression, false, native_expression, error, render_diagnostics)) {
			return false;
		}
		native_op.projections.push_back(std::move(native_expression));
	}
	return true;
}

static bool TryPlanExpandedSljitProjection(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                           vector<SljitNativeRegionOpPlan> &native_ops, string &error,
                                           bool render_diagnostics) {
	if (input_types.empty() && !node.output_types.empty()) {
		error = "projection expression graph lowering requires input types";
		return false;
	}

	SljitProjectionGraphLowering graph(input_types, render_diagnostics);
	vector<SljitNativeRegionExpressionPlan> final_projections;
	final_projections.reserve(node.projections.size());
	for (auto &fragment : node.projections) {
		if (!fragment->root) {
			error = "projection expression graph lowering requires rooted JIT IR";
			return false;
		}
		SljitNativeRegionExpressionPlan projection;
		if (!TryBuildSljitProjectionGraphExpression(*fragment->root, graph, projection)) {
			return false;
		}
		if (render_diagnostics) {
			projection.ir = fragment->ir;
		}
		final_projections.push_back(std::move(projection));
	}

	SljitNativeRegionOpPlan final_op;
	final_op.kind = SljitNativeRegionOpKind::PROJECTION;
	final_op.output_types = node.output_types;
	final_op.projections = std::move(final_projections);
	graph.native_ops.push_back(std::move(final_op));
	native_ops = std::move(graph.native_ops);
	return true;
}

SljitRegionNodePlan PlanSljitProjectionNode(const ExecutionRegionNode &node, const vector<LogicalType> &input_types,
                                            string &error, bool render_diagnostics) {
	if (!node.blocker_reason.empty() || node.projections.empty()) {
		return SljitNodeBlockerBoundary(node, "projection has no lowered JIT IR expressions");
	}

	SljitNativeRegionOpPlan native_op;
	if (TryPlanDirectSljitProjection(node, native_op, error, render_diagnostics)) {
		return SljitNativeNode(std::move(native_op), "native projection");
	}

	vector<SljitNativeRegionOpPlan> native_ops;
	if (!TryPlanExpandedSljitProjection(node, input_types, native_ops, error, render_diagnostics)) {
		return SljitUnsupportedExpressionBoundaryNode(error);
	}
	return SljitNativeNode(std::move(native_ops), "generated typed projection expression graph");
}

static void
AppendSljitSourceIR(string &reason, const ExecutionRegionNode &node, bool render_diagnostics,
                    ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE) {
	if (!render_diagnostics || !node.source) {
		return;
	}
	reason += ";";
	reason += DescribeExecutionRegionSourceInfo(*node.source, execution);
}

static SljitRegionNodePlan SljitNativeSourceNode(string reason, const ExecutionRegionNode &node, bool render_diagnostics,
                                                 bool uses_scan_filters = false) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.source_execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	result.uses_scan_filters = uses_scan_filters;
	result.reason = std::move(reason);
	AppendSljitSourceIR(result.reason, node, render_diagnostics, ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	return result;
}

string SljitSourceBoundaryReason(const ExecutionRegionNode &node, bool render_diagnostics) {
	string result =
	    node.blocker_reason.empty() ? "source node is outside SLJIT native region lowering" : node.blocker_reason;
	AppendSljitSourceIR(result, node, render_diagnostics);
	return result;
}

static void AppendSljitSourceFilterFacts(string &reason, const ExecutionRegionNode &node,
                                         const ExecutionRegionTableScanContract &contract, bool include_input_columns) {
	D_ASSERT(node.source);
	reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
	if (include_input_columns) {
		reason += ";source_contract_input_columns=" + std::to_string(contract.source_contract_input_column_count);
	}
	reason += ";source_contract_filter_prune_required=" +
	          string(contract.source_contract_filter_prune_required ? "true" : "false");
}

static SljitRegionNodePlan SljitSourceBoundaryRequiresContract(const ExecutionRegionNode &node,
                                                               const ExecutionRegionTableScanContract &contract,
                                                               bool include_strategy, bool render_diagnostics) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::BOUNDARY;
	result.requires_source_contract = true;
	result.reason = "DuckDB source boundary;source-contract-blocker:requires-source-contract;"
	                "source_execution=duckdb-source-boundary";
	if (include_strategy) {
		result.reason += ";source-strategy=duckdb-source-boundary";
	}
	AppendSljitSourceFilterFacts(result.reason, node, contract, true);
	AppendSljitSourceIR(result.reason, node, render_diagnostics,
	                    ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	return result;
}

static bool TryBuildSljitSourceContractOutputProjection(const ExecutionRegionNode &node,
                                                        const ExecutionRegionTableScanContract &contract,
                                                        SljitNativeRegionOpPlan &projection, string &error,
                                                        bool render_diagnostics) {
	auto &input_types = contract.source_contract_input_types;
	auto &projection_map = contract.source_contract_output_projection_map;
	if (projection_map.size() != node.output_types.size()) {
		error = "source contract projection map does not match source output column count";
		return false;
	}
	projection = SljitNativeRegionOpPlan();
	projection.kind = SljitNativeRegionOpKind::PROJECTION;
	projection.output_types.reserve(projection_map.size());
	projection.projections.reserve(projection_map.size());
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= input_types.size()) {
			error = "source contract projection references column outside source input";
			return false;
		}
		if (input_types[input_idx] != node.output_types[output_idx]) {
			error = "source contract projection type does not match source output type";
			return false;
		}
		auto ir = render_diagnostics ? "source-contract-output-reference" : string();
		projection.projections.push_back(
		    SljitNativeReferenceExpression(input_idx, input_types[input_idx], std::move(ir), true));
		projection.output_types.push_back(input_types[input_idx]);
	}
	return true;
}

static bool TryPlanSljitSourceFilters(const ExecutionRegionNode &node, vector<SljitNativeRegionOpPlan> &native_ops,
                                      string &error, bool render_diagnostics) {
	D_ASSERT(node.source);
	auto &contract = node.source->table_scan_contract;
	if (!contract.present || contract.source_contract_input_types.empty()) {
		error = "generated source filters require source contract input types";
		return false;
	}
	if (node.source->filters.empty()) {
		error = "generated source filters require source filter expressions";
		return false;
	}

	auto &source_types = contract.source_contract_input_types;
	vector<unique_ptr<ExecutionExpressionIR>> filter_trees;
	filter_trees.reserve(node.source->filters.size());
	for (auto &source_filter : node.source->filters) {
		if (!source_filter.generated_source_stage_candidate) {
			error = "source filter is not a generated source stage candidate";
			return false;
		}
		if (!source_filter.expression || !source_filter.expression->root) {
			error = source_filter.reason.empty() ? "source filter has no lowered expression IR" : source_filter.reason;
			return false;
		}
		if (source_filter.scan_column_index >= source_types.size()) {
			error = "source filter references column outside source input";
			return false;
		}

		vector<SljitNativeRegionExpressionPlan> source_filter_projection;
		source_filter_projection.push_back(SljitNativeReferenceExpression(
		    source_filter.scan_column_index, source_types[source_filter.scan_column_index], string(), true));
		unique_ptr<ExecutionExpressionIR> tree;
		if (!TryLowerNativeRegionExpressionTreeThroughProjection(*source_filter.expression, source_filter_projection,
		                                                         tree, error, render_diagnostics)) {
			return false;
		}
		filter_trees.push_back(std::move(tree));
	}

	ExecutionExpressionFragment fused_filter;
	fused_filter.return_type = LogicalType::BOOLEAN;
	if (filter_trees.size() == 1) {
		fused_filter.root = std::move(filter_trees[0]);
	} else {
		auto conjunction = make_uniq<ExecutionExpressionIR>();
		conjunction->kind = ExecutionExpressionIRKind::CONJUNCTION;
		conjunction->return_type = LogicalType::BOOLEAN;
		conjunction->physical_type = PhysicalType::BOOL;
		conjunction->validity = ExecutionExpressionValidityKind::THREE_VALUED_BOOLEAN;
		conjunction->source = ExecutionExpressionSourceKind::DERIVED;
		conjunction->exception_behavior = ExecutionExpressionExceptionKind::NONE;
		conjunction->conjunction_op = ExecutionExpressionConjunctionOp::AND;
		conjunction->children = std::move(filter_trees);
		fused_filter.root = std::move(conjunction);
	}
	if (render_diagnostics) {
		fused_filter.ir = "fused-generated-source-filters";
	}
	SljitNativeRegionOpPlan filter_op;
	filter_op.kind = SljitNativeRegionOpKind::FILTER;
	filter_op.output_types = source_types;
	if (!TryLowerNativeRegionExpression(fused_filter, true, filter_op.filter, error, render_diagnostics)) {
		return false;
	}

	SljitNativeRegionOpPlan projection;
	if (!TryBuildSljitSourceContractOutputProjection(node, contract, projection, error, render_diagnostics)) {
		return false;
	}
	native_ops.push_back(std::move(filter_op));
	native_ops.push_back(std::move(projection));
	return true;
}

static SljitRegionNodePlan PlanSljitSourceContractNode(const ExecutionRegionNode &node,
                                                       const ExecutionRegionContract &contract,
                                                       bool render_diagnostics) {
	D_ASSERT(node.source);
	auto &table_scan_contract = node.source->table_scan_contract;
	if (!table_scan_contract.present) {
		return SljitRegionBoundaryNode("source contract requires typed table scan contract IR");
	}
	if (!ExecutionRegionABIIsFullPipeline(contract.abi)) {
		return SljitRegionBoundaryNode("source contract requires full-pipeline region ABI");
	}

	if (node.source->filters.empty()) {
		if (table_scan_contract.dynamic_filters && table_scan_contract.filter_pushdown) {
			string reason =
			    "vectorized dynamic table scan filters;source-strategy=duckdb-scan-filtered-source-contract";
			reason += ";source_contract_filter_pushdown=true";
			reason += ";source_contract_dynamic_filters=true";
			return SljitNativeSourceNode(std::move(reason), node, render_diagnostics, true);
		}
		return SljitNativeSourceNode("table scan source contract", node, render_diagnostics);
	}

	if (table_scan_contract.filter_pushdown) {
		vector<SljitNativeRegionOpPlan> native_ops;
		string error;
		if (TryPlanSljitSourceFilters(node, native_ops, error, render_diagnostics)) {
			auto result = SljitNativeSourceNode("generated table scan source filters", node, render_diagnostics);
			result.native_ops = std::move(native_ops);
			return result;
		}
		string reason = "vectorized table scan filters;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=true";
		return SljitNativeSourceNode(std::move(reason), node, render_diagnostics, true);
	}

	return SljitRegionBoundaryNode("table scan source filters require DuckDB scan filter pushdown");
}

static SljitRegionNodePlan PlanSljitNativeStateScanSourceNode(const ExecutionRegionNode &node,
                                                              const ExecutionRegionContract &contract,
                                                              bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->native_state_scan_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitRegionBoundaryNode("native state scan source requires a ready state-scan contract");
	}
	if (!ExecutionRegionABIIsFullPipeline(contract.abi)) {
		return SljitRegionBoundaryNode("native state scan source requires full-pipeline region ABI");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionBoundaryNode("native state scan source does not own source-pushed filters");
	}

	string reason = "state scan source contract";
	if (render_diagnostics) {
		reason += ";native-state-scan-contract-status=";
		reason += ExecutionRegionStateContractStatusToString(node.source->native_state_scan_contract.status);
		reason += ";native-state-scan-capability=" + node.source->native_state_scan_contract.required_capability;
		reason += ";native-state-scan-contract-version=" + node.source->native_state_scan_contract.contract_version;
	}
	return SljitNativeSourceNode(std::move(reason), node, render_diagnostics);
}

static SljitRegionNodePlan PlanSljitNativeStatefulSourceNode(const ExecutionRegionNode &node,
                                                             const ExecutionRegionContract &contract,
                                                             bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->source_contract.status != ExecutionRegionSourceContractStatus::READY) {
		return SljitRegionBoundaryNode("stateful source requires a ready source contract");
	}
	if (!ExecutionRegionABIIsFullPipeline(contract.abi)) {
		return SljitRegionBoundaryNode("native stateful source requires full-pipeline region ABI");
	}
	if (!node.source->filters.empty()) {
		return SljitRegionBoundaryNode("native stateful source does not own source-pushed filters");
	}

	string reason = "stateful source contract";
	if (render_diagnostics) {
		reason += ";source-contract-status=";
		reason += ExecutionRegionSourceContractStatusToString(node.source->source_contract.status);
		reason += ";source-contract-capability=" + node.source->source_contract.required_capability;
		reason += ";source-contract-version=" + node.source->source_contract.contract_version;
	}
	return SljitNativeSourceNode(std::move(reason), node, render_diagnostics);
}

SljitRegionNodePlan PlanSljitSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract,
                                        ExecutionRegionSourceExecutionKind source_execution, bool render_diagnostics) {
	if (!node.source) {
		return SljitRegionBoundaryNode("source boundary requires typed source IR");
	}
	auto &source_contract = node.source->source_contract;
	if (source_contract.status == ExecutionRegionSourceContractStatus::NONE ||
	    source_contract.required_capability.empty() || source_contract.contract_version.empty() ||
	    (source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED && source_contract.blocker.empty())) {
		return SljitRegionBoundaryNode("source boundary requires source contract IR");
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY &&
	    !node.source->filters.empty()) {
		if (!node.source->table_scan_contract.present) {
			return SljitRegionBoundaryNode("source-pushed filters require typed table scan contract IR");
		}
		auto &table_scan_contract = node.source->table_scan_contract;
		return SljitSourceBoundaryRequiresContract(node, table_scan_contract, true, render_diagnostics);
	}
	if (source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT &&
	    source_contract.status == ExecutionRegionSourceContractStatus::READY) {
		if (node.source->kind == ExecutionRegionSourceKind::STATEFUL_OPERATOR) {
			if (node.source->native_state_scan_contract.status == ExecutionRegionStateContractStatus::READY) {
				return PlanSljitNativeStateScanSourceNode(node, contract, render_diagnostics);
			}
			return PlanSljitNativeStatefulSourceNode(node, contract, render_diagnostics);
		}
		return PlanSljitSourceContractNode(node, contract, render_diagnostics);
	}
	if (node.operator_kind == ExecutionRegionOperatorKind::TABLE_SCAN && !node.source->table_scan_contract.present) {
		return SljitRegionBoundaryNode("table scan source boundary requires typed table scan contract IR");
	}
	if (!node.source->filters.empty()) {
		if (!node.source->table_scan_contract.present) {
			return SljitRegionBoundaryNode("source-pushed filters require typed table scan contract IR");
		}
		auto &table_scan_contract = node.source->table_scan_contract;
		auto reason = "source-pushed filters require DuckDB scan source contract ownership;source_execution=" +
		              string(ExecutionRegionSourceExecutionKindToString(node.source->execution));
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, true);
		if (!ExecutionRegionABIIsFullPipeline(contract.abi)) {
			reason += ";source_contract_abi=full_pipeline_required";
			AppendSljitSourceIR(reason, node, render_diagnostics, source_execution);
			return SljitRegionBoundaryNode(std::move(reason));
		}
		return SljitSourceBoundaryRequiresContract(node, table_scan_contract, false, render_diagnostics);
	}
	auto boundary_reason = "DuckDB source boundary;" + node.blocker_reason;
	AppendSljitSourceIR(boundary_reason, node, render_diagnostics,
	                    ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	auto result = SljitRegionBoundaryNode(std::move(boundary_reason));
	result.requires_source_contract = source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED &&
	                                  !source_contract.required_capability.empty() &&
	                                  !source_contract.contract_version.empty() && !source_contract.blocker.empty();
	return result;
}

bool SljitCanExecuteSourceNode(const ExecutionRegionNode &node, const ExecutionRegionContract &contract) {
	if (!ExecutionRegionABIIsFullPipeline(contract.abi) || !node.source) {
		return false;
	}
	if (node.source->source_contract.status == ExecutionRegionSourceContractStatus::READY ||
	    node.source->native_state_scan_contract.status == ExecutionRegionStateContractStatus::READY) {
		return true;
	}
	return node.boundary == ExecutionRegionBoundaryKind::SCAN;
}

} // namespace duckdb

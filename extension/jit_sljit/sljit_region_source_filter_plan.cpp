//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_source_filter_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

void AppendSljitSourceFilterFacts(string &reason, const ExecutionRegionNode &node,
                                  const ExecutionRegionTableScanContract &contract, bool include_input_columns) {
	D_ASSERT(node.source);
	reason += ";source_filter_count=" + std::to_string(node.source->filters.size());
	if (include_input_columns) {
		reason += ";source_contract_input_columns=" + std::to_string(contract.source_contract_input_column_count);
	}
	reason += ";source_contract_filter_prune_required=" +
	          string(contract.source_contract_filter_prune_required ? "true" : "false");
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

static bool SourceContractOutputUsesIdentityInputLayout(const ExecutionRegionTableScanContract &contract) {
	auto &projection_map = contract.source_contract_output_projection_map;
	if (projection_map.size() != contract.source_contract_input_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		if (projection_map[output_idx] != output_idx) {
			return false;
		}
	}
	return true;
}

static bool CanPreserveScanFiltersWithGeneratedSourceFilter(const ExecutionRegionNode &node,
                                                            const ExecutionRegionTableScanContract &contract) {
	D_ASSERT(node.source);
	if (node.source->filters.size() != 1) {
		return false;
	}
	return SourceContractOutputUsesIdentityInputLayout(contract);
}

static SljitSourceRoutePlan BuildGeneratedSourceFilterRoute(const ExecutionRegionNode &node,
                                                            const ExecutionRegionTableScanContract &contract) {
	SljitSourceRoutePlan route;
	route.uses_scan_filters = CanPreserveScanFiltersWithGeneratedSourceFilter(node, contract);
	route.requires_source_contract_input_layout = true;
	return route;
}

static bool TryBuildSljitSourceFilterTrees(const ExecutionRegionSourceInfo &source,
                                           const vector<LogicalType> &source_types,
                                           vector<unique_ptr<ExecutionExpressionIR>> &filter_trees, string &error,
                                           bool render_diagnostics) {
	filter_trees.clear();
	filter_trees.reserve(source.filters.size());
	for (auto &source_filter : source.filters) {
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
	return true;
}

static ExecutionExpressionFragment
BuildSljitFusedSourceFilterFragment(vector<unique_ptr<ExecutionExpressionIR>> filter_trees, bool render_diagnostics) {
	D_ASSERT(!filter_trees.empty());
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
	return fused_filter;
}

static bool TryBuildSljitSourceFilterOp(vector<unique_ptr<ExecutionExpressionIR>> filter_trees,
                                        const vector<LogicalType> &source_types, SljitNativeRegionOpPlan &filter_op,
                                        string &error, bool render_diagnostics) {
	auto fused_filter = BuildSljitFusedSourceFilterFragment(std::move(filter_trees), render_diagnostics);
	filter_op = SljitNativeRegionOpPlan();
	filter_op.kind = SljitNativeRegionOpKind::FILTER;
	filter_op.output_types = source_types;
	return TryLowerNativeRegionExpression(fused_filter, true, filter_op.filter, error, render_diagnostics);
}

bool TryPlanSljitSourceFilters(const ExecutionRegionNode &node, SljitSourceFilterPlan &plan, string &error,
                               bool render_diagnostics) {
	D_ASSERT(node.source);
	auto &contract = node.source->table_scan_contract;
	plan = SljitSourceFilterPlan();
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
	if (!TryBuildSljitSourceFilterTrees(*node.source, source_types, filter_trees, error, render_diagnostics)) {
		return false;
	}

	SljitNativeRegionOpPlan filter_op;
	if (!TryBuildSljitSourceFilterOp(std::move(filter_trees), source_types, filter_op, error, render_diagnostics)) {
		return false;
	}

	SljitNativeRegionOpPlan projection;
	if (!TryBuildSljitSourceContractOutputProjection(node, contract, projection, error, render_diagnostics)) {
		return false;
	}
	plan.native_ops.push_back(std::move(filter_op));
	plan.native_ops.push_back(std::move(projection));
	plan.source_route = BuildGeneratedSourceFilterRoute(node, contract);
	return true;
}

} // namespace duckdb

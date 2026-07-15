//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_source_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

static SljitSourceContractPlan
SljitDuckDBScanFilteredSourceContractPlan(ExecutionRegionScanFilterMode mode = ExecutionRegionScanFilterMode::ALL) {
	SljitSourceContractPlan contract_plan;
	contract_plan.scan_filter_mode = mode;
	return contract_plan;
}

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

static void
AppendSljitSourceIR(string &reason, const ExecutionRegionNode &node, bool render_diagnostics,
                    ExecutionRegionSourceExecutionKind execution = ExecutionRegionSourceExecutionKind::NONE) {
	if (!render_diagnostics || !node.source) {
		return;
	}
	reason += ";";
	reason += DescribeExecutionRegionSourceInfo(*node.source, execution);
}

static SljitRegionNodePlan SljitNativeSourceNode(string reason, const ExecutionRegionNode &node,
                                                 bool render_diagnostics, SljitSourceContractPlan contract_plan = {},
                                                 vector<SljitNativeRegionOpPlan> native_ops = {}) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::NATIVE;
	result.source_execution = ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	if (contract_plan.source_output_types.empty()) {
		contract_plan.source_output_types = node.output_types;
	}
	result.source_contract = contract_plan;
	result.native_ops = std::move(native_ops);
	result.reason = std::move(reason);
	AppendSljitSourceIR(result.reason, node, render_diagnostics, ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT);
	return result;
}

static SljitRegionNodePlan SljitStorageFilteredSourceNode(string reason, const ExecutionRegionNode &node,
                                                          bool render_diagnostics) {
	auto result =
	    SljitNativeSourceNode(std::move(reason), node, render_diagnostics, SljitDuckDBScanFilteredSourceContractPlan());
	PlanSljitStorageScanFilters(node, result.scan_filters, render_diagnostics);
	if (render_diagnostics) {
		result.reason += ";compiled_storage_filter_count=" + std::to_string(result.scan_filters.size());
	}
	return result;
}

static const char *SljitSourceKindName(const ExecutionRegionNode &node) {
	return node.source->kind == ExecutionRegionSourceKind::TABLE_FUNCTION_SCAN ? "table-function" : "table scan";
}

static string SljitSourceContractName(const ExecutionRegionNode &node) {
	return string(SljitSourceKindName(node)) + " source contract";
}

struct SljitDirectBuildStorageFilterFacts {
	bool supported = false;
	bool has_string_match = false;
	bool is_string_set = false;
};

static SljitDirectBuildStorageFilterFacts
SljitDirectBuildStorageFilterExpressionFacts(const ExecutionExpressionIR &expression) {
	if (expression.kind == ExecutionExpressionIRKind::BINARY) {
		if (expression.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL || !expression.left ||
		    !expression.right) {
			return {};
		}
		auto reference = expression.left->kind == ExecutionExpressionIRKind::REFERENCE ? expression.left.get()
		                                                                               : expression.right.get();
		auto constant = reference == expression.left.get() ? expression.right.get() : expression.left.get();
		if (reference->kind != ExecutionExpressionIRKind::REFERENCE ||
		    constant->kind != ExecutionExpressionIRKind::CONSTANT) {
			return {};
		}
		return {true, false, reference->return_type.id() == LogicalTypeId::VARCHAR};
	}
	if (expression.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (expression.not_in || expression.children.size() < 2 || !expression.children[0] ||
		    expression.children[0]->kind != ExecutionExpressionIRKind::REFERENCE) {
			return {};
		}
		for (idx_t child_idx = 1; child_idx < expression.children.size(); child_idx++) {
			if (!expression.children[child_idx] ||
			    expression.children[child_idx]->kind != ExecutionExpressionIRKind::CONSTANT) {
				return {};
			}
		}
		return {true, false, expression.children[0]->return_type.id() == LogicalTypeId::VARCHAR};
	}
	if (expression.kind == ExecutionExpressionIRKind::INTRINSIC) {
		if (expression.intrinsic != ExecutionExpressionIntrinsicKind::STRING_PREFIX &&
		    expression.intrinsic != ExecutionExpressionIntrinsicKind::STRING_SUFFIX) {
			return {};
		}
		if (expression.children.size() != 2 || !expression.children[0] || !expression.children[1] ||
		    expression.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
		    expression.children[1]->kind != ExecutionExpressionIRKind::CONSTANT) {
			return {};
		}
		return {true, true, false};
	}
	if (expression.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    expression.conjunction_op != ExecutionExpressionConjunctionOp::AND || expression.children.empty()) {
		return {};
	}
	SljitDirectBuildStorageFilterFacts result {true, false, true};
	for (auto &child : expression.children) {
		if (!child) {
			return {};
		}
		auto child_facts = SljitDirectBuildStorageFilterExpressionFacts(*child);
		if (!child_facts.supported) {
			return {};
		}
		result.has_string_match = result.has_string_match || child_facts.has_string_match;
		result.is_string_set = result.is_string_set && child_facts.is_string_set;
	}
	return result;
}

static bool SljitSourceHasOnlyDirectBuildStorageFilters(const ExecutionRegionNode &node) {
	if (!node.source || node.source->filters.empty()) {
		return false;
	}
	bool has_string_match = false;
	bool all_string_sets = true;
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root) {
			return false;
		}
		auto facts = SljitDirectBuildStorageFilterExpressionFacts(*filter.expression->root);
		if (!facts.supported) {
			return false;
		}
		has_string_match = has_string_match || facts.has_string_match;
		all_string_sets = all_string_sets && facts.is_string_set;
	}
	return has_string_match || all_string_sets;
}

static bool SljitExpressionHasStorageSensitiveStringMatch(const ExecutionExpressionIR &expression) {
	if (expression.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    (expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_CONTAINS ||
	     expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_LIKE)) {
		return true;
	}
	if (expression.left && SljitExpressionHasStorageSensitiveStringMatch(*expression.left)) {
		return true;
	}
	if (expression.right && SljitExpressionHasStorageSensitiveStringMatch(*expression.right)) {
		return true;
	}
	if (expression.else_node && SljitExpressionHasStorageSensitiveStringMatch(*expression.else_node)) {
		return true;
	}
	for (auto &child : expression.children) {
		if (child && SljitExpressionHasStorageSensitiveStringMatch(*child)) {
			return true;
		}
	}
	return false;
}

static bool SljitSourceFiltersBenefitFromStorageEvaluation(const ExecutionRegionNode &node,
                                                           const ExecutionRegionTableScanContract &contract) {
	if (contract.estimated_source_cardinality < STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (auto &filter : node.source->filters) {
		if (!filter.expression || !filter.expression->root ||
		    !SljitExpressionHasStorageSensitiveStringMatch(*filter.expression->root) ||
		    filter.scan_column_index >= contract.source_contract_input_distinct_counts.size()) {
			continue;
		}
		auto distinct_count = contract.source_contract_input_distinct_counts[filter.scan_column_index];
		if (distinct_count > 0 && distinct_count <= contract.estimated_source_cardinality / STANDARD_VECTOR_SIZE) {
			return true;
		}
	}
	return false;
}

static bool SljitSourceHasStorageSensitiveStringMatch(const ExecutionRegionNode &node) {
	for (auto &filter : node.source->filters) {
		if (filter.expression && filter.expression->root &&
		    SljitExpressionHasStorageSensitiveStringMatch(*filter.expression->root)) {
			return true;
		}
	}
	return false;
}

string SljitSourceBoundaryReason(const ExecutionRegionNode &node, bool render_diagnostics) {
	string result =
	    node.blocker_reason.empty() ? "source node is outside SLJIT native region lowering" : node.blocker_reason;
	AppendSljitSourceIR(result, node, render_diagnostics);
	return result;
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

static SljitRegionNodePlan PlanSljitSourceContractNode(const ExecutionRegionNode &node,
                                                       const ExecutionRegionContract &contract, bool render_diagnostics,
                                                       const SljitSourceStrategyContext &strategy_context) {
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
			string reason = "vectorized dynamic " + string(SljitSourceKindName(node)) +
			                " filters;source-strategy=duckdb-scan-filtered-source-contract";
			AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
			reason += ";source_contract_filter_pushdown=true";
			reason += ";source_contract_dynamic_filters=true";
			return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
		}
		return SljitNativeSourceNode(SljitSourceContractName(node), node, render_diagnostics);
	}
	if (strategy_context.candidate_uses_scan_filters && table_scan_contract.filter_pushdown &&
	    !table_scan_contract.dynamic_filters) {
		string reason =
		    "vectorized " + string(SljitSourceKindName(node)) +
		    " filters selected by execution-region CBO;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_strategy_reason=cbo-selected-storage-ownership";
		reason += ";source_contract_filter_pushdown=true";
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}

	vector<SljitNativeRegionOpPlan> generated_filter_ops;
	SljitSourceContractPlan generated_filter_contract;
	string generated_filter_error;
	const bool generated_filters_ready = TryPlanSljitGeneratedSourceFilters(
	    node, generated_filter_contract, generated_filter_ops, generated_filter_error, render_diagnostics);
	if (table_scan_contract.dynamic_filters && table_scan_contract.filter_pushdown) {
		if (generated_filters_ready) {
			const bool storage_sensitive = SljitSourceFiltersBenefitFromStorageEvaluation(node, table_scan_contract);
			const bool mixed_filter_needs_amortization =
			    !strategy_context.supports_generated_mixed_filter || !SljitSourceHasStorageSensitiveStringMatch(node);
			if (storage_sensitive || mixed_filter_needs_amortization) {
				string reason = "vectorized static and dynamic " + string(SljitSourceKindName(node)) +
				                " filters;source-strategy=duckdb-scan-filtered-source-contract";
				AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
				reason += storage_sensitive ? ";source_strategy_reason=storage-sensitive-filter"
				                            : ";source_strategy_reason=insufficient-downstream-amortization";
				reason += ";source_contract_filter_pushdown=true";
				reason += ";source_contract_dynamic_filters=true";
				return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
			}
			generated_filter_contract.scan_filter_mode = ExecutionRegionScanFilterMode::DYNAMIC_ONLY;
			string reason = "generated static " + string(SljitSourceKindName(node)) +
			                " filters with vectorized dynamic filters;source-strategy=mixed-source-filter";
			AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
			reason += ";source_contract_filter_pushdown=dynamic-only";
			reason += ";source_contract_dynamic_filters=true";
			reason += ";source_contract_input_layout=true";
			if (render_diagnostics && !generated_filter_ops.empty()) {
				reason += ";generated_source_filter=" + DescribeNativeRegionExpression(generated_filter_ops[0].filter);
			}
			return SljitNativeSourceNode(std::move(reason), node, render_diagnostics,
			                             std::move(generated_filter_contract), std::move(generated_filter_ops));
		}
		string reason = "vectorized dynamic " + string(SljitSourceKindName(node)) +
		                " filters;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=true";
		reason += ";source_contract_dynamic_filters=true";
		if (!generated_filter_error.empty()) {
			reason += ";generated-source-filter-blocker:" + generated_filter_error;
		}
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}
	if (strategy_context.prefer_direct_build_scan_filters && table_scan_contract.filter_pushdown &&
	    SljitSourceHasOnlyDirectBuildStorageFilters(node)) {
		string reason = "vectorized " + string(SljitSourceKindName(node)) +
		                " filters before direct hash build;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=true";
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}

	if (generated_filters_ready) {
		string reason =
		    "generated " + string(SljitSourceKindName(node)) + " filters;source-strategy=generated-source-filter";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=false";
		reason += ";source_contract_input_layout=true";
		if (render_diagnostics && !generated_filter_ops.empty()) {
			reason += ";generated_source_filter=" + DescribeNativeRegionExpression(generated_filter_ops[0].filter);
		}
		return SljitNativeSourceNode(std::move(reason), node, render_diagnostics, std::move(generated_filter_contract),
		                             std::move(generated_filter_ops));
	}
	if (generated_filter_error.empty()) {
		generated_filter_error = "generated source filters are unavailable";
	}

	if (table_scan_contract.filter_pushdown) {
		string reason = "vectorized " + string(SljitSourceKindName(node)) +
		                " filters;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";generated-source-filter-blocker:" + generated_filter_error;
		reason += ";source_contract_filter_pushdown=true";
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}

	return SljitRegionBoundaryNode(string(SljitSourceKindName(node)) +
	                               " source filters require DuckDB scan filter pushdown");
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
                                        ExecutionRegionSourceExecutionKind source_execution, bool render_diagnostics,
                                        const SljitSourceStrategyContext &strategy_context) {
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
		return PlanSljitSourceContractNode(node, contract, render_diagnostics, strategy_context);
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

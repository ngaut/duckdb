//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_source_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "sljit_native_predicate_simd_plan.hpp"
#include "sljit_register_layout.hpp"

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
		reason += ";source_contract_input_columns=" + std::to_string(contract.source_contract_input_types.size());
	}
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

static bool SljitExpressionHasStringSearch(const ExecutionExpressionIR &expression) {
	if (expression.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    (expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_CONTAINS ||
	     expression.intrinsic == ExecutionExpressionIntrinsicKind::STRING_LIKE)) {
		return true;
	}
	if (expression.left && SljitExpressionHasStringSearch(*expression.left)) {
		return true;
	}
	if (expression.right && SljitExpressionHasStringSearch(*expression.right)) {
		return true;
	}
	if (expression.else_node && SljitExpressionHasStringSearch(*expression.else_node)) {
		return true;
	}
	for (auto &child : expression.children) {
		if (child && SljitExpressionHasStringSearch(*child)) {
			return true;
		}
	}
	return false;
}

//! Whether the generated filter (the first generated op; a projection repair may follow it) masks rows with packed
//! SIMD before it reads anything row by row
static bool SljitGeneratedFilterHasPackedPrefix(const vector<SljitNativeRegionOpPlan> &generated_filter_ops) {
	if (generated_filter_ops.empty() || !GetSljitNativeVectorRegisterLayout().HasOptionalInvariant()) {
		return false;
	}
	auto &filter = generated_filter_ops[0].Filter();
	return filter.kind == SljitNativeRegionExpressionKind::PREDICATE && filter.predicate && filter.expression_tree &&
	       TryPlanSljitNativePredicatePartialSimd(*filter.expression_tree, filter.predicate->source_not_null).supported;
}

//! Source-local cost heuristic, not a dictionary-encoding capability or a whole-pipeline admission rule. Compressed
//! storage evaluates a filter on a low-cardinality string column once per dictionary entry and scans the other
//! filtered columns only for the rows it keeps. A generated filter reads that column row by row through the
//! dictionary selection, which only pays when a packed SIMD prefix discards rows first. So storage keeps the
//! filters when one of them searches such a column, or when one reads such a column, the storage compresses, the
//! filters keep at least half the rows (a generated filter only sees the dynamic filters' survivors), and the
//! generated filter has no packed prefix.
static bool SljitPreferStorageFilters(const ExecutionRegionNode &node, const ExecutionRegionTableScanContract &contract,
                                      const vector<SljitNativeRegionOpPlan> &generated_filter_ops) {
	if (contract.estimated_source_cardinality < STANDARD_VECTOR_SIZE) {
		return false;
	}
	const bool row_by_row_string_filter =
	    contract.compressed_storage &&
	    node.source->estimated_source_cardinality * 2 >= contract.estimated_source_cardinality &&
	    !SljitGeneratedFilterHasPackedPrefix(generated_filter_ops);
	const auto distinct_count_limit = contract.estimated_source_cardinality / STANDARD_VECTOR_SIZE;
	for (auto &filter : node.source->filters) {
		if (SljitSourceFilterIsConstantTrue(filter)) {
			continue;
		}
		const bool low_cardinality_string_column =
		    filter.scan_column_index < contract.source_contract_input_types.size() &&
		    filter.scan_column_index < contract.source_contract_input_distinct_counts.size() &&
		    contract.source_contract_input_types[filter.scan_column_index].InternalType() == PhysicalType::VARCHAR &&
		    contract.source_contract_input_distinct_counts[filter.scan_column_index] > 0 &&
		    contract.source_contract_input_distinct_counts[filter.scan_column_index] <= distinct_count_limit;
		if (!low_cardinality_string_column) {
			continue;
		}
		if (row_by_row_string_filter || (filter.expression && filter.expression->root &&
		                                 SljitExpressionHasStringSearch(*filter.expression->root))) {
			return true;
		}
	}
	return false;
}

static SljitRegionNodePlan SljitSourceBoundaryRequiresContract(const ExecutionRegionNode &node,
                                                               const ExecutionRegionTableScanContract &contract,
                                                               bool include_strategy, bool render_diagnostics) {
	SljitRegionNodePlan result;
	result.kind = ExecutionRegionLoweringKind::BOUNDARY;
	result.fusion_blocker = "source-contract-blocker:requires-source-contract;source_execution=duckdb-source-boundary";
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

static SljitRegionNodePlan PlanSljitSourceContractNode(const ExecutionRegionNode &node, bool render_diagnostics,
                                                       ExecutionRegionSinkKind sink_kind) {
	D_ASSERT(node.source);
	auto &table_scan_contract = node.source->table_scan_contract;
	if (!table_scan_contract.present) {
		return SljitRegionBoundaryNode("source contract requires typed table scan contract IR");
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
	vector<SljitNativeRegionOpPlan> generated_filter_ops;
	SljitSourceContractPlan generated_filter_contract;
	string generated_filter_error;
	const bool generated_filters_ready = TryPlanSljitGeneratedSourceFilters(
	    node, generated_filter_contract, generated_filter_ops, generated_filter_error, render_diagnostics);
	if (sink_kind == ExecutionRegionSinkKind::HASH_JOIN_BUILD && table_scan_contract.filter_pushdown &&
	    SljitSourceHasOnlyDirectBuildStorageFilters(node)) {
		string reason = "vectorized " + string(SljitSourceKindName(node)) +
		                " filters before direct hash build;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_contract_filter_pushdown=true";
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}
	if (table_scan_contract.dynamic_filters && table_scan_contract.filter_pushdown) {
		if (generated_filters_ready) {
			if (SljitPreferStorageFilters(node, table_scan_contract, generated_filter_ops)) {
				string reason = "vectorized static and dynamic " + string(SljitSourceKindName(node)) +
				                " filters;source-strategy=duckdb-scan-filtered-source-contract";
				AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
				reason += ";source_strategy_reason=storage-sensitive-filter";
				reason += ";source_contract_filter_pushdown=true";
				reason += ";source_contract_dynamic_filters=true";
				return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
			}
			generated_filter_contract.scan_filter_mode =
			    ExecutionRegionScanFilterMode::DYNAMIC_FILTERS_WITH_STATIC_PRUNING;
			string reason = "generated static " + string(SljitSourceKindName(node)) +
			                " filters with vectorized dynamic filters;source-strategy=mixed-source-filter";
			AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
			reason += ";source_contract_filter_pushdown=static-pruning-plus-dynamic-residual";
			reason += ";source_contract_dynamic_filters=true";
			reason += ";source_contract_input_layout=true";
			if (render_diagnostics && !generated_filter_ops.empty()) {
				reason +=
				    ";generated_source_filter=" + DescribeNativeRegionExpression(generated_filter_ops[0].Filter());
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
	if (generated_filters_ready && table_scan_contract.filter_pushdown &&
	    SljitPreferStorageFilters(node, table_scan_contract, generated_filter_ops)) {
		string reason = "vectorized " + string(SljitSourceKindName(node)) +
		                " filters;source-strategy=duckdb-scan-filtered-source-contract";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += ";source_strategy_reason=storage-sensitive-filter";
		reason += ";source_contract_filter_pushdown=true";
		return SljitStorageFilteredSourceNode(std::move(reason), node, render_diagnostics);
	}

	if (generated_filters_ready) {
		if (node.source->kind == ExecutionRegionSourceKind::DUCKDB_TABLE_SCAN && table_scan_contract.filter_pushdown) {
			generated_filter_contract.scan_filter_mode = ExecutionRegionScanFilterMode::STATIC_PRUNING_ONLY;
		}
		string reason =
		    "generated " + string(SljitSourceKindName(node)) + " filters;source-strategy=generated-source-filter";
		AppendSljitSourceFilterFacts(reason, node, table_scan_contract, false);
		reason += generated_filter_contract.scan_filter_mode == ExecutionRegionScanFilterMode::STATIC_PRUNING_ONLY
		              ? ";source_contract_filter_pushdown=prune-only"
		              : ";source_contract_filter_pushdown=false";
		reason += ";source_contract_input_layout=true";
		if (render_diagnostics && !generated_filter_ops.empty()) {
			reason += ";generated_source_filter=" + DescribeNativeRegionExpression(generated_filter_ops[0].Filter());
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
                                                              bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->native_state_scan_contract.status != ExecutionRegionStateContractStatus::READY) {
		return SljitRegionBoundaryNode("native state scan source requires a ready state-scan contract");
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

static SljitRegionNodePlan PlanSljitNativeStatefulSourceNode(const ExecutionRegionNode &node, bool render_diagnostics) {
	D_ASSERT(node.source);
	if (node.source->source_contract.status != ExecutionRegionSourceContractStatus::READY) {
		return SljitRegionBoundaryNode("stateful source requires a ready source contract");
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

SljitRegionNodePlan PlanSljitSourceNode(const ExecutionRegionNode &node, ExecutionRegionSinkKind sink_kind,
                                        bool render_diagnostics) {
	if (!node.source) {
		return SljitRegionBoundaryNode("source boundary requires typed source IR");
	}
	auto source_execution = node.source->execution;
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
				return PlanSljitNativeStateScanSourceNode(node, render_diagnostics);
			}
			return PlanSljitNativeStatefulSourceNode(node, render_diagnostics);
		}
		return PlanSljitSourceContractNode(node, render_diagnostics, sink_kind);
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
		return SljitSourceBoundaryRequiresContract(node, table_scan_contract, false, render_diagnostics);
	}
	auto boundary_reason = "DuckDB source boundary;" + node.blocker_reason;
	AppendSljitSourceIR(boundary_reason, node, render_diagnostics,
	                    ExecutionRegionSourceExecutionKind::DUCKDB_SOURCE_BOUNDARY);
	auto result = SljitRegionBoundaryNode(std::move(boundary_reason));
	if (source_contract.status == ExecutionRegionSourceContractStatus::BLOCKED &&
	    !source_contract.required_capability.empty() && !source_contract.contract_version.empty() &&
	    !source_contract.blocker.empty()) {
		result.fusion_blocker =
		    "source-contract-blocker:requires-source-contract;source_execution=duckdb-source-boundary";
	}
	return result;
}

} // namespace duckdb

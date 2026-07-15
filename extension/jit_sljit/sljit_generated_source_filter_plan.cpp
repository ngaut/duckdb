//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_generated_source_filter_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include <algorithm>

namespace duckdb {

static bool SljitSourceContractCanUseGeneratedInputLayout(const ExecutionRegionNode &node) {
	if (!node.source || !node.source->table_scan_contract.present) {
		return false;
	}
	auto &contract = node.source->table_scan_contract;
	auto &projection_map = contract.source_contract_output_projection_map;
	if (contract.source_contract_input_types.empty() || projection_map.size() != node.output_types.size()) {
		return false;
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= contract.source_contract_input_types.size()) {
			return false;
		}
		if (contract.source_contract_input_types[input_idx] != node.output_types[output_idx]) {
			return false;
		}
	}
	for (auto &filter : node.source->filters) {
		if (!filter.generated_source_stage_candidate ||
		    filter.scan_column_index >= contract.source_contract_input_types.size()) {
			return false;
		}
	}
	return true;
}

static bool RewriteSljitSourceFilterExpressionReferences(ExecutionExpressionIR &expression, idx_t source_index) {
	if (expression.kind == ExecutionExpressionIRKind::REFERENCE) {
		if (expression.ref_index != 0) {
			return false;
		}
		expression.ref_index = source_index;
	}
	if (expression.left && !RewriteSljitSourceFilterExpressionReferences(*expression.left, source_index)) {
		return false;
	}
	if (expression.right && !RewriteSljitSourceFilterExpressionReferences(*expression.right, source_index)) {
		return false;
	}
	if (expression.else_node && !RewriteSljitSourceFilterExpressionReferences(*expression.else_node, source_index)) {
		return false;
	}
	for (auto &child : expression.children) {
		if (child && !RewriteSljitSourceFilterExpressionReferences(*child, source_index)) {
			return false;
		}
	}
	return true;
}

static bool TryBuildSljitGeneratedSourceFilterExpression(const ExecutionRegionSourceFilter &filter,
                                                         unique_ptr<ExecutionExpressionIR> &expression,
                                                         idx_t source_index, string &error) {
	if (!filter.generated_source_stage_candidate || !filter.expression || !filter.expression->root) {
		error = "source filter has no generated expression contract";
		return false;
	}
	auto fragment = *filter.expression;
	if (!RewriteSljitSourceFilterExpressionReferences(*fragment.root, source_index)) {
		error = "source filter expression references are not local to the filtered scan column";
		return false;
	}
	expression = std::move(fragment.root);
	return true;
}

static idx_t SljitGeneratedSourceFilterTypeCost(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return 64;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return 1;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return 2;
	case PhysicalType::INT128:
		return 4;
	case PhysicalType::VARCHAR:
		return 256;
	default:
		return 64;
	}
}

static bool SljitSourceFilterValueAsDouble(const LogicalType &type, const Value &value, double &result) {
	if (value.IsNull()) {
		return false;
	}
	if (type.id() == LogicalTypeId::DATE) {
		result = static_cast<double>(value.GetValue<date_t>().days);
		return true;
	}
	Value double_value;
	string error;
	if (!value.DefaultTryCastAs(LogicalType::DOUBLE, double_value, &error) || double_value.IsNull()) {
		return false;
	}
	result = double_value.GetValue<double>();
	return true;
}

static bool SljitSourceFilterColumnRange(const ExecutionRegionTableScanContract &contract, idx_t source_index,
                                         double &min_value, double &max_value) {
	if (source_index >= contract.source_contract_input_types.size() ||
	    source_index >= contract.source_contract_input_min_values.size() ||
	    source_index >= contract.source_contract_input_max_values.size()) {
		return false;
	}
	auto &type = contract.source_contract_input_types[source_index];
	if (!SljitSourceFilterValueAsDouble(type, contract.source_contract_input_min_values[source_index], min_value) ||
	    !SljitSourceFilterValueAsDouble(type, contract.source_contract_input_max_values[source_index], max_value)) {
		return false;
	}
	return max_value > min_value;
}

static idx_t SljitSelectivityToBasisPoints(double selectivity) {
	if (selectivity <= 0) {
		return 1;
	}
	if (selectivity >= 1) {
		return 10000;
	}
	return MaxValue<idx_t>(1, static_cast<idx_t>(selectivity * 10000.0));
}

static bool SljitSourceFilterReferenceAndConstant(const ExecutionExpressionIR &expression,
                                                  const ExecutionExpressionIR *&constant, bool &constant_on_left) {
	if (!expression.left || !expression.right) {
		return false;
	}
	if (expression.left->kind == ExecutionExpressionIRKind::REFERENCE &&
	    expression.right->kind == ExecutionExpressionIRKind::CONSTANT) {
		constant = expression.right.get();
		constant_on_left = false;
		return true;
	}
	if (expression.left->kind == ExecutionExpressionIRKind::CONSTANT &&
	    expression.right->kind == ExecutionExpressionIRKind::REFERENCE) {
		constant = expression.left.get();
		constant_on_left = true;
		return true;
	}
	return false;
}

static ExecutionExpressionBinaryOp SljitReverseComparisonOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		return ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		return ExecutionExpressionBinaryOp::COMPARE_LESSTHAN;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO;
	default:
		return op;
	}
}

static idx_t SljitEstimateComparisonSelectivityBasisPoints(const ExecutionRegionTableScanContract &contract,
                                                           idx_t source_index, ExecutionExpressionBinaryOp op,
                                                           const Value &constant) {
	double min_value;
	double max_value;
	if (!SljitSourceFilterColumnRange(contract, source_index, min_value, max_value)) {
		return 10000;
	}
	double constant_value;
	auto &source_type = contract.source_contract_input_types[source_index];
	if (!SljitSourceFilterValueAsDouble(source_type, constant, constant_value)) {
		return 10000;
	}
	auto domain = max_value - min_value;
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		if (constant_value < min_value || constant_value > max_value) {
			return 1;
		}
		if (source_index < contract.source_contract_input_distinct_counts.size() &&
		    contract.source_contract_input_distinct_counts[source_index] > 0) {
			return MaxValue<idx_t>(1, 10000 / contract.source_contract_input_distinct_counts[source_index]);
		}
		return SljitSelectivityToBasisPoints(1.0 / (domain + 1.0));
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_DISTINCT_FROM:
		return 10000;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SljitSelectivityToBasisPoints((constant_value - min_value) / domain);
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitSelectivityToBasisPoints((max_value - constant_value) / domain);
	default:
		return 10000;
	}
}

static idx_t SljitEstimateSourceFilterSelectivityBasisPoints(const ExecutionExpressionIR &expression,
                                                             const ExecutionRegionTableScanContract &contract,
                                                             idx_t source_index);

static idx_t SljitEstimateConjunctionSelectivityBasisPoints(const ExecutionExpressionIR &expression,
                                                            const ExecutionRegionTableScanContract &contract,
                                                            idx_t source_index) {
	if (expression.conjunction_op != ExecutionExpressionConjunctionOp::AND) {
		return 10000;
	}
	idx_t result = 10000;
	for (auto &child : expression.children) {
		if (!child) {
			continue;
		}
		auto child_selectivity = SljitEstimateSourceFilterSelectivityBasisPoints(*child, contract, source_index);
		result = MaxValue<idx_t>(1, result * child_selectivity / 10000);
	}
	return result;
}

static idx_t SljitEstimateInListSelectivityBasisPoints(const ExecutionExpressionIR &expression,
                                                       const ExecutionRegionTableScanContract &contract,
                                                       idx_t source_index) {
	if (source_index >= contract.source_contract_input_distinct_counts.size()) {
		return 10000;
	}
	auto distinct_count = contract.source_contract_input_distinct_counts[source_index];
	if (distinct_count == 0 || expression.children.size() < 2) {
		return 10000;
	}
	idx_t constant_count = 0;
	for (idx_t child_idx = 1; child_idx < expression.children.size(); child_idx++) {
		auto &child = expression.children[child_idx];
		if (child && child->kind == ExecutionExpressionIRKind::CONSTANT && !child->constant.IsNull()) {
			constant_count++;
		}
	}
	auto match_basis_points = MinValue<idx_t>(10000, constant_count * 10000 / distinct_count);
	return expression.not_in ? 10000 - match_basis_points : MaxValue<idx_t>(1, match_basis_points);
}

static idx_t SljitEstimateSourceFilterSelectivityBasisPoints(const ExecutionExpressionIR &expression,
                                                             const ExecutionRegionTableScanContract &contract,
                                                             idx_t source_index) {
	switch (expression.kind) {
	case ExecutionExpressionIRKind::BINARY: {
		const ExecutionExpressionIR *constant = nullptr;
		bool constant_on_left = false;
		if (!SljitSourceFilterReferenceAndConstant(expression, constant, constant_on_left)) {
			return 10000;
		}
		auto op = constant_on_left ? SljitReverseComparisonOp(expression.binary_op) : expression.binary_op;
		return SljitEstimateComparisonSelectivityBasisPoints(contract, source_index, op, constant->constant);
	}
	case ExecutionExpressionIRKind::CONJUNCTION:
		return SljitEstimateConjunctionSelectivityBasisPoints(expression, contract, source_index);
	case ExecutionExpressionIRKind::IN_LIST:
		return SljitEstimateInListSelectivityBasisPoints(expression, contract, source_index);
	default:
		return 10000;
	}
}

static idx_t SljitGeneratedSourceFilterOrderCost(const ExecutionRegionSourceFilter &filter,
                                                 const ExecutionRegionTableScanContract &contract) {
	idx_t result = filter.expression ? filter.expression->traits.expression_cost : 1024;
	if (filter.scan_column_index < contract.source_contract_input_types.size()) {
		result += SljitGeneratedSourceFilterTypeCost(contract.source_contract_input_types[filter.scan_column_index]);
	}
	if (filter.expression && filter.expression->root) {
		auto selectivity = SljitEstimateSourceFilterSelectivityBasisPoints(*filter.expression->root, contract,
		                                                                   filter.scan_column_index);
		result = MaxValue<idx_t>(1, result * selectivity / 10000);
	}
	return result;
}

static vector<const ExecutionRegionSourceFilter *>
SljitOrderGeneratedSourceFilters(const ExecutionRegionNode &node, const ExecutionRegionTableScanContract &contract) {
	vector<const ExecutionRegionSourceFilter *> ordered_filters;
	ordered_filters.reserve(node.source->filters.size());
	for (auto &filter : node.source->filters) {
		ordered_filters.push_back(&filter);
	}
	std::stable_sort(ordered_filters.begin(), ordered_filters.end(),
	                 [&](const ExecutionRegionSourceFilter *left, const ExecutionRegionSourceFilter *right) {
		                 return SljitGeneratedSourceFilterOrderCost(*left, contract) <
		                        SljitGeneratedSourceFilterOrderCost(*right, contract);
	                 });
	return ordered_filters;
}

static bool TryPlanSljitGeneratedSourceFilter(const ExecutionRegionNode &node,
                                              const ExecutionRegionTableScanContract &contract,
                                              const vector<LogicalType> &source_input_types,
                                              SljitNativeRegionOpPlan &native_op, string &error,
                                              bool render_diagnostics) {
	vector<unique_ptr<ExecutionExpressionIR>> predicates;
	predicates.reserve(node.source->filters.size());
	auto ordered_filters = SljitOrderGeneratedSourceFilters(node, contract);
	for (auto filter : ordered_filters) {
		unique_ptr<ExecutionExpressionIR> predicate;
		if (!TryBuildSljitGeneratedSourceFilterExpression(*filter, predicate, filter->scan_column_index, error)) {
			return false;
		}
		predicates.push_back(std::move(predicate));
	}
	if (predicates.empty()) {
		error = "generated source filter requires at least one predicate";
		return false;
	}

	ExecutionExpressionFragment expression;
	expression.expression_index = 0;
	expression.return_type = LogicalType::BOOLEAN;
	if (predicates.size() == 1) {
		expression.root = std::move(predicates[0]);
	} else {
		auto root = make_uniq<ExecutionExpressionIR>();
		root->kind = ExecutionExpressionIRKind::CONJUNCTION;
		root->return_type = LogicalType::BOOLEAN;
		root->physical_type = PhysicalType::BOOL;
		root->validity = ExecutionExpressionValidityKind::THREE_VALUED_BOOLEAN;
		root->source = ExecutionExpressionSourceKind::DERIVED;
		root->exception_behavior = ExecutionExpressionExceptionKind::NONE;
		root->conjunction_op = ExecutionExpressionConjunctionOp::AND;
		root->children = std::move(predicates);
		expression.root = std::move(root);
	}
	if (render_diagnostics) {
		expression.ir = "duckdb.expr typed-vector-ir;" + DescribeExecutionExpressionIR(*expression.root);
	}
	native_op = SljitNativeRegionOpPlan();
	native_op.kind = SljitNativeRegionOpKind::FILTER;
	native_op.operator_index = node.operator_index;
	native_op.input_types = source_input_types;
	native_op.output_types = source_input_types;
	if (!TryLowerNativeRegionExpression(expression, true, native_op.filter, error, render_diagnostics)) {
		return false;
	}
	return true;
}

static bool SljitSourceProjectionRepairRequired(const ExecutionRegionTableScanContract &contract,
                                                const vector<LogicalType> &source_input_types,
                                                const vector<LogicalType> &node_output_types) {
	auto &projection_map = contract.source_contract_output_projection_map;
	if (projection_map.size() != node_output_types.size() || projection_map.size() != source_input_types.size()) {
		return true;
	}
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		if (projection_map[output_idx] != output_idx ||
		    source_input_types[output_idx] != node_output_types[output_idx]) {
			return true;
		}
	}
	return false;
}

static bool TryPlanSljitSourceProjectionRepair(const ExecutionRegionNode &node,
                                               const vector<LogicalType> &source_input_types,
                                               SljitNativeRegionOpPlan &native_op, string &error) {
	auto &projection_map = node.source->table_scan_contract.source_contract_output_projection_map;
	if (projection_map.size() != node.output_types.size()) {
		error = "source projection repair requires a valid source output projection map";
		return false;
	}
	native_op = SljitNativeRegionOpPlan();
	native_op.kind = SljitNativeRegionOpKind::PROJECTION;
	native_op.operator_index = node.operator_index;
	native_op.input_types = source_input_types;
	native_op.output_types = node.output_types;
	native_op.projections.reserve(projection_map.size());
	for (idx_t output_idx = 0; output_idx < projection_map.size(); output_idx++) {
		auto input_idx = projection_map[output_idx];
		if (input_idx >= source_input_types.size() || source_input_types[input_idx] != node.output_types[output_idx]) {
			error = "source projection repair map is incompatible with source input layout";
			return false;
		}
		native_op.projections.push_back(
		    SljitNativeReferenceExpression(input_idx, node.output_types[output_idx], string(), true));
	}
	return true;
}

bool TryPlanSljitGeneratedSourceFilters(const ExecutionRegionNode &node, SljitSourceContractPlan &contract_plan,
                                        vector<SljitNativeRegionOpPlan> &native_ops, string &error,
                                        bool render_diagnostics) {
	if (!node.source || node.source->filters.empty()) {
		return false;
	}
	if (!SljitSourceContractCanUseGeneratedInputLayout(node)) {
		error = "generated source filters require a valid source input layout";
		return false;
	}

	auto &table_scan_contract = node.source->table_scan_contract;
	auto source_input_types = table_scan_contract.source_contract_input_types;
	native_ops.clear();
	native_ops.reserve(2);
	SljitNativeRegionOpPlan filter_op;
	if (!TryPlanSljitGeneratedSourceFilter(node, table_scan_contract, source_input_types, filter_op, error,
	                                       render_diagnostics)) {
		return false;
	}
	native_ops.push_back(std::move(filter_op));
	if (SljitSourceProjectionRepairRequired(table_scan_contract, source_input_types, node.output_types)) {
		SljitNativeRegionOpPlan projection_op;
		if (!TryPlanSljitSourceProjectionRepair(node, source_input_types, projection_op, error)) {
			return false;
		}
		native_ops.push_back(std::move(projection_op));
	}
	contract_plan.source_contract_input_types = std::move(source_input_types);
	contract_plan.source_output_types = contract_plan.source_contract_input_types;
	return true;
}

void PlanSljitStorageScanFilters(const ExecutionRegionNode &node, vector<SljitNativeScanFilterPlan> &scan_filters,
                                 bool render_diagnostics) {
	scan_filters.clear();
	if (!node.source || !node.source->table_scan_contract.present) {
		return;
	}
	auto &contract = node.source->table_scan_contract;
	for (auto &source_filter : node.source->filters) {
		if (source_filter.scan_column_index >= contract.source_contract_input_types.size()) {
			continue;
		}
		// The exact runtime membership operation remains storage-owned and
		// dominates an additional compiled predicate on the same source input.
		bool exact_filter_owns_column = false;
		for (auto &proof : node.source->exact_filter_proofs) {
			if (proof.source_input_index == source_filter.scan_column_index) {
				exact_filter_owns_column = true;
				break;
			}
		}
		if (exact_filter_owns_column) {
			continue;
		}
		unique_ptr<ExecutionExpressionIR> predicate;
		string error;
		if (!TryBuildSljitGeneratedSourceFilterExpression(source_filter, predicate, 0, error)) {
			continue;
		}
		ExecutionExpressionFragment fragment;
		fragment.expression_index = source_filter.filter_index;
		fragment.return_type = LogicalType::BOOLEAN;
		fragment.root = std::move(predicate);
		if (render_diagnostics) {
			fragment.ir = "duckdb.storage-filter typed-vector-ir;" + DescribeExecutionExpressionIR(*fragment.root);
		}

		SljitNativeScanFilterPlan scan_filter;
		scan_filter.filter_index = source_filter.filter_index;
		scan_filter.input_type = contract.source_contract_input_types[source_filter.scan_column_index];
		scan_filter.input_not_null = source_filter.scan_column_index < contract.source_contract_input_not_null.size() &&
		                             contract.source_contract_input_not_null[source_filter.scan_column_index];
		if (!TryLowerNativeRegionExpression(fragment, true, scan_filter.filter, error, render_diagnostics)) {
			continue;
		}
		scan_filters.push_back(std::move(scan_filter));
	}
}

} // namespace duckdb

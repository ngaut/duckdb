//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_expression_selectivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

namespace duckdb {

static bool SljitSelectivityValueAsDouble(const LogicalType &type, const Value &value, double &result) {
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

static idx_t SljitExpressionSelectivityToBasisPoints(double selectivity) {
	if (selectivity <= 0) {
		return 1;
	}
	if (selectivity >= 1) {
		return 10000;
	}
	return MaxValue<idx_t>(1, static_cast<idx_t>(selectivity * 10000.0));
}

static ExecutionExpressionBinaryOp SljitReverseSelectivityComparison(ExecutionExpressionBinaryOp op) {
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

static idx_t SljitEstimateReferenceComparisonSelectivityBasisPoints(const ExecutionExpressionIR &reference,
                                                                    ExecutionExpressionBinaryOp op,
                                                                    const Value &constant,
                                                                    const vector<Value> &source_min_values,
                                                                    const vector<Value> &source_max_values) {
	if (reference.kind != ExecutionExpressionIRKind::REFERENCE || reference.ref_index >= source_min_values.size() ||
	    reference.ref_index >= source_max_values.size()) {
		return 10000;
	}
	double min_value;
	double max_value;
	double constant_value;
	if (!SljitSelectivityValueAsDouble(reference.return_type, source_min_values[reference.ref_index], min_value) ||
	    !SljitSelectivityValueAsDouble(reference.return_type, source_max_values[reference.ref_index], max_value) ||
	    !SljitSelectivityValueAsDouble(reference.return_type, constant, constant_value) || max_value <= min_value) {
		return 10000;
	}
	const auto domain = max_value - min_value;
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOT_DISTINCT_FROM:
		if (constant_value < min_value || constant_value > max_value) {
			return 1;
		}
		return SljitExpressionSelectivityToBasisPoints(1.0 / (domain + 1.0));
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SljitExpressionSelectivityToBasisPoints((constant_value - min_value) / domain);
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitExpressionSelectivityToBasisPoints((max_value - constant_value) / domain);
	default:
		return 10000;
	}
}

static idx_t SljitEstimateExpressionSelectivityBasisPoints(const ExecutionExpressionIR &expression,
                                                           const vector<Value> &source_min_values,
                                                           const vector<Value> &source_max_values) {
	if (expression.kind == ExecutionExpressionIRKind::BINARY && expression.left && expression.right) {
		if (expression.left->kind == ExecutionExpressionIRKind::REFERENCE &&
		    expression.right->kind == ExecutionExpressionIRKind::CONSTANT) {
			return SljitEstimateReferenceComparisonSelectivityBasisPoints(*expression.left, expression.binary_op,
			                                                              expression.right->constant, source_min_values,
			                                                              source_max_values);
		}
		if (expression.left->kind == ExecutionExpressionIRKind::CONSTANT &&
		    expression.right->kind == ExecutionExpressionIRKind::REFERENCE) {
			return SljitEstimateReferenceComparisonSelectivityBasisPoints(
			    *expression.right, SljitReverseSelectivityComparison(expression.binary_op), expression.left->constant,
			    source_min_values, source_max_values);
		}
	}
	if (expression.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    expression.conjunction_op != ExecutionExpressionConjunctionOp::AND) {
		return 10000;
	}
	idx_t result = 10000;
	for (auto &child : expression.children) {
		if (child) {
			result = MaxValue<idx_t>(
			    1, result *
			           SljitEstimateExpressionSelectivityBasisPoints(*child, source_min_values, source_max_values) /
			           10000);
		}
	}
	return result;
}

} // namespace duckdb

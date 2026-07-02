//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_range_stats.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_range_analysis.hpp"
#include "sljit_executable_stats.hpp"

namespace duckdb {

static bool SljitExpressionRangeValues(const SljitNativeRegionExpressionPlan &expr,
                                       const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                       Value &min_value, Value &max_value) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (expr.constant_value.IsNull() || expr.constant_value.type() != expr.return_type) {
			return false;
		}
		min_value = expr.constant_value;
		max_value = expr.constant_value;
		return true;
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (expr.source_index >= input_min_values.size() || expr.source_index >= input_max_values.size() ||
		    input_min_values[expr.source_index].IsNull() || input_max_values[expr.source_index].IsNull() ||
		    input_min_values[expr.source_index].type() != expr.return_type ||
		    input_max_values[expr.source_index].type() != expr.return_type) {
			return false;
		}
		min_value = input_min_values[expr.source_index];
		max_value = input_max_values[expr.source_index];
		return true;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
		if (!expr.expression_tree) {
			return false;
		}
		SljitExecutableInt128Range range;
		if (!SljitExecutableExpressionTreeRange(*expr.expression_tree, input_min_values, input_max_values, range)) {
			return false;
		}
		return SljitExecutableRangeValue(expr.return_type, range.min, min_value) &&
		       SljitExecutableRangeValue(expr.return_type, range.max, max_value);
	}
	default:
		return false;
	}
}

static vector<Value> BuildSljitProjectionOutputRanges(const SljitNativeRegionOpPlan &op,
                                                      const vector<Value> &input_min_values,
                                                      const vector<Value> &input_max_values, bool min_values) {
	vector<Value> result;
	result.reserve(op.projections.size());
	for (auto &projection : op.projections) {
		Value min_value;
		Value max_value;
		if (!SljitExpressionRangeValues(projection, input_min_values, input_max_values, min_value, max_value)) {
			result.push_back(Value());
			continue;
		}
		result.push_back(min_values ? min_value : max_value);
	}
	return result;
}

void SljitUpdateExecutableCurrentRanges(const SljitNativeRegionOpPlan &op, vector<Value> &current_min_values,
                                        vector<Value> &current_max_values) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION: {
		auto next_min_values = BuildSljitProjectionOutputRanges(op, current_min_values, current_max_values, true);
		auto next_max_values = BuildSljitProjectionOutputRanges(op, current_min_values, current_max_values, false);
		current_min_values = std::move(next_min_values);
		current_max_values = std::move(next_max_values);
		return;
	}
	default:
		current_min_values.assign(op.output_types.size(), Value());
		current_max_values.assign(op.output_types.size(), Value());
		return;
	}
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_stats.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_stats.hpp"

namespace duckdb {

static bool SljitExpressionResultNotNull(const SljitNativeRegionExpressionPlan &expr,
                                         const vector<bool> &input_not_null) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return !expr.constant_value.IsNull();
	case SljitNativeRegionExpressionKind::REFERENCE:
		return expr.source_index < input_not_null.size() && input_not_null[expr.source_index];
	default:
		return false;
	}
}

static vector<bool> BuildSljitProjectionOutputNotNull(const SljitNativeRegionOpPlan &op,
                                                      const vector<bool> &input_not_null) {
	vector<bool> result;
	result.reserve(op.projections.size());
	for (auto &projection : op.projections) {
		result.push_back(SljitExpressionResultNotNull(projection, input_not_null));
	}
	return result;
}

void SljitUpdateExecutableCurrentNotNull(const SljitNativeRegionOpPlan &op, vector<bool> &current_not_null) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION:
		current_not_null = BuildSljitProjectionOutputNotNull(op, current_not_null);
		return;
	default:
		current_not_null.assign(op.output_types.size(), false);
		return;
	}
}

} // namespace duckdb

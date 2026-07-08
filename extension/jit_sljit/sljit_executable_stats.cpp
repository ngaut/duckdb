//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_stats.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_stats.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"

namespace duckdb {

template <class T>
static bool SljitIntegerStatsValueInRange(const Value &value) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::INT8: {
		const auto input = value.GetValueUnsafe<int8_t>();
		return input >= NumericLimits<T>::Minimum() && input <= NumericLimits<T>::Maximum();
	}
	case PhysicalType::INT16: {
		const auto input = value.GetValueUnsafe<int16_t>();
		return input >= NumericLimits<T>::Minimum() && input <= NumericLimits<T>::Maximum();
	}
	case PhysicalType::INT32: {
		const auto input = value.GetValueUnsafe<int32_t>();
		return input >= NumericLimits<T>::Minimum() && input <= NumericLimits<T>::Maximum();
	}
	case PhysicalType::INT64: {
		const auto input = value.GetValueUnsafe<int64_t>();
		return input >= NumericLimits<T>::Minimum() && input <= NumericLimits<T>::Maximum();
	}
	default:
		return false;
	}
}

static bool SljitIntegerCastStatsPreservesDistinct(const SljitNativeRegionExpressionPlan &expr,
                                                   const vector<Value> &input_min_values,
                                                   const vector<Value> &input_max_values) {
	if (expr.kind != SljitNativeRegionExpressionKind::INTEGER_CAST || expr.try_cast ||
	    expr.source_index >= input_min_values.size() || expr.source_index >= input_max_values.size()) {
		return false;
	}
	auto &min_value = input_min_values[expr.source_index];
	auto &max_value = input_max_values[expr.source_index];
	switch (expr.cast_target_width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return SljitIntegerStatsValueInRange<int8_t>(min_value) && SljitIntegerStatsValueInRange<int8_t>(max_value);
	case SljitNativeSignedIntegerWidth::INT16:
		return SljitIntegerStatsValueInRange<int16_t>(min_value) && SljitIntegerStatsValueInRange<int16_t>(max_value);
	case SljitNativeSignedIntegerWidth::INT32:
		return SljitIntegerStatsValueInRange<int32_t>(min_value) && SljitIntegerStatsValueInRange<int32_t>(max_value);
	case SljitNativeSignedIntegerWidth::INT64:
		return SljitIntegerStatsValueInRange<int64_t>(min_value) && SljitIntegerStatsValueInRange<int64_t>(max_value);
	default:
		return false;
	}
}

static idx_t SljitExpressionDistinctCount(const SljitNativeRegionExpressionPlan &expr,
                                          const vector<idx_t> &input_distinct_counts,
                                          const vector<Value> &input_min_values,
                                          const vector<Value> &input_max_values) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return expr.source_index < input_distinct_counts.size() ? input_distinct_counts[expr.source_index] : 0;
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
		if (expr.source_index >= input_distinct_counts.size() ||
		    !SljitIntegerCastStatsPreservesDistinct(expr, input_min_values, input_max_values)) {
			return 0;
		}
		return input_distinct_counts[expr.source_index];
	default:
		return 0;
	}
}

static vector<idx_t> BuildSljitProjectionOutputDistinctCounts(const SljitNativeRegionOpPlan &op,
                                                              const vector<idx_t> &input_distinct_counts,
                                                              const vector<Value> &input_min_values,
                                                              const vector<Value> &input_max_values) {
	vector<idx_t> result;
	result.reserve(op.projections.size());
	for (auto &projection : op.projections) {
		result.push_back(
		    SljitExpressionDistinctCount(projection, input_distinct_counts, input_min_values, input_max_values));
	}
	return result;
}

static vector<idx_t> BuildSljitHashJoinProbeOutputDistinctCounts(const SljitNativeRegionOpPlan &op,
                                                                 const vector<idx_t> &input_distinct_counts) {
	vector<idx_t> result;
	result.reserve(op.output_types.size());
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (contract.present) {
		for (auto input_idx : contract.lhs_output_column_indices) {
			result.push_back(input_idx < input_distinct_counts.size() ? input_distinct_counts[input_idx] : 0);
		}
	}
	while (result.size() < op.output_types.size()) {
		result.push_back(0);
	}
	if (result.size() > op.output_types.size()) {
		result.resize(op.output_types.size());
	}
	return result;
}

static bool SljitHashJoinProbeMayDuplicateProbeRows(const SljitNativeRegionOpPlan &op) {
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (!contract.present) {
		return false;
	}
	switch (contract.join_type) {
	case ExecutionRegionJoinType::INNER:
	case ExecutionRegionJoinType::LEFT:
	case ExecutionRegionJoinType::RIGHT:
	case ExecutionRegionJoinType::OUTER:
		return true;
	default:
		return false;
	}
}

bool SljitTryGetHashJoinRHSOutputConditionIndex(const ExecutionRegionHashJoinContract &contract, idx_t rhs_output_idx,
                                                idx_t &condition_idx) {
	if (rhs_output_idx >= contract.rhs_output_column_indices.size()) {
		return false;
	}
	const auto layout_idx = contract.rhs_output_column_indices[rhs_output_idx];
	if (layout_idx >= contract.equality_condition_count || layout_idx >= contract.comparison_types.size()) {
		return false;
	}
	condition_idx = layout_idx;
	return true;
}

bool SljitTryGetHashJoinProbeKeyInputIndex(const SljitNativeRegionOpPlan &op, idx_t condition_idx, idx_t &input_idx) {
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (condition_idx < op.hash_join_probe.keys.size()) {
		input_idx = op.hash_join_probe.keys[condition_idx].key_input_index;
		return true;
	}
	if (condition_idx < contract.lhs_probe_column_indices.size()) {
		input_idx = contract.lhs_probe_column_indices[condition_idx];
		return true;
	}
	return false;
}

static vector<idx_t>
BuildSljitHashJoinProbeOutputDistinctReserveCounts(const SljitNativeRegionOpPlan &op,
                                                   const vector<idx_t> &input_distinct_reserve_counts,
                                                   const vector<idx_t> &input_distinct_counts) {
	auto result = BuildSljitHashJoinProbeOutputDistinctCounts(op, input_distinct_reserve_counts);
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (!contract.present) {
		return result;
	}
	if (SljitHashJoinProbeMayDuplicateProbeRows(op)) {
		for (idx_t output_idx = 0; output_idx < contract.lhs_output_column_indices.size() && output_idx < result.size();
		     output_idx++) {
			if (result[output_idx] != 0) {
				continue;
			}
			const auto input_idx = contract.lhs_output_column_indices[output_idx];
			result[output_idx] = input_idx < input_distinct_counts.size() ? input_distinct_counts[input_idx] : 0;
		}
	}
	if (op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return result;
	}
	const auto rhs_output_offset = contract.lhs_output_column_indices.size();
	for (idx_t rhs_output_idx = 0; rhs_output_idx < contract.rhs_output_column_count; rhs_output_idx++) {
		const auto output_idx = rhs_output_offset + rhs_output_idx;
		if (output_idx >= result.size() || result[output_idx] != 0) {
			continue;
		}
		idx_t condition_idx;
		idx_t input_idx;
		if (!SljitTryGetHashJoinRHSOutputConditionIndex(contract, rhs_output_idx, condition_idx) ||
		    !SljitTryGetHashJoinProbeKeyInputIndex(op, condition_idx, input_idx)) {
			continue;
		}
		if (input_idx < input_distinct_reserve_counts.size() && input_distinct_reserve_counts[input_idx] != 0) {
			result[output_idx] = input_distinct_reserve_counts[input_idx];
			continue;
		}
		result[output_idx] = input_idx < input_distinct_counts.size() ? input_distinct_counts[input_idx] : 0;
	}
	return result;
}

static bool SljitExpressionResultNotNull(const SljitNativeRegionExpressionPlan &expr,
                                         const vector<bool> &input_not_null) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return !expr.constant_value.IsNull();
	case SljitNativeRegionExpressionKind::REFERENCE:
		return expr.source_index < input_not_null.size() && input_not_null[expr.source_index];
	case SljitNativeRegionExpressionKind::STRING_SUBSTRING:
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

static vector<bool> BuildSljitHashJoinProbeOutputNotNull(const SljitNativeRegionOpPlan &op,
                                                         const vector<bool> &input_not_null) {
	vector<bool> result;
	result.reserve(op.output_types.size());
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (contract.present) {
		for (auto input_idx : contract.lhs_output_column_indices) {
			result.push_back(input_idx < input_not_null.size() && input_not_null[input_idx]);
		}
	}
	while (result.size() < op.output_types.size()) {
		result.push_back(false);
	}
	if (result.size() > op.output_types.size()) {
		result.resize(op.output_types.size());
	}
	return result;
}

vector<bool> SljitBuildExecutableOutputNotNull(const SljitNativeRegionOpPlan &op, const vector<bool> &input_not_null) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return input_not_null;
	case SljitNativeRegionOpKind::PROJECTION:
		return BuildSljitProjectionOutputNotNull(op, input_not_null);
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return BuildSljitHashJoinProbeOutputNotNull(op, input_not_null);
	default:
		return vector<bool>(op.output_types.size(), false);
	}
}

void SljitUpdateExecutableCurrentNotNull(const SljitNativeRegionOpPlan &op, vector<bool> &current_not_null) {
	current_not_null = SljitBuildExecutableOutputNotNull(op, current_not_null);
}

void SljitUpdateExecutableCurrentDistinctCounts(const SljitNativeRegionOpPlan &op,
                                                vector<idx_t> &current_distinct_counts,
                                                const vector<Value> &current_min_values,
                                                const vector<Value> &current_max_values) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION:
		current_distinct_counts = BuildSljitProjectionOutputDistinctCounts(op, current_distinct_counts,
		                                                                   current_min_values, current_max_values);
		return;
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		current_distinct_counts = BuildSljitHashJoinProbeOutputDistinctCounts(op, current_distinct_counts);
		return;
	default:
		current_distinct_counts.assign(op.output_types.size(), 0);
		return;
	}
}

void SljitUpdateExecutableCurrentDistinctReserveCounts(const SljitNativeRegionOpPlan &op,
                                                       vector<idx_t> &current_distinct_reserve_counts,
                                                       const vector<idx_t> &current_distinct_counts,
                                                       const vector<Value> &current_min_values,
                                                       const vector<Value> &current_max_values) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::FILTER:
		return;
	case SljitNativeRegionOpKind::PROJECTION:
		current_distinct_reserve_counts = BuildSljitProjectionOutputDistinctCounts(
		    op, current_distinct_reserve_counts, current_min_values, current_max_values);
		return;
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		current_distinct_reserve_counts = BuildSljitHashJoinProbeOutputDistinctReserveCounts(
		    op, current_distinct_reserve_counts, current_distinct_counts);
		return;
	default:
		current_distinct_reserve_counts.assign(op.output_types.size(), 0);
		return;
	}
}

} // namespace duckdb

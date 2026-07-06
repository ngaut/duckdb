//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_range_stats.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_range_analysis.hpp"
#include "sljit_executable_stats.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"

namespace duckdb {

template <class T>
static bool SljitSignedIntegerStatsValue(const Value &value, int64_t &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = value.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		result = value.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		result = value.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		break;
	default:
		return false;
	}
	return result >= NumericLimits<T>::Minimum() && result <= NumericLimits<T>::Maximum();
}

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
	case SljitNativeRegionExpressionKind::INTEGER_CAST: {
		if (expr.try_cast || expr.source_index >= input_min_values.size() ||
		    expr.source_index >= input_max_values.size()) {
			return false;
		}
		auto &input_min = input_min_values[expr.source_index];
		auto &input_max = input_max_values[expr.source_index];
		int64_t min_integer;
		int64_t max_integer;
		switch (expr.cast_target_width) {
		case SljitNativeSignedIntegerWidth::INT8:
			if (!SljitSignedIntegerStatsValue<int8_t>(input_min, min_integer) ||
			    !SljitSignedIntegerStatsValue<int8_t>(input_max, max_integer)) {
				return false;
			}
			min_value = Value::TINYINT(UnsafeNumericCast<int8_t>(min_integer));
			max_value = Value::TINYINT(UnsafeNumericCast<int8_t>(max_integer));
			return true;
		case SljitNativeSignedIntegerWidth::INT16:
			if (!SljitSignedIntegerStatsValue<int16_t>(input_min, min_integer) ||
			    !SljitSignedIntegerStatsValue<int16_t>(input_max, max_integer)) {
				return false;
			}
			min_value = Value::SMALLINT(UnsafeNumericCast<int16_t>(min_integer));
			max_value = Value::SMALLINT(UnsafeNumericCast<int16_t>(max_integer));
			return true;
		case SljitNativeSignedIntegerWidth::INT32:
			if (!SljitSignedIntegerStatsValue<int32_t>(input_min, min_integer) ||
			    !SljitSignedIntegerStatsValue<int32_t>(input_max, max_integer)) {
				return false;
			}
			min_value = Value::INTEGER(UnsafeNumericCast<int32_t>(min_integer));
			max_value = Value::INTEGER(UnsafeNumericCast<int32_t>(max_integer));
			return true;
		case SljitNativeSignedIntegerWidth::INT64:
			if (!SljitSignedIntegerStatsValue<int64_t>(input_min, min_integer) ||
			    !SljitSignedIntegerStatsValue<int64_t>(input_max, max_integer)) {
				return false;
			}
			min_value = Value::BIGINT(min_integer);
			max_value = Value::BIGINT(max_integer);
			return true;
		default:
			return false;
		}
	}
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

static bool SljitTryCastHashJoinEqualityRangeValue(const Value &input, const LogicalType &output_type, Value &result) {
	if (input.IsNull()) {
		return false;
	}
	if (input.type() == output_type) {
		result = input;
		return true;
	}
	int64_t integer_value;
	switch (output_type.InternalType()) {
	case PhysicalType::INT8:
		if (!SljitSignedIntegerStatsValue<int8_t>(input, integer_value)) {
			return false;
		}
		result = Value::TINYINT(UnsafeNumericCast<int8_t>(integer_value));
		return true;
	case PhysicalType::INT16:
		if (!SljitSignedIntegerStatsValue<int16_t>(input, integer_value)) {
			return false;
		}
		result = Value::SMALLINT(UnsafeNumericCast<int16_t>(integer_value));
		return true;
	case PhysicalType::INT32:
		if (!SljitSignedIntegerStatsValue<int32_t>(input, integer_value)) {
			return false;
		}
		result = Value::INTEGER(UnsafeNumericCast<int32_t>(integer_value));
		return true;
	case PhysicalType::INT64:
		if (!SljitSignedIntegerStatsValue<int64_t>(input, integer_value)) {
			return false;
		}
		result = Value::BIGINT(integer_value);
		return true;
	default:
		return false;
	}
}

static vector<Value> BuildSljitHashJoinProbeOutputRanges(const SljitNativeRegionOpPlan &op,
                                                         const vector<Value> &input_values) {
	vector<Value> result;
	result.reserve(op.output_types.size());
	auto &contract = op.hash_join_probe.operator_info.hash_join_contract;
	if (contract.present) {
		for (idx_t output_idx = 0; output_idx < contract.lhs_output_column_indices.size(); output_idx++) {
			auto input_idx = contract.lhs_output_column_indices[output_idx];
			if (input_idx >= input_values.size() || output_idx >= op.output_types.size() ||
			    input_values[input_idx].IsNull() || input_values[input_idx].type() != op.output_types[output_idx]) {
				result.push_back(Value());
			} else {
				result.push_back(input_values[input_idx]);
			}
		}
	}
	while (result.size() < op.output_types.size()) {
		result.push_back(Value());
	}
	if (result.size() > op.output_types.size()) {
		result.resize(op.output_types.size());
	}
	if (!contract.present ||
	    op.hash_join_probe.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return result;
	}
	const auto rhs_output_offset = contract.lhs_output_column_indices.size();
	for (idx_t rhs_output_idx = 0; rhs_output_idx < contract.rhs_output_column_count; rhs_output_idx++) {
		const auto output_idx = rhs_output_offset + rhs_output_idx;
		if (output_idx >= result.size() || output_idx >= op.output_types.size()) {
			continue;
		}
		idx_t condition_idx;
		idx_t input_idx;
		if (!SljitTryGetHashJoinRHSOutputConditionIndex(contract, rhs_output_idx, condition_idx) ||
		    !SljitTryGetHashJoinProbeKeyInputIndex(op, condition_idx, input_idx) || input_idx >= input_values.size()) {
			continue;
		}
		Value range_value;
		if (SljitTryCastHashJoinEqualityRangeValue(input_values[input_idx], op.output_types[output_idx], range_value)) {
			result[output_idx] = std::move(range_value);
		}
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
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
		auto next_min_values = BuildSljitHashJoinProbeOutputRanges(op, current_min_values);
		auto next_max_values = BuildSljitHashJoinProbeOutputRanges(op, current_max_values);
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_perfect_hash_range_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_aggregate_perfect_hash_codegen.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

struct SljitInt128Range {
	hugeint_t min;
	hugeint_t max;
};

static bool SljitValueToHugeint(const Value &value, const LogicalType &expected_type, hugeint_t &result) {
	if (value.IsNull() || value.type() != expected_type) {
		return false;
	}
	if (value.type().id() == LogicalTypeId::DATE) {
		result = Hugeint::Convert(value.GetValueUnsafe<date_t>().days);
		return true;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		result = Hugeint::Convert(value.GetValueUnsafe<bool>() ? 1 : 0);
		return true;
	case PhysicalType::INT8:
		result = Hugeint::Convert(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		result = Hugeint::Convert(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		result = Hugeint::Convert(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = Hugeint::Convert(value.GetValueUnsafe<int64_t>());
		return true;
	case PhysicalType::UINT8:
		result = Hugeint::Convert(value.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::UINT16:
		result = Hugeint::Convert(value.GetValueUnsafe<uint16_t>());
		return true;
	case PhysicalType::UINT32:
		result = Hugeint::Convert(value.GetValueUnsafe<uint32_t>());
		return true;
	case PhysicalType::UINT64:
		result = Hugeint::Convert(value.GetValueUnsafe<uint64_t>());
		return true;
	case PhysicalType::INT128:
		result = value.GetValueUnsafe<hugeint_t>();
		return true;
	default:
		return false;
	}
}

static bool SljitRangeFromSource(idx_t source_index, const LogicalType &expected_type,
                                 const vector<Value> &source_min_values, const vector<Value> &source_max_values,
                                 SljitInt128Range &result) {
	if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
		return false;
	}
	return SljitValueToHugeint(source_min_values[source_index], expected_type, result.min) &&
	       SljitValueToHugeint(source_max_values[source_index], expected_type, result.max) && result.min <= result.max;
}

static bool SljitRangeAdd(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TryAddInPlace(result.min, right.min) && Hugeint::TryAddInPlace(result.max, right.max);
}

static bool SljitRangeSubtract(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TrySubtractInPlace(result.min, right.max) && Hugeint::TrySubtractInPlace(result.max, right.min);
}

static bool SljitRangeMultiply(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	hugeint_t values[4];
	if (!Hugeint::TryMultiply(left.min, right.min, values[0]) ||
	    !Hugeint::TryMultiply(left.min, right.max, values[1]) ||
	    !Hugeint::TryMultiply(left.max, right.min, values[2]) ||
	    !Hugeint::TryMultiply(left.max, right.max, values[3])) {
		return false;
	}
	result.min = values[0];
	result.max = values[0];
	for (idx_t value_idx = 1; value_idx < 4; value_idx++) {
		result.min = MinValue(result.min, values[value_idx]);
		result.max = MaxValue(result.max, values[value_idx]);
	}
	return true;
}

static bool SljitDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	const auto left_decimal = node.left->return_type.id() == LogicalTypeId::DECIMAL &&
	                          node.left->return_type.InternalType() == PhysicalType::INT64;
	const auto right_decimal = node.right->return_type.id() == LogicalTypeId::DECIMAL &&
	                           node.right->return_type.InternalType() == PhysicalType::INT64;
	if (!left_decimal && !right_decimal) {
		return true;
	}
	if (!left_decimal || !right_decimal || node.return_type.id() != LogicalTypeId::DECIMAL ||
	    node.return_type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

static bool SljitRangeScaleByPowerOfTen(SljitInt128Range &range, uint8_t scale_delta) {
	if (scale_delta == 0) {
		return true;
	}
	if (scale_delta >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto scale = Hugeint::Convert(NumericHelper::POWERS_OF_TEN[scale_delta]);
	return Hugeint::TryMultiply(range.min, scale, range.min) && Hugeint::TryMultiply(range.max, scale, range.max);
}

static bool SljitRangeCast(const ExecutionExpressionIR &node, const SljitInt128Range &child, SljitInt128Range &result) {
	if (!node.left) {
		return false;
	}
	result = child;
	if (node.return_type.InternalType() == node.left->return_type.InternalType()) {
		if (node.return_type.id() == LogicalTypeId::DECIMAL || node.left->return_type.id() == LogicalTypeId::DECIMAL) {
			if (node.return_type.id() != LogicalTypeId::DECIMAL ||
			    node.left->return_type.id() != LogicalTypeId::DECIMAL) {
				return false;
			}
			auto source_scale = DecimalType::GetScale(node.left->return_type);
			auto target_scale = DecimalType::GetScale(node.return_type);
			if (target_scale < source_scale) {
				return false;
			}
			return SljitRangeScaleByPowerOfTen(result, target_scale - source_scale);
		}
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DECIMAL && node.return_type.InternalType() == PhysicalType::INT64 &&
	    node.left->return_type.IsIntegral()) {
		return SljitRangeScaleByPowerOfTen(result, DecimalType::GetScale(node.return_type));
	}
	if (node.return_type.IsIntegral() && node.left->return_type.IsIntegral()) {
		return true;
	}
	return false;
}

static bool SljitExpressionTreeInt128Range(const ExecutionExpressionIR &node, const vector<Value> &source_min_values,
                                           const vector<Value> &source_max_values, SljitInt128Range &result) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		return SljitValueToHugeint(node.constant, node.return_type, result.min) &&
		       SljitValueToHugeint(node.constant, node.return_type, result.max);
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitRangeFromSource(node.ref_index, node.return_type, source_min_values, source_max_values, result);
	case ExecutionExpressionIRKind::CAST: {
		if (!node.left) {
			return false;
		}
		SljitInt128Range child;
		return SljitExpressionTreeInt128Range(*node.left, source_min_values, source_max_values, child) &&
		       SljitRangeCast(node, child, result);
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right || !SljitDecimal64BinaryHasRawSemantics(node)) {
			return false;
		}
		SljitInt128Range left;
		SljitInt128Range right;
		if (!SljitExpressionTreeInt128Range(*node.left, source_min_values, source_max_values, left) ||
		    !SljitExpressionTreeInt128Range(*node.right, source_min_values, source_max_values, right)) {
			return false;
		}
		switch (node.binary_op) {
		case ExecutionExpressionBinaryOp::ADD:
			return SljitRangeAdd(left, right, result);
		case ExecutionExpressionBinaryOp::SUBTRACT:
			return SljitRangeSubtract(left, right, result);
		case ExecutionExpressionBinaryOp::MULTIPLY:
			return SljitRangeMultiply(left, right, result);
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool SljitRangeAbsFitsLocalInt64Sum(const SljitInt128Range &range) {
	auto max_abs = range.max;
	if (max_abs < 0) {
		if (!Hugeint::TryNegate(max_abs, max_abs)) {
			return false;
		}
	}
	auto min_abs = range.min;
	if (min_abs < 0) {
		if (!Hugeint::TryNegate(min_abs, min_abs)) {
			return false;
		}
	}
	max_abs = MaxValue(max_abs, min_abs);
	auto limit = Hugeint::Convert(NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE));
	return max_abs <= limit;
}

static bool SljitPayloadRangeLocalSumLowerNeverOverflows(const SljitNativeRegionExpressionPlan &payload,
                                                         const vector<Value> &source_min_values,
                                                         const vector<Value> &source_max_values) {
	SljitInt128Range range;
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (!SljitValueToHugeint(payload.constant_value, payload.return_type, range.min) ||
		    !SljitValueToHugeint(payload.constant_value, payload.return_type, range.max)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (!SljitRangeFromSource(payload.source_index, payload.return_type, source_min_values, source_max_values,
		                          range)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		if (!payload.expression_tree ||
		    !SljitExpressionTreeInt128Range(*payload.expression_tree, source_min_values, source_max_values, range)) {
			return false;
		}
		break;
	default:
		return false;
	}
	return SljitRangeAbsFitsLocalInt64Sum(range);
}

static bool SljitLocalSumLowerNeverOverflows(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		break;
	}
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	const auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	const auto max_abs = NumericHelper::POWERS_OF_TEN[width] - 1;
	return max_abs <= NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE);
}

void AnnotateSljitLocalPerfectHashAggregatePlan(SljitLocalPerfectHashAggregatePlan &plan,
                                                const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                const vector<ExecutionRegionAggregateInput> &aggregates,
                                                const vector<Value> &source_min_values,
                                                const vector<Value> &source_max_values) {
	if (!plan.enabled) {
		return;
	}
	for (idx_t payload_idx = 0;
	     payload_idx < payloads.size() && payload_idx < aggregates.size() && payload_idx < plan.lanes.size();
	     payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			continue;
		}
		plan.lanes[payload_idx].local_lower_never_overflows =
		    SljitLocalSumLowerNeverOverflows(payloads[payload_idx].return_type) ||
		    SljitPayloadRangeLocalSumLowerNeverOverflows(payloads[payload_idx], source_min_values, source_max_values);
	}
}

} // namespace duckdb

#include "sljit_executable_range_analysis.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"

namespace duckdb {

bool SljitExecutableValueToHugeint(const Value &value, const LogicalType &expected_type, hugeint_t &result) {
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

static bool SljitExecutableRangeFromInput(idx_t source_index, const LogicalType &expected_type,
                                          const vector<Value> &input_min_values, const vector<Value> &input_max_values,
                                          SljitExecutableInt128Range &result) {
	if (source_index >= input_min_values.size() || source_index >= input_max_values.size()) {
		return false;
	}
	result.type = expected_type;
	return SljitExecutableValueToHugeint(input_min_values[source_index], expected_type, result.min) &&
	       SljitExecutableValueToHugeint(input_max_values[source_index], expected_type, result.max) &&
	       result.min <= result.max;
}

static bool SljitExecutableRangeAdd(const SljitExecutableInt128Range &left, const SljitExecutableInt128Range &right,
                                    const LogicalType &result_type, SljitExecutableInt128Range &result) {
	result.type = result_type;
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TryAddInPlace(result.min, right.min) && Hugeint::TryAddInPlace(result.max, right.max);
}

static bool SljitExecutableRangeSubtract(const SljitExecutableInt128Range &left,
                                         const SljitExecutableInt128Range &right, const LogicalType &result_type,
                                         SljitExecutableInt128Range &result) {
	result.type = result_type;
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TrySubtractInPlace(result.min, right.max) && Hugeint::TrySubtractInPlace(result.max, right.min);
}

static bool SljitExecutableRangeMultiply(const SljitExecutableInt128Range &left,
                                         const SljitExecutableInt128Range &right, const LogicalType &result_type,
                                         SljitExecutableInt128Range &result) {
	hugeint_t values[4];
	if (!Hugeint::TryMultiply(left.min, right.min, values[0]) ||
	    !Hugeint::TryMultiply(left.min, right.max, values[1]) ||
	    !Hugeint::TryMultiply(left.max, right.min, values[2]) ||
	    !Hugeint::TryMultiply(left.max, right.max, values[3])) {
		return false;
	}
	result.type = result_type;
	result.min = values[0];
	result.max = values[0];
	for (idx_t value_idx = 1; value_idx < 4; value_idx++) {
		result.min = MinValue(result.min, values[value_idx]);
		result.max = MaxValue(result.max, values[value_idx]);
	}
	return true;
}

static bool SljitExecutableDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
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

static bool SljitExecutableScaleRangeByPowerOfTen(SljitExecutableInt128Range &range, uint8_t scale_delta) {
	if (scale_delta == 0) {
		return true;
	}
	if (scale_delta >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto scale = Hugeint::Convert(NumericHelper::POWERS_OF_TEN[scale_delta]);
	return Hugeint::TryMultiply(range.min, scale, range.min) && Hugeint::TryMultiply(range.max, scale, range.max);
}

static bool SljitExecutableRangeCast(const ExecutionExpressionIR &node, const SljitExecutableInt128Range &child,
                                     SljitExecutableInt128Range &result) {
	if (!node.left) {
		return false;
	}
	result = child;
	result.type = node.return_type;
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
			return SljitExecutableScaleRangeByPowerOfTen(result, target_scale - source_scale);
		}
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DECIMAL && node.return_type.InternalType() == PhysicalType::INT64 &&
	    node.left->return_type.IsIntegral()) {
		return SljitExecutableScaleRangeByPowerOfTen(result, DecimalType::GetScale(node.return_type));
	}
	if (node.return_type.IsIntegral() && node.left->return_type.IsIntegral()) {
		return true;
	}
	return false;
}

bool SljitExecutableExpressionTreeRange(const ExecutionExpressionIR &node, const vector<Value> &input_min_values,
                                        const vector<Value> &input_max_values, SljitExecutableInt128Range &result) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		result.type = node.return_type;
		return SljitExecutableValueToHugeint(node.constant, node.return_type, result.min) &&
		       SljitExecutableValueToHugeint(node.constant, node.return_type, result.max);
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitExecutableRangeFromInput(node.ref_index, node.return_type, input_min_values, input_max_values,
		                                     result);
	case ExecutionExpressionIRKind::CAST: {
		if (!node.left) {
			return false;
		}
		SljitExecutableInt128Range child;
		return SljitExecutableExpressionTreeRange(*node.left, input_min_values, input_max_values, child) &&
		       SljitExecutableRangeCast(node, child, result);
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right || !SljitExecutableDecimal64BinaryHasRawSemantics(node)) {
			return false;
		}
		SljitExecutableInt128Range left;
		SljitExecutableInt128Range right;
		if (!SljitExecutableExpressionTreeRange(*node.left, input_min_values, input_max_values, left) ||
		    !SljitExecutableExpressionTreeRange(*node.right, input_min_values, input_max_values, right)) {
			return false;
		}
		switch (node.binary_op) {
		case ExecutionExpressionBinaryOp::ADD:
			return SljitExecutableRangeAdd(left, right, node.return_type, result);
		case ExecutionExpressionBinaryOp::SUBTRACT:
			return SljitExecutableRangeSubtract(left, right, node.return_type, result);
		case ExecutionExpressionBinaryOp::MULTIPLY:
			return SljitExecutableRangeMultiply(left, right, node.return_type, result);
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

bool SljitExecutableRangeValue(const LogicalType &type, const hugeint_t &input, Value &result) {
	if (type.id() == LogicalTypeId::DATE) {
		int32_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = Value::DATE(date_t(value));
		return true;
	}
	switch (type.InternalType()) {
	case PhysicalType::INT8: {
		int8_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = Value::TINYINT(value);
		return true;
	}
	case PhysicalType::INT16: {
		int16_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::SMALLINT(value);
		return true;
	}
	case PhysicalType::INT32: {
		int32_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::INTEGER(value);
		return true;
	}
	case PhysicalType::INT64: {
		int64_t value;
		if (!Hugeint::TryCast(input, value)) {
			return false;
		}
		result = type.id() == LogicalTypeId::DECIMAL
		             ? Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type))
		             : Value::BIGINT(value);
		return true;
	}
	case PhysicalType::INT128:
		if (type.id() != LogicalTypeId::DECIMAL) {
			result = Value::HUGEINT(input);
		} else {
			result = Value::DECIMAL(input, DecimalType::GetWidth(type), DecimalType::GetScale(type));
		}
		return true;
	default:
		return false;
	}
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_expression_type_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

bool SljitExpressionTreeBinaryOpSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return true;
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeComparisonSupported(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeIsInt64Node(const ExecutionExpressionIR &node) {
	return node.return_type.IsIntegral() && node.physical_type == PhysicalType::INT64;
}

bool SljitTypedExpressionTreeIsDecimal64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
}

bool SljitTypedExpressionTreeIsDoubleNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DOUBLE && node.physical_type == PhysicalType::DOUBLE;
}

//! A TINYINT: loads sign-extend, and only references and value-preserving casts produce one
static bool SljitTypedExpressionTreeIsInt8Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::TINYINT && node.physical_type == PhysicalType::INT8;
}

static bool SljitTypedExpressionTreeIsUInt8Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::UTINYINT && node.physical_type == PhysicalType::UINT8;
}

//! A USMALLINT or UINTEGER: loads zero-extend, and only references and value-preserving casts produce one
static bool SljitTypedExpressionTreeIsUInt16Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::USMALLINT && node.physical_type == PhysicalType::UINT16;
}

static bool SljitTypedExpressionTreeIsUInt32Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::UINTEGER && node.physical_type == PhysicalType::UINT32;
}

static bool SljitTypedExpressionTreeIsUInt64Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::UBIGINT && node.physical_type == PhysicalType::UINT64;
}

bool TryGetSljitTypedExpressionTreeDecimal64Range(const LogicalType &type, int64_t &result_min, int64_t &result_max) {
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	result_max = NumericHelper::POWERS_OF_TEN[width] - 1;
	result_min = -result_max;
	return true;
}

bool SljitTypedExpressionTreeIsInt32Node(const ExecutionExpressionIR &node) {
	return (node.return_type.IsIntegral() || node.return_type.id() == LogicalTypeId::DATE) &&
	       node.physical_type == PhysicalType::INT32;
}

bool SljitTypedExpressionTreeIsBoolNode(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::BOOLEAN && node.physical_type == PhysicalType::BOOL;
}

bool SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(const ExecutionExpressionIR &node) {
	return !node.try_cast && node.exception_behavior != ExecutionExpressionExceptionKind::NULL_ON_CAST_ERROR &&
	       node.exception_behavior != ExecutionExpressionExceptionKind::ERROR;
}

static bool SljitTypedExpressionTreeIsDecimal128Node(const ExecutionExpressionIR &node) {
	return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT128;
}

bool TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(const ExecutionExpressionIR &node, uint64_t &bound) {
	static constexpr uint64_t MAX_BOUND = static_cast<uint64_t>(NumericLimits<int64_t>::Maximum());
	if (!SljitTypedExpressionTreeIsDecimal128Node(node)) {
		return false;
	}
	const auto scale = DecimalType::GetScale(node.return_type);
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT: {
		if (node.constant.IsNull()) {
			bound = 0;
			return true;
		}
		int64_t value;
		if (!Hugeint::TryCast<int64_t>(node.constant.GetValueUnsafe<hugeint_t>(), value) ||
		    value == NumericLimits<int64_t>::Minimum()) {
			return false;
		}
		bound = static_cast<uint64_t>(value < 0 ? -value : value);
		return true;
	}
	case ExecutionExpressionIRKind::CAST: {
		if (!node.left || !SljitTypedExpressionTreeCastExceptionBehaviorIsSafe(node)) {
			return false;
		}
		auto &source = *node.left;
		int64_t source_min;
		int64_t source_max;
		if (SljitTypedExpressionTreeIsDecimal64Node(source)) {
			if (DecimalType::GetScale(source.return_type) != scale ||
			    !TryGetSljitTypedExpressionTreeDecimal64Range(source.return_type, source_min, source_max)) {
				return false;
			}
			bound = static_cast<uint64_t>(source_max);
			return true;
		}
		return TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(source, bound) &&
		       DecimalType::GetScale(source.return_type) == scale;
	}
	case ExecutionExpressionIRKind::BINARY: {
		if ((node.binary_op != ExecutionExpressionBinaryOp::ADD &&
		     node.binary_op != ExecutionExpressionBinaryOp::SUBTRACT) ||
		    !node.left || !node.right) {
			return false;
		}
		uint64_t left_bound;
		uint64_t right_bound;
		if (!TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(*node.left, left_bound) ||
		    !TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(*node.right, right_bound) ||
		    DecimalType::GetScale(node.left->return_type) != scale ||
		    DecimalType::GetScale(node.right->return_type) != scale) {
			return false;
		}
		// |left +/- right| <= left_bound + right_bound <= INT64_MAX, so the 64-bit register result can never overflow
		// and the node's arithmetic_overflow_check flag needs no runtime check.
		if (left_bound > MAX_BOUND - right_bound) {
			return false;
		}
		bound = left_bound + right_bound;
		return true;
	}
	default:
		return false;
	}
}

bool SljitTypedExpressionTreeIsNarrowDecimal128Node(const ExecutionExpressionIR &node) {
	uint64_t bound;
	return TryGetSljitTypedExpressionTreeNarrowDecimal128Bound(node, bound);
}

bool SljitTypedExpressionTreeIsValueNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node) ||
	       SljitTypedExpressionTreeIsBoolNode(node) || SljitTypedExpressionTreeIsDecimal64Node(node) ||
	       SljitTypedExpressionTreeIsUInt8Node(node) || SljitTypedExpressionTreeIsUInt16Node(node) ||
	       SljitTypedExpressionTreeIsUInt32Node(node) || SljitTypedExpressionTreeIsUInt64Node(node) ||
	       SljitTypedExpressionTreeIsNarrowDecimal128Node(node) || SljitTypedExpressionTreeIsDoubleNode(node);
}

bool SljitTypedExpressionTreeIsIntegralValueNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsIntegerNode(node) || SljitTypedExpressionTreeIsInt8Node(node) ||
	       SljitTypedExpressionTreeIsUInt8Node(node) || SljitTypedExpressionTreeIsUInt16Node(node) ||
	       SljitTypedExpressionTreeIsUInt32Node(node) || SljitTypedExpressionTreeIsUInt64Node(node);
}

bool SljitTypedExpressionTreeIsIntegerNode(const ExecutionExpressionIR &node) {
	return SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsInt32Node(node);
}

bool SljitTypedExpressionTreeSameIntegerKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right));
}

bool SljitTypedExpressionTreeSameValueKind(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return (SljitTypedExpressionTreeIsInt64Node(left) && SljitTypedExpressionTreeIsInt64Node(right)) ||
	       (SljitTypedExpressionTreeIsInt32Node(left) && SljitTypedExpressionTreeIsInt32Node(right)) ||
	       (SljitTypedExpressionTreeIsBoolNode(left) && SljitTypedExpressionTreeIsBoolNode(right)) ||
	       (SljitTypedExpressionTreeIsDecimal64Node(left) && SljitTypedExpressionTreeIsDecimal64Node(right)) ||
	       (SljitTypedExpressionTreeIsNarrowDecimal128Node(left) &&
	        SljitTypedExpressionTreeIsNarrowDecimal128Node(right)) ||
	       (SljitTypedExpressionTreeIsDoubleNode(left) && SljitTypedExpressionTreeIsDoubleNode(right));
}

static bool TryGetSljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node, SljitNativeIntegerKind &kind) {
	if (SljitTypedExpressionTreeIsBoolNode(node)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	if (SljitTypedExpressionTreeIsInt8Node(node)) {
		kind = SljitNativeIntegerKind::INT8;
		return true;
	}
	if (SljitTypedExpressionTreeIsUInt8Node(node)) {
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	}
	if (SljitTypedExpressionTreeIsUInt16Node(node)) {
		kind = SljitNativeIntegerKind::UINT16;
		return true;
	}
	if (SljitTypedExpressionTreeIsUInt32Node(node)) {
		kind = SljitNativeIntegerKind::UINT32;
		return true;
	}
	if (SljitTypedExpressionTreeIsUInt64Node(node)) {
		kind = SljitNativeIntegerKind::UINT64;
		return true;
	}
	if (SljitTypedExpressionTreeIsInt32Node(node)) {
		kind = SljitNativeIntegerKind::INT32;
		return true;
	}
	if (SljitTypedExpressionTreeIsDecimal64Node(node)) {
		kind = SljitNativeIntegerKind::DECIMAL64;
		return true;
	}
	// a DOUBLE's bits load, move and store as one machine word
	if (SljitTypedExpressionTreeIsInt64Node(node) || SljitTypedExpressionTreeIsNarrowDecimal128Node(node) ||
	    SljitTypedExpressionTreeIsDoubleNode(node)) {
		kind = SljitNativeIntegerKind::INT64;
		return true;
	}
	return false;
}

SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const ExecutionExpressionIR &node) {
	SljitNativeIntegerKind kind;
	if (TryGetSljitTypedExpressionTreeIntegerKind(node, kind)) {
		return kind;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree node type");
}

SljitNativeIntegerCompareOp SljitTypedExpressionTreeCompareOp(ExecutionExpressionBinaryOp op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		return SljitNativeIntegerCompareOp::EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		return SljitNativeIntegerCompareOp::NOT_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	default:
		throw InternalException("Unsupported SLJIT typed expression-tree comparison operator");
	}
}

bool TryGetSljitTypedExpressionTreeResultKind(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind) {
	return TryGetSljitTypedExpressionTreeIntegerKind(root, kind);
}

} // namespace duckdb

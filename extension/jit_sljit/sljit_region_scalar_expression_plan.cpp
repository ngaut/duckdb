//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_scalar_expression_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_plan_internal.hpp"

#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/value.hpp"

#include "sljit_native_plan.hpp"

namespace duckdb {

static bool SljitGuardedReferenceCanCopyRawValue(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::INTERVAL:
	case PhysicalType::UINT128:
	case PhysicalType::INT128:
		return true;
	default:
		return false;
	}
}

static bool TryReadNativeErrorGuardedReference(const ExecutionExpressionIR &root,
                                               SljitNativeRegionExpressionPlan &expr) {
	if (root.kind != ExecutionExpressionIRKind::CASE || root.children.size() != 2 || !root.else_node) {
		return false;
	}
	auto &when = *root.children[0];
	auto &then_node = *root.children[1];
	auto &else_node = *root.else_node;
	if (then_node.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    then_node.intrinsic != ExecutionExpressionIntrinsicKind::ERROR || then_node.children.size() != 1 ||
	    !then_node.children[0]) {
		return false;
	}
	auto &message_node = *then_node.children[0];
	if (message_node.kind != ExecutionExpressionIRKind::CONSTANT ||
	    message_node.return_type.id() != LogicalTypeId::VARCHAR || message_node.constant.IsNull()) {
		return false;
	}
	if (else_node.kind != ExecutionExpressionIRKind::REFERENCE || else_node.return_type != root.return_type) {
		return false;
	}
	if (!SljitGuardedReferenceCanCopyRawValue(root.physical_type)) {
		return false;
	}
	auto value_size = GetTypeIdSize(root.physical_type);
	if (value_size == 0) {
		return false;
	}

	SljitNativeIntegerCompareOp guard_compare_op;
	SljitNativeIntegerKind guard_kind;
	idx_t guard_source_index;
	int64_t guard_constant;
	bool guard_constant_on_left;
	if (!TryReadNativeIntegerCompareConstant(when, guard_compare_op, guard_kind, guard_source_index, guard_constant,
	                                         guard_constant_on_left) ||
	    guard_kind != SljitNativeIntegerKind::INT64) {
		return false;
	}

	expr.kind = SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE;
	expr.return_type = root.return_type;
	expr.source_index = else_node.ref_index;
	expr.guard_source_index = guard_source_index;
	expr.guard_compare_op = guard_compare_op;
	expr.guard_constant = guard_constant;
	expr.guard_constant_on_left = guard_constant_on_left;
	expr.guarded_value_size = value_size;
	expr.error_message = StringValue::Get(message_node.constant);
	return true;
}

static bool TryReadNativeDecimal128ScaleUp(const ExecutionExpressionIR &root, idx_t &source_index,
                                           int64_t &scale_factor) {
	if (root.kind != ExecutionExpressionIRKind::CAST || root.try_cast || !root.left ||
	    root.return_type.id() != LogicalTypeId::DECIMAL || root.return_type.InternalType() != PhysicalType::INT128 ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.left->return_type.id() != LogicalTypeId::DECIMAL ||
	    root.left->return_type.InternalType() != PhysicalType::INT128) {
		return false;
	}
	auto source_scale = DecimalType::GetScale(root.left->return_type);
	auto target_scale = DecimalType::GetScale(root.return_type);
	if (target_scale <= source_scale || target_scale - source_scale > 18) {
		return false;
	}
	int64_t factor = 1;
	for (idx_t scale_idx = source_scale; scale_idx < target_scale; scale_idx++) {
		factor *= 10;
	}
	source_index = root.left->ref_index;
	scale_factor = factor;
	return true;
}

static bool TryReadNativeDecimal64ToDouble(const ExecutionExpressionIR &root, idx_t &source_index,
                                           double &scale_factor) {
	if (root.kind != ExecutionExpressionIRKind::CAST || root.try_cast || !root.left ||
	    root.return_type.id() != LogicalTypeId::DOUBLE || root.return_type.InternalType() != PhysicalType::DOUBLE ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.left->return_type.id() != LogicalTypeId::DECIMAL ||
	    root.left->return_type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	auto source_scale = DecimalType::GetScale(root.left->return_type);
	double factor = 1;
	for (idx_t scale_idx = 0; scale_idx < source_scale; scale_idx++) {
		factor *= 10;
	}
	source_index = root.left->ref_index;
	scale_factor = factor;
	return true;
}

bool TryReadNativeScalarIntrinsicRegionExpression(const ExecutionExpressionIR &root,
                                                  SljitNativeRegionExpressionPlan &expr) {
	if (TryReadNativeErrorGuardedReference(root, expr)) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::STRING_COMPRESS && root.children.size() == 1 &&
	    root.children[0] && root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::VARCHAR && GetTypeIdSize(root.physical_type) > 0) {
		expr.kind = SljitNativeRegionExpressionKind::STRING_COMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		expr.string_compress_target_size = GetTypeIdSize(root.physical_type);
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::STRING_DECOMPRESS && root.children.size() == 1 &&
	    root.children[0] && root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.return_type.id() == LogicalTypeId::VARCHAR && GetTypeIdSize(root.children[0]->physical_type) > 0) {
		expr.kind = SljitNativeRegionExpressionKind::STRING_DECOMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		expr.string_decompress_source_size = GetTypeIdSize(root.children[0]->physical_type);
		return true;
	}

	idx_t source_index;
	SljitNativeSignedIntegerWidth integral_compress_source_width;
	SljitNativeUnsignedIntegerWidth integral_compress_target_width;
	int64_t integral_compress_minimum;
	if (TryReadNativeIntegralCompress(root, integral_compress_source_width, integral_compress_target_width,
	                                  source_index, integral_compress_minimum)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.cast_source_width = integral_compress_source_width;
		expr.unsigned_cast_target_width = integral_compress_target_width;
		expr.constant = integral_compress_minimum;
		return true;
	}
	SljitNativeUnsignedIntegerWidth integral_decompress_source_width;
	SljitNativeSignedIntegerWidth integral_decompress_target_width;
	int64_t integral_decompress_minimum;
	if (TryReadNativeIntegralDecompress(root, integral_decompress_source_width, integral_decompress_target_width,
	                                    source_index, integral_decompress_minimum)) {
		expr.kind = SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.unsigned_source_width = integral_decompress_source_width;
		expr.cast_target_width = integral_decompress_target_width;
		expr.constant = integral_decompress_minimum;
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC &&
	    root.intrinsic == ExecutionExpressionIntrinsicKind::DATE_YEAR &&
	    root.return_type.id() == LogicalTypeId::BIGINT && root.children.size() == 1 && root.children[0] &&
	    root.children[0]->kind == ExecutionExpressionIRKind::REFERENCE &&
	    root.children[0]->return_type.id() == LogicalTypeId::DATE) {
		expr.kind = SljitNativeRegionExpressionKind::DATE_YEAR;
		expr.integer_kind = SljitNativeIntegerKind::INT64;
		expr.return_type = root.return_type;
		expr.source_index = root.children[0]->ref_index;
		return true;
	}

	double double_scale;
	if (TryReadNativeDecimal64ToDouble(root, source_index, double_scale)) {
		expr.kind = SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.double_constant = double_scale;
		return true;
	}

	int64_t scale_factor;
	if (TryReadNativeDecimal128ScaleUp(root, source_index, scale_factor)) {
		expr.kind = SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP;
		expr.return_type = root.return_type;
		expr.source_index = source_index;
		expr.constant = scale_factor;
		return true;
	}
	return false;
}

} // namespace duckdb

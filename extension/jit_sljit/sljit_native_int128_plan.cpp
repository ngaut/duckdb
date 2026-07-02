//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_int128_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_integer_plan.hpp"

#include "sljit_native_integer_plan_helpers.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static bool TryReadNativeInt128Value(const ExecutionExpressionIR &constant, const LogicalType &source_type,
                                     uint64_t &lower, int64_t &upper, bool &constant_is_null) {
	if (constant.kind != ExecutionExpressionIRKind::CONSTANT || !IsNativeInt128ComparableNode(constant) ||
	    constant.return_type != source_type) {
		return false;
	}
	constant_is_null = constant.constant.IsNull();
	if (constant_is_null) {
		lower = 0;
		upper = 0;
		return true;
	}
	if (constant.constant.type().InternalType() != PhysicalType::INT128) {
		return false;
	}
	auto value = constant.constant.GetValueUnsafe<hugeint_t>();
	lower = value.lower;
	upper = value.upper;
	return true;
}

static bool TryReadNativeInt128ReferenceConstant(const ExecutionExpressionIR &candidate,
                                                 const ExecutionExpressionIR &constant, idx_t &source_index,
                                                 uint64_t &lower, int64_t &upper) {
	if (candidate.kind != ExecutionExpressionIRKind::REFERENCE || !IsNativeInt128ComparableNode(candidate)) {
		return false;
	}
	bool constant_is_null;
	if (!TryReadNativeInt128Value(constant, candidate.return_type, lower, upper, constant_is_null) ||
	    constant_is_null) {
		return false;
	}
	source_index = candidate.ref_index;
	return true;
}

bool TryReadNativeInt128CompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                          idx_t &left_index, idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.right->return_type ||
	    !IsNativeInt128ComparableNode(*root.left) || !IsNativeInt128ComparableNode(*root.right) ||
	    !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

bool TryReadNativeInt128CompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                        idx_t &source_index, uint64_t &lower, int64_t &upper, bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	if (TryReadNativeInt128ReferenceConstant(*root.left, *root.right, source_index, lower, upper)) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeInt128ReferenceConstant(*root.right, *root.left, source_index, lower, upper)) {
		return false;
	}
	constant_on_left = true;
	return true;
}

} // namespace duckdb

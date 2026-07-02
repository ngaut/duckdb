//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_integer_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_integer_plan.hpp"

#include "sljit_native_integer_plan_helpers.hpp"
#include "sljit_native_util.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

bool TryGetNativeSignedIntegerWidth(const ExecutionExpressionIR &node, SljitNativeSignedIntegerWidth &width) {
	return TryGetNativeSignedIntegerWidth(node.return_type.id(), node.physical_type, width);
}

string NativeIntegerBinaryReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	auto type_name = NativeIntegerTypeName(kind);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "native:" + type_name + "-add-constant";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "native:" + type_name + "-subtract-constant";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "native:" + type_name + "-multiply-constant";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

string NativeIntegerBinaryReferenceReason(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op) {
	auto type_name = NativeIntegerTypeName(kind);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		return "native:" + type_name + "-add-references";
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		return "native:" + type_name + "-subtract-references";
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		return "native:" + type_name + "-multiply-references";
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
}

string NativeIntegerCompareReason(SljitNativeIntegerKind kind) {
	return "native:" + NativeIntegerTypeName(kind) + "-compare-constant";
}

string NativeIntegerCompareReferenceReason(SljitNativeIntegerKind kind) {
	return "native:" + NativeIntegerTypeName(kind) + "-compare-references";
}

string NativeIntegerCastReason(SljitNativeSignedIntegerWidth source_width, SljitNativeSignedIntegerWidth target_width,
                               bool try_cast) {
	return string("native:") + (try_cast ? "try-" : "") + NativeSignedIntegerTypeName(source_width) + "-to-" +
	       NativeSignedIntegerTypeName(target_width) + "-cast";
}

string NativeIntegerCoalesceReason(SljitNativeSignedIntegerWidth width) {
	return "native:" + NativeSignedIntegerTypeName(width) + "-coalesce";
}

string NativeIntegerInListReason(SljitNativeIntegerKind kind, bool not_in) {
	return "native:" + NativeIntegerTypeName(kind) + (not_in ? "-not-in-list" : "-in-list");
}

string NativeIntegerBetweenReason(SljitNativeIntegerKind kind, bool not_between) {
	return "native:" + NativeIntegerTypeName(kind) + (not_between ? "-not-between" : "-between");
}

bool TryGetNativeIntegerCompareOp(ExecutionExpressionBinaryOp op, SljitNativeIntegerCompareOp &compare_op) {
	switch (op) {
	case ExecutionExpressionBinaryOp::COMPARE_EQUAL:
		compare_op = SljitNativeIntegerCompareOp::EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL:
		compare_op = SljitNativeIntegerCompareOp::NOT_EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHAN:
		compare_op = SljitNativeIntegerCompareOp::LESS_THAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHAN:
		compare_op = SljitNativeIntegerCompareOp::GREATER_THAN;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_LESSTHANOREQUALTO:
		compare_op = SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
		return true;
	case ExecutionExpressionBinaryOp::COMPARE_GREATERTHANOREQUALTO:
		compare_op = SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
		return true;
	default:
		return false;
	}
}

bool TryReadNativeIntegerBinaryConstant(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                        SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                        bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !TryGetNativeIntegerKind(root, kind) || !root.left ||
	    !root.right || !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	SljitNativeIntegerKind candidate_kind;
	if (TryReadNativeIntegerReferenceConstant(*root.left, *root.right, candidate_kind, source_index, constant_value) &&
	    candidate_kind == kind) {
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeIntegerReferenceConstant(*root.right, *root.left, candidate_kind, source_index, constant_value) ||
	    candidate_kind != kind) {
		return false;
	}
	constant_on_left = native_op == SljitNativeIntegerBinaryOp::SUBTRACT;
	return true;
}

bool TryReadNativeIntegerBinaryReferences(const ExecutionExpressionIR &root, SljitNativeIntegerBinaryOp &native_op,
                                          SljitNativeIntegerKind &kind, idx_t &left_index, idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || !TryGetNativeIntegerKind(root, kind) || !root.left ||
	    !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.return_type ||
	    root.right->return_type != root.return_type || !TryGetNativeIntegerBinaryOp(root.binary_op, native_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

bool TryReadNativeIntegerCompareConstant(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                         SljitNativeIntegerKind &kind, idx_t &source_index, int64_t &constant_value,
                                         bool &constant_on_left) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	SljitNativeIntegerKind candidate_kind;
	if (TryReadNativeComparableReferenceConstant(*root.left, *root.right, candidate_kind, source_index,
	                                             constant_value)) {
		kind = candidate_kind;
		constant_on_left = false;
		return true;
	}
	if (!TryReadNativeComparableReferenceConstant(*root.right, *root.left, candidate_kind, source_index,
	                                              constant_value)) {
		return false;
	}
	kind = candidate_kind;
	constant_on_left = true;
	return true;
}

bool TryReadNativeIntegerCompareReferences(const ExecutionExpressionIR &root, SljitNativeIntegerCompareOp &compare_op,
                                           SljitNativeIntegerKind &kind, idx_t &left_index, idx_t &right_index) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right || root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.right->kind != ExecutionExpressionIRKind::REFERENCE || root.left->return_type != root.right->return_type ||
	    !TryGetNativeComparableIntegerKind(*root.left, kind) ||
	    !TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	left_index = root.left->ref_index;
	right_index = root.right->ref_index;
	return true;
}

bool TryReadNativeIntegerCompareNullConstant(const ExecutionExpressionIR &root) {
	if (root.kind != ExecutionExpressionIRKind::BINARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || !root.right) {
		return false;
	}
	SljitNativeIntegerCompareOp compare_op;
	if (!TryGetNativeIntegerCompareOp(root.binary_op, compare_op)) {
		return false;
	}
	auto try_read_null_compare = [](const ExecutionExpressionIR &candidate, const ExecutionExpressionIR &constant) {
		if (candidate.kind != ExecutionExpressionIRKind::REFERENCE ||
		    constant.kind != ExecutionExpressionIRKind::CONSTANT || !constant.constant.IsNull()) {
			return false;
		}
		SljitNativeIntegerKind kind;
		if (TryGetNativeComparableIntegerKind(candidate, kind)) {
			int64_t constant_value;
			bool constant_is_null;
			return TryReadNativeIntegerConstant(constant, kind, constant_value, constant_is_null) && constant_is_null;
		}
		if (IsNativeInt128ComparableNode(candidate) && IsNativeInt128ComparableNode(constant) &&
		    candidate.return_type == constant.return_type) {
			return true;
		}
		return false;
	};
	return try_read_null_compare(*root.left, *root.right) || try_read_null_compare(*root.right, *root.left);
}

bool IsNativeCompareRoot(const ExecutionExpressionIR &root) {
	SljitNativeIntegerCompareOp compare_op;
	return root.kind == ExecutionExpressionIRKind::BINARY && root.return_type.id() == LogicalTypeId::BOOLEAN &&
	       TryGetNativeIntegerCompareOp(root.binary_op, compare_op);
}

bool TryReadNativeIntegerCast(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                              SljitNativeSignedIntegerWidth &target_width, idx_t &source_index, bool &try_cast) {
	if (root.kind != ExecutionExpressionIRKind::CAST || !root.left ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.left, source_width) ||
	    !TryGetNativeSignedIntegerWidth(root, target_width)) {
		return false;
	}
	source_index = root.left->ref_index;
	try_cast = root.try_cast;
	return true;
}

bool TryReadNativeSignedToUnsignedIntegerCast(const ExecutionExpressionIR &root,
                                              SljitNativeSignedIntegerWidth &source_width,
                                              SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                              bool &try_cast) {
	if (root.kind != ExecutionExpressionIRKind::CAST || !root.left ||
	    root.left->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.left, source_width) ||
	    !TryGetNativeUnsignedIntegerWidth(root, target_width)) {
		return false;
	}
	source_index = root.left->ref_index;
	try_cast = root.try_cast;
	return true;
}

static bool TryGetNativeSignedIntegerConstant(const ExecutionExpressionIR &node, SljitNativeSignedIntegerWidth width,
                                              int64_t &constant_value, bool &constant_is_null) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}
	constant_is_null = node.constant.IsNull();
	if (constant_is_null) {
		constant_value = 0;
		return true;
	}
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		constant_value = node.constant.GetValue<int8_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT16:
		constant_value = node.constant.GetValue<int16_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT32:
		constant_value = node.constant.GetValue<int32_t>();
		return true;
	case SljitNativeSignedIntegerWidth::INT64:
		constant_value = node.constant.GetValue<int64_t>();
		return true;
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

bool TryReadNativeIntegralCompress(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &source_width,
                                   SljitNativeUnsignedIntegerWidth &target_width, idx_t &source_index,
                                   int64_t &minimum) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS || root.children.size() != 2 ||
	    !root.children[0] || !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(*root.children[0], source_width) ||
	    !TryGetNativeUnsignedIntegerWidth(root, target_width)) {
		return false;
	}
	bool minimum_is_null;
	if (!TryGetNativeSignedIntegerConstant(*root.children[1], source_width, minimum, minimum_is_null) ||
	    minimum_is_null) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	return true;
}

bool TryReadNativeIntegralDecompress(const ExecutionExpressionIR &root, SljitNativeUnsignedIntegerWidth &source_width,
                                     SljitNativeSignedIntegerWidth &target_width, idx_t &source_index,
                                     int64_t &minimum) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_DECOMPRESS || root.children.size() != 2 ||
	    !root.children[0] || !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeUnsignedIntegerWidth(*root.children[0], source_width) ||
	    !TryGetNativeSignedIntegerWidth(root, target_width)) {
		return false;
	}
	bool minimum_is_null;
	if (!TryGetNativeSignedIntegerConstant(*root.children[1], target_width, minimum, minimum_is_null) ||
	    minimum_is_null) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	return true;
}

bool TryReadNativeIntegerCoalesce(const ExecutionExpressionIR &root, SljitNativeSignedIntegerWidth &width,
                                  idx_t &source_index, SljitNativeCoalesceRhsKind &rhs_kind, idx_t &right_source_index,
                                  int64_t &constant_value, bool &constant_is_null) {
	if (root.kind != ExecutionExpressionIRKind::COALESCE || root.children.size() != 2 ||
	    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    !TryGetNativeSignedIntegerWidth(root, width) || root.children[0]->return_type != root.return_type ||
	    root.children[1]->return_type != root.return_type) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	auto &rhs = *root.children[1];
	if (TryGetNativeSignedIntegerConstant(rhs, width, constant_value, constant_is_null)) {
		rhs_kind = SljitNativeCoalesceRhsKind::CONSTANT;
		right_source_index = 0;
		return true;
	}
	if (rhs.kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	rhs_kind = SljitNativeCoalesceRhsKind::REFERENCE;
	right_source_index = rhs.ref_index;
	constant_value = 0;
	constant_is_null = false;
	return true;
}

} // namespace duckdb

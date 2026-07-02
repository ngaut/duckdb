//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_predicate_structural_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "sljit_native_integer_plan.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

string NativeNullCheckReason(SljitNativeNullCheckOp op) {
	switch (op) {
	case SljitNativeNullCheckOp::IS_NULL:
		return "native:is-null";
	case SljitNativeNullCheckOp::IS_NOT_NULL:
		return "native:is-not-null";
	default:
		throw InternalException("Unknown SLJIT native null-check operator");
	}
}

static bool TryReadNativePredicateNullGuard(const ExecutionExpressionIR &node, vector<idx_t> &source_indices,
                                            bool &has_null_constant) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		source_indices.push_back(node.ref_index);
		return true;
	}
	if (node.kind == ExecutionExpressionIRKind::CONSTANT) {
		if (node.constant.IsNull()) {
			has_null_constant = true;
		}
		return true;
	}
	return false;
}

bool TryReadNativeNullCheck(const ExecutionExpressionIR &root, SljitNativeNullCheckOp &op, idx_t &source_index) {
	if (root.kind != ExecutionExpressionIRKind::UNARY || root.return_type.id() != LogicalTypeId::BOOLEAN ||
	    !root.left || root.left->kind != ExecutionExpressionIRKind::REFERENCE) {
		return false;
	}
	switch (root.unary_op) {
	case ExecutionExpressionUnaryOp::IS_NULL:
		op = SljitNativeNullCheckOp::IS_NULL;
		break;
	case ExecutionExpressionUnaryOp::IS_NOT_NULL:
		op = SljitNativeNullCheckOp::IS_NOT_NULL;
		break;
	default:
		return false;
	}
	source_index = root.left->ref_index;
	return true;
}

bool ShouldTryNativePredicateRoot(const ExecutionExpressionIR &root) {
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (root.kind == ExecutionExpressionIRKind::CONJUNCTION) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::REFERENCE) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::CONSTANT) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		return true;
	}
	if (IsNativeCompareRoot(root)) {
		return true;
	}
	if (root.kind == ExecutionExpressionIRKind::INTRINSIC) {
		switch (root.intrinsic) {
		case ExecutionExpressionIntrinsicKind::STRING_PREFIX:
		case ExecutionExpressionIntrinsicKind::STRING_SUFFIX:
		case ExecutionExpressionIntrinsicKind::STRING_CONTAINS:
		case ExecutionExpressionIntrinsicKind::STRING_LIKE:
			return true;
		default:
			break;
		}
	}
	return false;
}

static bool TryBuildNativeConstantPredicate(const ExecutionExpressionIR &root,
                                            unique_ptr<SljitNativePredicate> &predicate) {
	if (root.kind != ExecutionExpressionIRKind::CONSTANT) {
		return false;
	}

	auto result = make_uniq<SljitNativePredicate>();
	result->return_type = root.return_type;
	result->kind = SljitNativePredicateKind::CONSTANT;
	if (root.constant.IsNull()) {
		result->constant_is_null = true;
		predicate = std::move(result);
		return true;
	}
	if (root.constant.type().id() != LogicalTypeId::BOOLEAN) {
		return false;
	}
	result->constant_value = BooleanValue::Get(root.constant);
	predicate = std::move(result);
	return true;
}

bool TryBuildNativeConstantOrNullPredicate(const ExecutionExpressionIR &root,
                                           unique_ptr<SljitNativePredicate> &predicate) {
	if (root.kind != ExecutionExpressionIRKind::CONSTANT_OR_NULL) {
		return false;
	}
	if (root.children.empty()) {
		return false;
	}

	unique_ptr<SljitNativePredicate> child;
	if (!TryBuildNativePredicate(*root.children[0], child)) {
		return false;
	}

	auto result = make_uniq<SljitNativePredicate>();
	result->return_type = root.return_type;
	result->kind = SljitNativePredicateKind::CONSTANT_OR_NULL;
	result->child = std::move(child);
	for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
		if (!TryReadNativePredicateNullGuard(*root.children[child_idx], result->guard_source_indices,
		                                     result->guard_has_null_constant)) {
			return false;
		}
	}
	predicate = std::move(result);
	return true;
}

bool TryBuildNativeConjunctionPredicate(const ExecutionExpressionIR &root,
                                        unique_ptr<SljitNativePredicate> &predicate) {
	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION) {
		return false;
	}
	if (root.children.empty()) {
		return false;
	}

	auto result = make_uniq<SljitNativePredicate>();
	result->return_type = root.return_type;
	result->kind = SljitNativePredicateKind::CONJUNCTION;
	result->conjunction_op = root.conjunction_op;
	result->children.reserve(root.children.size());
	for (auto &child_node : root.children) {
		unique_ptr<SljitNativePredicate> child;
		if (!TryBuildNativePredicate(*child_node, child)) {
			return false;
		}
		result->children.push_back(std::move(child));
	}
	predicate = std::move(result);
	return true;
}

bool TryBuildNativeBasePredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate) {
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}

	if (TryBuildNativeConstantPredicate(root, predicate)) {
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::REFERENCE) {
		auto result = make_uniq<SljitNativePredicate>();
		result->return_type = root.return_type;
		result->kind = SljitNativePredicateKind::REFERENCE;
		result->source_index = root.ref_index;
		predicate = std::move(result);
		return true;
	}

	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT &&
	    root.left) {
		unique_ptr<SljitNativePredicate> child;
		if (!TryBuildNativePredicate(*root.left, child)) {
			return false;
		}
		auto result = make_uniq<SljitNativePredicate>();
		result->return_type = root.return_type;
		result->kind = SljitNativePredicateKind::NOT;
		result->child = std::move(child);
		predicate = std::move(result);
		return true;
	}

	return false;
}

bool TryBuildNativeNullCheckPredicate(const ExecutionExpressionIR &root, unique_ptr<SljitNativePredicate> &predicate) {
	SljitNativeNullCheckOp null_check_op;
	idx_t source_index;
	if (TryReadNativeNullCheck(root, null_check_op, source_index)) {
		auto result = make_uniq<SljitNativePredicate>();
		result->return_type = root.return_type;
		result->kind = SljitNativePredicateKind::NULL_CHECK;
		result->source_index = source_index;
		result->null_check_op = null_check_op;
		predicate = std::move(result);
		return true;
	}

	return false;
}

} // namespace duckdb

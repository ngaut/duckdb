//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_integer_set_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "sljit_native_integer_plan_helpers.hpp"

namespace duckdb {

bool TryReadNativeIntegerInList(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                vector<int64_t> &constants, bool &list_has_null, bool &not_in) {
	constants.clear();
	list_has_null = false;
	not_in = root.not_in;

	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() < 2 ||
		    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
		    !TryGetNativeComparableIntegerKind(*root.children[0], kind)) {
			return false;
		}
		source_index = root.children[0]->ref_index;
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			auto &child = *root.children[child_idx];
			bool constant_is_null;
			int64_t constant_value;
			if (!TryReadNativeIntegerConstant(child, kind, constant_value, constant_is_null)) {
				return false;
			}
			if (constant_is_null) {
				list_has_null = true;
				continue;
			}
			constants.push_back(constant_value);
		}
		return true;
	}

	auto candidate = &root;
	if (root.kind == ExecutionExpressionIRKind::UNARY && root.unary_op == ExecutionExpressionUnaryOp::NOT &&
	    root.left && root.left->kind == ExecutionExpressionIRKind::CONJUNCTION &&
	    root.left->conjunction_op == ExecutionExpressionConjunctionOp::OR) {
		not_in = true;
		candidate = root.left.get();
	}
	if (candidate->kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    candidate->conjunction_op != ExecutionExpressionConjunctionOp::OR || candidate->children.empty()) {
		return false;
	}

	bool initialized = false;
	for (auto &child_ptr : candidate->children) {
		auto &child = *child_ptr;
		if (child.kind != ExecutionExpressionIRKind::BINARY ||
		    child.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL || !child.left || !child.right) {
			return false;
		}
		SljitNativeIntegerKind child_kind;
		idx_t child_source_index;
		int64_t constant_value;
		bool constant_is_null;
		if (!TryReadNativeComparableReferenceMaybeNullConstant(*child.left, *child.right, child_kind,
		                                                       child_source_index, constant_value, constant_is_null) &&
		    !TryReadNativeComparableReferenceMaybeNullConstant(*child.right, *child.left, child_kind,
		                                                       child_source_index, constant_value, constant_is_null)) {
			return false;
		}
		if (!initialized) {
			kind = child_kind;
			source_index = child_source_index;
			initialized = true;
		} else if (kind != child_kind || source_index != child_source_index) {
			return false;
		}
		if (constant_is_null) {
			list_has_null = true;
		} else {
			constants.push_back(constant_value);
		}
	}
	return initialized;
}

bool TryReadNativeIntegerBetween(const ExecutionExpressionIR &root, SljitNativeIntegerKind &kind, idx_t &source_index,
                                 int64_t &lower, int64_t &upper, bool &lower_inclusive, bool &upper_inclusive,
                                 bool &not_between) {
	if (root.kind == ExecutionExpressionIRKind::BETWEEN) {
		if (root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 3 ||
		    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
		    !TryGetNativeComparableIntegerKind(*root.children[0], kind)) {
			return false;
		}
		auto &lower_node = *root.children[1];
		auto &upper_node = *root.children[2];
		bool lower_is_null;
		bool upper_is_null;
		if (!TryReadNativeIntegerConstant(lower_node, kind, lower, lower_is_null) ||
		    !TryReadNativeIntegerConstant(upper_node, kind, upper, upper_is_null) || lower_is_null || upper_is_null) {
			return false;
		}
		source_index = root.children[0]->ref_index;
		lower_inclusive = root.lower_inclusive;
		upper_inclusive = root.upper_inclusive;
		not_between = root.not_between;
		return true;
	}

	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    root.conjunction_op != ExecutionExpressionConjunctionOp::AND ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 2) {
		return false;
	}

	bool initialized = false;
	bool has_lower = false;
	bool has_upper = false;
	for (auto &child_ptr : root.children) {
		if (!child_ptr) {
			return false;
		}
		SljitNativeIntegerCompareOp compare_op;
		SljitNativeIntegerKind child_kind;
		idx_t child_source_index;
		int64_t child_constant;
		bool constant_on_left;
		if (!TryReadNativeIntegerCompareConstant(*child_ptr, compare_op, child_kind, child_source_index, child_constant,
		                                         constant_on_left)) {
			return false;
		}
		if (!initialized) {
			kind = child_kind;
			source_index = child_source_index;
			initialized = true;
		} else if (kind != child_kind || source_index != child_source_index) {
			return false;
		}

		bool child_is_lower;
		bool child_inclusive;
		switch (compare_op) {
		case SljitNativeIntegerCompareOp::LESS_THAN:
			child_is_lower = constant_on_left;
			child_inclusive = false;
			break;
		case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
			child_is_lower = constant_on_left;
			child_inclusive = true;
			break;
		case SljitNativeIntegerCompareOp::GREATER_THAN:
			child_is_lower = !constant_on_left;
			child_inclusive = false;
			break;
		case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
			child_is_lower = !constant_on_left;
			child_inclusive = true;
			break;
		default:
			return false;
		}

		if (child_is_lower) {
			if (has_lower) {
				return false;
			}
			lower = child_constant;
			lower_inclusive = child_inclusive;
			has_lower = true;
		} else {
			if (has_upper) {
				return false;
			}
			upper = child_constant;
			upper_inclusive = child_inclusive;
			has_upper = true;
		}
	}
	not_between = false;
	return initialized && has_lower && has_upper;
}

} // namespace duckdb

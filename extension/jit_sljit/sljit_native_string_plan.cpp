//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_string_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_string_plan.hpp"

#include "duckdb/common/types/value.hpp"

namespace duckdb {

bool TryReadNativeStringPrefixConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &prefix) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    root.intrinsic != ExecutionExpressionIntrinsicKind::STRING_PREFIX ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR ||
	    root.children[1]->kind != ExecutionExpressionIRKind::CONSTANT ||
	    root.children[1]->return_type.id() != LogicalTypeId::VARCHAR || root.children[1]->constant.IsNull()) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	prefix = StringValue::Get(root.children[1]->constant);
	return true;
}

bool TryReadNativeStringMatchConstant(const ExecutionExpressionIR &root, ExecutionExpressionIntrinsicKind intrinsic,
                                      idx_t &source_index, string &constant) {
	if (root.kind != ExecutionExpressionIRKind::INTRINSIC || root.intrinsic != intrinsic ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR ||
	    root.children[1]->kind != ExecutionExpressionIRKind::CONSTANT ||
	    root.children[1]->return_type.id() != LogicalTypeId::VARCHAR || root.children[1]->constant.IsNull()) {
		return false;
	}
	source_index = root.children[0]->ref_index;
	constant = StringValue::Get(root.children[1]->constant);
	return true;
}

bool SljitNativeLikePatternIsPercentOnly(const string &pattern) {
	for (auto character : pattern) {
		if (character == '_') {
			return false;
		}
	}
	return true;
}

static bool IsNativeAsciiString(const string &value);

static bool TryReadNativeStringReferenceMaybeNullConstant(const ExecutionExpressionIR &reference,
                                                          const ExecutionExpressionIR &constant_node,
                                                          idx_t &source_index, string &constant,
                                                          bool &constant_is_null) {
	if (reference.kind != ExecutionExpressionIRKind::REFERENCE ||
	    reference.return_type.id() != LogicalTypeId::VARCHAR ||
	    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
	    constant_node.return_type.id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	source_index = reference.ref_index;
	constant.clear();
	constant_is_null = constant_node.constant.IsNull();
	if (constant_is_null) {
		return true;
	}
	constant = StringValue::Get(constant_node.constant);
	return IsNativeAsciiString(constant);
}

bool TryReadNativeStringEqualConstant(const ExecutionExpressionIR &root, idx_t &source_index, string &constant) {
	if (root.kind != ExecutionExpressionIRKind::BINARY ||
	    root.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL ||
	    root.return_type.id() != LogicalTypeId::BOOLEAN || !root.left || !root.right) {
		return false;
	}
	auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
		bool constant_is_null;
		if (!TryReadNativeStringReferenceMaybeNullConstant(reference, constant_node, source_index, constant,
		                                                   constant_is_null) ||
		    constant_is_null) {
			return false;
		}
		return true;
	};
	return try_read(*root.left, *root.right) || try_read(*root.right, *root.left);
}

bool TryReadNativeStringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                       vector<string> &constants, bool &list_has_null, bool &not_in) {
	constants.clear();
	list_has_null = false;
	not_in = false;
	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (root.return_type.id() != LogicalTypeId::BOOLEAN || root.children.size() < 2 || !root.children[0] ||
		    root.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
		    root.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		source_index = root.children[0]->ref_index;
		not_in = root.not_in;
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			auto &child = *root.children[child_idx];
			if (child.kind != ExecutionExpressionIRKind::CONSTANT || child.return_type.id() != LogicalTypeId::VARCHAR) {
				return false;
			}
			if (child.constant.IsNull()) {
				list_has_null = true;
				continue;
			}
			auto constant = StringValue::Get(child.constant);
			if (!IsNativeAsciiString(constant)) {
				return false;
			}
			constants.push_back(std::move(constant));
		}
		return !constants.empty() || list_has_null;
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
		idx_t child_source_index;
		string constant;
		bool constant_is_null;
		if (!TryReadNativeStringReferenceMaybeNullConstant(*child.left, *child.right, child_source_index, constant,
		                                                   constant_is_null) &&
		    !TryReadNativeStringReferenceMaybeNullConstant(*child.right, *child.left, child_source_index, constant,
		                                                   constant_is_null)) {
			return false;
		}
		if (!initialized) {
			source_index = child_source_index;
			initialized = true;
		} else if (source_index != child_source_index) {
			return false;
		}
		if (constant_is_null) {
			list_has_null = true;
		} else {
			constants.push_back(std::move(constant));
		}
	}
	return initialized && (!constants.empty() || list_has_null);
}

static bool TryReadNativeInt64Constant(const ExecutionExpressionIR &node, int64_t &value) {
	if (node.kind != ExecutionExpressionIRKind::CONSTANT || node.constant.IsNull() || !node.return_type.IsIntegral()) {
		return false;
	}
	Value cast_value;
	string error;
	if (!node.constant.DefaultTryCastAs(LogicalType::BIGINT, cast_value, &error) || cast_value.IsNull()) {
		return false;
	}
	value = cast_value.GetValue<int64_t>();
	return true;
}

static bool IsNativeAsciiString(const string &value) {
	for (auto character : value) {
		if (static_cast<unsigned char>(character) >= 0x80) {
			return false;
		}
	}
	return true;
}

static bool TryReadNativeSubstringReference(const ExecutionExpressionIR &substring, idx_t &source_index,
                                            idx_t &substring_length) {
	if (substring.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    substring.intrinsic != ExecutionExpressionIntrinsicKind::STRING_SUBSTRING ||
	    substring.return_type.id() != LogicalTypeId::VARCHAR || substring.children.size() != 3 ||
	    !substring.children[0] || !substring.children[1] || !substring.children[2] ||
	    substring.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    substring.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
		return false;
	}

	int64_t start;
	int64_t length;
	if (!TryReadNativeInt64Constant(*substring.children[1], start) ||
	    !TryReadNativeInt64Constant(*substring.children[2], length) || start != 1 || length < 0 ||
	    static_cast<uint64_t>(length) > NumericLimits<idx_t>::Maximum()) {
		return false;
	}

	source_index = substring.children[0]->ref_index;
	substring_length = UnsafeNumericCast<idx_t>(length);
	return true;
}

static bool TryReadNativeSubstringConstant(const ExecutionExpressionIR &substring,
                                           const ExecutionExpressionIR &constant, idx_t &source_index,
                                           idx_t &substring_length, string &value) {
	if (!TryReadNativeSubstringReference(substring, source_index, substring_length) ||
	    constant.kind != ExecutionExpressionIRKind::CONSTANT || constant.return_type.id() != LogicalTypeId::VARCHAR ||
	    constant.constant.IsNull()) {
		return false;
	}
	value = StringValue::Get(constant.constant);
	return value.size() == substring_length && IsNativeAsciiString(value);
}

static bool TryReadNativeSubstringEqualConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, string &constant) {
	if (root.kind != ExecutionExpressionIRKind::BINARY ||
	    root.binary_op != ExecutionExpressionBinaryOp::COMPARE_EQUAL || !root.left || !root.right) {
		return false;
	}
	return TryReadNativeSubstringConstant(*root.left, *root.right, source_index, substring_length, constant) ||
	       TryReadNativeSubstringConstant(*root.right, *root.left, source_index, substring_length, constant);
}

bool TryReadNativeStringSubstringInListConstant(const ExecutionExpressionIR &root, idx_t &source_index,
                                                idx_t &substring_length, vector<string> &constants) {
	constants.clear();
	if (root.return_type.id() != LogicalTypeId::BOOLEAN) {
		return false;
	}

	if (root.kind == ExecutionExpressionIRKind::IN_LIST) {
		if (root.not_in || root.children.size() < 2 || !root.children[0] ||
		    !TryReadNativeSubstringReference(*root.children[0], source_index, substring_length)) {
			return false;
		}
		for (idx_t child_idx = 1; child_idx < root.children.size(); child_idx++) {
			auto &child = *root.children[child_idx];
			if (child.kind != ExecutionExpressionIRKind::CONSTANT || child.return_type.id() != LogicalTypeId::VARCHAR ||
			    child.constant.IsNull()) {
				return false;
			}
			auto constant = StringValue::Get(child.constant);
			if (constant.size() != substring_length || !IsNativeAsciiString(constant)) {
				return false;
			}
			constants.push_back(std::move(constant));
		}
		return !constants.empty();
	}

	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    root.conjunction_op != ExecutionExpressionConjunctionOp::OR || root.children.empty()) {
		return false;
	}

	bool initialized = false;
	for (auto &child : root.children) {
		idx_t child_source_index;
		idx_t child_substring_length;
		string constant;
		if (!TryReadNativeSubstringEqualConstant(*child, child_source_index, child_substring_length, constant)) {
			return false;
		}
		if (!initialized) {
			source_index = child_source_index;
			substring_length = child_substring_length;
			initialized = true;
		} else if (source_index != child_source_index || substring_length != child_substring_length) {
			return false;
		}
		constants.push_back(std::move(constant));
	}
	return initialized && !constants.empty();
}

} // namespace duckdb

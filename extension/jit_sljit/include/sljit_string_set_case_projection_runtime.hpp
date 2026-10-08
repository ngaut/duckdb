//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_string_set_case_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_post_join_projection_fast_path.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

#include <cstring>

namespace duckdb {

static bool SljitIsIntegerConstantValue(const ExecutionExpressionIR &node, int32_t expected) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && node.return_type.id() == LogicalTypeId::INTEGER &&
	       !node.constant.IsNull() && node.constant.GetValue<int32_t>() == expected;
}

static bool SljitTryReadStringReferenceCompareConstant(const ExecutionExpressionIR &node, idx_t reference_index,
                                                       ExecutionExpressionBinaryOp expected_op, string &constant) {
	if (node.kind != ExecutionExpressionIRKind::BINARY || node.binary_op != expected_op || !node.left || !node.right) {
		return false;
	}
	auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
		if (reference.kind != ExecutionExpressionIRKind::REFERENCE || reference.ref_index != reference_index ||
		    reference.return_type.id() != LogicalTypeId::VARCHAR ||
		    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
		    constant_node.return_type.id() != LogicalTypeId::VARCHAR || constant_node.constant.IsNull()) {
			return false;
		}
		constant = StringValue::Get(constant_node.constant);
		return true;
	};
	return try_read(*node.left, *node.right) || try_read(*node.right, *node.left);
}

static bool SljitSameStringConstantSet(const std::array<string, 2> &left, const std::array<string, 2> &right) {
	return (left[0] == right[0] && left[1] == right[1]) || (left[0] == right[1] && left[1] == right[0]);
}

static bool SljitTryReadStringSetCaseExpression(const SljitExecutableRegionExpression &expr, idx_t reference_index,
                                                bool match_values, std::array<string, 2> &constants) {
	auto &plan = expr.plan;
	if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
	    plan.return_type.id() != LogicalTypeId::INTEGER) {
		return false;
	}
	auto &root = *plan.expression_tree;
	if (root.kind != ExecutionExpressionIRKind::CASE || root.children.size() != 2 || !root.children[0] ||
	    !root.children[1] || !root.else_node || !SljitIsIntegerConstantValue(*root.children[1], 1) ||
	    !SljitIsIntegerConstantValue(*root.else_node, 0)) {
		return false;
	}
	auto &predicate = *root.children[0];
	const auto expected_conjunction =
	    match_values ? ExecutionExpressionConjunctionOp::OR : ExecutionExpressionConjunctionOp::AND;
	const auto expected_compare =
	    match_values ? ExecutionExpressionBinaryOp::COMPARE_EQUAL : ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL;
	if (predicate.kind != ExecutionExpressionIRKind::CONJUNCTION || predicate.conjunction_op != expected_conjunction ||
	    predicate.children.size() != 2 || !predicate.children[0] || !predicate.children[1]) {
		return false;
	}
	return SljitTryReadStringReferenceCompareConstant(*predicate.children[0], reference_index, expected_compare,
	                                                  constants[0]) &&
	       SljitTryReadStringReferenceCompareConstant(*predicate.children[1], reference_index, expected_compare,
	                                                  constants[1]);
}

struct SljitStringConstantSignature {
	uint64_t header = 0;
	uint64_t inline_tail = 0;
	bool inlined = false;
};

static SljitStringConstantSignature SljitPrepareStringConstantSignature(const string &constant) {
	SljitStringConstantSignature result;
	string_t value(constant);
	result.header = Load<uint64_t>(const_data_ptr_cast(&value));
	result.inlined = value.IsInlined();
	if (result.inlined) {
		result.inline_tail = Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t));
	}
	return result;
}

static bool SljitStringEqualsConstantWithHeader(const string_t &value, const string &constant,
                                                const SljitStringConstantSignature &signature) {
	if (signature.inlined) {
		return Load<uint64_t>(const_data_ptr_cast(&value) + sizeof(uint64_t)) == signature.inline_tail;
	}
	return memcmp(value.GetData(), constant.data(), constant.size()) == 0;
}

static bool SljitStringEqualsConstant(const string_t &value, const string &constant,
                                      const SljitStringConstantSignature &signature) {
	return Load<uint64_t>(const_data_ptr_cast(&value)) == signature.header &&
	       SljitStringEqualsConstantWithHeader(value, constant, signature);
}

//! Two-value membership is the common direct-aggregate predicate. Load the
//! string layout header once, then retain the ordinary full comparison for
//! each compatible signature so prefix collisions preserve equality semantics.
static bool SljitStringEqualsEitherConstant(const string_t &value, const string &first_constant,
                                            const SljitStringConstantSignature &first_signature,
                                            const string &second_constant,
                                            const SljitStringConstantSignature &second_signature) {
	const auto header = Load<uint64_t>(const_data_ptr_cast(&value));
	const bool matches_first_header = header == first_signature.header;
	const bool matches_second_header = header == second_signature.header;
	if (!matches_first_header && !matches_second_header) {
		return false;
	}
	if (matches_first_header && SljitStringEqualsConstantWithHeader(value, first_constant, first_signature)) {
		return true;
	}
	return matches_second_header && SljitStringEqualsConstantWithHeader(value, second_constant, second_signature);
}

} // namespace duckdb

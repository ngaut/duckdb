//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_predicate_simd_plan.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_predicate_simd_plan.hpp"

#include "sljit_native_plan.hpp"

#include <cstdlib>

namespace duckdb {

static unique_ptr<ExecutionExpressionIR> CopySljitConjunctionRange(const ExecutionExpressionIR &root, idx_t begin,
                                                                   idx_t end) {
	D_ASSERT(root.kind == ExecutionExpressionIRKind::CONJUNCTION);
	D_ASSERT(begin < end);
	D_ASSERT(end <= root.children.size());
	if (end - begin == 1) {
		return root.children[begin]->Copy();
	}

	auto result = root.Copy();
	result->left.reset();
	result->right.reset();
	result->else_node.reset();
	result->children.clear();
	result->children.reserve(end - begin);
	for (idx_t child_idx = begin; child_idx < end; child_idx++) {
		result->children.push_back(root.children[child_idx]->Copy());
	}
	return result;
}

SljitNativePredicatePartialSimdPlan TryPlanSljitNativePredicatePartialSimd(const ExecutionExpressionIR &root,
                                                                           const vector<bool> &source_not_null) {
	SljitNativePredicatePartialSimdPlan result;
	if (getenv("DUCKDB_JIT_NO_PARTIAL_PREDICATE_SIMD")) {
		return result; // focused A/B toggle; the complete native predicate remains canonical
	}
	if (root.kind != ExecutionExpressionIRKind::CONJUNCTION ||
	    root.conjunction_op != ExecutionExpressionConjunctionOp::AND || root.children.size() < 2) {
		return result;
	}

	idx_t prefix_count = 0;
	while (prefix_count < root.children.size() &&
	       TryPlanSljitTypedExpressionTreeSimd(*root.children[prefix_count]).supported) {
		prefix_count++;
	}
	if (prefix_count == 0 || prefix_count == root.children.size()) {
		return result;
	}

	auto packed_prefix = CopySljitConjunctionRange(root, 0, prefix_count);
	auto simd = TryPlanSljitTypedExpressionTreeSimd(*packed_prefix);
	if (!SljitTypedExpressionTreeSimdHybridFilterProfitable(simd)) {
		return result;
	}

	auto residual_ir = CopySljitConjunctionRange(root, prefix_count, root.children.size());
	unique_ptr<SljitNativePredicate> residual;
	if (!TryBuildNativePredicate(*residual_ir, residual)) {
		return result;
	}
	residual->source_not_null = source_not_null;

	result.supported = true;
	result.packed_prefix = std::move(packed_prefix);
	result.residual = std::move(residual);
	result.simd = std::move(simd);
	return result;
}

} // namespace duckdb

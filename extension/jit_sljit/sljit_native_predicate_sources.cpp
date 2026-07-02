//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_predicate_sources.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_plan.hpp"

#include "duckdb/common/exception.hpp"

#include <algorithm>

namespace duckdb {

static void AppendSljitNativePredicateSourceIndex(vector<idx_t> &source_indices, idx_t source_index) {
	source_indices.push_back(source_index);
}

static void CollectSljitNativePredicateSourceIndices(const SljitNativePredicate &predicate,
                                                     vector<idx_t> &source_indices) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.source_index);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.source_index);
		AppendSljitNativePredicateSourceIndex(source_indices, predicate.right_source_index);
		return;
	case SljitNativePredicateKind::NOT:
		if (predicate.child) {
			CollectSljitNativePredicateSourceIndices(*predicate.child, source_indices);
		}
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child) {
				CollectSljitNativePredicateSourceIndices(*child, source_indices);
			}
		}
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto source_index : predicate.guard_source_indices) {
			AppendSljitNativePredicateSourceIndex(source_indices, source_index);
		}
		if (predicate.child) {
			CollectSljitNativePredicateSourceIndices(*predicate.child, source_indices);
		}
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

void FinalizeSljitNativePredicateSourceIndices(SljitNativePredicate &predicate) {
	vector<idx_t> source_indices;
	CollectSljitNativePredicateSourceIndices(predicate, source_indices);
	std::sort(source_indices.begin(), source_indices.end());
	source_indices.erase(std::unique(source_indices.begin(), source_indices.end()), source_indices.end());
	predicate.source_indices = std::move(source_indices);
}

} // namespace duckdb

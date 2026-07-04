//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_expression_inputs.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_executable_expression_codegen.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static idx_t FindSljitInputSource(const vector<idx_t> &input_sources, idx_t input_source_index) {
	for (idx_t source_idx = 0; source_idx < input_sources.size(); source_idx++) {
		if (input_sources[source_idx] == input_source_index) {
			return source_idx;
		}
	}
	return DConstants::INVALID_INDEX;
}

static idx_t AddSljitExecutableInputSource(vector<idx_t> &input_sources, idx_t input_source_index,
                                           vector<bool> *local_source_not_null = nullptr,
                                           const vector<bool> *input_not_null = nullptr) {
	auto existing_idx = FindSljitInputSource(input_sources, input_source_index);
	if (existing_idx != DConstants::INVALID_INDEX) {
		return existing_idx;
	}
	input_sources.push_back(input_source_index);
	if (local_source_not_null) {
		local_source_not_null->push_back(input_not_null && input_source_index < input_not_null->size()
		                                     ? (*input_not_null)[input_source_index]
		                                     : false);
	}
	return input_sources.size() - 1;
}

static void RemapSljitPredicateToExecutableInputs(SljitNativePredicate &predicate, vector<idx_t> &input_sources,
                                                  vector<bool> &local_source_not_null,
                                                  const vector<bool> *input_not_null) {
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
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index,
		                                                       &local_source_not_null, input_not_null);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		predicate.source_index = AddSljitExecutableInputSource(input_sources, predicate.source_index,
		                                                       &local_source_not_null, input_not_null);
		predicate.right_source_index = AddSljitExecutableInputSource(input_sources, predicate.right_source_index,
		                                                             &local_source_not_null, input_not_null);
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto &source_index : predicate.guard_source_indices) {
			source_index =
			    AddSljitExecutableInputSource(input_sources, source_index, &local_source_not_null, input_not_null);
		}
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources, local_source_not_null,
			                                      input_not_null);
		}
		return;
	case SljitNativePredicateKind::NOT:
		if (predicate.child) {
			RemapSljitPredicateToExecutableInputs(*predicate.child, input_sources, local_source_not_null,
			                                      input_not_null);
		}
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child) {
				RemapSljitPredicateToExecutableInputs(*child, input_sources, local_source_not_null, input_not_null);
			}
		}
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

static void SetDenseSljitPredicateSourceIndices(SljitNativePredicate &predicate, idx_t source_count) {
	predicate.source_indices.clear();
	predicate.source_indices.reserve(source_count);
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		predicate.source_indices.push_back(source_idx);
	}
}

static void RemapSljitConstantOrNullToExecutableInputs(SljitNativeConstantOrNull &constant_or_null,
                                                       vector<idx_t> &input_sources,
                                                       vector<bool> &local_source_not_null,
                                                       const vector<bool> *input_not_null) {
	for (auto &source_index : constant_or_null.guard_source_indices) {
		source_index =
		    AddSljitExecutableInputSource(input_sources, source_index, &local_source_not_null, input_not_null);
	}
}

static void PopulateSljitExecutableInputSourceFacts(const vector<idx_t> &input_sources,
                                                    vector<bool> &input_source_not_null,
                                                    const vector<bool> *input_not_null) {
	input_source_not_null.clear();
	input_source_not_null.reserve(input_sources.size());
	for (auto input_source : input_sources) {
		input_source_not_null.push_back(SljitSourceKnownNotNull(input_not_null, input_source));
	}
}

static void PrepareExecutableRegionExpressionInputs(SljitExecutableRegionExpression &expr,
                                                    const vector<bool> *input_not_null) {
	auto &semantic = expr.plan;
	expr.input_source_indices.clear();
	expr.input_source_not_null.clear();
	switch (semantic.kind) {
	case SljitNativeRegionExpressionKind::PREDICATE:
		if (semantic.predicate) {
			vector<bool> local_source_not_null;
			RemapSljitPredicateToExecutableInputs(*semantic.predicate, expr.input_source_indices, local_source_not_null,
			                                      input_not_null);
			SetDenseSljitPredicateSourceIndices(*semantic.predicate, expr.input_source_indices.size());
			expr.input_source_not_null = local_source_not_null;
			semantic.predicate->source_not_null = std::move(local_source_not_null);
		}
		return;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		RemapSljitConstantOrNullToExecutableInputs(semantic.constant_or_null, expr.input_source_indices,
		                                           expr.input_source_not_null, input_not_null);
		return;
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		expr.input_source_indices = semantic.expression_tree_source_indices;
		PopulateSljitExecutableInputSourceFacts(expr.input_source_indices, expr.input_source_not_null, input_not_null);
		return;
	default:
		return;
	}
}

void SljitPrepareExecutableRegionExpression(const SljitNativeRegionExpressionPlan &plan,
                                            SljitExecutableRegionExpression &expr, const vector<bool> *input_not_null,
                                            bool copy_auxiliary_expression_tree) {
	expr.plan = plan.Copy(copy_auxiliary_expression_tree, false);
	PrepareExecutableRegionExpressionInputs(expr, input_not_null);
}

} // namespace duckdb

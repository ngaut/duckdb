//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/constants.hpp"

namespace duckdb {

static bool TryReadProjectionSourceReferenceIndex(const SljitNativeRegionExpressionPlan &projection,
                                                  idx_t &source_index) {
	if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		source_index = projection.source_index;
		return true;
	}
	if (projection.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
	    projection.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		return false;
	}
	if (!projection.expression_tree || projection.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
	    projection.expression_tree->ref_index >= projection.expression_tree_source_indices.size()) {
		return false;
	}
	source_index = projection.expression_tree_source_indices[projection.expression_tree->ref_index];
	return true;
}

static bool SljitAddProjectionSourceColumn(idx_t source_index, idx_t input_column_count, vector<uint8_t> &referenced) {
	if (source_index >= input_column_count) {
		return false;
	}
	referenced[source_index] = 1;
	return true;
}

static bool SljitAddProjectionSourceColumns(const vector<idx_t> &source_indices, idx_t input_column_count,
                                            vector<uint8_t> &referenced) {
	for (auto source_index : source_indices) {
		if (!SljitAddProjectionSourceColumn(source_index, input_column_count, referenced)) {
			return false;
		}
	}
	return true;
}

template <class PREDICATE, class HANDLE_SOURCE, class HANDLE_BEFORE_SCALARS, class HANDLE_AFTER_SCALARS>
static bool SljitTryApplyProjectionPredicateSources(PREDICATE &predicate, HANDLE_SOURCE &handle_source,
                                                    HANDLE_BEFORE_SCALARS &handle_before_scalars,
                                                    HANDLE_AFTER_SCALARS &handle_after_scalars) {
	if (!handle_before_scalars(predicate)) {
		return false;
	}
	switch (predicate.kind) {
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
		if (!handle_source(predicate.source_index)) {
			return false;
		}
		break;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		if (!handle_source(predicate.source_index) || !handle_source(predicate.right_source_index)) {
			return false;
		}
		break;
	default:
		break;
	}
	if (!handle_after_scalars(predicate)) {
		return false;
	}
	if (predicate.child && !SljitTryApplyProjectionPredicateSources(*predicate.child, handle_source,
	                                                                handle_before_scalars, handle_after_scalars)) {
		return false;
	}
	for (auto &child : predicate.children) {
		if (!SljitTryApplyProjectionPredicateSources(*child, handle_source, handle_before_scalars,
		                                             handle_after_scalars)) {
			return false;
		}
	}
	return true;
}

static bool SljitAddProjectionPredicateSourceColumns(const SljitNativePredicate &predicate, idx_t input_column_count,
                                                     vector<uint8_t> &referenced) {
	auto add_source = [&](idx_t source_index) {
		return SljitAddProjectionSourceColumn(source_index, input_column_count, referenced);
	};
	auto add_source_indices = [&](const SljitNativePredicate &predicate) {
		return SljitAddProjectionSourceColumns(predicate.source_indices, input_column_count, referenced);
	};
	auto ignore_guard_sources = [&](const SljitNativePredicate &) {
		return true;
	};
	return SljitTryApplyProjectionPredicateSources(predicate, add_source, add_source_indices, ignore_guard_sources);
}

template <class PLAN, class HANDLE_SOURCE, class HANDLE_SOURCES, class HANDLE_CONSTANT, class HANDLE_PREDICATE>
static bool SljitTryApplyProjectionPlanSources(PLAN &plan, HANDLE_SOURCE &&handle_source,
                                               HANDLE_SOURCES &&handle_sources, HANDLE_CONSTANT &&handle_constant,
                                               HANDLE_PREDICATE &&handle_predicate) {
	switch (plan.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return handle_constant(plan);
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
	case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
	case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		return handle_source(plan.source_index) && handle_sources(plan.expression_tree_source_indices);
	case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
		return handle_source(plan.source_index) && handle_source(plan.guard_source_index) &&
		       handle_sources(plan.expression_tree_source_indices);
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		return handle_sources(plan.constant_or_null.guard_source_indices) &&
		       handle_sources(plan.expression_tree_source_indices);
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		return handle_source(plan.source_index) && handle_source(plan.right_source_index) &&
		       handle_sources(plan.expression_tree_source_indices);
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		if (!handle_source(plan.source_index)) {
			return false;
		}
		if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
		    !handle_source(plan.right_source_index)) {
			return false;
		}
		return handle_sources(plan.expression_tree_source_indices);
	case SljitNativeRegionExpressionKind::PREDICATE:
		return handle_predicate(plan);
	case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return handle_sources(plan.expression_tree_source_indices);
	default:
		return false;
	}
}

static bool SljitAddProjectionExpressionSourceColumns(const SljitNativeRegionExpressionPlan &plan,
                                                      idx_t input_column_count, vector<uint8_t> &referenced) {
	auto add_source = [&](idx_t source_index) {
		return SljitAddProjectionSourceColumn(source_index, input_column_count, referenced);
	};
	auto add_sources = [&](const vector<idx_t> &source_indices) {
		return SljitAddProjectionSourceColumns(source_indices, input_column_count, referenced);
	};
	auto add_constant = [&](const SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto add_predicate = [&](const SljitNativeRegionExpressionPlan &plan) {
		if (plan.predicate) {
			return SljitAddProjectionPredicateSourceColumns(*plan.predicate, input_column_count, referenced);
		}
		return add_sources(plan.expression_tree_source_indices);
	};
	return SljitTryApplyProjectionPlanSources(plan, add_source, add_sources, add_constant, add_predicate);
}

static bool SljitTryFindSingleProjectionPlanSource(const SljitNativeRegionExpressionPlan &plan, idx_t &source_index) {
	source_index = DConstants::INVALID_INDEX;
	auto capture_source = [&](idx_t candidate) {
		if (source_index == DConstants::INVALID_INDEX) {
			source_index = candidate;
			return true;
		}
		return source_index == candidate;
	};
	auto capture_sources = [&](const vector<idx_t> &candidates) {
		for (auto candidate : candidates) {
			if (!capture_source(candidate)) {
				return false;
			}
		}
		return true;
	};
	auto reject_constant = [&](const SljitNativeRegionExpressionPlan &) {
		return false;
	};
	auto reject_predicate = [&](const SljitNativeRegionExpressionPlan &) {
		return false;
	};
	if (!SljitTryApplyProjectionPlanSources(plan, capture_source, capture_sources, reject_constant, reject_predicate)) {
		return false;
	}
	return source_index != DConstants::INVALID_INDEX;
}

static bool SljitAddProjectionOutputSourceColumns(const SljitExecutableRegionOp &projection_op,
                                                  idx_t input_column_count, idx_t projection_idx,
                                                  optional_ptr<const vector<uint8_t>> skip_projection,
                                                  vector<uint8_t> &referenced) {
	if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
		return true;
	}
	if (projection_idx >= projection_op.projections.size()) {
		return false;
	}
	return SljitAddProjectionExpressionSourceColumns(projection_op.projections[projection_idx].plan, input_column_count,
	                                                 referenced);
}

static bool SljitBuildProjectionSourceColumnSet(const SljitExecutableRegionOp &projection_op, idx_t input_column_count,
                                                optional_ptr<const vector<idx_t>> output_to_projection,
                                                optional_ptr<const vector<uint8_t>> skip_projection,
                                                vector<uint8_t> &referenced) {
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || input_column_count == 0) {
		return false;
	}
	referenced.assign(input_column_count, 0);
	if (output_to_projection) {
		for (auto projection_idx : *output_to_projection) {
			if (!SljitAddProjectionOutputSourceColumns(projection_op, input_column_count, projection_idx,
			                                           skip_projection, referenced)) {
				return false;
			}
		}
		return true;
	}

	for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
		if (!SljitAddProjectionOutputSourceColumns(projection_op, input_column_count, projection_idx, skip_projection,
		                                           referenced)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryNormalizeSingleSourceProjectionPlan(SljitNativeRegionExpressionPlan &plan, idx_t source_index) {
	auto normalize_source = [&](idx_t &candidate) {
		if (candidate != source_index) {
			return false;
		}
		candidate = 0;
		return true;
	};
	auto normalize_sources = [&](vector<idx_t> &candidates) {
		for (auto &candidate : candidates) {
			if (!normalize_source(candidate)) {
				return false;
			}
		}
		return true;
	};

	auto normalize_constant = [&](SljitNativeRegionExpressionPlan &) {
		return true;
	};
	auto reject_predicate = [&](SljitNativeRegionExpressionPlan &) {
		return false;
	};
	return SljitTryApplyProjectionPlanSources(plan, normalize_source, normalize_sources, normalize_constant,
	                                          reject_predicate);
}

static void SljitBuildBorrowedProjectionExpression(const SljitExecutableRegionExpression &source,
                                                   SljitExecutableRegionExpression &target) {
	target.plan = source.plan.Copy(true, false);
	target.input_source_indices = source.input_source_indices;
	target.input_source_not_null = source.input_source_not_null;
	target.function = source.function;
	target.flat_function = source.flat_function;
	target.select_function = source.select_function;
	target.predicate_function = source.predicate_function;
	target.predicate_select_function = source.predicate_select_function;
	target.overflow_message = source.overflow_message;
}

static bool SljitTryBuildSingleSourceProjectionExpression(const SljitExecutableRegionExpression &source,
                                                          SljitExecutableRegionExpression &target,
                                                          idx_t &join_output_source_index) {
	SljitBuildBorrowedProjectionExpression(source, target);
	join_output_source_index = DConstants::INVALID_INDEX;
	if (!target.input_source_indices.empty()) {
		if (target.input_source_indices.size() != 1) {
			return false;
		}
		join_output_source_index = target.input_source_indices[0];
		target.input_source_indices[0] = 0;
		return SljitTryNormalizeSingleSourceProjectionPlan(target.plan, join_output_source_index);
	}

	if (!SljitTryFindSingleProjectionPlanSource(target.plan, join_output_source_index)) {
		return false;
	}
	return SljitTryNormalizeSingleSourceProjectionPlan(target.plan, join_output_source_index);
}

static bool SljitProjectionIsSingleSourceReferenceLike(const SljitNativeRegionExpressionPlan &plan) {
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		return plan.source_index == 0;
	}
	if (plan.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
	    plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		return false;
	}
	if (!plan.expression_tree || plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
	    plan.expression_tree->ref_index != 0) {
		return false;
	}
	return plan.expression_tree_source_indices.size() == 1 && plan.expression_tree_source_indices[0] == 0 &&
	       plan.return_type == plan.expression_tree->return_type;
}

static bool SljitTryGetSingleSourceReferenceProjectionIndex(const SljitExecutableRegionExpression &source,
                                                            idx_t &source_index) {
	SljitExecutableRegionExpression remapped_expr;
	return SljitTryBuildSingleSourceProjectionExpression(source, remapped_expr, source_index) &&
	       SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan);
}

} // namespace duckdb

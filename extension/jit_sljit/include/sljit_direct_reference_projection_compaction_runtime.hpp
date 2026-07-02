//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_reference_projection_compaction_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static bool SljitDirectReferenceProjectionAllValidSource(UnifiedVectorFormat &format, Vector &source, idx_t count) {
	source.ToUnifiedFormat(format);
	auto sel = SljitNormalizedSourceSelectionData(format);
	return SljitNormalizedSourceAllValid(format, sel, count);
}

static bool SljitTryReadDirectReferenceProjectionSource(const SljitExecutableRegionExpression &expression,
                                                        idx_t &source_index) {
	auto &plan = expression.plan;
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		source_index = plan.source_index;
		return true;
	}
	if (plan.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
	    plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		return false;
	}
	if (!plan.expression_tree || plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
	    plan.return_type != plan.expression_tree->return_type) {
		return false;
	}
	auto &source_indices =
	    expression.input_source_indices.empty() ? plan.expression_tree_source_indices : expression.input_source_indices;
	if (plan.expression_tree->ref_index >= source_indices.size()) {
		return false;
	}
	source_index = source_indices[plan.expression_tree->ref_index];
	return true;
}

template <class T>
static void SljitCopySelectedFixedValues(const_data_ptr_t source_data, const sel_t *source_sel,
                                         const SelectionVector *filter_selection, idx_t source_offset, idx_t count,
                                         data_ptr_t target_data, idx_t target_offset) {
	auto source = reinterpret_cast<const T *>(source_data);
	auto target = reinterpret_cast<T *>(target_data);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto logical_idx =
		    filter_selection ? filter_selection->get_index(source_offset + row_idx) : source_offset + row_idx;
		auto source_idx = source_sel ? source_sel[logical_idx] : logical_idx;
		target[target_offset + row_idx] = source[source_idx];
	}
}

static bool SljitCopySelectedFixedValues(const LogicalType &type, const_data_ptr_t source_data, const sel_t *source_sel,
                                         const SelectionVector *filter_selection, idx_t source_offset, idx_t count,
                                         data_ptr_t target_data, idx_t target_offset) {
	switch (GetTypeIdSize(type.InternalType())) {
	case 1:
		SljitCopySelectedFixedValues<uint8_t>(source_data, source_sel, filter_selection, source_offset, count,
		                                      target_data, target_offset);
		return true;
	case 2:
		SljitCopySelectedFixedValues<uint16_t>(source_data, source_sel, filter_selection, source_offset, count,
		                                       target_data, target_offset);
		return true;
	case 4:
		SljitCopySelectedFixedValues<uint32_t>(source_data, source_sel, filter_selection, source_offset, count,
		                                       target_data, target_offset);
		return true;
	case 8:
		SljitCopySelectedFixedValues<uint64_t>(source_data, source_sel, filter_selection, source_offset, count,
		                                       target_data, target_offset);
		return true;
	case 16:
		SljitCopySelectedFixedValues<hugeint_t>(source_data, source_sel, filter_selection, source_offset, count,
		                                        target_data, target_offset);
		return true;
	default:
		return false;
	}
}

template <class T, SljitNativeIntegerCompareOp COMPARE_OP>
struct SljitCompareNativeIntegerValues;

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::EQUAL> {
	static inline bool Operation(T left, T right) {
		return left == right;
	}
};

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::NOT_EQUAL> {
	static inline bool Operation(T left, T right) {
		return left != right;
	}
};

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::LESS_THAN> {
	static inline bool Operation(T left, T right) {
		return left < right;
	}
};

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::GREATER_THAN> {
	static inline bool Operation(T left, T right) {
		return left > right;
	}
};

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL> {
	static inline bool Operation(T left, T right) {
		return left <= right;
	}
};

template <class T>
struct SljitCompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL> {
	static inline bool Operation(T left, T right) {
		return left >= right;
	}
};

template <bool HAS_SELECTION>
static inline idx_t SljitSourceIndex(const sel_t *sel, idx_t logical_idx) {
	return HAS_SELECTION ? sel[logical_idx] : logical_idx;
}

template <class T, class PROJECT_T, SljitNativeIntegerCompareOp COMPARE_OP, bool HAS_LEFT_SEL, bool HAS_RIGHT_SEL,
          bool HAS_PROJECT_SEL>
static idx_t SljitCompactComparedReferenceValues(const_data_ptr_t left_data_p, const sel_t *left_sel,
                                                 const_data_ptr_t right_data_p, const sel_t *right_sel,
                                                 const_data_ptr_t project_data_p, const sel_t *project_sel, idx_t count,
                                                 idx_t source_offset, data_ptr_t target_data_p, idx_t target_offset) {
	auto left_data = reinterpret_cast<const T *>(left_data_p);
	auto right_data = reinterpret_cast<const T *>(right_data_p);
	auto project_data = reinterpret_cast<const PROJECT_T *>(project_data_p);
	auto target_data = reinterpret_cast<PROJECT_T *>(target_data_p);
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto logical_idx = source_offset + row_idx;
		auto left_idx = SljitSourceIndex<HAS_LEFT_SEL>(left_sel, logical_idx);
		auto right_idx = SljitSourceIndex<HAS_RIGHT_SEL>(right_sel, logical_idx);
		if (!SljitCompareNativeIntegerValues<T, COMPARE_OP>::Operation(left_data[left_idx], right_data[right_idx])) {
			continue;
		}
		auto project_idx = SljitSourceIndex<HAS_PROJECT_SEL>(project_sel, logical_idx);
		target_data[target_offset + selected_count] = project_data[project_idx];
		selected_count++;
	}
	return selected_count;
}

template <class T, class PROJECT_T, bool HAS_LEFT_SEL, bool HAS_RIGHT_SEL, bool HAS_PROJECT_SEL>
static idx_t SljitCompactComparedReferenceValuesForSelections(const_data_ptr_t left_data, const sel_t *left_sel,
                                                              const_data_ptr_t right_data, const sel_t *right_sel,
                                                              const_data_ptr_t project_data, const sel_t *project_sel,
                                                              idx_t count, idx_t source_offset,
                                                              SljitNativeIntegerCompareOp compare_op,
                                                              data_ptr_t target_data, idx_t target_offset) {
	switch (compare_op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::EQUAL, HAS_LEFT_SEL,
		                                           HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::NOT_EQUAL, HAS_LEFT_SEL,
		                                           HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::LESS_THAN, HAS_LEFT_SEL,
		                                           HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::GREATER_THAN,
		                                           HAS_LEFT_SEL, HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL,
		                                           HAS_LEFT_SEL, HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SljitCompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL,
		                                           HAS_LEFT_SEL, HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, target_data,
		    target_offset);
	default:
		throw InternalException("Unsupported SLJIT integer comparison op");
	}
}

template <class T, class PROJECT_T>
static idx_t SljitCompactComparedReferenceValuesForSelectionShape(
    const_data_ptr_t left_data, const sel_t *left_sel, const_data_ptr_t right_data, const sel_t *right_sel,
    const_data_ptr_t project_data, const sel_t *project_sel, idx_t count, idx_t source_offset,
    SljitNativeIntegerCompareOp compare_op, data_ptr_t target_data, idx_t target_offset) {
	if (left_sel) {
		if (right_sel) {
			if (project_sel) {
				return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, true, true, true>(
				    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
				    compare_op, target_data, target_offset);
			}
			return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, true, true, false>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		if (project_sel) {
			return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, true, false, true>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, true, false, false>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
	}
	if (right_sel) {
		if (project_sel) {
			return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, false, true, true>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, false, true, false>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
	}
	if (project_sel) {
		return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, false, false, true>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
	}
	return SljitCompactComparedReferenceValuesForSelections<T, PROJECT_T, false, false, false>(
	    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
	    target_data, target_offset);
}

template <class T>
static bool SljitTryCompactComparedReferenceValuesForProjectType(
    const LogicalType &project_type, const_data_ptr_t left_data, const sel_t *left_sel, const_data_ptr_t right_data,
    const sel_t *right_sel, const_data_ptr_t project_data, const sel_t *project_sel, idx_t count, idx_t source_offset,
    SljitNativeIntegerCompareOp compare_op, data_ptr_t target_data, idx_t target_offset, idx_t &selected_count) {
	switch (GetTypeIdSize(project_type.InternalType())) {
	case 1:
		selected_count = SljitCompactComparedReferenceValuesForSelectionShape<T, uint8_t>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
		return true;
	case 2:
		selected_count = SljitCompactComparedReferenceValuesForSelectionShape<T, uint16_t>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
		return true;
	case 4:
		selected_count = SljitCompactComparedReferenceValuesForSelectionShape<T, uint32_t>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
		return true;
	case 8:
		selected_count = SljitCompactComparedReferenceValuesForSelectionShape<T, uint64_t>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
		return true;
	default:
		return false;
	}
}

static bool SljitTryCompactComparedReferenceValues(SljitNativeIntegerKind compare_kind, const LogicalType &project_type,
                                                   const_data_ptr_t left_data, const sel_t *left_sel,
                                                   const_data_ptr_t right_data, const sel_t *right_sel,
                                                   const_data_ptr_t project_data, const sel_t *project_sel, idx_t count,
                                                   idx_t source_offset, SljitNativeIntegerCompareOp compare_op,
                                                   data_ptr_t target_data, idx_t target_offset, idx_t &selected_count) {
	switch (compare_kind) {
	case SljitNativeIntegerKind::INT8:
		return SljitTryCompactComparedReferenceValuesForProjectType<int8_t>(
		    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
		    compare_op, target_data, target_offset, selected_count);
	case SljitNativeIntegerKind::UINT8:
		return SljitTryCompactComparedReferenceValuesForProjectType<uint8_t>(
		    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
		    compare_op, target_data, target_offset, selected_count);
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryCompactComparedReferenceValuesForProjectType<int32_t>(
		    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
		    compare_op, target_data, target_offset, selected_count);
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SljitTryCompactComparedReferenceValuesForProjectType<int64_t>(
		    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
		    compare_op, target_data, target_offset, selected_count);
	default:
		return false;
	}
}

} // namespace duckdb

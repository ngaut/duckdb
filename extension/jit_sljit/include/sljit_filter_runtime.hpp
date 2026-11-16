//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_filter_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <limits>
#include <type_traits>

namespace duckdb {

//! The input column a predicate source slot reads. A predicate names its sources by their position in the
//! expression's input sources, as its generated selector reads them, not by input column
static idx_t SljitPredicateSourceColumn(const vector<idx_t> &input_source_indices, idx_t source_slot) {
	return source_slot < input_source_indices.size() ? input_source_indices[source_slot] : DConstants::INVALID_INDEX;
}

template <class T>
static bool SljitTryFastSelectFlatAllValidBetween(idx_t source_index, int64_t lower_value, int64_t upper_value,
                                                  bool lower_inclusive, bool upper_inclusive, bool not_between,
                                                  DataChunk &input, SelectionVector &filter_selection,
                                                  idx_t &selected_count) {
	if (not_between || source_index >= input.ColumnCount() ||
	    lower_value < static_cast<int64_t>(std::numeric_limits<T>::min()) ||
	    lower_value > static_cast<int64_t>(std::numeric_limits<T>::max()) ||
	    upper_value < static_cast<int64_t>(std::numeric_limits<T>::min()) ||
	    upper_value > static_cast<int64_t>(std::numeric_limits<T>::max())) {
		return false;
	}
	if (!lower_inclusive) {
		if (lower_value == static_cast<int64_t>(std::numeric_limits<T>::max())) {
			selected_count = 0;
			return true;
		}
		lower_value++;
	}
	if (upper_inclusive) {
		if (upper_value == static_cast<int64_t>(std::numeric_limits<T>::max())) {
			return false;
		}
		upper_value++;
	}
	if (upper_value <= lower_value) {
		selected_count = 0;
		return true;
	}

	UnifiedVectorFormat source_format;
	input.data[source_index].ToUnifiedFormat(source_format);
	if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
		return false;
	}
	if (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) {
		return false;
	}

	using UNSIGNED_T = typename std::make_unsigned<T>::type;
	const auto lower = static_cast<UNSIGNED_T>(static_cast<T>(lower_value));
	const auto upper = static_cast<UNSIGNED_T>(static_cast<T>(upper_value));
	const auto width = static_cast<UNSIGNED_T>(upper - lower);
	const auto source_data = UnifiedVectorFormat::GetData<T>(source_format);
	auto result_data = filter_selection.data();

	selected_count = 0;
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		const auto value = static_cast<UNSIGNED_T>(source_data[row_idx]);
		if (static_cast<UNSIGNED_T>(value - lower) < width) {
			result_data[selected_count++] = UnsafeNumericCast<sel_t>(row_idx);
		}
	}
	return true;
}

static bool SljitTryFastSelectFlatAllValidIntegerBetween(const SljitNativePredicate &predicate,
                                                         const vector<idx_t> &input_source_indices, DataChunk &input,
                                                         SelectionVector &filter_selection, idx_t &selected_count) {
	if (predicate.kind != SljitNativePredicateKind::INTEGER_BETWEEN) {
		return false;
	}
	const auto source_column = SljitPredicateSourceColumn(input_source_indices, predicate.source_index);
	switch (predicate.integer_kind) {
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidBetween<int32_t>(
		    source_column, predicate.lower, predicate.upper, predicate.lower_inclusive, predicate.upper_inclusive,
		    predicate.not_between, input, filter_selection, selected_count);
	default:
		return false;
	}
}

template <class T, SljitNativeIntegerCompareOp OP>
static bool SljitCompareSelectedValues(T left, T right) {
	switch (OP) {
	case SljitNativeIntegerCompareOp::EQUAL:
		return left == right;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		return left != right;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return left < right;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return left > right;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return left <= right;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return left >= right;
	default:
		return false;
	}
}

template <class T, SljitNativeIntegerCompareOp OP>
static idx_t SljitSelectFlatAllValidCompareReferencesLoop(const T *source_data, const sel_t *source_sel_data,
                                                          const T *right_source_data,
                                                          const sel_t *right_source_sel_data, idx_t count,
                                                          SelectionVector &filter_selection) {
	auto result_data = filter_selection.data();
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto left = source_data[source_sel_data ? source_sel_data[row_idx] : row_idx];
		const auto right = right_source_data[right_source_sel_data ? right_source_sel_data[row_idx] : row_idx];
		if (SljitCompareSelectedValues<T, OP>(left, right)) {
			result_data[selected_count++] = UnsafeNumericCast<sel_t>(row_idx);
		}
	}
	return selected_count;
}

template <class T>
static bool SljitTryFastSelectFlatAllValidCompareReferences(idx_t source_index, idx_t right_source_index,
                                                            SljitNativeIntegerCompareOp compare_op, DataChunk &input,
                                                            SelectionVector &filter_selection, idx_t &selected_count) {
	if (source_index >= input.ColumnCount() || right_source_index >= input.ColumnCount()) {
		return false;
	}

	UnifiedVectorFormat source_format;
	UnifiedVectorFormat right_source_format;
	input.data[source_index].ToUnifiedFormat(source_format);
	input.data[right_source_index].ToUnifiedFormat(right_source_format);
	if ((source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) ||
	    (right_source_format.validity.CanHaveNull() && !right_source_format.validity.CheckAllValid(input.size()))) {
		return false;
	}

	const auto source_data = UnifiedVectorFormat::GetData<T>(source_format);
	const auto right_source_data = UnifiedVectorFormat::GetData<T>(right_source_format);
	const auto source_sel_data = source_format.sel ? source_format.sel->data() : nullptr;
	const auto right_source_sel_data = right_source_format.sel ? right_source_format.sel->data() : nullptr;
	switch (compare_op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		selected_count = SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::EQUAL>(
		    source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		selected_count = SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::NOT_EQUAL>(
		    source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		selected_count = SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::LESS_THAN>(
		    source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		selected_count = SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::GREATER_THAN>(
		    source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		selected_count =
		    SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL>(
		        source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		selected_count =
		    SljitSelectFlatAllValidCompareReferencesLoop<T, SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL>(
		        source_data, source_sel_data, right_source_data, right_source_sel_data, input.size(), filter_selection);
		return true;
	default:
		return false;
	}
}

static bool SljitTryFastSelectFlatAllValidIntegerCompareReferences(const SljitNativePredicate &predicate,
                                                                   const vector<idx_t> &input_source_indices,
                                                                   DataChunk &input, SelectionVector &filter_selection,
                                                                   idx_t &selected_count) {
	if (predicate.kind != SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES) {
		return false;
	}
	const auto source_column = SljitPredicateSourceColumn(input_source_indices, predicate.source_index);
	const auto right_source_column = SljitPredicateSourceColumn(input_source_indices, predicate.right_source_index);
	switch (predicate.integer_kind) {
	case SljitNativeIntegerKind::INT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<int8_t>(
		    source_column, right_source_column, predicate.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::UINT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<uint8_t>(
		    source_column, right_source_column, predicate.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidCompareReferences<int32_t>(
		    source_column, right_source_column, predicate.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SljitTryFastSelectFlatAllValidCompareReferences<int64_t>(
		    source_column, right_source_column, predicate.compare_op, input, filter_selection, selected_count);
	default:
		return false;
	}
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectExpression(const SljitExecutableRegionExpression &expression, DataChunk &input,
                                   SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch,
                                   const SelectionVector *execute_sel, idx_t count,
                                   bool materialize_all_true_selection = true,
                                   optional_ptr<ExecutionRegionRuntime> runtime = nullptr) {
	auto &filter = expression.plan;
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		idx_t fast_selected_count;
		if (!execute_sel && filter.predicate &&
		    SljitTryFastSelectFlatAllValidIntegerBetween(*filter.predicate, expression.input_source_indices, input,
		                                                 filter_selection, fast_selected_count)) {
			if (runtime) {
				RecordSljitRegionBackendWork(*runtime, count);
			}
			return fast_selected_count;
		}
		if (!execute_sel && filter.predicate &&
		    SljitTryFastSelectFlatAllValidIntegerCompareReferences(*filter.predicate, expression.input_source_indices,
		                                                           input, filter_selection, fast_selected_count)) {
			if (runtime) {
				RecordSljitRegionBackendWork(*runtime, count);
			}
			return fast_selected_count;
		}
		auto native_input =
		    SljitPrepareNativePredicateInput(adapter_scratch, input, expression.input_source_indices, execute_sel,
		                                     count, nullptr, nullptr, filter_selection.data());
		if (expression.predicate_residual_dictionary.Function() && !execute_sel) {
			native_input.residual_dictionary = adapter_scratch.predicate_sources.ResidualDictionary(
			    input, expression.input_source_indices, expression.predicate_residual_dictionary_source,
			    expression.predicate_residual_dictionary.Function(), count);
		}
		SljitExecuteNativeFunction(expression.predicate_select.Function(), native_input);
		if (runtime) {
			RecordSljitRegionGeneratedWork(*runtime, SljitNativeRegionOpKind::FILTER, count);
		}
		return native_input.selected_count;
	}
	if (filter.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		if (!expression.select.Function()) {
			throw InternalException("SLJIT typed filter expression has no generated selector");
		}
		SljitNativeVectorInput native_input;
		adapter_scratch.PrepareExpressionTree(input, expression, native_input, execute_sel, count);
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.true_sel = filter_selection.data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.count = count;
		SljitExecuteNativeFunction(expression.select.Function(), native_input);
		if (runtime) {
			RecordSljitRegionGeneratedWork(*runtime, SljitNativeRegionOpKind::FILTER, count);
		}
		// A generated selector may leave an all-true identity vector unwritten.
		// Ordinary filters represent that result by count alone; direct selector
		// consumers request concrete indices here without burdening every generated
		// lane group with a runtime mode branch.
		if (materialize_all_true_selection && native_input.selected_count == count) {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				filter_selection.set_index(row_idx, execute_sel ? execute_sel->get_index(row_idx) : row_idx);
			}
		}
		return native_input.selected_count;
	}
	throw InternalException("SLJIT filter expression has no generated selector");
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectExpression(const SljitExecutableRegionExpression &expression, DataChunk &input,
                                   SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch,
                                   bool materialize_all_true_selection = true) {
	return SljitSelectExpression(expression, input, filter_selection, adapter_scratch, nullptr, input.size(),
	                             materialize_all_true_selection);
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectFilter(SljitExecutableRegionOp &op, DataChunk &input, SelectionVector &filter_selection,
                               ADAPTER_SCRATCH &adapter_scratch, const SelectionVector *execute_sel = nullptr,
                               idx_t count = DConstants::INVALID_INDEX,
                               optional_ptr<ExecutionRegionRuntime> runtime = nullptr) {
	// Every filter primitive represents selected_count == count as the unchanged
	// input view, so its selector need not write an all-true selection.
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	D_ASSERT(op.Filter());
	auto &filter = *op.Filter();
	if (filter.batch_select) {
		const auto &batch_select = *filter.batch_select;
		const auto local_source_index = batch_select.source_index;
		D_ASSERT(local_source_index < filter.expression.input_source_indices.size());
		const auto input_source_index = filter.expression.input_source_indices[local_source_index];
		D_ASSERT(input_source_index < input.ColumnCount());
		auto selected = SljitSelectNativeStringLikeBatch(batch_select, input.data[input_source_index], filter_selection,
		                                                 execute_sel, count);
		if (runtime) {
			RecordSljitRegionBackendWork(*runtime, count);
		}
		return selected;
	}
	return SljitSelectExpression(filter.expression, input, filter_selection, adapter_scratch, execute_sel, count, false,
	                             runtime);
}

} // namespace duckdb

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
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <limits>
#include <type_traits>

namespace duckdb {

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

static bool SljitTryFastSelectFlatAllValidIntegerBetween(const SljitNativeRegionExpressionPlan &filter,
                                                         DataChunk &input, SelectionVector &filter_selection,
                                                         idx_t &selected_count) {
	if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_BETWEEN) {
		return false;
	}
	switch (filter.integer_kind) {
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidBetween<int32_t>(
		    filter.source_index, filter.lower, filter.upper, filter.lower_inclusive, filter.upper_inclusive,
		    filter.not_between, input, filter_selection, selected_count);
	default:
		return false;
	}
}

static bool SljitTryFastSelectFlatAllValidIntegerBetween(const SljitNativePredicate &predicate, DataChunk &input,
                                                         SelectionVector &filter_selection, idx_t &selected_count) {
	if (predicate.kind != SljitNativePredicateKind::INTEGER_BETWEEN) {
		return false;
	}
	switch (predicate.integer_kind) {
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidBetween<int32_t>(
		    predicate.source_index, predicate.lower, predicate.upper, predicate.lower_inclusive,
		    predicate.upper_inclusive, predicate.not_between, input, filter_selection, selected_count);
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

static bool SljitTryFastSelectFlatAllValidIntegerCompareReferences(const SljitNativeRegionExpressionPlan &filter,
                                                                   DataChunk &input, SelectionVector &filter_selection,
                                                                   idx_t &selected_count) {
	if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
		return false;
	}
	switch (filter.integer_kind) {
	case SljitNativeIntegerKind::INT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<int8_t>(
		    filter.source_index, filter.right_source_index, filter.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::UINT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<uint8_t>(
		    filter.source_index, filter.right_source_index, filter.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidCompareReferences<int32_t>(
		    filter.source_index, filter.right_source_index, filter.compare_op, input, filter_selection, selected_count);
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SljitTryFastSelectFlatAllValidCompareReferences<int64_t>(
		    filter.source_index, filter.right_source_index, filter.compare_op, input, filter_selection, selected_count);
	default:
		return false;
	}
}

static bool SljitTryFastSelectFlatAllValidIntegerCompareReferences(const SljitNativePredicate &predicate,
                                                                   DataChunk &input, SelectionVector &filter_selection,
                                                                   idx_t &selected_count) {
	if (predicate.kind != SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES) {
		return false;
	}
	switch (predicate.integer_kind) {
	case SljitNativeIntegerKind::INT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<int8_t>(
		    predicate.source_index, predicate.right_source_index, predicate.compare_op, input, filter_selection,
		    selected_count);
	case SljitNativeIntegerKind::UINT8:
		return SljitTryFastSelectFlatAllValidCompareReferences<uint8_t>(
		    predicate.source_index, predicate.right_source_index, predicate.compare_op, input, filter_selection,
		    selected_count);
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidCompareReferences<int32_t>(
		    predicate.source_index, predicate.right_source_index, predicate.compare_op, input, filter_selection,
		    selected_count);
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return SljitTryFastSelectFlatAllValidCompareReferences<int64_t>(
		    predicate.source_index, predicate.right_source_index, predicate.compare_op, input, filter_selection,
		    selected_count);
	default:
		return false;
	}
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectExpression(SljitExecutableRegionExpression &expression, DataChunk &input,
                                   SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch,
                                   const SelectionVector *execute_sel, idx_t count,
                                   bool materialize_all_true_selection = true) {
	auto &filter = expression.plan;
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		idx_t fast_selected_count;
		if (!execute_sel && filter.predicate &&
		    SljitTryFastSelectFlatAllValidIntegerBetween(*filter.predicate, input, filter_selection,
		                                                 fast_selected_count)) {
			return fast_selected_count;
		}
		if (!execute_sel && filter.predicate &&
		    SljitTryFastSelectFlatAllValidIntegerCompareReferences(*filter.predicate, input, filter_selection,
		                                                           fast_selected_count)) {
			return fast_selected_count;
		}
		auto native_input =
		    SljitPrepareNativePredicateInput(adapter_scratch, input, expression.input_source_indices, execute_sel,
		                                     count, nullptr, nullptr, filter_selection.data(), nullptr);
		SljitExecuteNativeFunction(expression.predicate_select.Function(), native_input);
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
		native_input.overflow_message = expression.overflow_message.c_str();
		native_input.query_location = filter.query_location;
		native_input.count = count;
		SljitExecuteNativeFunction(expression.select.Function(), native_input);
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
	D_ASSERT(filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
	         filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
	idx_t fast_selected_count;
	if (!execute_sel &&
	    SljitTryFastSelectFlatAllValidIntegerBetween(filter, input, filter_selection, fast_selected_count)) {
		return fast_selected_count;
	}
	if (!execute_sel &&
	    SljitTryFastSelectFlatAllValidIntegerCompareReferences(filter, input, filter_selection, fast_selected_count)) {
		return fast_selected_count;
	}
	UnifiedVectorFormat source_format;
	UnifiedVectorFormat right_source_format;
	input.data[filter.source_index].ToUnifiedFormat(source_format);
	if (filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
		D_ASSERT(filter.right_source_index < input.ColumnCount());
		input.data[filter.right_source_index].ToUnifiedFormat(right_source_format);
	}

	SljitNativeVectorInput native_input;
	native_input.source_data = filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK
	                               ? nullptr
	                               : NativeIntegerSourceData(source_format, filter.integer_kind);
	native_input.right_source_data = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
	                                     ? NativeIntegerSourceData(right_source_format, filter.integer_kind)
	                                     : nullptr;
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
	native_input.right_source_sel =
	    filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES && right_source_format.sel
	        ? right_source_format.sel->data()
	        : nullptr;
	native_input.source_validity = source_format.validity.GetData();
	native_input.right_source_validity = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
	                                         ? right_source_format.validity.GetData()
	                                         : nullptr;
	native_input.constants = filter.constants.data();
	native_input.constant = filter.constant;
	native_input.result_data = nullptr;
	native_input.result_validity = nullptr;
	native_input.true_sel = filter_selection.data();
	native_input.false_sel = nullptr;
	native_input.selected_count = 0;
	native_input.overflow_message = nullptr;
	native_input.count = count;
	SljitExecuteNativeFunction(expression.select.Function(), native_input);

	return native_input.selected_count;
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectExpression(SljitExecutableRegionExpression &expression, DataChunk &input,
                                   SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch,
                                   bool materialize_all_true_selection = true) {
	return SljitSelectExpression(expression, input, filter_selection, adapter_scratch, nullptr, input.size(),
	                             materialize_all_true_selection);
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectFilter(SljitExecutableRegionOp &op, DataChunk &input, SelectionVector &filter_selection,
                               ADAPTER_SCRATCH &adapter_scratch, const SelectionVector *execute_sel = nullptr,
                               idx_t count = DConstants::INVALID_INDEX) {
	// Every filter primitive represents selected_count == count as the unchanged
	// input view, so its selector need not write an all-true selection.
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	return SljitSelectExpression(op.filter, input, filter_selection, adapter_scratch, execute_sel, count, false);
}

template <class ADAPTER_SCRATCH>
static void SljitExecuteFilter(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
                               SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch) {
	auto selected_count = SljitSelectFilter(op, input, filter_selection, adapter_scratch);
	if (selected_count == input.size()) {
		output.Reference(input);
	} else if (selected_count > 0) {
		output.Slice(input, filter_selection, selected_count);
	}
}

} // namespace duckdb

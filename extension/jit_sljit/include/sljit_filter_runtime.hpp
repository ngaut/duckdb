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

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <exception>
#include <limits>
#include <type_traits>

namespace duckdb {

template <class T>
static bool SljitTryFastSelectFlatAllValidInclusiveExclusiveBetween(const SljitNativeRegionExpressionPlan &filter,
                                                                    DataChunk &input, SelectionVector &filter_selection,
                                                                    idx_t &selected_count) {
	if (filter.lower < static_cast<int64_t>(std::numeric_limits<T>::min()) ||
	    filter.upper > static_cast<int64_t>(std::numeric_limits<T>::max())) {
		return false;
	}
	if (filter.upper <= filter.lower) {
		selected_count = 0;
		return true;
	}

	UnifiedVectorFormat source_format;
	input.data[filter.source_index].ToUnifiedFormat(source_format);
	if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
		return false;
	}
	if (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) {
		return false;
	}

	using UNSIGNED_T = typename std::make_unsigned<T>::type;
	const auto lower = static_cast<UNSIGNED_T>(static_cast<T>(filter.lower));
	const auto upper = static_cast<UNSIGNED_T>(static_cast<T>(filter.upper));
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
	if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_BETWEEN || filter.not_between ||
	    !filter.lower_inclusive || filter.upper_inclusive || filter.source_index >= input.ColumnCount()) {
		return false;
	}
	switch (filter.integer_kind) {
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return SljitTryFastSelectFlatAllValidInclusiveExclusiveBetween<int32_t>(filter, input, filter_selection,
		                                                                        selected_count);
	default:
		return false;
	}
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectExpression(SljitExecutableRegionExpression &expression, DataChunk &input,
                                   SelectionVector &filter_selection, ADAPTER_SCRATCH &adapter_scratch) {
	auto &filter = expression.plan;
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		auto &predicate_sources = adapter_scratch.predicate_sources;
		predicate_sources.Prepare(&input, expression.input_source_indices);

		SljitNativePredicateInput native_input;
		native_input.source_data = predicate_sources.DataArray();
		native_input.source_sel = predicate_sources.SelectionArray();
		native_input.source_validity = predicate_sources.ValidityArray();
		native_input.sources_all_valid = predicate_sources.SourcesAllValid();
		native_input.execute_sel = nullptr;
		native_input.result_data = nullptr;
		native_input.result_validity = nullptr;
		native_input.true_sel = filter_selection.data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.count = input.size();
		expression.predicate_select_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return native_input.selected_count;
	}
	if (filter.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		if (!expression.select_function) {
			throw InternalException("SLJIT typed filter expression has no generated selector");
		}
		SljitNativeVectorInput native_input;
		adapter_scratch.PrepareExpressionTree(input, expression, native_input, nullptr, input.size());
		native_input.execute_sel = nullptr;
		native_input.true_sel = filter_selection.data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.overflow_message = expression.overflow_message.c_str();
		native_input.query_location = filter.query_location;
		native_input.count = input.size();
		expression.select_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return native_input.selected_count;
	}
	D_ASSERT(filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
	         filter.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
	         filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
	idx_t fast_selected_count;
	if (SljitTryFastSelectFlatAllValidIntegerBetween(filter, input, filter_selection, fast_selected_count)) {
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
	native_input.execute_sel = nullptr;
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
	native_input.count = input.size();
	expression.select_function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}

	return native_input.selected_count;
}

template <class ADAPTER_SCRATCH>
static idx_t SljitSelectFilter(SljitExecutableRegionOp &op, DataChunk &input, SelectionVector &filter_selection,
                               ADAPTER_SCRATCH &adapter_scratch) {
	return SljitSelectExpression(op.filter, input, filter_selection, adapter_scratch);
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

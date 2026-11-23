//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_expression_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_runtime.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar/string_common.hpp"

namespace duckdb {

//! Sizes a flat result the expression wrote; a result whose rows all turned out valid drops its validity mask, so the
//! consumers (generated loops, native operators) take their paths for vectors without NULLs
static void SljitFinishFlatProjectionResult(Vector &result, idx_t count) {
	auto &validity = FlatVector::ValidityMutable(result);
	if (validity.CheckAllValid(count)) {
		validity.Reset(count);
	}
	FlatVector::SetSize(result, count_t(count));
}

template <class ADAPTER_SCRATCH>
static bool SljitTryExecuteProjectionExpressionOverDictionary(SljitExecutableRegionExpression &expr, DataChunk &input,
                                                              Vector &result, const SelectionVector *execute_sel,
                                                              idx_t count, ADAPTER_SCRATCH &adapter_scratch,
                                                              optional_ptr<ExecutionRegionRuntime> runtime);

template <class ADAPTER_SCRATCH>
static void SljitExecuteProjectionExpression(SljitExecutableRegionExpression &expr, DataChunk &input, Vector &result,
                                             const SelectionVector *execute_sel, idx_t count,
                                             ADAPTER_SCRATCH &adapter_scratch,
                                             optional_ptr<ExecutionRegionRuntime> runtime) {
	auto &plan = expr.plan;
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		D_ASSERT(plan.source_index < input.ColumnCount());
		if (execute_sel) {
			result.Slice(input.data[plan.source_index], *execute_sel, count);
		} else {
			result.Reference(input.data[plan.source_index]);
		}
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT) {
		result.Reference(plan.constant_value, count_t(count));
		result.Flatten();
		FlatVector::SetSize(result, count_t(count));
		return;
	}
	if (SljitTryExecuteProjectionExpressionOverDictionary(expr, input, result, execute_sel, count, adapter_scratch,
	                                                      runtime)) {
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);

		auto native_input = SljitPrepareNativePredicateInput(
		    adapter_scratch, input, expr.input_source_indices, execute_sel, count,
		    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result)), result_validity.GetData(), nullptr);
		SljitExecuteNativeFunction(expr.predicate.Function(), native_input);
		if (runtime) {
			RecordSljitRegionGeneratedWork(*runtime, SljitNativeRegionOpKind::PROJECTION, count);
		}
		SljitFinishFlatProjectionResult(result, count);
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		SljitNativeVectorInput native_input;
		adapter_scratch.PrepareExpressionTree(input, expr, native_input, execute_sel, count);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);
		validity_t *result_validity_data = result_validity.GetData();

		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		// The generated tree stores its root in the root's physical layout: a narrow DECIMAL128 root is sign-extended
		// into hugeint elements, a DOUBLE root stores its bits
		native_input.result_data = FlatVector::GetDataMutable(result);
		native_input.result_validity = result_validity_data;
		native_input.count = count;
		SljitExecuteNativeFunction(expr.vector.Function(), native_input);
		if (runtime) {
			RecordSljitRegionGeneratedWork(*runtime, SljitNativeRegionOpKind::PROJECTION, count);
		}
		SljitFinishFlatProjectionResult(result, count);
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE) {
		UnifiedVectorFormat source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);

		auto source_data = UnifiedVectorFormat::GetData<int64_t>(source_format);
		auto result_data = FlatVector::GetDataMutable<double>(result);
		// DuckDB's cast: a value exact in a double divides by the exact 10^scale, any other takes the exact conversion
		const auto scale = plan.double_source_decimal_scale;
		const auto factor = NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
		const int64_t exact_limit = int64_t(1) << 53;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
			if (!source_format.validity.RowIsValid(source_idx)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			const auto value = source_data[source_idx];
			result_data[row_idx] = scale == 0 || (value >= -exact_limit && value <= exact_limit)
			                           ? double(value) / factor
			                           : SljitNativeDecimalToDouble(value, scale);
		}
		SljitFinishFlatProjectionResult(result, count);
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP) {
		UnifiedVectorFormat source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);

		auto source_data = UnifiedVectorFormat::GetData<hugeint_t>(source_format);
		auto result_data = FlatVector::GetDataMutable<hugeint_t>(result);
		hugeint_t scale_factor(plan.constant);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
			if (!source_format.validity.RowIsValid(source_idx)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			hugeint_t scaled;
			if (!Hugeint::TryMultiply(source_data[source_idx], scale_factor, scaled)) {
				throw OutOfRangeException("Overflow in DECIMAL128 scale-up");
			}
			result_data[row_idx] = scaled;
		}
		SljitFinishFlatProjectionResult(result, count);
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::STRING_SUBSTRING) {
		UnifiedVectorFormat source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);
		StringVector::AddHeapReference(result, input.data[plan.source_index]);

		auto source_data = UnifiedVectorFormat::GetData<string_t>(source_format);
		auto result_data = FlatVector::GetDataMutable<string_t>(result);
		const auto length = plan.string_substring_length;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
			if (!source_format.validity.RowIsValid(source_idx)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			result_data[row_idx] = SubstringPrefixUnicode(source_data[source_idx], length);
		}
		SljitFinishFlatProjectionResult(result, count);
		return;
	}
	D_ASSERT(plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
	         plan.kind == SljitNativeRegionExpressionKind::CONSTANT ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
	         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
	         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
	         plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
	         plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE ||
	         plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP ||
	         plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
	         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS ||
	         plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	         plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
	         plan.kind == SljitNativeRegionExpressionKind::STRING_SUBSTRING ||
	         plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
	         plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
	         plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
	UnifiedVectorFormat source_format;
	UnifiedVectorFormat right_source_format;
	input.data[plan.source_index].ToUnifiedFormat(source_format);
	auto has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
	                        plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
	                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
	                        plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY ||
	                        plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
	                        (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
	                         plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE);
	if (has_right_source) {
		auto right_source_index = plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE
		                              ? plan.guard_source_index
		                              : plan.right_source_index;
		D_ASSERT(right_source_index < input.ColumnCount());
		input.data[right_source_index].ToUnifiedFormat(right_source_format);
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.Reset(count);
	validity_t *result_validity_data = nullptr;
	if ((plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST && plan.list_has_null) ||
	    (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && plan.try_cast) ||
	    (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST && plan.try_cast) ||
	    plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
	    (plan.kind != SljitNativeRegionExpressionKind::NULL_CHECK &&
	     (source_format.validity.CanHaveNull() || (has_right_source && right_source_format.validity.CanHaveNull())))) {
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);
		result_validity_data = result_validity.GetData();
	}

	SljitNativeVectorInput native_input;
	if (plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK) {
		native_input.source_data = nullptr;
	} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
		native_input.source_data = source_format.data;
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
		native_input.source_data = NativeUnsignedIntegerSourceData(source_format, plan.unsigned_source_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
	           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
	           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
	           plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY) {
		native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.cast_source_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
		native_input.source_data = NativeIntegerSourceData(source_format, SljitNativeIntegerKind::INT32);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
	           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
		native_input.source_data = source_format.data;
	} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
		native_input.source_data = source_format.data;
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
		native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.signed_integer_width);
	} else {
		native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
	}
	if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
	    plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
		native_input.right_source_data = NativeSignedIntegerSourceData(right_source_format, plan.signed_integer_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
		native_input.right_source_data = right_source_format.data;
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
	           plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
		native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY) {
		native_input.right_source_data =
		    NativeSignedIntegerSourceData(right_source_format, plan.right_cast_source_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
		native_input.right_source_data = NativeIntegerSourceData(right_source_format, SljitNativeIntegerKind::INT64);
	} else {
		native_input.right_source_data = nullptr;
	}
	native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
	native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
	native_input.right_source_sel =
	    has_right_source && right_source_format.sel ? right_source_format.sel->data() : nullptr;
	native_input.source_validity = source_format.validity.GetData();
	native_input.right_source_validity = has_right_source ? right_source_format.validity.GetData() : nullptr;
	native_input.constants = plan.constants.data();
	native_input.constant =
	    plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ? plan.guard_constant : plan.constant;
	native_input.double_constant = plan.double_constant;
	native_input.source_decimal = SljitNativeDecimalSourceScaleFor(plan.double_source_decimal_scale);
	native_input.right_source_decimal = SljitNativeDecimalSourceScaleFor(plan.double_right_source_decimal_scale);
	if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
		native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST) {
		native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
		native_input.result_data = NativeSignedIntegerResultData(result, plan.signed_integer_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
		native_input.result_data = FlatVector::GetDataMutable(result);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS) {
		native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
		native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
		native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
	} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
	           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
		if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<float>(result));
		} else {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
		}
	} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
		native_input.result_data = FlatVector::GetDataMutable(result);
	} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
	           plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
		native_input.result_data = NativeIntegerResultData(result, plan.integer_kind);
	} else if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_WIDENING_MULTIPLY) {
		native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<hugeint_t>(result));
	} else {
		native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
	}
	native_input.result_vector = plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ? &result : nullptr;
	native_input.result_validity = result_validity_data;
	native_input.true_sel = nullptr;
	native_input.false_sel = nullptr;
	native_input.selected_count = 0;
	native_input.error_message =
	    plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ? plan.error_message.c_str() : nullptr;
	native_input.cast_chain = {&plan.cast_chain, runtime.get()};
	native_input.overflow_value = 0;
	native_input.string_decompress_source_size = plan.string_decompress_source_size;
	native_input.active_source_index = 0;
	native_input.active_result_index = 0;
	native_input.count = count;
	native_input.has_error = false;
	auto use_flat_function = expr.flat.Function() && !execute_sel &&
	                         SljitUnifiedFormatHasIdentitySelection(source_format) &&
	                         source_format.validity.CannotHaveNull() &&
	                         (!has_right_source || (SljitUnifiedFormatHasIdentitySelection(right_source_format) &&
	                                                right_source_format.validity.CannotHaveNull()));
	auto vector_function = use_flat_function ? expr.flat.Function() : expr.vector.Function();
	SljitExecuteNativeFunction(vector_function, native_input);
	if (runtime) {
		RecordSljitRegionGeneratedWork(*runtime, SljitNativeRegionOpKind::PROJECTION, count);
	}
	SljitFinishFlatProjectionResult(result, count);
}

//! The input column an executable expression reads when it reads exactly one
static bool SljitTryFindSingleExecutableExpressionSource(const SljitExecutableRegionExpression &expr,
                                                         idx_t &source_index) {
	if (expr.input_source_indices.empty()) {
		return SljitTryFindSingleProjectionPlanSource(expr.plan, source_index);
	}
	source_index = expr.input_source_indices[0];
	for (auto input_index : expr.input_source_indices) {
		if (input_index != source_index) {
			return false;
		}
	}
	return true;
}

//! Like DuckDB's dictionary function execution: an expression that cannot throw and reads one storage-dictionary
//! column runs once per dictionary entry, and its result stays a dictionary over the input's selection, which the
//! consumers (dictionary group lookups, dictionary filters) keep exploiting.
template <class ADAPTER_SCRATCH>
static bool SljitTryExecuteProjectionExpressionOverDictionary(SljitExecutableRegionExpression &expr, DataChunk &input,
                                                              Vector &result, const SelectionVector *execute_sel,
                                                              idx_t count, ADAPTER_SCRATCH &adapter_scratch,
                                                              optional_ptr<ExecutionRegionRuntime> runtime) {
	static constexpr idx_t MAX_DICTIONARY_SIZE = 20000;
	idx_t source_index;
	if (!expr.plan.cannot_throw || result.GetType().InternalType() == PhysicalType::STRUCT ||
	    !SljitTryFindSingleExecutableExpressionSource(expr, source_index) || source_index >= input.ColumnCount()) {
		return false;
	}
	auto &source = input.data[source_index];
	if (source.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return false;
	}
	const auto dictionary_size = DictionaryVector::DictionarySize(source);
	const auto &dictionary_id = DictionaryVector::DictionaryId(source);
	if (!dictionary_size.IsValid() || dictionary_id.empty() || dictionary_size.GetIndex() >= MAX_DICTIONARY_SIZE) {
		return false;
	}
	const auto entry_count = dictionary_size.GetIndex();
	if (!adapter_scratch.dictionary_result || adapter_scratch.dictionary_result_source_id != dictionary_id) {
		// a dictionary larger than a vector pays off only for full batches, which selective filters do not deliver
		if (entry_count > STANDARD_VECTOR_SIZE && count * 2 <= STANDARD_VECTOR_SIZE) {
			return false;
		}
		auto dictionary = DictionaryVector::CreateReusableDictionary(result.GetType(), entry_count);
		DataChunk entries;
		entries.InitializeEmpty(input.GetTypes());
		for (idx_t offset = 0; offset < entry_count; offset += STANDARD_VECTOR_SIZE) {
			const auto entry_batch = MinValue<idx_t>(entry_count - offset, STANDARD_VECTOR_SIZE);
			Vector entry_source(DictionaryVector::Child(source), offset, offset + entry_batch);
			entries.data[source_index].Reference(entry_source);
			entries.SetChildCardinality(entry_batch);
			Vector entry_result(result.GetType());
			SljitExecuteProjectionExpression(expr, entries, entry_result, nullptr, entry_batch, adapter_scratch,
			                                 runtime);
			VectorOperations::Copy(entry_result, dictionary->data, entry_batch, 0, offset);
		}
		adapter_scratch.dictionary_result = std::move(dictionary);
		adapter_scratch.dictionary_result_source_id = dictionary_id;
	}
	if (runtime) {
		RecordSljitRegionRuntimePath(*runtime, SljitNativeRegionOpKind::PROJECTION, "dictionary_expression", count);
	}
	auto &source_selection = DictionaryVector::SelVector(source);
	if (!execute_sel) {
		result.Dictionary(adapter_scratch.dictionary_result, source_selection, count);
		return true;
	}
	SelectionVector selection(count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		selection.set_index(row_idx, source_selection.get_index(execute_sel->get_index(row_idx)));
	}
	result.Dictionary(adapter_scratch.dictionary_result, selection, count);
	return true;
}

} // namespace duckdb

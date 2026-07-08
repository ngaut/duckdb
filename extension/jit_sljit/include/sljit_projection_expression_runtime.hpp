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

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/function/scalar/string_common.hpp"

namespace duckdb {

template <class ADAPTER_SCRATCH>
static void SljitExecuteProjectionExpression(SljitExecutableRegionExpression &expr, DataChunk &input, Vector &result,
                                             const SelectionVector *execute_sel, idx_t count,
                                             ADAPTER_SCRATCH &adapter_scratch) {
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
	if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);

		auto native_input =
		    SljitPrepareNativePredicateInput(adapter_scratch, input, expr.input_source_indices, execute_sel, count,
		                                     reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result)),
		                                     result_validity.GetData(), nullptr, nullptr);
		SljitExecuteNativeFunction(expr.predicate_function, native_input);
		FlatVector::SetSize(result, count_t(count));
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
		auto constant = plan.constant_or_null.guard_has_null_constant || plan.constant_or_null.constant.IsNull()
		                    ? Value(plan.return_type)
		                    : plan.constant_or_null.constant;
		result.Reference(constant, count_t(count));
		result.Flatten();
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.EnsureWritable();
		if (!constant.IsNull()) {
			result_validity.SetAllValid(count);
			auto native_input =
			    SljitPrepareNativePredicateInput(adapter_scratch, input, expr.input_source_indices, execute_sel, count,
			                                     nullptr, result_validity.GetData(), nullptr, nullptr);
			SljitExecuteNativeFunction(expr.predicate_function, native_input);
		}
		FlatVector::SetSize(result, count_t(count));
		return;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
	    plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		SljitNativeVectorInput native_input;
		adapter_scratch.PrepareExpressionTree(input, expr, native_input, execute_sel, count);
		auto result_kind = plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE
		                       ? SljitNativeIntegerKind::DECIMAL64
		                       : plan.integer_kind;

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		validity_t *result_validity_data = nullptr;
		if (adapter_scratch.source_can_have_null ||
		    plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);
			result_validity_data = result_validity.GetData();
		}

		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.result_data = NativeIntegerResultData(result, result_kind);
		native_input.result_validity = result_validity_data;
		native_input.overflow_message = expr.overflow_message.c_str();
		native_input.query_location = plan.query_location;
		native_input.count = count;
		SljitExecuteNativeFunction(expr.function, native_input);
		FlatVector::SetSize(result, count_t(count));
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
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
			if (!source_format.validity.RowIsValid(source_idx)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			result_data[row_idx] = static_cast<double>(source_data[source_idx]) / plan.double_constant;
		}
		FlatVector::SetSize(result, count_t(count));
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
		FlatVector::SetSize(result, count_t(count));
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
		FlatVector::SetSize(result, count_t(count));
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
	           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
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
	native_input.source_double_scale = plan.double_source_scale;
	native_input.right_source_double_scale = plan.double_right_source_scale;
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
	} else {
		native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
	}
	native_input.result_vector = plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ? &result : nullptr;
	native_input.result_validity = result_validity_data;
	native_input.true_sel = nullptr;
	native_input.false_sel = nullptr;
	native_input.selected_count = 0;
	native_input.overflow_message =
	    plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
	            plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
	            plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
	            plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
	        ? expr.overflow_message.c_str()
	        : nullptr;
	native_input.error_message =
	    plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ? plan.error_message.c_str() : nullptr;
	native_input.query_location = plan.query_location;
	native_input.overflow_value = 0;
	native_input.string_decompress_source_size = plan.string_decompress_source_size;
	native_input.active_source_index = 0;
	native_input.active_result_index = 0;
	native_input.count = count;
	native_input.has_error = false;
	auto use_flat_function = expr.flat_function && !execute_sel &&
	                         SljitUnifiedFormatHasIdentitySelection(source_format) &&
	                         source_format.validity.CannotHaveNull() &&
	                         (!has_right_source || (SljitUnifiedFormatHasIdentitySelection(right_source_format) &&
	                                                right_source_format.validity.CannotHaveNull()));
	auto vector_function = use_flat_function ? expr.flat_function : expr.function;
	SljitExecuteNativeFunction(vector_function, native_input);
	FlatVector::SetSize(result, count_t(count));
}

} // namespace duckdb

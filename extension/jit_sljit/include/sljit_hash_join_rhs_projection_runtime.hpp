//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_rhs_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_projection_batch_runtime.hpp"
#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_hash_join_rhs_fixed_column_runtime.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_inline_string_decompress_projection_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool SljitReferenceProjectionTypesMatch(const LogicalType &source_type, const LogicalType &target_type) {
	if (source_type == target_type) {
		return true;
	}
	return source_type.InternalType() == PhysicalType::VARCHAR && target_type.InternalType() == PhysicalType::VARCHAR;
}

static bool SljitTryGatherHashJoinRHSReferenceProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                                const SljitNativeRegionExpressionPlan &plan,
                                                                idx_t rhs_col_idx, Vector &row_pointers, Vector &target,
                                                                idx_t current_size, idx_t count) {
	if (!SljitProjectionIsSingleSourceReferenceLike(plan) ||
	    !SljitReferenceProjectionTypesMatch(plan.return_type, target.GetType())) {
		return false;
	}
	data_ptr_t target_data = nullptr;
	unique_ptr<Vector> owned_result;
	if (DirectAppendSupportsFixedSizeType(target.GetType())) {
		target_data =
		    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
		owned_result = make_uniq<Vector>(target.GetType(), target_data, count);
	} else if (target.GetType().id() == LogicalTypeId::VARCHAR) {
		owned_result = make_uniq<Vector>(target.GetType());
	} else {
		return false;
	}
	auto &result = *owned_result;
	SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, result);
	SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
	return true;
}

static bool SljitTryMaterializeHashJoinRHSInlineStringDecompressProjectionToBatch(
    const ExecutionHashJoinProbeBinding &binding, const SljitNativeRegionExpressionPlan &plan, idx_t rhs_col_idx,
    Vector &row_pointers, Vector &target, idx_t current_size, idx_t count) {
	if (plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS || plan.source_index != 0 ||
	    plan.string_decompress_source_size != sizeof(uhugeint_t) || plan.return_type != target.GetType() ||
	    target.GetType().id() != LogicalTypeId::VARCHAR || target.GetVectorType() != VectorType::FLAT_VECTOR ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    FlatVector::GetCapacity(target) < current_size + count) {
		return false;
	}

	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
	    rhs_source.physical_type != PhysicalType::UINT128 ||
	    rhs_source.storage_kind != ExecutionHashJoinRHSFixedColumnStorageKind::ROW ||
	    rhs_source.layout_offset == DConstants::INVALID_INDEX) {
		return false;
	}

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	auto target_data = FlatVector::GetDataMutable<string_t>(target);
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.EnsureWritable();
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto target_idx = current_size + row_idx;
		auto row_location = row_pointer_data[row_idx];
		if (!SljitHashJoinRHSFixedColumnSourceIsValid(row_location, rhs_source)) {
			target_validity.SetInvalid(target_idx);
			continue;
		}
		target_validity.SetValid(target_idx);
		if (!SljitTryDecodeInlineCompressedString16Value(Load<uhugeint_t>(row_location + rhs_source.layout_offset),
		                                                 target_data[target_idx])) {
			return false;
		}
	}
	return true;
}

static bool SljitTryMaterializeBoundHashJoinRHSProjectionToBatch(
    ExecutionRegionRuntime &runtime, const ExecutionHashJoinProbeBinding &binding,
    const SljitJoinProjectionAggregateInputSource &source, const SelectionVector &build_selection, Vector &row_pointers,
    Vector &target, idx_t current_size, idx_t count, bool &used_row_pointer_generated_source) {
	used_row_pointer_generated_source = false;
	if (!source.IsBuildInput() || !source.expression || source.input_idx >= binding.rhs_output_column_count) {
		return false;
	}
	auto &expression = *source.expression;
	const auto rhs_idx = source.input_idx;
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT) {
			return SljitTryGatherHashJoinRHSReferenceProjectionToBatch(binding, expression.plan, rhs_idx, row_pointers,
			                                                           target, current_size, count);
		}
		if (SljitTryMaterializeHashJoinRHSInlineStringDecompressProjectionToBatch(
		        binding, expression.plan, rhs_idx, row_pointers, target, current_size, count)) {
			used_row_pointer_generated_source = true;
			return true;
		}
	} else if (binding.layout_kind != ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		return false;
	}
	const auto output_idx = binding.lhs_output_column_indices.size() + rhs_idx;
	if (output_idx >= binding.output_types.size()) {
		return false;
	}
	const auto &type = binding.output_types[output_idx];
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		if (!binding.perfect_layout.ready || rhs_idx >= binding.perfect_layout.rhs_dictionary_buffers.size() ||
		    rhs_idx >= binding.perfect_layout.rhs_output_types.size() ||
		    type != binding.perfect_layout.rhs_output_types[rhs_idx]) {
			return false;
		}
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_RHS_INPUT) {
			if (current_size != 0 || target.GetType() != type) {
				return false;
			}
			target.Dictionary(ExecutionHashJoinPerfectRHSDictionary(binding, rhs_idx), build_selection, count);
			return true;
		}
	}
	Vector value(type);
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		value.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_idx], build_selection, count);
	} else {
		SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_idx, value);
	}
	DataChunk input;
	input.InitializeEmpty(vector<LogicalType> {type});
	input.data[0].Reference(value);
	input.SetChildCardinality(count);
	D_ASSERT(source.adapter);
	return SljitTryExecuteProjectionExpressionToBatch(expression, input, target, current_size, count, nullptr,
	                                                  *source.adapter, &runtime);
}

static bool SljitTryMaterializeBoundHashJoinMixedProjectionToBatch(
    ExecutionRegionRuntime &runtime, const ExecutionHashJoinProbeBinding &binding,
    const SljitJoinProjectionAggregateInputSource &source, DataChunk &join_input,
    const SelectionVector &match_selection, Vector &row_pointers, Vector &target, idx_t current_size, idx_t count,
    SljitExpressionAdapterScratch &adapter_scratch) {
	if (!source.expression || source.expression_sources.empty() ||
	    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
		return false;
	}
	vector<LogicalType> types;
	for (const auto &input : source.expression_sources) {
		types.push_back(input.type);
	}
	DataChunk input;
	input.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t column_idx = 0; column_idx < source.expression_sources.size(); column_idx++) {
		const auto &column = source.expression_sources[column_idx];
		if (column.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT) {
			if (column.input_idx >= join_input.ColumnCount() ||
			    join_input.data[column.input_idx].GetType() != column.type) {
				return false;
			}
			input.data[column_idx].Slice(join_input.data[column.input_idx], match_selection, count);
		} else {
			SljitGatherHashJoinRHSColumn(binding, row_pointers, count, column.input_idx, input.data[column_idx]);
		}
	}
	input.SetChildCardinality(count);
	return SljitTryExecuteProjectionExpressionToBatch(*source.expression, input, target, current_size, count, nullptr,
	                                                  adapter_scratch, &runtime);
}

} // namespace duckdb

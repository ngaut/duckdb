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
#include "sljit_inline_string_decompress_projection_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/execution/join_hashtable.hpp"

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

static bool SljitHashJoinRHSFixedColumnSourceIsValid(data_ptr_t row_pointer,
                                                     const ExecutionHashJoinRHSFixedColumnSource &source) {
	if (!row_pointer) {
		return false;
	}
	if (source.all_valid) {
		return true;
	}
	if (source.layout_column_idx == DConstants::INVALID_INDEX || source.layout_column_count == 0) {
		return false;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	JoinHashTable::ValidityBytes::GetEntryIndex(source.layout_column_idx, entry_idx, idx_in_entry);
	return JoinHashTable::ValidityBytes::RowIsValid(
	    JoinHashTable::ValidityBytes(row_pointer, source.layout_column_count).GetValidityEntryUnsafe(entry_idx),
	    idx_in_entry);
}

static bool SljitDateDaysAreFinite(int32_t days) {
	return days != date_t::infinity().days && days != date_t::ninfinity().days;
}

static int64_t SljitExtractFiniteDateYear(int32_t days) {
	int32_t year = Date::EPOCH_YEAR;
	while (days < 0) {
		days += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (days >= Date::DAYS_PER_YEAR_INTERVAL) {
		days -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	auto year_offset = days / 365;
	while (days < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	return year + year_offset;
}

static bool SljitTryMaterializeHashJoinRHSDateYearProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                                    const SljitNativeRegionExpressionPlan &plan,
                                                                    idx_t rhs_col_idx, Vector &row_pointers,
                                                                    Vector &target, data_ptr_t target_data,
                                                                    idx_t current_size, idx_t count) {
	if (plan.kind != SljitNativeRegionExpressionKind::DATE_YEAR || plan.source_index != 0 ||
	    plan.return_type != target.GetType() || target.GetType().InternalType() != PhysicalType::INT64 ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (!plan.expression_tree_source_indices.empty() &&
	    (plan.expression_tree_source_indices.size() != 1 || plan.expression_tree_source_indices[0] != 0)) {
		return false;
	}

	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
	    rhs_source.type.id() != LogicalTypeId::DATE || rhs_source.physical_type != PhysicalType::INT32 ||
	    rhs_source.layout_offset == DConstants::INVALID_INDEX) {
		return false;
	}

	Vector result(target.GetType(), target_data, count);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<int64_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.Reset(count);
	result_validity.EnsureWritable();
	result_validity.SetAllValid(count);

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	if (rhs_source.all_valid) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_location = row_pointer_data[row_idx];
			if (!row_location) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			auto days = Load<int32_t>(row_location + rhs_source.layout_offset);
			if (!SljitDateDaysAreFinite(days)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			result_data[row_idx] = SljitExtractFiniteDateYear(days);
		}
	} else {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_location = row_pointer_data[row_idx];
			if (!SljitHashJoinRHSFixedColumnSourceIsValid(row_location, rhs_source)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			auto days = Load<int32_t>(row_location + rhs_source.layout_offset);
			if (!SljitDateDaysAreFinite(days)) {
				result_validity.SetInvalid(row_idx);
				continue;
			}
			result_data[row_idx] = SljitExtractFiniteDateYear(days);
		}
	}
	FlatVector::SetSize(result, count_t(count));
	SljitCopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
	return true;
}

static bool
SljitTryMaterializeHashJoinRHSInlineStringDecompressProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                                      const SljitNativeRegionExpressionPlan &plan,
                                                                      idx_t rhs_col_idx, Vector &row_pointers,
                                                                      Vector &target, idx_t current_size,
                                                                      idx_t count) {
	if (plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS || plan.source_index != 0 ||
	    plan.string_decompress_source_size != sizeof(uhugeint_t) || plan.return_type != target.GetType() ||
	    target.GetType().id() != LogicalTypeId::VARCHAR || target.GetVectorType() != VectorType::FLAT_VECTOR ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    FlatVector::GetCapacity(target) < current_size + count) {
		return false;
	}

	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
	    rhs_source.physical_type != PhysicalType::UINT128 || rhs_source.layout_offset == DConstants::INVALID_INDEX) {
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

static bool SljitTryBuildHashJoinProjectionExpressionInput(
    const ExecutionHashJoinProbeBinding &binding, const SljitExecutableRegionExpression &source_expr,
    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, idx_t count,
    SljitExecutableRegionExpression &remapped_expr, DataChunk &expression_input, vector<Vector> &expression_sources) {
	if (!binding.ready || !binding.hash_table ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	const auto join_output_column_count = lhs_column_count + binding.rhs_output_column_count;
	if (join_output_column_count == 0) {
		return false;
	}

	vector<uint8_t> referenced_columns;
	if (!SljitTryCollectHashJoinProjectionExpressionSources(source_expr, join_output_column_count,
	                                                        referenced_columns)) {
		return false;
	}
	vector<idx_t> source_map(join_output_column_count, DConstants::INVALID_INDEX);
	vector<LogicalType> expression_types;
	expression_sources.clear();
	expression_sources.reserve(join_output_column_count);
	expression_types.reserve(join_output_column_count);

	for (idx_t source_idx = 0; source_idx < join_output_column_count; source_idx++) {
		if (!referenced_columns[source_idx]) {
			continue;
		}
		auto &source_type = binding.output_types[source_idx];
		expression_types.push_back(source_type);
		source_map[source_idx] = expression_sources.size();
		expression_sources.emplace_back(source_type);
		auto &source = expression_sources.back();
		if (source_idx < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[source_idx];
			if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != source_type) {
				return false;
			}
			source.Slice(join_input.data[input_col], match_selection, count);
			continue;
		}
		const auto rhs_col_idx = source_idx - lhs_column_count;
		if (rhs_col_idx >= binding.rhs_output_column_count) {
			return false;
		}
		SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, source);
	}
	if (expression_sources.empty()) {
		return false;
	}

	SljitBuildBorrowedProjectionExpression(source_expr, remapped_expr);
	if (!SljitTryRemapHashJoinProjectionExpressionSources(source_map, remapped_expr)) {
		return false;
	}

	expression_input.InitializeEmpty(expression_types);
	for (idx_t source_idx = 0; source_idx < expression_sources.size(); source_idx++) {
		expression_input.data[source_idx].Reference(expression_sources[source_idx]);
	}
	expression_input.SetChildCardinality(count);
	return true;
}

static bool SljitTryMaterializeHashJoinComputedRHSProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
                                                                    const SljitExecutableRegionExpression &source_expr,
                                                                    Vector &row_pointers, DataChunk &batch,
                                                                    idx_t output_idx, idx_t current_size, idx_t count,
                                                                    SljitExpressionAdapterScratch &adapter_scratch,
                                                                    bool &used_row_pointer_generated_source) {
	used_row_pointer_generated_source = false;
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_expr, join_output_source_index)) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	if (rhs_col_idx >= binding.rhs_output_column_count || join_output_source_index >= binding.output_types.size()) {
		return false;
	}

	auto &target = batch.data[output_idx];
	if (SljitTryGatherHashJoinRHSReferenceProjectionToBatch(binding, remapped_expr.plan, rhs_col_idx, row_pointers,
	                                                        target, current_size, count)) {
		return true;
	}
	if (DirectAppendSupportsFixedSizeType(target.GetType())) {
		auto target_data =
		    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
		if (SljitTryMaterializeHashJoinRHSDateYearProjectionToBatch(
		        binding, remapped_expr.plan, rhs_col_idx, row_pointers, target, target_data, current_size, count)) {
			used_row_pointer_generated_source = true;
			return true;
		}
	}
	if (SljitTryMaterializeHashJoinRHSInlineStringDecompressProjectionToBatch(
	        binding, remapped_expr.plan, rhs_col_idx, row_pointers, target, current_size, count)) {
		used_row_pointer_generated_source = true;
		return true;
	}
	if (!remapped_expr.function && !remapped_expr.flat_function) {
		return false;
	}

	auto &source_type = binding.output_types[join_output_source_index];
	Vector gathered_source(source_type);
	SljitGatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, gathered_source);

	DataChunk gathered_input;
	gathered_input.InitializeEmpty(vector<LogicalType> {source_type});
	gathered_input.data[0].Reference(gathered_source);
	gathered_input.SetChildCardinality(count);
	return SljitTryExecuteProjectionExpressionToBatch(remapped_expr, gathered_input, target, current_size, count,
	                                                  nullptr, adapter_scratch);
}

static bool SljitTryMaterializePerfectHashJoinComputedRHSProjectionToBatch(
    const ExecutionHashJoinProbeBinding &binding, const SljitExecutableRegionExpression &source_expr,
    const SelectionVector &build_selection, DataChunk &batch, idx_t output_idx, idx_t current_size, idx_t count,
    SljitExpressionAdapterScratch &adapter_scratch) {
	if (!binding.ready || binding.layout_kind != ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE ||
	    !binding.perfect_layout.ready ||
	    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}

	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_expr, join_output_source_index)) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count || join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	if (rhs_col_idx >= binding.perfect_layout.rhs_output_column_count ||
	    rhs_col_idx >= binding.perfect_layout.rhs_dictionary_buffers.size() ||
	    rhs_col_idx >= binding.perfect_layout.rhs_output_types.size()) {
		return false;
	}

	auto &source_type = binding.perfect_layout.rhs_output_types[rhs_col_idx];
	if (binding.output_types[join_output_source_index] != source_type) {
		return false;
	}
	Vector source(source_type);
	source.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_col_idx], build_selection, count);

	DataChunk input;
	input.InitializeEmpty(vector<LogicalType> {source_type});
	input.data[0].Reference(source);
	input.SetChildCardinality(count);
	return SljitTryExecuteProjectionExpressionToBatch(remapped_expr, input, batch.data[output_idx], current_size, count,
	                                                  nullptr, adapter_scratch);
}

static bool SljitTryMaterializeHashJoinMixedProjectionToBatch(
    const ExecutionHashJoinProbeBinding &binding, const SljitExecutableRegionExpression &source_expr,
    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, DataChunk &batch,
    idx_t output_idx, idx_t current_size, idx_t count, SljitExpressionAdapterScratch &adapter_scratch) {
	auto &target = batch.data[output_idx];
	if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
	    !SljitDirectProjectionBatchSupportsType(target.GetType()) ||
	    FlatVector::GetCapacity(target) < current_size + count) {
		return false;
	}

	SljitExecutableRegionExpression remapped_expr;
	DataChunk expression_input;
	vector<Vector> expression_sources;
	if (!SljitTryBuildHashJoinProjectionExpressionInput(binding, source_expr, join_input, match_selection, row_pointers,
	                                                    count, remapped_expr, expression_input, expression_sources)) {
		return false;
	}

	return SljitTryExecuteProjectionExpressionToBatch(remapped_expr, expression_input, target, current_size, count,
	                                                  nullptr, adapter_scratch);
}

} // namespace duckdb

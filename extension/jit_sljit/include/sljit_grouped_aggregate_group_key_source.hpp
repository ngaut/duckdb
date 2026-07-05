//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_group_key_source.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_projection_source_runtime.hpp"

#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

static bool SljitGroupKeyCompressedUnsignedTargetType(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

static bool SljitTryReadDateYearCompressGroupKey(const SljitNativeRegionExpressionPlan &plan,
                                                 const LogicalType &target_type,
                                                 const ExecutionRowPointerGroupKeySource &group_source,
                                                 int64_t &minimum) {
	if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
	    plan.expression_tree_source_indices.size() != 1 || plan.expression_tree_source_indices[0] != 0 ||
	    plan.return_type.InternalType() != target_type.InternalType() ||
	    group_source.source_type.id() != LogicalTypeId::DATE ||
	    group_source.source_physical_type != PhysicalType::INT32 ||
	    !SljitGroupKeyCompressedUnsignedTargetType(target_type.InternalType())) {
		return false;
	}
	auto &compress = *plan.expression_tree;
	if (compress.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    compress.intrinsic != ExecutionExpressionIntrinsicKind::INTEGRAL_COMPRESS ||
	    compress.return_type.InternalType() != target_type.InternalType() || compress.children.size() != 2 ||
	    !compress.children[0] || !compress.children[1]) {
		return false;
	}
	auto &date_year = *compress.children[0];
	auto &constant = *compress.children[1];
	if (date_year.kind != ExecutionExpressionIRKind::INTRINSIC ||
	    date_year.intrinsic != ExecutionExpressionIntrinsicKind::DATE_YEAR ||
	    date_year.return_type.id() != LogicalTypeId::BIGINT || date_year.physical_type != PhysicalType::INT64 ||
	    date_year.children.size() != 1 || !date_year.children[0] ||
	    date_year.children[0]->kind != ExecutionExpressionIRKind::REFERENCE ||
	    date_year.children[0]->ref_index != 0 || date_year.children[0]->return_type.id() != LogicalTypeId::DATE ||
	    date_year.children[0]->physical_type != PhysicalType::INT32 ||
	    constant.kind != ExecutionExpressionIRKind::CONSTANT || constant.constant.IsNull() ||
	    constant.return_type.id() != LogicalTypeId::BIGINT || constant.physical_type != PhysicalType::INT64) {
		return false;
	}
	minimum = constant.constant.GetValueUnsafe<int64_t>();
	return true;
}

static bool SljitTryMapGroupKeyCast(const SljitNativeRegionExpressionPlan &plan, const LogicalType &target_type,
                                    ExecutionRowPointerGroupKeySource &group_source) {
	auto signed_width_matches_physical_type = [](SljitNativeSignedIntegerWidth width, PhysicalType physical_type) {
		switch (width) {
		case SljitNativeSignedIntegerWidth::INT8:
			return physical_type == PhysicalType::INT8;
		case SljitNativeSignedIntegerWidth::INT16:
			return physical_type == PhysicalType::INT16;
		case SljitNativeSignedIntegerWidth::INT32:
			return physical_type == PhysicalType::INT32;
		case SljitNativeSignedIntegerWidth::INT64:
			return physical_type == PhysicalType::INT64;
		default:
			return false;
		}
	};
	auto unsigned_width_matches_physical_type = [](SljitNativeUnsignedIntegerWidth width, PhysicalType physical_type) {
		switch (width) {
		case SljitNativeUnsignedIntegerWidth::UINT8:
			return physical_type == PhysicalType::UINT8;
		case SljitNativeUnsignedIntegerWidth::UINT16:
			return physical_type == PhysicalType::UINT16;
		case SljitNativeUnsignedIntegerWidth::UINT32:
			return physical_type == PhysicalType::UINT32;
		default:
			return false;
		}
	};

	group_source.ready = false;
	group_source.target_type = target_type;
	group_source.target_physical_type = target_type.InternalType();
	if (plan.return_type.InternalType() != target_type.InternalType()) {
		return false;
	}

	int64_t date_year_minimum;
	if (SljitTryReadDateYearCompressGroupKey(plan, target_type, group_source, date_year_minimum)) {
		group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS;
		group_source.cast_constant = date_year_minimum;
		group_source.ready = true;
		return true;
	}

	if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && !plan.try_cast && plan.source_index == 0) {
		if (group_source.source_physical_type == target_type.InternalType() &&
		    signed_width_matches_physical_type(plan.cast_source_width, group_source.source_physical_type) &&
		    signed_width_matches_physical_type(plan.cast_target_width, target_type.InternalType())) {
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
			group_source.ready = true;
			return true;
		}
		if (group_source.source_physical_type == PhysicalType::INT64 &&
		    target_type.InternalType() == PhysicalType::INT32 &&
		    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT64 &&
		    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT32) {
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32;
			group_source.ready = true;
			return true;
		}
		if (group_source.source_physical_type == PhysicalType::INT64 &&
		    target_type.InternalType() == PhysicalType::INT16 &&
		    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT64 &&
		    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT16) {
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16;
			group_source.ready = true;
			return true;
		}
		if (group_source.source_physical_type == PhysicalType::INT32 &&
		    target_type.InternalType() == PhysicalType::INT8 &&
		    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT32 &&
		    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT8) {
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8;
			group_source.ready = true;
			return true;
		}
		return false;
	}

	if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS && plan.source_index == 0 &&
	    signed_width_matches_physical_type(plan.cast_source_width, group_source.source_physical_type) &&
	    unsigned_width_matches_physical_type(plan.unsigned_cast_target_width, target_type.InternalType())) {
		group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS;
		group_source.cast_constant = plan.constant;
		group_source.ready = true;
		return true;
	}

	if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS && plan.source_index == 0 &&
	    group_source.source_physical_type == PhysicalType::VARCHAR) {
		switch (target_type.InternalType()) {
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::UINT128:
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS;
			group_source.ready = true;
			return true;
		default:
			break;
		}
	}
	return false;
}

static void SljitInitializeRowPointerGroupKeySource(const ExecutionHashJoinRHSFixedColumnSource &rhs_source,
                                                    const LogicalType &target_type,
                                                    ExecutionRowPointerGroupKeySource &group_source) {
	group_source.source_kind = ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD;
	group_source.source_type = rhs_source.type;
	group_source.target_type = target_type;
	group_source.source_physical_type = rhs_source.physical_type;
	group_source.target_physical_type = target_type.InternalType();
	group_source.input_vector_index = DConstants::INVALID_INDEX;
	group_source.row_layout_offset = rhs_source.layout_offset;
	group_source.row_layout_column_idx = rhs_source.layout_column_idx;
	group_source.row_layout_column_count = rhs_source.layout_column_count;
	group_source.all_valid = rhs_source.all_valid;
}

static void SljitInitializeInputVectorGroupKeySource(idx_t input_vector_index, const LogicalType &source_type,
                                                     const LogicalType &target_type,
                                                     ExecutionRowPointerGroupKeySource &group_source,
                                                     idx_t hash_join_condition_idx = DConstants::INVALID_INDEX) {
	group_source.source_kind = ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR;
	group_source.source_type = source_type;
	group_source.target_type = target_type;
	group_source.source_physical_type = source_type.InternalType();
	group_source.target_physical_type = target_type.InternalType();
	group_source.input_vector_index = input_vector_index;
	group_source.input_vector_repeats_with_row_pointer = false;
	group_source.hash_join_condition_idx = hash_join_condition_idx;
	group_source.hash_join_build_key_physical_type = PhysicalType::INVALID;
	group_source.row_layout_offset = DConstants::INVALID_INDEX;
	group_source.row_layout_column_idx = DConstants::INVALID_INDEX;
	group_source.row_layout_column_count = 0;
	group_source.all_valid = false;
}

static void SljitAttachHashJoinBuildConditionType(const ExecutionHashJoinProbeBinding &binding,
                                                  ExecutionRowPointerGroupKeySource &group_source,
                                                  idx_t condition_idx) {
	if (condition_idx >= binding.rhs_condition_types.size()) {
		return;
	}
	group_source.hash_join_build_key_physical_type = binding.rhs_condition_types[condition_idx].InternalType();
}

static bool SljitInputVectorGroupSourceUsesProjection(const ExecutionRowPointerGroupKeySource &source) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return true;
	default:
		return false;
	}
}

static bool SljitTryGetHashJoinLHSInputConditionIndex(const ExecutionHashJoinProbeBinding &binding,
                                                      idx_t input_vector_index, const LogicalType &source_type,
                                                      idx_t &condition_idx) {
	if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
		if (binding.probe_key_input_indices.size() == 1 && binding.probe_key_input_indices[0] == input_vector_index &&
		    binding.perfect_layout.ready && binding.perfect_layout.key_type == source_type) {
			condition_idx = 0;
			return true;
		}
		return false;
	}
	for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
		if (key_idx >= binding.table_layout.condition_types.size()) {
			return false;
		}
		if (binding.probe_key_input_indices[key_idx] == input_vector_index &&
		    binding.table_layout.condition_types[key_idx] == source_type) {
			condition_idx = key_idx;
			return true;
		}
	}
	return false;
}

static bool SljitTryGetHashJoinRHSFixedColumnConditionIndex(const ExecutionHashJoinProbeBinding &binding,
                                                            const ExecutionHashJoinRHSFixedColumnSource &source,
                                                            idx_t &condition_idx) {
	const auto condition_count =
	    MinValue<idx_t>(binding.table_layout.condition_count, binding.table_layout.layout_offsets.size());
	for (idx_t key_idx = 0; key_idx < condition_count; key_idx++) {
		if (key_idx >= binding.table_layout.condition_types.size()) {
			return false;
		}
		if (source.layout_offset == binding.table_layout.layout_offsets[key_idx] &&
		    source.type == binding.table_layout.condition_types[key_idx]) {
			condition_idx = key_idx;
			return true;
		}
	}
	return false;
}

static bool SljitTryResolveHashJoinMatchedProbeInputForOutput(const ExecutionHashJoinProbeBinding &binding,
                                                              idx_t join_output_source_index,
                                                              idx_t &probe_input_idx, idx_t &condition_idx,
                                                              bool &repeats_with_row_pointer) {
	probe_input_idx = DConstants::INVALID_INDEX;
	condition_idx = DConstants::INVALID_INDEX;
	repeats_with_row_pointer = false;
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count) {
		if (join_output_source_index >= binding.output_types.size()) {
			return false;
		}
		probe_input_idx = binding.lhs_output_column_indices[join_output_source_index];
		SljitTryGetHashJoinLHSInputConditionIndex(
		    binding, probe_input_idx, binding.output_types[join_output_source_index], condition_idx);
		return true;
	}

	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, join_output_source_index - lhs_column_count, rhs_source)) {
		return false;
	}
	if (!SljitTryGetHashJoinRHSFixedColumnConditionIndex(binding, rhs_source, condition_idx) ||
	    condition_idx >= binding.probe_key_input_indices.size()) {
		return false;
	}
	probe_input_idx = binding.probe_key_input_indices[condition_idx];
	repeats_with_row_pointer = true;
	return true;
}

static bool SljitHashJoinRHSFixedColumnIsNullFilteredConditionKey(const ExecutionHashJoinProbeBinding &binding,
                                                                  const ExecutionHashJoinRHSFixedColumnSource &source) {
	if (!binding.table_layout.null_keys_are_filtered) {
		return false;
	}
	idx_t condition_idx;
	return SljitTryGetHashJoinRHSFixedColumnConditionIndex(binding, source, condition_idx);
}

static bool SljitTryFinalizeRowPointerGroupKeySource(const SljitNativeRegionExpressionPlan &plan,
                                                     const LogicalType &group_type,
                                                     ExecutionRowPointerGroupKeySource &group_source) {
	if (SljitProjectionIsSingleSourceReferenceLike(plan) &&
	    group_source.source_physical_type == group_type.InternalType() &&
	    plan.return_type.InternalType() == group_type.InternalType()) {
		group_source.ready = true;
		group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
		return true;
	}
	return SljitTryMapGroupKeyCast(plan, group_type, group_source);
}

static bool SljitTryBuildRowPointerGroupKeySource(const ExecutionHashJoinProbeBinding &binding,
                                                  SljitExecutableRegionExpression &projection,
                                                  const ExecutionRegionGroupInput &group,
                                                  ExecutionRowPointerGroupKeySource &group_source) {
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection, remapped_expr, join_output_source_index)) {
		return false;
	}
	if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	    join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count) {
		const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
		const auto &source_type = binding.output_types[join_output_source_index];
		idx_t condition_idx = DConstants::INVALID_INDEX;
		SljitTryGetHashJoinLHSInputConditionIndex(binding, input_col, source_type, condition_idx);
		SljitInitializeInputVectorGroupKeySource(input_col, source_type, group.type, group_source, condition_idx);
		SljitAttachHashJoinBuildConditionType(binding, group_source, condition_idx);
		return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source)) {
		return false;
	}
	idx_t condition_idx;
	if (SljitTryGetHashJoinRHSFixedColumnConditionIndex(binding, rhs_source, condition_idx)) {
		if (condition_idx >= binding.probe_key_input_indices.size() ||
		    condition_idx >= binding.table_layout.condition_types.size()) {
			return false;
		}
		SljitInitializeInputVectorGroupKeySource(binding.probe_key_input_indices[condition_idx],
		                                         binding.table_layout.condition_types[condition_idx], group.type,
		                                         group_source, condition_idx);
		group_source.input_vector_repeats_with_row_pointer = true;
		SljitAttachHashJoinBuildConditionType(binding, group_source, condition_idx);
		return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
	}
	SljitInitializeRowPointerGroupKeySource(rhs_source, group.type, group_source);
	if (SljitHashJoinRHSFixedColumnIsNullFilteredConditionKey(binding, rhs_source)) {
		group_source.all_valid = true;
	}
	return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
}

static bool SljitTryBuildHashJoinOutputVectorGroupKeySource(const ExecutionHashJoinProbeBinding &binding,
                                                            SljitExecutableRegionExpression &projection,
                                                            const ExecutionRegionGroupInput &group,
                                                            ExecutionRowPointerGroupKeySource &group_source) {
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection, remapped_expr, join_output_source_index) ||
	    join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	const auto &source_type = binding.output_types[join_output_source_index];
	SljitInitializeInputVectorGroupKeySource(join_output_source_index, source_type, group.type, group_source);
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	group_source.input_vector_repeats_with_row_pointer =
	    join_output_source_index >= lhs_column_count &&
	    binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE &&
	    binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD;
	return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
}

static bool SljitTryBuildProjectionOutputVectorGroupKeySource(const SljitExecutableRegionOp &projection_op,
                                                              idx_t projection_idx,
                                                              const ExecutionRegionGroupInput &group,
                                                              ExecutionRowPointerGroupKeySource &group_source) {
	if (projection_idx >= projection_op.output_types.size() || projection_op.output_types[projection_idx] != group.type) {
		return false;
	}
	SljitInitializeInputVectorGroupKeySource(projection_idx, group.type, group.type, group_source);
	group_source.ready = true;
	group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	return true;
}

static bool SljitTryResolveProjectionSemanticIndex(SljitExecutableRegionOp &projection_op,
                                                   optional_ptr<const vector<idx_t>> semantic_to_projection,
                                                   idx_t semantic_idx, idx_t &projection_idx) {
	if (semantic_to_projection) {
		if (semantic_idx >= semantic_to_projection->size()) {
			return false;
		}
		projection_idx = (*semantic_to_projection)[semantic_idx];
	} else {
		projection_idx = semantic_idx;
	}
	return projection_idx < projection_op.projections.size() && projection_idx < projection_op.output_types.size();
}

static bool SljitTryBuildRowPointerGroupKeySources(const ExecutionHashJoinProbeBinding &binding,
                                                   SljitExecutableRegionOp &projection_op,
                                                   SljitExecutableRegionOp &aggregate_op,
                                                   vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr,
                                                   optional_ptr<vector<uint8_t>> group_source_uses_projection_output =
                                                       nullptr,
                                                   optional_ptr<string> blocker = nullptr) {
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty()) {
		if (blocker) {
			*blocker = "aggregate_shape";
		}
		return false;
	}
	group_sources.clear();
	group_sources.reserve(sink_info.groups.size());
	if (group_source_uses_projection_output) {
		group_source_uses_projection_output->clear();
		group_source_uses_projection_output->reserve(sink_info.groups.size());
	}
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, group.input_index,
		                                            projection_idx)) {
			if (blocker) {
				*blocker = "group" + to_string(group_idx) + "_projection_index";
			}
			group_sources.clear();
			return false;
		}
		if (projection_op.output_types[projection_idx].InternalType() != group.type.InternalType()) {
			if (blocker) {
				*blocker = "group" + to_string(group_idx) + "_projection_type";
			}
			group_sources.clear();
			return false;
		}
			ExecutionRowPointerGroupKeySource group_source;
			bool uses_projection_output = false;
			if (!SljitTryBuildRowPointerGroupKeySource(binding, projection_op.projections[projection_idx], group,
			                                           group_source)) {
				if (!SljitTryBuildHashJoinOutputVectorGroupKeySource(binding, projection_op.projections[projection_idx],
				                                                     group, group_source) &&
				    !SljitTryBuildProjectionOutputVectorGroupKeySource(projection_op, projection_idx, group,
				                                                       group_source)) {
					if (blocker) {
						*blocker = "group" + to_string(group_idx) + "_source";
					}
				group_sources.clear();
				if (group_source_uses_projection_output) {
					group_source_uses_projection_output->clear();
				}
				return false;
			}
			uses_projection_output = true;
		}
		group_sources.push_back(std::move(group_source));
		if (group_source_uses_projection_output) {
			group_source_uses_projection_output->push_back(uses_projection_output);
		}
	}
	return true;
}

} // namespace duckdb

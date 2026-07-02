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

	if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && !plan.try_cast && plan.source_index == 0) {
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
	group_source.target_type = target_type;
	group_source.source_physical_type = rhs_source.physical_type;
	group_source.target_physical_type = target_type.InternalType();
	group_source.input_vector_index = DConstants::INVALID_INDEX;
	group_source.row_layout_offset = rhs_source.layout_offset;
	group_source.row_layout_column_idx = rhs_source.layout_column_idx;
	group_source.row_layout_column_count = rhs_source.layout_column_count;
	group_source.all_valid = rhs_source.all_valid;
}

static void SljitInitializeInputVectorGroupKeySource(idx_t input_vector_index, PhysicalType source_physical_type,
                                                     const LogicalType &target_type,
                                                     ExecutionRowPointerGroupKeySource &group_source) {
	group_source.source_kind = ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR;
	group_source.target_type = target_type;
	group_source.source_physical_type = source_physical_type;
	group_source.target_physical_type = target_type.InternalType();
	group_source.input_vector_index = input_vector_index;
	group_source.row_layout_offset = DConstants::INVALID_INDEX;
	group_source.row_layout_column_idx = DConstants::INVALID_INDEX;
	group_source.row_layout_column_count = 0;
	group_source.all_valid = false;
}

static bool SljitHashJoinRHSFixedColumnIsNullFilteredConditionKey(const ExecutionHashJoinProbeBinding &binding,
                                                                  const ExecutionHashJoinRHSFixedColumnSource &source) {
	if (!binding.table_layout.null_keys_are_filtered) {
		return false;
	}
	const auto condition_count =
	    MinValue<idx_t>(binding.table_layout.condition_count, binding.table_layout.layout_offsets.size());
	for (idx_t key_idx = 0; key_idx < condition_count; key_idx++) {
		if (key_idx >= binding.table_layout.condition_types.size()) {
			return false;
		}
		if (source.layout_offset == binding.table_layout.layout_offsets[key_idx] &&
		    source.type == binding.table_layout.condition_types[key_idx]) {
			return true;
		}
	}
	return false;
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
		SljitInitializeInputVectorGroupKeySource(
		    input_col, binding.output_types[join_output_source_index].InternalType(), group.type, group_source);
		return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source)) {
		return false;
	}
	SljitInitializeRowPointerGroupKeySource(rhs_source, group.type, group_source);
	if (SljitHashJoinRHSFixedColumnIsNullFilteredConditionKey(binding, rhs_source)) {
		group_source.all_valid = true;
	}
	return SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source);
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
                                                   optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr) {
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty()) {
		return false;
	}
	group_sources.clear();
	group_sources.reserve(sink_info.groups.size());
	for (auto &group : sink_info.groups) {
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, group.input_index,
		                                            projection_idx) ||
		    projection_op.output_types[projection_idx].InternalType() != group.type.InternalType()) {
			group_sources.clear();
			return false;
		}
		ExecutionRowPointerGroupKeySource group_source;
		if (!SljitTryBuildRowPointerGroupKeySource(binding, projection_op.projections[projection_idx], group,
		                                           group_source)) {
			group_sources.clear();
			return false;
		}
		group_sources.push_back(std::move(group_source));
	}
	return true;
}

} // namespace duckdb

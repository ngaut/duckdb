//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pre_join_projection_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

enum class SljitPreJoinProjectionViewColumnKind : uint8_t { REFERENCE, INT64_TO_INT32_CAST };

struct SljitPreJoinProjectionViewColumn {
	SljitPreJoinProjectionViewColumnKind kind = SljitPreJoinProjectionViewColumnKind::REFERENCE;
	idx_t source_idx = DConstants::INVALID_INDEX;
	LogicalType source_type;
	LogicalType target_type;
	bool source_range_fits_target = false;
};

struct SljitPreJoinProjectionViewDescriptor {
	idx_t projection_idx = DConstants::INVALID_INDEX;
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	vector<SljitPreJoinProjectionViewColumn> columns;
	vector<idx_t> projected_to_source;
	vector<idx_t> hash_probe_key_source_indices;
	vector<idx_t> residual_probe_source_indices;
	bool hash_probe_key_inputs_match_source = false;
	bool hash_probe_key_inputs_can_remap = false;
	bool residual_probe_sources_can_remap = true;
	bool source_key0_int64_to_int32_unchecked = false;

	bool CanElideProjectionWithCurrentHashProbe() const {
		return projection_idx != DConstants::INVALID_INDEX && hash_join_idx != DConstants::INVALID_INDEX &&
		       residual_probe_sources_can_remap &&
		       (hash_probe_key_inputs_match_source || hash_probe_key_inputs_can_remap);
	}

	bool PreservesSourceColumnOrdinals() const {
		for (idx_t output_idx = 0; output_idx < projected_to_source.size(); output_idx++) {
			if (projected_to_source[output_idx] != output_idx) {
				return false;
			}
		}
		return true;
	}
};

static bool SljitTryReadSignedIntegerValue(const Value &value, int64_t &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		result = NumericCast<int64_t>(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static bool SljitSourceRangeFitsInt32(idx_t source_index, const vector<Value> &source_min_values,
                                      const vector<Value> &source_max_values) {
	if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
		return false;
	}
	int64_t min_value;
	int64_t max_value;
	if (!SljitTryReadSignedIntegerValue(source_min_values[source_index], min_value) ||
	    !SljitTryReadSignedIntegerValue(source_max_values[source_index], max_value)) {
		return false;
	}
	return min_value >= NumericLimits<int32_t>::Minimum() && max_value <= NumericLimits<int32_t>::Maximum();
}

static bool SljitTryBuildPreJoinProjectionViewColumn(const SljitExecutableRegionOp &projection_op,
                                                     idx_t projection_output_idx,
                                                     const vector<Value> &source_min_values,
                                                     const vector<Value> &source_max_values,
                                                     SljitPreJoinProjectionViewColumn &column) {
	column = SljitPreJoinProjectionViewColumn();
	if (projection_output_idx >= projection_op.projections.size() ||
	    projection_output_idx >= projection_op.output_types.size()) {
		return false;
	}
	auto &projection = projection_op.projections[projection_output_idx];
	auto &plan = projection.plan;
	if (plan.return_type != projection_op.output_types[projection_output_idx]) {
		return false;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		if (plan.source_index >= projection_op.input_types.size() ||
		    projection_op.input_types[plan.source_index] != plan.return_type) {
			return false;
		}
		column.kind = SljitPreJoinProjectionViewColumnKind::REFERENCE;
		column.source_idx = plan.source_index;
		column.source_type = projection_op.input_types[plan.source_index];
		column.target_type = plan.return_type;
		return true;
	}
	if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && !plan.try_cast &&
	    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT64 &&
	    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT32 &&
	    plan.return_type.InternalType() == PhysicalType::INT32 &&
	    plan.source_index < projection_op.input_types.size() &&
	    projection_op.input_types[plan.source_index].InternalType() == PhysicalType::INT64) {
		column.kind = SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST;
		column.source_idx = plan.source_index;
		column.source_type = projection_op.input_types[plan.source_index];
		column.target_type = plan.return_type;
		column.source_range_fits_target =
		    SljitSourceRangeFitsInt32(plan.source_index, source_min_values, source_max_values);
		return true;
	}
	return false;
}

static bool SljitTryBuildPreJoinResidualProbeSourceRemap(const SljitNativeHashJoinProbePlan &join,
                                                         const vector<SljitPreJoinProjectionViewColumn> &columns,
                                                         vector<idx_t> &residual_probe_source_indices) {
	auto &residual_sources = join.operator_info.hash_join_contract.residual_sources;
	residual_probe_source_indices.clear();
	if (!join.residual_predicate) {
		return true;
	}
	residual_probe_source_indices.assign(residual_sources.size(), DConstants::INVALID_INDEX);
	for (auto &source : residual_sources) {
		if (source.source_index >= residual_probe_source_indices.size()) {
			return false;
		}
		if (source.kind == ExecutionHashJoinResidualSourceKind::BUILD) {
			continue;
		}
		if (source.kind != ExecutionHashJoinResidualSourceKind::PROBE || source.input_index >= columns.size()) {
			return false;
		}
		auto &column = columns[source.input_index];
		if (column.kind != SljitPreJoinProjectionViewColumnKind::REFERENCE || column.source_type != source.type ||
		    column.target_type != source.type) {
			return false;
		}
		residual_probe_source_indices[source.source_index] = column.source_idx;
	}
	return true;
}

static bool SljitTryBuildPreJoinProjectionViewDescriptor(const vector<SljitExecutableRegionOp> &ops,
                                                         idx_t projection_idx, idx_t hash_join_idx,
                                                         const vector<Value> &source_min_values,
                                                         const vector<Value> &source_max_values,
                                                         SljitPreJoinProjectionViewDescriptor &descriptor) {
	descriptor = SljitPreJoinProjectionViewDescriptor();
	if (projection_idx >= ops.size() || hash_join_idx >= ops.size()) {
		return false;
	}
	auto &projection_op = ops[projection_idx];
	auto &hash_join_op = ops[hash_join_idx];
	if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    hash_join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
	    projection_op.projections.size() != projection_op.output_types.size()) {
		return false;
	}
	descriptor.projection_idx = projection_idx;
	descriptor.hash_join_idx = hash_join_idx;
	descriptor.columns.reserve(projection_op.projections.size());
	descriptor.projected_to_source.reserve(projection_op.projections.size());
	for (idx_t projection_output_idx = 0; projection_output_idx < projection_op.projections.size();
	     projection_output_idx++) {
		SljitPreJoinProjectionViewColumn column;
		if (!SljitTryBuildPreJoinProjectionViewColumn(projection_op, projection_output_idx, source_min_values,
		                                              source_max_values, column)) {
			descriptor = SljitPreJoinProjectionViewDescriptor();
			return false;
		}
		descriptor.projected_to_source.push_back(column.source_idx);
		descriptor.columns.push_back(std::move(column));
	}

	auto &join = hash_join_op.hash_join_probe.plan;
	descriptor.residual_probe_sources_can_remap = SljitTryBuildPreJoinResidualProbeSourceRemap(
	    join, descriptor.columns, descriptor.residual_probe_source_indices);
	descriptor.hash_probe_key_inputs_match_source = true;
	descriptor.hash_probe_key_inputs_can_remap = descriptor.residual_probe_sources_can_remap;
	descriptor.hash_probe_key_source_indices.reserve(join.keys.size());
	for (idx_t key_idx = 0; key_idx < join.keys.size(); key_idx++) {
		auto &key = join.keys[key_idx];
		if (key.key_input_index >= descriptor.columns.size()) {
			descriptor.hash_probe_key_inputs_match_source = false;
			descriptor.hash_probe_key_inputs_can_remap = false;
			continue;
		}
		auto &column = descriptor.columns[key.key_input_index];
		descriptor.hash_probe_key_source_indices.push_back(column.source_idx);
		if (key.key_input_index != column.source_idx) {
			descriptor.hash_probe_key_inputs_match_source = false;
		}
		if (column.kind == SljitPreJoinProjectionViewColumnKind::REFERENCE) {
			if (!SljitHashJoinKeyKindMatchesPhysicalType(key.key_kind, column.source_type.InternalType())) {
				descriptor.hash_probe_key_inputs_match_source = false;
				descriptor.hash_probe_key_inputs_can_remap = false;
			}
			continue;
		}
		if (column.kind == SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST &&
		    !(key.key_input_index == 0 &&
		      SljitHashJoinKeyCanUseInt64SourceForInt32Key(key_idx, key.key_kind, column.source_type.InternalType()))) {
			descriptor.hash_probe_key_inputs_match_source = false;
			descriptor.hash_probe_key_inputs_can_remap = false;
			continue;
		}
		if (column.kind == SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST) {
			descriptor.hash_probe_key_inputs_match_source = false;
		}
	}
	if (!join.keys.empty() && join.keys[0].key_input_index < descriptor.columns.size()) {
		auto &key0_column = descriptor.columns[join.keys[0].key_input_index];
		descriptor.source_key0_int64_to_int32_unchecked =
		    key0_column.kind == SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST &&
		    key0_column.source_range_fits_target;
	}
	return !descriptor.columns.empty();
}

static bool SljitHashJoinSourceKey0RangeFitsInt32(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                  const vector<Value> &source_min_values,
                                                  const vector<Value> &source_max_values) {
	if (hash_join_idx >= ops.size()) {
		return false;
	}
	auto &join_op = ops[hash_join_idx];
	if (join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
		return false;
	}
	auto &join = join_op.hash_join_probe.plan;
	if (join.keys.empty()) {
		return false;
	}
	return SljitSourceRangeFitsInt32(join.keys[0].key_input_index, source_min_values, source_max_values);
}

} // namespace duckdb

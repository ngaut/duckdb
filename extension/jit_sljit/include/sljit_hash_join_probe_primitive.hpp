//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

struct SljitHashJoinProbeInputRemap {
	vector<idx_t> key_input_indices;
	vector<idx_t> residual_probe_source_indices;
	SljitNativeHashJoinProbePlan prepared_plan;
	bool has_prepared_plan = false;

	SljitHashJoinProbeInputRemap() = default;

	SljitHashJoinProbeInputRemap(const SljitHashJoinProbeInputRemap &other)
	    : key_input_indices(other.key_input_indices),
	      residual_probe_source_indices(other.residual_probe_source_indices),
	      has_prepared_plan(other.has_prepared_plan) {
		if (has_prepared_plan) {
			prepared_plan = other.prepared_plan.Copy(false);
		}
	}

	SljitHashJoinProbeInputRemap &operator=(const SljitHashJoinProbeInputRemap &other) {
		if (this == &other) {
			return *this;
		}
		key_input_indices = other.key_input_indices;
		residual_probe_source_indices = other.residual_probe_source_indices;
		has_prepared_plan = other.has_prepared_plan;
		if (has_prepared_plan) {
			prepared_plan = other.prepared_plan.Copy(false);
		} else {
			prepared_plan = SljitNativeHashJoinProbePlan();
		}
		return *this;
	}

	SljitHashJoinProbeInputRemap(SljitHashJoinProbeInputRemap &&) = default;
	SljitHashJoinProbeInputRemap &operator=(SljitHashJoinProbeInputRemap &&) = default;

	bool HasKeyInputRemap() const {
		return !key_input_indices.empty();
	}

	bool HasResidualProbeSourceRemap() const {
		return !residual_probe_source_indices.empty();
	}

	bool HasRemap() const {
		return HasKeyInputRemap() || HasResidualProbeSourceRemap();
	}
};

struct SljitHashJoinProbeMaterializePrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
};

struct SljitHashJoinProbeSelectionPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	bool source_key0_int64_to_int32_unchecked = false;
	SljitHashJoinProbeInputRemap input_remap;
	vector<idx_t> output_column_map;
	idx_t output_projection_idx = DConstants::INVALID_INDEX;

	bool HasOutputColumnMap() const {
		return !output_column_map.empty();
	}
};

struct SljitMarkProbeFilterBoundaryPrimitive {
	idx_t hash_join_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	idx_t downstream_projection_idx = DConstants::INVALID_INDEX;
	bool apply_filter_selection = false;
	bool materialize_filter_selection = false;
	bool allow_marker_omission = false;
};

static bool SljitCanBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::NONE &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
}

static SljitHashJoinProbeMaterializePrimitive
SljitBindHashJoinProbeMaterializePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                           bool source_key0_int64_to_int32_unchecked = false) {
	if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
		throw InternalException("SLJIT hash join materialize primitive cannot bind requested operator");
	}
	SljitHashJoinProbeMaterializePrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	return primitive;
}

static bool SljitCanBindHashJoinProbeSelectionPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                        idx_t hash_join_idx) {
	return hash_join_idx < ops.size() && ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       (ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	        ops[hash_join_idx].hash_join_probe.plan.output_mode ==
	            ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY);
}

static bool SljitPreparedHashJoinRemapKeyMatchesType(SljitNativeHashJoinKeyKind key_kind,
                                                     PhysicalType physical_type) {
	switch (key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return physical_type == PhysicalType::INT8;
	case SljitNativeHashJoinKeyKind::INT16:
		return physical_type == PhysicalType::INT16;
	case SljitNativeHashJoinKeyKind::INT32:
		return physical_type == PhysicalType::INT32;
	case SljitNativeHashJoinKeyKind::INT64:
		return physical_type == PhysicalType::INT64;
	case SljitNativeHashJoinKeyKind::INT128:
		return physical_type == PhysicalType::INT128;
	case SljitNativeHashJoinKeyKind::UINT8:
		return physical_type == PhysicalType::UINT8;
	case SljitNativeHashJoinKeyKind::UINT16:
		return physical_type == PhysicalType::UINT16;
	case SljitNativeHashJoinKeyKind::UINT32:
		return physical_type == PhysicalType::UINT32;
	case SljitNativeHashJoinKeyKind::UINT64:
		return physical_type == PhysicalType::UINT64;
	case SljitNativeHashJoinKeyKind::UINT128:
		return physical_type == PhysicalType::UINT128;
	default:
		return false;
	}
}

static bool SljitPreparedHashJoinRemapKeySourceSupported(const SljitNativeHashJoinProbeKeyPlan &key, idx_t key_idx,
                                                         PhysicalType source_type) {
	if (SljitPreparedHashJoinRemapKeyMatchesType(key.key_kind, source_type)) {
		return true;
	}
	return key_idx == 0 && key.key_kind == SljitNativeHashJoinKeyKind::INT32 && source_type == PhysicalType::INT64;
}

static void SljitApplyPreparedHashJoinResidualProbeSourceRemap(ExecutionRegionOperatorInfo &operator_info,
                                                               const vector<LogicalType> &input_types,
                                                               const vector<idx_t> &residual_probe_source_indices) {
	if (residual_probe_source_indices.empty()) {
		return;
	}
	auto &residual_sources = operator_info.hash_join_contract.residual_sources;
	if (residual_probe_source_indices.size() != residual_sources.size()) {
		throw InternalException("SLJIT prepared hash join residual source count mismatch");
	}
	for (auto &source : residual_sources) {
		if (source.kind == ExecutionHashJoinResidualSourceKind::BUILD) {
			continue;
		}
		if (source.kind != ExecutionHashJoinResidualSourceKind::PROBE ||
		    source.source_index >= residual_probe_source_indices.size()) {
			throw InternalException("SLJIT prepared hash join residual source shape mismatch");
		}
		const auto input_idx = residual_probe_source_indices[source.source_index];
		if (input_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (input_idx >= input_types.size()) {
			throw InternalException("SLJIT prepared hash join residual probe source is out of range");
		}
		if (input_types[input_idx] != source.type) {
			throw InternalException("SLJIT prepared hash join residual probe source type mismatch");
		}
		source.input_index = input_idx;
	}
}

static void SljitPrepareHashJoinProbeInputRemap(const SljitNativeHashJoinProbePlan &plan,
                                                const vector<LogicalType> &input_types,
                                                SljitHashJoinProbeInputRemap &input_remap) {
	if (!input_remap.HasRemap()) {
		return;
	}
	input_remap.prepared_plan = plan.Copy(false);
	input_remap.prepared_plan.input_types = input_types;
	if (input_remap.HasKeyInputRemap()) {
		if (input_remap.key_input_indices.size() != plan.keys.size() ||
		    plan.operator_info.hash_join_keys.size() != plan.keys.size()) {
			throw InternalException("SLJIT prepared hash join probe key count mismatch");
		}
		for (idx_t key_idx = 0; key_idx < input_remap.prepared_plan.keys.size(); key_idx++) {
			const auto input_idx = input_remap.key_input_indices[key_idx];
			if (input_idx >= input_types.size()) {
				throw InternalException("SLJIT prepared hash join probe key input is out of range");
			}
			auto &key = input_remap.prepared_plan.keys[key_idx];
			if (!SljitPreparedHashJoinRemapKeySourceSupported(key, key_idx, input_types[input_idx].InternalType())) {
				throw InternalException("SLJIT prepared hash join probe key source type is unsupported");
			}
			key.key_input_index = input_idx;
			input_remap.prepared_plan.operator_info.hash_join_keys[key_idx].input_index = input_idx;
		}
	}
	SljitApplyPreparedHashJoinResidualProbeSourceRemap(input_remap.prepared_plan.operator_info, input_types,
	                                                   input_remap.residual_probe_source_indices);
	input_remap.has_prepared_plan = true;
}

static SljitHashJoinProbeSelectionPrimitive SljitBindHashJoinProbeSelectionPrimitive(
    const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeInputRemap input_remap = SljitHashJoinProbeInputRemap(), vector<idx_t> output_column_map = {},
    idx_t output_projection_idx = DConstants::INVALID_INDEX,
    optional_ptr<const vector<LogicalType>> remapped_input_types = nullptr) {
	if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		throw InternalException("SLJIT hash join selection primitive cannot bind requested operator");
	}
	if (output_projection_idx != DConstants::INVALID_INDEX &&
	    (output_projection_idx >= ops.size() ||
	     ops[output_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		throw InternalException("SLJIT hash join selection primitive cannot bind output producer projection");
	}
	if (input_remap.HasRemap()) {
		if (!remapped_input_types) {
			throw InternalException("SLJIT hash join selection remap requires prepared input types");
		}
		SljitPrepareHashJoinProbeInputRemap(ops[hash_join_idx].hash_join_probe.plan, *remapped_input_types,
		                                    input_remap);
	}
	SljitHashJoinProbeSelectionPrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32_unchecked;
	primitive.input_remap = std::move(input_remap);
	primitive.output_column_map = std::move(output_column_map);
	primitive.output_projection_idx = output_projection_idx;
	return primitive;
}

static bool SljitCanBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                         idx_t hash_join_idx, idx_t filter_idx) {
	return hash_join_idx < ops.size() && filter_idx < ops.size() &&
	       ops[hash_join_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
	       ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
}

static bool SljitCanBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                         const SljitMarkProbeFilterBoundaryPrimitive &primitive) {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, primitive.hash_join_idx, primitive.filter_idx)) {
		return false;
	}
	if (primitive.downstream_projection_idx != DConstants::INVALID_INDEX &&
	    (primitive.downstream_projection_idx >= ops.size() ||
	     ops[primitive.downstream_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	if (primitive.allow_marker_omission && !primitive.apply_filter_selection) {
		return false;
	}
	if (primitive.materialize_filter_selection && !primitive.apply_filter_selection) {
		return false;
	}
	return true;
}

static SljitMarkProbeFilterBoundaryPrimitive
SljitBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                          idx_t filter_idx, bool apply_filter_selection = false,
                                          idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
                                          bool allow_marker_omission = false,
                                          bool materialize_filter_selection = false) {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx)) {
		throw InternalException("SLJIT MARK probe filter boundary primitive cannot bind requested operators");
	}
	if (downstream_projection_idx != DConstants::INVALID_INDEX &&
	    (downstream_projection_idx >= ops.size() ||
	     ops[downstream_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		throw InternalException("SLJIT MARK probe filter boundary primitive cannot bind downstream projection");
	}
	if (allow_marker_omission && !apply_filter_selection) {
		throw InternalException("SLJIT MARK probe filter boundary marker omission requires an applied filter");
	}
	if (materialize_filter_selection && !apply_filter_selection) {
		throw InternalException("SLJIT MARK probe filter boundary materialization requires an applied filter");
	}
	SljitMarkProbeFilterBoundaryPrimitive primitive;
	primitive.hash_join_idx = hash_join_idx;
	primitive.filter_idx = filter_idx;
	primitive.apply_filter_selection = apply_filter_selection;
	primitive.materialize_filter_selection = materialize_filter_selection;
	primitive.allow_marker_omission = allow_marker_omission;
	primitive.downstream_projection_idx = downstream_projection_idx;
	return primitive;
}

} // namespace duckdb

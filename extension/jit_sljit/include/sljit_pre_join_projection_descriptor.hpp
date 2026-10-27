//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pre_join_projection_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_region_executable.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

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

static bool SljitTryBindElidedHashJoinProbeSelection(const vector<SljitExecutableRegionOp> &ops, idx_t projection_idx,
                                                     idx_t hash_join_idx, const vector<Value> &source_min_values,
                                                     const vector<Value> &source_max_values,
                                                     SljitHashJoinProbeSelectionPrimitive &primitive) {
	if (projection_idx >= ops.size() || !SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return false;
	}
	const auto &projection = ops[projection_idx];
	if (projection.kind != SljitNativeRegionOpKind::PROJECTION || projection.projections.empty() ||
	    projection.projections.size() != projection.output_types.size()) {
		return false;
	}
	vector<SljitBatchLayoutColumn> columns;
	columns.reserve(projection.projections.size());
	bool source_layout_preserved = true;
	for (idx_t op_idx = 0; op_idx < projection_idx; op_idx++) {
		source_layout_preserved &= ops[op_idx].kind == SljitNativeRegionOpKind::FILTER;
	}
	for (idx_t output_idx = 0; output_idx < projection.projections.size(); output_idx++) {
		const auto &expression = projection.projections[output_idx].plan;
		if (expression.source_index >= projection.input_types.size() ||
		    expression.return_type != projection.output_types[output_idx]) {
			return false;
		}
		const auto &source_type = projection.input_types[expression.source_index];
		if (expression.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (source_type != expression.return_type) {
				return false;
			}
		} else if (expression.kind != SljitNativeRegionExpressionKind::INTEGER_CAST || expression.try_cast ||
		           expression.cast_source_width != SljitNativeSignedIntegerWidth::INT64 ||
		           expression.cast_target_width != SljitNativeSignedIntegerWidth::INT32 ||
		           source_type.InternalType() != PhysicalType::INT64 ||
		           expression.return_type.InternalType() != PhysicalType::INT32) {
			return false;
		}
		// Selection may discard any row, including before a probe key is read. Only defer a proven-total cast.
		if (expression.kind != SljitNativeRegionExpressionKind::REFERENCE &&
		    (!source_layout_preserved ||
		     !SljitSourceRangeFitsInt32(expression.source_index, source_min_values, source_max_values))) {
			return false;
		}
		columns.push_back({expression.source_index, source_type, expression.return_type});
	}

	const auto &join = ops[hash_join_idx].hash_join_probe.plan;
	bool needs_input_plan = join.residual_predicate;
	for (idx_t key_idx = 0; key_idx < join.keys.size(); key_idx++) {
		const auto &key = join.keys[key_idx];
		if (key.key_input_index >= columns.size()) {
			return false;
		}
		const auto &column = columns[key.key_input_index];
		if (column.type != key.key_type) {
			return false;
		}
		if (column.source_type == column.type) {
			if (!SljitHashJoinKeyKindMatchesPhysicalType(key.key_kind, column.source_type.InternalType())) {
				return false;
			}
		} else if (key.key_input_index != 0 ||
		           !SljitHashJoinKeyCanUseInt64SourceForInt32Key(key_idx, key.key_kind,
		                                                         column.source_type.InternalType()) ||
		           projection.projections[key.key_input_index].plan.cast_chain.steps.empty()) {
			return false;
		}
		needs_input_plan |= key.key_input_index != column.source_index || column.source_type != column.type;
	}
	const auto &residual_sources = join.operator_info.hash_join_contract.residual_sources;
	if (join.residual_predicate) {
		for (const auto &source : residual_sources) {
			if (source.source_index >= residual_sources.size()) {
				return false;
			}
			if (source.kind == ExecutionHashJoinResidualSourceKind::BUILD) {
				continue;
			}
			if (source.kind != ExecutionHashJoinResidualSourceKind::PROBE || source.input_index >= columns.size()) {
				return false;
			}
			const auto &column = columns[source.input_index];
			if (column.source_type != source.type || column.type != source.type) {
				return false;
			}
		}
	}
	SljitHashJoinProbeSelectionPrimitive candidate;
	candidate.hash_join_idx = hash_join_idx;
	candidate.output_layout =
	    make_shared_ptr<SljitBatchLayout>(projection_idx, projection.input_types, std::move(columns));
	const auto &layout = *candidate.output_layout;
	if (!join.keys.empty()) {
		const auto &key0 = layout.Column(join.keys[0].key_input_index);
		candidate.source_key0_int64_to_int32_unchecked =
		    key0.source_type != key0.type &&
		    SljitSourceRangeFitsInt32(key0.source_index, source_min_values, source_max_values);
	}
	if (needs_input_plan) {
		if (join.operator_info.hash_join_keys.size() != join.keys.size()) {
			return false;
		}
		auto input_plan = make_shared_ptr<SljitNativeHashJoinProbePlan>(join.Copy(false));
		input_plan->input_types = projection.input_types;
		for (idx_t key_idx = 0; key_idx < input_plan->keys.size(); key_idx++) {
			auto &key = input_plan->keys[key_idx];
			const auto &column = layout.Column(key.key_input_index);
			if (column.source_type != column.type) {
				key.source_cast = projection.projections[key.key_input_index].plan.cast_chain;
			}
			key.key_input_index = column.source_index;
			input_plan->operator_info.hash_join_keys[key_idx].input_index = column.source_index;
		}
		if (join.residual_predicate) {
			for (auto &source : input_plan->operator_info.hash_join_contract.residual_sources) {
				if (source.kind == ExecutionHashJoinResidualSourceKind::PROBE) {
					source.input_index = layout.Column(source.input_index).source_index;
				}
			}
		}
		candidate.input_plan = std::move(input_plan);
	}
	primitive = std::move(candidate);
	return true;
}

static bool SljitHashJoinSourceKey0RangeFitsInt32(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                  const vector<Value> &source_min_values,
                                                  const vector<Value> &source_max_values) {
	if (hash_join_idx >= ops.size()) {
		return false;
	}
	// The source ranges bound the probe key only while the probe reads the source layout: selection-only filters
	// may precede it, any other operator changes the layout.
	for (idx_t op_idx = 0; op_idx < hash_join_idx; op_idx++) {
		if (ops[op_idx].kind != SljitNativeRegionOpKind::FILTER) {
			return false;
		}
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

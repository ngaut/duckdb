//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_row_pointer_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_row_pointer_preaggregation_analysis.hpp"
#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"

namespace duckdb {

template <class START_GROUP, class VISIT_ROW>
static bool SljitForEachPreaggregatedRowPointerGroup(DataChunk &payload_input, Vector &row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     vector<UnifiedVectorFormat> &input_group_formats,
                                                     START_GROUP &&start_group, VISIT_ROW &&visit_row,
                                                     idx_t &group_count) {
	group_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto preaggregation_decision = SljitChooseRowPointerPreaggregationStrategy(payload_input, row_pointers, count,
	                                                                           group_sources, input_group_formats);
	if (!preaggregation_decision.UseConsecutiveGroups() && !preaggregation_decision.UseLocalDistinctGroups()) {
		return false;
	}

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const bool uses_input_vector_groups = SljitRowPointerPreaggregationUsesInputVectorGroups(group_sources);
	vector<ExecutionRowPointerGroupKeySource> row_pointer_group_sources;
	if (!SljitTryPrepareRowPointerGroupSources(group_sources, row_pointer_group_sources)) {
		return false;
	}
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(row_pointer_group_sources);
	data_ptr_t active_row_pointer = nullptr;
	idx_t active_row_idx = DConstants::INVALID_INDEX;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> local_group_row_pointers;
	std::array<idx_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> local_group_row_indices;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		idx_t group_idx = group_count;
		if (preaggregation_decision.UseConsecutiveGroups()) {
			bool same_group = false;
			if (has_active_row_pointer) {
				same_group = SljitRowPointerPreaggregationRowsEqual(
				    active_row_pointer, active_row_idx, row_pointer, row_idx, uses_input_vector_groups,
				    same_row_pointer_is_equal, group_sources, row_pointer_group_sources, input_group_formats);
			}
			if (!same_group) {
				active_row_pointer = row_pointer;
				active_row_idx = row_idx;
				has_active_row_pointer = true;
				if (!start_group(row_idx, row_pointer, group_count)) {
					return false;
				}
				group_count++;
			}
			group_idx = group_count - 1;
		} else {
			bool found_group = false;
			for (idx_t candidate_idx = 0; candidate_idx < group_count; candidate_idx++) {
				if (SljitRowPointerPreaggregationRowsEqual(
				        local_group_row_pointers[candidate_idx], local_group_row_indices[candidate_idx], row_pointer,
				        row_idx, uses_input_vector_groups, same_row_pointer_is_equal, group_sources,
				        row_pointer_group_sources, input_group_formats)) {
					group_idx = candidate_idx;
					found_group = true;
					break;
				}
			}
			if (!found_group) {
				if (group_count >= SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS) {
					return false;
				}
				if (!start_group(row_idx, row_pointer, group_count)) {
					return false;
				}
				local_group_row_pointers[group_count] = row_pointer;
				local_group_row_indices[group_count] = row_idx;
				group_idx = group_count;
				group_count++;
			}
		}
		if (!visit_row(row_idx, group_idx)) {
			return false;
		}
	}
	return group_count != count;
}

static bool SljitTryPreaggregateRowPointerPrimitiveGroups(
    DataChunk &payload_input, Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t &compact_count, const char *&failure_reason) {
	compact_count = 0;
	failure_reason = "shape";
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}

	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(payload_input, payload_source_indices, payload_lanes)) {
		failure_reason = "payload_sources";
		return false;
	}
	scratch.Prepare(payload_lanes, count);
	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto start_group = [&](idx_t row_idx, data_ptr_t row_pointer, idx_t group_idx) {
		compact_row_pointer_data[group_idx] = row_pointer;
		scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
		return SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes);
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		return SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                         group_idx);
	};
	idx_t group_count;
	if (!SljitForEachPreaggregatedRowPointerGroup(payload_input, row_pointers, group_sources,
	                                              scratch.input_group_formats, start_group, visit_row, group_count)) {
		failure_reason = "groups";
		return false;
	}
	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}

static bool SljitTryPreaggregateRowPointerFusedPrimitiveGroups(
    SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    idx_t &compact_count, const char *&failure_reason) {
	compact_count = 0;
	failure_reason = "shape";
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !op.aggregate_update.fused_payload_update_function) {
		return false;
	}

	if (!SljitPrepareFusedPreaggregatedPrimitiveScratch(scratch, payload_lanes, count, count)) {
		failure_reason = "scratch";
		return false;
	}

	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto start_group = [&](idx_t row_idx, data_ptr_t row_pointer, idx_t group_idx) {
		compact_row_pointer_data[group_idx] = row_pointer;
		scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
		scratch.group_row_counts.push_back(0);
		return true;
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		D_ASSERT(group_idx < scratch.group_row_counts.size());
		scratch.group_row_counts[group_idx]++;
		scratch.fused_row_state_addresses[row_idx] = SljitFusedPreaggregatedPrimitiveStateAddress(scratch, group_idx);
		return true;
	};
	idx_t group_count;
	if (!SljitForEachPreaggregatedRowPointerGroup(payload_input, row_pointers, group_sources,
	                                              scratch.input_group_formats, start_group, visit_row, group_count)) {
		failure_reason = "groups";
		return false;
	}

	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function,
	    op.aggregate_update.plan.sink_info.aggregates, op.aggregate_update.plan.sink_info.aggregate_contract,
	    payload_lanes, payload_input, scratch.fused_row_state_addresses.data(), nullptr, nullptr, false, count,
	    payload_scratch, optional_ptr<const vector<idx_t>>(&payload_source_indices));

	if (!SljitExtractFusedPreaggregatedPrimitiveDeltas(scratch, payload_lanes, group_count)) {
		failure_reason = "extract";
		return false;
	}

	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}
} // namespace duckdb

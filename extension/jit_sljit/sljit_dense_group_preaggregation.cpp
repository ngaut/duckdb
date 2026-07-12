#include "sljit_dense_group_preaggregation.hpp"

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"

#include <array>
#include <type_traits>

namespace duckdb {

template <class GROUP_TYPE>
static bool SljitDensePrimitiveGroupRange(GROUP_TYPE min_key, GROUP_TYPE max_key, idx_t &range) {
	if constexpr (std::is_same<GROUP_TYPE, hugeint_t>::value || std::is_same<GROUP_TYPE, uhugeint_t>::value) {
		return false;
	} else {
		if (max_key < min_key) {
			return false;
		}
		auto key = min_key;
		range = 1;
		while (key < max_key) {
			if (range == SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT) {
				return false;
			}
			key = static_cast<GROUP_TYPE>(key + static_cast<GROUP_TYPE>(1));
			range++;
		}
		return true;
	}
}

template <class GROUP_TYPE>
static bool
TryBuildDensePrimitiveGroupsTemplated(DataChunk &input, idx_t group_source_index, DataChunk &compact_groups,
                                      std::array<sel_t, SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT> &row_group_indices,
                                      idx_t &group_count) {
	if constexpr (std::is_same<GROUP_TYPE, hugeint_t>::value || std::is_same<GROUP_TYPE, uhugeint_t>::value) {
		return false;
	} else {
		const auto count = input.size();
		group_count = 0;
		if (count < 2 || count > SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT ||
		    group_source_index >= input.ColumnCount() || compact_groups.ColumnCount() != 1) {
			return false;
		}

		UnifiedVectorFormat group_format;
		input.data[group_source_index].ToUnifiedFormat(group_format);
		auto group_data = UnifiedVectorFormat::GetData<GROUP_TYPE>(group_format);
		auto group_sel = group_format.sel;
		auto &group_validity = group_format.validity;
		const bool can_have_null = group_validity.CanHaveNull();
		const auto sample_count = MinValue<idx_t>(count, 64);
		const auto first_source_idx = group_sel->get_index(0);
		if (can_have_null && !group_validity.RowIsValid(first_source_idx)) {
			return false;
		}
		GROUP_TYPE min_key = group_data[first_source_idx];
		GROUP_TYPE max_key = min_key;
		for (idx_t sample_idx = 1; sample_idx < sample_count; sample_idx++) {
			const auto row_idx = sample_idx * (count - 1) / (sample_count - 1);
			const auto source_idx = group_sel->get_index(row_idx);
			if (can_have_null && !group_validity.RowIsValid(source_idx)) {
				return false;
			}
			const auto key = group_data[source_idx];
			min_key = MinValue(min_key, key);
			max_key = MaxValue(max_key, key);
		}
		idx_t sample_range;
		if (!SljitDensePrimitiveGroupRange(min_key, max_key, sample_range) ||
		    count < sample_range * SLJIT_LOCAL_DENSE_PREAGGREGATION_MIN_COMPRESSION) {
			return false;
		}
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			if (can_have_null && !group_validity.RowIsValid(source_idx)) {
				return false;
			}
			const auto key = group_data[source_idx];
			min_key = MinValue(min_key, key);
			max_key = MaxValue(max_key, key);
		}
		idx_t range;
		if (!SljitDensePrimitiveGroupRange(min_key, max_key, range) ||
		    count < range * SLJIT_LOCAL_DENSE_PREAGGREGATION_MIN_COMPRESSION) {
			return false;
		}
		std::array<idx_t, SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT> group_indices;
		group_indices.fill(DConstants::INVALID_INDEX);
		auto target_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			const auto key = group_data[source_idx];
			const auto offset = static_cast<idx_t>(key - min_key);
			auto group_idx = group_indices[offset];
			if (group_idx == DConstants::INVALID_INDEX) {
				group_idx = group_count++;
				group_indices[offset] = group_idx;
				target_data[group_idx] = key;
			}
			row_group_indices[row_idx] = static_cast<sel_t>(group_idx);
		}
		FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
		return true;
	}
	return false;
}

static bool
TryBuildDensePrimitiveGroups(DataChunk &input, idx_t group_source_index, DataChunk &compact_groups,
                             std::array<sel_t, SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT> &row_group_indices,
                             idx_t &group_count) {
	if (group_source_index >= input.ColumnCount() || compact_groups.ColumnCount() != 1 ||
	    compact_groups.data[0].GetType() != input.data[group_source_index].GetType()) {
		return false;
	}
	switch (input.data[group_source_index].GetType().InternalType()) {
	case PhysicalType::INT8:
		return TryBuildDensePrimitiveGroupsTemplated<int8_t>(input, group_source_index, compact_groups,
		                                                     row_group_indices, group_count);
	case PhysicalType::INT16:
		return TryBuildDensePrimitiveGroupsTemplated<int16_t>(input, group_source_index, compact_groups,
		                                                      row_group_indices, group_count);
	case PhysicalType::INT32:
		return TryBuildDensePrimitiveGroupsTemplated<int32_t>(input, group_source_index, compact_groups,
		                                                      row_group_indices, group_count);
	case PhysicalType::INT64:
		return TryBuildDensePrimitiveGroupsTemplated<int64_t>(input, group_source_index, compact_groups,
		                                                      row_group_indices, group_count);
	case PhysicalType::UINT8:
		return TryBuildDensePrimitiveGroupsTemplated<uint8_t>(input, group_source_index, compact_groups,
		                                                      row_group_indices, group_count);
	case PhysicalType::UINT16:
		return TryBuildDensePrimitiveGroupsTemplated<uint16_t>(input, group_source_index, compact_groups,
		                                                       row_group_indices, group_count);
	case PhysicalType::UINT32:
		return TryBuildDensePrimitiveGroupsTemplated<uint32_t>(input, group_source_index, compact_groups,
		                                                       row_group_indices, group_count);
	case PhysicalType::UINT64:
		return TryBuildDensePrimitiveGroupsTemplated<uint64_t>(input, group_source_index, compact_groups,
		                                                       row_group_indices, group_count);
	default:
		return false;
	}
}

bool TryPreaggregateDensePrimitiveGroups(SljitExecutableRegionOp &op, DataChunk &input,
                                         const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                         DataChunk &compact_groups,
                                         SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1) {
		return false;
	}
	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(op, input, payload_lanes)) {
		return false;
	}
	std::array<sel_t, SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT> row_group_indices;
	idx_t group_count;
	if (!TryBuildDensePrimitiveGroups(input, sink_info.groups[0].input_index, compact_groups, row_group_indices,
	                                  group_count)) {
		return false;
	}
	scratch.Prepare(payload_lanes, group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		if (!SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes)) {
			return false;
		}
	}
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                       row_group_indices[row_idx])) {
			return false;
		}
	}
	return true;
}

bool TryPreaggregateDenseFusedPrimitiveGroups(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch) {
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1) {
		return false;
	}
	vector<idx_t> payload_source_indices;
	if (!SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregate_update.payload_descriptors,
	                                                 payload_source_indices) ||
	    !SljitCanPreaggregateInputVectorFusedPrimitivePayloads(op, input, payload_source_indices, reduction_lanes)) {
		return false;
	}
	std::array<sel_t, SLJIT_LOCAL_DENSE_PREAGGREGATED_GROUP_LIMIT> row_group_indices;
	idx_t group_count;
	if (!TryBuildDensePrimitiveGroups(input, sink_info.groups[0].input_index, compact_groups, row_group_indices,
	                                  group_count) ||
	    !SljitPrepareFusedPreaggregatedPrimitiveScratch(scratch, payload_lanes, group_count, input.size())) {
		return false;
	}
	scratch.group_row_counts.resize(group_count, 0);
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		const auto group_idx = row_group_indices[row_idx];
		D_ASSERT(group_idx < scratch.group_row_counts.size());
		scratch.group_row_counts[group_idx]++;
		scratch.fused_row_state_addresses[row_idx] = SljitFusedPreaggregatedPrimitiveStateAddress(scratch, group_idx);
	}
	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    aggregate_update.payloads, aggregate_update.fused_payload_update.Function(), sink_info.aggregate_contract,
	    aggregate_update.payload_descriptors, payload_lanes, reduction_lanes, input,
	    scratch.fused_row_state_addresses.data(), nullptr, nullptr, false, input.size(), payload_scratch,
	    optional_ptr<const vector<idx_t>>(&payload_source_indices));
	if (!SljitExtractFusedPreaggregatedPrimitiveDeltas(scratch, payload_lanes, group_count)) {
		throw InternalException("SLJIT dense fused preaggregated primitive delta extraction failed");
	}
	return true;
}

} // namespace duckdb

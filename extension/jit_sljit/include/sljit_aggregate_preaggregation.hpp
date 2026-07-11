//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_flat_single_preaggregation.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"
#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_dense_group_preaggregation.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include <array>
#include <type_traits>

namespace duckdb {

template <class GROUP_TYPE>
static bool TryPreaggregateBoundedPrimitiveGroupsTemplated(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	const auto count = input.size();
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (count < 2 || sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
		return false;
	}
	const auto group_source_index = sink_info.groups[0].input_index;
	if (group_source_index >= input.ColumnCount()) {
		return false;
	}

	UnifiedVectorFormat group_format;
	input.data[group_source_index].ToUnifiedFormat(group_format);
	auto group_data = UnifiedVectorFormat::GetData<GROUP_TYPE>(group_format);
	auto group_sel = group_format.sel;
	auto &group_validity = group_format.validity;
	const bool can_have_null = group_validity.CanHaveNull();

	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(op, input, payload_lanes)) {
		return false;
	}
	scratch.Prepare(payload_lanes, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT);

	std::array<GROUP_TYPE, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
	auto target_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);
	idx_t group_count = 0;

	auto append_group = [&](GROUP_TYPE key) {
		if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
			return DConstants::INVALID_INDEX;
		}
		if (!SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes)) {
			return DConstants::INVALID_INDEX;
		}
		const auto group_idx = group_count++;
		keys[group_idx] = key;
		target_data[group_idx] = key;
		return group_idx;
	};

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = group_sel->get_index(row_idx);
		if (can_have_null && !group_validity.RowIsValid(source_idx)) {
			return false;
		}
		auto key = group_data[source_idx];
		idx_t group_idx = 0;
		for (; group_idx < group_count; group_idx++) {
			if (keys[group_idx] == key) {
				break;
			}
		}
		if (group_idx == group_count) {
			group_idx = append_group(key);
			if (group_idx == DConstants::INVALID_INDEX) {
				return false;
			}
		}
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                       group_idx)) {
			return false;
		}
	}
	if (group_count == count) {
		return false;
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
	return true;
}

template <class T>
static bool TryPreaggregateConsecutivePrimitiveGroupsTemplated(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	const auto count = input.size();
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (count < 2 || sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
		return false;
	}
	const auto group_source_index = sink_info.groups[0].input_index;
	if (group_source_index >= input.ColumnCount()) {
		return false;
	}

	UnifiedVectorFormat group_format;
	input.data[group_source_index].ToUnifiedFormat(group_format);
	auto group_data = UnifiedVectorFormat::GetData<T>(group_format);
	auto group_sel = group_format.sel;
	auto &group_validity = group_format.validity;
	const auto sample_count = MinValue<idx_t>(count, 64);
	bool has_consecutive_repeat = false;
	bool monotonic_nondecreasing = true;
	auto previous_key = group_data[group_sel->get_index(0)];
	if (!group_validity.RowIsValid(group_sel->get_index(0))) {
		return false;
	}
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		const auto source_idx = group_sel->get_index(row_idx);
		if (!group_validity.RowIsValid(source_idx)) {
			return false;
		}
		auto key = group_data[source_idx];
		if (key == previous_key) {
			has_consecutive_repeat = true;
		}
		if (key < previous_key) {
			monotonic_nondecreasing = false;
		}
		previous_key = key;
	}
	const bool prefer_consecutive_runs = monotonic_nondecreasing && has_consecutive_repeat;
	if (!prefer_consecutive_runs &&
	    TryPreaggregateDensePrimitiveGroups(op, input, payload_lanes, compact_groups, scratch)) {
		return true;
	}
	if (!prefer_consecutive_runs &&
	    TryPreaggregateBoundedPrimitiveGroupsTemplated<T>(op, input, payload_lanes, compact_groups, scratch)) {
		return true;
	}
	if (TryPreaggregateFlatAllValidSinglePrimitiveGroup<T>(op, input, payload_lanes, compact_groups, scratch)) {
		return true;
	}
	if (!has_consecutive_repeat) {
		return false;
	}

	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(op, input, payload_lanes)) {
		return false;
	}
	scratch.Prepare(payload_lanes, count);

	auto target_data = PrepareFlatPreaggregatedGroupTarget<T>(compact_groups);
	idx_t group_count = 0;
	T active_key {};
	bool has_active_key = false;

	auto start_group = [&](T key) {
		if (!SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes)) {
			return false;
		}
		active_key = key;
		has_active_key = true;
		target_data[group_count] = key;
		group_count++;
		return true;
	};

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = group_sel->get_index(row_idx);
		if (!group_validity.RowIsValid(source_idx)) {
			return false;
		}
		auto key = group_data[source_idx];
		if (!has_active_key || !(key == active_key)) {
			if (!start_group(key)) {
				return false;
			}
		}
		const auto payload_group_idx = group_count - 1;
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                       payload_group_idx)) {
			return false;
		}
	}
	if (group_count == count) {
		return false;
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
	return true;
}

static bool
TryPreaggregateConsecutivePrimitiveGroups(SljitExecutableRegionOp &op, DataChunk &input,
                                          const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                          DataChunk &compact_groups,
                                          SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
		return false;
	}
	const auto group_source_index = sink_info.groups[0].input_index;
	if (group_source_index >= input.ColumnCount() ||
	    input.data[group_source_index].GetType() != sink_info.groups[0].type ||
	    compact_groups.data[0].GetType() != sink_info.groups[0].type) {
		return false;
	}
	switch (input.data[group_source_index].GetType().InternalType()) {
	case PhysicalType::INT8:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int8_t>(op, input, payload_lanes, compact_groups,
		                                                                  scratch);
	case PhysicalType::INT16:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int16_t>(op, input, payload_lanes, compact_groups,
		                                                                   scratch);
	case PhysicalType::INT32:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int32_t>(op, input, payload_lanes, compact_groups,
		                                                                   scratch);
	case PhysicalType::INT64:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int64_t>(op, input, payload_lanes, compact_groups,
		                                                                   scratch);
	case PhysicalType::INT128:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<hugeint_t>(op, input, payload_lanes, compact_groups,
		                                                                     scratch);
	case PhysicalType::UINT8:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint8_t>(op, input, payload_lanes, compact_groups,
		                                                                   scratch);
	case PhysicalType::UINT16:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint16_t>(op, input, payload_lanes, compact_groups,
		                                                                    scratch);
	case PhysicalType::UINT32:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint32_t>(op, input, payload_lanes, compact_groups,
		                                                                    scratch);
	case PhysicalType::UINT64:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint64_t>(op, input, payload_lanes, compact_groups,
		                                                                    scratch);
	case PhysicalType::UINT128:
		return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uhugeint_t>(op, input, payload_lanes, compact_groups,
		                                                                      scratch);
	default:
		return false;
	}
}

struct SljitPreaggregatedInputVectorGroupKeySource {
	const ExecutionRowPointerGroupKeySource *source = nullptr;
	UnifiedVectorFormat format;
};

static bool SljitPreparePreaggregatedInputVectorGroupKeySource(DataChunk &input,
                                                               const ExecutionRowPointerGroupKeySource &source,
                                                               SljitPreaggregatedInputVectorGroupKeySource &prepared) {
	prepared = SljitPreaggregatedInputVectorGroupKeySource();
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.input_vector_index >= input.ColumnCount() ||
	    input.data[source.input_vector_index].GetType() != source.source_type ||
	    !SljitPreaggregationInputVectorGroupCastSupported(source)) {
		return false;
	}
	prepared.source = &source;
	input.data[source.input_vector_index].ToUnifiedFormat(prepared.format);
	return true;
}

template <class TARGET_TYPE>
static bool SljitLoadPreaggregatedInputVectorGroupKey(SljitPreaggregatedInputVectorGroupKeySource &prepared,
                                                      idx_t row_idx, TARGET_TYPE &key) {
	auto &source = *prepared.source;
	const auto source_idx = prepared.format.sel->get_index(row_idx);
	if (!prepared.format.validity.RowIsValid(source_idx)) {
		return false;
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		key = UnifiedVectorFormat::GetData<TARGET_TYPE>(prepared.format)[source_idx];
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if constexpr (std::is_same<TARGET_TYPE, int32_t>::value) {
			const auto value = UnifiedVectorFormat::GetData<int64_t>(prepared.format)[source_idx];
			if (source.unchecked_integral_cast) {
				key = static_cast<int32_t>(value);
				return true;
			}
			return TryCast::Operation<int64_t, int32_t>(value, key, false);
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if constexpr (std::is_same<TARGET_TYPE, int16_t>::value) {
			const auto value = UnifiedVectorFormat::GetData<int64_t>(prepared.format)[source_idx];
			if (source.unchecked_integral_cast) {
				key = static_cast<int16_t>(value);
				return true;
			}
			return TryCast::Operation<int64_t, int16_t>(value, key, false);
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if constexpr (std::is_same<TARGET_TYPE, int8_t>::value) {
			const auto value = UnifiedVectorFormat::GetData<int32_t>(prepared.format)[source_idx];
			if (source.unchecked_integral_cast) {
				key = static_cast<int8_t>(value);
				return true;
			}
			return TryCast::Operation<int32_t, int8_t>(value, key, false);
		}
		return false;
	default:
		return false;
	}
}

template <class TARGET_TYPE, class KEY_DISPATCH>
static bool SljitDispatchPreaggregatedInputVectorGroupKeyCast(SljitPreaggregatedInputVectorGroupKeySource &group_source,
                                                              KEY_DISPATCH &dispatch) {
	if (!group_source.source) {
		return false;
	}
	auto &source = *group_source.source;
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		return dispatch.template Execute<TARGET_TYPE, TARGET_TYPE, false>(group_source.format);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if constexpr (std::is_same<TARGET_TYPE, int32_t>::value) {
			return dispatch.template Execute<int32_t, int64_t, true>(group_source.format);
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if constexpr (std::is_same<TARGET_TYPE, int16_t>::value) {
			return dispatch.template Execute<int16_t, int64_t, true>(group_source.format);
		}
		return false;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if constexpr (std::is_same<TARGET_TYPE, int8_t>::value) {
			return dispatch.template Execute<int8_t, int32_t, true>(group_source.format);
		}
		return false;
	default:
		return false;
	}
}

template <class TARGET_DISPATCH>
static bool SljitDispatchPreaggregatedInputVectorGroupTargetType(PhysicalType target_type, TARGET_DISPATCH &dispatch) {
	switch (target_type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	case PhysicalType::UINT8:
		return dispatch.template Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.template Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.template Execute<uint32_t>();
	case PhysicalType::UINT64:
		return dispatch.template Execute<uint64_t>();
	case PhysicalType::INT128:
		return dispatch.template Execute<hugeint_t>();
	case PhysicalType::UINT128:
		return dispatch.template Execute<uhugeint_t>();
	default:
		return false;
	}
}

template <class TARGET_TYPE>
static bool TryPreaggregateInputVectorPrimitiveGroupRunsTemplated(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	const auto count = input.size();
	auto &sink_info = op.aggregate_update.plan.sink_info;
	group_count = 0;
	if (count < 2 || sink_info.groups.size() != 1 || group_sources.size() != 1 ||
	    sink_info.groups[0].type.InternalType() != group_sources[0].target_physical_type) {
		return false;
	}

	SljitPreaggregatedInputVectorGroupKeySource group_source;
	if (!SljitPreparePreaggregatedInputVectorGroupKeySource(input, group_sources[0], group_source)) {
		return false;
	}

	TARGET_TYPE previous_key;
	if (!SljitLoadPreaggregatedInputVectorGroupKey(group_source, 0, previous_key)) {
		return false;
	}
	const auto sample_count = MinValue<idx_t>(count, 64);
	bool has_consecutive_repeat = false;
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		TARGET_TYPE key;
		if (!SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key)) {
			return false;
		}
		if (key == previous_key) {
			has_consecutive_repeat = true;
			break;
		}
		previous_key = key;
	}
	if (!has_consecutive_repeat) {
		return false;
	}

	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(input, payload_source_indices, payload_lanes)) {
		return false;
	}
	scratch.Prepare(payload_lanes, count);
	TARGET_TYPE *run_key_data = nullptr;
	if (run_group_keys) {
		if (run_group_keys->ColumnCount() != 1 || run_group_keys->data[0].GetType() != sink_info.groups[0].type) {
			return false;
		}
		run_key_data = PrepareFlatPreaggregatedGroupTarget<TARGET_TYPE>(*run_group_keys);
	}

	TARGET_TYPE active_key {};
	bool has_active_key = false;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		TARGET_TYPE key;
		if (!SljitLoadPreaggregatedInputVectorGroupKey(group_source, row_idx, key)) {
			return false;
		}
		if (!has_active_key || !(key == active_key)) {
			if (!SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes)) {
				return false;
			}
			active_key = key;
			has_active_key = true;
			scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
			if (run_key_data) {
				run_key_data[group_count] = key;
			}
			group_count++;
		}
		const auto payload_group_idx = group_count - 1;
		if (!SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                       payload_group_idx)) {
			return false;
		}
	}
	if (group_count == count || group_count != scratch.group_rows.size()) {
		group_count = 0;
		return false;
	}
	if (run_group_keys) {
		FinishFlatPreaggregatedGroupTarget(*run_group_keys, group_count);
	}
	return true;
}

struct SljitInputVectorPrimitiveGroupRunsTargetDispatch {
	SljitExecutableRegionOp &op;
	DataChunk &input;
	const vector<ExecutionRowPointerGroupKeySource> &group_sources;
	const vector<idx_t> &payload_source_indices;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes;
	SljitPreaggregatedPrimitiveAggregateScratch &scratch;
	optional_ptr<DataChunk> run_group_keys;
	idx_t &group_count;

	template <class TARGET_TYPE>
	bool Execute() {
		return TryPreaggregateInputVectorPrimitiveGroupRunsTemplated<TARGET_TYPE>(
		    op, input, group_sources, payload_source_indices, payload_lanes, scratch, run_group_keys, group_count);
	}
};

static bool TryPreaggregateInputVectorPrimitiveGroupRuns(
    SljitExecutableRegionOp &op, DataChunk &input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, optional_ptr<DataChunk> run_group_keys, idx_t &group_count) {
	if (group_sources.size() != 1) {
		return false;
	}
	SljitInputVectorPrimitiveGroupRunsTargetDispatch dispatch {
	    op, input, group_sources, payload_source_indices, payload_lanes, scratch, run_group_keys, group_count};
	return SljitDispatchPreaggregatedInputVectorGroupTargetType(group_sources[0].target_physical_type, dispatch);
}

} // namespace duckdb

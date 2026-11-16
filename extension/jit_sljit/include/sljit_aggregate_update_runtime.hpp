//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_primitive_aggregate_update_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_ungrouped_aggregate_update_primitive.hpp"

namespace duckdb {

static SinkResultType
SljitExecutePrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                     SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                     DataChunk &input, const SelectionVector *execute_sel = nullptr,
                                     idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	auto &sink_info = op.AggregateUpdate().plan.sink_info;
	if (SljitGroupedPrimitiveAggregateSinkKind(sink_info.Kind())) {
		auto &bound = scratch.AggregateBoundGroupedUpdate(op_idx);
		SljitBindRecordedGroupedPrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, bound);
		return SljitExecuteBoundGroupedPrimitiveAggregateUpdate(runtime, scratch, bound, input, execute_sel, count,
		                                                        defer_grouped_finish);
	}
	SljitBoundUngroupedPrimitiveAggregateUpdate bound;
	SljitBindRecordedUngroupedPrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, bound);
	return SljitExecuteBoundUngroupedPrimitiveAggregateUpdate(runtime, scratch, bound, input, execute_sel, count);
}

template <class T>
static void SljitAccumulateDistinctGroupKey(Vector &keys, idx_t count, int64_t min_value, uint64_t span,
                                            uint64_t stride, SljitDistinctGroupScratch &scratch) {
	UnifiedVectorFormat format;
	keys.ToUnifiedFormat(format);
	auto data = UnifiedVectorFormat::GetData<T>(format);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = format.sel->get_index(row_idx);
		// a NULL or out-of-range key keeps its row: the sink sees it as it would without the bitmap
		const auto offset =
		    static_cast<uint64_t>(static_cast<int64_t>(data[source_idx])) - static_cast<uint64_t>(min_value);
		const bool in_domain = format.validity.RowIsValid(source_idx) && offset < span;
		scratch.in_domain[row_idx] = scratch.in_domain[row_idx] && in_domain;
		scratch.domain_index[row_idx] += in_domain ? offset * stride : 0;
	}
}

//! Selects the rows whose group this thread has not handed to the sink yet, marking those groups seen; returns false
//! when a group key's type has no bounded domain (every row then goes to the sink)
static bool SljitSelectNewDistinctGroups(const SljitExecutableAggregateUpdate &aggregate_update, DataChunk &input,
                                         SljitDistinctGroupScratch &scratch, idx_t &new_count) {
	const auto count = input.size();
	auto &groups = aggregate_update.plan.sink_info.groups;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		scratch.domain_index[row_idx] = 0;
		scratch.in_domain[row_idx] = true;
	}
	uint64_t stride = 1;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &range = aggregate_update.integral_group_key_ranges[group_idx];
		auto &keys = input.data[groups[group_idx].input_index];
		const auto span = static_cast<uint64_t>(range.max_value) - static_cast<uint64_t>(range.min_value) + 1;
		switch (keys.GetType().InternalType()) {
		case PhysicalType::BOOL:
			SljitAccumulateDistinctGroupKey<bool>(keys, count, range.min_value, span, stride, scratch);
			break;
		case PhysicalType::INT8:
			SljitAccumulateDistinctGroupKey<int8_t>(keys, count, range.min_value, span, stride, scratch);
			break;
		case PhysicalType::INT16:
			SljitAccumulateDistinctGroupKey<int16_t>(keys, count, range.min_value, span, stride, scratch);
			break;
		case PhysicalType::INT32:
			SljitAccumulateDistinctGroupKey<int32_t>(keys, count, range.min_value, span, stride, scratch);
			break;
		case PhysicalType::INT64:
			SljitAccumulateDistinctGroupKey<int64_t>(keys, count, range.min_value, span, stride, scratch);
			break;
		default:
			return false;
		}
		stride *= span;
	}
	new_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto index = scratch.domain_index[row_idx];
		auto &word = scratch.seen[index / 64];
		const auto bit = uint64_t(1) << (index % 64);
		const bool seen = scratch.in_domain[row_idx] && (word & bit) != 0;
		word |= scratch.in_domain[row_idx] ? bit : 0;
		scratch.new_rows.set_index(new_count, row_idx);
		new_count += !seen;
	}
	return true;
}

} // namespace duckdb

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
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (SljitGroupedPrimitiveAggregateSinkKind(sink_info.kind)) {
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

static SinkResultType SljitExecuteDuckDBAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                        ExecutionOperatorRuntime &native_runtime,
                                                        SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                        SljitExecutableRegionOp &op, DataChunk &input,
                                                        const SelectionVector *execute_sel = nullptr,
                                                        idx_t count = DConstants::INVALID_INDEX) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	const bool sink_was_bound = scratch.HasSinkBinding(op_idx);
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	if (!sink_was_bound) {
		// the groups seen so far belong to the sink state this binding replaces
		scratch.ResetDistinctGroups(op_idx);
	}
	if (execute_sel != nullptr || count != input.size()) {
		throw InternalException("SLJIT native aggregate sink update requires a materialized input chunk");
	}
	if (!binding.aggregate_update.state) {
		throw InternalException("SLJIT aggregate update sink binding is missing native sink state");
	}
	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto sink_input = &input;
	if (op.aggregate_update.distinct_group_domain_size != 0) {
		auto &distinct_groups = scratch.DistinctGroups(op_idx, op.aggregate_update.distinct_group_domain_size);
		idx_t new_count;
		if (SljitSelectNewDistinctGroups(op.aggregate_update, input, distinct_groups, new_count)) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "native_sink_update.repeated_groups_dropped",
			                             input.size() - new_count);
			if (new_count == 0) {
				return SinkResultType::NEED_MORE_INPUT;
			}
			if (new_count < input.size()) {
				if (distinct_groups.new_input.ColumnCount() == 0) {
					distinct_groups.new_input.InitializeEmpty(input.GetTypes());
				}
				distinct_groups.new_input.Slice(input, distinct_groups.new_rows, new_count);
				sink_input = &distinct_groups.new_input;
			}
		}
	}
	auto sink_result = binding.aggregate_update.state->Sink(*sink_input);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "native_sink_update", aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "native_sink_update", input.size());
	RecordSljitRegionRuntimeDelegation(runtime, op.kind, "native_sink_update", input.size());
	return sink_result;
}

static SinkResultType SljitExecuteNativePipelineAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input, const SelectionVector *execute_sel = nullptr,
    idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	if (op.aggregate_update.plan.UsesPrimitivePayloads()) {
		return SljitExecutePrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel,
		                                            count, defer_grouped_finish);
	}
	return SljitExecuteDuckDBAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel, count);
}

} // namespace duckdb

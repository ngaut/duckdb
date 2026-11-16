//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_tail_delegation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_update_runtime.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

//! The generated steps' output enters DuckDB's own operators at the operator the tail starts at: DuckDB runs them,
//! their flushes and the sink. Selected rows enter as a slice of their chunk. A hash aggregate without aggregates
//! needs each group once: rows repeating a group of a small bounded domain this thread already handed to the sink stay
//! out of DuckDB's lookups
struct SljitNativeTailDelegationRuntimeState {
	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, idx_t tail_start_idx, const SljitRuntimeBatchView &input) {
		if (input.count == 0) {
			return false;
		}
		auto &tail_op = ops[tail_start_idx];
		RecordSljitRegionRuntimePath(runtime, tail_op.Kind(), "native_tail_delegation", input.count);
		RecordSljitRegionRuntimeDelegation(runtime, tail_op.Kind(), "native_tail_delegation", input.count);
		auto &chunk = SljitBindRuntimeBatchInput(input, "SLJIT native tail delegation");
		if (input.HasHashJoinSelection()) {
			throw InternalException("SLJIT native tail delegation received a hash join selection view");
		}
		reference<DataChunk> pushed(chunk);
		if (input.selection) {
			selected.Ensure(runtime.GetAllocator(), chunk.GetTypes());
			selected.chunk.Reset();
			selected.chunk.Slice(chunk, *input.selection, input.count);
			pushed = selected.chunk;
		}
		if (tail_op.Kind() == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		    tail_op.AggregateUpdate().distinct_group_domain_size != 0) {
			auto &aggregate_update = tail_op.AggregateUpdate();
			auto &distinct_groups = scratch.DistinctGroups(tail_start_idx, aggregate_update.distinct_group_domain_size);
			idx_t new_count;
			if (SljitSelectNewDistinctGroups(aggregate_update, pushed.get(), distinct_groups, new_count)) {
				RecordSljitRegionRuntimePath(runtime, tail_op.Kind(), "native_tail_delegation.repeated_groups_dropped",
				                             pushed.get().size() - new_count);
				if (new_count == 0) {
					return false;
				}
				if (new_count < pushed.get().size()) {
					if (distinct_groups.new_input.ColumnCount() == 0) {
						distinct_groups.new_input.InitializeEmpty(pushed.get().GetTypes());
					}
					distinct_groups.new_input.Slice(pushed.get(), distinct_groups.new_rows, new_count);
					pushed = distinct_groups.new_input;
				}
			}
		}
		auto sink_result =
		    runtime.ExecutionOperators().PushOperators(SljitNativeTailOperatorIndex(ops, tail_start_idx), pushed.get());
		return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
	}

private:
	SljitDataChunkBatch selected;
};

} // namespace duckdb

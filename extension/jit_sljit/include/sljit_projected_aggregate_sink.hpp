//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_aggregate_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_update_runtime.hpp"
#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

static bool SljitExecuteProjectedAggregateBatch(ExecutionRegionRuntime &runtime,
                                                ExecutionOperatorRuntime &native_runtime,
                                                SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
                                                SljitExecutableRegionOp &aggregate_op, DataChunk &input,
                                                bool &deferred_grouped_finish, ExecutionRegionResult &result) {
	if (input.size() == 0) {
		return false;
	}
	SinkResultType sink_result;
	if (SljitGroupedPrimitiveAggregateSinkKind(aggregate_op.aggregate_update.plan.sink_info.kind)) {
		if (!aggregate_op.aggregate_update.plan.UsesPrimitivePayloads()) {
			throw InternalException("SLJIT projected grouped aggregate recipe requires a bound grouped backend");
		}
		auto &bound_grouped_update = scratch.AggregateBoundGroupedUpdate(aggregate_idx);
		SljitBindGroupedPrimitiveAggregateUpdate(native_runtime, scratch, aggregate_idx, aggregate_op, input,
		                                         bound_grouped_update);
		sink_result = SljitExecuteBoundGroupedPrimitiveAggregateUpdate(runtime, scratch, bound_grouped_update, input,
		                                                               nullptr, input.size(), true,
		                                                               optional_ptr<bool>(&deferred_grouped_finish));
	} else {
		if (!aggregate_op.aggregate_update.plan.UsesPrimitivePayloads()) {
			throw InternalException("SLJIT projected aggregate recipe requires a primitive aggregate backend");
		}
		sink_result =
		    SljitExecutePrimitiveAggregateUpdate(runtime, native_runtime, scratch, aggregate_idx, aggregate_op, input,
		                                         nullptr, DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
	}
	if (SljitSinkResultStopsPipeline(sink_result)) {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, aggregate_idx, deferred_grouped_finish);
		return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
	}
	return false;
}

struct SljitProjectedAggregateSink {
	SljitProjectedAggregateSink(vector<SljitExecutableRegionOp> &ops_p, ExecutionRegionRuntime &runtime_p,
	                            ExecutionOperatorRuntime &native_runtime_p, SljitRegionExecutionScratch &scratch_p,
	                            ExecutionRegionResult &result_p, idx_t projection_idx_p,
	                            SljitExecutableRegionOp &projection_op_p, idx_t aggregate_idx_p,
	                            SljitExecutableRegionOp &aggregate_op_p, bool &deferred_grouped_finish_p,
	                            idx_t &processed_p, SljitDataChunkBatch &projected_batch_p, const char *append_phase_p,
	                            optional_ptr<SljitDirectJoinOutputAggregateStrategy> direct_aggregate_p = nullptr)
	    : ops(ops_p), runtime(runtime_p), native_runtime(native_runtime_p), scratch(scratch_p), result(result_p),
	      projection_idx(projection_idx_p), projection_op(projection_op_p), aggregate_idx(aggregate_idx_p),
	      aggregate_op(aggregate_op_p), deferred_grouped_finish(deferred_grouped_finish_p), processed(processed_p),
	      projected_batch(projected_batch_p), append_phase(append_phase_p), direct_aggregate(direct_aggregate_p) {
	}

	void FlushDirectAggregate() {
		SljitFlushDirectJoinOutputAggregate(runtime, ops, direct_aggregate);
	}

	void Charge(idx_t rows) {
		SljitChargeDownstreamRows(processed, rows);
	}

	bool ExecuteProjectedBatch(DataChunk &projected) {
		FlushDirectAggregate();
		if (projected.size() == 0) {
			return false;
		}
		if (SljitExecuteProjectedAggregateBatch(runtime, native_runtime, scratch, aggregate_idx, aggregate_op,
		                                        projected, deferred_grouped_finish, result)) {
			return true;
		}
		Charge(projected.size());
		return false;
	}

	bool FlushProjectedBatch() {
		FlushDirectAggregate();
		if (projected_batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(projected_batch.chunk,
		                                [&](DataChunk &projected) { return ExecuteProjectedBatch(projected); });
	}

	bool AppendProjectedBatch(DataChunk &projected) {
		projected_batch.Ensure(runtime.GetAllocator(), projection_op.output_types);
		return SljitAppendChunkToInitializedBatch(
		    runtime, projected_batch.chunk, projected, projection_idx,
		    optional_ptr<const SljitExecutableRegionOp>(&projection_op), append_phase,
		    [&]() { return FlushProjectedBatch(); }, [&](DataChunk &batch) { return ExecuteProjectedBatch(batch); });
	}

	template <class MATERIALIZE_BATCH>
	bool TryAppendDirectProjectedBatch(DataChunk &projected, bool &handled, MATERIALIZE_BATCH &&materialize_batch) {
		handled = false;
		if (projected.size() == 0) {
			return false;
		}
		FlushDirectAggregate();
		projected_batch.Ensure(runtime.GetAllocator(), projection_op.output_types);
		if (projected_batch.chunk.size() + projected.size() > STANDARD_VECTOR_SIZE) {
			if (FlushProjectedBatch()) {
				return true;
			}
		}
		if (!materialize_batch(projected_batch.chunk)) {
			return false;
		}
		handled = true;
		if (projected_batch.chunk.size() == STANDARD_VECTOR_SIZE) {
			if (FlushProjectedBatch()) {
				return true;
			}
		}
		return false;
	}

	void FinishDeferredGroupedUpdate() {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, aggregate_idx, deferred_grouped_finish);
	}

	optional_ptr<bool> DeferredGroupedFinishPtr() {
		return optional_ptr<bool>(&deferred_grouped_finish);
	}

	vector<SljitExecutableRegionOp> &ops;
	ExecutionRegionRuntime &runtime;
	ExecutionOperatorRuntime &native_runtime;
	SljitRegionExecutionScratch &scratch;
	ExecutionRegionResult &result;
	idx_t projection_idx;
	SljitExecutableRegionOp &projection_op;
	idx_t aggregate_idx;
	SljitExecutableRegionOp &aggregate_op;
	bool &deferred_grouped_finish;
	idx_t &processed;
	SljitDataChunkBatch &projected_batch;
	const char *append_phase;
	optional_ptr<SljitDirectJoinOutputAggregateStrategy> direct_aggregate;
};

} // namespace duckdb

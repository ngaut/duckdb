//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_tail_delegation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
struct SljitNativeTailDelegationRuntimeState {
	explicit SljitNativeTailDelegationRuntimeState(
	    EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from_p)
	    : execute_native_full_pipeline_from(execute_native_full_pipeline_from_p) {
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitFullPipelinePrimitiveStep &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		const auto tail_start_idx = primitive.Op(0);
		auto &tail_op = ops[tail_start_idx];
		RecordSljitRegionRuntimePath(runtime, tail_op.kind, "native_tail_delegation", input.count);
		RecordSljitRegionRuntimeDelegation(runtime, tail_op.kind, "native_tail_delegation", input.count);
		auto &chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT native tail delegation");
		auto sink_result = execute_native_full_pipeline_from(scratch, tail_start_idx, chunk);
		auto stopped = SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
		if (stopped) {
			execute_native_full_pipeline_from.Finalize(scratch);
			return true;
		}
		processed_batches++;
		return false;
	}

	void Finalize(SljitRegionExecutionScratch &scratch) {
		execute_native_full_pipeline_from.Finalize(scratch);
	}

private:
	EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from;
};

} // namespace duckdb

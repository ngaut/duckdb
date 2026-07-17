//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_materialize_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_drain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include <array>

namespace duckdb {

class SljitHashJoinProbeMaterializePrimitiveRuntime {
public:
	SljitHashJoinProbeMaterializePrimitiveRuntime(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                              vector<SljitExecutableRegionOp> &ops_p,
	                                              SljitRegionExecutionScratch &scratch_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p), scratch(scratch_p) {
	}

	template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_OUTPUT_BATCH>
	bool Execute(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             bool direct_handoff, EXECUTE_HASH_JOIN_PROBE &execute_hash_join_probe,
	             EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &join_input = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT hash join materialize primitive");
		if (join_input.size() == 0) {
			return false;
		}
		auto &primitive = step.hash_join_probe_materialize;
		const auto hash_join_idx = primitive.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		auto handle_output = [&](DataChunk &output) {
			return AppendBatch(step_idx, step, output, direct_handoff, execute_output_batch);
		};
		auto handle_defer = [&](string &deferred_reason) {
			if (Flush(step_idx, execute_output_batch)) {
				return true;
			}
			return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
		};
		return SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output,
		                                      execute_hash_join_probe, handle_output, handle_defer,
		                                      primitive.source_key0_int64_to_int32_unchecked);
	}

	template <class EXECUTE_OUTPUT_BATCH>
	bool Flush(idx_t step_idx, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &hash_join_materialize_batch = batches[step_idx];
		if (hash_join_materialize_batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(hash_join_materialize_batch.chunk, execute_output_batch);
	}

private:
	template <class EXECUTE_OUTPUT_BATCH>
	bool AppendBatch(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, DataChunk &output, bool direct_handoff,
	                 EXECUTE_OUTPUT_BATCH &execute_output_batch) {
		if (direct_handoff) {
			if (Flush(step_idx, execute_output_batch)) {
				return true;
			}
			const auto hash_join_idx = step.hash_join_probe_materialize.hash_join_idx;
			RecordSljitRegionRuntimePath(runtime, ops[hash_join_idx].kind, "direct_materialized_handoff",
			                             output.size());
			return execute_output_batch(output);
		}
		auto &hash_join_materialize_batch = batches[step_idx];
		hash_join_materialize_batch.Ensure(runtime.GetAllocator(), output.GetTypes());
		auto &batch = hash_join_materialize_batch.chunk;
		auto flush_batch = [&]() -> bool {
			return Flush(step_idx, execute_output_batch);
		};
		const auto hash_join_idx = step.hash_join_probe_materialize.hash_join_idx;
		return SljitAppendChunkToInitializedBatch(runtime, batch, output, hash_join_idx,
		                                          optional_ptr<const SljitExecutableRegionOp>(&ops[hash_join_idx]),
		                                          "hash_join_output_buffer_append", flush_batch, execute_output_batch);
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> batches;
};

} // namespace duckdb

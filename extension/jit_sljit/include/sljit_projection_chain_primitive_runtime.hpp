//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_chain_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"

#include <array>

namespace duckdb {

class SljitProjectionChainPrimitiveRuntime {
public:
	SljitProjectionChainPrimitiveRuntime(ExecutionRegionRuntime &runtime_p, vector<SljitExecutableRegionOp> &ops_p,
	                                     SljitRegionExecutionScratch &scratch_p)
	    : runtime(runtime_p), ops(ops_p), scratch(scratch_p) {
	}

	template <class EXECUTE_OUTPUT_BATCH>
	bool Execute(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             bool direct_handoff, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &projection_chain_batch = projection_chain_batches[step_idx];
		auto &selected_hash_join_input = selected_hash_join_inputs[step_idx];
		auto &synthetic_projection_output = synthetic_projection_outputs[step_idx];
		optional_ptr<SljitProjectionChainSyntheticProjectionScratch> synthetic_projection_scratch;
		if (step.projection_chain.HasBoundComposedProjection()) {
			synthetic_projection_scratch = &synthetic_projection_scratch_states[step_idx];
		}
		return SljitExecuteProjectionChainPrimitive(
		    runtime, scratch, ops, step.projection_chain, input, projection_chain_batch, selected_hash_join_input,
		    synthetic_projection_output, synthetic_projection_scratch, direct_handoff, execute_output_batch);
	}

	template <class EXECUTE_OUTPUT_BATCH>
	bool Flush(idx_t step_idx, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &projection_chain_batch = projection_chain_batches[step_idx];
		if (projection_chain_batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(projection_chain_batch.chunk, execute_output_batch);
	}

private:
	ExecutionRegionRuntime &runtime;
	vector<SljitExecutableRegionOp> &ops;
	SljitRegionExecutionScratch &scratch;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> projection_chain_batches;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> selected_hash_join_inputs;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> synthetic_projection_outputs;
	std::array<SljitProjectionChainSyntheticProjectionScratch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES>
	    synthetic_projection_scratch_states;
};

} // namespace duckdb

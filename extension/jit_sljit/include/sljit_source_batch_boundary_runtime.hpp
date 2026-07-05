//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_source_batch_boundary_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include <array>

namespace duckdb {

class SljitSourceBatchBoundaryRuntime {
public:
	SljitSourceBatchBoundaryRuntime(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                vector<SljitExecutableRegionOp> &ops_p)
	    : runtime(runtime_p), result(result_p), ops(ops_p) {
	}

	template <class EXECUTE_OUTPUT_BATCH>
	bool Execute(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step, const SljitRuntimeBatchView &input,
	             bool have_more_output, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT source batch boundary");
		if (chunk.size() == 0) {
			return false;
		}
		auto execute_boundary_batch = [&](DataChunk &batch, bool batch_has_more_output) -> bool {
			if (SljitAdvanceSinkBatchBlocked(runtime, batch, batch_has_more_output)) {
				return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
			}
			return execute_output_batch(batch, batch_has_more_output);
		};
		const auto op_idx = step.Op(0);
		auto &trace_op = ops[op_idx];
		RecordSljitRegionRuntimePath(runtime, trace_op.kind, "source_batch_boundary", chunk.size());
		auto flush_batch = [&](bool batch_has_more_output) -> bool {
			return FlushWithHaveMore(step_idx, execute_boundary_batch, batch_has_more_output);
		};

		auto &boundary_batch = boundary_batches[step_idx];
		boundary_batch.EnsureFromChunk(runtime.GetAllocator(), chunk);
		auto &batch = boundary_batch.chunk;
		if (batch.size() + chunk.size() > STANDARD_VECTOR_SIZE && flush_batch(true)) {
			return true;
		}
		if (!CanCoalesce(chunk)) {
			if (batch.size() > 0 && flush_batch(true)) {
				return true;
			}
			RecordSljitRegionRuntimePath(runtime, trace_op.kind, "source_batch_boundary_reference_handoff",
			                             chunk.size());
			return execute_boundary_batch(chunk, have_more_output);
		}
		if (!ShouldBatch(batch.size(), chunk.size())) {
			return execute_boundary_batch(chunk, have_more_output);
		}
		if (chunk.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
			return execute_boundary_batch(chunk, have_more_output);
		}
		auto stage_start = SljitRegionStageStart(runtime);
		if (!SljitTryFastAppendFixedAllValid(batch, chunk)) {
			batch.Append(chunk);
		}
		RecordSljitRegionStageRuntime(runtime, op_idx, trace_op.kind, "source_batch_boundary_append", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, trace_op.kind, "source_batch", chunk.size());
		if (batch.size() == STANDARD_VECTOR_SIZE && flush_batch(have_more_output)) {
			return true;
		}
		return false;
	}

	template <class EXECUTE_OUTPUT_BATCH>
	bool Flush(idx_t step_idx, EXECUTE_OUTPUT_BATCH &&execute_output_batch) {
		auto &boundary_batch = boundary_batches[step_idx];
		if (boundary_batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(boundary_batch.chunk, execute_output_batch);
	}

private:
	template <class EXECUTE_OUTPUT_BATCH>
	bool FlushWithHaveMore(idx_t step_idx, EXECUTE_OUTPUT_BATCH &&execute_output_batch, bool batch_has_more_output) {
		auto &boundary_batch = boundary_batches[step_idx];
		if (boundary_batch.Empty()) {
			return false;
		}
		auto execute_batch = [&](DataChunk &batch) {
			return execute_output_batch(batch, batch_has_more_output);
		};
		return SljitFlushDataChunkBatch(boundary_batch.chunk, execute_batch);
	}

	static bool ShouldBatch(idx_t pending_count, idx_t chunk_count) {
		if (pending_count != 0) {
			return true;
		}
		return chunk_count < STANDARD_VECTOR_SIZE / 2;
	}

	static bool CanCoalesce(DataChunk &chunk) {
		for (auto &vector : chunk.data) {
			if (!TypeIsConstantSize(vector.GetType().InternalType())) {
				return false;
			}
		}
		return true;
	}

	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	vector<SljitExecutableRegionOp> &ops;
	std::array<SljitDataChunkBatch, SLJIT_FULL_PIPELINE_MAX_PRIMITIVES> boundary_batches;
};

} // namespace duckdb

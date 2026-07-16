//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_source_fetch_primitive_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_runtime_batch_state.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

struct SljitSourceFetchPrimitiveRuntime {
	SljitSourceFetchPrimitiveRuntime(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                 const SljitFullPipelineRecipe &recipe)
	    : runtime(runtime_p), result(result_p), selected_hash_join_sink(recipe.UsesSelectedHashJoinSinkRuntime()),
	      preserves_partitioned_source_chunks(recipe.preserves_partitioned_source_chunks) {
	}

	template <class EXECUTE_NEXT_STEP>
	bool Execute(DataChunk &source_chunk, bool have_more_output, EXECUTE_NEXT_STEP &&execute_next_step) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (selected_hash_join_sink ||
		    (runtime.PreserveSourceChunkBoundaries() && preserves_partitioned_source_chunks) ||
		    !ShouldBatchSourceContractChunk(source_contract_batch.Count(), source_chunk.size())) {
			return ExecuteSourceChunk(source_chunk, have_more_output, execute_next_step);
		}
		if (source_contract_batch.Initialized() && source_contract_batch.ColumnCount() != source_chunk.ColumnCount()) {
			if (Flush(true, execute_next_step)) {
				return true;
			}
			return ExecuteSourceChunk(source_chunk, have_more_output, execute_next_step);
		}

		source_contract_batch.EnsureFromChunk(runtime.GetAllocator(), source_chunk);
		auto &batch = source_contract_batch.chunk;
		if (batch.size() + source_chunk.size() > STANDARD_VECTOR_SIZE && Flush(true, execute_next_step)) {
			return true;
		}
		if (!ShouldBatchSourceContractChunk(batch.size(), source_chunk.size())) {
			return ExecuteSourceChunk(source_chunk, have_more_output, execute_next_step);
		}

		if (!SljitTryFastAppendFixedAllValid(batch, source_chunk)) {
			batch.Append(source_chunk);
		}
		if (batch.size() == STANDARD_VECTOR_SIZE) {
			return Flush(have_more_output, execute_next_step);
		}
		return have_more_output ? false : Flush(false, execute_next_step);
	}

	template <class EXECUTE_NEXT_STEP>
	bool Flush(bool have_more_output, EXECUTE_NEXT_STEP &&execute_next_step) {
		if (source_contract_batch.Empty()) {
			return false;
		}
		auto execute_source_batch = [&](DataChunk &batch) {
			return ExecuteSourceChunk(batch, have_more_output, execute_next_step);
		};
		return SljitFlushDataChunkBatch(source_contract_batch.chunk, execute_source_batch);
	}

private:
	template <class EXECUTE_NEXT_STEP>
	bool ExecuteSourceChunk(DataChunk &source_chunk, bool have_more_output, EXECUTE_NEXT_STEP &execute_next_step) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, source_chunk.size());
		}
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		auto input_view = SljitRuntimeBatchViewFromChunk(source_chunk);
		return execute_next_step(input_view, have_more_output);
	}

	static bool ShouldBatchSourceContractChunk(idx_t pending_count, idx_t chunk_count) {
		if (pending_count != 0) {
			return true;
		}
		return chunk_count < STANDARD_VECTOR_SIZE / 2;
	}

private:
	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	bool selected_hash_join_sink;
	bool preserves_partitioned_source_chunks;
	SljitDataChunkBatch source_contract_batch;
};

} // namespace duckdb

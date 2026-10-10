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
	explicit SljitSourceFetchPrimitiveRuntime(ExecutionRegionRuntime &runtime_p) : runtime(runtime_p) {
	}

	template <class EXECUTE_NEXT_STEP>
	bool Execute(DataChunk &source_chunk, EXECUTE_NEXT_STEP &&execute_next_step) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (!ShouldBatchSourceContractChunk(source_contract_batch.Count(), source_chunk.size())) {
			return ExecuteSourceChunk(source_chunk, execute_next_step);
		}
		if (source_contract_batch.Initialized() && source_contract_batch.ColumnCount() != source_chunk.ColumnCount()) {
			if (Flush(execute_next_step)) {
				return true;
			}
			return ExecuteSourceChunk(source_chunk, execute_next_step);
		}

		source_contract_batch.EnsureFromChunk(runtime.GetAllocator(), source_chunk);
		auto &batch = source_contract_batch.chunk;
		if (batch.size() + source_chunk.size() > STANDARD_VECTOR_SIZE && Flush(execute_next_step)) {
			return true;
		}
		if (!ShouldBatchSourceContractChunk(batch.size(), source_chunk.size())) {
			return ExecuteSourceChunk(source_chunk, execute_next_step);
		}

		if (!SljitTryFastAppendFixedAllValid(batch, source_chunk)) {
			batch.Append(source_chunk);
		}
		if (batch.size() == STANDARD_VECTOR_SIZE) {
			return Flush(execute_next_step);
		}
		return false;
	}

	template <class EXECUTE_NEXT_STEP>
	bool Flush(EXECUTE_NEXT_STEP &&execute_next_step) {
		if (source_contract_batch.Empty()) {
			return false;
		}
		auto execute_source_batch = [&](DataChunk &batch) {
			return ExecuteSourceChunk(batch, execute_next_step);
		};
		return SljitFlushDataChunkBatch(source_contract_batch.chunk, execute_source_batch);
	}

private:
	template <class EXECUTE_NEXT_STEP>
	bool ExecuteSourceChunk(DataChunk &source_chunk, EXECUTE_NEXT_STEP &execute_next_step) {
		if (source_chunk.size() == 0) {
			return false;
		}
		if (runtime.TraceRuntime()) {
			runtime.RecordJitRuntimeProof(ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP, source_chunk.size());
		}
		auto input_view = SljitRuntimeBatchViewFromChunk(source_chunk);
		return execute_next_step(input_view);
	}

	static bool ShouldBatchSourceContractChunk(idx_t pending_count, idx_t chunk_count) {
		if (pending_count != 0) {
			return true;
		}
		return chunk_count < STANDARD_VECTOR_SIZE / 2;
	}

private:
	ExecutionRegionRuntime &runtime;
	SljitDataChunkBatch source_contract_batch;
};

} // namespace duckdb

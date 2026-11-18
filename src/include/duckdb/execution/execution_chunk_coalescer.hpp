//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_chunk_coalescer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_region_kernel.hpp"

namespace duckdb {

//! Core-owned compaction policy. Encodings useful to consumers pass through unchanged.
struct ExecutionChunkCoalescer {
	static constexpr idx_t MIN_PASSTHROUGH_ROWS = STANDARD_VECTOR_SIZE / 2;
	DUCKDB_API static bool CanAppend(const DataChunk &input);
	DUCKDB_API static void Append(DataChunk &target, DataChunk &input);
};

//! Buffers within the executor's partition boundary; it never owns source cursors or sink backpressure.
class DUCKDB_API ExecutionCoalescingChunkStep final : public ExecutionRegionChunkStep {
public:
	ExecutionCoalescingChunkStep(Allocator &allocator, unique_ptr<ExecutionRegionChunkStep> consumer);
	SinkResultType ExecuteChunk(DataChunk &input, ExecutionSourceChunkProof proof) override;
	void FlushChunks() override;
	void FinishChunks() override;
	optional_ptr<DataChunk> ContinuesNatively(DataChunk &input, ExecutionSourceChunkProof proof) override;

private:
	SinkResultType FlushBuffered();

private:
	Allocator &allocator;
	unique_ptr<ExecutionRegionChunkStep> consumer;
	DataChunk buffered;
	//! What the sources prove about every buffered row
	ExecutionSourceChunkProof buffered_proof;
	bool initialized = false;
	bool finished = false;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_order_execution_scratch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"
#include "sljit_scratch_access.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

struct SljitOrderExecutionScratch {
	void Resize(idx_t count) {
		key_chunks.resize(count);
		payload_chunks.resize(count);
	}

	void InitializeSink(Allocator &allocator, idx_t op_idx, const SljitExecutableRegionOp &op) {
		SljitInitializeScratchChunk(allocator, op.order_sink.plan.key_types, key_chunks[op_idx]);
		SljitInitializeScratchChunk(allocator, op.order_sink.plan.input_types, payload_chunks[op_idx]);
	}

	DataChunk &KeyChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(key_chunks, op_idx, "SLJIT ordered sink has no order-key scratch chunk");
	}

	DataChunk &PayloadChunk(idx_t op_idx) {
		return SljitCheckedScratchPtr(payload_chunks, op_idx, "SLJIT ordered sink has no payload scratch chunk");
	}

private:
	vector<unique_ptr<DataChunk>> key_chunks;
	vector<unique_ptr<DataChunk>> payload_chunks;
};

} // namespace duckdb

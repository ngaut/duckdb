//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_runtime_batch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_runtime_batch_state.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

static bool SljitTryFastAppendFixedFlatAllValid(DataChunk &target, DataChunk &source) {
	const auto append_count = source.size();
	if (append_count == 0) {
		return true;
	}
	if (target.ColumnCount() != source.ColumnCount()) {
		return false;
	}
	const auto target_count = target.size();
	const auto new_count = target_count + append_count;
	if (new_count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		if (target_vector.GetType() != source_vector.GetType()) {
			return false;
		}
		if (!TypeIsConstantSize(target_vector.GetType().InternalType())) {
			return false;
		}
		if (target_vector.GetVectorType() != VectorType::FLAT_VECTOR ||
		    source_vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		if (FlatVector::GetCapacity(target_vector) < new_count) {
			return false;
		}
		if (FlatVector::Validity(target_vector).CanHaveNull() ||
		    !FlatVector::Validity(source_vector).CheckAllValid(append_count)) {
			return false;
		}
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		const auto type_size = GetTypeIdSize(target_vector.GetType().InternalType());
		auto target_data = FlatVector::GetDataMutable(target_vector) + target_count * type_size;
		auto source_data = FlatVector::GetData(source_vector);
		memcpy(target_data, source_data, append_count * type_size);
		FlatVector::SetSize(target_vector, new_count);
	}
	target.CheckCardinality(new_count);
	return true;
}

template <class FLUSH_BATCH, class EXECUTE_BATCH>
static bool SljitAppendChunkToInitializedBatch(ExecutionRegionRuntime &runtime, DataChunk &batch, DataChunk &chunk,
                                               idx_t trace_op_idx, optional_ptr<const SljitExecutableRegionOp> trace_op,
                                               const char *append_phase, const char *boundary_phase,
                                               FLUSH_BATCH flush_batch, EXECUTE_BATCH execute_batch) {
	if (chunk.size() == 0) {
		return false;
	}
	if (batch.ColumnCount() != chunk.ColumnCount()) {
		if (flush_batch()) {
			return true;
		}
		return execute_batch(chunk);
	}
	if (batch.size() + chunk.size() > STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	if (chunk.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
		return execute_batch(chunk);
	}
	const bool trace_append = trace_op && append_phase;
	std::chrono::steady_clock::time_point append_stage_start;
	if (trace_append) {
		append_stage_start = SljitRegionStageStart(runtime);
	}
	if (!SljitTryFastAppendFixedFlatAllValid(batch, chunk)) {
		batch.Append(chunk);
	}
	if (trace_append) {
		RecordSljitRegionStageRuntime(runtime, trace_op_idx, trace_op->kind, append_phase, append_stage_start);
	}
	if (trace_op && boundary_phase) {
		RecordSljitRegionMaterializationBoundary(runtime, trace_op->kind, boundary_phase, chunk.size());
	}
	if (batch.size() == STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	return false;
}

template <class EXECUTE_BATCH>
static bool SljitFlushDataChunkBatch(DataChunk &batch, EXECUTE_BATCH execute_batch) {
	if (batch.size() == 0) {
		return false;
	}
	if (execute_batch(batch)) {
		return true;
	}
	batch.Reset();
	return false;
}

struct SljitRuntimeChunkBatch {
	explicit SljitRuntimeChunkBatch(ExecutionRegionRuntime &runtime_p) : runtime(runtime_p) {
	}

	SljitRuntimeChunkBatch(ExecutionRegionRuntime &runtime_p, idx_t trace_op_idx_p,
	                       optional_ptr<const SljitExecutableRegionOp> trace_op_p, const char *append_phase_p,
	                       const char *boundary_phase_p)
	    : runtime(runtime_p), trace_op_idx(trace_op_idx_p), trace_op(trace_op_p), append_phase(append_phase_p),
	      boundary_phase(boundary_phase_p) {
	}

	template <class EXECUTE_BATCH>
	bool Flush(EXECUTE_BATCH execute_batch) {
		if (batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(batch.chunk, execute_batch);
	}

	template <class EXECUTE_BATCH>
	bool Append(DataChunk &chunk, const vector<LogicalType> &batch_types, EXECUTE_BATCH execute_batch) {
		if (chunk.size() == 0) {
			return false;
		}
		batch.Ensure(runtime.GetAllocator(), batch_types);
		auto flush_batch = [&]() {
			return Flush(execute_batch);
		};
		return SljitAppendChunkToInitializedBatch(runtime, batch.chunk, chunk, trace_op_idx, trace_op, append_phase,
		                                          boundary_phase, flush_batch, execute_batch);
	}

	SljitDataChunkBatch &Batch() {
		return batch;
	}

private:
	ExecutionRegionRuntime &runtime;
	SljitDataChunkBatch batch;
	idx_t trace_op_idx = 0;
	optional_ptr<const SljitExecutableRegionOp> trace_op;
	const char *append_phase = nullptr;
	const char *boundary_phase = nullptr;
};

} // namespace duckdb

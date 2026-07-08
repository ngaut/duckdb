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
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

static bool SljitSourceVectorKnownAllValid(Vector &source) {
	switch (source.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		return FlatVector::Validity(source).CannotHaveNull();
	case VectorType::CONSTANT_VECTOR:
		return !ConstantVector::IsNull(source);
	case VectorType::DICTIONARY_VECTOR: {
		auto &child = DictionaryVector::Child(source);
		return child.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(child).CannotHaveNull();
	}
	default:
		return false;
	}
}

static bool SljitVectorCanFastAppendFixedAllValid(Vector &target, Vector &source, idx_t append_count,
                                                  idx_t new_count) {
	if (target.GetType() != source.GetType()) {
		return false;
	}
	if (target.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	if (FlatVector::GetCapacity(target) < new_count) {
		return false;
	}
	if (FlatVector::Validity(target).CanHaveNull() || !SljitSourceVectorKnownAllValid(source)) {
		return false;
	}
	const auto physical_type = target.GetType().InternalType();
	return TypeIsConstantSize(physical_type);
}

template <class T>
static void SljitCopyFixedWidthSelectedAllValid(const UnifiedVectorFormat &source_format, data_ptr_t target_data,
                                                idx_t append_count) {
	auto source_data = reinterpret_cast<const T *>(source_format.data);
	auto target = reinterpret_cast<T *>(target_data);
	for (idx_t row_idx = 0; row_idx < append_count; row_idx++) {
		target[row_idx] = source_data[source_format.sel->get_index(row_idx)];
	}
}

static void SljitCopyFixedWidthUnifiedAllValid(const UnifiedVectorFormat &source_format, data_ptr_t target_data,
                                               idx_t append_count, idx_t type_size) {
	if (source_format.sel == FlatVector::IncrementalSelectionVector()) {
		memcpy(target_data, source_format.data, append_count * type_size);
		return;
	}
	switch (type_size) {
	case sizeof(uint8_t):
		SljitCopyFixedWidthSelectedAllValid<uint8_t>(source_format, target_data, append_count);
		return;
	case sizeof(uint16_t):
		SljitCopyFixedWidthSelectedAllValid<uint16_t>(source_format, target_data, append_count);
		return;
	case sizeof(uint32_t):
		SljitCopyFixedWidthSelectedAllValid<uint32_t>(source_format, target_data, append_count);
		return;
	case sizeof(uint64_t):
		SljitCopyFixedWidthSelectedAllValid<uint64_t>(source_format, target_data, append_count);
		return;
	default:
		break;
	}
	for (idx_t row_idx = 0; row_idx < append_count; row_idx++) {
		const auto source_idx = source_format.sel->get_index(row_idx);
		memcpy(target_data + row_idx * type_size, source_format.data + source_idx * type_size, type_size);
	}
}

static void SljitFastAppendFixedAllValidVector(Vector &target, Vector &source, idx_t target_count, idx_t append_count,
                                               idx_t new_count) {
	const auto physical_type = target.GetType().InternalType();
	const auto type_size = GetTypeIdSize(physical_type);
	auto target_data = FlatVector::GetDataMutable(target) + target_count * type_size;
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	SljitCopyFixedWidthUnifiedAllValid(source_format, target_data, append_count, type_size);
	FlatVector::SetSize(target, new_count);
}

static bool SljitTryFastAppendFixedAllValid(DataChunk &target, DataChunk &source) {
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
		if (!SljitVectorCanFastAppendFixedAllValid(target.data[col_idx], source.data[col_idx], append_count,
		                                           new_count)) {
			return false;
		}
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		SljitFastAppendFixedAllValidVector(target.data[col_idx], source.data[col_idx], target_count, append_count,
		                                    new_count);
	}
	target.CheckCardinality(new_count);
	return true;
}

template <class FLUSH_BATCH, class EXECUTE_BATCH>
static bool SljitAppendChunkToInitializedBatch(ExecutionRegionRuntime &runtime, DataChunk &batch, DataChunk &chunk,
                                               idx_t trace_op_idx, optional_ptr<const SljitExecutableRegionOp> trace_op,
                                               const char *append_phase, FLUSH_BATCH flush_batch,
                                               EXECUTE_BATCH execute_batch) {
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
	if (!SljitTryFastAppendFixedAllValid(batch, chunk)) {
		batch.Append(chunk);
	}
	if (trace_append) {
		RecordSljitRegionStageRuntime(runtime, trace_op_idx, trace_op->kind, append_phase, append_stage_start);
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
	                       optional_ptr<const SljitExecutableRegionOp> trace_op_p, const char *append_phase_p)
	    : runtime(runtime_p), trace_op_idx(trace_op_idx_p), trace_op(trace_op_p), append_phase(append_phase_p) {
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
		                                          flush_batch, execute_batch);
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
};

} // namespace duckdb

#include "duckdb/execution/execution_chunk_coalescer.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

bool ExecutionChunkCoalescer::CanAppend(const DataChunk &input) {
	for (auto &column : input.data) {
		switch (column.GetVectorType()) {
		case VectorType::FLAT_VECTOR:
		case VectorType::CONSTANT_VECTOR:
		case VectorType::SEQUENCE_VECTOR:
			break;
		case VectorType::DICTIONARY_VECTOR:
			if (DictionaryVector::IsGlobalDictionary(column) || !DictionaryVector::DictionaryId(column).empty()) {
				return false;
			}
			break;
		default:
			return false;
		}
		auto type = column.GetType().InternalType();
		if (!TypeIsConstantSize(type) && type != PhysicalType::VARCHAR) {
			return false;
		}
	}
	return true;
}

static const_data_ptr_t CoalescerAllValidData(Vector &column, optional_ptr<const SelectionVector> &selection) {
	selection = nullptr;
	reference<Vector> flat(column);
	if (column.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		selection = &DictionaryVector::SelVector(column);
		flat = DictionaryVector::Child(column);
	}
	if (flat.get().GetVectorType() != VectorType::FLAT_VECTOR || !FlatVector::Validity(flat.get()).CannotHaveNull()) {
		return nullptr;
	}
	return FlatVector::GetData(flat.get());
}

template <idx_t WIDTH>
static void CoalescerGather(data_ptr_t target, const_data_ptr_t source, const SelectionVector &selection, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		memcpy(target + i * WIDTH, source + selection.get_index(i) * WIDTH, WIDTH);
	}
}

void ExecutionChunkCoalescer::Append(DataChunk &target, DataChunk &input) {
	const auto offset = target.size();
	const auto count = input.size();
	if (offset + count > STANDARD_VECTOR_SIZE || target.ColumnCount() != input.ColumnCount()) {
		throw InternalException("Chunk coalescer capacity or column count mismatch");
	}
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		auto &source = input.data[i];
		auto &destination = target.data[i];
		destination.CheckCapacity(offset + count);
		D_ASSERT(source.GetType() == destination.GetType());
		if (source.GetType().InternalType() == PhysicalType::VARCHAR) {
			// State scans can borrow strings from a pinned partition that expires on the next fetch. Auxiliary
			// data is not an ownership guarantee for every value, so buffered strings use the native owning copy.
			VectorOperations::Copy(source, destination, count, 0, offset);
			continue;
		}
		optional_ptr<const SelectionVector> selection;
		auto source_data = CoalescerAllValidData(source, selection);
		if (!source_data || !FlatVector::Validity(destination).CannotHaveNull()) {
			VectorOperations::Copy(source, destination, count, 0, offset);
			continue;
		}
		const auto width = GetTypeIdSize(source.GetType().InternalType());
		auto target_data = FlatVector::GetDataMutable(destination) + offset * width;
		if (!selection) {
			memcpy(target_data, source_data, count * width);
			continue;
		}
		switch (width) {
		case 1:
			CoalescerGather<1>(target_data, source_data, *selection, count);
			break;
		case 2:
			CoalescerGather<2>(target_data, source_data, *selection, count);
			break;
		case 4:
			CoalescerGather<4>(target_data, source_data, *selection, count);
			break;
		case 8:
			CoalescerGather<8>(target_data, source_data, *selection, count);
			break;
		case 16:
			CoalescerGather<16>(target_data, source_data, *selection, count);
			break;
		default:
			VectorOperations::Copy(source, destination, count, 0, offset);
			break;
		}
	}
	target.SetChildCardinality(offset + count);
}

ExecutionCoalescingChunkStep::ExecutionCoalescingChunkStep(Allocator &allocator_p,
                                                           unique_ptr<ExecutionRegionChunkStep> consumer_p)
    : allocator(allocator_p), consumer(std::move(consumer_p)) {
	if (!consumer) {
		throw InternalException("Chunk coalescer requires a consumer");
	}
}

optional_ptr<DataChunk> ExecutionCoalescingChunkStep::ContinuesNatively(DataChunk &input,
                                                                        ExecutionSourceChunkProof proof) {
	auto native_input = consumer->ContinuesNatively(input, proof);
	if (native_input && buffered.size()) {
		// rows buffered from earlier chunks reach the consumer before DuckDB's operators take this one
		FlushBuffered();
		consumer->FlushChunks();
	}
	return native_input;
}

SinkResultType ExecutionCoalescingChunkStep::FlushBuffered() {
	if (!finished && buffered.size()) {
		finished = consumer->ExecuteChunk(buffered, buffered_proof) == SinkResultType::FINISHED;
		buffered.Reset();
	}
	return finished ? SinkResultType::FINISHED : SinkResultType::NEED_MORE_INPUT;
}

SinkResultType ExecutionCoalescingChunkStep::ExecuteChunk(DataChunk &input, ExecutionSourceChunkProof proof) {
	if (finished) {
		return SinkResultType::FINISHED;
	}
	if (!input.size()) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (!buffered.size() && input.size() >= ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS) {
		finished = consumer->ExecuteChunk(input, proof) == SinkResultType::FINISHED;
		return finished ? SinkResultType::FINISHED : SinkResultType::NEED_MORE_INPUT;
	}
	if (!ExecutionChunkCoalescer::CanAppend(input)) {
		if (FlushBuffered() == SinkResultType::FINISHED) {
			return SinkResultType::FINISHED;
		}
		finished = consumer->ExecuteChunk(input, proof) == SinkResultType::FINISHED;
		return finished ? SinkResultType::FINISHED : SinkResultType::NEED_MORE_INPUT;
	}
	if (buffered.size() + input.size() > STANDARD_VECTOR_SIZE && FlushBuffered() == SinkResultType::FINISHED) {
		return SinkResultType::FINISHED;
	}
	if (!buffered.size() && input.size() >= ExecutionChunkCoalescer::MIN_PASSTHROUGH_ROWS) {
		finished = consumer->ExecuteChunk(input, proof) == SinkResultType::FINISHED;
		return finished ? SinkResultType::FINISHED : SinkResultType::NEED_MORE_INPUT;
	}
	if (!initialized) {
		buffered.Initialize(allocator, input.GetTypes());
		initialized = true;
	}
	buffered_proof = buffered.size() ? buffered_proof.Merge(proof) : proof;
	ExecutionChunkCoalescer::Append(buffered, input);
	return buffered.size() == STANDARD_VECTOR_SIZE ? FlushBuffered() : SinkResultType::NEED_MORE_INPUT;
}

void ExecutionCoalescingChunkStep::FlushChunks() {
	if (FlushBuffered() != SinkResultType::FINISHED) {
		consumer->FlushChunks();
	}
}

void ExecutionCoalescingChunkStep::FinishChunks() {
	FlushBuffered();
	consumer->FinishChunks();
}

} // namespace duckdb
